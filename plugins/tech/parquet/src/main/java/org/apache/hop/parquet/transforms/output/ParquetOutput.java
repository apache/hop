/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.parquet.transforms.output;

import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.vfs2.FileObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;

public class ParquetOutput extends BaseTransform<ParquetOutputMeta, ParquetOutputData> {

  public ParquetOutput(
      TransformMeta transformMeta,
      ParquetOutputMeta meta,
      ParquetOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {

    // Pre-calculate some values...
    //
    data.pageSize =
        Const.toInt(resolve(meta.getDataPageSize()), ParquetProperties.DEFAULT_PAGE_SIZE);
    data.dictionaryPageSize =
        Const.toInt(
            resolve(meta.getDictionaryPageSize()), ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE);
    data.rowGroupSize =
        Const.toInt(
            resolve(meta.getRowGroupSize()), ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT);
    data.maxSplitSizeRows = Const.toLong(resolve(meta.getFileSplitSize()), -1);

    return super.init();
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null && first) {
      logBasic("No rows found for processing, stopping transform");
      setOutputDone();
      return false;
    }

    if (row == null) {
      closeFile();
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.sourceFieldIndexes = new ArrayList<>();
      for (int i = 0; i < meta.getFields().size(); i++) {
        ParquetField field = meta.getFields().get(i);
        int index = getInputRowMeta().indexOfValue(field.getSourceFieldName());
        if (index < 0) {
          throw new HopException("Unable to find source field '" + field.getSourceFieldName());
        }
        data.sourceFieldIndexes.add(index);
      }
      openNewFile();
    }

    // See if we don't need to create a new file split into parts...
    //
    if (meta.isFilenameIncludingSplitNr()
        && data.maxSplitSizeRows > 0
        && data.splitRowCount >= data.maxSplitSizeRows) {
      // Close file and start a new one...
      //
      closeFile();
      openNewFile();
    }

    // Write the row, handled by class ParquetWriteSupport
    //
    try {
      IRowMeta parquetRowMeta = getInputRowMeta().clone();

      // convert date/timestamp => long
      for (int i = 0; i < data.sourceFieldIndexes.size(); i++) {
        int idx = data.sourceFieldIndexes.get(i);
        IValueMeta valueMeta = parquetRowMeta.getValueMeta(idx);
        if (valueMeta.getType() == IValueMeta.TYPE_TIMESTAMP) {
          // Update of type meta
          IValueMeta longMeta = new ValueMetaInteger(valueMeta.getName());
          longMeta.setConversionMask(valueMeta.getConversionMask());
          longMeta.setLength(valueMeta.getLength(), valueMeta.getPrecision());
          parquetRowMeta.setValueMeta(idx, longMeta);
        }
      }

      // Clone Rows and convert Date & Timetims to Long
      Object[] parquetRow = row.clone();
      for (int i = 0; i < data.sourceFieldIndexes.size(); i++) {
        int idx = data.sourceFieldIndexes.get(i);
        Object value = parquetRow[idx];
        if (getInputRowMeta().getValueMeta(idx).getType() == IValueMeta.TYPE_TIMESTAMP) {
          if (value instanceof java.util.Date) {
            parquetRow[idx] = ((java.util.Date) value).getTime();
          } else if (value instanceof byte[]) {
            String dateStr = new String((byte[]) value, StandardCharsets.UTF_8);
            SimpleDateFormat sdf =
                new SimpleDateFormat(parquetRowMeta.getValueMeta(idx).getFormatMask());
            Date date = sdf.parse(dateStr);
            parquetRow[idx] = date.getTime();
          }
        }
      }

      data.writer.write(new RowMetaAndData(parquetRowMeta, parquetRow));
      incrementLinesOutput();
      data.splitRowCount++;
    } catch (Exception e) {
      throw new HopException("Error writing row to parquet file", e);
    }

    putRow(getInputRowMeta(), row);
    return true;
  }

  private void openNewFile() throws HopException {

    data.splitRowCount = 0;
    data.split++;

    // Hadoop configuration
    //
    data.conf = new Configuration();

    // Parquet Properties
    //
    ParquetProperties.Builder builder = ParquetProperties.builder();
    builder =
        switch (meta.getVersion()) {
          case Version1 -> builder.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0);
          case Version2 -> builder.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0);
        };
    data.props = builder.build();

    SchemaBuilder.FieldAssembler<Schema> fieldAssembler =
        SchemaBuilder.record("ApacheHopParquetSchema").fields();

    // Build the Parquet Schema
    //
    for (int i = 0; i < meta.getFields().size(); i++) {
      ParquetField field = meta.getFields().get(i);
      IValueMeta valueMeta = getInputRowMeta().getValueMeta(data.sourceFieldIndexes.get(i));

      // Start a new field
      SchemaBuilder.BaseFieldTypeBuilder<Schema> fieldBuilder =
          fieldAssembler.name(field.getTargetFieldName()).type().nullable();

      // Match these data types with class ParquetWriteSupport
      //
      Schema timestampMilliType;
      fieldAssembler =
          switch (valueMeta.getType()) {
            case IValueMeta.TYPE_TIMESTAMP, IValueMeta.TYPE_DATE -> {
              timestampMilliType =
                  LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
              yield fieldAssembler
                  .name(field.getTargetFieldName())
                  .type()
                  .unionOf()
                  .nullType()
                  .and()
                  .type(timestampMilliType)
                  .endUnion()
                  .noDefault();
            }
            case IValueMeta.TYPE_INTEGER -> fieldBuilder.longType().noDefault();
            case IValueMeta.TYPE_NUMBER -> fieldBuilder.doubleType().noDefault();
            case IValueMeta.TYPE_BOOLEAN -> fieldBuilder.booleanType().noDefault();
            case IValueMeta.TYPE_STRING, IValueMeta.TYPE_BIGNUMBER ->
                // Convert BigDecimal to String,otherwise we'll have all sorts of conversion issues.
                //
                fieldBuilder.stringType().noDefault();
            case IValueMeta.TYPE_BINARY -> fieldBuilder.bytesType().noDefault();
            case IValueMeta.TYPE_JSON -> fieldBuilder.stringType().noDefault();
            case IValueMeta.TYPE_UUID -> fieldBuilder.stringType().noDefault();
            default ->
                throw new HopException(
                    "Writing Hop data type '"
                        + valueMeta.getTypeDesc()
                        + "' to Parquet is not supported");
          };
    }
    data.avroSchema = fieldAssembler.endRecord();

    // Convert from Avro to Parquet schema
    //
    MessageType messageType = new AvroSchemaConverter().convert(data.avroSchema);

    // Calculate the filename...
    //
    data.filename = buildFilename(getPipeline().getExecutionStartDate());

    try {
      FileObject fileObject = HopVfs.getFileObject(data.filename, variables);

      // See if we need to create the parent folder(s)...
      //
      if (meta.isFilenameCreatingParentFolders()) {
        FileObject parentFolder = fileObject.getParent();
        if (parentFolder != null && !parentFolder.exists()) {
          // Try to create the parent folder...
          //
          parentFolder.createFolder();
        }
      }

      data.outputStream = HopVfs.getOutputStream(data.filename, false, variables);
      data.outputFile = new ParquetOutputFile(data.outputStream);

      data.writer =
          new ParquetWriterBuilder(
                  messageType,
                  data.avroSchema,
                  data.outputFile,
                  data.sourceFieldIndexes,
                  meta.getFields())
              .withPageSize(data.pageSize)
              .withDictionaryPageSize(data.dictionaryPageSize)
              .withValidation(ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED)
              .withCompressionCodec(meta.getCompressionCodec())
              .withRowGroupSize(data.rowGroupSize)
              .withWriterVersion(data.props.getWriterVersion())
              .withWriteMode(ParquetFileWriter.Mode.CREATE)
              .build();

    } catch (Exception e) {
      throw new HopException("Unable to create output file '" + data.filename + "'", e);
    }
  }

  private String buildFilename(Date date) {
    String filename = resolve(meta.getFilenameBase());
    if (meta.isFilenameIncludingDate()) {
      filename += "-" + new SimpleDateFormat("yyyyMMdd").format(date);
    }
    if (meta.isFilenameIncludingTime()) {
      filename += "-" + new SimpleDateFormat("HHmmss").format(date);
    }
    if (meta.isFilenameIncludingDateTime()) {
      filename +=
          "-" + new SimpleDateFormat(resolve(meta.getFilenameDateTimeFormat())).format(date);
    }
    if (meta.isFilenameIncludingCopyNr()) {
      filename += "-" + new DecimalFormat("00").format(getCopyNr());
    }
    if (meta.isFilenameIncludingSplitNr()) {
      filename += "-" + new DecimalFormat("0000").format(data.split);
    }
    if (data.isBeamContext()) {
      filename += "_" + getLogChannelId() + "_" + data.getBeamBundleNr();
    }
    filename += "." + Const.NVL(resolve(meta.getFilenameExtension()), "parquet");
    filename += meta.getCompressionCodec().getExtension();
    return filename;
  }

  private void closeFile() throws HopException {
    try {
      data.writer.close();
    } catch (Exception e) {
      throw new HopException("Error closing file " + data.filename, e);
    }
  }

  @Override
  public void batchComplete() throws HopException {
    if (!data.isBeamContext()) {
      closeFile();
    }
  }

  @Override
  public void startBundle() throws HopException {
    if (!first) {
      openNewFile();
    }
  }

  @Override
  public void finishBundle() throws HopException {
    closeFile();
  }
}
