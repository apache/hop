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

import java.io.OutputStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Set;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.vfs2.FileObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
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
      if (!data.filesClosed) closeFiles();
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

      if (meta.isFilenameInField()) {
        data.filenameFieldIndex = getInputRowMeta().indexOfValue(meta.getFilenameField());
        if (data.filenameFieldIndex < 0) {
          throw new HopException("Unable to find filename field '" + meta.getFilenameField());
        }
      }

      if (!meta.isFilenameInField()) {
        openNewFile(row);
      }
    }

    // See if we don't need to create a new file split into parts...
    //
    if (meta.isFilenameIncludingSplitNr()
        && data.maxSplitSizeRows > 0
        && data.splitRowCount >= data.maxSplitSizeRows) {
      // Close file and start a new one...
      //
      closeFile(data.currentFilename);
      // File is closed. Remove it from the list of opened writers
      data.writers.remove(data.currentFilename);

      openNewFile(row);
    } else if (meta.isFilenameInField()) {
      openNewFile(row);
    }

    // Write the row, handled by class ParquetWriteSupport
    //
    try {
      data.writers.get(data.currentFilename).write(new RowMetaAndData(getInputRowMeta(), row));
      incrementLinesOutput();

      if (!meta.isFilenameInField()) data.splitRowCount++;

    } catch (Exception e) {
      throw new HopException("Error writing row to parquet file", e);
    }

    putRow(getInputRowMeta(), row);
    return true;
  }

  private void openNewFile(Object[] row) throws HopException {

    // Calculate the filename...
    //
    String filename = buildFilename(row, getPipeline().getExecutionStartDate());
    if (!data.writers.containsKey(filename)) {
      data.splitRowCount = 0;
      data.split++;

      // Hadoop configuration
      //
      data.conf = new Configuration();

      // Parquet Properties
      //
      ParquetProperties.Builder builder = ParquetProperties.builder();
      switch (meta.getVersion()) {
        case Version1:
          builder = builder.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0);
          break;
        case Version2:
          builder = builder.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0);
          break;
      }
      data.props = builder.build();

      createParquetFileSchema();

      // Convert from Avro to Parquet schema
      //
      MessageType messageType = new AvroSchemaConverter().convert(data.avroSchema);

      try {
        logDebug("Opening file: " + filename);
        FileObject fileObject = HopVfs.getFileObject(filename);

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

        // adding filename to result
        if (meta.isAddToResultFilenames()) {
          // Add this to the result file names...
          ResultFile resultFile =
              new ResultFile(
                  ResultFile.FILE_TYPE_GENERAL,
                  fileObject,
                  getPipelineMeta().getName(),
                  getTransformName());
          resultFile.setComment("This file was created with a Parquet Output transform by Hop");
          addResultFile(resultFile);
        }

        OutputStream outputStream = HopVfs.getOutputStream(filename, false);
        ParquetOutputFile outputFile = new ParquetOutputFile(outputStream);

        data.writers.put(
            filename,
            new ParquetWriterBuilder(
                    messageType,
                    data.avroSchema,
                    outputFile,
                    data.sourceFieldIndexes,
                    meta.getFields())
                .withPageSize(data.pageSize)
                .withDictionaryPageSize(data.dictionaryPageSize)
                .withValidation(ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED)
                .withCompressionCodec(meta.getCompressionCodec())
                .withRowGroupSize(data.rowGroupSize)
                .withWriterVersion(data.props.getWriterVersion())
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .build());

      } catch (Exception e) {
        throw new HopException("Unable to create output file '" + filename + "'", e);
      }
    }
    data.currentFilename = filename;
  }

  private void createParquetFileSchema() throws HopException {
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
      switch (valueMeta.getType()) {
        case IValueMeta.TYPE_DATE:
          Schema timestampMilliType =
              LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
          fieldAssembler =
              fieldAssembler
                  .name(field.getTargetFieldName())
                  .type()
                  .unionOf()
                  .nullType()
                  .and()
                  .type(timestampMilliType)
                  .endUnion()
                  .noDefault();
          break;
        case IValueMeta.TYPE_INTEGER:
          fieldAssembler = fieldBuilder.longType().noDefault();
          break;
        case IValueMeta.TYPE_NUMBER:
          fieldAssembler = fieldBuilder.doubleType().noDefault();
          break;
        case IValueMeta.TYPE_BOOLEAN:
          fieldAssembler = fieldBuilder.booleanType().noDefault();
          break;
        case IValueMeta.TYPE_STRING:
        case IValueMeta.TYPE_BIGNUMBER:
          // Convert BigDecimal to String,otherwise we'll have all sorts of conversion issues.
          //
          fieldAssembler = fieldBuilder.stringType().noDefault();
          break;
        case IValueMeta.TYPE_BINARY:
          fieldAssembler = fieldBuilder.bytesType().noDefault();
          break;
        default:
          throw new HopException(
              "Writing Hop data type '"
                  + valueMeta.getTypeDesc()
                  + "' to Parquet is not supported");
      }
    }
    data.avroSchema = fieldAssembler.endRecord();
  }

  private String buildFilename(Object[] row, Date date) {

    String filename = null;

    if (!meta.isFilenameInField()) {
      filename = resolve(meta.getFilenameBase());
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
        filename += "_" + log.getLogChannelId() + "_" + data.getBeamBundleNr();
      }
      filename += "." + Const.NVL(resolve(meta.getFilenameExtension()), "parquet");
      filename += meta.getCompressionCodec().getExtension();
    } else {
      filename =
          (String) row[data.filenameFieldIndex]
              + "."
              + Const.NVL(resolve(meta.getFilenameExtension()), "parquet");
    }
    return filename;
  }

  private void closeFile(String filename) throws HopException {

    try {
      // Close connections of any managed
      data.writers.get(filename).close();
      logDebug("Closed file: " + filename);
    } catch (Exception e) {
      throw new HopException("Error closing file " + filename, e);
    }
  }

  private void closeFiles() throws HopException {

    Set<String> filesToClose = data.writers.keySet();
    for (String filename : filesToClose) {
      closeFile(filename);
    }
    data.filesClosed = true;
  }

  @Override
  public void batchComplete() throws HopException {
    if (!data.isBeamContext()) {
      if (!data.filesClosed) closeFiles();
    }
  }

  @Override
  public void startBundle() throws HopException {
    Object[] row = getRow();

    if (!first) {
      openNewFile(row);
    }
  }

  @Override
  public void finishBundle() throws HopException {
    if (!data.filesClosed) closeFiles();
  }

  @Override
  public void dispose() {
    if (!data.filesClosed) {
      try {
        closeFiles();
      } catch (HopException e) {
        logError("Error while closing parquets files", e);
      }
    }
    super.dispose();
  }
}
