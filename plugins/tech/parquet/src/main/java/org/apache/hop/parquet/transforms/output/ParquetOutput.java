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
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.Selectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.lineage.LineageFileIoEmitter;
import org.apache.hop.lineage.model.FileIoOperation;
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

  /** How many partitions are written to at the same time when nothing else is configured. */
  static final int DEFAULT_MAX_OPEN_PARTITIONS = 10;

  /**
   * The partition value Hive and Spark both use for a null, so that partitioned data written here
   * can be read back by them.
   */
  static final String DEFAULT_PARTITION_NAME = "__HIVE_DEFAULT_PARTITION__";

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
        Const.toIntExpanded(resolve(meta.getDataPageSize()), ParquetProperties.DEFAULT_PAGE_SIZE);
    data.dictionaryPageSize =
        Const.toIntExpanded(
            resolve(meta.getDictionaryPageSize()), ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE);
    data.rowGroupSize =
        Const.toIntExpanded(
            resolve(meta.getRowGroupSize()), ParquetProperties.DEFAULT_PAGE_ROW_COUNT_LIMIT);
    data.maxSplitSizeRows = Const.toLongExpanded(resolve(meta.getFileSplitSize()), -1);
    data.maxOpenPartitions =
        Const.toIntExpanded(resolve(meta.getMaxOpenPartitions()), DEFAULT_MAX_OPEN_PARTITIONS);
    if (data.maxOpenPartitions < 1) {
      data.maxOpenPartitions = 1;
    }

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
      if (meta.isPartitioning()) {
        closeAllPartitionWriters();
      } else {
        closeFile();
      }
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      resolveOutputFields();
      if (meta.isPartitioning()) {
        data.runToken = UUID.randomUUID().toString().substring(0, 8);
        initWriterProperties();
        data.messageType = buildSchema();
        data.partitionWriters = data.newPartitionWriterMap();
        data.clearedPartitions = new HashSet<>();
        if (meta.getWriteMode() == ParquetWriteMode.OverwriteAll) {
          clearBaseFolder();
        }
      } else {
        openNewFile();
      }
    }

    // See if we don't need to create a new file split into parts...
    //
    if (!meta.isPartitioning()
        && meta.isFilenameIncludingSplitNr()
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
          if (value instanceof java.util.Date date) {
            parquetRow[idx] = date.getTime();
          } else if (value instanceof byte[] bytes) {
            String dateStr = new String(bytes, StandardCharsets.UTF_8);
            SimpleDateFormat sdf =
                new SimpleDateFormat(parquetRowMeta.getValueMeta(idx).getFormatMask());
            Date date = sdf.parse(dateStr);
            parquetRow[idx] = date.getTime();
          }
        }
      }

      if (meta.isPartitioning()) {
        ParquetOutputData.PartitionWriter partitionWriter =
            getPartitionWriter(partitionPath(getInputRowMeta(), row));
        partitionWriter.writer.write(new RowMetaAndData(parquetRowMeta, parquetRow));
        partitionWriter.rowCount++;
      } else {
        data.writer.write(new RowMetaAndData(parquetRowMeta, parquetRow));
        data.splitRowCount++;
      }
      incrementLinesOutput();
    } catch (Exception e) {
      throw new HopException("Error writing row to parquet file", e);
    }

    putRow(getInputRowMeta(), row);
    return true;
  }

  private void openNewFile() throws HopException {
    data.splitRowCount = 0;
    data.split++;

    initWriterProperties();

    MessageType messageType = buildSchema();

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
      data.countingStream = new CountingOutputStream(data.outputStream);
      data.outputFile = new ParquetOutputFile(data.countingStream);

      data.writer =
          new ParquetWriterBuilder(
                  messageType,
                  data.avroSchema,
                  data.outputFile,
                  data.sourceFieldIndexes,
                  data.outputFields)
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

  /**
   * Sets up the Hadoop configuration and Parquet properties. Both the single-file and the
   * partitioned paths need these before a writer can be built.
   */
  private void initWriterProperties() {
    data.conf = new Configuration();

    ParquetProperties.Builder builder = ParquetProperties.builder();
    builder =
        switch (meta.getVersion()) {
          case Version1 -> builder.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0);
          case Version2 -> builder.withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0);
        };
    data.props = builder.build();
  }

  /**
   * Builds the Avro schema for the resolved output fields and converts it to a Parquet schema. Kept
   * separate from opening a file so the partitioned path can build it once and reuse it for every
   * partition.
   */
  private MessageType buildSchema() throws HopException {
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler =
        SchemaBuilder.record("ApacheHopParquetSchema").fields();

    // Build the Parquet Schema
    for (int i = 0; i < data.outputFields.size(); i++) {
      ParquetField field = data.outputFields.get(i);
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
    return new AvroSchemaConverter().convert(data.avroSchema);
  }

  void resolveOutputFields() throws HopException {
    data.outputFields = new ArrayList<>();
    data.sourceFieldIndexes = new ArrayList<>();
    resolvePartitionFields();

    if (meta.getFields() == null || meta.getFields().isEmpty()) {
      IRowMeta inputRowMeta = getInputRowMeta();
      for (int i = 0; i < inputRowMeta.size(); i++) {
        if (data.partitionFieldIndexes.contains(i)) {
          // The value lives in the folder name, so it is not written into the file. This is what
          // Spark's partitionBy() does, and what a reader expects when it recovers the column from
          // the path.
          continue;
        }
        String fieldName = inputRowMeta.getValueMeta(i).getName();
        data.outputFields.add(new ParquetField(fieldName, fieldName));
        data.sourceFieldIndexes.add(i);
      }
      verifyFieldsRemain();
      return;
    }

    for (ParquetField field : meta.getFields()) {
      int index = getInputRowMeta().indexOfValue(field.getSourceFieldName());
      if (index < 0) {
        throw new HopException("Unable to find source field '" + field.getSourceFieldName() + "'");
      }
      if (data.partitionFieldIndexes.contains(index)) {
        continue;
      }
      String targetFieldName = Const.NVL(field.getTargetFieldName(), field.getSourceFieldName());
      data.outputFields.add(new ParquetField(field.getSourceFieldName(), targetFieldName));
      data.sourceFieldIndexes.add(index);
    }
    verifyFieldsRemain();
  }

  /** Resolves the configured partition field names to indexes in the incoming row. */
  private void resolvePartitionFields() throws HopException {
    data.partitionFieldIndexes = new ArrayList<>();
    if (!meta.isPartitioning()) {
      return;
    }
    for (ParquetPartitionField field : meta.getPartitionFields()) {
      String name = field.getName();
      if (name == null || name.trim().isEmpty()) {
        continue;
      }
      int index = getInputRowMeta().indexOfValue(name);
      if (index < 0) {
        throw new HopException("Unable to find partition field '" + name + "' in the input");
      }
      if (data.partitionFieldIndexes.contains(index)) {
        throw new HopException("Partition field '" + name + "' is listed more than once");
      }
      data.partitionFieldIndexes.add(index);
    }
  }

  private void verifyFieldsRemain() throws HopException {
    if (data.outputFields.isEmpty()) {
      throw new HopException(
          "Every output field is a partition field, which would leave nothing to write into the "
              + "Parquet files. Leave at least one non-partition field.");
    }
  }

  String buildFilename(Date date) {
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
    String extension = Const.NVL(resolve(meta.getFilenameExtension()), "parquet");
    String compressionExtension = meta.getCompressionCodec().getExtension();
    if (meta.isFilenameCompressionBeforeExtension()) {
      // Spark-style: file.snappy.parquet
      filename += compressionExtension;
      filename += "." + extension;
    } else {
      // Backward compatible: file.parquet.snappy
      filename += "." + extension;
      filename += compressionExtension;
    }
    return filename;
  }

  /**
   * Builds the {@code name=value/...} folder path for a row, in the configured partition field
   * order. Nulls become {@link #DEFAULT_PARTITION_NAME} and characters that would break the path
   * are percent-escaped, both so the result can be read back by Hive and Spark.
   */
  String partitionPath(IRowMeta rowMeta, Object[] row) throws HopException {
    StringBuilder path = new StringBuilder();
    for (int i = 0; i < data.partitionFieldIndexes.size(); i++) {
      int index = data.partitionFieldIndexes.get(i);
      String value = rowMeta.getString(row, index);
      if (i > 0) {
        path.append('/');
      }
      path.append(escapePathValue(rowMeta.getValueMeta(index).getName()))
          .append('=')
          .append(
              value == null || value.isEmpty() ? DEFAULT_PARTITION_NAME : escapePathValue(value));
    }
    return path.toString();
  }

  /** Percent-escapes the characters that cannot appear in a partition folder name. */
  static String escapePathValue(String value) {
    StringBuilder escaped = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      boolean needsEscape =
          c == '%' || c == '=' || c == '/' || c == '\\' || c == ':' || c == '"' || c == '\''
              || c < 0x20 || c == 0x7f;
      if (needsEscape) {
        escaped.append('%').append(String.format("%02X", (int) c));
      } else {
        escaped.append(c);
      }
    }
    return escaped.toString();
  }

  /**
   * Returns the open writer for a partition, opening one if needed. When too many partitions are
   * open at once the least recently written one is closed first, so a wide partition key cannot run
   * the transform out of memory: every open Parquet writer buffers up to a full row group.
   */
  private ParquetOutputData.PartitionWriter getPartitionWriter(String partitionPath)
      throws HopException {
    ParquetOutputData.PartitionWriter partitionWriter = data.partitionWriters.get(partitionPath);

    if (partitionWriter != null
        && meta.isFilenameIncludingSplitNr()
        && data.maxSplitSizeRows > 0
        && partitionWriter.rowCount >= data.maxSplitSizeRows) {
      // This partition's current file is full, roll over to the next part.
      closePartitionWriter(partitionPath, partitionWriter);
      data.partitionWriters.remove(partitionPath);
      partitionWriter = null;
    }

    if (partitionWriter != null) {
      return partitionWriter;
    }

    while (data.partitionWriters.size() >= data.maxOpenPartitions) {
      Iterator<Map.Entry<String, ParquetOutputData.PartitionWriter>> iterator =
          data.partitionWriters.entrySet().iterator();
      Map.Entry<String, ParquetOutputData.PartitionWriter> oldest = iterator.next();
      closePartitionWriter(oldest.getKey(), oldest.getValue());
      iterator.remove();
    }

    partitionWriter = openPartitionWriter(partitionPath);
    data.partitionWriters.put(partitionPath, partitionWriter);
    return partitionWriter;
  }

  private ParquetOutputData.PartitionWriter openPartitionWriter(String partitionPath)
      throws HopException {
    String folder = partitionFolder(partitionPath);
    applyWriteMode(partitionPath, folder);

    String filename = buildPartitionFilename(folder, getPipeline().getExecutionStartDate());
    try {
      FileObject fileObject = HopVfs.getFileObject(filename, variables);
      FileObject parentFolder = fileObject.getParent();
      if (parentFolder != null && !parentFolder.exists()) {
        // Partition folders are created regardless of the "create parent folders" option: the
        // layout is what the transform was asked to produce, not something the user typed.
        parentFolder.createFolder();
      }

      OutputStream outputStream = HopVfs.getOutputStream(filename, false, variables);
      CountingOutputStream countingStream = new CountingOutputStream(outputStream);
      ParquetOutputFile outputFile = new ParquetOutputFile(countingStream);

      ParquetWriter<RowMetaAndData> writer =
          new ParquetWriterBuilder(
                  data.messageType,
                  data.avroSchema,
                  outputFile,
                  data.sourceFieldIndexes,
                  data.outputFields)
              .withPageSize(data.pageSize)
              .withDictionaryPageSize(data.dictionaryPageSize)
              .withValidation(ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED)
              .withCompressionCodec(meta.getCompressionCodec())
              .withRowGroupSize(data.rowGroupSize)
              .withWriterVersion(data.props.getWriterVersion())
              .withWriteMode(ParquetFileWriter.Mode.CREATE)
              .build();

      if (isDetailed()) {
        logDetailed("Opened partition file '" + filename + "'");
      }
      return new ParquetOutputData.PartitionWriter(
          filename, outputStream, countingStream, outputFile, writer);
    } catch (Exception e) {
      throw new HopException("Unable to create partition output file '" + filename + "'", e);
    }
  }

  /** The folder a partition's files go into: the base folder plus the partition path. */
  private String partitionFolder(String partitionPath) {
    String base = resolve(meta.getFilenameBase());
    if (base.endsWith("/")) {
      base = base.substring(0, base.length() - 1);
    }
    return base + "/" + toVfsPath(partitionPath);
  }

  /**
   * HopVfs resolves a path as a URI and percent-decodes it, so a {@code %2F} we wrote for a value
   * containing a separator would come back as a real folder level. Encoding our own {@code %} as
   * {@code %25} means VFS decodes it back to a literal {@code %}, leaving the Hive-style {@code
   * name=EU%2FWest} on disk for a reader to decode.
   */
  static String toVfsPath(String partitionPath) {
    return partitionPath.replace("%", "%25");
  }

  /**
   * Names a file inside a partition folder. The base name is not repeated, since the folder already
   * identifies the data; the copy number and a per-run sequence keep parallel copies and re-opened
   * partitions from colliding.
   */
  private String buildPartitionFilename(String folder, Date date) {
    StringBuilder filename = new StringBuilder(folder).append("/part");
    if (meta.isFilenameIncludingDate()) {
      filename.append('-').append(new SimpleDateFormat("yyyyMMdd").format(date));
    }
    if (meta.isFilenameIncludingTime()) {
      filename.append('-').append(new SimpleDateFormat("HHmmss").format(date));
    }
    if (meta.isFilenameIncludingDateTime()) {
      filename
          .append('-')
          .append(new SimpleDateFormat(resolve(meta.getFilenameDateTimeFormat())).format(date));
    }
    filename.append('-').append(new DecimalFormat("00").format(getCopyNr()));
    filename.append('-').append(new DecimalFormat("0000").format(data.partitionFileNr++));
    // Without something unique per run, a second run in append mode would write the same
    // part name and silently replace the first run's file.
    filename.append('-').append(data.runToken);
    if (data.isBeamContext()) {
      filename.append('_').append(getLogChannelId()).append('_').append(data.getBeamBundleNr());
    }
    String extension = Const.NVL(resolve(meta.getFilenameExtension()), "parquet");
    String compressionExtension = meta.getCompressionCodec().getExtension();
    if (meta.isFilenameCompressionBeforeExtension()) {
      filename.append(compressionExtension).append('.').append(extension);
    } else {
      filename.append('.').append(extension).append(compressionExtension);
    }
    return filename.toString();
  }

  /**
   * Applies the configured write mode the first time this run touches a partition folder. The
   * overwrite-all mode is handled once up front, before any file is opened.
   */
  private void applyWriteMode(String partitionPath, String folder) throws HopException {
    ParquetWriteMode mode = meta.getWriteMode();
    if (mode == null || mode == ParquetWriteMode.Append || mode == ParquetWriteMode.OverwriteAll) {
      return;
    }
    if (data.clearedPartitions.contains(partitionPath)) {
      // Already dealt with; a re-opened partition must not wipe what this run just wrote.
      return;
    }
    data.clearedPartitions.add(partitionPath);

    try {
      FileObject folderObject = HopVfs.getFileObject(folder, variables);
      if (!folderObject.exists()) {
        return;
      }
      if (mode == ParquetWriteMode.FailIfExists) {
        throw new HopException("Partition folder '" + folder + "' already exists");
      }
      // OverwritePartitions
      int deleted = folderObject.delete(Selectors.EXCLUDE_SELF);
      if (isDetailed()) {
        logDetailed("Emptied partition folder '" + folder + "', removed " + deleted + " file(s)");
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to apply write mode to partition folder '" + folder + "'", e);
    }
  }

  /** Empties the base folder once, for the overwrite-all mode. */
  private void clearBaseFolder() throws HopException {
    if (data.baseFolderCleared) {
      return;
    }
    data.baseFolderCleared = true;
    String base = partitionFolder("");
    try {
      FileObject folderObject = HopVfs.getFileObject(base, variables);
      if (folderObject.exists()) {
        int deleted = folderObject.delete(Selectors.EXCLUDE_SELF);
        logBasic("Emptied output folder '" + base + "', removed " + deleted + " file(s)");
      }
    } catch (Exception e) {
      throw new HopException("Unable to empty output folder '" + base + "'", e);
    }
  }

  private void closeAllPartitionWriters() throws HopException {
    if (data.partitionWriters == null) {
      return;
    }
    // Copy first: closing reports lineage, and the map must not be mutated while iterating.
    List<Map.Entry<String, ParquetOutputData.PartitionWriter>> open =
        new ArrayList<>(data.partitionWriters.entrySet());
    data.partitionWriters.clear();
    HopException firstFailure = null;
    for (Map.Entry<String, ParquetOutputData.PartitionWriter> entry : open) {
      try {
        closePartitionWriter(entry.getKey(), entry.getValue());
      } catch (HopException e) {
        // Keep closing the rest so no file is left half-written, then report the first failure.
        if (firstFailure == null) {
          firstFailure = e;
        }
      }
    }
    if (firstFailure != null) {
      throw firstFailure;
    }
  }

  private void closePartitionWriter(
      String partitionPath, ParquetOutputData.PartitionWriter partitionWriter) throws HopException {
    try {
      partitionWriter.writer.close();
      if (partitionWriter.countingStream != null) {
        long written = partitionWriter.countingStream.getCount();
        dataVolumeOut = (dataVolumeOut != null ? dataVolumeOut : 0L) + written;
        if (!data.isBeamContext() && written > 0) {
          try {
            FileObject outFile = HopVfs.getFileObject(partitionWriter.filename, variables);
            LineageFileIoEmitter.emitTransformFileIo(
                this, FileIoOperation.WRITE, null, outFile, written, true, null);
          } catch (Exception ignored) {
            // optional lineage
          }
        }
      }
    } catch (Exception e) {
      throw new HopException(
          "Error closing file "
              + partitionWriter.filename
              + " for partition '"
              + partitionPath
              + "'",
          e);
    }
  }

  private void closeFile() throws HopException {
    try {
      data.writer.close();
      if (data.countingStream != null) {
        long written = data.countingStream.getCount();
        dataVolumeOut = (dataVolumeOut != null ? dataVolumeOut : 0L) + written;
        if (!data.isBeamContext() && written > 0 && data.filename != null) {
          try {
            FileObject outFile = HopVfs.getFileObject(data.filename, variables);
            LineageFileIoEmitter.emitTransformFileIo(
                this, FileIoOperation.WRITE, null, outFile, written, true, null);
          } catch (Exception ignored) {
            // optional lineage
          }
        }
      }
    } catch (Exception e) {
      throw new HopException("Error closing file " + data.filename, e);
    }
  }

  @Override
  public void batchComplete() throws HopException {
    if (!data.isBeamContext()) {
      if (meta.isPartitioning()) {
        closeAllPartitionWriters();
      } else {
        closeFile();
      }
    }
  }

  @Override
  public void startBundle() throws HopException {
    if (!first && !meta.isPartitioning()) {
      openNewFile();
    }
  }

  @Override
  public void finishBundle() throws HopException {
    if (meta.isPartitioning()) {
      closeAllPartitionWriters();
    } else {
      closeFile();
    }
  }
}
