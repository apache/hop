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

package org.apache.hop.beam.pipeline.handler;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.HopRowToParquetFn;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.parquet.transforms.output.ParquetField;
import org.apache.hop.parquet.transforms.output.ParquetOutput;
import org.apache.hop.parquet.transforms.output.ParquetOutputMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Issue #2357: Parquet file output on Beam.
 *
 * <p>One file, written with {@code FileIO.write().via(ParquetIO.sink(schema))}. Date and time are
 * resolved when the pipeline is submitted. Copy number and split number are not extra Beam shards.
 * Partitioned output, a field with its own Parquet type, and a JSON or UUID field are refused. JSON
 * and UUID become their own Parquet columns on the local engine. ParquetIO would write both as
 * plain strings.
 */
public class BeamParquetOutputTransformHandler extends BeamBaseTransformHandler
    implements IBeamPipelineTransformHandler {

  @Override
  public boolean isInput() {
    return false;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      String runConfigurationName,
      IBeamPipelineEngineRunConfiguration runConfiguration,
      String dataSamplersJson,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      Map<String, PCollection<HopRow>> transformCollectionMap,
      Pipeline pipeline,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      PCollection<HopRow> input,
      String parentLogChannelId)
      throws HopException {

    if (input == null || previousTransforms == null || previousTransforms.isEmpty()) {
      throw new HopException(
          "Parquet output '" + transformMeta.getName() + "' needs one incoming transform");
    }
    ParquetOutputMeta meta = new ParquetOutputMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);
    if (meta.isPartitioning()) {
      throw new HopException(
          "Parquet output '" + transformMeta.getName() + "' cannot partition files on Beam");
    }
    if (rowMeta == null) {
      throw new HopException(
          "Parquet output '" + transformMeta.getName() + "' has no incoming row layout");
    }

    List<ParquetField> fields = new ArrayList<>();
    List<IValueMeta> valueMetas = new ArrayList<>();
    List<String> sourceFields = new ArrayList<>();
    if (meta.getFields() != null) {
      for (ParquetField field : meta.getFields()) {
        if (field == null) {
          continue;
        }
        if (field.parquetFieldType() != null) {
          throw new HopException(
              "Parquet output '"
                  + transformMeta.getName()
                  + "' cannot write Parquet type '"
                  + field.getParquetType()
                  + "' on Beam");
        }
        if (rowMeta.indexOfValue(field.getSourceFieldName()) < 0) {
          throw new HopException(
              "Field [" + field.getSourceFieldName() + "] couldn't be found in the input stream!");
        }
        IValueMeta valueMeta =
            rowMeta.getValueMeta(rowMeta.indexOfValue(field.getSourceFieldName()));
        if (valueMeta.getType() == IValueMeta.TYPE_JSON
            || valueMeta.getType() == IValueMeta.TYPE_UUID) {
          String kind = valueMeta.getType() == IValueMeta.TYPE_JSON ? "JSON" : "UUID";
          throw new HopException(
              "Parquet output '"
                  + transformMeta.getName()
                  + "' cannot write "
                  + kind
                  + " field '"
                  + field.getSourceFieldName()
                  + "' on Beam. On the local engine a "
                  + kind
                  + " field is a "
                  + kind
                  + " column, and ParquetIO would write it as a plain string.");
        }
        ParquetField named = new ParquetField(field);
        if (Utils.isEmpty(named.getTargetFieldName())) {
          named.setTargetFieldName(named.getSourceFieldName());
        }
        fields.add(named);
        valueMetas.add(valueMeta);
        sourceFields.add(field.getSourceFieldName());
      }
    }
    if (fields.isEmpty()) {
      throw new HopException(
          "Parquet output '" + transformMeta.getName() + "' has no output fields");
    }

    Schema schema = ParquetOutput.avroSchema(fields, valueMetas);
    String filename = outputFile(meta, variables);
    createParent(meta, filename);
    File file = new File(filename);
    String directory = file.getParent() == null ? "." : file.getParent();
    String onlyName = file.getName();

    org.apache.beam.sdk.io.parquet.ParquetIO.Sink sink =
        org.apache.beam.sdk.io.parquet.ParquetIO.sink(schema);
    CompressionCodecName codec = meta.getCompressionCodec();
    if (codec != null) {
      sink = sink.withCompressionCodec(codec);
    }
    int rowGroup = Const.toInt(variables.resolve(meta.getRowGroupSize()), -1);
    if (rowGroup > 0) {
      sink = sink.withRowGroupSize(rowGroup);
    }
    int page = Const.toInt(variables.resolve(meta.getDataPageSize()), -1);
    if (page > 0) {
      sink = sink.withPageSize(page);
    }

    PCollection<GenericRecord> records =
        input
            .apply(
                transformMeta.getName() + " To Parquet",
                ParDo.of(
                    new HopRowToParquetFn(
                        transformMeta.getName(),
                        JsonRowMeta.toJson(rowMeta),
                        schema.toString(),
                        sourceFields)))
            .setCoder(AvroCoder.of(schema));
    records.apply(
        transformMeta.getName() + " Write",
        FileIO.<GenericRecord>write()
            .via(sink)
            .to(directory)
            .withNumShards(1)
            .withIgnoreWindowing()
            .withTempDirectory(directory)
            .withNaming(new FixedName(onlyName)));

    transformCollectionMap.put(transformMeta.getName(), input);
    log.logBasic(
        "Handled transform (PARQUET OUTPUT) : " + transformMeta.getName() + ", " + filename);
  }

  static String outputFile(ParquetOutputMeta meta, IVariables variables) throws HopException {
    if (Utils.isEmpty(meta.getFilenameBase())) {
      throw new HopException("Parquet output has no filename");
    }
    String filename = variables.resolve(meta.getFilenameBase());
    Date date = new Date();
    if (meta.isFilenameIncludingDate()) {
      filename += "-" + new SimpleDateFormat("yyyyMMdd").format(date);
    }
    if (meta.isFilenameIncludingTime()) {
      filename += "-" + new SimpleDateFormat("HHmmss").format(date);
    }
    if (meta.isFilenameIncludingDateTime()) {
      filename +=
          "-"
              + new SimpleDateFormat(variables.resolve(meta.getFilenameDateTimeFormat()))
                  .format(date);
    }
    String extension = Const.NVL(variables.resolve(meta.getFilenameExtension()), "parquet");
    CompressionCodecName codec =
        meta.getCompressionCodec() == null
            ? CompressionCodecName.UNCOMPRESSED
            : meta.getCompressionCodec();
    String compressionExtension = codec.getExtension();
    if (meta.isFilenameCompressionBeforeExtension()) {
      filename += compressionExtension;
      filename += "." + extension;
    } else {
      filename += "." + extension;
      filename += compressionExtension;
    }
    return filename;
  }

  private static void createParent(ParquetOutputMeta meta, String filename) throws HopException {
    if (!meta.isFilenameCreatingParentFolders()) {
      return;
    }
    FileObject parent = null;
    try {
      parent = HopVfs.getFileObject(filename).getParent();
      if (parent != null && !parent.exists()) {
        parent.createFolder();
      }
    } catch (Exception e) {
      throw new HopException("Unable to prepare the parent folder for " + filename, e);
    } finally {
      if (parent != null) {
        try {
          parent.close();
        } catch (Exception ignored) {
          // closing the parent is best-effort
        }
      }
    }
  }

  /** One shard, and the name the local transform would use without a copy or split number. */
  private static final class FixedName implements FileIO.Write.FileNaming {
    private final String name;

    private FixedName(String name) {
      this.name = name;
    }

    @Override
    public String getFilename(
        BoundedWindow window,
        PaneInfo pane,
        int numShards,
        int shardIndex,
        Compression compression) {
      return name;
    }
  }
}
