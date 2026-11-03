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

import java.net.URI;
import java.nio.file.Paths;
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
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
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
 * <p>One file, written with {@code FileIO.write().via(ParquetIO.sink(schema))}. The directory is
 * split from the filename without {@code java.io.File}, so a {@code gs://} or {@code s3://} URI
 * keeps both slashes. Date and time are resolved when the pipeline is submitted. Copy number and
 * split number are not extra Beam shards. Partitioned output, a field with its own Parquet type,
 * and a JSON or UUID field are refused. JSON and UUID become their own Parquet columns on the local
 * engine. ParquetIO would write both as plain strings.
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
    // Submission builds the graph before BeamPipelineEngine registers FileSystems. Register here
    // so a scheme that has a Beam filesystem is parsed by it. If that fails, the split below
    // keeps the scheme by cutting the string on the last '/'.
    registerFileSystems(pipeline);
    OutputLocation location = outputLocation(filename);
    String directory = location.directory;
    String onlyName = location.filename;

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

  /**
   * Directory passed to {@code FileIO.write().to} and {@code withTempDirectory}, and the file name
   * inside it.
   *
   * <p>{@code new File("gs://bucket/out/data.parquet").getParent()} is {@code gs:/bucket/out}. Beam
   * then writes to that directory. {@link FileSystems#matchNewResource} parses the path when that
   * scheme is registered. Otherwise the path is split on the last {@code '/'}, which keeps the
   * scheme's {@code //}. A {@code file://} URI is turned into its local path first, because the
   * local filesystem would resolve {@code file://} against the working directory.
   */
  static OutputLocation outputLocation(String filename) {
    String path = localPath(filename);
    if (path == null) {
      path = filename;
    }
    OutputLocation fromBeam = locationFromBeam(path);
    if (fromBeam != null) {
      return fromBeam;
    }
    return locationFromSlash(path);
  }

  /**
   * A {@code file://} URI names a local file, but {@code LocalFileSystem.matchNewResource} parses
   * it with {@code Paths.get}, which drops a slash and resolves it against the working directory.
   * The local path is what {@code FileIO} can write.
   */
  private static String localPath(String filename) {
    int colon = filename.indexOf(':');
    if (!isFileScheme(filename, colon)) {
      return null;
    }
    try {
      URI uri = URI.create(filename);
      try {
        return Paths.get(uri).toString();
      } catch (IllegalArgumentException authority) {
        String path = uri.getPath();
        if (path == null || path.isEmpty()) {
          return null;
        }
        return path;
      }
    } catch (RuntimeException ignored) {
      return null;
    }
  }

  private static void registerFileSystems(Pipeline pipeline) {
    if (pipeline == null) {
      return;
    }
    try {
      FileSystems.registerFileSystemsOnce(pipeline.getOptions());
    } catch (RuntimeException ignored) {
      // A registrar that cannot start must not stop the write. The string split keeps the scheme.
    }
  }

  private static OutputLocation locationFromBeam(String path) {
    try {
      ResourceId resource = FileSystems.matchNewResource(path, false);
      String name = resource.getFilename();
      if (name == null || name.isEmpty()) {
        return null;
      }
      String directory = resource.getCurrentDirectory().toString();
      if (!beamSplitKeepsThePath(path, directory, name)) {
        return null;
      }
      return new OutputLocation(withoutTrailingSeparator(directory), name);
    } catch (RuntimeException ignored) {
      return null;
    }
  }

  /** Reject a parse that dropped {@code ://} or turned a relative path into an absolute one. */
  private static boolean beamSplitKeepsThePath(String path, String directory, String name) {
    if (directory == null || directory.isEmpty()) {
      return false;
    }
    int scheme = path.indexOf("://");
    if (scheme >= 0 && !directory.startsWith(path.substring(0, scheme + 3))) {
      return false;
    }
    int slash = lastSeparator(path);
    String expectedName = slash < 0 ? path : path.substring(slash + 1);
    if (!expectedName.equals(name)) {
      return false;
    }
    boolean pathAbsolute = path.startsWith("/") || isWindowsDrive(path);
    boolean directoryAbsolute = directory.startsWith("/") || isWindowsDrive(directory);
    return pathAbsolute || !directoryAbsolute;
  }

  private static OutputLocation locationFromSlash(String path) {
    int slash = lastSeparator(path);
    if (slash < 0) {
      return new OutputLocation(".", path);
    }
    if (slash == 0) {
      return new OutputLocation("/", path.substring(1));
    }
    return new OutputLocation(path.substring(0, slash), path.substring(slash + 1));
  }

  private static int lastSeparator(String path) {
    int slash = path.lastIndexOf('/');
    if (path.contains("://")) {
      return slash;
    }
    return Math.max(slash, path.lastIndexOf('\\'));
  }

  private static String withoutTrailingSeparator(String directory) {
    if (directory.length() <= 1) {
      return directory;
    }
    char last = directory.charAt(directory.length() - 1);
    if (last != '/' && last != '\\') {
      return directory;
    }
    String stripped = directory.substring(0, directory.length() - 1);
    // Leave "gs://" and a Windows drive root "C:\" alone.
    if (stripped.endsWith(":/") || stripped.endsWith(":") || stripped.isEmpty()) {
      return directory;
    }
    return stripped;
  }

  private static boolean isWindowsDrive(String path) {
    return path.length() >= 2 && path.charAt(1) == ':' && Character.isLetter(path.charAt(0));
  }

  /**
   * A path with no scheme, a Windows drive, or a {@code file://} URI. {@code gs://}, {@code s3://}
   * and the other remote schemes are not.
   */
  static boolean isLocalPath(String filename) {
    if (filename == null || filename.isEmpty()) {
      return false;
    }
    int colon = filename.indexOf(':');
    if (colon < 0) {
      return true;
    }
    if (isWindowsDrive(filename)) {
      return true;
    }
    return isFileScheme(filename, colon);
  }

  private static boolean isFileScheme(String filename, int colon) {
    return colon == 4 && filename.regionMatches(true, 0, "file", 0, 4);
  }

  /** Directory {@code FileIO} writes into, and the single file name inside it. */
  static final class OutputLocation {
    final String directory;
    final String filename;

    private OutputLocation(String directory, String filename) {
      this.directory = directory;
      this.filename = filename;
    }
  }

  private static void createParent(ParquetOutputMeta meta, String filename) throws HopException {
    if (!meta.isFilenameCreatingParentFolders()) {
      return;
    }
    // Object stores have no real folders. Creating the parent through Hop VFS runs on the machine
    // that submits the pipeline and needs that machine's cloud credentials. Only a local path, or
    // a file:// URI, gets a parent folder.
    if (!isLocalPath(filename)) {
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
