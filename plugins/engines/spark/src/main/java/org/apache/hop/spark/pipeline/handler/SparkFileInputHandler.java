/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.spark.pipeline.handler;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.trim;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.core.SparkNativeMetrics;
import org.apache.hop.spark.engines.ISparkPipelineEngineRunConfiguration;
import org.apache.hop.spark.pkg.SparkProjectPackage;
import org.apache.hop.spark.transforms.io.SparkField;
import org.apache.hop.spark.transforms.io.SparkFileInputMeta;
import org.apache.hop.spark.util.SparkPathDialect;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Native Spark file read for {@link SparkFileInputMeta}.
 *
 * <p>CSV/text with an explicit field list is read as strings first, then projected/cast <strong>by
 * column name</strong>. Applying a StructType schema directly to the CSV reader maps by position,
 * so omitting a middle column (or a header/schema mismatch) silently shifts values under the wrong
 * names.
 */
public class SparkFileInputHandler extends SparkBaseTransformHandler {

  /** Common Hop/Spark date masks used when a field has no format set. */
  private static final String[] DEFAULT_DATE_FORMATS = {
    "yyyy/MM/dd", "yyyy-MM-dd", "MM/dd/yyyy", "dd/MM/yyyy", "yyyyMMdd"
  };

  private static final String[] DEFAULT_TIMESTAMP_FORMATS = {
    "yyyy/MM/dd HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy/MM/dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy/MM/dd",
    "yyyy-MM-dd"
  };

  @Override
  public boolean isInput() {
    return true;
  }

  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      String runConfigurationName,
      ISparkPipelineEngineRunConfiguration runConfiguration,
      IHopMetadataProvider metadataProvider,
      String metastoreJson,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      Map<String, Dataset<Row>> transformDatasetMap,
      SparkSession spark,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      Dataset<Row> input)
      throws HopException {

    SparkFileInputMeta meta = new SparkFileInputMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    if (log != null) {
      String phWarn = SparkProjectPackage.projectHomeDataPathWarning(meta.getFilePath());
      if (phWarn != null) {
        log.logBasic("WARNING: " + phWarn);
      }
    }
    String resolved = variables.resolve(meta.getFilePath());
    String path = SparkPathDialect.toSparkUri(resolved, runConfiguration);
    if (StringUtils.isEmpty(path)) {
      throw new HopException(
          "Spark File Input '" + transformMeta.getName() + "' has no file path configured");
    }
    if (log != null && StringUtils.isNotEmpty(resolved) && !resolved.trim().equals(path)) {
      log.logBasic(
          "Spark File Input '"
              + transformMeta.getName()
              + "' path scheme map: '"
              + resolved
              + "' -> '"
              + path
              + "'");
    }

    String format = SparkFileIoSupport.normalizeFormat(meta.getFileFormat());
    DataFrameReader reader = spark.read().format(format);

    Map<String, String> options =
        SparkFileIoSupport.parseExtraOptions(variables, meta.getExtraOptions());
    boolean delimited =
        SparkFileInputMeta.FORMAT_CSV.equals(format)
            || SparkFileInputMeta.FORMAT_TEXT.equals(format);

    if (delimited) {
      options.putIfAbsent("header", Boolean.toString(meta.isHeader()));
      if (StringUtils.isNotEmpty(meta.getSeparator())) {
        options.putIfAbsent("sep", variables.resolve(meta.getSeparator()));
      }
      if (StringUtils.isNotEmpty(meta.getQuote())) {
        options.putIfAbsent("quote", variables.resolve(meta.getQuote()));
      }
      if (meta.isMultiLine()) {
        options.putIfAbsent("multiLine", "true");
      }
      // Leading spaces in numeric fields (" 1", " 13520") are common in Hop samples
      options.putIfAbsent("ignoreLeadingWhiteSpace", "true");
      options.putIfAbsent("ignoreTrailingWhiteSpace", "true");
      // Prefer permissive mode so a bad date/int does not fail the whole job
      options.putIfAbsent("mode", "PERMISSIVE");
    }

    for (Map.Entry<String, String> e : options.entrySet()) {
      reader = reader.option(e.getKey(), e.getValue());
    }

    boolean hasFields = meta.getFields() != null && !meta.getFields().isEmpty();
    boolean nameBasedProjection = delimited && hasFields && !meta.isInferSchema();

    // For delimited + explicit fields: a reader StructType is bound by POSITION, while the Hop
    // field list is by name (subset, any order, case-insensitive). So the schema we push is built
    // from the file's own header: every file column in file order, typed where a Hop numeric
    // field matches, string otherwise. Same by-name projection/cast below either way; numeric
    // columns are then parsed once by the CSV converter instead of tokenized, wrapped and cast,
    // and no header-inference job is needed. Any problem reading the header → schema-less load.
    if (nameBasedProjection && meta.isHeader() && !meta.isMultiLine()) {
      StructType typed =
          typedSchemaFromHeader(log, spark, path, options, meta.getFields(), transformMeta);
      if (typed != null) {
        reader = reader.schema(typed);
      }
    }
    // Infer schema only when requested and no field list.
    if (delimited && meta.isInferSchema() && !hasFields) {
      reader = reader.option("inferSchema", "true");
    }

    Dataset<Row> dataset;
    try {
      dataset = reader.load(path);
    } catch (Exception e) {
      throw new HopException(
          SparkPathDialect.withPathHint(
              "Error reading '"
                  + path
                  + "' as "
                  + format
                  + " in transform '"
                  + transformMeta.getName()
                  + "'",
              path),
          e);
    }

    if (nameBasedProjection) {
      // header=false → Spark names columns _c0,_c1,... — map by position to the field list
      if (!meta.isHeader() && isPositionalSparkCsvColumns(dataset.columns())) {
        dataset = projectAndCastByPosition(log, transformMeta.getName(), dataset, meta.getFields());
      } else {
        dataset = projectAndCastByName(log, transformMeta.getName(), dataset, meta.getFields());
      }
    } else if (hasFields && !delimited) {
      // Parquet/ORC/JSON: select by name + cast when a field list is provided
      dataset = projectAndCastByName(log, transformMeta.getName(), dataset, meta.getFields());
    }

    dataset = trackMetrics(dataset, transformMeta, SparkNativeMetrics.Role.INPUT);
    transformDatasetMap.put(transformMeta.getName(), dataset);
    log.logBasic(
        "Handled Spark File Input : "
            + transformMeta.getName()
            + " format="
            + format
            + " path="
            + path
            + " columns="
            + Arrays.toString(dataset.columns()));
  }

  /** True when Spark assigned default positional CSV names (_c0, _c1, …). */
  static boolean isPositionalSparkCsvColumns(String[] columns) {
    if (columns == null || columns.length == 0) {
      return false;
    }
    for (int i = 0; i < columns.length; i++) {
      if (!("_c" + i).equals(columns[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Map Spark's default {@code _cN} columns to the configured field list by position (no-header
   * CSV). Extra file columns are dropped; missing positions become null.
   */
  static Dataset<Row> projectAndCastByPosition(
      ILogChannel log, String transformName, Dataset<Row> source, List<SparkField> fields)
      throws HopException {
    String[] columns = source.columns();
    List<Column> projected = new ArrayList<>();
    int i = 0;
    for (SparkField field : fields) {
      if (StringUtils.isEmpty(field.getName())) {
        continue;
      }
      if (i < columns.length) {
        projected.add(castColumn(source, columns[i], field).alias(field.getName()));
      } else {
        if (log != null) {
          log.logError(
              "Spark File Input '"
                  + transformName
                  + "': no file column at position "
                  + i
                  + " for field '"
                  + field.getName()
                  + "' — filled with nulls");
        }
        projected.add(lit(null).cast(DataTypes.StringType).alias(field.getName()));
      }
      i++;
    }
    if (projected.isEmpty()) {
      throw new HopException(
          "Spark File Input '" + transformName + "' field list produced no output columns");
    }
    if (log != null) {
      log.logBasic(
          "Spark File Input '"
              + transformName
              + "': mapped "
              + Math.min(i, columns.length)
              + " positional column(s) (_cN) to field list (header=false)");
    }
    return source.select(projected.toArray(new Column[0]));
  }

  /**
   * Select and cast columns by name in the order of {@code fields}. Extra file columns are dropped;
   * missing names become null columns with a warning.
   */
  public static Dataset<Row> projectAndCastByName(
      ILogChannel log, String transformName, Dataset<Row> source, List<SparkField> fields)
      throws HopException {

    Set<String> available =
        Arrays.stream(source.columns()).collect(Collectors.toCollection(HashSet::new));
    // Case-insensitive lookup map for header names
    Map<String, String> availableByLower = new java.util.LinkedHashMap<>();
    for (String c : source.columns()) {
      availableByLower.put(c.toLowerCase(Locale.ROOT), c);
    }

    List<String> missing = new ArrayList<>();
    List<String> unused = new ArrayList<>(available);
    List<Column> projected = new ArrayList<>();

    for (SparkField field : fields) {
      if (StringUtils.isEmpty(field.getName())) {
        continue;
      }
      String want = field.getName();
      String actual =
          available.contains(want) ? want : availableByLower.get(want.toLowerCase(Locale.ROOT));
      if (actual == null) {
        missing.add(want);
        projected.add(lit(null).cast(DataTypes.StringType).alias(want));
        continue;
      }
      unused.remove(actual);
      projected.add(castColumn(source, actual, field).alias(want));
    }

    if (!missing.isEmpty() && log != null) {
      log.logError(
          "Spark File Input '"
              + transformName
              + "': field(s) not found in file header "
              + Arrays.toString(source.columns())
              + ": "
              + missing
              + " — filled with nulls");
    }
    if (!unused.isEmpty() && log != null) {
      log.logBasic(
          "Spark File Input '"
              + transformName
              + "': file column(s) not in field list (dropped): "
              + unused);
    }
    if (projected.isEmpty()) {
      throw new HopException(
          "Spark File Input '" + transformName + "' field list produced no output columns");
    }

    return source.select(projected.toArray(new Column[0]));
  }

  private static Column castColumn(Dataset<Row> source, String columnName, SparkField field)
      throws HopException {
    Column c = col(columnName);
    int typeId = hopTypeId(field);

    // Already typed by the reader schema: no string round trip
    DataType readerType = readerType(source, columnName);
    if (readerType != null && readerType.equals(typedReaderType(typeId))) {
      return c.alias(field.getName());
    }

    // Always trim strings before numeric/date conversion
    Column trimmed = trim(c.cast(DataTypes.StringType));

    return switch (typeId) {
      case IValueMeta.TYPE_STRING, IValueMeta.TYPE_INET -> trimmed.alias(field.getName());
      case IValueMeta.TYPE_INTEGER -> trimmed.cast(DataTypes.LongType);
      case IValueMeta.TYPE_NUMBER -> trimmed.cast(DataTypes.DoubleType);
      case IValueMeta.TYPE_BIGNUMBER -> trimmed.cast(DataTypes.createDecimalType(38, 18));
      case IValueMeta.TYPE_BOOLEAN -> trimmed.cast(DataTypes.BooleanType);
      case IValueMeta.TYPE_DATE -> parseDate(trimmed, field.getFormatMask(), false);
      case IValueMeta.TYPE_TIMESTAMP -> parseDate(trimmed, field.getFormatMask(), true);
      case IValueMeta.TYPE_BINARY -> c.cast(DataTypes.BinaryType);
      default -> trimmed;
    };
  }

  private static int hopTypeId(SparkField field) throws HopException {
    String hopType = StringUtils.defaultIfBlank(field.getHopType(), "String");
    try {
      return org.apache.hop.core.row.value.ValueMetaFactory.getIdForValueMeta(hopType);
    } catch (Exception e) {
      throw new HopException(
          "Unknown Hop type '" + hopType + "' for field '" + field.getName() + "'", e);
    }
  }

  private static DataType readerType(Dataset<Row> source, String columnName) {
    for (StructField f : source.schema().fields()) {
      if (f.name().equals(columnName)) {
        return f.dataType();
      }
    }
    return null;
  }

  /**
   * Spark type the CSV converter may parse directly for a Hop type; null keeps the column as string
   * for the lenient cast path (dates need the format mask, booleans accept Y/N, decimals need
   * precision).
   */
  static DataType typedReaderType(int hopTypeId) {
    return switch (hopTypeId) {
      case IValueMeta.TYPE_INTEGER -> DataTypes.LongType;
      case IValueMeta.TYPE_NUMBER -> DataTypes.DoubleType;
      default -> null;
    };
  }

  /**
   * Build the reader schema from the first line of the (first) file: file columns in file order,
   * typed where a Hop numeric field matches the header name. Returns null when the header cannot be
   * read or is ambiguous, in which case the caller keeps the schema-less load.
   */
  static StructType typedSchemaFromHeader(
      ILogChannel log,
      SparkSession spark,
      String path,
      Map<String, String> options,
      List<SparkField> fields,
      TransformMeta transformMeta) {
    try {
      org.apache.hadoop.conf.Configuration conf = spark.sessionState().newHadoopConf();
      org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(path);
      org.apache.hadoop.fs.FileSystem fs = p.getFileSystem(conf);
      org.apache.hadoop.fs.Path file = firstDataFile(fs, p);
      if (file == null) {
        return null;
      }
      String headerLine = readFirstLine(fs, conf, file, options.getOrDefault("encoding", "UTF-8"));
      if (StringUtils.isBlank(headerLine)) {
        return null;
      }
      String[] names = parseHeader(headerLine, options);
      if (names == null || names.length == 0) {
        return null;
      }
      Map<String, Integer> typeByLower = new java.util.HashMap<>();
      for (SparkField field : fields) {
        if (StringUtils.isNotEmpty(field.getName())) {
          typeByLower.put(field.getName().toLowerCase(Locale.ROOT), hopTypeId(field));
        }
      }
      Set<String> seen = new HashSet<>();
      StructField[] structFields = new StructField[names.length];
      for (int i = 0; i < names.length; i++) {
        String name = names[i] == null ? "" : names[i];
        if (name.isEmpty() || !seen.add(name)) {
          // Spark would rename empty/duplicate headers; keep the proven path instead
          return null;
        }
        Integer typeId = typeByLower.get(name.toLowerCase(Locale.ROOT));
        DataType type = typeId == null ? null : typedReaderType(typeId);
        structFields[i] =
            new StructField(
                name, type == null ? DataTypes.StringType : type, true, Metadata.empty());
      }
      return new StructType(structFields);
    } catch (Exception e) {
      if (log != null) {
        log.logDetailed(
            "Typed CSV schema not applied for '"
                + transformMeta.getName()
                + "' (falling back to schema-less load): "
                + e.getMessage());
      }
      return null;
    }
  }

  private static org.apache.hadoop.fs.Path firstDataFile(
      org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path p) throws java.io.IOException {
    org.apache.hadoop.fs.FileStatus[] matches = fs.globStatus(p);
    if (matches == null || matches.length == 0) {
      return null;
    }
    List<org.apache.hadoop.fs.FileStatus> candidates = new ArrayList<>();
    for (org.apache.hadoop.fs.FileStatus status : matches) {
      if (status.isDirectory()) {
        for (org.apache.hadoop.fs.FileStatus child : fs.listStatus(status.getPath())) {
          if (child.isFile() && !isHidden(child.getPath())) {
            candidates.add(child);
          }
        }
      } else if (status.isFile() && !isHidden(status.getPath())) {
        candidates.add(status);
      }
    }
    if (candidates.isEmpty()) {
      return null;
    }
    // Same choice Spark's header inference makes: the first file in path order
    candidates.sort(java.util.Comparator.comparing(f -> f.getPath().toString()));
    return candidates.get(0).getPath();
  }

  private static boolean isHidden(org.apache.hadoop.fs.Path path) {
    String name = path.getName();
    return name.startsWith("_") || name.startsWith(".");
  }

  private static String readFirstLine(
      org.apache.hadoop.fs.FileSystem fs,
      org.apache.hadoop.conf.Configuration conf,
      org.apache.hadoop.fs.Path file,
      String encoding)
      throws java.io.IOException {
    org.apache.hadoop.io.compress.CompressionCodec codec =
        new org.apache.hadoop.io.compress.CompressionCodecFactory(conf).getCodec(file);
    java.io.InputStream in = fs.open(file);
    if (codec != null) {
      in = codec.createInputStream(in);
    }
    try (java.io.BufferedReader reader =
        new java.io.BufferedReader(new java.io.InputStreamReader(in, encoding))) {
      String line = reader.readLine();
      if (line != null && !line.isEmpty() && line.charAt(0) == '\uFEFF') {
        line = line.substring(1);
      }
      return line;
    }
  }

  /** Split the header with the same delimiter/quote/escape the Spark CSV reader will use. */
  private static String[] parseHeader(String line, Map<String, String> options) {
    com.univocity.parsers.csv.CsvParserSettings settings =
        new com.univocity.parsers.csv.CsvParserSettings();
    com.univocity.parsers.csv.CsvFormat format = settings.getFormat();
    format.setDelimiter(options.getOrDefault("sep", options.getOrDefault("delimiter", ",")));
    String quote = options.getOrDefault("quote", "\"");
    if (!quote.isEmpty()) {
      format.setQuote(quote.charAt(0));
    }
    String escape = options.getOrDefault("escape", "\\");
    if (!escape.isEmpty()) {
      format.setQuoteEscape(escape.charAt(0));
    }
    settings.setIgnoreLeadingWhitespaces(
        Boolean.parseBoolean(options.getOrDefault("ignoreLeadingWhiteSpace", "true")));
    settings.setIgnoreTrailingWhitespaces(
        Boolean.parseBoolean(options.getOrDefault("ignoreTrailingWhiteSpace", "true")));
    settings.setMaxColumns(20000);
    return new com.univocity.parsers.csv.CsvParser(settings).parseLine(line);
  }

  /**
   * Parse date/timestamp strings. Uses the field format mask when set; otherwise tries common Hop
   * masks (including {@code yyyy/MM/dd}).
   */
  private static Column parseDate(Column stringCol, String formatMask, boolean timestamp) {
    if (StringUtils.isNotEmpty(formatMask)) {
      return timestamp
          ? to_timestamp(stringCol, formatMask)
          : to_date(stringCol, formatMask).cast(DataTypes.TimestampType);
    }
    String[] formats = timestamp ? DEFAULT_TIMESTAMP_FORMATS : DEFAULT_DATE_FORMATS;
    // coalesce(to_timestamp(c, f1), to_timestamp(c, f2), ...)
    Column parsed = null;
    for (String f : formats) {
      Column attempt =
          timestamp
              ? to_timestamp(stringCol, f)
              : to_date(stringCol, f).cast(DataTypes.TimestampType);
      parsed = parsed == null ? attempt : org.apache.spark.sql.functions.coalesce(parsed, attempt);
    }
    return parsed;
  }
}
