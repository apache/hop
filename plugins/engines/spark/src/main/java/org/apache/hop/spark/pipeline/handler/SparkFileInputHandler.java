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
import org.apache.hop.spark.transforms.io.SparkField;
import org.apache.hop.spark.transforms.io.SparkFileInputMeta;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

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

    String path = variables.resolve(meta.getFilePath());
    if (StringUtils.isEmpty(path)) {
      throw new HopException(
          "Spark File Input '" + transformMeta.getName() + "' has no file path configured");
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

    // For delimited + explicit fields: do NOT push StructType into the reader (positional).
    // Infer schema only when requested and no field list.
    if (delimited && meta.isInferSchema() && !hasFields) {
      reader = reader.option("inferSchema", "true");
    }

    Dataset<Row> dataset;
    try {
      dataset = reader.load(path);
    } catch (Exception e) {
      throw new HopException(
          "Error reading '"
              + path
              + "' as "
              + format
              + " in transform '"
              + transformMeta.getName()
              + "'",
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
    // Always trim strings before numeric/date conversion
    Column trimmed = trim(c.cast(DataTypes.StringType));

    String hopType = StringUtils.defaultIfBlank(field.getHopType(), "String");
    int typeId;
    try {
      typeId = org.apache.hop.core.row.value.ValueMetaFactory.getIdForValueMeta(hopType);
    } catch (Exception e) {
      throw new HopException(
          "Unknown Hop type '" + hopType + "' for field '" + field.getName() + "'", e);
    }

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
