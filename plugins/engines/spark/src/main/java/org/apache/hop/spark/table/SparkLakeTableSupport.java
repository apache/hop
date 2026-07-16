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

package org.apache.hop.spark.table;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.spark.pipeline.handler.SparkFileInputHandler;
import org.apache.hop.spark.pipeline.handler.SparkFileIoSupport;
import org.apache.hop.spark.transforms.table.SparkLakeTableInputMeta;
import org.apache.hop.spark.transforms.table.SparkLakeTableOutputMeta;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Format × mode decision table for lake table I/O.
 *
 * <ul>
 *   <li>Delta PATH: {@code format("delta").load/save(path)} (requires DeltaCatalog on session)
 *   <li>Iceberg PATH: path identifier under built-in Hadoop catalog {@code hop_iceberg.`uri`} —
 *       bare {@code format("iceberg").load(path)} defaults to HiveCatalog and is not used
 * </ul>
 */
public final class SparkLakeTableSupport {

  private SparkLakeTableSupport() {}

  public static String normalizeFormat(String format) throws HopException {
    if (StringUtils.isEmpty(format)) {
      return SparkLakeFormats.FORMAT_DELTA;
    }
    String f = format.trim().toLowerCase(Locale.ROOT);
    if (SparkLakeFormats.FORMAT_DELTA.equals(f) || SparkLakeFormats.FORMAT_ICEBERG.equals(f)) {
      return f;
    }
    throw new HopException(
        "Unsupported lake table format '"
            + format
            + "'. Supported: "
            + SparkLakeFormats.FORMAT_DELTA
            + ", "
            + SparkLakeFormats.FORMAT_ICEBERG
            + ".");
  }

  public static String normalizeIdentifierMode(String mode) throws HopException {
    if (StringUtils.isEmpty(mode)) {
      return SparkLakeTableInputMeta.MODE_PATH;
    }
    String m = mode.trim().toUpperCase(Locale.ROOT);
    if (SparkLakeTableInputMeta.MODE_PATH.equals(m)
        || SparkLakeTableInputMeta.MODE_TABLE.equals(m)) {
      return m;
    }
    throw new HopException(
        "Unsupported identifier mode '"
            + mode
            + "'. Supported: "
            + SparkLakeTableInputMeta.MODE_PATH
            + ", "
            + SparkLakeTableInputMeta.MODE_TABLE
            + ".");
  }

  /**
   * Normalize a filesystem path to a URI string suitable for Iceberg path identifiers ({@code
   * file:///…}, {@code s3a://…}, etc.).
   */
  public static String toTableLocationUri(String path) {
    if (StringUtils.isEmpty(path)) {
      return path;
    }
    String p = path.trim();
    if (p.contains("://") || p.startsWith("file:")) {
      return p;
    }
    Path absolute = Paths.get(p).toAbsolutePath().normalize();
    return absolute.toUri().toString();
  }

  /**
   * Spark SQL multi-part identifier for an Iceberg path-based table under the hop path catalog:
   * {@code hop_iceberg.`file:///path/to/table`}.
   */
  public static String icebergPathSqlIdentifier(String path) {
    String uri = toTableLocationUri(path);
    // Escape any backticks in the URI (unlikely) by doubling them for Spark SQL quoting
    String escaped = uri.replace("`", "``");
    return SparkLakeFormats.ICEBERG_PATH_CATALOG_NAME + ".`" + escaped + "`";
  }

  public static String normalizeTimeTravelType(String type) throws HopException {
    if (StringUtils.isEmpty(type)) {
      return SparkLakeTableInputMeta.TIME_TRAVEL_NONE;
    }
    String t = type.trim().toUpperCase(Locale.ROOT);
    if (SparkLakeTableInputMeta.TIME_TRAVEL_NONE.equals(t)
        || SparkLakeTableInputMeta.TIME_TRAVEL_VERSION.equals(t)
        || SparkLakeTableInputMeta.TIME_TRAVEL_TIMESTAMP.equals(t)) {
      return t;
    }
    throw new HopException(
        "Unsupported time travel type '"
            + type
            + "'. Supported: "
            + SparkLakeTableInputMeta.TIME_TRAVEL_NONE
            + ", "
            + SparkLakeTableInputMeta.TIME_TRAVEL_VERSION
            + ", "
            + SparkLakeTableInputMeta.TIME_TRAVEL_TIMESTAMP
            + ".");
  }

  /**
   * Maps UI time-travel settings to format-specific option keys (for unit tests and Delta reader).
   *
   * <ul>
   *   <li>Delta VERSION → {@code versionAsOf}
   *   <li>Delta TIMESTAMP → {@code timestampAsOf}
   *   <li>Iceberg VERSION → {@code snapshot-id}
   *   <li>Iceberg TIMESTAMP → {@code as-of-timestamp} (not applied via format reader for PATH; SQL
   *       clause is used instead)
   * </ul>
   */
  public static Map<String, String> timeTravelOptionMap(
      String format, String timeTravelType, String version, String timestamp) throws HopException {
    Map<String, String> map = new java.util.LinkedHashMap<>();
    String fmt = normalizeFormat(format);
    String tt = normalizeTimeTravelType(timeTravelType);
    if (SparkLakeTableInputMeta.TIME_TRAVEL_NONE.equals(tt)) {
      return map;
    }
    if (SparkLakeTableInputMeta.TIME_TRAVEL_VERSION.equals(tt)) {
      if (StringUtils.isEmpty(version)) {
        throw new HopException("Time travel type VERSION requires a version / snapshot id value");
      }
      if (SparkLakeFormats.FORMAT_DELTA.equals(fmt)) {
        map.put("versionAsOf", version.trim());
      } else {
        map.put("snapshot-id", version.trim());
      }
      return map;
    }
    // TIMESTAMP
    if (StringUtils.isEmpty(timestamp)) {
      throw new HopException("Time travel type TIMESTAMP requires a timestamp value");
    }
    if (SparkLakeFormats.FORMAT_DELTA.equals(fmt)) {
      map.put("timestampAsOf", timestamp.trim());
    } else {
      map.put("as-of-timestamp", timestamp.trim());
    }
    return map;
  }

  /**
   * Resolve MERGE target SQL identifier for Delta/Iceberg PATH or TABLE mode.
   *
   * <ul>
   *   <li>Delta PATH → {@code delta.`path`}
   *   <li>Iceberg PATH → {@code hop_iceberg.`uri`}
   *   <li>TABLE → resolved multi-part table id
   * </ul>
   */
  public static String resolveMergeTargetSqlId(
      SparkSession spark,
      IVariables variables,
      org.apache.hop.spark.transforms.table.SparkLakeTableMergeMeta meta,
      String transformName)
      throws HopException {
    return resolveLakeTargetSqlId(
        spark,
        variables,
        meta.getFormat(),
        meta.getIdentifierMode(),
        meta.getTablePath(),
        meta.getTableIdentifier(),
        transformName,
        "Merge");
  }

  /**
   * Resolve maintenance target: SQL id for Delta/DELETE, plus Iceberg CALL catalog and table ref.
   */
  public static MaintenanceTarget resolveMaintenanceTarget(
      SparkSession spark,
      IVariables variables,
      org.apache.hop.spark.transforms.table.SparkLakeTableMaintenanceMeta meta,
      String transformName)
      throws HopException {
    String format = normalizeFormat(meta.getFormat());
    String mode = normalizeIdentifierMode(meta.getIdentifierMode());
    String sqlId =
        resolveLakeTargetSqlId(
            spark,
            variables,
            format,
            mode,
            meta.getTablePath(),
            meta.getTableIdentifier(),
            transformName,
            "Maintenance");

    String procedureCatalog = null;
    String tableRefForCall = null;
    if (SparkLakeFormats.FORMAT_ICEBERG.equals(format)) {
      if (SparkLakeTableInputMeta.MODE_PATH.equals(mode)) {
        procedureCatalog = SparkLakeFormats.ICEBERG_PATH_CATALOG_NAME;
        tableRefForCall = toTableLocationUri(variables.resolve(meta.getTablePath()));
      } else {
        String tableId = resolveTableIdentifier(meta.getTableIdentifier(), null, variables);
        String[] parts = tableId.split("\\.");
        if (parts.length >= 3) {
          procedureCatalog = parts[0];
          StringBuilder rest = new StringBuilder(parts[1]);
          for (int i = 2; i < parts.length; i++) {
            rest.append('.').append(parts[i]);
          }
          tableRefForCall = rest.toString();
        } else if (parts.length == 2) {
          procedureCatalog = SparkLakeFormats.ICEBERG_PATH_CATALOG_NAME;
          tableRefForCall = tableId;
        } else {
          throw new HopException(
              "Iceberg TABLE maintenance needs catalog.ns.table identifier, got: " + tableId);
        }
      }
    }
    return new MaintenanceTarget(sqlId, procedureCatalog, tableRefForCall, format);
  }

  public static String resolveLakeTargetSqlId(
      SparkSession spark,
      IVariables variables,
      String formatRaw,
      String modeRaw,
      String tablePath,
      String tableIdentifier,
      String transformName,
      String role)
      throws HopException {
    String format = normalizeFormat(formatRaw);
    String mode = normalizeIdentifierMode(modeRaw);

    if (SparkLakeTableInputMeta.MODE_TABLE.equals(mode)) {
      return resolveTableIdentifier(tableIdentifier, null, variables);
    }

    String path = variables.resolve(tablePath);
    if (StringUtils.isEmpty(path)) {
      throw new HopException(
          "Spark Lake Table "
              + role
              + " '"
              + transformName
              + "' has no table path configured for PATH mode");
    }

    if (SparkLakeFormats.FORMAT_DELTA.equals(format)) {
      String loc = path.replace("`", "``");
      return "delta.`" + loc + "`";
    }

    if (spark != null) {
      ensureIcebergPathCatalog(spark);
    }
    return icebergPathSqlIdentifier(path);
  }

  /** Maintenance target resolution result. */
  public record MaintenanceTarget(
      String targetSqlId, String procedureCatalog, String tableRefForCall, String format) {}

  /**
   * Resolve TABLE mode identifier (e.g. {@code lake.db.orders}). Optionally prefixes Spark catalog
   * name from metadata when the identifier has only two parts.
   */
  public static String resolveTableIdentifier(
      String tableIdentifier, String sparkCatalogName, IVariables variables) throws HopException {
    String id = variables != null ? variables.resolve(tableIdentifier) : tableIdentifier;
    if (StringUtils.isEmpty(id)) {
      throw new HopException("TABLE mode requires a table identifier (e.g. lake.db.orders)");
    }
    id = id.trim();
    String cat =
        variables != null && StringUtils.isNotEmpty(sparkCatalogName)
            ? variables.resolve(sparkCatalogName)
            : sparkCatalogName;
    if (StringUtils.isNotEmpty(cat)) {
      cat = cat.trim();
      // If id is ns.table (2 parts), prefix catalog name
      long dots = id.chars().filter(ch -> ch == '.').count();
      if (dots == 1 && !id.startsWith(cat + ".")) {
        id = cat + "." + id;
      }
    }
    return id;
  }

  /** Read a lake table as a Dataset (PATH + TABLE), with optional time travel. */
  public static Dataset<Row> resolveRead(
      SparkSession spark,
      IVariables variables,
      ILogChannel log,
      String transformName,
      SparkLakeTableInputMeta meta)
      throws HopException {

    String format = normalizeFormat(meta.getFormat());
    String mode = normalizeIdentifierMode(meta.getIdentifierMode());
    Map<String, String> options =
        SparkFileIoSupport.parseExtraOptions(variables, meta.getExtraOptions());

    String ttType = normalizeTimeTravelType(meta.getTimeTravelType());
    String ttVersion =
        StringUtils.isNotEmpty(meta.getTimeTravelVersion())
            ? variables.resolve(meta.getTimeTravelVersion())
            : null;
    String ttTimestamp =
        StringUtils.isNotEmpty(meta.getTimeTravelTimestamp())
            ? variables.resolve(meta.getTimeTravelTimestamp())
            : null;

    Map<String, String> ttOptions = timeTravelOptionMap(format, ttType, ttVersion, ttTimestamp);
    options.putAll(ttOptions);

    Dataset<Row> dataset;
    if (SparkLakeTableInputMeta.MODE_TABLE.equals(mode)) {
      String tableId = resolveTableIdentifier(meta.getTableIdentifier(), null, variables);
      // Prefer catalogMetadataName only for session plan; identifier is full Spark id
      dataset =
          readTable(spark, format, tableId, options, ttType, ttVersion, ttTimestamp, transformName);
    } else {
      String path = variables.resolve(meta.getTablePath());
      if (StringUtils.isEmpty(path)) {
        throw new HopException(
            "Spark Lake Table Input '" + transformName + "' has no table path configured");
      }
      if (SparkLakeFormats.FORMAT_DELTA.equals(format)) {
        dataset = readDeltaPath(spark, path, options, transformName);
      } else {
        dataset = readIcebergPath(spark, path, ttType, ttVersion, ttTimestamp, transformName);
      }
    }

    if (meta.getFields() != null && !meta.getFields().isEmpty()) {
      dataset =
          SparkFileInputHandler.projectAndCastByName(log, transformName, dataset, meta.getFields());
    }
    return dataset;
  }

  /** Write a Dataset to a lake table (PATH + TABLE). Action runs immediately. */
  public static void resolveWrite(
      SparkSession spark,
      Dataset<Row> dataset,
      IVariables variables,
      String transformName,
      SparkLakeTableOutputMeta meta)
      throws HopException {

    String format = normalizeFormat(meta.getFormat());
    String mode = normalizeIdentifierMode(meta.getIdentifierMode());
    Map<String, String> options =
        SparkFileIoSupport.parseExtraOptions(variables, meta.getExtraOptions());

    SaveMode saveMode = SparkFileIoSupport.toSaveMode(meta.getSaveMode());
    if (StringUtils.isEmpty(meta.getSaveMode())) {
      saveMode = SaveMode.ErrorIfExists;
    }

    String[] partitionColumns = parsePartitionColumns(meta.getPartitionByColumns());

    Integer coalesce = null;
    if (StringUtils.isNotEmpty(meta.getCoalescePartitions())) {
      int c = Const.toInt(variables.resolve(meta.getCoalescePartitions()), -1);
      if (c > 0) {
        coalesce = c;
      }
    }

    Dataset<Row> toWrite = dataset;
    if (coalesce != null) {
      toWrite = toWrite.coalesce(coalesce);
    }

    if (SparkLakeTableInputMeta.MODE_TABLE.equals(mode)) {
      String tableId = resolveTableIdentifier(meta.getTableIdentifier(), null, variables);
      writeTable(
          spark, toWrite, format, tableId, saveMode, options, partitionColumns, transformName);
      return;
    }

    String path = variables.resolve(meta.getTablePath());
    if (StringUtils.isEmpty(path)) {
      throw new HopException(
          "Spark Lake Table Output '" + transformName + "' has no table path configured");
    }

    if (SparkLakeFormats.FORMAT_DELTA.equals(format)) {
      SparkFileIoSupport.writeDataset(
          toWrite, SparkLakeFormats.FORMAT_DELTA, path, saveMode, options, partitionColumns, null);
    } else {
      writeIcebergPath(spark, toWrite, path, saveMode, partitionColumns, transformName);
    }
  }

  private static Dataset<Row> readTable(
      SparkSession spark,
      String format,
      String tableId,
      Map<String, String> options,
      String ttType,
      String ttVersion,
      String ttTimestamp,
      String transformName)
      throws HopException {
    try {
      if (SparkLakeFormats.FORMAT_ICEBERG.equals(format)) {
        String sql = buildIcebergTimeTravelSql(tableId, ttType, ttVersion, ttTimestamp);
        return spark.sql(sql);
      }
      // Delta TABLE — requires DeltaCatalog on spark_catalog
      DataFrameReader reader = spark.read().format(SparkLakeFormats.FORMAT_DELTA);
      for (Map.Entry<String, String> e : options.entrySet()) {
        reader = reader.option(e.getKey(), e.getValue());
      }
      return reader.table(tableId);
    } catch (Exception e) {
      throw new HopException(
          "Error reading "
              + format
              + " table '"
              + tableId
              + "' in transform '"
              + transformName
              + "'. For Iceberg ensure the Spark catalog is registered (SparkCatalog metadata /"
              + " hop_iceberg). For Delta ensure spark_catalog=DeltaCatalog.",
          e);
    }
  }

  private static void writeTable(
      SparkSession spark,
      Dataset<Row> dataset,
      String format,
      String tableId,
      SaveMode saveMode,
      Map<String, String> options,
      String[] partitionColumns,
      String transformName)
      throws HopException {
    try {
      if (SparkLakeFormats.FORMAT_ICEBERG.equals(format)) {
        // Prefer writeTo (KD-24 spike choice for Iceberg TABLE)
        switch (saveMode) {
          case Overwrite ->
              applyPartitioning(
                      dataset.writeTo(tableId).using(SparkLakeFormats.FORMAT_ICEBERG),
                      partitionColumns)
                  .createOrReplace();
          case Append -> {
            try {
              applyPartitioning(dataset.writeTo(tableId), partitionColumns).append();
            } catch (Exception appendEx) {
              applyPartitioning(
                      dataset.writeTo(tableId).using(SparkLakeFormats.FORMAT_ICEBERG),
                      partitionColumns)
                  .create();
            }
          }
          case ErrorIfExists ->
              applyPartitioning(
                      dataset.writeTo(tableId).using(SparkLakeFormats.FORMAT_ICEBERG),
                      partitionColumns)
                  .create();
          case Ignore -> {
            try {
              applyPartitioning(
                      dataset.writeTo(tableId).using(SparkLakeFormats.FORMAT_ICEBERG),
                      partitionColumns)
                  .create();
            } catch (Exception ignoreEx) {
              if (!isTableAlreadyExists(ignoreEx)) {
                throw ignoreEx;
              }
            }
          }
          default ->
              throw new HopException("Unsupported save mode " + saveMode + " for Iceberg TABLE");
        }
        return;
      }

      // Delta TABLE (advanced): saveAsTable / insertInto via format writer
      Dataset<Row> toWrite = dataset;
      if (partitionColumns != null && partitionColumns.length > 0) {
        // saveAsTable uses partitionBy on DataFrameWriter
      }
      var writer = toWrite.write().format(SparkLakeFormats.FORMAT_DELTA).mode(saveMode);
      for (Map.Entry<String, String> e : options.entrySet()) {
        writer = writer.option(e.getKey(), e.getValue());
      }
      if (partitionColumns != null && partitionColumns.length > 0) {
        writer = writer.partitionBy(partitionColumns);
      }
      if (saveMode == SaveMode.Append) {
        try {
          writer.insertInto(tableId);
        } catch (Exception insertEx) {
          writer.saveAsTable(tableId);
        }
      } else {
        writer.saveAsTable(tableId);
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          "Error writing "
              + format
              + " table '"
              + tableId
              + "' in transform '"
              + transformName
              + "'. Iceberg uses writeTo; Delta uses saveAsTable (requires DeltaCatalog).",
          e);
    }
  }

  private static Dataset<Row> readDeltaPath(
      SparkSession spark, String path, Map<String, String> options, String transformName)
      throws HopException {
    DataFrameReader reader = spark.read().format(SparkLakeFormats.FORMAT_DELTA);
    for (Map.Entry<String, String> e : options.entrySet()) {
      reader = reader.option(e.getKey(), e.getValue());
    }
    try {
      return reader.load(path);
    } catch (Exception e) {
      throw new HopException(
          "Error reading Delta table at '"
              + path
              + "' in transform '"
              + transformName
              + "'. Ensure the Delta connector is on the engine classpath and the session has"
              + " DeltaSparkSessionExtension + DeltaCatalog (see plugins/engines/spark/README.md).",
          e);
    }
  }

  private static Dataset<Row> readIcebergPath(
      SparkSession spark,
      String path,
      String timeTravelType,
      String version,
      String timestamp,
      String transformName)
      throws HopException {
    ensureIcebergPathCatalog(spark);
    String sqlId = icebergPathSqlIdentifier(path);
    String sql = buildIcebergTimeTravelSql(sqlId, timeTravelType, version, timestamp);
    try {
      return spark.sql(sql);
    } catch (Exception e) {
      throw new HopException(
          "Error reading Iceberg table at '"
              + path
              + "' (sql="
              + sql
              + ") in transform '"
              + transformName
              + "'. Ensure the Iceberg runtime is on the engine classpath and session has"
              + " IcebergSparkSessionExtensions + Hadoop catalog '"
              + SparkLakeFormats.ICEBERG_PATH_CATALOG_NAME
              + "' (see plugins/engines/spark/README.md).",
          e);
    }
  }

  /**
   * Builds {@code SELECT * FROM id [VERSION|TIMESTAMP AS OF …]} for Iceberg path identifiers.
   * Package-visible for unit tests.
   */
  static String buildIcebergTimeTravelSql(
      String sqlIdentifier, String timeTravelType, String version, String timestamp)
      throws HopException {
    String tt = normalizeTimeTravelType(timeTravelType);
    if (SparkLakeTableInputMeta.TIME_TRAVEL_NONE.equals(tt)) {
      return "SELECT * FROM " + sqlIdentifier;
    }
    if (SparkLakeTableInputMeta.TIME_TRAVEL_VERSION.equals(tt)) {
      if (StringUtils.isEmpty(version)) {
        throw new HopException("Iceberg VERSION time travel requires a snapshot id");
      }
      // Snapshot ids are numeric; pass as bare token (no quotes) so Spark parses as long
      String snap = version.trim();
      if (!snap.matches("-?\\d+")) {
        // Allow quoted non-numeric only if user provided quotes; otherwise quote string
        return "SELECT * FROM " + sqlIdentifier + " VERSION AS OF " + snap;
      }
      return "SELECT * FROM " + sqlIdentifier + " VERSION AS OF " + snap;
    }
    if (StringUtils.isEmpty(timestamp)) {
      throw new HopException("Iceberg TIMESTAMP time travel requires a timestamp value");
    }
    String ts = timestamp.trim().replace("'", "''");
    return "SELECT * FROM " + sqlIdentifier + " TIMESTAMP AS OF TIMESTAMP '" + ts + "'";
  }

  private static void writeIcebergPath(
      SparkSession spark,
      Dataset<Row> dataset,
      String path,
      SaveMode saveMode,
      String[] partitionColumns,
      String transformName)
      throws HopException {
    ensureIcebergPathCatalog(spark);
    String sqlId = icebergPathSqlIdentifier(path);

    try {
      switch (saveMode) {
        case Overwrite ->
            applyPartitioning(
                    dataset.writeTo(sqlId).using(SparkLakeFormats.FORMAT_ICEBERG), partitionColumns)
                .createOrReplace();
        case Append -> {
          try {
            applyPartitioning(dataset.writeTo(sqlId), partitionColumns).append();
          } catch (Exception appendEx) {
            // Table may not exist yet — create
            try {
              applyPartitioning(
                      dataset.writeTo(sqlId).using(SparkLakeFormats.FORMAT_ICEBERG),
                      partitionColumns)
                  .create();
            } catch (Exception createEx) {
              throw new HopException(
                  "Iceberg append failed for '"
                      + path
                      + "' and create fallback also failed in transform '"
                      + transformName
                      + "'",
                  appendEx);
            }
          }
        }
        case ErrorIfExists ->
            applyPartitioning(
                    dataset.writeTo(sqlId).using(SparkLakeFormats.FORMAT_ICEBERG), partitionColumns)
                .create();
        case Ignore -> {
          try {
            applyPartitioning(
                    dataset.writeTo(sqlId).using(SparkLakeFormats.FORMAT_ICEBERG), partitionColumns)
                .create();
          } catch (Exception ignoreEx) {
            if (!isTableAlreadyExists(ignoreEx)) {
              throw ignoreEx;
            }
          }
        }
        default ->
            throw new HopException(
                "Unsupported save mode "
                    + saveMode
                    + " for Iceberg PATH in transform '"
                    + transformName
                    + "'");
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          "Error writing Iceberg table at '"
              + path
              + "' (identifier "
              + sqlId
              + ") in transform '"
              + transformName
              + "'. Ensure Iceberg is on the classpath and hop_iceberg Hadoop catalog is"
              + " configured (see plugins/engines/spark/README.md).",
          e);
    }
  }

  private static org.apache.spark.sql.DataFrameWriterV2<Row> applyPartitioning(
      org.apache.spark.sql.DataFrameWriterV2<Row> writer, String[] partitionColumns) {
    if (partitionColumns == null || partitionColumns.length == 0) {
      return writer;
    }
    org.apache.spark.sql.Column first = org.apache.spark.sql.functions.col(partitionColumns[0]);
    if (partitionColumns.length == 1) {
      return writer.partitionedBy(first);
    }
    org.apache.spark.sql.Column[] rest =
        new org.apache.spark.sql.Column[partitionColumns.length - 1];
    for (int i = 1; i < partitionColumns.length; i++) {
      rest[i - 1] = org.apache.spark.sql.functions.col(partitionColumns[i]);
    }
    return writer.partitionedBy(first, rest);
  }

  /**
   * Ensure the built-in Hadoop catalog for path identifiers is registered on this session. Safe to
   * call multiple times; no-ops when already present.
   */
  public static void ensureIcebergPathCatalog(SparkSession spark) {
    String existing = spark.conf().get(SparkLakeFormats.SPARK_CONF_ICEBERG_PATH_CATALOG, "");
    if (StringUtils.isNotEmpty(existing)) {
      return;
    }
    String warehouse =
        Paths.get(System.getProperty("java.io.tmpdir"), "hop-iceberg-path-catalog-warehouse")
            .toAbsolutePath()
            .normalize()
            .toUri()
            .toString();
    spark
        .conf()
        .set(SparkLakeFormats.SPARK_CONF_ICEBERG_PATH_CATALOG, SparkLakeFormats.ICEBERG_CATALOG);
    spark.conf().set(SparkLakeFormats.SPARK_CONF_ICEBERG_PATH_CATALOG_TYPE, "hadoop");
    spark.conf().set(SparkLakeFormats.SPARK_CONF_ICEBERG_PATH_CATALOG_WAREHOUSE, warehouse);
  }

  private static String[] parsePartitionColumns(String partitionByColumns) {
    if (StringUtils.isEmpty(partitionByColumns)) {
      return null;
    }
    List<String> parts = new ArrayList<>();
    for (String p : partitionByColumns.split(",")) {
      if (StringUtils.isNotEmpty(p.trim())) {
        parts.add(p.trim());
      }
    }
    return parts.isEmpty() ? null : parts.toArray(new String[0]);
  }

  private static boolean isTableAlreadyExists(Throwable e) {
    Throwable t = e;
    while (t != null) {
      String name = t.getClass().getSimpleName();
      String msg = t.getMessage() != null ? t.getMessage() : "";
      if (name.contains("TableAlreadyExists")
          || msg.contains("TABLE_OR_VIEW_ALREADY_EXISTS")
          || msg.toLowerCase(Locale.ROOT).contains("already exists")) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }

  /** Formats referenced by a lake input/output meta (for session planning). */
  public static void collectFormat(String format, java.util.Set<String> into) throws HopException {
    if (StringUtils.isEmpty(format)) {
      into.add(SparkLakeFormats.FORMAT_DELTA);
      return;
    }
    into.add(normalizeFormat(format));
  }
}
