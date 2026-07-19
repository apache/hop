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

import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;

/**
 * Builds Spark SQL for lakehouse maintenance (OPTIMIZE / VACUUM / expire / rewrite / DELETE). Does
 * not execute SQL.
 */
public final class SparkMaintenanceSqlBuilder {

  public static final String OP_OPTIMIZE = "OPTIMIZE";
  public static final String OP_VACUUM = "VACUUM";
  public static final String OP_EXPIRE_SNAPSHOTS = "EXPIRE_SNAPSHOTS";
  public static final String OP_REWRITE_MANIFESTS = "REWRITE_MANIFESTS";
  public static final String OP_DELETE_WHERE = "DELETE_WHERE";

  private SparkMaintenanceSqlBuilder() {}

  public static String normalizeOperation(String operation) throws HopException {
    if (StringUtils.isEmpty(operation)) {
      throw new HopException("Maintenance operation is required");
    }
    String op = operation.trim().toUpperCase(Locale.ROOT);
    return switch (op) {
      case OP_OPTIMIZE, "COMPACT", "REWRITE_DATA_FILES" -> OP_OPTIMIZE;
      case OP_VACUUM -> OP_VACUUM;
      case OP_EXPIRE_SNAPSHOTS, "EXPIRE" -> OP_EXPIRE_SNAPSHOTS;
      case OP_REWRITE_MANIFESTS -> OP_REWRITE_MANIFESTS;
      case OP_DELETE_WHERE, "DELETE" -> OP_DELETE_WHERE;
      default ->
          throw new HopException(
              "Unsupported maintenance operation '"
                  + operation
                  + "'. Supported: OPTIMIZE, VACUUM, EXPIRE_SNAPSHOTS, REWRITE_MANIFESTS,"
                  + " DELETE_WHERE");
    };
  }

  /**
   * @param format delta or iceberg
   * @param targetSqlId SQL table id (e.g. {@code delta.`/p`} or {@code lake.db.t})
   * @param procedureCatalog catalog name for Iceberg CALL (e.g. {@code hop_iceberg} or {@code
   *     lake}); required for Iceberg CALL ops
   * @param tableRefForCall table reference string for CALL table => '…' (path URI or ns.table)
   * @param operation normalized operation
   * @param retentionHours required for VACUUM / EXPIRE_SNAPSHOTS
   * @param whereClause optional WHERE for OPTIMIZE / DELETE
   * @param zOrderColumns optional comma-separated ZORDER columns (Delta OPTIMIZE only)
   * @param retainLast optional retain_last for Iceberg expire (default 1 if blank)
   */
  public static String build(
      String format,
      String targetSqlId,
      String procedureCatalog,
      String tableRefForCall,
      String operation,
      String retentionHours,
      String whereClause,
      String zOrderColumns,
      String retainLast)
      throws HopException {

    String fmt = SparkLakeTableSupport.normalizeFormat(format);
    String op = normalizeOperation(operation);

    if (StringUtils.isEmpty(targetSqlId) && !isIcebergCall(op, fmt)) {
      throw new HopException("Maintenance target table identifier is empty");
    }

    return switch (op) {
      case OP_OPTIMIZE ->
          buildOptimize(
              fmt, targetSqlId, procedureCatalog, tableRefForCall, whereClause, zOrderColumns);
      case OP_VACUUM -> buildVacuum(fmt, targetSqlId, retentionHours);
      case OP_EXPIRE_SNAPSHOTS ->
          buildExpireSnapshots(fmt, procedureCatalog, tableRefForCall, retentionHours, retainLast);
      case OP_REWRITE_MANIFESTS -> buildRewriteManifests(fmt, procedureCatalog, tableRefForCall);
      case OP_DELETE_WHERE -> buildDeleteWhere(targetSqlId, whereClause);
      default -> throw new HopException("Unhandled operation " + op);
    };
  }

  public static boolean requiresDestructiveAck(String operation) throws HopException {
    String op = normalizeOperation(operation);
    return OP_VACUUM.equals(op) || OP_EXPIRE_SNAPSHOTS.equals(op) || OP_DELETE_WHERE.equals(op);
  }

  private static boolean isIcebergCall(String op, String format) {
    return SparkLakeFormats.FORMAT_ICEBERG.equals(format)
        && (OP_OPTIMIZE.equals(op)
            || OP_EXPIRE_SNAPSHOTS.equals(op)
            || OP_REWRITE_MANIFESTS.equals(op));
  }

  private static String buildOptimize(
      String format,
      String targetSqlId,
      String procedureCatalog,
      String tableRefForCall,
      String whereClause,
      String zOrderColumns)
      throws HopException {
    if (SparkLakeFormats.FORMAT_DELTA.equals(format)) {
      StringBuilder sql = new StringBuilder("OPTIMIZE ").append(targetSqlId);
      if (StringUtils.isNotEmpty(whereClause)) {
        sql.append(" WHERE ").append(whereClause.trim());
      }
      if (StringUtils.isNotEmpty(zOrderColumns)) {
        sql.append(" ZORDER BY (").append(zOrderColumns.trim()).append(')');
      }
      return sql.toString();
    }
    // Iceberg compact = rewrite_data_files
    requireCallArgs(procedureCatalog, tableRefForCall, "OPTIMIZE/rewrite_data_files");
    return "CALL "
        + procedureCatalog
        + ".system.rewrite_data_files(table => '"
        + escapeSqlLiteral(tableRefForCall)
        + "')";
  }

  private static String buildVacuum(String format, String targetSqlId, String retentionHours)
      throws HopException {
    if (!SparkLakeFormats.FORMAT_DELTA.equals(format)) {
      throw new HopException(
          "VACUUM is Delta-only. For Iceberg use EXPIRE_SNAPSHOTS (and optionally remove orphan"
              + " files via advanced SQL).");
    }
    double hours = parsePositiveRetentionHours(retentionHours, "VACUUM");
    return "VACUUM " + targetSqlId + " RETAIN " + formatHours(hours) + " HOURS";
  }

  private static String buildExpireSnapshots(
      String format,
      String procedureCatalog,
      String tableRefForCall,
      String retentionHours,
      String retainLast)
      throws HopException {
    if (!SparkLakeFormats.FORMAT_ICEBERG.equals(format)) {
      throw new HopException("EXPIRE_SNAPSHOTS is Iceberg-only. For Delta use VACUUM.");
    }
    requireCallArgs(procedureCatalog, tableRefForCall, "EXPIRE_SNAPSHOTS");
    // Prefer retain_last for simplicity; if retentionHours set, use older_than interval via
    // timestamp expression is complex — require retain_last or retentionHours mapped to retain.
    int retain = 1;
    if (StringUtils.isNotEmpty(retainLast)) {
      try {
        retain = Integer.parseInt(retainLast.trim());
      } catch (NumberFormatException e) {
        throw new HopException("retainLast must be an integer >= 1", e);
      }
    }
    if (retain < 1) {
      throw new HopException("retainLast must be >= 1 for EXPIRE_SNAPSHOTS");
    }
    // Always require explicit retention policy: either retainLast was set or retentionHours
    if (StringUtils.isEmpty(retainLast) && StringUtils.isEmpty(retentionHours)) {
      throw new HopException(
          "EXPIRE_SNAPSHOTS requires retainLast (>=1) and/or retentionHours (documented; retainLast"
              + " is applied to CALL retain_last)");
    }
    if (StringUtils.isNotEmpty(retentionHours)) {
      // Validate positive; Iceberg CALL uses retain_last primarily in v1
      parsePositiveRetentionHours(retentionHours, "EXPIRE_SNAPSHOTS");
    }
    return "CALL "
        + procedureCatalog
        + ".system.expire_snapshots(table => '"
        + escapeSqlLiteral(tableRefForCall)
        + "', retain_last => "
        + retain
        + ")";
  }

  private static String buildRewriteManifests(
      String format, String procedureCatalog, String tableRefForCall) throws HopException {
    if (!SparkLakeFormats.FORMAT_ICEBERG.equals(format)) {
      throw new HopException("REWRITE_MANIFESTS is Iceberg-only");
    }
    requireCallArgs(procedureCatalog, tableRefForCall, "REWRITE_MANIFESTS");
    return "CALL "
        + procedureCatalog
        + ".system.rewrite_manifests(table => '"
        + escapeSqlLiteral(tableRefForCall)
        + "')";
  }

  private static String buildDeleteWhere(String targetSqlId, String whereClause)
      throws HopException {
    if (StringUtils.isEmpty(targetSqlId)) {
      throw new HopException("DELETE_WHERE requires a target table");
    }
    if (StringUtils.isEmpty(whereClause)) {
      throw new HopException("DELETE_WHERE requires a WHERE clause (refusing unrestricted DELETE)");
    }
    return "DELETE FROM " + targetSqlId + " WHERE " + whereClause.trim();
  }

  private static void requireCallArgs(String catalog, String tableRef, String op)
      throws HopException {
    if (StringUtils.isEmpty(catalog) || !catalog.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      throw new HopException(
          op + " requires a Spark procedure catalog name (e.g. hop_iceberg or lake)");
    }
    if (StringUtils.isEmpty(tableRef)) {
      throw new HopException(op + " requires a table reference for CALL table => '…'");
    }
  }

  private static double parsePositiveRetentionHours(String retentionHours, String op)
      throws HopException {
    if (StringUtils.isEmpty(retentionHours)) {
      throw new HopException(
          op + " requires retentionHours (no silent default — refuse destructive ops without it)");
    }
    try {
      double h = Double.parseDouble(retentionHours.trim());
      if (h <= 0) {
        throw new HopException(op + " retentionHours must be > 0");
      }
      return h;
    } catch (NumberFormatException e) {
      throw new HopException(op + " retentionHours must be a number: " + retentionHours, e);
    }
  }

  private static String formatHours(double hours) {
    if (hours == Math.rint(hours)) {
      return Long.toString((long) hours);
    }
    return Double.toString(hours);
  }

  static String escapeSqlLiteral(String value) {
    return value.replace("'", "''");
  }
}
