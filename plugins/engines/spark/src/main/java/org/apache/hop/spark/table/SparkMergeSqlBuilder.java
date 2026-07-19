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
 * Builds Spark SQL {@code MERGE INTO} statements for lakehouse upserts. Does not execute SQL.
 *
 * <p>Target and source identifiers are inserted as provided (already resolved/quoted by the
 * caller). The merge condition is operator-authored SQL and is not escaped beyond basic emptiness
 * checks.
 */
public final class SparkMergeSqlBuilder {

  public static final String MATCHED_UPDATE_ALL = "UPDATE_ALL";
  public static final String MATCHED_DELETE = "DELETE";
  public static final String MATCHED_NONE = "NONE";

  public static final String NOT_MATCHED_INSERT_ALL = "INSERT_ALL";
  public static final String NOT_MATCHED_NONE = "NONE";

  public static final String NOT_MATCHED_BY_SOURCE_DELETE = "DELETE";
  public static final String NOT_MATCHED_BY_SOURCE_NONE = "NONE";

  private SparkMergeSqlBuilder() {}

  /**
   * @param targetSqlId target table SQL identifier (e.g. {@code delta.`/path`} or {@code
   *     lake.db.t})
   * @param sourceViewName temp view name for source rows (unquoted identifier)
   * @param mergeCondition ON clause body (e.g. {@code t.id = s.id})
   * @param matchedAction {@link #MATCHED_UPDATE_ALL}, {@link #MATCHED_DELETE}, or {@link
   *     #MATCHED_NONE}
   * @param notMatchedAction {@link #NOT_MATCHED_INSERT_ALL} or {@link #NOT_MATCHED_NONE}
   * @param notMatchedBySourceAction {@link #NOT_MATCHED_BY_SOURCE_DELETE} or {@link
   *     #NOT_MATCHED_BY_SOURCE_NONE} (Delta/Iceberg support varies)
   */
  public static String build(
      String targetSqlId,
      String sourceViewName,
      String mergeCondition,
      String matchedAction,
      String notMatchedAction,
      String notMatchedBySourceAction)
      throws HopException {

    if (StringUtils.isEmpty(targetSqlId)) {
      throw new HopException("MERGE target table identifier is empty");
    }
    if (StringUtils.isEmpty(sourceViewName) || !isSafeSqlIdentifier(sourceViewName)) {
      throw new HopException(
          "MERGE source view name is invalid (use letters, digits, underscore): " + sourceViewName);
    }
    if (StringUtils.isEmpty(mergeCondition)) {
      throw new HopException("MERGE condition (ON clause) is required");
    }

    String matched = normalizeMatched(matchedAction);
    String notMatched = normalizeNotMatched(notMatchedAction);
    String notMatchedBySource = normalizeNotMatchedBySource(notMatchedBySourceAction);

    if (MATCHED_NONE.equals(matched)
        && NOT_MATCHED_NONE.equals(notMatched)
        && NOT_MATCHED_BY_SOURCE_NONE.equals(notMatchedBySource)) {
      throw new HopException(
          "MERGE requires at least one action (matched UPDATE/DELETE and/or not-matched INSERT)");
    }

    StringBuilder sql = new StringBuilder();
    sql.append("MERGE INTO ")
        .append(targetSqlId)
        .append(" AS t\n")
        .append("USING ")
        .append(sourceViewName)
        .append(" AS s\n")
        .append("ON ")
        .append(mergeCondition.trim())
        .append('\n');

    if (MATCHED_UPDATE_ALL.equals(matched)) {
      sql.append("WHEN MATCHED THEN UPDATE SET *\n");
    } else if (MATCHED_DELETE.equals(matched)) {
      sql.append("WHEN MATCHED THEN DELETE\n");
    }

    if (NOT_MATCHED_INSERT_ALL.equals(notMatched)) {
      sql.append("WHEN NOT MATCHED THEN INSERT *\n");
    }

    if (NOT_MATCHED_BY_SOURCE_DELETE.equals(notMatchedBySource)) {
      // Supported on recent Delta; Iceberg support depends on version
      sql.append("WHEN NOT MATCHED BY SOURCE THEN DELETE\n");
    }

    return sql.toString().trim();
  }

  /** Sanitize transform name into a temp-view identifier. */
  public static String sourceViewName(String transformName) {
    String base = StringUtils.defaultIfBlank(transformName, "src").replaceAll("[^A-Za-z0-9_]", "_");
    if (base.isEmpty() || Character.isDigit(base.charAt(0))) {
      base = "s_" + base;
    }
    return "hop_merge_src_" + base;
  }

  public static boolean isSafeSqlIdentifier(String name) {
    return name != null && name.matches("[A-Za-z_][A-Za-z0-9_]*");
  }

  private static String normalizeMatched(String action) throws HopException {
    if (StringUtils.isEmpty(action)) {
      return MATCHED_UPDATE_ALL;
    }
    String a = action.trim().toUpperCase(Locale.ROOT);
    return switch (a) {
      case MATCHED_UPDATE_ALL, "UPDATE", "UPDATESET", "UPDATE_SET" -> MATCHED_UPDATE_ALL;
      case MATCHED_DELETE -> MATCHED_DELETE;
      case MATCHED_NONE, "SKIP" -> MATCHED_NONE;
      default ->
          throw new HopException(
              "Unsupported matched action '" + action + "'. Supported: UPDATE_ALL, DELETE, NONE");
    };
  }

  private static String normalizeNotMatched(String action) throws HopException {
    if (StringUtils.isEmpty(action)) {
      return NOT_MATCHED_INSERT_ALL;
    }
    String a = action.trim().toUpperCase(Locale.ROOT);
    return switch (a) {
      case NOT_MATCHED_INSERT_ALL, "INSERT", "INSERTSET", "INSERT_SET" -> NOT_MATCHED_INSERT_ALL;
      case NOT_MATCHED_NONE, "SKIP" -> NOT_MATCHED_NONE;
      default ->
          throw new HopException(
              "Unsupported not-matched action '" + action + "'. Supported: INSERT_ALL, NONE");
    };
  }

  private static String normalizeNotMatchedBySource(String action) throws HopException {
    if (StringUtils.isEmpty(action)) {
      return NOT_MATCHED_BY_SOURCE_NONE;
    }
    String a = action.trim().toUpperCase(Locale.ROOT);
    return switch (a) {
      case NOT_MATCHED_BY_SOURCE_DELETE -> NOT_MATCHED_BY_SOURCE_DELETE;
      case NOT_MATCHED_BY_SOURCE_NONE, "SKIP" -> NOT_MATCHED_BY_SOURCE_NONE;
      default ->
          throw new HopException(
              "Unsupported not-matched-by-source action '" + action + "'. Supported: DELETE, NONE");
    };
  }
}
