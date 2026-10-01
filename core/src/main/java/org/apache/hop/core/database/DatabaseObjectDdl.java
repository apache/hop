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

package org.apache.hop.core.database;

import java.util.Locale;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;

/**
 * Helpers for CREATE TABLE / CREATE VIEW text shown in the Database perspective object-information
 * tab.
 */
public final class DatabaseObjectDdl {

  private DatabaseObjectDdl() {}

  /**
   * Pick the catalog definition from a query row. Prefers columns named like {@code CREATE}, {@code
   * VIEW_DEFINITION}, {@code sql} or {@code TEXT}; otherwise the last non-empty string.
   */
  public static String extractDefinition(RowMetaAndData row) throws HopValueException {
    if (row == null || row.getRowMeta() == null || row.getData() == null) {
      return null;
    }
    IRowMeta meta = row.getRowMeta();
    int fallback = -1;
    for (int i = 0; i < meta.size(); i++) {
      String value = row.getString(i, null);
      if (Utils.isEmpty(value)) {
        continue;
      }
      fallback = i;
      String column = Const.NVL(meta.getValueMeta(i).getName(), "").toUpperCase(Locale.ROOT);
      if (column.contains("CREATE")
          || column.contains("DEFINITION")
          || column.contains("DDL")
          || "SQL".equals(column)
          || "TEXT".equals(column)) {
        return value;
      }
    }
    return fallback >= 0 ? row.getString(fallback, null) : null;
  }

  /**
   * If {@code definition} is already a CREATE statement, return it (with a trailing semicolon).
   * Otherwise wrap it as {@code CREATE VIEW qualified AS ...}.
   */
  public static String asCreateViewStatement(String qualifiedName, String definition) {
    if (Utils.isEmpty(definition)) {
      return "";
    }
    String trimmed = definition.trim();
    if (startsWithCreate(trimmed)) {
      return ensureSemicolon(trimmed);
    }
    String name = Utils.isEmpty(qualifiedName) ? "view" : qualifiedName;
    return "CREATE VIEW " + name + " AS" + Const.CR + trimmed + ";";
  }

  public static boolean startsWithCreate(String sql) {
    if (Utils.isEmpty(sql)) {
      return false;
    }
    return sql.trim().toUpperCase(Locale.ROOT).startsWith("CREATE");
  }

  public static String ensureSemicolon(String sql) {
    if (Utils.isEmpty(sql)) {
      return sql;
    }
    String trimmed = sql.trim();
    if (trimmed.endsWith(";")) {
      return trimmed;
    }
    return trimmed + ";";
  }

  /**
   * Last-resort view DDL when the catalog has no SELECT text: column list only.
   *
   * @param comment optional leading SQL comment (without {@code --})
   */
  public static String synthesizeCreateView(String qualifiedName, IRowMeta fields, String comment) {
    StringBuilder buffer = new StringBuilder();
    if (!Utils.isEmpty(comment)) {
      buffer.append("-- ").append(comment).append(Const.CR);
    }
    buffer.append("CREATE VIEW ").append(qualifiedName).append(" AS").append(Const.CR);
    if (fields == null || fields.isEmpty()) {
      buffer.append("SELECT *");
    } else {
      buffer.append("SELECT").append(Const.CR);
      for (int i = 0; i < fields.size(); i++) {
        IValueMeta value = fields.getValueMeta(i);
        if (i > 0) {
          buffer.append(",").append(Const.CR);
        }
        buffer.append("  ").append(value.getName());
      }
    }
    buffer.append(';');
    return buffer.toString();
  }
}
