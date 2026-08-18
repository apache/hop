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
package org.apache.hop.core.database.types;

import java.util.List;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;

/**
 * Resolves a column type through the dialect's rules, falling back to plain JDBC behaviour.
 *
 * <p>This is the entry point that will replace the {@code isXVariant()} switches once callers are
 * migrated. It is not wired into the engine yet.
 */
public final class DatabaseTypeMapper {

  private DatabaseTypeMapper() {
    // Utility class.
  }

  /**
   * Maps a column to Hop value metadata: dialect rules first, then the standard JDBC mapping.
   *
   * @return the value metadata, or null when nothing claimed the column, so that the caller can
   *     still consult the value meta plugins as it does today
   */
  public static IValueMeta getValueMeta(
      IVariables variables,
      DatabaseMeta databaseMeta,
      DatabaseColumn column,
      boolean ignoreLength,
      boolean lazyConversion)
      throws HopDatabaseException {
    for (IDatabaseTypeRule rule : rulesFor(databaseMeta)) {
      IValueMeta valueMeta = rule.getValueMeta(variables, databaseMeta, column);
      if (valueMeta != null) {
        return valueMeta;
      }
    }
    return StandardJdbcTypeMapper.getValueMeta(
        variables, databaseMeta, column, ignoreLength, lazyConversion);
  }

  /**
   * Renders the column type for a value, without the column name.
   *
   * @return the DDL type, or null when no rule claims it, so that the caller can fall back to the
   *     dialect's own getFieldDefinition
   */
  public static String getColumnType(
      IVariables variables,
      DatabaseMeta databaseMeta,
      IValueMeta valueMeta,
      ColumnContext context) {
    if (databaseMeta == null) {
      return null;
    }
    return getColumnType(variables, databaseMeta.getIDatabase(), valueMeta, context);
  }

  /**
   * The column type for this value, asked of the dialect directly.
   *
   * <p>A dialect assembling an ALTER TABLE statement has itself but no {@link DatabaseMeta} wrapper
   * to hand, so this is the form the write path is actually reached through.
   *
   * @return the DDL type, or null when no rule claims the value and the caller should fall back
   */
  public static String getColumnType(
      IVariables variables, IDatabase database, IValueMeta valueMeta, ColumnContext context) {
    if (database == null || valueMeta == null) {
      return null;
    }
    for (IDatabaseTypeRule rule : DatabaseTypeRuleRegistry.getTypeRules(database)) {
      String columnType = rule.getColumnType(variables, database, valueMeta, context);
      if (columnType != null) {
        return columnType;
      }
    }
    return null;
  }

  /**
   * How values of this type move across JDBC on this database.
   *
   * <p>Called for every value of every row, so it is kept to a cached lookup that usually finds
   * nothing.
   *
   * @return the binding, or null to use the default JDBC handling for the Hop type
   */
  public static IValueBinding getBinding(IDatabase database, IValueMeta valueMeta) {
    if (database == null || valueMeta == null) {
      return null;
    }
    for (IDatabaseTypeRule rule : DatabaseTypeRuleRegistry.getBindingRules(database)) {
      IValueBinding binding = rule.getBinding(database, valueMeta);
      if (binding != null) {
        return binding;
      }
    }
    return null;
  }

  private static List<IDatabaseTypeRule> rulesFor(DatabaseMeta databaseMeta) {
    if (databaseMeta == null || databaseMeta.getIDatabase() == null) {
      return List.of();
    }
    return DatabaseTypeRuleRegistry.getTypeRules(databaseMeta.getIDatabase());
  }
}
