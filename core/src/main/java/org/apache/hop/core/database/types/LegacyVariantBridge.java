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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;

/**
 * Keeps the deprecated {@code isXVariant()} flags working now that the rules they used to select
 * are declared by the dialects.
 *
 * <p>A flag such as {@code isMySqlVariant()} means "treat me like MySQL", and some dialects say so
 * without extending {@code MySqlDatabaseMeta} — Hive and SingleStore in Hop, and any number of
 * plugins Hop does not ship. Those dialects declare no rules of their own, so this hands them the
 * shared rules the flag was asking for.
 *
 * <p>It resolves to {@link ColumnTypeRules} directly rather than looking up the dialect the flag
 * names. Going through the plugin registry would make compatibility depend on that plugin being
 * installed, and fail silently when it is not.
 *
 * <p>Core is left holding only the mapping from each deprecated flag to the rules it implies, which
 * is all the flag ever meant. When the flags go, this goes with them.
 */
public final class LegacyVariantBridge {

  /** A deprecated flag whose rules are shared, because more than one dialect claims it. */
  private record SharedFlag(Predicate<IDatabase> flag, List<IDatabaseTypeRule> rules) {}

  /**
   * isMySqlVariant() is the only flag with claimants other than the dialect it names: Generic, Hive
   * and SingleStore all say they are MySQL-like without extending MySqlDatabaseMeta. Its rules are
   * therefore shared rather than private to the MySQL plugin.
   */
  private static final List<SharedFlag> SHARED_FLAGS =
      List.of(new SharedFlag(IDatabase::isMySqlVariant, ColumnTypeRules.MYSQL_COMPATIBLE));

  /** A deprecated flag whose rules belong to the one dialect that declares them. */
  private record DialectFlag(String dialectType, Predicate<IDatabase> flag) {}

  /**
   * These flags are answered only by the dialect they name, and by the dialects that extend it and
   * so inherit its rules outright. Nothing in Hop needs them bridged. They are still bridged for
   * the sake of a dialect from outside Hop that claims one, which is what the flag was for.
   */
  private static final List<DialectFlag> DIALECT_FLAGS =
      List.of(
          new DialectFlag("POSTGRESQL", IDatabase::isPostgresVariant),
          new DialectFlag("ORACLE", IDatabase::isOracleVariant),
          new DialectFlag("SQLITE", IDatabase::isSqliteVariant),
          new DialectFlag("TERADATA", IDatabase::isTeradataVariant));

  private static final Map<String, List<IDatabaseTypeRule>> dialectRuleCache =
      new ConcurrentHashMap<>();

  private LegacyVariantBridge() {
    // Utility class.
  }

  /**
   * The rules a dialect inherits purely by answering a deprecated flag.
   *
   * @param database the dialect being resolved
   * @param ownDialectTypes its own plugin type and its ancestors', so that a dialect which already
   *     is the one a flag names is not handed its own rules a second time
   */
  public static List<IDatabaseTypeRule> forDialect(
      IDatabase database, List<String> ownDialectTypes) {
    List<IDatabaseTypeRule> rules = new ArrayList<>(2);
    for (SharedFlag shared : SHARED_FLAGS) {
      if (shared.flag().test(database)) {
        rules.addAll(shared.rules());
      }
    }
    for (DialectFlag variant : DIALECT_FLAGS) {
      if (ownDialectTypes.contains(variant.dialectType()) || !variant.flag().test(database)) {
        continue;
      }
      rules.addAll(rulesOf(variant.dialectType()));
    }
    return rules;
  }

  /**
   * The rules declared by the dialect registered under this plugin id.
   *
   * <p>Reaching for a plugin makes this depend on that plugin being installed, so when it is not we
   * say so rather than quietly doing nothing. Only a dialect from outside Hop can get here: the
   * ones Hop ships either are that dialect or extend it.
   */
  private static List<IDatabaseTypeRule> rulesOf(String dialectType) {
    return dialectRuleCache.computeIfAbsent(
        dialectType,
        type -> {
          try {
            IPlugin plugin = PluginRegistry.getInstance().getPlugin(DatabasePluginType.class, type);
            if (plugin == null) {
              LogChannel.GENERAL.logBasic(
                  "A database connection asks to be treated as "
                      + type
                      + ", but that database plugin is not installed, so its column type rules"
                      + " cannot be applied.");
              return List.of();
            }
            Object dialect = PluginRegistry.getInstance().loadClass(plugin);
            return dialect instanceof IDatabase resolved
                ? List.copyOf(resolved.getTypeRules())
                : List.<IDatabaseTypeRule>of();
          } catch (Exception e) {
            LogChannel.GENERAL.logError(
                "Unable to read the column type rules of the " + type + " dialect", e);
            return List.of();
          }
        });
  }

  /** Drops the cached dialect rules. Plugin registration changes between tests need this. */
  public static void clearCache() {
    dialectRuleCache.clear();
  }
}
