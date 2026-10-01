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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.value.ValueMetaFactory;

/**
 * Collects the type rules that apply to a dialect, from the dialect itself and from any {@link
 * DatabaseTypeRulesPlugin} that targets it.
 *
 * <p>Externally contributed rules come first, so a plugin can both add types a dialect never had
 * and correct one it gets wrong.
 */
public final class DatabaseTypeRuleRegistry {

  /**
   * Contributed rules keyed by the dialect's type chain. Only the plugin lookup is cached: the
   * legacy variant bridge reads flags off the instance, so caching it per class would be wrong for
   * any dialect that decides them at runtime.
   */
  private static final Map<List<String>, List<IDatabaseTypeRule>> contributedCache =
      new ConcurrentHashMap<>();

  private DatabaseTypeRuleRegistry() {
    // Utility class.
  }

  /**
   * The dialect's own plugin type and those of its ancestors, most specific first.
   *
   * <p>This is the replacement for asking {@code isPostgresVariant()}: Redshift reports {@code
   * [REDSHIFT, POSTGRESQL]} because it extends the Postgres dialect, so a rule written for
   * POSTGRESQL applies to it without anyone maintaining a list of Postgres-like databases.
   */
  public static List<String> getDialectTypes(IDatabase database) {
    List<String> types = new ArrayList<>();
    for (Class<?> clazz = database.getClass(); clazz != null; clazz = clazz.getSuperclass()) {
      // Not @Inherited, so this only ever returns the annotation declared on that exact class.
      DatabaseMetaPlugin annotation = clazz.getAnnotation(DatabaseMetaPlugin.class);
      if (annotation != null && !types.contains(annotation.type())) {
        types.add(annotation.type());
      }
    }
    return types;
  }

  /** All rules that apply to this dialect, in the order they should be consulted. */
  public static List<IDatabaseTypeRule> getTypeRules(IDatabase database) {
    List<String> dialectTypes = getDialectTypes(database);
    List<IDatabaseTypeRule> contributed =
        contributedCache.computeIfAbsent(dialectTypes, types -> contributedRules(database));

    List<IDatabaseTypeRule> own = database.getTypeRules();
    List<IDatabaseTypeRule> legacy = LegacyVariantBridge.forDialect(database, dialectTypes);
    if (own.isEmpty() && legacy.isEmpty()) {
      return contributed;
    }

    List<IDatabaseTypeRule> rules =
        new ArrayList<>(contributed.size() + own.size() + legacy.size());
    rules.addAll(contributed);
    rules.addAll(own);
    // Last, so that a dialect which has migrated overrides the flags it may still answer.
    rules.addAll(legacy);
    return rules;
  }

  private static List<IDatabaseTypeRule> contributedRules(IDatabase database) {
    List<String> dialectTypes = getDialectTypes(database);
    List<IDatabaseTypeRule> rules = new ArrayList<>();

    List<IPlugin> plugins;
    try {
      plugins = PluginRegistry.getInstance().getPlugins(DatabaseTypeRulesPluginType.class);
    } catch (Exception e) {
      // No plugin registry in this context, for example a unit test using dialects directly.
      return rules;
    }

    for (IPlugin plugin : plugins) {
      try {
        Object loaded = PluginRegistry.getInstance().loadClass(plugin);
        if (!(loaded instanceof IDatabaseTypeRuleProvider provider)) {
          continue;
        }
        DatabaseTypeRulesPlugin annotation =
            loaded.getClass().getAnnotation(DatabaseTypeRulesPlugin.class);
        if (annotation == null || !appliesTo(annotation, dialectTypes)) {
          continue;
        }
        if (!valueTypesAvailable(annotation)) {
          // A rule set for a value type nobody installed is skipped, not an error.
          continue;
        }
        rules.addAll(provider.getTypeRules());
      } catch (Exception e) {
        // One broken contributor must not take down every database connection in the process.
        LogChannel.GENERAL.logError(
            "Unable to load database type rules from plugin " + plugin.getIds()[0], e);
      }
    }
    return rules;
  }

  private static boolean appliesTo(DatabaseTypeRulesPlugin annotation, List<String> dialectTypes) {
    if (annotation.dialects().length == 0) {
      return true;
    }
    return Arrays.stream(annotation.dialects()).anyMatch(dialectTypes::contains);
  }

  private static boolean valueTypesAvailable(DatabaseTypeRulesPlugin annotation) {
    if (annotation.valueTypes().length == 0) {
      return true;
    }
    List<String> available = Arrays.asList(ValueMetaFactory.getValueMetaNames());
    return available.containsAll(Arrays.asList(annotation.valueTypes()));
  }

  /**
   * The rules that can supply a value binding, cached per dialect class.
   *
   * <p>This is read for every value of every row, so it must not rebuild the rule list each time.
   * Bindings describe how a driver behaves, which is a property of the dialect rather than of one
   * connection, so caching them per class is safe where caching the full rule list would not be:
   * Oracle, for one, chooses different type rules per connection.
   */
  private static final Map<Class<?>, List<IDatabaseTypeRule>> bindingRuleCache =
      new ConcurrentHashMap<>();

  /** The rules that can supply a value binding for this dialect. Usually empty. */
  public static List<IDatabaseTypeRule> getBindingRules(IDatabase database) {
    return bindingRuleCache.computeIfAbsent(
        database.getClass(),
        clazz -> {
          List<IDatabaseTypeRule> candidates = new ArrayList<>();
          for (IDatabaseTypeRule rule : getTypeRules(database)) {
            if (rule.suppliesBindings()) {
              candidates.add(rule);
            }
          }
          return List.copyOf(candidates);
        });
  }

  /** Drops the cached rules. Plugin registration changes between tests need this. */
  public static void clearCache() {
    contributedCache.clear();
    bindingRuleCache.clear();
    LegacyVariantBridge.clearCache();
  }
}
