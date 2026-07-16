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

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.spark.metadata.SparkCatalog;
import org.apache.spark.sql.SparkSession;

/**
 * Expands {@link SparkCatalog} metadata into {@code spark.sql.catalog.<name>.*} configuration for
 * hop-run builders and active sessions.
 */
public final class SparkCatalogApplier {

  private SparkCatalogApplier() {}

  /**
   * Build Spark conf key/value pairs for a catalog definition. Does not log credential or confExtra
   * values.
   */
  public static Map<String, String> toSparkConfigs(SparkCatalog catalog, IVariables variables)
      throws HopException {
    if (catalog == null) {
      throw new HopException("SparkCatalog is null");
    }
    String name = resolve(variables, catalog.getCatalogName());
    if (StringUtils.isEmpty(name)) {
      // Fall back to Hop metadata entry name
      name = resolve(variables, catalog.getName());
    }
    if (StringUtils.isEmpty(name)) {
      throw new HopException(
          "SparkCatalog '"
              + catalog.getName()
              + "' has no Spark catalog name (catalogName / name)");
    }
    if (!name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      throw new HopException(
          "Spark catalog name '"
              + name
              + "' is invalid (use letters, digits, underscore; start with letter/underscore)");
    }

    String type =
        StringUtils.defaultIfBlank(catalog.getCatalogType(), SparkCatalog.TYPE_HADOOP)
            .trim()
            .toLowerCase(Locale.ROOT);

    Map<String, String> conf = new LinkedHashMap<>();
    String prefix = "spark.sql.catalog." + name;

    String impl = resolve(variables, catalog.getImplementation());
    if (StringUtils.isEmpty(impl)) {
      impl = SparkLakeFormats.ICEBERG_CATALOG;
    }

    switch (type) {
      case SparkCatalog.TYPE_HADOOP -> {
        conf.put(prefix, impl);
        conf.put(prefix + ".type", "hadoop");
        String warehouse = resolve(variables, catalog.getWarehouse());
        if (StringUtils.isEmpty(warehouse)) {
          throw new HopException(
              "SparkCatalog '" + catalog.getName() + "' (hadoop) requires a warehouse path/URI");
        }
        conf.put(prefix + ".warehouse", toUriIfLocalPath(warehouse));
      }
      case SparkCatalog.TYPE_REST -> {
        conf.put(prefix, impl);
        conf.put(prefix + ".type", "rest");
        String uri = resolve(variables, catalog.getUri());
        if (StringUtils.isEmpty(uri)) {
          throw new HopException(
              "SparkCatalog '" + catalog.getName() + "' (rest) requires a URI endpoint");
        }
        conf.put(prefix + ".uri", uri);
        String warehouse = resolve(variables, catalog.getWarehouse());
        if (StringUtils.isNotEmpty(warehouse)) {
          conf.put(prefix + ".warehouse", toUriIfLocalPath(warehouse));
        }
      }
      case SparkCatalog.TYPE_CUSTOM -> {
        conf.put(prefix, impl);
      }
      case SparkCatalog.TYPE_HIVE, SparkCatalog.TYPE_GLUE -> {
        // Advanced: operator supplies full conf via confExtra / implementation
        if (StringUtils.isNotEmpty(impl)) {
          conf.put(prefix, impl);
        }
        conf.put(prefix + ".type", type);
      }
      default ->
          throw new HopException(
              "Unsupported SparkCatalog type '"
                  + type
                  + "' on '"
                  + catalog.getName()
                  + "'. Supported: hadoop, rest, custom, hive, glue.");
    }

    // Optional credential — only if operator maps it via confExtra typically; expose as token
    // property for REST when set
    String credential = resolve(variables, catalog.getCredential());
    if (StringUtils.isNotEmpty(credential) && SparkCatalog.TYPE_REST.equals(type)) {
      conf.putIfAbsent(prefix + ".token", credential);
    }

    // confExtra: key=value lines → spark.sql.catalog.<name>.key=value
    // (or full spark.* keys if they contain a dot prefix already starting with spark.)
    String extra = catalog.getConfExtra();
    if (StringUtils.isNotEmpty(extra)) {
      for (String line : extra.split("\\r?\\n")) {
        String trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
          continue;
        }
        int eq = trimmed.indexOf('=');
        if (eq <= 0) {
          continue;
        }
        String key = resolve(variables, trimmed.substring(0, eq).trim());
        String value = resolve(variables, trimmed.substring(eq + 1).trim());
        if (StringUtils.isEmpty(key)) {
          continue;
        }
        if (key.startsWith("spark.")) {
          conf.put(key, value);
        } else {
          conf.put(prefix + "." + key, value);
        }
      }
    }

    return conf;
  }

  public static void applyToBuilder(
      SparkSession.Builder builder, SparkCatalog catalog, IVariables variables)
      throws HopException {
    for (Map.Entry<String, String> e : toSparkConfigs(catalog, variables).entrySet()) {
      builder.config(e.getKey(), e.getValue());
    }
  }

  public static void applyToSession(
      SparkSession session, SparkCatalog catalog, IVariables variables) throws HopException {
    for (Map.Entry<String, String> e : toSparkConfigs(catalog, variables).entrySet()) {
      session.conf().set(e.getKey(), e.getValue());
    }
  }

  private static String resolve(IVariables variables, String value) {
    if (value == null) {
      return null;
    }
    return variables != null ? variables.resolve(value) : value;
  }

  static String toUriIfLocalPath(String path) {
    if (StringUtils.isEmpty(path)) {
      return path;
    }
    String p = path.trim();
    if (p.contains("://") || p.startsWith("file:")) {
      return p;
    }
    return java.nio.file.Paths.get(p).toAbsolutePath().normalize().toUri().toString();
  }
}
