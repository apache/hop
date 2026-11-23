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

package org.apache.hop.lineage.datahub;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.LineageRelationalIdentity;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * Which DataHub platform instance the tables of a database server belong to, by the lineage
 * namespace of the server ({@code postgres://db.example.com:5432}).
 *
 * <p>DataHub's SQL sources prefix a table's name with the {@code platform_instance} of the source
 * that read it ({@code warehouse.sales.public.orders}), so two servers with the same database and
 * table names stay two datasets. Hop names a table the same way when its connection has a platform
 * instance here; without one, the name is the table's own.
 */
final class DataHubDatabaseInstances {

  /** As a setting: {@code <namespace>=<instance>} pairs, separated by commas. */
  static final String SEPARATOR = ",";

  private DataHubDatabaseInstances() {}

  /**
   * The namespace of each connection with its instance.
   *
   * @throws HopException when a connection does not exist, has no host to name it by, or two
   *     connections to the same server have different instances
   */
  static Map<String, String> resolve(
      List<DataHubDatabaseInstance> instances,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    Map<String, String> byNamespace = new LinkedHashMap<>();
    if (instances == null) {
      return byNamespace;
    }
    for (DataHubDatabaseInstance row : instances) {
      String connection = row == null ? null : variables.resolve(row.getConnection());
      String instance = row == null ? null : variables.resolve(row.getPlatformInstance());
      if (StringUtils.isBlank(connection) && StringUtils.isBlank(instance)) {
        continue;
      }
      if (StringUtils.isBlank(connection) || StringUtils.isBlank(instance)) {
        throw new HopException(
            "A platform instance needs both a connection and an instance: '"
                + StringUtils.defaultString(connection)
                + "', '"
                + StringUtils.defaultString(instance)
                + "'");
      }
      if (StringUtils.containsAny(instance, SEPARATOR + "=")) {
        throw new HopException(
            "Platform instance '" + instance.trim() + "' contains a ',' or a '='");
      }
      DatabaseMeta databaseMeta =
          metadataProvider == null
              ? null
              : metadataProvider.getSerializer(DatabaseMeta.class).load(connection.trim());
      if (databaseMeta == null) {
        throw new HopException(
            "Relational connection '" + connection + "' of a platform instance does not exist");
      }
      String namespace = LineageRelationalIdentity.namespace(databaseMeta, variables);
      if (StringUtils.isBlank(namespace)) {
        throw new HopException(
            "Relational connection '"
                + connection
                + "' has no host, so its tables can't be given a platform instance");
      }
      String previous = byNamespace.putIfAbsent(namespace, instance.trim());
      if (previous != null && !previous.equals(instance.trim())) {
        throw new HopException(
            "Two connections to "
                + namespace
                + " have different platform instances: '"
                + previous
                + "' and '"
                + instance.trim()
                + "'");
      }
    }
    return byNamespace;
  }

  /** The instances as one setting value. */
  static String format(Map<String, String> byNamespace) {
    StringBuilder value = new StringBuilder();
    byNamespace.forEach(
        (namespace, instance) -> {
          if (!value.isEmpty()) {
            value.append(SEPARATOR);
          }
          value.append(namespace).append('=').append(instance);
        });
    return value.toString();
  }

  /**
   * The instances of a setting value.
   *
   * @throws IllegalArgumentException when a pair has no namespace or no instance
   */
  static Map<String, String> parse(String value) {
    if (StringUtils.isBlank(value)) {
      return Map.of();
    }
    Map<String, String> byNamespace = new LinkedHashMap<>();
    for (String pair : value.split(SEPARATOR)) {
      if (StringUtils.isBlank(pair)) {
        continue;
      }
      int equals = pair.lastIndexOf('=');
      String namespace = equals < 0 ? "" : pair.substring(0, equals).trim();
      String instance = equals < 0 ? "" : pair.substring(equals + 1).trim();
      if (namespace.isEmpty() || instance.isEmpty()) {
        throw new IllegalArgumentException(
            "A database platform instance is '<namespace>=<instance>', such as"
                + " 'postgres://db.example.com:5432=warehouse', not: "
                + pair.trim());
      }
      byNamespace.put(namespace, instance);
    }
    return Collections.unmodifiableMap(byNamespace);
  }
}
