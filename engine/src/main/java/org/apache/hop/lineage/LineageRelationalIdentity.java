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

package org.apache.hop.lineage;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.model.RelationalTable;

/**
 * Single source of truth for the OpenLineage relational dataset identity documented on the
 * "OpenLineage dataset identity" page of the user manual. Lives in the engine (not the sink)
 * because computing the {@code namespace} needs the {@link DatabaseMeta} the plugin never sees, and
 * because a Table Output (known table) and a parsed Table Input {@code SELECT} must produce a
 * byte-identical identity for the same physical table.
 *
 * <p>Namespace (§3.1) is {@code scheme://host:port}; the scheme is mapped from the Hop database
 * type (not the raw JDBC sub-protocol, which differs — Postgres is {@code postgresql} in JDBC but
 * {@code postgres} in the OpenLineage naming spec). Name (§3.2) is {@code
 * {database}.{schema}.{table}} with only the segments the database has. Casing is the caller's
 * responsibility: segments must be supplied exactly as the database stores them so both halves of a
 * stitch agree.
 */
public final class LineageRelationalIdentity {

  /**
   * Hop database type ({@code DatabaseMeta.getPluginId()}) → OpenLineage naming-spec scheme. Types
   * absent here fall back to the lowercased Hop type id, matching the contract's "Generic JDBC"
   * row.
   */
  private static final Map<String, String> SCHEME_BY_TYPE =
      Map.ofEntries(
          Map.entry("POSTGRESQL", "postgres"),
          Map.entry("MYSQL", "mysql"),
          Map.entry("MARIADB", "mysql"),
          Map.entry("REDSHIFT", "redshift"),
          Map.entry("ORACLE", "oracle"),
          Map.entry("MSSQL", "mssql"),
          Map.entry("MSSQLNATIVE", "mssql"),
          Map.entry("DB2", "db2"));

  private LineageRelationalIdentity() {}

  /**
   * OpenLineage dataset namespace for the given connection, or {@code null} when there is no host
   * to key on (e.g. embedded/file databases, which this phase does not identify as relational
   * sources).
   */
  public static String namespace(DatabaseMeta databaseMeta, IVariables variables) {
    if (databaseMeta == null) {
      return null;
    }
    String host = resolve(variables, databaseMeta.getHostname());
    String port = resolve(variables, databaseMeta.getPort());
    return buildNamespace(databaseMeta.getPluginId(), host, port, defaultPortOrZero(databaseMeta));
  }

  /**
   * Pure core of {@link #namespace}: builds {@code scheme://host[:port]} from already-resolved
   * parts. Package-visible so the contract rules can be unit-tested without a live {@link
   * DatabaseMeta}. Returns {@code null} when {@code host} is blank.
   */
  static String buildNamespace(String hopTypeId, String host, String port, int defaultPort) {
    if (StringUtils.isBlank(host)) {
      return null;
    }
    String scheme = schemeFor(hopTypeId);
    String effectivePort = StringUtils.isBlank(port) ? portString(defaultPort) : port.trim();
    StringBuilder ns =
        new StringBuilder(scheme).append("://").append(host.trim().toLowerCase(Locale.ROOT));
    if (!StringUtils.isBlank(effectivePort)) {
      ns.append(':').append(effectivePort);
    }
    return ns.toString();
  }

  /**
   * Fully qualified dataset name {@code {database}.{schema}.{table}} (§3.2): the present segments
   * joined with {@code .}, verbatim (no re-casing, no quoting). Returns {@code null} when the table
   * is blank.
   */
  public static String datasetName(RelationalTable table) {
    if (table == null || StringUtils.isBlank(table.getTable())) {
      return null;
    }
    StringBuilder name = new StringBuilder();
    appendSegment(name, table.getDatabase());
    appendSegment(name, table.getSchema());
    appendSegment(name, table.getTable());
    return name.toString();
  }

  private static void appendSegment(StringBuilder name, String segment) {
    if (StringUtils.isBlank(segment)) {
      return;
    }
    if (name.length() > 0) {
      name.append('.');
    }
    name.append(segment.trim());
  }

  /**
   * Qualifies a table with the connection's catalog when it has none. Tables recovered by parsing a
   * {@code SELECT} carry no catalog segment (SQL cannot name it), so a read of {@code schema.table}
   * would not match a write of {@code catalog.schema.table} for the same table. Filling the blank
   * catalog with the connection's database — which both sides share — makes the two identities
   * agree. Tables that already carry a catalog, and blank {@code defaultCatalog}, are returned
   * unchanged.
   */
  public static RelationalTable withDefaultCatalog(RelationalTable table, String defaultCatalog) {
    return withDefaults(table, defaultCatalog, null);
  }

  /**
   * Fills a table's blank catalog and/or schema from the connection defaults so that tables named
   * at different levels of qualification (a bare {@code table} in a {@code SELECT}, a {@code
   * schema.table} write) resolve to the same {@code catalog.schema.table} identity. Segments that
   * are already present, and blank defaults, are left untouched.
   */
  public static RelationalTable withDefaults(
      RelationalTable table, String defaultCatalog, String defaultSchema) {
    if (table == null) {
      return null;
    }
    String database =
        StringUtils.isBlank(table.getDatabase()) && StringUtils.isNotBlank(defaultCatalog)
            ? defaultCatalog.trim()
            : table.getDatabase();
    String schema =
        StringUtils.isBlank(table.getSchema()) && StringUtils.isNotBlank(defaultSchema)
            ? defaultSchema.trim()
            : table.getSchema();
    if (Objects.equals(database, table.getDatabase())
        && Objects.equals(schema, table.getSchema())) {
      return table;
    }
    return new RelationalTable(database, schema, table.getTable());
  }

  private static String schemeFor(String hopTypeId) {
    if (StringUtils.isBlank(hopTypeId)) {
      return "jdbc";
    }
    String upper = hopTypeId.trim().toUpperCase(Locale.ROOT);
    return SCHEME_BY_TYPE.getOrDefault(upper, upper.toLowerCase(Locale.ROOT));
  }

  private static int defaultPortOrZero(DatabaseMeta databaseMeta) {
    try {
      return databaseMeta.getDefaultDatabasePort();
    } catch (RuntimeException e) {
      // Some IDatabase implementations throw for portless (embedded) databases.
      return 0;
    }
  }

  private static String portString(int port) {
    return port > 0 ? Integer.toString(port) : null;
  }

  private static String resolve(IVariables variables, String raw) {
    if (raw == null) {
      return null;
    }
    return variables == null ? raw : variables.resolve(raw);
  }
}
