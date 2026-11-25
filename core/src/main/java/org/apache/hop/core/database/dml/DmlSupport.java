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

package org.apache.hop.core.database.dml;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;

/**
 * Picks the upsert form for a connection. Transforms ask this instead of reading the dialect's
 * {@link DmlSyntax} themselves.
 *
 * <p>An upsert needs a declared {@link UpsertForm}: the one asked for, or else the first declared
 * one that qualifies. A form qualifies when the server is at least its minimum version, and, for
 * {@link UpsertForm#ON_CONFLICT}, when the table has a primary key or unique index on exactly the
 * key columns; {@link UpsertForm#ON_DUPLICATE_KEY} also needs that to be the table's only unique
 * index, because it reacts to any of them. Both can only update columns they insert.
 *
 * <p>Without a connection (a dialog, a pipeline check) only the dialect is checked; a driver that
 * can't tell its version gets the benefit of the doubt.
 */
public final class DmlSupport {
  private static final Class<?> PKG = DmlSupport.class;

  private DmlSupport() {}

  /** What the unique indexes of a table say about a set of key columns. */
  public enum UniqueKey {
    /** The primary key or a unique index is on exactly the key columns, and no other exists. */
    ONLY_THE_KEY,
    /** One is on exactly the key columns, and others exist on other columns. */
    THE_KEY_AND_OTHERS,
    /** None is on exactly the key columns. */
    NONE
  }

  /**
   * Checks the dialect only, without a connection: for dialogs and pipeline checks.
   *
   * @param databaseMeta the connection
   * @param form the upsert form asked for, or null for the dialect's preference
   * @return why the upsert can't be written on this database, or null if it can
   */
  public static String unsupportedReason(DatabaseMeta databaseMeta, UpsertForm form) {
    if (databaseMeta == null) {
      return BaseMessages.getString(PKG, "DmlSupport.Error.NoConnection");
    }
    DmlSyntax syntax = databaseMeta.getIDatabase().getDmlSyntax();
    String database = databaseMeta.getPluginName();
    if (form == UpsertForm.MERGE && (syntax == null || syntax.upsert(form) == null)) {
      return BaseMessages.getString(PKG, "DmlSupport.Error.MergeNotSupported", database);
    }
    if (syntax == null || syntax.upserts().isEmpty()) {
      return BaseMessages.getString(PKG, "DmlSupport.Error.UpsertNotSupported", database);
    }
    if (form != null && syntax.upsert(form) == null) {
      return BaseMessages.getString(PKG, "DmlSupport.Error.FormNotDeclared", database, form.name());
    }
    return null;
  }

  /**
   * Checks the dialect and the server version of an open connection, without a table: for a
   * pipeline check or a transform that connects before it knows its columns. The unique keys an
   * {@code ON CONFLICT} needs are only checked by {@link #plan}.
   *
   * @param databaseMeta the connection
   * @param metaData the metadata of the open connection; null to check the dialect only
   * @param form the upsert form asked for, or null for any declared one
   * @return why the upsert can't be written on this server, or null if it can
   */
  public static String unsupportedReason(
      DatabaseMeta databaseMeta, DatabaseMetaData metaData, UpsertForm form) {
    String reason = unsupportedReason(databaseMeta, form);
    if (reason != null || metaData == null) {
      return reason;
    }
    DmlSyntax syntax = databaseMeta.getIDatabase().getDmlSyntax();
    List<DmlSyntax.Upsert> candidates = form == null ? syntax.upserts() : syntax.upserts(form);
    Reasons reasons = new Reasons();
    for (DmlSyntax.Upsert candidate : candidates) {
      String why = serverReason(databaseMeta.getPluginName(), metaData, candidate);
      if (why == null) {
        return null;
      }
      reasons.server(why);
    }
    return reasons.toString();
  }

  /**
   * Chooses the upsert form for a connection and a table.
   *
   * @param databaseMeta the connection
   * @param metaData the metadata of the open connection, for the server version and the indexes of
   *     the table; null to check the dialect only
   * @param spec what the statement does
   * @param schema the schema of the target table, unquoted; null or empty for the default
   * @param table the target table, unquoted
   * @param form the upsert form asked for, or null to let the dialect's preference decide
   * @return the plan; {@code DmlPlan.getUnsupportedReason()} says why when nothing fits
   */
  public static DmlPlan plan(
      DatabaseMeta databaseMeta,
      DatabaseMetaData metaData,
      DmlSpec spec,
      String schema,
      String table,
      UpsertForm form) {
    String reason = unsupportedReason(databaseMeta, form);
    if (reason != null) {
      return DmlPlan.unsupported(null, reason);
    }
    DmlSyntax syntax = databaseMeta.getIDatabase().getDmlSyntax();
    String database = databaseMeta.getPluginName();
    List<DmlSyntax.Upsert> candidates = form == null ? syntax.upserts() : syntax.upserts(form);
    Reasons reasons = new Reasons();
    for (DmlSyntax.Upsert candidate : candidates) {
      String server = serverReason(database, metaData, candidate);
      if (server != null) {
        reasons.server(server);
        continue;
      }
      String why = tableReason(metaData, spec, schema, table, candidate);
      if (why == null) {
        return DmlPlan.of(syntax, candidate);
      }
      reasons.table(why);
    }
    return DmlPlan.unsupported(syntax, reasons.toString());
  }

  /**
   * Why each candidate form was refused. A reason the user can do something about on the table
   * comes before one about the server version, which only Insert/Update gets around.
   */
  private static final class Reasons {
    private final Set<String> table = new LinkedHashSet<>();
    private final Set<String> server = new LinkedHashSet<>();

    void table(String reason) {
      table.add(reason);
    }

    void server(String reason) {
      server.add(reason);
    }

    @Override
    public String toString() {
      List<String> all = new ArrayList<>(table);
      all.addAll(server);
      return all.isEmpty() ? null : String.join(Const.CR, all);
    }
  }

  /**
   * @return why the server can't run the upsert form: too old, or a MariaDB server for a MySQL row
   *     alias; null if it can or can't tell
   */
  private static String serverReason(
      String database, DatabaseMetaData metaData, DmlSyntax.Upsert upsert) {
    String tooOld = versionReason(database, metaData, upsert);
    if (tooOld != null) {
      return tooOld;
    }
    if (upsert.rowAlias() && isMariaDb(metaData)) {
      return BaseMessages.getString(PKG, "DmlSupport.Error.RowAliasOnMariaDb", database);
    }
    return null;
  }

  /**
   * MariaDB answers on a MySQL connection, and reports versions (10, 11) that MySQL's row alias
   * would qualify for. It has no row alias, so its product version gives it away.
   */
  private static boolean isMariaDb(DatabaseMetaData metaData) {
    if (metaData == null) {
      return false;
    }
    try {
      String version = metaData.getDatabaseProductVersion();
      String product = metaData.getDatabaseProductName();
      return (version != null && version.toLowerCase(Locale.ROOT).contains("mariadb"))
          || (product != null && product.toLowerCase(Locale.ROOT).contains("mariadb"));
    } catch (SQLException | RuntimeException e) {
      return false;
    }
  }

  /**
   * @return why the upsert form can't write the spec to this table, or null if it can
   */
  private static String tableReason(
      DatabaseMetaData metaData,
      DmlSpec spec,
      String schema,
      String table,
      DmlSyntax.Upsert upsert) {
    UpsertForm form = upsert.form();
    if (form == UpsertForm.MERGE) {
      return null;
    }
    List<DmlColumn> updateOnly = spec.getUpdateOnlyColumns();
    if (!updateOnly.isEmpty()) {
      return BaseMessages.getString(
          PKG, "DmlSupport.Error.UpdateOnlyColumn", form.name(), updateOnly.get(0).column());
    }
    if (metaData == null) {
      return null;
    }
    List<String> keys = spec.getKeys().stream().map(DmlColumn::column).toList();
    UniqueKey uniqueKey;
    try {
      uniqueKey = uniqueKey(metaData, schema, table, keys);
    } catch (SQLException e) {
      return BaseMessages.getString(
          PKG, "DmlSupport.Error.UniqueKeyUnknown", form.name(), table, e.getMessage());
    }
    if (uniqueKey == UniqueKey.NONE) {
      return BaseMessages.getString(
          PKG, "DmlSupport.Error.NoUniqueKey", form.name(), String.join(", ", keys), table);
    }
    if (form == UpsertForm.ON_DUPLICATE_KEY && uniqueKey == UniqueKey.THE_KEY_AND_OTHERS) {
      return BaseMessages.getString(
          PKG, "DmlSupport.Error.OtherUniqueKey", form.name(), String.join(", ", keys), table);
    }
    return null;
  }

  /**
   * @return why the server is too old for the upsert form, or null when it is not or can't tell
   */
  private static String versionReason(
      String database, DatabaseMetaData metaData, DmlSyntax.Upsert upsert) {
    if (metaData == null || upsert.minimumMajorVersion() <= 0) {
      return null;
    }
    int[] version = version(metaData);
    if (version == null || !tooOld(version, upsert)) {
      return null;
    }
    String server =
        upsert.minimumMinorVersion() > 0
            ? version[0] + "." + version[1]
            : Integer.toString(version[0]);
    if (upsert.form() == UpsertForm.MERGE) {
      return BaseMessages.getString(
          PKG, "DmlSupport.Error.MergeVersionTooOld", database, server, upsert.minimumVersion());
    }
    return BaseMessages.getString(
        PKG,
        "DmlSupport.Error.VersionTooOld",
        database,
        upsert.form().name(),
        server,
        upsert.minimumVersion());
  }

  private static boolean tooOld(int[] version, DmlSyntax.Upsert upsert) {
    if (version[0] != upsert.minimumMajorVersion()) {
      return version[0] < upsert.minimumMajorVersion();
    }
    return version[1] < upsert.minimumMinorVersion();
  }

  /**
   * @return {major, minor}, or null when the driver can't tell
   */
  private static int[] version(DatabaseMetaData metaData) {
    try {
      return new int[] {metaData.getDatabaseMajorVersion(), metaData.getDatabaseMinorVersion()};
    } catch (SQLException | RuntimeException e) {
      // A driver that can't tell its version gets the benefit of the doubt; the first statement
      // says more than a guess would.
      return null;
    }
  }

  /**
   * Reads the primary key and the unique indexes of a table and compares them with the key columns,
   * ignoring case and order. The table is found as {@link DmlColumns#fromTable} finds it: only the
   * table the statement writes to counts, not a table of the same name in another schema or
   * database. A partial unique index (one with a filter condition) doesn't count either, since an
   * {@code ON CONFLICT} can't use it without its predicate. A driver that can't list indexes is
   * asked for the primary key only.
   *
   * @param metaData the metadata of an open connection
   * @param schema the schema, unquoted; null or empty for the connection's current schema
   * @param table the table, unquoted
   * @param keyColumns the key columns, unquoted
   * @return what the unique keys of the table say about the key columns; {@code NONE} when the
   *     table isn't found
   * @throws SQLException when the primary key can't be read either, or when without a schema the
   *     table is in several schemas and not in the current one
   */
  public static UniqueKey uniqueKey(
      DatabaseMetaData metaData, String schema, String table, List<String> keyColumns)
      throws SQLException {
    Set<String> key = new HashSet<>();
    for (String column : keyColumns) {
      key.add(column.toUpperCase(Locale.ROOT));
    }
    DmlTable.Found found = DmlTable.find(metaData, schema, table);
    if (found == null) {
      return UniqueKey.NONE;
    }
    Map<String, Set<String>> uniques = readUniqueKeys(metaData, found.location());
    boolean match = false;
    boolean others = false;
    for (Set<String> unique : uniques.values()) {
      if (unique.equals(key)) {
        match = true;
      } else {
        others = true;
      }
    }
    if (!match) {
      return UniqueKey.NONE;
    }
    return others ? UniqueKey.THE_KEY_AND_OTHERS : UniqueKey.ONLY_THE_KEY;
  }

  private static Map<String, Set<String>> readUniqueKeys(
      DatabaseMetaData metaData, DmlTable.Location location) throws SQLException {
    Map<String, Set<String>> uniques = new LinkedHashMap<>();
    String catalog = location.catalogArgument();
    Set<String> primaryKey = new HashSet<>();
    try (ResultSet resultSet =
        metaData.getPrimaryKeys(catalog, location.schema(), location.table())) {
      while (resultSet.next()) {
        if (location.holds(
            resultSet.getString("TABLE_CAT"),
            resultSet.getString("TABLE_SCHEM"),
            resultSet.getString("TABLE_NAME"))) {
          primaryKey.add(resultSet.getString("COLUMN_NAME").toUpperCase(Locale.ROOT));
        }
      }
    }
    if (!primaryKey.isEmpty()) {
      uniques.put("\u0000primary key", primaryKey);
    }
    Map<String, Set<String>> indexes = new LinkedHashMap<>();
    Set<String> partial = new HashSet<>();
    try (ResultSet resultSet =
        metaData.getIndexInfo(catalog, location.schema(), location.table(), true, false)) {
      while (resultSet.next()) {
        String column = resultSet.getString("COLUMN_NAME");
        String index = resultSet.getString("INDEX_NAME");
        if (column == null
            || index == null
            || resultSet.getBoolean("NON_UNIQUE")
            || !location.holds(
                resultSet.getString("TABLE_CAT"),
                resultSet.getString("TABLE_SCHEM"),
                resultSet.getString("TABLE_NAME"))) {
          continue;
        }
        if (!Utils.isEmpty(filterCondition(resultSet))) {
          partial.add(index);
        }
        indexes
            .computeIfAbsent(index, name -> new HashSet<>())
            .add(column.toUpperCase(Locale.ROOT));
      }
    } catch (SQLException | RuntimeException e) {
      // Some drivers can't list indexes; the primary key is still worth knowing.
    }
    partial.forEach(indexes::remove);
    uniques.putAll(indexes);
    return uniques;
  }

  /**
   * @return FILTER_CONDITION, the predicate of a partial index; null when there is none or the
   *     driver doesn't report it
   */
  private static String filterCondition(ResultSet resultSet) {
    try {
      return resultSet.getString("FILTER_CONDITION");
    } catch (SQLException | RuntimeException e) {
      return null;
    }
  }
}
