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
package org.apache.hop.core.database;

import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.hop.core.row.IRowMeta;

/**
 * The row layouts {@link Database#getLookup(PreparedStatement, boolean, boolean)} derived, per
 * lookup statement and lazy conversion flag.
 *
 * <p>Update, Insert/Update, Dimension and Combination lookup and Synchronize after merge execute
 * their lookup statement once per row. Deriving the layout from the result set metadata every time
 * (value metadata plugins, the dialect's type rules) costs more than the lookup itself. A transform
 * may alternate a few statements on one Database, so a handful are kept, the oldest replaced first.
 *
 * <p>A cached layout is only handed out while the result set still describes the same columns: the
 * same statement can answer different columns, for example a {@code SELECT *} after the table was
 * altered, or a statement object a driver hands out again for other SQL. The comparison reads only
 * the plain column properties the layout is derived from; it does not map any type.
 *
 * <p>Not thread-safe, like the Database it belongs to.
 */
final class LookupRowMetaCache {

  static final int SIZE = 8;

  private final Entry[] entries = new Entry[SIZE];
  private int nextSlot;

  /**
   * @return the layout derived earlier for this statement and lazy conversion flag, when the result
   *     set metadata still describes the same columns; null otherwise
   */
  IRowMeta get(PreparedStatement statement, boolean lazyConversion, ResultSetMetaData metaData)
      throws SQLException {
    for (Entry entry : entries) {
      if (entry != null
          && entry.statement == statement
          && entry.lazyConversion == lazyConversion
          && entry.columns.matches(metaData)) {
        return entry.rowMeta;
      }
    }
    return null;
  }

  /** Keeps the layout derived for this statement and flag, replacing any earlier one. */
  void put(
      PreparedStatement statement,
      boolean lazyConversion,
      ResultSetMetaData metaData,
      IRowMeta rowMeta)
      throws SQLException {
    Entry entry = new Entry(statement, lazyConversion, Columns.of(metaData), rowMeta);
    for (int i = 0; i < entries.length; i++) {
      Entry existing = entries[i];
      if (existing != null
          && existing.statement == statement
          && existing.lazyConversion == lazyConversion) {
        entries[i] = entry;
        return;
      }
    }
    entries[nextSlot] = entry;
    nextSlot = (nextSlot + 1) % entries.length;
  }

  /** Forgets the layouts of a statement that is closed or no longer used for lookups. */
  void forget(PreparedStatement statement) {
    if (statement == null) {
      return;
    }
    for (int i = 0; i < entries.length; i++) {
      if (entries[i] != null && entries[i].statement == statement) {
        entries[i] = null;
      }
    }
  }

  void clear() {
    Arrays.fill(entries, null);
    nextSlot = 0;
  }

  private record Entry(
      PreparedStatement statement, boolean lazyConversion, Columns columns, IRowMeta rowMeta) {}

  /** The column properties of a result set that its layout is derived from. */
  private static final class Columns {
    private final String[] names;
    private final String[] labels;
    private final int[] types;
    private final String[] typeNames;
    private final int[] precisions;
    private final int[] scales;

    private Columns(int count) {
      names = new String[count];
      labels = new String[count];
      types = new int[count];
      typeNames = new String[count];
      precisions = new int[count];
      scales = new int[count];
    }

    static Columns of(ResultSetMetaData metaData) throws SQLException {
      int count = metaData.getColumnCount();
      Columns columns = new Columns(count);
      for (int i = 0; i < count; i++) {
        int column = i + 1;
        columns.names[i] = metaData.getColumnName(column);
        columns.labels[i] = metaData.getColumnLabel(column);
        columns.types[i] = metaData.getColumnType(column);
        columns.typeNames[i] = metaData.getColumnTypeName(column);
        columns.precisions[i] = metaData.getPrecision(column);
        columns.scales[i] = metaData.getScale(column);
      }
      return columns;
    }

    boolean matches(ResultSetMetaData metaData) throws SQLException {
      int count = types.length;
      if (metaData.getColumnCount() != count) {
        return false;
      }
      for (int i = 0; i < count; i++) {
        int column = i + 1;
        if (metaData.getColumnType(column) != types[i]
            || metaData.getPrecision(column) != precisions[i]
            || metaData.getScale(column) != scales[i]
            || !Objects.equals(metaData.getColumnName(column), names[i])
            || !Objects.equals(metaData.getColumnLabel(column), labels[i])
            || !Objects.equals(metaData.getColumnTypeName(column), typeNames[i])) {
          return false;
        }
      }
      return true;
    }
  }
}
