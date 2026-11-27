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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import lombok.Getter;
import org.apache.hop.core.exception.HopDatabaseException;

/**
 * What a DML statement does, independent of how a database writes it: the operation (an upsert or a
 * plain INSERT), the target table, the key columns that identify a row (always matched with "="),
 * the columns written on insert and those written on update.
 *
 * <p>The values of one row are passed as a bind row that holds one value per source column: the key
 * columns first, then the other columns, each column once whatever the number of lists it is in.
 * Every statement built for this spec says where each of its parameters comes from in that row, see
 * {@link DmlStatement#getLayout()}.
 */
@Getter
public final class DmlSpec {

  /** What the statement does. */
  private final DmlOperation operation;

  /** The target table, already quoted and schema-qualified. */
  private final String quotedTable;

  /** The columns that identify a row; empty for an INSERT. */
  private final List<DmlColumn> keys;

  /** The columns written by the insert, keys excluded. */
  private final List<DmlColumn> inserted;

  /**
   * The columns written by the update; empty for an insert-only upsert. Key columns in this list
   * are never updated.
   */
  private final List<DmlColumn> updated;

  /**
   * Update a matched row only when one of the updated columns differs, comparing NULLs as equal.
   * When a column of the SET list can't be compared, every matched row is updated, see {@link
   * #getUncomparableSetColumns()}.
   */
  private final boolean updateOnlyChanged;

  /** Every column a bind row holds, keys first, each once. */
  private final List<DmlColumn> sourceColumns;

  /** The updated columns that are not key columns: the SET list. */
  private final List<DmlColumn> setColumns;

  private DmlSpec(
      DmlOperation operation,
      String quotedTable,
      List<DmlColumn> keys,
      List<DmlColumn> inserted,
      List<DmlColumn> updated,
      boolean updateOnlyChanged)
      throws HopDatabaseException {
    this.operation = operation;
    this.quotedTable = quotedTable;
    this.keys = keys == null ? List.of() : List.copyOf(keys);
    this.inserted = inserted == null ? List.of() : List.copyOf(inserted);
    this.updated = updated == null ? List.of() : List.copyOf(updated);
    this.updateOnlyChanged = updateOnlyChanged;

    if (operation == DmlOperation.UPSERT && this.keys.isEmpty()) {
      throw new HopDatabaseException("An upsert needs at least one key column");
    }

    Set<String> names = new HashSet<>();
    List<DmlColumn> source = new ArrayList<>();
    for (List<DmlColumn> columns : List.of(this.keys, this.inserted, this.updated)) {
      for (DmlColumn column : columns) {
        if (names.add(upper(column))) {
          source.add(column);
        }
      }
    }
    this.sourceColumns = List.copyOf(source);

    Set<String> keyNames = new HashSet<>();
    for (DmlColumn key : this.keys) {
      keyNames.add(upper(key));
    }
    List<DmlColumn> set = new ArrayList<>();
    Set<String> setNames = new HashSet<>();
    for (DmlColumn column : this.updated) {
      if (!keyNames.contains(upper(column)) && setNames.add(upper(column))) {
        set.add(column);
      }
    }
    this.setColumns = List.copyOf(set);

    if (operation == DmlOperation.INSERT && this.sourceColumns.isEmpty()) {
      throw new HopDatabaseException("An INSERT needs at least one column");
    }
  }

  /**
   * @param quotedTable the target table, quoted and schema-qualified
   * @param columns the columns to insert, in the order of the bind row
   * @throws HopDatabaseException without a column
   */
  public static DmlSpec insert(String quotedTable, List<DmlColumn> columns)
      throws HopDatabaseException {
    return new DmlSpec(DmlOperation.INSERT, quotedTable, List.of(), columns, List.of(), false);
  }

  /**
   * @param quotedTable the target table, quoted and schema-qualified
   * @param keys the columns that identify a row, always inserted
   * @param inserted the other columns written on insert
   * @param updated the columns written on update; empty for insert-only
   * @param updateOnlyChanged true to update a matched row only when one of the updated columns
   *     differs, comparing NULLs as equal
   * @throws HopDatabaseException without a key column
   */
  public static DmlSpec upsert(
      String quotedTable,
      List<DmlColumn> keys,
      List<DmlColumn> inserted,
      List<DmlColumn> updated,
      boolean updateOnlyChanged)
      throws HopDatabaseException {
    return new DmlSpec(
        DmlOperation.UPSERT, quotedTable, keys, inserted, updated, updateOnlyChanged);
  }

  /**
   * @return the columns an upsert inserts: the keys, then the inserted columns, each once
   */
  public List<DmlColumn> getInsertColumns() {
    List<DmlColumn> columns = new ArrayList<>();
    Set<String> names = new HashSet<>();
    for (List<DmlColumn> list : List.of(keys, inserted)) {
      for (DmlColumn column : list) {
        if (names.add(upper(column))) {
          columns.add(column);
        }
      }
    }
    return columns;
  }

  /**
   * @return the updated columns that an upsert does not insert. {@code ON CONFLICT} and {@code ON
   *     DUPLICATE KEY} can only update a column from the row they tried to insert, so they can't
   *     write these.
   */
  public List<DmlColumn> getUpdateOnlyColumns() {
    Set<String> insertNames = new HashSet<>();
    for (DmlColumn column : getInsertColumns()) {
      insertNames.add(upper(column));
    }
    List<DmlColumn> columns = new ArrayList<>();
    for (DmlColumn column : setColumns) {
      if (!insertNames.contains(upper(column))) {
        columns.add(column);
      }
    }
    return columns;
  }

  /**
   * @return the columns of the SET list that the database can't compare with {@code <>}. With one
   *     of them, an update of changed rows only can't tell whether a row changed, and updates every
   *     matched row.
   */
  public List<DmlColumn> getUncomparableSetColumns() {
    return setColumns.stream().filter(column -> !column.comparable()).toList();
  }

  /**
   * @return the position of every key column in {@code getSourceColumns()}, in key order
   */
  public int[] getKeyIndexes() {
    int[] indexes = new int[keys.size()];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = sourceIndex(keys.get(i));
    }
    return indexes;
  }

  /**
   * @return the position of the column in {@code getSourceColumns()}, matched ignoring case
   */
  int sourceIndex(DmlColumn column) {
    String name = upper(column);
    for (int i = 0; i < sourceColumns.size(); i++) {
      if (upper(sourceColumns.get(i)).equals(name)) {
        return i;
      }
    }
    throw new IllegalArgumentException("Column " + column.column() + " is not in the spec");
  }

  static String upper(DmlColumn column) {
    return column.column() == null ? "" : column.column().toUpperCase(Locale.ROOT);
  }

  @Override
  public String toString() {
    return operation + " " + quotedTable + " keys=" + keys + " source=" + sourceColumns;
  }
}
