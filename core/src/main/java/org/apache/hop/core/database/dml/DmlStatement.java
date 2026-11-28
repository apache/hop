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

import java.util.List;
import lombok.Getter;

/**
 * A DML statement for one row or a fixed number of rows, with where each of its parameters comes
 * from.
 *
 * <p>The value of the parameter at position {@code p} (zero-based; JDBC index {@code p + 1}) of a
 * row is {@code bindRow[getLayout()[p]]}, where the bind row holds one value per source column of
 * the {@link DmlSpec}. {@code getRowColumns()} names the column of every parameter of a row.
 *
 * <p>The parameters of one row are contiguous: row {@code r} (zero-based) of a statement of several
 * rows binds its row columns at parameter indexes {@code r * getParametersPerRow() + 1} and up, see
 * {@link #parameterIndex(int, int)}.
 */
@Getter
public final class DmlStatement {

  private final String sql;

  /** The number of rows the statement carries. */
  private final int rows;

  /** The columns of one row's parameters, in parameter order. */
  private final List<DmlColumn> rowColumns;

  private final int[] layout;

  public DmlStatement(String sql, List<DmlColumn> rowColumns, int[] layout) {
    this(sql, 1, rowColumns, layout);
  }

  public DmlStatement(String sql, int rows, List<DmlColumn> rowColumns, int[] layout) {
    this.sql = sql;
    this.rows = rows;
    this.rowColumns = List.copyOf(rowColumns);
    this.layout = layout.clone();
  }

  /**
   * @return for each parameter position within a row, the index of its value in the bind row
   */
  public int[] getLayout() {
    return layout.clone();
  }

  /**
   * @return the index of the value of the parameter at this position in the bind row
   */
  public int sourceIndex(int position) {
    return layout[position];
  }

  /**
   * @return the number of parameters of one row
   */
  public int getParametersPerRow() {
    return layout.length;
  }

  /**
   * @return the number of parameters of the statement, of all its rows
   */
  public int getParameterCount() {
    return rows * layout.length;
  }

  /**
   * @param row the row within the statement, zero-based
   * @param position the parameter within the row, zero-based
   * @return the JDBC parameter index, one-based
   */
  public int parameterIndex(int row, int position) {
    return row * layout.length + position + 1;
  }

  @Override
  public String toString() {
    return sql;
  }
}
