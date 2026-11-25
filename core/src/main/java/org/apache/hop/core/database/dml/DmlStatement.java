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
 * A DML statement for one row, with where each of its parameters comes from.
 *
 * <p>The value of the parameter at position {@code p} (zero-based; JDBC index {@code p + 1}) is
 * {@code bindRow[getLayout()[p]]}, where the bind row holds one value per source column of the
 * {@link DmlSpec}. {@code getRowColumns()} names the column of every parameter.
 */
@Getter
public final class DmlStatement {

  private final String sql;

  /** The columns of the parameters, in parameter order. */
  private final List<DmlColumn> rowColumns;

  private final int[] layout;

  public DmlStatement(String sql, List<DmlColumn> rowColumns, int[] layout) {
    this.sql = sql;
    this.rowColumns = List.copyOf(rowColumns);
    this.layout = layout.clone();
  }

  /**
   * @return for each parameter position, the index of its value in the bind row
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

  public int getParameterCount() {
    return layout.length;
  }

  @Override
  public String toString() {
    return sql;
  }
}
