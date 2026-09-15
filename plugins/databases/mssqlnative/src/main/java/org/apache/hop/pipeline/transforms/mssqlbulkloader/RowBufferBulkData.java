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

package org.apache.hop.pipeline.transforms.mssqlbulkloader;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Presents one batch of already converted pipeline rows to {@code SQLServerBulkCopy}.
 *
 * <p>The driver pulls rather than is pushed: it calls {@link #next()} and {@link #getRowData()}
 * until the batch runs out. Values are handed over as the objects the driver expects for the target
 * column - no text rendering, so nothing has to be re-parsed and nothing can collide with a
 * separator.
 *
 * <p>Ordinals are 1-based and contiguous, which makes the ordinal of a column and its position in
 * the array returned by {@link #getRowData()} the same thing.
 */
public class RowBufferBulkData implements ISQLServerBulkData {

  private static final long serialVersionUID = 1L;

  /** One target column, described the way the driver wants to hear it. */
  public record Column(String name, int sqlType, int precision, int scale)
      implements Serializable {}

  private final List<Object[]> rows;
  private final Column[] columns;
  private final Set<Integer> ordinals;

  private int cursor = -1;

  public RowBufferBulkData(List<Object[]> rows, Column[] columns) {
    this.rows = rows;
    this.columns = columns;

    // LinkedHashSet: the driver iterates this set, so the order has to stay 1..n.
    this.ordinals = new LinkedHashSet<>(columns.length);
    for (int i = 1; i <= columns.length; i++) {
      ordinals.add(i);
    }
  }

  @Override
  public Set<Integer> getColumnOrdinals() {
    return ordinals;
  }

  @Override
  public String getColumnName(int column) {
    return columns[column - 1].name();
  }

  @Override
  public int getColumnType(int column) {
    return columns[column - 1].sqlType();
  }

  @Override
  public int getPrecision(int column) {
    return columns[column - 1].precision();
  }

  @Override
  public int getScale(int column) {
    return columns[column - 1].scale();
  }

  @Override
  public Object[] getRowData() {
    return rows.get(cursor);
  }

  @Override
  public boolean next() {
    return ++cursor < rows.size();
  }
}
