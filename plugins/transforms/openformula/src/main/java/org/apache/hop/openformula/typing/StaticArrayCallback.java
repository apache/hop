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

package org.apache.hop.openformula.typing;

import org.apache.hop.openformula.EvaluationException;

/** An ArrayCallback backed by a 2D array of values. */
public class StaticArrayCallback implements ArrayCallback {
  private final Object[][] data;
  private final int rowCount;
  private final int columnCount;

  public StaticArrayCallback(Object[][] data) {
    this.data = data;
    this.rowCount = (data == null) ? 0 : data.length;
    this.columnCount = (rowCount == 0 || data[0] == null) ? 0 : data[0].length;
  }

  @Override
  public int getRowCount() {
    return rowCount;
  }

  @Override
  public int getColumnCount() {
    return columnCount;
  }

  @Override
  public Object getValue(int row, int column) throws EvaluationException {
    if (row < 0 || row >= rowCount || column < 0 || column >= columnCount) {
      return null;
    }
    return data[row][column];
  }

  @Override
  public DataType getType(int row, int column) throws EvaluationException {
    Object val = getValue(row, column);
    if (val == null) return DataType.ANY;
    if (val instanceof Number) return DataType.NUMBER;
    if (val instanceof String) return DataType.TEXT;
    if (val instanceof Boolean) return DataType.LOGICAL;
    if (val instanceof java.util.Date) return DataType.DATETIME;
    if (val instanceof ArrayCallback) return DataType.ARRAY;
    return DataType.ANY;
  }
}
