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

package org.apache.hop.openformula.ast;

import java.util.List;
import org.apache.hop.openformula.EvaluationException;
import org.apache.hop.openformula.FormulaContext;
import org.apache.hop.openformula.error.FormulaErrorValue;
import org.apache.hop.openformula.typing.ArrayCallback;
import org.apache.hop.openformula.typing.DataType;
import org.apache.hop.openformula.typing.TypedValue;

public record ArrayExpr(List<List<Expression>> rows) implements Expression {
  @Override
  public TypedValue evaluate(FormulaContext context) throws EvaluationException {
    if (rows == null || rows.isEmpty()) {
      return TypedValue.ofArray(new EmptyArrayCallback());
    }

    int numRows = rows.size();
    int numCols = rows.get(0).size();

    // Check rectangular shape
    for (List<Expression> row : rows) {
      if (row.size() != numCols) {
        return TypedValue.ofError(FormulaErrorValue.ERROR_ILLEGAL_ARRAY_VALUE);
      }
    }

    TypedValue[][] evaluated = new TypedValue[numRows][numCols];
    for (int r = 0; r < numRows; r++) {
      List<Expression> row = rows.get(r);
      for (int c = 0; c < numCols; c++) {
        Expression expr = row.get(c);
        evaluated[r][c] = (expr == null) ? TypedValue.NULL : expr.evaluate(context);
      }
    }

    return TypedValue.ofArray(new EvaluatedArrayCallback(evaluated));
  }

  @Override
  public DataType getType(FormulaContext context) {
    return DataType.ARRAY;
  }

  private static class EmptyArrayCallback implements ArrayCallback {
    @Override
    public int getRowCount() {
      return 0;
    }

    @Override
    public int getColumnCount() {
      return 0;
    }

    @Override
    public Object getValue(int row, int column) {
      return null;
    }

    @Override
    public DataType getType(int row, int column) {
      return DataType.ANY;
    }
  }

  private static class EvaluatedArrayCallback implements ArrayCallback {
    private final TypedValue[][] data;
    private final int rows;
    private final int cols;

    EvaluatedArrayCallback(TypedValue[][] data) {
      this.data = data;
      this.rows = data.length;
      this.cols = (rows == 0) ? 0 : data[0].length;
    }

    @Override
    public int getRowCount() {
      return rows;
    }

    @Override
    public int getColumnCount() {
      return cols;
    }

    @Override
    public Object getValue(int row, int column) {
      if (row < 0 || row >= rows || column < 0 || column >= cols) return null;
      return data[row][column].value();
    }

    @Override
    public DataType getType(int row, int column) {
      if (row < 0 || row >= rows || column < 0 || column >= cols) return DataType.ANY;
      return data[row][column].type();
    }
  }
}
