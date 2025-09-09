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

package org.apache.hop.pipeline.transforms.formula.util;

import static org.apache.hop.pipeline.transforms.formula.util.FormulaFieldsExtractor.getFormulaFieldList;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.transforms.formula.FormulaMetaFunction;
import org.apache.hop.pipeline.transforms.formula.FormulaPoi;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.Row;

public class FormulaParser {

  private FormulaMetaFunction formulaMetaFunction;
  private IRowMeta rowMeta;
  private String formula;
  private List<String> formulaFieldList;
  private Object[] dataRow;
  private FormulaPoi.Evaluator evaluator;
  private HashMap<String, String> replaceMap;

  public FormulaParser(
      FormulaMetaFunction formulaMetaFunction,
      IRowMeta rowMeta,
      Object[] dataRow,
      FormulaPoi poi,
      IVariables variables,
      HashMap<String, String> replaceMap,
      List<String> formulaFieldList) {
    this.formulaMetaFunction = formulaMetaFunction;
    this.rowMeta = rowMeta;
    this.dataRow = dataRow;
    this.replaceMap = replaceMap;
    formula = variables.resolve(formulaMetaFunction.getFormula());

    this.formulaFieldList = formulaFieldList;

    boolean getNewList = false;
    for (String formulaField : formulaFieldList) {
      // check if we are working with a field that was replaced earlier.
      Set<String> replaceKeys = replaceMap.keySet();
      if (replaceKeys.contains(formulaField)) {
        String realFieldName = replaceMap.get(formulaField);
        formula = formula.replace("[" + formulaField + "]", "[" + realFieldName + "]");
        getNewList = true;
      }
    }

    if (getNewList) {
      this.formulaFieldList = getFormulaFieldList(variables.resolve(formula));
    }
    this.evaluator = poi.evaluator(formulaFieldList.size() + 1);
    this.evaluator.evaluator().clearAllCachedResultValues();
  }

  public CellValue getFormulaValue() throws HopValueException {
    String parsedFormula = formula;
    int fieldIndex = 65;
    int colIndex = 0;
    Row row = evaluator.row();

    // reset, something changed else reuse to leverage the formula parsing cache which does speed up
    // a lot the runtime
    if (row.getLastCellNum() > 0 && row.getLastCellNum() != formulaFieldList.size() + 1) {
      if (evaluator.row() != null) {
        evaluator.sheet().removeRow(evaluator.row());
      }
      row = evaluator.sheet().createRow(0);
      evaluator.row(row);
    }

    for (String formulaField : formulaFieldList) {
      char s = (char) fieldIndex;
      final Cell cell;
      if (row.getLastCellNum() <= colIndex) {
        cell = row.createCell(colIndex);
      } else {
        cell = row.getCell(colIndex);
      }

      int fieldPosition = rowMeta.indexOfValue(formulaField);

      parsedFormula = parsedFormula.replace("[" + formulaField + "]", s + "1");

      IValueMeta fieldMeta = rowMeta.getValueMeta(fieldPosition);
      if (dataRow[fieldPosition] != null) {
        if (fieldMeta.isString()) { // most common first to avoid a lot of "if" for nothing
          cell.setCellValue(rowMeta.getString(dataRow, fieldPosition));
        } else if (fieldMeta.isBoolean()) {
          cell.setCellValue(rowMeta.getBoolean(dataRow, fieldPosition));
        } else if (fieldMeta.isBigNumber()) {
          cell.setCellValue(new HSSFRichTextString(rowMeta.getString(dataRow, fieldPosition)));
        } else if (fieldMeta.isDate()) {
          cell.setCellValue(rowMeta.getDate(dataRow, fieldPosition));
        } else if (fieldMeta.isInteger()) {
          cell.setCellValue(rowMeta.getInteger(dataRow, fieldPosition));
        } else if (fieldMeta.isNumber()) {
          cell.setCellValue(rowMeta.getNumber(dataRow, fieldPosition));
        } else {
          cell.setCellValue(rowMeta.getString(dataRow, fieldPosition));
        }
      } else {
        if (formulaMetaFunction.isSetNa()) {
          cell.setCellFormula("NA()");
        } else {
          cell.setBlank();
        }
      }

      fieldIndex++;
      colIndex++;
    }

    final Cell formulaCell;
    if (row.getLastCellNum() <= colIndex) {
      formulaCell = row.createCell(colIndex);
      formulaCell.setCellFormula(parsedFormula);
    } else { // already created/parsed
      formulaCell = row.getCell(colIndex);
    }

    return evaluator.evaluator().evaluate(formulaCell);
  }
}
