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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.formula.Formula;
import org.apache.hop.pipeline.transforms.formula.FormulaMetaFunction;
import org.apache.hop.pipeline.transforms.formula.FormulaPoi;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.util.CellReference;

public class FormulaParser {
  private static final Class<?> PKG = Formula.class; // for i18n purposes

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

      String s = CellReference.convertNumToColString(colIndex);
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
        // most common first to avoid a lot of "if" for nothing
        if (fieldMeta.isString()) {
          cell.setCellValue(rowMeta.getString(dataRow, fieldPosition));
        } else if (fieldMeta.isBoolean()) {
          cell.setCellValue(rowMeta.getBoolean(dataRow, fieldPosition));
        } else if (fieldMeta.isBigNumber()) {
          cell.setCellValue(rowMeta.getNumber(dataRow, fieldPosition));
        } else if (fieldMeta.isDate()) {
          Date date = rowMeta.getDate(dataRow, fieldPosition);
          checkSupportedDate(fieldMeta, date);
          cell.setCellValue(date);
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

  /**
   * Formulas are evaluated as Excel date serial numbers, which start at 1899-12-31 (serial 0). POI
   * silently maps anything older to the same BAD_DATE sentinel (-1), so every earlier date would
   * collapse to one value and come out of the transform blank or, after date arithmetic, plain
   * wrong. Report it instead of losing the value without a trace.
   *
   * @param fieldMeta the metadata of the date field being written to a cell
   * @param date the value to write, may be null
   * @throws HopValueException when the date can not be represented as an Excel date serial number
   */
  private void checkSupportedDate(IValueMeta fieldMeta, Date date) throws HopValueException {
    if (date == null || DateUtil.getExcelDate(date) >= 0) {
      return;
    }
    throw new HopValueException(
        BaseMessages.getString(
            PKG,
            "Formula.Exception.DateBeforeExcelEpoch",
            fieldMeta.getName(),
            fieldMeta.getString(date)));
  }
}
