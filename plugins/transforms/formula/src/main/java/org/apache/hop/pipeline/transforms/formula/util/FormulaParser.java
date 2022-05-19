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

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.transforms.formula.FormulaMetaFunction;
import org.apache.poi.ss.usermodel.*;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FormulaParser {

  private FormulaMetaFunction formulaMetaFunction;
  private IRowMeta rowMeta;
  private String[] fieldNames;
  private String formula;
  private List<String> formulaFieldList;
  private Object[] dataRow;
  private Row sheetRow;
  private FormulaEvaluator evaluator;

  public FormulaParser(
      FormulaMetaFunction formulaMetaFunction, IRowMeta rowMeta, Object[] dataRow, Row sheetRow) {
    this.formulaMetaFunction = formulaMetaFunction;
    this.rowMeta = rowMeta;
    this.dataRow = dataRow;
    this.sheetRow = sheetRow;
    fieldNames = rowMeta.getFieldNames();
    formula = formulaMetaFunction.getFormula();
    evaluator = sheetRow.getSheet().getWorkbook().getCreationHelper().createFormulaEvaluator();

    formulaFieldList = new ArrayList<>();
    Pattern regex = Pattern.compile("\\[(.*?)\\]");
    Matcher regexMatcher = regex.matcher(formula);

    while (regexMatcher.find()) {
      formulaFieldList.add(regexMatcher.group(1));
    }
  }

  public CellValue getFormulaValue() {
    String parsedFormula = formula;
    int fieldIndex = 65;
    int colIndex = 0;
    for (String formulaField : formulaFieldList) {
      char s = (char) fieldIndex;
      Cell cell = sheetRow.createCell(colIndex);
      int fieldPosition = rowMeta.indexOfValue(formulaField);

      IValueMeta fieldMeta = rowMeta.getValueMeta(fieldPosition);
      if (fieldMeta.isBoolean()) {
        cell.setCellValue((Boolean) dataRow[fieldPosition]);
      } else if (fieldMeta.isBigNumber()) {
        cell.setCellValue((RichTextString) dataRow[fieldPosition]);
      } else if (fieldMeta.isDate()) {
        cell.setCellValue((Date) dataRow[fieldPosition]);
      } else if (fieldMeta.isInteger()) {
        cell.setCellValue((Long) dataRow[fieldPosition]);
      } else if (fieldMeta.isNumber()) {
        cell.setCellValue((Double) dataRow[fieldPosition]);
      } else if (fieldMeta.isString()) {
        cell.setCellValue((String) dataRow[fieldPosition]);
      } else if (fieldMeta.getType() == IValueMeta.TYPE_TIMESTAMP) {
        cell.setCellValue((Timestamp) dataRow[fieldPosition]);
      } else {
        cell.setCellValue((String) dataRow[fieldPosition]);
      }

      parsedFormula = parsedFormula.replaceAll("\\[" + formulaField + "\\]", s + "1");
      fieldIndex++;
      colIndex++;
    }

    Cell formulaCell = sheetRow.createCell(colIndex);
    formulaCell.setCellFormula(parsedFormula);

    return evaluator.evaluate(formulaCell);
  }
}
