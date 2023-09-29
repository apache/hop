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

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.transforms.formula.FormulaMetaFunction;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
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
  private HashMap<String, String> replaceMap;

  public FormulaParser(
      FormulaMetaFunction formulaMetaFunction, IRowMeta rowMeta, Object[] dataRow, Row sheetRow, IVariables variables, HashMap<String, String> replaceMap) {
    this.formulaMetaFunction = formulaMetaFunction;
    this.rowMeta = rowMeta;
    this.dataRow = dataRow;
    this.sheetRow = sheetRow;
    fieldNames = rowMeta.getFieldNames();
    this.replaceMap = replaceMap;
    formula = variables.resolve(formulaMetaFunction.getFormula());
    evaluator = sheetRow.getSheet().getWorkbook().getCreationHelper().createFormulaEvaluator();

    formulaFieldList = getFormulaFieldList(formula);

    boolean getNewList = false;
    for(String formulaField : formulaFieldList){
      // check if we are working with a field that was replaced earlier.
      Set<String> replaceKeys = replaceMap.keySet();
      if(replaceKeys.contains(formulaField)){
        String realFieldName = replaceMap.get(formulaField);
        formula = formula.replaceAll("\\[" + formulaField + "\\]", "\\[" + realFieldName + "\\]");
        getNewList = true;
      }
    }

    if(getNewList){
      formulaFieldList = getFormulaFieldList(formula);
    }
  }

  private List<String> getFormulaFieldList(String formula){
    List<String> theFields = new ArrayList<>();
    Pattern regex = Pattern.compile("\\[(.*?)\\]");
    Matcher regexMatcher = regex.matcher(formula);

    while (regexMatcher.find()) {
      theFields.add(regexMatcher.group(1));
    }
    return theFields;
  }

  public CellValue getFormulaValue() throws HopValueException {
    String parsedFormula = formula;
    int fieldIndex = 65;
    int colIndex = 0;
    for (String formulaField : formulaFieldList) {
      char s = (char) fieldIndex;
      Cell cell = sheetRow.createCell(colIndex);

      int fieldPosition = rowMeta.indexOfValue(formulaField);

      parsedFormula = parsedFormula.replaceAll("\\[" + formulaField + "\\]", s + "1");

      IValueMeta fieldMeta = rowMeta.getValueMeta(fieldPosition);
      if (dataRow[fieldPosition] != null) {
        if (fieldMeta.isBoolean()) {
          cell.setCellValue(rowMeta.getBoolean(dataRow, fieldPosition));
        } else if (fieldMeta.isBigNumber()) {
          cell.setCellValue(new HSSFRichTextString(rowMeta.getString(dataRow, fieldPosition)));
        } else if (fieldMeta.isDate()) {
          cell.setCellValue(rowMeta.getDate(dataRow, fieldPosition));
        } else if (fieldMeta.isInteger()) {
          cell.setCellValue(rowMeta.getInteger(dataRow, fieldPosition));
        } else if (fieldMeta.isNumber()) {
          cell.setCellValue(rowMeta.getNumber(dataRow, fieldPosition));
        } else if (fieldMeta.isString()) {
          cell.setCellValue(rowMeta.getString(dataRow, fieldPosition));
        } else {
          cell.setCellValue(rowMeta.getString(dataRow, fieldPosition));
        }
      }else{
        cell.setBlank();
      }

      fieldIndex++;
      colIndex++;
    }

    Cell formulaCell = sheetRow.createCell(colIndex);
    formulaCell.setCellFormula(parsedFormula);

    return evaluator.evaluate(formulaCell);
  }
}
