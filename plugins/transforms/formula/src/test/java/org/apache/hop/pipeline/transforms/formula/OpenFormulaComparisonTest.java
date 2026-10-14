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

package org.apache.hop.pipeline.transforms.formula;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.logging.Logger;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.openformula.Formula;
import org.apache.hop.pipeline.transforms.openformula.RowFormulaContext;
import org.apache.poi.hssf.usermodel.HSSFFormulaEvaluator;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.CellValue;
import org.junit.jupiter.api.Test;

/**
 * Shared expressions on the POI Formula transform and the OpenFormula engine. Each side uses its
 * own syntax. Elapsed times are logged and are not a build gate.
 */
class OpenFormulaComparisonTest {
  private static final Logger LOG = Logger.getLogger(OpenFormulaComparisonTest.class.getName());
  private static final int ROWS = 20_000;

  @Test
  void sharedExpressionsMatch() throws Exception {
    assertEquals("Y", openFormula("IF([flag];\"Y\";\"N\")", flagRow(true)));
    assertEquals("N", openFormula("IF([flag];\"Y\";\"N\")", flagRow(false)));
    assertEquals("Y", poiIf(true));
    assertEquals("N", poiIf(false));

    assertEquals(5.0d, ((Number) openFormula("[a]+[b]", intRow(2, 3))).doubleValue(), 0.0001);
    assertEquals(5.0d, poiSum(2, 3), 0.0001);

    assertEquals("hop", openFormula("LOWER([name])", nameRow("Hop")));
    assertEquals("hop", poiLower("Hop"));
  }

  @Test
  void logEvaluationTime() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBoolean("flag"));
    RowFormulaContext context = new RowFormulaContext(rowMeta);
    Formula formula = new Formula("IF([flag];\"Y\";\"N\")");
    formula.initialize(context);

    long openStart = System.nanoTime();
    for (int i = 0; i < ROWS; i++) {
      context.setRowData(new Object[] {i % 2 == 0});
      formula.evaluate();
    }
    long openMs = (System.nanoTime() - openStart) / 1_000_000L;

    long poiMs;
    try (HSSFWorkbook workbook = new HSSFWorkbook()) {
      HSSFSheet sheet = workbook.createSheet();
      HSSFRow row = sheet.createRow(0);
      row.createCell(0).setCellValue(true);
      row.createCell(1).setCellFormula("IF(A1,\"Y\",\"N\")");
      HSSFFormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
      long poiStart = System.nanoTime();
      for (int i = 0; i < ROWS; i++) {
        row.getCell(0).setCellValue(i % 2 == 0);
        evaluator.clearAllCachedResultValues();
        evaluator.evaluate(row.getCell(1));
      }
      poiMs = (System.nanoTime() - poiStart) / 1_000_000L;
    }
    LOG.info("OpenFormula " + ROWS + " IF rows: " + openMs + " ms");
    LOG.info("POI Formula " + ROWS + " IF rows: " + poiMs + " ms");
  }

  private static Object openFormula(String expression, RowSetup setup) throws Exception {
    RowFormulaContext context = new RowFormulaContext(setup.rowMeta);
    context.setRowData(setup.row);
    Formula formula = new Formula(expression);
    formula.initialize(context);
    return formula.evaluate();
  }

  private static String poiIf(boolean flag) throws Exception {
    try (HSSFWorkbook workbook = new HSSFWorkbook()) {
      HSSFRow row = workbook.createSheet().createRow(0);
      row.createCell(0).setCellValue(flag);
      row.createCell(1).setCellFormula("IF(A1,\"Y\",\"N\")");
      CellValue value =
          workbook.getCreationHelper().createFormulaEvaluator().evaluate(row.getCell(1));
      assertEquals(CellType.STRING, value.getCellType());
      return value.getStringValue();
    }
  }

  private static double poiSum(int a, int b) throws Exception {
    try (HSSFWorkbook workbook = new HSSFWorkbook()) {
      HSSFRow row = workbook.createSheet().createRow(0);
      row.createCell(0).setCellValue(a);
      row.createCell(1).setCellValue(b);
      row.createCell(2).setCellFormula("A1+B1");
      CellValue value =
          workbook.getCreationHelper().createFormulaEvaluator().evaluate(row.getCell(2));
      return value.getNumberValue();
    }
  }

  private static String poiLower(String name) throws Exception {
    try (HSSFWorkbook workbook = new HSSFWorkbook()) {
      HSSFRow row = workbook.createSheet().createRow(0);
      row.createCell(0).setCellValue(name);
      row.createCell(1).setCellFormula("LOWER(A1)");
      return workbook
          .getCreationHelper()
          .createFormulaEvaluator()
          .evaluate(row.getCell(1))
          .getStringValue();
    }
  }

  private static RowSetup flagRow(boolean flag) {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBoolean("flag"));
    return new RowSetup(rowMeta, new Object[] {flag});
  }

  private static RowSetup intRow(int a, int b) {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("a"));
    rowMeta.addValueMeta(new ValueMetaInteger("b"));
    return new RowSetup(rowMeta, new Object[] {(long) a, (long) b});
  }

  private static RowSetup nameRow(String name) {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return new RowSetup(rowMeta, new Object[] {name});
  }

  private record RowSetup(RowMeta rowMeta, Object[] row) {}
}
