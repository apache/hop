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
package org.apache.hop.pipeline.transforms.formula;

import java.io.IOException;
import java.util.function.Consumer;
import org.apache.poi.hssf.usermodel.HSSFFormulaEvaluator;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFFormulaEvaluator;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

// very thin "factory abstraction" (mainly accessors) on top of HSS/XSS workbooks
// the diff in perf is literally x5 so if we can use HSS it is a win for end users
// but it is limited in number of columns
public class FormulaPoi {
  private final Consumer<String> log;
  private XSS slow;
  private HSS fast;

  public FormulaPoi(final Consumer<String> log) {
    this.log = log;
  }

  public Evaluator evaluator(final int columns) {
    return columns <= 256
        ? (fast == null ? fast = new HSS() : fast).incr()
        : (slow == null ? slow = new XSS() : slow);
  }

  public void reset() {
    if (fast != null) { // force a reset after a chunk to enable to control the memory this way too
      fast.currentCount = HSS.RESET_INTERVAL;
      fast.incr();
    }
  }

  public void destroy() throws IOException {
    if (slow != null) {
      slow.workBook.close();
    }
    if (fast != null) {
      fast.workBook.close();
    }
  }

  public interface Evaluator {
    Sheet sheet();

    Row row();

    void row(Row row);

    FormulaEvaluator evaluator();
  }

  private class XSS implements Evaluator {
    private final XSSFWorkbook workBook;
    private final XSSFSheet workSheet;
    private XSSFRow sheetRow;
    private final XSSFFormulaEvaluator evaluator;

    private XSS() {
      log.accept("using xss implementation which is slow");
      workBook = new XSSFWorkbook();
      workBook.setCellFormulaValidation(false); // we parse it anyway so no need to do it twice
      workSheet = workBook.createSheet();
      sheetRow = workSheet.createRow(0);
      evaluator = sheetRow.getSheet().getWorkbook().getCreationHelper().createFormulaEvaluator();
    }

    @Override
    public Sheet sheet() {
      return workSheet;
    }

    @Override
    public Row row() {
      return sheetRow;
    }

    @Override
    public void row(final Row row) {
      sheetRow = (XSSFRow) row;
    }

    @Override
    public FormulaEvaluator evaluator() {
      return evaluator;
    }
  }

  // HSS is fast compared to XSS but it stores unicodestrings
  // in the internalworkbook of the workbook
  // so we must reset it from time to time
  private static class HSS implements Evaluator {
    private static final int RESET_INTERVAL =
        Integer.getInteger(HSS.class.getName().replace('$', '.') + ".resetInterval", 10_000);

    private HSSFWorkbook workBook;
    private HSSFSheet workSheet;
    private HSSFRow sheetRow;
    private HSSFFormulaEvaluator evaluator;

    private int currentCount = RESET_INTERVAL;

    private HSS() {
      incr();
    }

    @Override
    public Sheet sheet() {
      return workSheet;
    }

    @Override
    public Row row() {
      return sheetRow;
    }

    @Override
    public void row(final Row row) {
      sheetRow = (HSSFRow) row;
    }

    @Override
    public FormulaEvaluator evaluator() {
      return evaluator;
    }

    public Evaluator incr() {
      if (currentCount++ == RESET_INTERVAL) {
        workBook = new HSSFWorkbook();
        workSheet = workBook.createSheet();
        sheetRow = workSheet.createRow(0);
        evaluator = sheetRow.getSheet().getWorkbook().getCreationHelper().createFormulaEvaluator();
        currentCount = 0;
      }
      return this;
    }
  }
}
