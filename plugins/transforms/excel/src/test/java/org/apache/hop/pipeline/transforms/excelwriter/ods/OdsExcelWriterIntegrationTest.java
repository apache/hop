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

package org.apache.hop.pipeline.transforms.excelwriter.ods;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.excelwriter.ExcelWriterOutputField;
import org.apache.hop.pipeline.transforms.excelwriter.ExcelWriterOutputFormat;
import org.apache.hop.pipeline.transforms.excelwriter.ExcelWriterTransform;
import org.apache.hop.pipeline.transforms.excelwriter.ExcelWriterTransformData;
import org.apache.hop.pipeline.transforms.excelwriter.ExcelWriterTransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;
import org.odftoolkit.odfdom.doc.table.OdfTable;
import org.odftoolkit.odfdom.doc.table.OdfTableCell;
import org.odftoolkit.odfdom.dom.element.office.OfficeAnnotationElement;
import org.odftoolkit.odfdom.dom.element.text.TextAElement;

/**
 * End-to-end style test exercising the main ODS writer features together in one workflow: file
 * template, header, data types, formula, hyperlink, comment, append, and active sheet.
 */
class OdsExcelWriterIntegrationTest {

  private static final String SHEET_NAME = "Report";

  @TempDir File tempDir;

  private TransformMockHelper<ExcelWriterTransformMeta, ExcelWriterTransformData> mockHelper;
  private ExcelWriterTransform transform;
  private ExcelWriterTransformMeta meta;
  private ExcelWriterTransformData data;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    mockHelper =
        new TransformMockHelper<>(
            "ODS Excel Writer Integration",
            ExcelWriterTransformMeta.class,
            ExcelWriterTransformData.class);
    when(mockHelper.logChannelFactory.create(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);

    meta = new ExcelWriterTransformMeta();
    meta.setDefault();
    meta.getFile().setExtension(ExcelWriterOutputFormat.EXT_ODS);
    meta.getFile().setSheetname(SHEET_NAME);
    meta.setHeaderEnabled(true);
    meta.setMakeSheetActive(true);
    meta.setForceFormulaRecalculation(true);
    meta.getFile().setAutosizecolums(true);

    data = new ExcelWriterTransformData();
    data.realSheetname = SHEET_NAME;
    data.createNewFile = true;
    data.createNewSheet = true;

    data.inputRowMeta = new org.apache.hop.core.row.RowMeta();
    data.inputRowMeta.addValueMeta(new ValueMetaString("label"));
    data.inputRowMeta.addValueMeta(new ValueMetaInteger("amount"));
    data.inputRowMeta.addValueMeta(new ValueMetaString("totalFormula"));
    data.inputRowMeta.addValueMeta(new ValueMetaString("url"));
    data.inputRowMeta.addValueMeta(new ValueMetaString("note"));

    ExcelWriterOutputField labelField = new ExcelWriterOutputField();
    labelField.setName("label");
    ExcelWriterOutputField amountField = new ExcelWriterOutputField();
    amountField.setName("amount");
    ExcelWriterOutputField totalField = new ExcelWriterOutputField();
    totalField.setName("totalFormula");
    totalField.setFormula(true);
    ExcelWriterOutputField urlField = new ExcelWriterOutputField();
    urlField.setName("url");
    urlField.setHyperlinkField("url");
    ExcelWriterOutputField noteField = new ExcelWriterOutputField();
    noteField.setName("note");
    noteField.setCommentField("note");
    noteField.setCommentAuthorField("label");
    meta.setOutputFields(
        java.util.List.of(labelField, amountField, totalField, urlField, noteField));

    data.fieldnrs = new int[] {0, 1, 2, 3, 4};
    data.linkfieldnrs = new int[] {-1, -1, -1, 3, -1};
    data.commentfieldnrs = new int[] {-1, -1, -1, -1, 4};
    data.commentauthorfieldnrs = new int[] {0, -1, -1, -1, -1};

    transform =
        spy(
            new ExcelWriterTransform(
                mockHelper.transformMeta,
                meta,
                data,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));
  }

  @AfterEach
  void cleanUp() {
    mockHelper.cleanUp();
  }

  @Test
  void testFullOdsWorkflow() throws Exception {
    File templateFile = new File(tempDir, "workbook-template.ods");
    try (OdfSpreadsheetDocument template = OdfSpreadsheetDocument.newSpreadsheetDocument()) {
      template.getTableList().get(0).setTableName("Unused");
      template.save(templateFile);
    }

    File outputFile = new File(tempDir, "report.ods");
    doReturn(outputFile.getAbsolutePath()).when(transform).buildFilename(0);
    meta.getTemplate().setTemplateEnabled(true);
    meta.getTemplate().setTemplateFileName(templateFile.getAbsolutePath());
    data.realTemplateFileName = templateFile.getAbsolutePath();

    assertTrue(transform.init());
    Object[] row1 =
        new Object[] {"Alpha", 10L, "=A2+B2", "https://hop.apache.org", "first row comment"};
    Object[] row2 = new Object[] {"Beta", 20L, "=A3+B3", "https://apache.org", "second row"};

    transform.prepareNextOutputFile(row1);
    transform.writeNextLine(data.currentWorkbookDefinition, row1);
    transform.writeNextLine(data.currentWorkbookDefinition, row2);
    transform.closeFiles();

    meta.setAppendLines(true);
    meta.setHeaderEnabled(false);
    meta.getFile().setIfFileExists(ExcelWriterTransformMeta.IF_FILE_EXISTS_REUSE);
    meta.getFile().setIfSheetExists(ExcelWriterTransformMeta.IF_SHEET_EXISTS_REUSE);
    data.createNewFile = false;
    data.createNewSheet = false;
    data.usedFiles.clear();

    transform.prepareNextOutputFile(
        new Object[] {"Gamma", 30L, "=A4+B4", "https://example.com", ""});
    transform.writeNextLine(
        data.currentWorkbookDefinition,
        new Object[] {"Gamma", 30L, "=A4+B4", "https://example.com", ""});
    transform.closeFiles();

    try (OdfSpreadsheetDocument document = OdfSpreadsheetDocument.loadDocument(outputFile)) {
      OdfTable table = document.getTableByName(SHEET_NAME);
      assertNotNull(table);
      assertEquals(SHEET_NAME, OdsTableHelper.getActiveTableName(document));

      assertEquals("label", cellText(table, 0, 0));
      assertEquals("amount", cellText(table, 1, 0));
      assertEquals("Alpha", cellText(table, 0, 1));
      assertEquals("10.0", cellText(table, 1, 1));
      assertEquals("Beta", cellText(table, 0, 2));
      assertEquals("Gamma", cellText(table, 0, 3));

      OdfTableCell formulaCell = table.getCellByPosition(2, 1);
      assertEquals("of:=.[A2]+.[B2]", formulaCell.getFormula());

      TextAElement link =
          (TextAElement)
              table.getCellByPosition(3, 1).getOdfElement().getElementsByTagName("text:a").item(0);
      assertNotNull(link);
      assertEquals("https://hop.apache.org", link.getXlinkHrefAttribute());

      OfficeAnnotationElement annotation =
          (OfficeAnnotationElement)
              table
                  .getCellByPosition(4, 1)
                  .getOdfElement()
                  .getElementsByTagName("office:annotation")
                  .item(0);
      assertNotNull(annotation);
      assertTrue(annotation.getTextContent().contains("first row comment"));

      assertTrue(table.getColumnByIndex(0).isOptimalWidth());
    }
  }

  private String cellText(OdfTable table, int col, int row) throws Exception {
    return table.getCellByPosition(col, row).getDisplayText();
  }
}
