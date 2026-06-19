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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
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
import org.odftoolkit.odfdom.type.Color;

class OdsExcelWriterTest {

  private static final String SHEET_NAME = "Output";

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
            "ODS Excel Writer Test",
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

    data = new ExcelWriterTransformData();
    data.realSheetname = SHEET_NAME;
    data.createNewFile = true;
    data.createNewSheet = true;
    data.inputRowMeta = new org.apache.hop.core.row.RowMeta();
    data.inputRowMeta.addValueMeta(new ValueMetaString("name"));
    data.inputRowMeta.addValueMeta(new ValueMetaInteger("count"));

    ExcelWriterOutputField nameField = new ExcelWriterOutputField();
    nameField.setName("name");
    ExcelWriterOutputField countField = new ExcelWriterOutputField();
    countField.setName("count");
    List<ExcelWriterOutputField> fields = new ArrayList<>();
    fields.add(nameField);
    fields.add(countField);
    meta.setOutputFields(fields);

    data.fieldnrs = new int[] {0, 1};
    data.linkfieldnrs = new int[] {-1, -1};
    data.commentfieldnrs = new int[] {-1, -1};
    data.commentauthorfieldnrs = new int[] {-1, -1};

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
  void testWriteOdsWithHeaderAndData() throws Exception {
    File outputFile = new File(tempDir, "output.ods");
    doReturn(outputFile.getAbsolutePath()).when(transform).buildFilename(0);
    assertTrue(transform.init());

    transform.prepareNextOutputFile(new Object[] {"alpha", 42L});
    transform.writeNextLine(data.currentWorkbookDefinition, new Object[] {"alpha", 42L});
    transform.closeFiles();

    try (OdfSpreadsheetDocument document = OdfSpreadsheetDocument.loadDocument(outputFile)) {
      OdfTable table = document.getTableByName(SHEET_NAME);
      assertEquals("name", getCellText(table, 0, 0));
      assertEquals("count", getCellText(table, 1, 0));
      assertEquals("alpha", getCellText(table, 0, 1));
      assertEquals("42.0", getCellText(table, 1, 1));
    }
  }

  @Test
  void testAppendToExistingOds() throws Exception {
    File outputFile = new File(tempDir, "append.ods");
    doReturn(outputFile.getAbsolutePath()).when(transform).buildFilename(0);
    assertTrue(transform.init());

    transform.prepareNextOutputFile(new Object[] {"first", 1L});
    transform.writeNextLine(data.currentWorkbookDefinition, new Object[] {"first", 1L});
    transform.closeFiles();

    meta.setAppendLines(true);
    meta.getFile().setIfFileExists(ExcelWriterTransformMeta.IF_FILE_EXISTS_REUSE);
    meta.getFile().setIfSheetExists(ExcelWriterTransformMeta.IF_SHEET_EXISTS_REUSE);
    meta.setHeaderEnabled(false);
    data.createNewFile = false;
    data.createNewSheet = false;
    data.usedFiles.clear();

    transform.prepareNextOutputFile(new Object[] {"second", 2L});
    transform.writeNextLine(data.currentWorkbookDefinition, new Object[] {"second", 2L});
    transform.closeFiles();

    try (OdfSpreadsheetDocument document = OdfSpreadsheetDocument.loadDocument(outputFile)) {
      OdfTable table = document.getTableByName(SHEET_NAME);
      assertEquals("name", getCellText(table, 0, 0));
      assertEquals("first", getCellText(table, 0, 1));
      assertEquals("second", getCellText(table, 0, 2));
    }
  }

  @Test
  void testStyleCellReference() throws Exception {
    File templateFile = new File(tempDir, "styled-template.ods");
    try (OdfSpreadsheetDocument template = OdfSpreadsheetDocument.newSpreadsheetDocument()) {
      OdfTable templateTable = template.getTableList().get(0);
      templateTable.setTableName(SHEET_NAME);
      OdfTableCell styleSource = templateTable.getCellByPosition(1, 0);
      styleSource.setCellBackgroundColor(Color.RED);
      styleSource.setStringValue("style-source");
      template.save(templateFile);
    }

    File outputFile = new File(tempDir, "styled-output.ods");
    doReturn(outputFile.getAbsolutePath()).when(transform).buildFilename(0);
    meta.setHeaderEnabled(false);
    meta.getTemplate().setTemplateEnabled(true);
    meta.getTemplate().setTemplateFileName(templateFile.getAbsolutePath());
    data.realTemplateFileName = templateFile.getAbsolutePath();
    meta.getOutputFields().get(0).setStyleCell("B1");

    assertTrue(transform.init());
    transform.prepareNextOutputFile(new Object[] {"styled-value", 1L});
    transform.writeNextLine(data.currentWorkbookDefinition, new Object[] {"styled-value", 1L});
    transform.closeFiles();

    try (OdfSpreadsheetDocument document = OdfSpreadsheetDocument.loadDocument(outputFile)) {
      OdfTable table = document.getTableByName(SHEET_NAME);
      OdfTableCell writtenCell = table.getCellByPosition(0, 0);
      OdfTableCell referenceCell = table.getCellByPosition(1, 0);
      assertEquals("styled-value", writtenCell.getDisplayText());
      assertEquals(referenceCell.getStyleName(), writtenCell.getStyleName());
      assertNotNull(writtenCell.getStyleName());
    }
  }

  @Test
  void testHyperlinkAndComment() throws Exception {
    File outputFile = new File(tempDir, "link-comment.ods");
    doReturn(outputFile.getAbsolutePath()).when(transform).buildFilename(0);
    meta.setHeaderEnabled(false);

    data.inputRowMeta = new org.apache.hop.core.row.RowMeta();
    data.inputRowMeta.addValueMeta(new ValueMetaString("label"));
    data.inputRowMeta.addValueMeta(new ValueMetaString("url"));
    data.inputRowMeta.addValueMeta(new ValueMetaString("note"));
    data.inputRowMeta.addValueMeta(new ValueMetaString("author"));

    ExcelWriterOutputField labelField = new ExcelWriterOutputField();
    labelField.setName("label");
    labelField.setHyperlinkField("url");
    labelField.setCommentField("note");
    labelField.setCommentAuthorField("author");
    meta.setOutputFields(List.of(labelField));

    data.fieldnrs = new int[] {0};
    data.linkfieldnrs = new int[] {1};
    data.commentfieldnrs = new int[] {2};
    data.commentauthorfieldnrs = new int[] {3};

    assertTrue(transform.init());
    transform.prepareNextOutputFile(
        new Object[] {"Hop", "https://hop.apache.org", "A note", "Tester"});
    transform.writeNextLine(
        data.currentWorkbookDefinition,
        new Object[] {"Hop", "https://hop.apache.org", "A note", "Tester"});
    transform.closeFiles();

    try (OdfSpreadsheetDocument document = OdfSpreadsheetDocument.loadDocument(outputFile)) {
      OdfTable table = document.getTableByName(SHEET_NAME);
      OdfTableCell cell = table.getCellByPosition(0, 0);
      TextAElement link =
          (TextAElement) cell.getOdfElement().getElementsByTagName("text:a").item(0);
      assertNotNull(link);
      assertEquals("Hop", link.getTextContent());
      assertEquals("https://hop.apache.org", link.getXlinkHrefAttribute());

      OfficeAnnotationElement annotation =
          (OfficeAnnotationElement)
              cell.getOdfElement().getElementsByTagName("office:annotation").item(0);
      assertNotNull(annotation);
      assertFalse(annotation.getTextContent().isBlank());
    }
  }

  @Test
  void testFormatMask() throws Exception {
    File outputFile = new File(tempDir, "format.ods");
    doReturn(outputFile.getAbsolutePath()).when(transform).buildFilename(0);
    meta.setHeaderEnabled(false);

    data.inputRowMeta = new org.apache.hop.core.row.RowMeta();
    data.inputRowMeta.addValueMeta(new ValueMetaNumber("amount"));

    ExcelWriterOutputField amountField = new ExcelWriterOutputField();
    amountField.setName("amount");
    amountField.setFormat("0.00");
    meta.setOutputFields(List.of(amountField));
    data.fieldnrs = new int[] {0};
    data.linkfieldnrs = new int[] {-1};
    data.commentfieldnrs = new int[] {-1};
    data.commentauthorfieldnrs = new int[] {-1};

    assertTrue(transform.init());
    transform.prepareNextOutputFile(new Object[] {12.3});
    transform.writeNextLine(data.currentWorkbookDefinition, new Object[] {12.3});
    transform.closeFiles();

    try (OdfSpreadsheetDocument document = OdfSpreadsheetDocument.loadDocument(outputFile)) {
      OdfTable table = document.getTableByName(SHEET_NAME);
      OdfTableCell cell = table.getCellByPosition(0, 0);
      assertEquals("12.30", cell.getDisplayText());
    }
  }

  @Test
  void testFormulaField() throws Exception {
    File outputFile = new File(tempDir, "formula.ods");
    doReturn(outputFile.getAbsolutePath()).when(transform).buildFilename(0);
    meta.setHeaderEnabled(false);
    meta.setForceFormulaRecalculation(true);

    data.inputRowMeta = new org.apache.hop.core.row.RowMeta();
    data.inputRowMeta.addValueMeta(new ValueMetaNumber("left"));
    data.inputRowMeta.addValueMeta(new ValueMetaNumber("right"));
    data.inputRowMeta.addValueMeta(new ValueMetaString("totalFormula"));

    ExcelWriterOutputField leftField = new ExcelWriterOutputField();
    leftField.setName("left");
    ExcelWriterOutputField rightField = new ExcelWriterOutputField();
    rightField.setName("right");
    ExcelWriterOutputField totalField = new ExcelWriterOutputField();
    totalField.setName("totalFormula");
    totalField.setFormula(true);
    meta.setOutputFields(List.of(leftField, rightField, totalField));

    data.fieldnrs = new int[] {0, 1, 2};
    data.linkfieldnrs = new int[] {-1, -1, -1};
    data.commentfieldnrs = new int[] {-1, -1, -1};
    data.commentauthorfieldnrs = new int[] {-1, -1, -1};

    assertTrue(transform.init());
    transform.prepareNextOutputFile(new Object[] {10.0, 20.0, "=A1+B1"});
    transform.writeNextLine(data.currentWorkbookDefinition, new Object[] {10.0, 20.0, "=A1+B1"});
    transform.closeFiles();

    try (OdfSpreadsheetDocument document = OdfSpreadsheetDocument.loadDocument(outputFile)) {
      OdfTable table = document.getTableByName(SHEET_NAME);
      OdfTableCell formulaCell = table.getCellByPosition(2, 0);
      assertEquals("of:=.[A1]+.[B1]", formulaCell.getFormula());
      assertNull(formulaCell.getOdfElement().getOfficeValueAttribute());
    }
  }

  @Test
  void testPushDownExistingCells() throws Exception {
    File outputFile = new File(tempDir, "pushdown.ods");
    doReturn(outputFile.getAbsolutePath()).when(transform).buildFilename(0);
    meta.setHeaderEnabled(false);
    assertTrue(transform.init());

    transform.prepareNextOutputFile(new Object[] {"existing", 1L});
    transform.writeNextLine(data.currentWorkbookDefinition, new Object[] {"existing", 1L});
    transform.closeFiles();

    meta.setHeaderEnabled(false);
    meta.setRowWritingMethod(ExcelWriterTransformMeta.ROW_WRITE_PUSH_DOWN);
    meta.getFile().setIfFileExists(ExcelWriterTransformMeta.IF_FILE_EXISTS_REUSE);
    meta.getFile().setIfSheetExists(ExcelWriterTransformMeta.IF_SHEET_EXISTS_REUSE);
    data.createNewFile = false;
    data.createNewSheet = false;
    data.shiftExistingCells = true;
    data.usedFiles.clear();

    transform.prepareNextOutputFile(new Object[] {"inserted", 2L});
    transform.writeNextLine(data.currentWorkbookDefinition, new Object[] {"inserted", 2L});
    transform.closeFiles();

    try (OdfSpreadsheetDocument document = OdfSpreadsheetDocument.loadDocument(outputFile)) {
      OdfTable table = document.getTableByName(SHEET_NAME);
      assertEquals("inserted", getCellText(table, 0, 0));
      assertEquals("existing", getCellText(table, 0, 1));
    }
  }

  @Test
  void testTemplateSheetCloneWithPushDown() throws Exception {
    File templateFile = new File(tempDir, "sheet-template.ods");
    String templateSheetName = "TemplateSheet";
    try (OdfSpreadsheetDocument template = OdfSpreadsheetDocument.newSpreadsheetDocument()) {
      OdfTable templateTable = template.getTableList().get(0);
      templateTable.setTableName(templateSheetName);
      templateTable.getCellByPosition(0, 0).setStringValue("header");
      templateTable.getCellByPosition(0, 1).setStringValue("footer");
      template.save(templateFile);
    }

    File outputFile = new File(tempDir, "cloned-sheet.ods");
    doReturn(outputFile.getAbsolutePath()).when(transform).buildFilename(0);
    meta.setHeaderEnabled(false);
    meta.setRowWritingMethod(ExcelWriterTransformMeta.ROW_WRITE_PUSH_DOWN);
    meta.getTemplate().setTemplateEnabled(true);
    meta.getTemplate().setTemplateFileName(templateFile.getAbsolutePath());
    meta.getTemplate().setTemplateSheetEnabled(true);
    meta.getTemplate().setTemplateSheetHidden(true);
    meta.getTemplate().setTemplateSheetName(templateSheetName);
    data.realTemplateFileName = templateFile.getAbsolutePath();
    data.realTemplateSheetName = templateSheetName;

    assertTrue(transform.init());
    data.shiftExistingCells = true;
    transform.prepareNextOutputFile(new Object[] {"row-data", 5L});
    transform.writeNextLine(data.currentWorkbookDefinition, new Object[] {"row-data", 5L});
    transform.closeFiles();

    try (OdfSpreadsheetDocument document = OdfSpreadsheetDocument.loadDocument(outputFile)) {
      OdfTable outputTable = document.getTableByName(SHEET_NAME);
      assertNotNull(outputTable);
      assertEquals("row-data", getCellText(outputTable, 0, 0));
      assertEquals("header", getCellText(outputTable, 0, 1));
      assertEquals("footer", getCellText(outputTable, 0, 2));

      OdfTable hiddenTemplate = document.getTableByName(templateSheetName);
      assertNotNull(hiddenTemplate);
      assertEquals(
          "collapse",
          hiddenTemplate
              .getOdfElement()
              .getAttributeNS(
                  org.odftoolkit.odfdom.dom.OdfDocumentNamespace.TABLE.getUri(), "visibility"));
    }
  }

  @Test
  void testMakeActiveSheet() throws Exception {
    File outputFile = new File(tempDir, "active.ods");
    try (OdfSpreadsheetDocument seed = OdfSpreadsheetDocument.newSpreadsheetDocument()) {
      seed.getTableList().get(0).setTableName("Other");
      OdfTable outputSeed = org.odftoolkit.odfdom.doc.table.OdfTable.newTable(seed, 1, 1);
      outputSeed.setTableName(SHEET_NAME);
      seed.save(outputFile);
    }

    doReturn(outputFile.getAbsolutePath()).when(transform).buildFilename(0);
    meta.setHeaderEnabled(false);
    meta.setMakeSheetActive(true);
    meta.getFile().setIfFileExists(ExcelWriterTransformMeta.IF_FILE_EXISTS_REUSE);
    meta.getFile().setIfSheetExists(ExcelWriterTransformMeta.IF_SHEET_EXISTS_REUSE);
    data.createNewFile = false;
    data.createNewSheet = false;

    assertTrue(transform.init());
    transform.prepareNextOutputFile(new Object[] {"active", 1L});
    transform.writeNextLine(data.currentWorkbookDefinition, new Object[] {"active", 1L});
    transform.closeFiles();

    try (OdfSpreadsheetDocument document = OdfSpreadsheetDocument.loadDocument(outputFile)) {
      assertEquals(SHEET_NAME, OdsTableHelper.getActiveTableName(document));
    }
  }

  @Test
  void testAutoSizeColumns() throws Exception {
    File outputFile = new File(tempDir, "autosize.ods");
    doReturn(outputFile.getAbsolutePath()).when(transform).buildFilename(0);
    meta.setHeaderEnabled(false);
    meta.getFile().setAutosizecolums(true);

    assertTrue(transform.init());
    transform.prepareNextOutputFile(new Object[] {"short", 1L});
    transform.writeNextLine(data.currentWorkbookDefinition, new Object[] {"much-longer-value", 2L});
    transform.closeFiles();

    try (OdfSpreadsheetDocument document = OdfSpreadsheetDocument.loadDocument(outputFile)) {
      OdfTable table = document.getTableByName(SHEET_NAME);
      assertTrue(table.getColumnByIndex(0).isOptimalWidth());
      assertTrue(table.getColumnByIndex(1).isOptimalWidth());
    }
  }

  @Test
  void testProtectSheet() throws Exception {
    File outputFile = new File(tempDir, "protected.ods");
    doReturn(outputFile.getAbsolutePath()).when(transform).buildFilename(0);
    meta.setHeaderEnabled(false);
    meta.getFile().setProtectsheet(true);
    meta.getFile().setPassword("secret");

    assertTrue(transform.init());
    transform.prepareNextOutputFile(new Object[] {"locked", 1L});
    transform.writeNextLine(data.currentWorkbookDefinition, new Object[] {"locked", 1L});
    transform.closeFiles();

    try (OdfSpreadsheetDocument document = OdfSpreadsheetDocument.loadDocument(outputFile)) {
      OdfTable table = document.getTableByName(SHEET_NAME);
      assertTrue(table.getOdfElement().getTableProtectedAttribute());
      assertEquals(
          "5en6G6MezRroT3XKqkdPOmY/BfQ=", table.getOdfElement().getTableProtectionKeyAttribute());
    }
  }

  private String getCellText(OdfTable table, int col, int row) throws Exception {
    OdfTableCell cell = table.getCellByPosition(col, row);
    return cell.getDisplayText();
  }
}
