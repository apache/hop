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

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Calendar;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.io.CountingOutputStream;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.excelwriter.ExcelWriterOutputField;
import org.apache.hop.pipeline.transforms.excelwriter.ExcelWriterTransform;
import org.apache.hop.pipeline.transforms.excelwriter.ExcelWriterTransformData;
import org.apache.hop.pipeline.transforms.excelwriter.ExcelWriterTransformMeta;
import org.apache.hop.pipeline.transforms.excelwriter.ExcelWriterWorkbookDefinition;
import org.apache.poi.ss.util.CellReference;
import org.odftoolkit.odfdom.doc.OdfSpreadsheetDocument;
import org.odftoolkit.odfdom.doc.table.OdfTable;
import org.odftoolkit.odfdom.doc.table.OdfTableCell;
import org.odftoolkit.odfdom.dom.element.table.TableTableElement;
import org.w3c.dom.Node;

/**
 * ODS output backend for {@link ExcelWriterTransform}. Supports basic cell values, styling,
 * hyperlinks, comments, formulas, headers, footers, append, templates, starting cell, sheet clone,
 * row push-down, auto-size columns, and active sheet selection.
 */
public class OdsExcelWriter {

  private static final Class<?> PKG = ExcelWriterTransformMeta.class;

  private final ExcelWriterTransform transform;
  private final ExcelWriterTransformMeta meta;
  private final ExcelWriterTransformData data;

  public OdsExcelWriter(ExcelWriterTransform transform) {
    this.transform = transform;
    this.meta = transform.getMeta();
    this.data = transform.getData();
  }

  public void prepareNextOutputFile(Object[] row) throws HopException {
    try {
      if (data.isBeamContext() && meta.getFile().isFileNameInField()) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "ExcelWriterTransform.Exception.FilenameFromFieldNotSupportedInBeam"));
      }

      int numOfFields = !Utils.isEmpty(meta.getOutputFields()) ? meta.getOutputFields().size() : 0;
      if (numOfFields == 0) {
        numOfFields = data.inputRowMeta != null ? data.inputRowMeta.size() : 0;
      }

      int splitNr = 0;
      if (!meta.getFile().isFileNameInField()) {
        splitNr = transform.getNextSplitNr(meta.getFile().getFileName());
      }

      FileObject file = getFileLocation(row);

      if (!file.getParent().exists() && meta.getFile().isCreateParentFolder()) {
        transform.createParentFolder(file);
      }

      if (transform.isDebug()) {
        transform.logDebug(
            BaseMessages.getString(
                PKG, "ExcelWriterTransform.Log.OpeningFile", file.getName().toString()));
      }

      if (file.exists() && data.createNewFile && !file.delete()) {
        if (transform.isBasic()) {
          transform.logBasic(
              BaseMessages.getString(
                  PKG,
                  "ExcelWriterTransform.Log.CouldNotDeleteStaleFile",
                  file.getName().toString()));
        }
        transform.setErrors(1);
        throw new HopException("Could not delete stale file " + file.getName().toString());
      }

      if (meta.isAddToResultFilenames()) {
        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                file,
                transform.getPipelineMeta().getName(),
                transform.getTransformName());
        resultFile.setComment(
            "This file was created with an Excel writer transform by Hop : The Hop Orchestration Platform");
        transform.addResultFile(resultFile);
      }

      boolean appendingToSheet = true;
      if (!file.exists()) {
        if (meta.getTemplate().isTemplateEnabled()) {
          ensureTemplateExtensionMatches();
          FileObject templateFile = HopVfs.getFileObject(data.realTemplateFileName, transform);
          if (templateFile.exists()) {
            ExcelWriterTransform.copyFile(templateFile, file);
          } else {
            if (transform.isBasic()) {
              transform.logBasic(
                  BaseMessages.getString(
                      PKG, "ExcelWriterTransform.Log.TemplateMissing", data.realTemplateFileName));
            }
            transform.setErrors(1);
            throw new HopException("Template file missing: " + data.realTemplateFileName);
          }
        } else {
          createEmptyOdsFile(file);
        }
        appendingToSheet = false;
      }

      OdfSpreadsheetDocument document;
      try (InputStream inputStream = HopVfs.getInputStream(HopVfs.getFilename(file), transform)) {
        document = OdfSpreadsheetDocument.loadDocument(inputStream);
      }

      ResolvedTable resolved = resolveTable(document);
      OdfTable table = resolved.table();
      if (resolved.newlyCreated()) {
        appendingToSheet = false;
      }

      if (!Utils.isEmpty(data.realStartingCell)) {
        CellReference cellRef = new CellReference(data.realStartingCell);
        data.startingRow = cellRef.getRow();
        data.startingCol = cellRef.getCol();
      } else {
        data.startingRow = 0;
        data.startingCol = 0;
      }

      int posX = data.startingCol;
      int posY = data.startingRow;

      if (!data.createNewSheet && meta.isAppendLines() && appendingToSheet) {
        posY = findLastUsedRow(table) + 1;
      }

      if (!data.createNewSheet && meta.getAppendOffset() != 0 && appendingToSheet) {
        posY += meta.getAppendOffset();
      }

      if (!data.createNewSheet && meta.getAppendEmpty() > 0 && appendingToSheet) {
        for (int i = 0; i < meta.getAppendEmpty(); i++) {
          openLine(table, posY);
          if (!data.shiftExistingCells || meta.isAppendLines()) {
            posY++;
          }
        }
      }

      if (meta.getFile().isProtectsheet()) {
        OdsTableHelper.protectTable(table, data.realPassword);
      }

      String baseFileName =
          !meta.getFile().isFileNameInField()
              ? meta.getFile().getFileName()
              : file.getName().toString();

      int startY =
          !Utils.isEmpty(data.realStartingCell) ? posY : Math.max(posY, findLastUsedRow(table));

      ExcelWriterWorkbookDefinition workbookDefinition =
          prepareWorkbookDefinition(
              numOfFields, splitNr, file, document, table, posX, baseFileName, startY);

      if (meta.isHeaderEnabled()
          && !(!data.createNewSheet && meta.isAppendOmitHeader() && appendingToSheet)) {
        writeHeader(workbookDefinition, posX, posY);
      }

      if (transform.isDebug()) {
        transform.logDebug(
            BaseMessages.getString(
                PKG, "ExcelWriterTransform.Log.FileOpened", file.getName().toString()));
      }
    } catch (Exception e) {
      transform.logError("Error opening new ODS file", e);
      transform.setErrors(1);
      throw new HopException("Error opening new ODS file", e);
    }
  }

  public void writeNextLine(ExcelWriterWorkbookDefinition workbookDefinition, Object[] row)
      throws HopException {
    try {
      OdfTable table = workbookDefinition.getOdsWorkbookHandle().getTable();
      openLine(table, workbookDefinition.getPosY());
      int rowIndex = workbookDefinition.getPosY();

      if (Utils.isEmpty(meta.getOutputFields())) {
        int nr = data.inputRowMeta.size();
        int x = workbookDefinition.getPosX();
        for (int i = 0; i < nr; i++) {
          writeField(
              workbookDefinition,
              table,
              rowIndex,
              x++,
              row[i],
              data.inputRowMeta.getValueMeta(i),
              null,
              row,
              i,
              false);
        }
        workbookDefinition.setPosX(data.startingCol);
        workbookDefinition.incrementY();
      } else {
        int x = workbookDefinition.getPosX();
        for (int i = 0; i < meta.getOutputFields().size(); i++) {
          ExcelWriterOutputField field = meta.getOutputFields().get(i);
          writeField(
              workbookDefinition,
              table,
              rowIndex,
              x++,
              row[data.fieldnrs[i]],
              data.inputRowMeta.getValueMeta(data.fieldnrs[i]),
              field,
              row,
              i,
              false);
        }
        workbookDefinition.setPosX(data.startingCol);
        workbookDefinition.incrementY();
      }
    } catch (Exception e) {
      transform.logError("Error writing ODS line: " + e);
      throw new HopException(e);
    }
  }

  public void closeOutputFile(ExcelWriterWorkbookDefinition fileDefinition) throws HopException {
    OutputStream out = null;
    CountingOutputStream countingOut = null;
    try {
      countingOut =
          new CountingOutputStream(HopVfs.getOutputStream(fileDefinition.getFile(), false));
      out = new BufferedOutputStream(countingOut);

      if (meta.isFooterEnabled()) {
        writeHeader(fileDefinition, fileDefinition.getPosX(), fileDefinition.getPosY());
      }

      OdfSpreadsheetDocument document = fileDefinition.getOdsWorkbookHandle().getDocument();
      OdfTable table = fileDefinition.getOdsWorkbookHandle().getTable();
      if (meta.getFile().isAutosizecolums()) {
        int columnCount =
            !Utils.isEmpty(meta.getOutputFields())
                ? meta.getOutputFields().size()
                : (data.inputRowMeta != null ? data.inputRowMeta.size() : 0);
        OdsTableHelper.autoSizeColumns(table, data.startingCol, columnCount);
      }
      if (meta.isForceFormulaRecalculation()) {
        OdsFormulaHelper.prepareForRecalculation(document);
      }
      document.save(out);
      document.close();
    } catch (Exception e) {
      throw new HopException(e);
    } finally {
      if (out != null) {
        try {
          out.flush();
          if (countingOut != null) {
            long written = countingOut.getCount();
            transform.recordBytesWritten(written, fileDefinition.getFile());
          }
          out.close();
        } catch (Exception e) {
          throw new HopException("Error closing ODS file " + fileDefinition.getFile(), e);
        }
      }
    }
  }

  private void writeHeader(ExcelWriterWorkbookDefinition workbookDefinition, int posX, int posY)
      throws HopException {
    try {
      OdfTable table = workbookDefinition.getOdsWorkbookHandle().getTable();
      openLine(table, posY);
      int x = posX;
      if (!Utils.isEmpty(meta.getOutputFields())) {
        for (int i = 0; i < meta.getOutputFields().size(); i++) {
          ExcelWriterOutputField field = meta.getOutputFields().get(i);
          String fieldName = !Utils.isEmpty(field.getTitle()) ? field.getTitle() : field.getName();
          writeField(
              workbookDefinition,
              table,
              posY,
              x++,
              fieldName,
              new ValueMetaString(fieldName),
              field,
              null,
              i,
              true);
        }
      } else if (data.inputRowMeta != null) {
        for (int i = 0; i < data.inputRowMeta.size(); i++) {
          String fieldName = data.inputRowMeta.getFieldNames()[i];
          writeField(
              workbookDefinition,
              table,
              posY,
              x++,
              fieldName,
              new ValueMetaString(fieldName),
              null,
              null,
              -1,
              true);
        }
      }
      workbookDefinition.setPosY(posY + 1);
      transform.incrementLinesOutput();
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  private void writeField(
      ExcelWriterWorkbookDefinition workbookDefinition,
      OdfTable table,
      int rowIndex,
      int colIndex,
      Object value,
      IValueMeta vMeta,
      ExcelWriterOutputField excelField,
      Object[] row,
      int fieldNr,
      boolean isTitle)
      throws Exception {
    OdfSpreadsheetDocument document = workbookDefinition.getOdsWorkbookHandle().getDocument();
    OdfTableCell cell = table.getCellByPosition(colIndex, rowIndex);
    boolean cellExisted = OdsStyleHelper.cellHadContent(cell);

    if (!(cellExisted && meta.isLeaveExistingStylesUnchanged())) {
      if (!isTitle
          && fieldNr >= 0
          && !Utils.isEmpty(workbookDefinition.getCachedOdsStyle(fieldNr))) {
        applyStyleName(cell, workbookDefinition.getCachedOdsStyle(fieldNr));
      } else {
        if (excelField != null) {
          String styleRef = null;
          if (!isTitle && !Utils.isEmpty(excelField.getStyleCell())) {
            styleRef = excelField.getStyleCell();
          } else if (isTitle && !Utils.isEmpty(excelField.getTitleStyleCell())) {
            styleRef = excelField.getTitleStyleCell();
          }
          if (styleRef != null) {
            OdfTableCell styleCell = OdsStyleHelper.getCellFromReference(document, table, styleRef);
            if (styleCell != null && styleCell != cell) {
              OdsStyleHelper.copyStyle(cell, styleCell);
            }
          }
        }

        if (!isTitle && fieldNr >= 0) {
          workbookDefinition.cacheOdsStyle(fieldNr, cell.getStyleName());
        }
      }
    }

    boolean isFormulaField = !isTitle && excelField != null && excelField.isFormula();

    if (!isFormulaField) {
      setCellValue(cell, value, vMeta);
    }

    if (!(cellExisted && meta.isLeaveExistingStylesUnchanged())) {
      if (!isTitle
          && excelField != null
          && !Utils.isEmpty(excelField.getFormat())
          && !excelField.getFormat().startsWith("Image")) {
        OdsStyleHelper.applyFormat(cell, excelField.getFormat());
      } else if (!isTitle
          && excelField != null
          && Utils.isEmpty(excelField.getFormat())
          && (vMeta.getType() == IValueMeta.TYPE_DATE
              || vMeta.getType() == IValueMeta.TYPE_TIMESTAMP)) {
        String format = vMeta.getFormatMask();
        if (!Utils.isEmpty(format)) {
          OdsStyleHelper.applyFormat(cell, format);
        }
      }
    }

    if (!isTitle && excelField != null && fieldNr >= 0 && data.linkfieldnrs[fieldNr] >= 0) {
      String link =
          data.inputRowMeta
              .getValueMeta(data.linkfieldnrs[fieldNr])
              .getString(row[data.linkfieldnrs[fieldNr]]);
      if (!Utils.isEmpty(link)) {
        String displayText = value != null ? vMeta.getString(value) : "";
        if (!(cellExisted && meta.isLeaveExistingStylesUnchanged())) {
          if (!Utils.isEmpty(workbookDefinition.getCachedOdsLinkStyle(fieldNr))) {
            applyStyleName(cell, workbookDefinition.getCachedOdsLinkStyle(fieldNr));
          }
        }
        OdsStyleHelper.applyHyperlink(document, table, cell, link, displayText);
        if (!(cellExisted && meta.isLeaveExistingStylesUnchanged()) && fieldNr >= 0) {
          workbookDefinition.cacheOdsLinkStyle(fieldNr, cell.getStyleName());
        }
      }
    }

    if (!isTitle && excelField != null && fieldNr >= 0 && data.commentfieldnrs[fieldNr] >= 0) {
      String comment =
          data.inputRowMeta
              .getValueMeta(data.commentfieldnrs[fieldNr])
              .getString(row[data.commentfieldnrs[fieldNr]]);
      if (!Utils.isEmpty(comment)) {
        String author =
            data.commentauthorfieldnrs[fieldNr] >= 0
                ? data.inputRowMeta
                    .getValueMeta(data.commentauthorfieldnrs[fieldNr])
                    .getString(row[data.commentauthorfieldnrs[fieldNr]])
                : "Apache Hop";
        OdsStyleHelper.applyComment(cell, author, comment);
      }
    }

    if (isFormulaField && value != null) {
      OdsFormulaHelper.applyFormula(cell, vMeta.getString(value));
    }
  }

  private void applyStyleName(OdfTableCell cell, String styleName) {
    if (cell != null && !Utils.isEmpty(styleName)) {
      ((org.odftoolkit.odfdom.dom.element.OdfStylableElement) cell.getOdfElement())
          .setStyleName(styleName);
    }
  }

  private void setCellValue(OdfTableCell cell, Object value, IValueMeta vMeta) throws Exception {
    if (value == null) {
      return;
    }
    switch (vMeta.getType()) {
      case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP -> {
        if (vMeta.getDate(value) != null) {
          Calendar calendar = Calendar.getInstance();
          calendar.setTime(vMeta.getDate(value));
          cell.setDateValue(calendar);
        }
      }
      case IValueMeta.TYPE_BOOLEAN -> cell.setBooleanValue(vMeta.getBoolean(value));
      case IValueMeta.TYPE_BIGNUMBER, IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_INTEGER ->
          cell.setDoubleValue(vMeta.getNumber(value));
      default -> cell.setStringValue(vMeta.getString(value));
    }
  }

  private void createEmptyOdsFile(FileObject file) throws Exception {
    OdfSpreadsheetDocument document = OdfSpreadsheetDocument.newSpreadsheetDocument();
    try {
      OdfTable table = document.getTableByName(data.realSheetname);
      if (table == null) {
        if (!document.getTableList().isEmpty()) {
          table = document.getTableList().get(0);
          table.setTableName(data.realSheetname);
        } else {
          table = OdfTable.newTable(document, 1, 1);
          table.setTableName(data.realSheetname);
        }
      }
      try (OutputStream out = HopVfs.getOutputStream(file, false)) {
        document.save(out);
      }
    } finally {
      document.close();
    }
  }

  private void openLine(OdfTable table, int rowIndex) {
    if (data.shiftExistingCells) {
      OdsTableHelper.shiftRowsDown(table, rowIndex);
    }
  }

  private ResolvedTable resolveTable(OdfSpreadsheetDocument document) throws Exception {
    String existingActiveTable = OdsTableHelper.getActiveTableName(document);
    int replacingTableAt = -1;
    boolean newlyCreated = false;

    OdfTable table = document.getTableByName(data.realSheetname);
    if (table != null && data.createNewSheet) {
      replacingTableAt = OdsTableHelper.getTableIndex(document, data.realSheetname);
      removeTable(document, table);
      table = null;
    }

    if (table == null) {
      if (meta.getTemplate().isTemplateSheetEnabled()) {
        OdfTable templateTable = document.getTableByName(data.realTemplateSheetName);
        if (templateTable == null) {
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "ExcelWriterTransform.Exception.TemplateNotFound",
                  data.realTemplateSheetName));
        }
        table = OdsTableHelper.cloneTable(document, templateTable, data.realSheetname);
        if (meta.getTemplate().isTemplateSheetHidden()) {
          OdsTableHelper.setTableVisible(templateTable, false);
        }
      } else {
        table = OdfTable.newTable(document, 1, 1);
        table.setTableName(data.realSheetname);
      }
      newlyCreated = true;
      if (replacingTableAt > -1) {
        OdsTableHelper.moveTableToIndex(document, table, replacingTableAt);
      }
      if (!Utils.isEmpty(existingActiveTable) && !meta.isMakeSheetActive()) {
        OdsTableHelper.setActiveTableName(document, existingActiveTable);
      }
    }

    if (meta.isMakeSheetActive()) {
      OdsTableHelper.setActiveTableName(document, data.realSheetname);
    }
    return new ResolvedTable(table, newlyCreated);
  }

  private record ResolvedTable(OdfTable table, boolean newlyCreated) {}

  private void removeTable(OdfSpreadsheetDocument document, OdfTable table) {
    TableTableElement tableElement = table.getOdfElement();
    Node parent = tableElement.getParentNode();
    if (parent != null) {
      parent.removeChild(tableElement);
    }
  }

  private int findLastUsedRow(OdfTable table) {
    int rowCount = table.getRowCount();
    for (int row = rowCount - 1; row >= 0; row--) {
      if (rowHasContent(table, row)) {
        return row;
      }
    }
    return -1;
  }

  private boolean rowHasContent(OdfTable table, int rowIndex) {
    int columnCount = table.getColumnCount();
    for (int col = 0; col < columnCount; col++) {
      OdfTableCell cell = table.getCellByPosition(col, rowIndex);
      if (cell != null && !Utils.isEmpty(cell.getDisplayText())) {
        return true;
      }
    }
    return false;
  }

  private void ensureTemplateExtensionMatches() throws HopException {
    String templateExt =
        HopVfs.getFileObject(data.realTemplateFileName, transform).getName().getExtension();
    if (!meta.getFile().getExtension().equalsIgnoreCase(templateExt)) {
      throw new HopException(
          "Template Format Mismatch: Template has extension: "
              + templateExt
              + ", but output file has extension: "
              + meta.getFile().getExtension()
              + ". Template and output file must share the same format!");
    }
  }

  private ExcelWriterWorkbookDefinition prepareWorkbookDefinition(
      int numOfFields,
      int splitNr,
      FileObject file,
      OdfSpreadsheetDocument document,
      OdfTable table,
      int posX,
      String baseFileName,
      int startY) {
    OdsWorkbookHandle handle = new OdsWorkbookHandle(document, table);
    ExcelWriterWorkbookDefinition workbookDefinition =
        new ExcelWriterWorkbookDefinition(baseFileName, file, handle, posX, startY);
    workbookDefinition.setSplitNr(splitNr);
    data.usedFiles.add(workbookDefinition);
    data.currentWorkbookDefinition = workbookDefinition;
    workbookDefinition.clearStyleCache(numOfFields);
    return workbookDefinition;
  }

  private FileObject getFileLocation(Object[] row) throws HopFileException {
    String buildFilename =
        (!meta.getFile().isFileNameInField())
            ? transform.buildFilename(transform.getNextSplitNr(meta.getFile().getFileName()))
            : transform.buildFilename(data.inputRowMeta, row);
    return HopVfs.getFileObject(buildFilename, transform);
  }
}
