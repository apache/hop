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

package org.apache.hop.pipeline.transforms.excelwriter;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.staticschema.metadata.SchemaDefinition;
import org.apache.hop.staticschema.metadata.SchemaFieldDefinition;
import org.apache.hop.staticschema.util.SchemaDefinitionUtil;
import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.ClientAnchor;
import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Drawing;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Hyperlink;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.RichTextString;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ExcelWriterTransform
    extends BaseTransform<ExcelWriterTransformMeta, ExcelWriterTransformData> {

  private static final Class<?> PKG = ExcelWriterTransformMeta.class;

  public static final String STREAMER_FORCE_RECALC_PROP_NAME =
      "HOP_EXCEL_WRITER_STREAMER_FORCE_RECALCULATE";

  public static final int BYTE_ARRAY_MAX_OVERRIDE = 250000000;
  public static final String CONST_COULDN_T_BE_FOUND_IN_THE_INPUT_STREAM =
      "] couldn't be found in the input stream!";

  public ExcelWriterTransform(
      TransformMeta transformMeta,
      ExcelWriterTransformMeta meta,
      ExcelWriterTransformData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
    IOUtils.setByteArrayMaxOverride(BYTE_ARRAY_MAX_OVERRIDE);
  }

  @Override
  public boolean processRow() throws HopException {

    // get next row
    Object[] r = getRow();

    // We might have a row here, or we might get a null row if there was no input.
    //
    if (first) {
      first = false;
      if (r == null) {
        data.outputRowMeta = new RowMeta();
        data.inputRowMeta = new RowMeta();
      } else {
        data.outputRowMeta = getInputRowMeta().clone();
        data.inputRowMeta = getInputRowMeta().clone();
      }

      // If we are usign a schema and ingoring fields create the outputfields
      if (meta.isIgnoreFields()) {
        meta.setOutputFields(new ArrayList<>());
        try {
          SchemaDefinition loadedSchemaDefinition =
              (new SchemaDefinitionUtil())
                  .loadSchemaDefinition(metadataProvider, meta.getSchemaDefinition());
          if (loadedSchemaDefinition != null) {
            for (SchemaFieldDefinition schemaFieldDefinition :
                loadedSchemaDefinition.getFieldDefinitions()) {
              ExcelWriterOutputField excelOutputField = new ExcelWriterOutputField();
              excelOutputField.setName(schemaFieldDefinition.getName());
              excelOutputField.setFormat(schemaFieldDefinition.getFormatMask());
              excelOutputField.setType(schemaFieldDefinition.getHopType());
              meta.getOutputFields().add(excelOutputField);
            }
          }
        } catch (HopTransformException e) {
          // ignore any errors here.
        }
      }

      // If we are supposed to create the file up front regardless of whether we receive input rows
      // then this is the place to do it.
      //
      if (!meta.getFile().isDoNotOpenNewFileInit()) {
        try {
          prepareNextOutputFile(r);
        } catch (HopException e) {
          logError(
              BaseMessages.getString(
                  PKG,
                  "ExcelWriterTransform.Exception.CouldNotPrepareFile",
                  resolve(meta.getFile().getFileName())));
          setErrors(1L);
          stopAll();
          return false;
        }
      }

      if (r != null) {
        // If we are supposed to create a file after receiving the first row, we do this here.
        //
        if (meta.getFile().isDoNotOpenNewFileInit()) {
          prepareNextOutputFile(r);
        }

        // Let's remember where the fields are in the input row
        int outputFieldsCount = meta.getOutputFields().size();
        data.commentauthorfieldnrs = new int[outputFieldsCount];
        data.commentfieldnrs = new int[outputFieldsCount];
        data.linkfieldnrs = new int[outputFieldsCount];
        data.fieldnrs = new int[outputFieldsCount];

        int i = 0;
        for (ExcelWriterOutputField outputField : meta.getOutputFields()) {
          // Output Fields
          String outputFieldName = outputField.getName();
          data.fieldnrs[i] = data.inputRowMeta.indexOfValue(outputFieldName);
          if (data.fieldnrs[i] < 0) {
            logError("Field [" + outputFieldName + CONST_COULDN_T_BE_FOUND_IN_THE_INPUT_STREAM);
            setErrors(1);
            stopAll();
            return false;
          }

          // Comment Fields
          String commentField = outputField.getCommentField();
          data.commentfieldnrs[i] = data.inputRowMeta.indexOfValue(commentField);
          if (data.commentfieldnrs[i] < 0 && !Utils.isEmpty(commentField)) {
            logError(
                "Comment Field [" + commentField + CONST_COULDN_T_BE_FOUND_IN_THE_INPUT_STREAM);
            setErrors(1);
            stopAll();
            return false;
          }

          // Comment Author Fields
          String commentAuthorField = outputField.getCommentAuthorField();
          data.commentauthorfieldnrs[i] = data.inputRowMeta.indexOfValue(commentAuthorField);
          if (data.commentauthorfieldnrs[i] < 0 && !Utils.isEmpty(commentAuthorField)) {
            logError(
                "Comment Author Field ["
                    + commentAuthorField
                    + CONST_COULDN_T_BE_FOUND_IN_THE_INPUT_STREAM);
            setErrors(1);
            stopAll();
            return false;
          }

          // Link Fields
          String hyperlinkField = outputField.getHyperlinkField();
          data.linkfieldnrs[i] = data.inputRowMeta.indexOfValue(hyperlinkField);
          if (data.linkfieldnrs[i] < 0 && !Utils.isEmpty(hyperlinkField)) {
            logError("Link Field [" + hyperlinkField + CONST_COULDN_T_BE_FOUND_IN_THE_INPUT_STREAM);
            setErrors(1);
            stopAll();
            return false;
          }

          // Increase counter
          ++i;
        }
      }
    }

    if (r != null) {
      // check if the filename has changed between rows
      if (meta.getFile().isFileNameInField()) {
        if (data.isBeamContext()) {
          throw new HopException(
              "Storing filenames in an input field is not supported in Beam pipelines");
        }

        if (!data.currentWorkbookDefinition.getFile().equals(getFileLocation(r))) {
          // check if the file is already used and switch or create new file
          boolean fileFound = false;
          for (int i = 0; i < data.usedFiles.size(); i++) {
            if (data.usedFiles.get(i).getFile().equals(getFileLocation(r))) {
              fileFound = true;
              data.currentWorkbookDefinition = data.usedFiles.get(i);
              break;
            }
          }
          if (!fileFound) {
            prepareNextOutputFile(r);
          }
        }
      }

      // File Splitting Feature, is it time to create a new file?
      if (!meta.isAppendLines()
          && !meta.getFile().isFileNameInField()
          && meta.getFile().getSplitEvery() > 0
          && data.currentWorkbookDefinition.getDatalines() > 0
          && data.currentWorkbookDefinition.getDatalines() % meta.getFile().getSplitEvery() == 0) {
        prepareNextOutputFile(r);
      }

      writeNextLine(data.currentWorkbookDefinition, r);
      incrementLinesOutput();

      data.currentWorkbookDefinition.setDatalines(
          data.currentWorkbookDefinition.getDatalines() + 1);

      // pass on the row unchanged
      putRow(data.outputRowMeta, r);

      // Some basic logging
      if (checkFeedback(getLinesOutput()) && isBasic()) {
        logBasic("Linenr " + getLinesOutput());
      }
      return true;
    } else {
      closeFiles();
      setOutputDone();
      return false;
    }
  }

  public void closeFiles() throws HopException {

    // Close all files and dispose objects
    for (ExcelWriterWorkbookDefinition workbookDefinition : data.usedFiles) {
      closeOutputFile(workbookDefinition);
    }
    data.usedFiles.clear();
  }

  private void createParentFolder(FileObject filename) throws Exception {
    // Check for parent folder
    FileObject parentfolder = null;
    try {
      // Get parent folder
      parentfolder = filename.getParent();
      if (parentfolder.exists()) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ExcelWriter.Log.ParentFolderExist", HopVfs.getFriendlyURI(parentfolder)));
        }
      } else {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG,
                  "ExcelWriter.Log.ParentFolderNotExist",
                  HopVfs.getFriendlyURI(parentfolder)));
        }
        if (meta.getFile().isCreateParentFolder()) {
          parentfolder.createFolder();
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG,
                    "ExcelWriter.Log.ParentFolderCreated",
                    HopVfs.getFriendlyURI(parentfolder)));
          }
        } else {
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "ExcelWriter.Log.ParentFolderNotExistCreateIt",
                  HopVfs.getFriendlyURI(parentfolder),
                  HopVfs.getFriendlyURI(filename)));
        }
      }
    } finally {
      if (parentfolder != null) {
        try {
          parentfolder.close();
        } catch (Exception ex) {
          // Ignore
        }
      }
    }
  }

  private void closeOutputFile(ExcelWriterWorkbookDefinition file) throws HopException {
    OutputStream out = null;
    try {
      out = new BufferedOutputStream(HopVfs.getOutputStream(file.getFile(), false));
      // may have to write a footer here
      if (meta.isFooterEnabled()) {
        writeHeader(file, file.getSheet(), file.getPosX(), file.getPosY());
      }
      // handle auto size for columns
      if (meta.getFile().isAutosizecolums()) {

        // track all columns for autosizing if using streaming worksheet
        if (file.getSheet() instanceof SXSSFSheet sxssfSheet) {
          sxssfSheet.trackAllColumnsForAutoSizing();
        }

        if (Utils.isEmpty(meta.getOutputFields())) {
          for (int i = 0; i < data.inputRowMeta.size(); i++) {
            file.getSheet().autoSizeColumn(i + data.startingCol);
          }
        } else {
          for (int i = 0; i < meta.getOutputFields().size(); i++) {
            file.getSheet().autoSizeColumn(i + data.startingCol);
          }
        }
      }
      // force recalculation of formulas if requested
      if (meta.isForceFormulaRecalculation()) {
        recalculateAllWorkbookFormulas(data.currentWorkbookDefinition);
      }

      // This closes the output stream as well
      file.getWorkbook().write(out);
      file.getWorkbook().close();

      // deleting the temporary files
      if (file.getWorkbook() instanceof SXSSFWorkbook sxssfWorkbook) {
        sxssfWorkbook.dispose();
      }

    } catch (IOException e) {
      throw new HopException(e);
    } finally {
      if (out != null) {
        try {
          out.flush();
          out.close();
        } catch (Exception e) {
          throw new HopException("Error closing excel file " + file.getFile(), e);
        }
      }
    }
  }

  // recalculates all formula fields for the entire workbook
  // package-local visibility for testing purposes
  void recalculateAllWorkbookFormulas(ExcelWriterWorkbookDefinition workbookDefinition) {
    if (workbookDefinition.getWorkbook() instanceof XSSFWorkbook) {
      // XLSX needs full reevaluation
      FormulaEvaluator evaluator =
          workbookDefinition.getWorkbook().getCreationHelper().createFormulaEvaluator();
      for (int sheetNum = 0;
          sheetNum < workbookDefinition.getWorkbook().getNumberOfSheets();
          sheetNum++) {
        Sheet sheet = workbookDefinition.getWorkbook().getSheetAt(sheetNum);
        for (Row r : sheet) {
          for (Cell c : r) {
            if (c.getCellType() == CellType.FORMULA) {
              evaluator.evaluateFormulaCell(c);
            }
          }
        }
      }
    } else if (workbookDefinition.getWorkbook() instanceof HSSFWorkbook hssfWorkbook) {
      // XLS supports a "dirty" flag to have excel recalculate everything when a sheet is opened
      for (int sheetNum = 0;
          sheetNum < workbookDefinition.getWorkbook().getNumberOfSheets();
          sheetNum++) {
        HSSFSheet sheet = hssfWorkbook.getSheetAt(sheetNum);
        sheet.setForceFormulaRecalculation(true);
      }
    } else {
      String forceRecalc = getVariable(STREAMER_FORCE_RECALC_PROP_NAME, "N");
      if ("Y".equals(forceRecalc)) {
        workbookDefinition.getWorkbook().setForceFormulaRecalculation(true);
      }
    }
  }

  public void writeNextLine(ExcelWriterWorkbookDefinition workbookDefinition, Object[] r)
      throws HopException {
    try {
      openLine(workbookDefinition.getSheet(), workbookDefinition.getPosY());
      Row xlsRow = workbookDefinition.getSheet().getRow(workbookDefinition.getPosY());
      if (xlsRow == null) {
        xlsRow = workbookDefinition.getSheet().createRow(workbookDefinition.getPosY());
      }

      Object v = null;
      if (Utils.isEmpty(meta.getOutputFields())) {
        //  Write all values in stream to text file.
        int nr = data.inputRowMeta.size();
        workbookDefinition.clearStyleCache(nr);
        data.linkfieldnrs = new int[nr];
        data.commentfieldnrs = new int[nr];
        int x = data.currentWorkbookDefinition.getPosX();
        for (int i = 0; i < nr; i++) {
          v = r[i];
          writeField(
              workbookDefinition,
              v,
              data.inputRowMeta.getValueMeta(i),
              null,
              xlsRow,
              x++,
              r,
              i,
              false);
        }
        // go to the next line
        workbookDefinition.setPosX(data.startingCol);
        workbookDefinition.incrementY();
      } else {
        /*
         * Only write the fields specified!
         */
        int x = data.currentWorkbookDefinition.getPosX();
        for (int i = 0; i < meta.getOutputFields().size(); i++) {
          v = r[data.fieldnrs[i]];
          ExcelWriterOutputField field = meta.getOutputFields().get(i);
          writeField(
              workbookDefinition,
              v,
              data.inputRowMeta.getValueMeta(data.fieldnrs[i]),
              field,
              xlsRow,
              x++,
              r,
              i,
              false);
        }
        // go to the next line
        workbookDefinition.setPosX(data.startingCol);
        workbookDefinition.incrementY();
      }
    } catch (Exception e) {
      logError("Error writing line :" + e.toString());
      throw new HopException(e);
    }
  }

  private Comment createCellComment(
      ExcelWriterWorkbookDefinition workbookDefinition, String author, String comment) {
    // comments only supported for XLSX
    if (workbookDefinition.getSheet() instanceof XSSFSheet) {
      CreationHelper factory = workbookDefinition.getWorkbook().getCreationHelper();
      Drawing drawing = workbookDefinition.getSheet().createDrawingPatriarch();

      ClientAnchor anchor = factory.createClientAnchor();
      Comment cmt = drawing.createCellComment(anchor);
      RichTextString str = factory.createRichTextString(comment);
      cmt.setString(str);
      cmt.setAuthor(author);
      return cmt;
    }
    return null;
  }

  /**
   * @param reference
   * @return the cell the reference points to
   */
  private Cell getCellFromReference(
      ExcelWriterWorkbookDefinition workbookDefinition, String reference) {

    CellReference cellRef = new CellReference(reference);
    String sheetName = cellRef.getSheetName();

    Sheet sheet = workbookDefinition.getSheet();
    if (!Utils.isEmpty(sheetName)) {
      sheet = workbookDefinition.getWorkbook().getSheet(sheetName);
    }
    if (sheet == null) {
      return null;
    }
    // reference is assumed to be absolute
    Row xlsRow = sheet.getRow(cellRef.getRow());
    if (xlsRow == null) {
      return null;
    }
    return xlsRow.getCell(cellRef.getCol());
  }

  // VisibleForTesting
  void writeField(
      ExcelWriterWorkbookDefinition workbookDefinition,
      Object v,
      IValueMeta vMeta,
      ExcelWriterOutputField excelField,
      Row xlsRow,
      int posX,
      Object[] row,
      int fieldNr,
      boolean isTitle)
      throws HopException {
    try {
      boolean cellExisted = true;
      // get the cell
      Cell cell = xlsRow.getCell(posX);
      if (cell == null) {
        cellExisted = false;
        cell = xlsRow.createCell(posX);
      }

      // if cell existed and existing cell's styles should not be changed, don't
      if (!(cellExisted && meta.isLeaveExistingStylesUnchanged())) {

        // if the style of this field is cached, reuse it
        if (!isTitle && workbookDefinition.getCachedStyle(fieldNr) != null) {
          cell.setCellStyle(workbookDefinition.getCachedStyle(fieldNr));
        } else {
          // apply style if requested
          if (excelField != null) {

            // determine correct cell for title or data rows
            String styleRef = null;
            if (!isTitle && !Utils.isEmpty(excelField.getStyleCell())) {
              styleRef = excelField.getStyleCell();
            } else if (isTitle && !Utils.isEmpty(excelField.getTitleStyleCell())) {
              styleRef = excelField.getTitleStyleCell();
            }

            if (styleRef != null) {
              Cell styleCell = getCellFromReference(workbookDefinition, styleRef);
              if (styleCell != null && cell != styleCell) {
                cell.setCellStyle(styleCell.getCellStyle());
              }
            }
          }

          // set cell format as specified, specific format overrides cell specification
          if (!isTitle
              && excelField != null
              && !Utils.isEmpty(excelField.getFormat())
              && !excelField.getFormat().startsWith("Image")) {
            setDataFormat(workbookDefinition, excelField.getFormat(), cell);
          }

          if (!isTitle
              && excelField != null
              && Utils.isEmpty(excelField.getFormat())
              && (vMeta.getType() == IValueMeta.TYPE_DATE
                  || vMeta.getType() == IValueMeta.TYPE_TIMESTAMP)) {
            String format = vMeta.getFormatMask();
            if (!Utils.isEmpty(format)) {
              setDataFormat(workbookDefinition, format, cell);
            }
          }
          // cache it for later runs
          if (!isTitle) {
            workbookDefinition.cacheStyle(fieldNr, cell.getCellStyle());
          }
        }
      }

      // create link on cell if requested
      if (!isTitle && excelField != null && data.linkfieldnrs[fieldNr] >= 0) {
        String link =
            data.inputRowMeta
                .getValueMeta(data.linkfieldnrs[fieldNr])
                .getString(row[data.linkfieldnrs[fieldNr]]);
        if (!Utils.isEmpty(link)) {
          CreationHelper ch = workbookDefinition.getWorkbook().getCreationHelper();
          // set the link on the cell depending on link type
          Hyperlink hyperLink = null;
          if (link.startsWith("http:") || link.startsWith("https:") || link.startsWith("ftp:")) {
            hyperLink = ch.createHyperlink(HyperlinkType.URL);
            hyperLink.setLabel("URL Link");
          } else if (link.startsWith("mailto:")) {
            hyperLink = ch.createHyperlink(HyperlinkType.EMAIL);
            hyperLink.setLabel("Email Link");
          } else if (link.startsWith("'")) {
            hyperLink = ch.createHyperlink(HyperlinkType.DOCUMENT);
            hyperLink.setLabel("Link within this document");
          } else {
            hyperLink = ch.createHyperlink(HyperlinkType.FILE);
            hyperLink.setLabel("Link to a file");
          }

          hyperLink.setAddress(link);
          cell.setHyperlink(hyperLink);

          // if cell existed and existing cell's styles should not be changed, don't
          if (!(cellExisted && meta.isLeaveExistingStylesUnchanged())) {

            if (workbookDefinition.getCachedLinkStyle(fieldNr) != null) {
              cell.setCellStyle(workbookDefinition.getCachedLinkStyle(fieldNr));
            } else {
              Font origFont =
                  workbookDefinition.getWorkbook().getFontAt(cell.getCellStyle().getFontIndex());
              Font hlinkFont = workbookDefinition.getWorkbook().createFont();
              // reproduce original font characteristics

              hlinkFont.setBold(origFont.getBold());
              hlinkFont.setCharSet(origFont.getCharSet());
              hlinkFont.setFontHeight(origFont.getFontHeight());
              hlinkFont.setFontName(origFont.getFontName());
              hlinkFont.setItalic(origFont.getItalic());
              hlinkFont.setStrikeout(origFont.getStrikeout());
              hlinkFont.setTypeOffset(origFont.getTypeOffset());
              // make it blue and underlined
              hlinkFont.setUnderline(Font.U_SINGLE);
              hlinkFont.setColor(IndexedColors.BLUE.getIndex());
              CellStyle style = cell.getCellStyle();
              style.setFont(hlinkFont);
              cell.setCellStyle(style);
              workbookDefinition.cacheLinkStyle(fieldNr, cell.getCellStyle());
            }
          }
        }
      }

      // create comment on cell if requested
      if (!isTitle
          && excelField != null
          && data.commentfieldnrs[fieldNr] >= 0
          && workbookDefinition.getWorkbook() instanceof XSSFWorkbook) {
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
          cell.setCellComment(createCellComment(workbookDefinition, author, comment));
        }
      }
      // cell is getting a formula value or static content
      if (!isTitle && excelField != null && excelField.isFormula()) {
        // formula case
        cell.setCellFormula(vMeta.getString(v));
      } else {
        // static content case
        switch (vMeta.getType()) {
          case IValueMeta.TYPE_DATE:
            if (v != null && vMeta.getDate(v) != null) {
              cell.setCellValue(vMeta.getDate(v));
            }
            break;
          case IValueMeta.TYPE_BOOLEAN:
            if (v != null) {
              cell.setCellValue(vMeta.getBoolean(v));
            }
            break;
          case IValueMeta.TYPE_BIGNUMBER, IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_INTEGER:
            if (v != null) {
              cell.setCellValue(vMeta.getNumber(v));
            }
            break;
          default:
            // fallthrough: output the data value as a string
            if (v != null) {
              cell.setCellValue(vMeta.getString(v));
            }
            break;
        }
      }
    } catch (Exception e) {
      logError(
          "Error writing field ("
              + workbookDefinition.getPosX()
              + ","
              + workbookDefinition.getPosY()
              + ") : "
              + e.toString());
      logError(Const.getStackTracker(e));
      throw new HopException(e);
    }
  }

  /**
   * Set specified cell format
   *
   * @param excelFieldFormat the specified format
   * @param cell the cell to set up format
   */
  private void setDataFormat(
      ExcelWriterWorkbookDefinition workbookDefinition, String excelFieldFormat, Cell cell) {
    if (isDebug()) {
      logDebug(
          BaseMessages.getString(
              PKG,
              "ExcelWriterTransform.Log.SetDataFormat",
              excelFieldFormat,
              CellReference.convertNumToColString(cell.getColumnIndex()),
              cell.getRowIndex()));
    }

    DataFormat format = workbookDefinition.getWorkbook().createDataFormat();
    short formatIndex = format.getFormat(excelFieldFormat);
    CellStyle style = workbookDefinition.getWorkbook().createCellStyle();
    style.cloneStyleFrom(cell.getCellStyle());
    style.setDataFormat(formatIndex);
    cell.setCellStyle(style);
  }

  /**
   * Returns the output filename that belongs to this transform observing the file split feature
   *
   * @return current output filename to write to
   */
  public String buildFilename(IRowMeta rowMeta, Object[] row) {
    return meta.buildFilename(rowMeta, row, this);
  }

  /**
   * Returns the output filename that belongs to this transform observing the file split feature
   *
   * @return current output filename to write to
   */
  public String buildFilename(int splitNr) {
    return meta.buildFilename(
        this, getCopy(), splitNr, data.isBeamContext(), getLogChannelId(), data.getBeamBundleNr());
  }

  /**
   * Copies a VFS File
   *
   * @param in the source file object
   * @param out the destination file object
   * @throws HopException
   */
  public static void copyFile(FileObject in, FileObject out) throws HopException {
    try (BufferedInputStream fis = new BufferedInputStream(HopVfs.getInputStream(in));
        BufferedOutputStream fos = new BufferedOutputStream(HopVfs.getOutputStream(out, false))) {
      byte[] buf = new byte[1024 * 1024]; // copy in chunks of 1 MB
      int i = 0;
      while ((i = fis.read(buf)) != -1) {
        fos.write(buf, 0, i);
      }
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  public void prepareNextOutputFile(Object[] row) throws HopException {
    try {
      // Validation
      //
      // sheet name shouldn't exceed 31 character
      if (data.realSheetname != null && data.realSheetname.length() > 31) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "ExcelWriterTransform.Exception.MaxSheetName", data.realSheetname));
      }

      // Getting field names from input is not supported in a Beam context
      //
      if (data.isBeamContext() && meta.getFile().isFileNameInField()) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "ExcelWriterTransform.Exception.FilenameFromFieldNotSupportedInBeam"));
      }

      // clear style cache
      int numOfFields = !Utils.isEmpty(meta.getOutputFields()) ? meta.getOutputFields().size() : 0;
      if (numOfFields == 0) {
        numOfFields = data.inputRowMeta != null ? data.inputRowMeta.size() : 0;
      }

      int splitNr = 0;
      if (!meta.getFile().isFileNameInField()) {
        splitNr = getNextSplitNr(meta.getFile().getFileName());
      }

      FileObject file = getFileLocation(row);

      if (!file.getParent().exists() && meta.getFile().isCreateParentFolder()) {
        logDebug(
            "Create parent directory for "
                + file.getName().toString()
                + " because it does not exist.");
        createParentFolder(file);
      }

      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG, "ExcelWriterTransform.Log.OpeningFile", file.getName().toString()));
      }

      // determine whether existing file must be deleted
      if (file.exists() && data.createNewFile && !file.delete()) {
        if (isBasic()) {
          logBasic(
              BaseMessages.getString(
                  PKG,
                  "ExcelWriterTransform.Log.CouldNotDeleteStaleFile",
                  file.getName().toString()));
        }
        setErrors(1);
        throw new HopException("Could not delete stale file " + file.getName().toString());
      }

      // adding filename to result
      if (meta.isAddToResultFilenames()) {
        // Add this to the result file names...
        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                file,
                getPipelineMeta().getName(),
                getTransformName());
        resultFile.setComment(
            "This file was created with an Excel writer transform by Hop : The Hop Orchestration Platform");
        addResultFile(resultFile);
      }
      boolean appendingToSheet = true;
      // if now no file exists we must create it as indicated by user
      if (!file.exists()) {
        // if template file is enabled
        if (meta.getTemplate().isTemplateEnabled()) {
          // handle template case (must have same format)
          // ensure extensions match
          String templateExt =
              HopVfs.getFileObject(data.realTemplateFileName, variables).getName().getExtension();
          if (!meta.getFile().getExtension().equalsIgnoreCase(templateExt)) {
            throw new HopException(
                "Template Format Mismatch: Template has extension: "
                    + templateExt
                    + ", but output file has extension: "
                    + meta.getFile().getExtension()
                    + ". Template and output file must share the same format!");
          }

          if (HopVfs.getFileObject(data.realTemplateFileName, variables).exists()) {
            // if the template exists just copy the template in place
            copyFile(HopVfs.getFileObject(data.realTemplateFileName, variables), file);
          } else {
            // template is missing, log it and get out
            if (isBasic()) {
              logBasic(
                  BaseMessages.getString(
                      PKG, "ExcelWriterTransform.Log.TemplateMissing", data.realTemplateFileName));
            }
            setErrors(1);
            throw new HopException("Template file missing: " + data.realTemplateFileName);
          }
        } else {
          // handle fresh file case, just create a fresh workbook
          try (Workbook wb =
              meta.getFile().getExtension().equalsIgnoreCase("xlsx")
                  ? new XSSFWorkbook()
                  : new HSSFWorkbook()) {
            wb.createSheet(data.realSheetname);
            try (OutputStream out = HopVfs.getOutputStream(file, false)) {
              wb.write(out);
            }
          }
        }
        appendingToSheet = false;
      }

      // Start creating the workbook
      Workbook wb;
      Sheet sheet;
      // file is guaranteed to be in place now
      if (meta.getFile().getExtension().equalsIgnoreCase("xlsx")) {
        try (InputStream inputStream = HopVfs.getInputStream(HopVfs.getFilename(file), variables)) {
          XSSFWorkbook xssfWorkbook = new XSSFWorkbook(inputStream);
          if (meta.getFile().isStreamingData() && !meta.getTemplate().isTemplateEnabled()) {
            wb = new SXSSFWorkbook(xssfWorkbook, 100);
          } else {
            // Initialize it later after writing header/template because SXSSFWorkbook can't
            // read/rewrite existing data,
            // only append.
            wb = xssfWorkbook;
          }
        }
      } else {
        try (InputStream inputStream = HopVfs.getInputStream(file)) {
          wb = new HSSFWorkbook(inputStream);
        }
      }

      int existingActiveSheetIndex = wb.getActiveSheetIndex();
      int replacingSheetAt = -1;

      if (wb.getSheet(data.realSheetname) != null && data.createNewSheet) {
        // sheet exists, replace or reuse as indicated by user
        replacingSheetAt = wb.getSheetIndex(wb.getSheet(data.realSheetname));
        wb.removeSheetAt(replacingSheetAt);
      }

      // if sheet is now missing, we need to create a new one
      if (wb.getSheet(data.realSheetname) == null) {
        if (meta.getTemplate().isTemplateSheetEnabled()) {
          Sheet ts = wb.getSheet(data.realTemplateSheetName);
          // if template sheet is missing, break
          if (ts == null) {
            throw new HopException(
                BaseMessages.getString(
                    PKG,
                    "ExcelWriterTransform.Exception.TemplateNotFound",
                    data.realTemplateSheetName));
          }
          sheet = wb.cloneSheet(wb.getSheetIndex(ts));
          wb.setSheetName(wb.getSheetIndex(sheet), data.realSheetname);
          // unhide sheet in case it was hidden
          wb.setSheetHidden(wb.getSheetIndex(sheet), false);
          if (meta.getTemplate().isTemplateSheetHidden()) {
            wb.setSheetHidden(wb.getSheetIndex(ts), true);
          }
        } else {
          // no template to use, simply create a new sheet
          sheet = wb.createSheet(data.realSheetname);
        }
        if (replacingSheetAt > -1) {
          wb.setSheetOrder(sheet.getSheetName(), replacingSheetAt);
        }
        // preserves active sheet selection in workbook
        wb.setActiveSheet(existingActiveSheetIndex);
        wb.setSelectedTab(existingActiveSheetIndex);
        appendingToSheet = false;
      } else {
        // sheet is there and should be reused
        sheet = wb.getSheet(data.realSheetname);
      }
      // if use chose to make the current sheet active, do so
      if (meta.isMakeSheetActive()) {
        int sheetIndex = wb.getSheetIndex(sheet);
        wb.setActiveSheet(sheetIndex);
        wb.setSelectedTab(sheetIndex);
      }
      // handle write protection
      if (meta.getFile().isProtectsheet()) {
        protectSheet(sheet, data.realPassword);
      }

      // starting cell support
      if (!Utils.isEmpty(data.realStartingCell)) {
        CellReference cellRef = new CellReference(data.realStartingCell);
        data.startingRow = cellRef.getRow();
        data.startingCol = cellRef.getCol();
      } else {
        data.startingRow = 0;
        data.startingCol = 0;
      }

      // Calculate the starting positions in the sheet.
      //
      int posX;
      int posY;
      posX = data.startingCol;
      posY = data.startingRow;

      // Find last row and append accordingly
      if (!data.createNewSheet && meta.isAppendLines() && appendingToSheet) {
        if (sheet.getPhysicalNumberOfRows() > 0) {
          posY = sheet.getLastRowNum() + 1;
        } else {
          posY = 0;
        }
      }

      // offset by configured value
      // Find last row and append accordingly
      if (!data.createNewSheet && meta.getAppendOffset() != 0 && appendingToSheet) {
        posY += meta.getAppendOffset();
      }

      // may have to write a few empty lines
      if (!data.createNewSheet && meta.getAppendEmpty() > 0 && appendingToSheet) {
        for (int i = 0; i < meta.getAppendEmpty(); i++) {
          sheet = openLine(sheet, posY);
          if (!data.shiftExistingCells || meta.isAppendLines()) {
            posY++;
          }
        }
      }

      // save file for later usage
      String baseFileName;
      if (!meta.getFile().isFileNameInField()) {
        baseFileName = meta.getFile().getFileName();
      } else {
        baseFileName = file.getName().toString();
      }

      // If starting cell provided, use the posY derived above, otherwise use default behaviour
      int startY =
          !Utils.isEmpty(data.realStartingCell) ? posY : Math.max(posY, sheet.getLastRowNum());

      ExcelWriterWorkbookDefinition workbookDefinition =
          prepareWorkbookDefinition(
              numOfFields, splitNr, file, wb, sheet, posX, baseFileName, startY);

      // may have to write a header here
      if (meta.isHeaderEnabled()
          && !(!data.createNewSheet && meta.isAppendOmitHeader() && appendingToSheet)) {
        data.currentWorkbookDefinition.setSheet(writeHeader(workbookDefinition, sheet, posX, posY));
      }

      // Reload Worksheet in Streaming mode when a template is used
      if (meta.getFile().getExtension().equalsIgnoreCase("xlsx")
          && meta.getFile().isStreamingData()
          && meta.getTemplate().isTemplateEnabled()) {
        try (InputStream inputStream = HopVfs.getInputStream(HopVfs.getFilename(file), variables)) {
          XSSFWorkbook xssfWorkbook = new XSSFWorkbook(inputStream);
          wb = new SXSSFWorkbook(xssfWorkbook, 100);
          sheet = wb.getSheet(data.realSheetname);
          // Replace workbookDefinition with reloaded one
          data.usedFiles.remove(workbookDefinition);
          prepareWorkbookDefinition(
              numOfFields, splitNr, file, wb, sheet, posX, baseFileName, startY);
        }
      }

      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG, "ExcelWriterTransform.Log.FileOpened", file.getName().toString()));
      }

    } catch (Exception e) {
      logError("Error opening new file", e);
      setErrors(1);
      throw new HopException("Error opening new file", e);
    }
  }

  private ExcelWriterWorkbookDefinition prepareWorkbookDefinition(
      int numOfFields,
      int splitNr,
      FileObject file,
      Workbook wb,
      Sheet sheet,
      int posX,
      String baseFileName,
      int startY) {
    ExcelWriterWorkbookDefinition workbookDefinition =
        new ExcelWriterWorkbookDefinition(baseFileName, file, wb, sheet, posX, startY);
    workbookDefinition.setSplitNr(splitNr);
    data.usedFiles.add(workbookDefinition);
    data.currentWorkbookDefinition = workbookDefinition;
    data.currentWorkbookDefinition.clearStyleCache(numOfFields);
    return workbookDefinition;
  }

  private Sheet openLine(Sheet sheet, int posY) {
    if (data.shiftExistingCells) {
      sheet.shiftRows(posY, Math.max(posY, sheet.getLastRowNum()), 1);
    }
    return sheet;
  }

  private Sheet writeHeader(
      ExcelWriterWorkbookDefinition workbookDefinition, Sheet sheet, int posX, int posY)
      throws HopException {
    try {
      sheet = openLine(sheet, posY);
      Row xlsRow = sheet.getRow(posY);
      if (xlsRow == null) {
        xlsRow = sheet.createRow(posY);
      }
      // If we have fields specified: list them in this order!
      if (!Utils.isEmpty(meta.getOutputFields())) {
        for (int i = 0; i < meta.getOutputFields().size(); i++) {
          ExcelWriterOutputField field = meta.getOutputFields().get(i);
          String fieldName = !Utils.isEmpty(field.getTitle()) ? field.getTitle() : field.getName();
          IValueMeta vMeta = new ValueMetaString(fieldName);
          writeField(workbookDefinition, fieldName, vMeta, field, xlsRow, posX++, null, -1, true);
        }
        // Just put all field names in
      } else if (data.inputRowMeta != null) {
        for (int i = 0; i < data.inputRowMeta.size(); i++) {
          String fieldName = data.inputRowMeta.getFieldNames()[i];
          IValueMeta vMeta = new ValueMetaString(fieldName);
          writeField(workbookDefinition, fieldName, vMeta, null, xlsRow, posX++, null, -1, true);
        }
      }
      workbookDefinition.setPosY(posY + 1);
      incrementLinesOutput();
    } catch (Exception e) {
      throw new HopException(e);
    }
    return sheet;
  }

  @Override
  public boolean init() {
    if (super.init()) {
      data.realSheetname = resolve(meta.getFile().getSheetname());
      data.realTemplateSheetName = resolve(meta.getTemplate().getTemplateSheetName());
      data.realTemplateFileName = resolve(meta.getTemplate().getTemplateFileName());
      data.realStartingCell = resolve(meta.getStartingCell());
      data.realPassword = Utils.resolvePassword(variables, meta.getFile().getPassword());
      data.realProtectedBy = resolve(meta.getFile().getProtectedBy());

      data.shiftExistingCells =
          ExcelWriterTransformMeta.ROW_WRITE_PUSH_DOWN.equals(meta.getRowWritingMethod());
      data.createNewSheet =
          ExcelWriterTransformMeta.IF_SHEET_EXISTS_CREATE_NEW.equals(
              meta.getFile().getIfSheetExists());
      data.createNewFile =
          ExcelWriterTransformMeta.IF_FILE_EXISTS_CREATE_NEW.equals(
              meta.getFile().getIfFileExists());
      return true;
    }
    return false;
  }

  @Override
  public void startBundle() throws HopException {
    // Generate a new file for the next bundle
    //
    if (!first) {
      prepareNextOutputFile(null);
    }
  }

  @Override
  public void finishBundle() throws HopException {
    closeFiles();
  }

  @Override
  public void batchComplete() throws HopException {
    // Call to make sure the files are closed in a Beam context
    // On Beam the single threader engine works with one row at a time.
    // We need to keep the file(s) open until the end of the bundle.
    //
    if (!data.isBeamContext()) {
      closeFiles();
    }
  }

  /** Write protect Sheet by setting password works only for xls output at the moment */
  protected void protectSheet(Sheet sheet, String password) {
    if (sheet instanceof HSSFSheet) {
      // Write protect Sheet by setting password
      // works only for xls output at the moment
      sheet.protectSheet(password);
    }
  }

  /**
   * This function will return the filepath
   *
   * @param row
   * @return Filepath
   */
  private FileObject getFileLocation(Object[] row) throws HopFileException {
    String buildFilename =
        (!meta.getFile().isFileNameInField())
            ? buildFilename(getNextSplitNr(meta.getFile().getFileName()))
            : buildFilename(data.inputRowMeta, row);
    return HopVfs.getFileObject(buildFilename, variables);
  }

  /**
   * This function returns the max splitnumber of the file
   *
   * @param fileName
   * @return
   */
  private int getNextSplitNr(String fileName) {
    int splitNr = 0;
    boolean fileFound = false;
    // Check if file exists and fetch max splitNr
    for (ExcelWriterWorkbookDefinition workbookDefinition : data.usedFiles) {
      if (workbookDefinition.getFileName().equals(fileName)) {
        fileFound = true;
        if (workbookDefinition.getSplitNr() > splitNr) {
          splitNr = workbookDefinition.getSplitNr();
        }
      }
    }
    // if a file exists increase the splitNr

    if (fileFound) {
      splitNr++;
    }
    return splitNr;
  }
}
