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

package org.apache.hop.pipeline.transforms.excelinput;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;

/** Meta data for the Excel transform. */
@Transform(
    id = "ExcelInput",
    image = "excelinput.svg",
    name = "i18n::BaseTransform.TypeLongDesc.ExcelInput",
    description = "i18n::BaseTransform.TypeTooltipDesc.ExcelInput",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/excelinput.html")
@InjectionSupported(
    localizationPrefix = "ExcelInput.Injection.",
    groups = {"FIELDS", "SHEETS", "FILENAME_LINES"})
public class ExcelInputMeta extends BaseTransformMeta
    implements ITransformMeta<ExcelInput, ExcelInputData> {
  private static final Class<?> PKG = ExcelInputMeta.class; // For Translator

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };
  public static final String[] RequiredFilesCode = new String[] {"N", "Y"};

  private static final String YES = "Y";
  public static final int TYPE_TRIM_NONE = 0;
  public static final int TYPE_TRIM_LEFT = 1;
  public static final int TYPE_TRIM_RIGHT = 2;
  public static final int TYPE_TRIM_BOTH = 3;

  public static final String[] type_trimCode = {"none", "left", "right", "both"};

  public static final String[] type_trimDesc = {
    BaseMessages.getString(PKG, "ExcelInputMeta.TrimType.None"),
    BaseMessages.getString(PKG, "ExcelInputMeta.TrimType.Left"),
    BaseMessages.getString(PKG, "ExcelInputMeta.TrimType.Right"),
    BaseMessages.getString(PKG, "ExcelInputMeta.TrimType.Both")
  };

  public static final String STRING_SEPARATOR = " \t --> ";

  /** The filenames to load or directory in case a filemask was set. */
  @Injection(name = "FILENAME", group = "FILENAME_LINES")
  private String[] fileName;

  /** The regular expression to use (null means: no mask) */
  @Injection(name = "FILEMASK", group = "FILENAME_LINES")
  private String[] fileMask;

  /** Wildcard or filemask to exclude (regular expression) */
  @Injection(name = "EXCLUDE_FILEMASK", group = "FILENAME_LINES")
  private String[] excludeFileMask;

  /** Array of boolean values as string, indicating if a file is required. */
  @Injection(name = "FILE_REQUIRED", group = "FILENAME_LINES")
  private String[] fileRequired;

  /** The fieldname that holds the name of the file */
  private String fileField;

  /** The names of the sheets to load. Null means: all sheets... */
  @Injection(name = "SHEET_NAME", group = "SHEETS")
  private String[] sheetName;

  /** The row-nr where we start processing. */
  @Injection(name = "SHEET_START_ROW", group = "SHEETS")
  private int[] startRow;

  /** The column-nr where we start processing. */
  @Injection(name = "SHEET_START_COL", group = "SHEETS")
  private int[] startColumn;

  /** The fieldname that holds the name of the sheet */
  private String sheetField;

  /** The cell-range starts with a header-row */
  private boolean startsWithHeader;

  /** Stop reading when you hit an empty row. */
  private boolean stopOnEmpty;

  /** Avoid empty rows in the result. */
  private boolean ignoreEmptyRows;

  /**
   * The fieldname containing the row number. An empty (null) value means that no row number is
   * included in the output. This is the rownumber of all written rows (not the row in the sheet).
   */
  private String rowNumberField;

  /**
   * The fieldname containing the sheet row number. An empty (null) value means that no sheet row
   * number is included in the output. Sheet row number is the row number in the sheet.
   */
  private String sheetRowNumberField;

  /** The maximum number of rows that this transform writes to the next transform. */
  private long rowLimit;

  /**
   * The fields to read in the range. Note: the number of columns in the range has to match
   * field.length
   */
  @InjectionDeep private ExcelInputField[] field;

  /** Strict types : will generate erros */
  private boolean strictTypes;

  /** Ignore error : turn into warnings */
  private boolean errorIgnored;

  /** If error line are skipped, you can replay without introducing doubles. */
  private boolean errorLineSkipped;

  /** The directory that will contain warning files */
  private String warningFilesDestinationDirectory;

  /** The extension of warning files */
  private String warningFilesExtension;

  /** The directory that will contain error files */
  private String errorFilesDestinationDirectory;

  /** The extension of error files */
  private String errorFilesExtension;

  /** The directory that will contain line number files */
  private String lineNumberFilesDestinationDirectory;

  /** The extension of line number files */
  private String lineNumberFilesExtension;

  /** Are we accepting filenames in input rows? */
  private boolean acceptingFilenames;

  /** The field in which the filename is placed */
  private String acceptingField;

  /** The transformName to accept filenames from */
  private String acceptingTransformName;

  /** Array of boolean values as string, indicating if we need to fetch sub folders. */
  @Injection(name = "INCLUDE_SUBFOLDERS", group = "FILENAME_LINES")
  private String[] includeSubFolders;

  /** The transform to accept filenames from */
  private TransformMeta acceptingTransform;

  /** The encoding to use for reading: null or empty string means system default encoding */
  private String encoding;

  /** The add filenames to result filenames flag */
  private boolean isaddresult;

  /** Additional fields */
  private String shortFileFieldName;

  private String pathFieldName;
  private String hiddenFieldName;
  private String lastModificationTimeFieldName;
  private String uriNameFieldName;
  private String rootUriNameFieldName;
  private String extensionFieldName;
  private String sizeFieldName;

  @Injection(name = "SPREADSHEET_TYPE")
  private SpreadSheetType spreadSheetType;

  public ExcelInputMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the shortFileFieldName. */
  public String getShortFileNameField() {
    return shortFileFieldName;
  }

  /** @param field The shortFileFieldName to set. */
  public void setShortFileNameField(String field) {
    shortFileFieldName = field;
  }

  /** @return Returns the pathFieldName. */
  public String getPathField() {
    return pathFieldName;
  }

  /** @param field The pathFieldName to set. */
  public void setPathField(String field) {
    this.pathFieldName = field;
  }

  /** @return Returns the hiddenFieldName. */
  public String isHiddenField() {
    return hiddenFieldName;
  }

  /** @param field The hiddenFieldName to set. */
  public void setIsHiddenField(String field) {
    hiddenFieldName = field;
  }

  /** @return Returns the lastModificationTimeFieldName. */
  public String getLastModificationDateField() {
    return lastModificationTimeFieldName;
  }

  /** @param field The lastModificationTimeFieldName to set. */
  public void setLastModificationDateField(String field) {
    lastModificationTimeFieldName = field;
  }

  /** @return Returns the uriNameFieldName. */
  public String getUriField() {
    return uriNameFieldName;
  }

  /** @param field The uriNameFieldName to set. */
  public void setUriField(String field) {
    uriNameFieldName = field;
  }

  /** @return Returns the uriNameFieldName. */
  public String getRootUriField() {
    return rootUriNameFieldName;
  }

  /** @param field The rootUriNameFieldName to set. */
  public void setRootUriField(String field) {
    rootUriNameFieldName = field;
  }

  /** @return Returns the extensionFieldName. */
  public String getExtensionField() {
    return extensionFieldName;
  }

  /** @param field The extensionFieldName to set. */
  public void setExtensionField(String field) {
    extensionFieldName = field;
  }

  /** @return Returns the sizeFieldName. */
  public String getSizeField() {
    return sizeFieldName;
  }

  /** @param field The sizeFieldName to set. */
  public void setSizeField(String field) {
    sizeFieldName = field;
  }

  /** @return Returns the fieldLength. */
  public ExcelInputField[] getField() {
    return field;
  }

  /** @param fields The excel input fields to set. */
  public void setField(ExcelInputField[] fields) {
    this.field = fields;
  }

  /** @return Returns the fileField. */
  public String getFileField() {
    return fileField;
  }

  /** @param fileField The fileField to set. */
  public void setFileField(String fileField) {
    this.fileField = fileField;
  }

  /** @return Returns the fileMask. */
  public String[] getFileMask() {
    return fileMask;
  }

  /** @param fileMask The fileMask to set. */
  public void setFileMask(String[] fileMask) {
    this.fileMask = fileMask;
  }

  /** @return Returns the excludeFileMask. */
  public String[] getExcludeFileMask() {
    return excludeFileMask;
  }

  /** @param excludeFileMask The excludeFileMask to set. */
  public void setExcludeFileMask(String[] excludeFileMask) {
    this.excludeFileMask = excludeFileMask;
  }

  public String[] getIncludeSubFolders() {
    return includeSubFolders;
  }

  public void setIncludeSubFolders(String[] includeSubFoldersin) {
    if (includeSubFoldersin != null) {
      includeSubFolders = new String[includeSubFoldersin.length];
      for (int i = 0; i < includeSubFoldersin.length && i < includeSubFolders.length; i++) {
        this.includeSubFolders[i] = getRequiredFilesCode(includeSubFoldersin[i]);
      }
    } else {
      includeSubFolders = new String[0];
    }
  }

  public String getRequiredFilesCode(String tt) {
    if (tt == null) {
      return RequiredFilesCode[0];
    }
    if (tt.equals(RequiredFilesDesc[1])) {
      return RequiredFilesCode[1];
    } else {
      return RequiredFilesCode[0];
    }
  }

  public String getRequiredFilesDesc(String tt) {
    if (tt == null) {
      return RequiredFilesDesc[0];
    }
    if (tt.equals(RequiredFilesCode[1])) {
      return RequiredFilesDesc[1];
    } else {
      return RequiredFilesDesc[0];
    }
  }

  /** @return Returns the fileName. */
  public String[] getFileName() {
    return fileName;
  }

  /** @param fileName The fileName to set. */
  public void setFileName(String[] fileName) {
    this.fileName = fileName;
  }

  /** @return Returns the ignoreEmptyRows. */
  public boolean ignoreEmptyRows() {
    return ignoreEmptyRows;
  }

  /** @param ignoreEmptyRows The ignoreEmptyRows to set. */
  public void setIgnoreEmptyRows(boolean ignoreEmptyRows) {
    this.ignoreEmptyRows = ignoreEmptyRows;
  }

  /** @return Returns the rowLimit. */
  public long getRowLimit() {
    return rowLimit;
  }

  /** @param rowLimit The rowLimit to set. */
  public void setRowLimit(long rowLimit) {
    this.rowLimit = rowLimit;
  }

  /** @return Returns the rowNumberField. */
  public String getRowNumberField() {
    return rowNumberField;
  }

  /** @param rowNumberField The rowNumberField to set. */
  public void setRowNumberField(String rowNumberField) {
    this.rowNumberField = rowNumberField;
  }

  /** @return Returns the sheetRowNumberField. */
  public String getSheetRowNumberField() {
    return sheetRowNumberField;
  }

  /** @param rowNumberField The rowNumberField to set. */
  public void setSheetRowNumberField(String rowNumberField) {
    this.sheetRowNumberField = rowNumberField;
  }

  /** @return Returns the sheetField. */
  public String getSheetField() {
    return sheetField;
  }

  /** @param sheetField The sheetField to set. */
  public void setSheetField(String sheetField) {
    this.sheetField = sheetField;
  }

  /** @return Returns the sheetName. */
  public String[] getSheetName() {
    return sheetName;
  }

  /** @param sheetName The sheetName to set. */
  public void setSheetName(String[] sheetName) {
    this.sheetName = sheetName;
  }

  /** @return Returns the startColumn. */
  public int[] getStartColumn() {
    return startColumn;
  }

  /** @param startColumn The startColumn to set. */
  public void setStartColumn(int[] startColumn) {
    this.startColumn = startColumn;
  }

  /** @return Returns the startRow. */
  public int[] getStartRow() {
    return startRow;
  }

  /** @param startRow The startRow to set. */
  public void setStartRow(int[] startRow) {
    this.startRow = startRow;
  }

  /** @return Returns the startsWithHeader. */
  public boolean startsWithHeader() {
    return startsWithHeader;
  }

  /** @param startsWithHeader The startsWithHeader to set. */
  public void setStartsWithHeader(boolean startsWithHeader) {
    this.startsWithHeader = startsWithHeader;
  }

  /** @return Returns the stopOnEmpty. */
  public boolean stopOnEmpty() {
    return stopOnEmpty;
  }

  /** @param stopOnEmpty The stopOnEmpty to set. */
  public void setStopOnEmpty(boolean stopOnEmpty) {
    this.stopOnEmpty = stopOnEmpty;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  @Override
  public Object clone() {
    ExcelInputMeta retval = (ExcelInputMeta) super.clone();
    normilizeAllocation();

    int nrfiles = fileName.length;
    int nrsheets = sheetName.length;
    int nrFields = field.length;

    retval.allocate(nrfiles, nrsheets, nrFields);

    for (int i = 0; i < nrFields; i++) {
      retval.field[i] = (ExcelInputField) field[i].clone();
    }

    System.arraycopy(fileName, 0, retval.fileName, 0, nrfiles);
    System.arraycopy(fileMask, 0, retval.fileMask, 0, nrfiles);
    System.arraycopy(excludeFileMask, 0, retval.excludeFileMask, 0, nrfiles);
    System.arraycopy(fileRequired, 0, retval.fileRequired, 0, nrfiles);
    System.arraycopy(includeSubFolders, 0, retval.includeSubFolders, 0, nrfiles);

    System.arraycopy(sheetName, 0, retval.sheetName, 0, nrsheets);
    System.arraycopy(startColumn, 0, retval.startColumn, 0, nrsheets);
    System.arraycopy(startRow, 0, retval.startRow, 0, nrsheets);

    return retval;
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      startsWithHeader = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "header"));
      String nempty = XmlHandler.getTagValue(transformNode, "noempty");
      ignoreEmptyRows = YES.equalsIgnoreCase(nempty) || nempty == null;
      String soempty = XmlHandler.getTagValue(transformNode, "stoponempty");
      stopOnEmpty = YES.equalsIgnoreCase(soempty) || nempty == null;
      sheetRowNumberField = XmlHandler.getTagValue(transformNode, "sheetrownumfield");
      rowNumberField = XmlHandler.getTagValue(transformNode, "rownum_field");
      rowNumberField = XmlHandler.getTagValue(transformNode, "rownumfield");
      rowLimit = Const.toLong(XmlHandler.getTagValue(transformNode, "limit"), 0);
      encoding = XmlHandler.getTagValue(transformNode, "encoding");
      String addToResult = XmlHandler.getTagValue(transformNode, "add_to_result_filenames");
      if (Utils.isEmpty(addToResult)) {
        isaddresult = true;
      } else {
        isaddresult = "Y".equalsIgnoreCase(addToResult);
      }
      sheetField = XmlHandler.getTagValue(transformNode, "sheetfield");
      fileField = XmlHandler.getTagValue(transformNode, "filefield");

      acceptingFilenames =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "accept_filenames"));
      acceptingField = XmlHandler.getTagValue(transformNode, "accept_field");
      acceptingTransformName = XmlHandler.getTagValue(transformNode, "accept_transform_name");

      Node filenode = XmlHandler.getSubNode(transformNode, "file");
      Node sheetsnode = XmlHandler.getSubNode(transformNode, "sheets");
      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrfiles = XmlHandler.countNodes(filenode, "name");
      int nrsheets = XmlHandler.countNodes(sheetsnode, "sheet");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrfiles, nrsheets, nrFields);

      for (int i = 0; i < nrfiles; i++) {
        Node filenamenode = XmlHandler.getSubNodeByNr(filenode, "name", i);
        Node filemasknode = XmlHandler.getSubNodeByNr(filenode, "filemask", i);
        Node excludefilemasknode = XmlHandler.getSubNodeByNr(filenode, "exclude_filemask", i);
        Node fileRequirednode = XmlHandler.getSubNodeByNr(filenode, "file_required", i);
        Node includeSubFoldersnode = XmlHandler.getSubNodeByNr(filenode, "include_subfolders", i);
        fileName[i] = XmlHandler.getNodeValue(filenamenode);
        fileMask[i] = XmlHandler.getNodeValue(filemasknode);
        excludeFileMask[i] = XmlHandler.getNodeValue(excludefilemasknode);
        fileRequired[i] = XmlHandler.getNodeValue(fileRequirednode);
        includeSubFolders[i] = XmlHandler.getNodeValue(includeSubFoldersnode);
      }

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
        field[i] = new ExcelInputField();

        field[i].setName(XmlHandler.getTagValue(fnode, "name"));
        field[i].setType(ValueMetaFactory.getIdForValueMeta(XmlHandler.getTagValue(fnode, "type")));
        field[i].setLength(Const.toInt(XmlHandler.getTagValue(fnode, "length"), -1));
        field[i].setPrecision(Const.toInt(XmlHandler.getTagValue(fnode, "precision"), -1));
        String srepeat = XmlHandler.getTagValue(fnode, "repeat");
        field[i].setTrimType(getTrimTypeByCode(XmlHandler.getTagValue(fnode, "trim_type")));

        if (srepeat != null) {
          field[i].setRepeated(YES.equalsIgnoreCase(srepeat));
        } else {
          field[i].setRepeated(false);
        }

        field[i].setFormat(XmlHandler.getTagValue(fnode, "format"));
        field[i].setCurrencySymbol(XmlHandler.getTagValue(fnode, "currency"));
        field[i].setDecimalSymbol(XmlHandler.getTagValue(fnode, "decimal"));
        field[i].setGroupSymbol(XmlHandler.getTagValue(fnode, "group"));
      }

      for (int i = 0; i < nrsheets; i++) {
        Node snode = XmlHandler.getSubNodeByNr(sheetsnode, "sheet", i);

        sheetName[i] = XmlHandler.getTagValue(snode, "name");
        startRow[i] = Const.toInt(XmlHandler.getTagValue(snode, "startrow"), 0);
        startColumn[i] = Const.toInt(XmlHandler.getTagValue(snode, "startcol"), 0);
      }

      strictTypes = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "strict_types"));
      errorIgnored = YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "error_ignored"));
      errorLineSkipped =
          YES.equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "error_line_skipped"));
      warningFilesDestinationDirectory =
          XmlHandler.getTagValue(transformNode, "bad_line_files_destination_directory");
      warningFilesExtension = XmlHandler.getTagValue(transformNode, "bad_line_files_extension");
      errorFilesDestinationDirectory =
          XmlHandler.getTagValue(transformNode, "error_line_files_destination_directory");
      errorFilesExtension = XmlHandler.getTagValue(transformNode, "error_line_files_extension");
      lineNumberFilesDestinationDirectory =
          XmlHandler.getTagValue(transformNode, "line_number_files_destination_directory");
      lineNumberFilesExtension =
          XmlHandler.getTagValue(transformNode, "line_number_files_extension");

      shortFileFieldName = XmlHandler.getTagValue(transformNode, "shortFileFieldName");
      pathFieldName = XmlHandler.getTagValue(transformNode, "pathFieldName");
      hiddenFieldName = XmlHandler.getTagValue(transformNode, "hiddenFieldName");
      lastModificationTimeFieldName =
          XmlHandler.getTagValue(transformNode, "lastModificationTimeFieldName");
      uriNameFieldName = XmlHandler.getTagValue(transformNode, "uriNameFieldName");
      rootUriNameFieldName = XmlHandler.getTagValue(transformNode, "rootUriNameFieldName");
      extensionFieldName = XmlHandler.getTagValue(transformNode, "extensionFieldName");
      sizeFieldName = XmlHandler.getTagValue(transformNode, "sizeFieldName");

      try {
        spreadSheetType =
            SpreadSheetType.valueOf(XmlHandler.getTagValue(transformNode, "spreadsheet_type"));
      } catch (Exception e) {
        spreadSheetType = SpreadSheetType.POI;
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to read transform information from XML", e);
    }
  }

  public String[] normilizeArray(String[] array, int length) {
    String[] newArray = new String[length];
    if (array != null) {
      if (array.length <= length) {
        System.arraycopy(array, 0, newArray, 0, array.length);
      } else {
        System.arraycopy(array, 0, newArray, 0, length);
      }
    }
    return newArray;
  }

  public int[] normilizeArray(int[] array, int length) {
    int[] newArray = new int[length];
    if (array != null) {
      if (array.length <= length) {
        System.arraycopy(array, 0, newArray, 0, array.length);
      } else {
        System.arraycopy(array, 0, newArray, 0, length);
      }
    }
    return newArray;
  }

  public void normilizeAllocation() {
    int nrfiles = 0;
    int nrsheets = 0;

    if (fileName != null) {
      nrfiles = fileName.length;
    } else {
      fileName = new String[0];
    }
    if (sheetName != null) {
      nrsheets = sheetName.length;
    } else {
      sheetName = new String[0];
    }
    if (field == null) {
      field = new ExcelInputField[0];
    }

    fileMask = normilizeArray(fileMask, nrfiles);
    excludeFileMask = normilizeArray(excludeFileMask, nrfiles);
    fileRequired = normilizeArray(fileRequired, nrfiles);
    includeSubFolders = normilizeArray(includeSubFolders, nrfiles);

    startRow = normilizeArray(startRow, nrsheets);
    startColumn = normilizeArray(startColumn, nrsheets);
  }

  public void allocate(int nrfiles, int nrsheets, int nrFields) {
    allocateFiles(nrfiles);
    sheetName = new String[nrsheets];
    startRow = new int[nrsheets];
    startColumn = new int[nrsheets];
    field = new ExcelInputField[nrFields];
  }

  public void allocateFiles(int nrfiles) {
    fileName = new String[nrfiles];
    fileMask = new String[nrfiles];
    excludeFileMask = new String[nrfiles];
    fileRequired = new String[nrfiles];
    includeSubFolders = new String[nrfiles];
  }

  @Override
  public void setDefault() {
    startsWithHeader = true;
    ignoreEmptyRows = true;
    rowNumberField = StringUtil.EMPTY_STRING;
    sheetRowNumberField = StringUtil.EMPTY_STRING;
    isaddresult = true;
    int nrfiles = 0;
    int nrFields = 0;
    int nrsheets = 0;

    allocate(nrfiles, nrsheets, nrFields);

    rowLimit = 0L;

    strictTypes = false;
    errorIgnored = false;
    errorLineSkipped = false;
    warningFilesDestinationDirectory = null;
    warningFilesExtension = "warning";
    errorFilesDestinationDirectory = null;
    errorFilesExtension = "error";
    lineNumberFilesDestinationDirectory = null;
    lineNumberFilesExtension = "line";

    spreadSheetType = SpreadSheetType.POI; // default.
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    for (int i = 0; i < field.length; i++) {
      int type = field[i].getType();
      if (type == IValueMeta.TYPE_NONE) {
        type = IValueMeta.TYPE_STRING;
      }
      try {
        IValueMeta v = ValueMetaFactory.createValueMeta(field[i].getName(), type);
        v.setLength(field[i].getLength());
        v.setPrecision(field[i].getPrecision());
        v.setOrigin(name);
        v.setConversionMask(field[i].getFormat());
        v.setDecimalSymbol(field[i].getDecimalSymbol());
        v.setGroupingSymbol(field[i].getGroupSymbol());
        v.setCurrencySymbol(field[i].getCurrencySymbol());
        row.addValueMeta(v);
      } catch (Exception e) {
        throw new HopTransformException(e);
      }
    }
    if (fileField != null && fileField.length() > 0) {
      IValueMeta v = new ValueMetaString(fileField);
      v.setLength(250);
      v.setPrecision(-1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (sheetField != null && sheetField.length() > 0) {
      IValueMeta v = new ValueMetaString(sheetField);
      v.setLength(250);
      v.setPrecision(-1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (sheetRowNumberField != null && sheetRowNumberField.length() > 0) {
      IValueMeta v = new ValueMetaInteger(sheetRowNumberField);
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (rowNumberField != null && rowNumberField.length() > 0) {
      IValueMeta v = new ValueMetaInteger(rowNumberField);
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    // Add additional fields

    if (getShortFileNameField() != null && getShortFileNameField().length() > 0) {
      IValueMeta v = new ValueMetaString(variables.resolve(getShortFileNameField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (getExtensionField() != null && getExtensionField().length() > 0) {
      IValueMeta v = new ValueMetaString(variables.resolve(getExtensionField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (getPathField() != null && getPathField().length() > 0) {
      IValueMeta v = new ValueMetaString(variables.resolve(getPathField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (getSizeField() != null && getSizeField().length() > 0) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(getSizeField()));
      v.setOrigin(name);
      v.setLength(9);
      row.addValueMeta(v);
    }
    if (isHiddenField() != null && isHiddenField().length() > 0) {
      IValueMeta v = new ValueMetaBoolean(variables.resolve(isHiddenField()));
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    if (getLastModificationDateField() != null && getLastModificationDateField().length() > 0) {
      IValueMeta v = new ValueMetaDate(variables.resolve(getLastModificationDateField()));
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (getUriField() != null && getUriField().length() > 0) {
      IValueMeta v = new ValueMetaString(variables.resolve(getUriField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    if (getRootUriField() != null && getRootUriField().length() > 0) {
      IValueMeta v = new ValueMetaString(variables.resolve(getRootUriField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(1024);
    normilizeAllocation();

    retval.append("    ").append(XmlHandler.addTagValue("header", startsWithHeader));
    retval.append("    ").append(XmlHandler.addTagValue("noempty", ignoreEmptyRows));
    retval.append("    ").append(XmlHandler.addTagValue("stoponempty", stopOnEmpty));
    retval.append("    ").append(XmlHandler.addTagValue("filefield", fileField));
    retval.append("    ").append(XmlHandler.addTagValue("sheetfield", sheetField));
    retval.append("    ").append(XmlHandler.addTagValue("sheetrownumfield", sheetRowNumberField));
    retval.append("    ").append(XmlHandler.addTagValue("rownumfield", rowNumberField));
    retval.append("    ").append(XmlHandler.addTagValue("sheetfield", sheetField));
    retval.append("    ").append(XmlHandler.addTagValue("filefield", fileField));
    retval.append("    ").append(XmlHandler.addTagValue("limit", rowLimit));
    retval.append("    ").append(XmlHandler.addTagValue("encoding", encoding));
    retval.append("    " + XmlHandler.addTagValue("add_to_result_filenames", isaddresult));

    retval.append("    ").append(XmlHandler.addTagValue("accept_filenames", acceptingFilenames));
    retval.append("    ").append(XmlHandler.addTagValue("accept_field", acceptingField));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "accept_transform_name",
                (acceptingTransform != null
                    ? acceptingTransform.getName()
                    : StringUtil.EMPTY_STRING)));

    /*
     * Describe the files to read
     */
    retval.append("    <file>").append(Const.CR);
    for (int i = 0; i < fileName.length; i++) {
      retval.append("      ").append(XmlHandler.addTagValue("name", fileName[i]));
      retval.append("      ").append(XmlHandler.addTagValue("filemask", fileMask[i]));
      retval
          .append("      ")
          .append(XmlHandler.addTagValue("exclude_filemask", excludeFileMask[i]));
      retval.append("      ").append(XmlHandler.addTagValue("file_required", fileRequired[i]));
      retval
          .append("      ")
          .append(XmlHandler.addTagValue("include_subfolders", includeSubFolders[i]));
    }
    retval.append("    </file>").append(Const.CR);

    /*
     * Describe the fields to read
     */
    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < field.length; i++) {
      retval.append("      <field>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", field[i].getName()));
      retval.append("        ").append(XmlHandler.addTagValue("type", field[i].getTypeDesc()));
      retval.append("        ").append(XmlHandler.addTagValue("length", field[i].getLength()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("precision", field[i].getPrecision()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("trim_type", field[i].getTrimTypeCode()));
      retval.append("        ").append(XmlHandler.addTagValue("repeat", field[i].isRepeated()));

      retval.append("        ").append(XmlHandler.addTagValue("format", field[i].getFormat()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("currency", field[i].getCurrencySymbol()));
      retval
          .append("        ")
          .append(XmlHandler.addTagValue("decimal", field[i].getDecimalSymbol()));
      retval.append("        ").append(XmlHandler.addTagValue("group", field[i].getGroupSymbol()));

      retval.append("      </field>").append(Const.CR);
    }
    retval.append("    </fields>").append(Const.CR);

    /*
     * Describe the sheets to load...
     */
    retval.append("    <sheets>").append(Const.CR);
    for (int i = 0; i < sheetName.length; i++) {
      retval.append("      <sheet>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", sheetName[i]));
      retval.append("        ").append(XmlHandler.addTagValue("startrow", startRow[i]));
      retval.append("        ").append(XmlHandler.addTagValue("startcol", startColumn[i]));
      retval.append("        </sheet>").append(Const.CR);
    }
    retval.append("    </sheets>").append(Const.CR);

    // ERROR HANDLING
    retval.append("    ").append(XmlHandler.addTagValue("strict_types", strictTypes));
    retval.append("    ").append(XmlHandler.addTagValue("error_ignored", errorIgnored));
    retval.append("    ").append(XmlHandler.addTagValue("error_line_skipped", errorLineSkipped));

    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "bad_line_files_destination_directory", warningFilesDestinationDirectory));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("bad_line_files_extension", warningFilesExtension));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "error_line_files_destination_directory", errorFilesDestinationDirectory));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("error_line_files_extension", errorFilesExtension));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "line_number_files_destination_directory", lineNumberFilesDestinationDirectory));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("line_number_files_extension", lineNumberFilesExtension));

    retval.append("    ").append(XmlHandler.addTagValue("shortFileFieldName", shortFileFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("pathFieldName", pathFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("hiddenFieldName", hiddenFieldName));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue("lastModificationTimeFieldName", lastModificationTimeFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("uriNameFieldName", uriNameFieldName));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("rootUriNameFieldName", rootUriNameFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("extensionFieldName", extensionFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("sizeFieldName", sizeFieldName));

    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "spreadsheet_type",
                (spreadSheetType != null ? spreadSheetType.toString() : StringUtil.EMPTY_STRING)));

    return retval.toString();
  }

  private String getValueOrEmptyIfNull(String str) {
    return str == null ? StringUtils.EMPTY : str;
  }

  public static final int getTrimTypeByCode(String tt) {
    if (tt != null) {
      for (int i = 0; i < type_trimCode.length; i++) {
        if (type_trimCode[i].equalsIgnoreCase(tt)) {
          return i;
        }
      }
    }
    return 0;
  }

  public static final int getTrimTypeByDesc(String tt) {
    if (tt != null) {
      for (int i = 0; i < type_trimDesc.length; i++) {
        if (type_trimDesc[i].equalsIgnoreCase(tt)) {
          return i;
        }
      }
    }
    return 0;
  }

  public static final String getTrimTypeCode(int i) {
    if (i < 0 || i >= type_trimCode.length) {
      return type_trimCode[0];
    }
    return type_trimCode[i];
  }

  public static final String getTrimTypeDesc(int i) {
    if (i < 0 || i >= type_trimDesc.length) {
      return type_trimDesc[0];
    }
    return type_trimDesc[i];
  }

  public String[] getFilePaths(IVariables variables) {
    normilizeAllocation();
    return FileInputList.createFilePathList(
        variables, fileName, fileMask, excludeFileMask, fileRequired, includeSubFolderBoolean());
  }

  public FileInputList getFileList(IVariables variables) {
    normilizeAllocation();
    return FileInputList.createFileList(
        variables, fileName, fileMask, excludeFileMask, fileRequired, includeSubFolderBoolean());
  }

  private boolean[] includeSubFolderBoolean() {
    normilizeAllocation();
    int len = fileName.length;
    boolean[] includeSubFolderBoolean = new boolean[len];
    for (int i = 0; i < len; i++) {
      includeSubFolderBoolean[i] = YES.equalsIgnoreCase(includeSubFolders[i]);
    }
    return includeSubFolderBoolean;
  }

  public String getLookupTransformName() {
    if (acceptingFilenames
        && acceptingTransform != null
        && !Utils.isEmpty(acceptingTransform.getName())) {
      return acceptingTransform.getName();
    }
    return null;
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    acceptingTransform = TransformMeta.findTransform(transforms, acceptingTransformName);
  }

  public String[] getInfoTransforms() {
    return null;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;

    // See if we get input...
    if (input.length > 0) {
      if (!isAcceptingFilenames()) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "ExcelInputMeta.CheckResult.NoInputError"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "ExcelInputMeta.CheckResult.AcceptFilenamesOk"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "ExcelInputMeta.CheckResult.NoInputOk"),
              transformMeta);
      remarks.add(cr);
    }

    FileInputList fileList = getFileList(variables);
    if (fileList.nrOfFiles() == 0) {
      if (!isAcceptingFilenames()) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "ExcelInputMeta.CheckResult.ExpectedFilesError"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "ExcelInputMeta.CheckResult.ExpectedFilesOk",
                  StringUtil.EMPTY_STRING + fileList.nrOfFiles()),
              transformMeta);
      remarks.add(cr);
    }
  }

  public IRowMeta getEmptyFields() {
    IRowMeta row = new RowMeta();
    if (field != null) {
      for (int i = 0; i < field.length; i++) {
        IValueMeta v;
        try {
          v = ValueMetaFactory.createValueMeta(field[i].getName(), field[i].getType());
        } catch (HopPluginException e) {
          v = new ValueMetaNone(field[i].getName());
        }
        row.addValueMeta(v);
      }
    }

    return row;
  }

  @Override
  public ExcelInput createTransform(
      TransformMeta transformMeta,
      ExcelInputData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ExcelInput(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public ExcelInputData getTransformData() {
    return new ExcelInputData();
  }

  public String getWarningFilesDestinationDirectory() {
    return warningFilesDestinationDirectory;
  }

  public void setWarningFilesDestinationDirectory(String badLineFilesDestinationDirectory) {
    this.warningFilesDestinationDirectory = badLineFilesDestinationDirectory;
  }

  public String getBadLineFilesExtension() {
    return warningFilesExtension;
  }

  public void setBadLineFilesExtension(String badLineFilesExtension) {
    this.warningFilesExtension = badLineFilesExtension;
  }

  public boolean isErrorIgnored() {
    return errorIgnored;
  }

  public void setErrorIgnored(boolean errorIgnored) {
    this.errorIgnored = errorIgnored;
  }

  public String getErrorFilesDestinationDirectory() {
    return errorFilesDestinationDirectory;
  }

  public void setErrorFilesDestinationDirectory(String errorLineFilesDestinationDirectory) {
    this.errorFilesDestinationDirectory = errorLineFilesDestinationDirectory;
  }

  public String getErrorFilesExtension() {
    return errorFilesExtension;
  }

  public void setErrorFilesExtension(String errorLineFilesExtension) {
    this.errorFilesExtension = errorLineFilesExtension;
  }

  public String getLineNumberFilesDestinationDirectory() {
    return lineNumberFilesDestinationDirectory;
  }

  public void setLineNumberFilesDestinationDirectory(String lineNumberFilesDestinationDirectory) {
    this.lineNumberFilesDestinationDirectory = lineNumberFilesDestinationDirectory;
  }

  public String getLineNumberFilesExtension() {
    return lineNumberFilesExtension;
  }

  public void setLineNumberFilesExtension(String lineNumberFilesExtension) {
    this.lineNumberFilesExtension = lineNumberFilesExtension;
  }

  public boolean isErrorLineSkipped() {
    return errorLineSkipped;
  }

  public void setErrorLineSkipped(boolean errorLineSkipped) {
    this.errorLineSkipped = errorLineSkipped;
  }

  public boolean isStrictTypes() {
    return strictTypes;
  }

  public void setStrictTypes(boolean strictTypes) {
    this.strictTypes = strictTypes;
  }

  public String[] getFileRequired() {
    return fileRequired;
  }

  public void setFileRequired(String[] fileRequiredin) {
    fileRequired = new String[fileRequiredin.length];
    for (int i = 0; i < fileRequiredin.length && i < fileRequired.length; i++) {
      this.fileRequired[i] = getRequiredFilesCode(fileRequiredin[i]);
    }
  }

  /** @return Returns the acceptingField. */
  public String getAcceptingField() {
    return acceptingField;
  }

  /** @param acceptingField The acceptingField to set. */
  public void setAcceptingField(String acceptingField) {
    this.acceptingField = acceptingField;
  }

  /** @return Returns the acceptingFilenames. */
  public boolean isAcceptingFilenames() {
    return acceptingFilenames;
  }

  /** @param acceptingFilenames The acceptingFilenames to set. */
  public void setAcceptingFilenames(boolean acceptingFilenames) {
    this.acceptingFilenames = acceptingFilenames;
  }

  /** @return Returns the acceptingTransform. */
  public TransformMeta getAcceptingTransform() {
    return acceptingTransform;
  }

  /** @param acceptingTransform The acceptingTransform to set. */
  public void setAcceptingTransform(TransformMeta acceptingTransform) {
    this.acceptingTransform = acceptingTransform;
  }

  /** @return Returns the acceptingTransformName. */
  public String getAcceptingTransformName() {
    return acceptingTransformName;
  }

  /** @param acceptingTransformName The acceptingTransformName to set. */
  public void setAcceptingTransformName(String acceptingTransformName) {
    this.acceptingTransformName = acceptingTransformName;
  }

  /** @return the encoding */
  public String getEncoding() {
    return encoding;
  }

  /** @param encoding the encoding to set */
  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /** @param isaddresult The isaddresult to set. */
  public void setAddResultFile(boolean isaddresult) {
    this.isaddresult = isaddresult;
  }

  /** @return Returns isaddresult. */
  public boolean isAddResultFile() {
    return isaddresult;
  }

  /**
   * Read all sheets if the sheet names are left blank.
   *
   * @return true if all sheets are read.
   */
  public boolean readAllSheets() {
    return Utils.isEmpty(sheetName) || (sheetName.length == 1 && Utils.isEmpty(sheetName[0]));
  }

  /**
   * @param variables the variable variables to use
   * @param definitions
   * @param iResourceNaming
   * @param metadataProvider the metadataProvider in which non-hop metadata could reside.
   * @return the filename of the exported resource
   */
  @Override
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming iResourceNaming,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      normilizeAllocation();
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous transforms, forget about this!
      //
      if (!acceptingFilenames) {

        // Replace the filename ONLY (folder or filename)
        //
        for (int i = 0; i < fileName.length; i++) {
          FileObject fileObject = HopVfs.getFileObject(variables.resolve(fileName[i]));
          fileName[i] =
              iResourceNaming.nameResource(fileObject, variables, Utils.isEmpty(fileMask[i]));
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  public SpreadSheetType getSpreadSheetType() {
    return spreadSheetType;
  }

  public void setSpreadSheetType(SpreadSheetType spreadSheetType) {
    this.spreadSheetType = spreadSheetType;
  }

  /**
   * If we use injection we can have different arrays lengths. We need synchronize them for
   * consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
    this.normilizeAllocation();
  }
}
