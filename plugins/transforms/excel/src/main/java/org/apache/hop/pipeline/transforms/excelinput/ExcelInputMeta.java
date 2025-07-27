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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.FileInputList;
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
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;

/** Meta data for the Excel transform. */
@Transform(
    id = "ExcelInput",
    image = "excelinput.svg",
    name = "i18n::ExcelInput.Name",
    description = "i18n::ExcelInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::ExcelInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/excelinput.html")
public class ExcelInputMeta extends BaseTransformMeta<ExcelInput, ExcelInputData> {
  private static final Class<?> PKG = ExcelInputMeta.class;

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };
  public static final String[] RequiredFilesCode = new String[] {"N", "Y"};

  @HopMetadataProperty(
      key = "file",
      injectionGroupKey = "FILENAME_LINES",
      injectionGroupDescription = "ExcelInput.Injection.FILENAME_LINES",
      inlineListTags = {
        "name",
        "filemask",
        "exclude_filemask",
        "file_required",
        "include_subfolders"
      })
  private List<EIFile> files;

  /** The fieldname that holds the name of the file */
  @HopMetadataProperty(key = "filefield")
  private String fileField;

  @HopMetadataProperty(
      groupKey = "sheets",
      key = "sheet",
      injectionKey = "SHEET",
      injectionKeyDescription = "ExcelInput.Injection.SHEET",
      injectionGroupKey = "SHEETS",
      injectionGroupDescription = "ExcelInput.Injection.SHEETS")
  private List<EISheet> sheets;

  /** The fieldname that holds the name of the sheet */
  @HopMetadataProperty(key = "sheetfield", injectionKeyDescription = "Sheet name field")
  private String sheetField;

  /** The cell-range starts with a header-row */
  @HopMetadataProperty(
      key = "header",
      injectionKeyDescription = "The cell-range starts with a header-row?")
  private boolean startsWithHeader;

  @HopMetadataProperty(
      key = "schema_definition",
      injectionKeyDescription = "The fields schema definition")
  private String schemaDefinition;

  /** Stop reading when you hit an empty row. */
  @HopMetadataProperty(
      key = "stoponempty",
      injectionKeyDescription = "Stop reading when you hit an empty row")
  private boolean stopOnEmpty;

  /** Avoid empty rows in the result. */
  @HopMetadataProperty(key = "noempty", injectionKeyDescription = "Avoid empty rows in the result")
  private boolean ignoreEmptyRows;

  /**
   * The fieldname containing the row number. An empty (null) value means that no row number is
   * included in the output. This is the rownumber of all written rows (not the row in the sheet).
   */
  @HopMetadataProperty(
      key = "rownumfield",
      injectionKeyDescription = "The field name containing the row number.")
  private String rowNumberField;

  /**
   * The fieldname containing the sheet row number. An empty (null) value means that no sheet row
   * number is included in the output. Sheet row number is the row number in the sheet.
   */
  @HopMetadataProperty(
      key = "sheetrownumfield",
      injectionKeyDescription = "The field name containing the sheet row number")
  private String sheetRowNumberField;

  /** The maximum number of rows that this transform writes to the next transform. */
  @HopMetadataProperty(
      key = "limit",
      injectionKeyDescription =
          "The maximum number of rows that this transform writes to the next transform")
  private long rowLimit;

  /**
   * The fields to read in the range. Note: the number of columns in the range has to match
   * field.length
   */
  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "ExcelInput.Injection.FIELDS")
  private List<ExcelInputField> fields;

  /** Strict types : will generate errors */
  @HopMetadataProperty(
      key = "strict_types",
      injectionKeyDescription = "Strict types : data conversion errors are thrown")
  private boolean strictTypes;

  /** Ignore error : turn into warnings */
  @HopMetadataProperty(
      key = "error_ignored",
      injectionKeyDescription = "Turn errors into warnings, ignoring them")
  private boolean errorIgnored;

  /** If error line are skipped, you can replay without introducing doubles. */
  @HopMetadataProperty(key = "error_line_skipped", injectionKeyDescription = "Skip error lines?")
  private boolean errorLineSkipped;

  /** The directory that will contain warning files */
  @HopMetadataProperty(
      key = "bad_line_files_destination_directory",
      injectionKeyDescription = "The directory that will contain warning files")
  private String warningFilesDestinationDirectory;

  /** The extension of warning files */
  @HopMetadataProperty(
      key = "bad_line_files_extension",
      injectionKeyDescription = "The extension of warning files")
  private String warningFilesExtension;

  /** The directory that will contain error files */
  @HopMetadataProperty(
      key = "error_line_files_destination_directory",
      injectionKeyDescription = "The directory that will contain error files")
  private String errorFilesDestinationDirectory;

  /** The extension of error files */
  @HopMetadataProperty(
      key = "error_line_files_extension",
      injectionKeyDescription = "The extension of error files")
  private String errorFilesExtension;

  /** The directory that will contain line number files */
  @HopMetadataProperty(
      key = "line_number_files_destination_directory",
      injectionKeyDescription = "The directory that will contain line number files")
  private String lineNumberFilesDestinationDirectory;

  /** The extension of line number files */
  @HopMetadataProperty(
      key = "line_number_files_extension",
      injectionKeyDescription = "The extension of line number files")
  private String lineNumberFilesExtension;

  /** Are we accepting filenames in input rows? */
  @HopMetadataProperty(
      key = "accept_filenames",
      injectionKeyDescription = "Are we accepting filenames in input rows?")
  private boolean acceptingFilenames;

  /** The field in which the filename is placed */
  @HopMetadataProperty(
      key = "accept_field",
      injectionKeyDescription = "The field in which the filename is placed")
  private String acceptingField;

  /** The transformName to accept filenames from */
  @HopMetadataProperty(
      key = "accept_transform_name",
      injectionKeyDescription = "The transform name to accept filenames from")
  private String acceptingTransformName;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @HopMetadataProperty(
      key = "encoding",
      injectionKeyDescription =
          "The encoding to use for reading: null or empty string means system default encoding")
  private String encoding;

  /** The add filenames to result filenames flag */
  @HopMetadataProperty(
      key = "add_to_result_filenames",
      injectionKeyDescription = "Add filenames to result?")
  private boolean addFilenamesToResult;

  /** Additional fields */
  @HopMetadataProperty(
      key = "shortFileFieldName",
      injectionKeyDescription = "Extra output: short file field name")
  private String shortFileFieldName;

  @HopMetadataProperty(
      key = "pathFieldName",
      injectionKeyDescription = "Extra output: path field name")
  private String pathFieldName;

  @HopMetadataProperty(
      key = "hiddenFieldName",
      injectionKeyDescription = "Extra output: hidden flag field name")
  private String hiddenFieldName;

  @HopMetadataProperty(
      key = "lastModificationTimeFieldName",
      injectionKeyDescription = "Extra output: last modification time field name")
  private String lastModificationTimeFieldName;

  @HopMetadataProperty(
      key = "uriNameFieldName",
      injectionKeyDescription = "Extra output: URI field name")
  private String uriNameFieldName;

  @HopMetadataProperty(
      key = "rootUriNameFieldName",
      injectionKeyDescription = "Extra output: root URI field name")
  private String rootUriNameFieldName;

  @HopMetadataProperty(
      key = "extensionFieldName",
      injectionKeyDescription = "Extra output: extension field name")
  private String extensionFieldName;

  @HopMetadataProperty(
      key = "sizeFieldName",
      injectionKeyDescription = "Extra output: file size field name")
  private String sizeFieldName;

  @HopMetadataProperty(
      enumNameWhenNotFound = "POI",
      key = "spreadsheet_type",
      injectionKey = "SPREADSHEET_TYPE",
      injectionKeyDescription = "ExcelInput.Injection.SPREADSHEET_TYPE")
  private SpreadSheetType spreadSheetType;

  public ExcelInputMeta() {
    super();
    this.fields = new ArrayList<>();
    this.files = new ArrayList<>();
    this.sheets = new ArrayList<>();
  }

  public ExcelInputMeta(ExcelInputMeta m) {
    this();
    m.fields.forEach(f -> this.fields.add(new ExcelInputField(f)));
    m.sheets.forEach(s -> this.sheets.add(new EISheet(s)));
    m.files.forEach(f -> this.files.add(new EIFile(f)));
    this.fileField = m.fileField;
    this.sheetField = m.sheetField;
    this.startsWithHeader = m.startsWithHeader;
    this.schemaDefinition = m.schemaDefinition;
    this.stopOnEmpty = m.stopOnEmpty;
    this.ignoreEmptyRows = m.ignoreEmptyRows;
    this.rowNumberField = m.rowNumberField;
    this.sheetRowNumberField = m.sheetRowNumberField;
    this.rowLimit = m.rowLimit;
    this.strictTypes = m.strictTypes;
    this.errorIgnored = m.errorIgnored;
    this.errorLineSkipped = m.errorLineSkipped;
    this.warningFilesDestinationDirectory = m.warningFilesDestinationDirectory;
    this.warningFilesExtension = m.warningFilesExtension;
    this.errorFilesDestinationDirectory = m.errorFilesDestinationDirectory;
    this.errorFilesExtension = m.errorFilesExtension;
    this.lineNumberFilesDestinationDirectory = m.lineNumberFilesDestinationDirectory;
    this.lineNumberFilesExtension = m.lineNumberFilesExtension;
    this.acceptingFilenames = m.acceptingFilenames;
    this.acceptingField = m.acceptingField;
    this.acceptingTransformName = m.acceptingTransformName;
    this.encoding = m.encoding;
    this.addFilenamesToResult = m.addFilenamesToResult;
    this.shortFileFieldName = m.shortFileFieldName;
    this.pathFieldName = m.pathFieldName;
    this.hiddenFieldName = m.hiddenFieldName;
    this.lastModificationTimeFieldName = m.lastModificationTimeFieldName;
    this.uriNameFieldName = m.uriNameFieldName;
    this.rootUriNameFieldName = m.rootUriNameFieldName;
    this.extensionFieldName = m.extensionFieldName;
    this.sizeFieldName = m.sizeFieldName;
    this.spreadSheetType = m.spreadSheetType;
  }

  @Override
  public ExcelInputMeta clone() {
    return new ExcelInputMeta(this);
  }

  @Override
  public void setDefault() {
    startsWithHeader = true;
    ignoreEmptyRows = true;
    rowNumberField = StringUtil.EMPTY_STRING;
    sheetRowNumberField = StringUtil.EMPTY_STRING;
    addFilenamesToResult = true;

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

    spreadSheetType = SpreadSheetType.SAX_POI; // default.
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

    // Clean the row if eventually is dirty
    row.clear();

    for (ExcelInputField field : fields) {
      int type = field.getHopType();
      if (type == IValueMeta.TYPE_NONE) {
        type = IValueMeta.TYPE_STRING;
      }
      try {
        IValueMeta v = ValueMetaFactory.createValueMeta(field.getName(), type);
        v.setLength(field.getLength());
        v.setPrecision(field.getPrecision());
        v.setOrigin(name);
        v.setConversionMask(field.getFormat());
        v.setDecimalSymbol(field.getDecimalSymbol());
        v.setGroupingSymbol(field.getGroupSymbol());
        v.setCurrencySymbol(field.getCurrencySymbol());
        row.addValueMeta(v);
      } catch (Exception e) {
        throw new HopTransformException(e);
      }
    }
    if (fileField != null && !fileField.isEmpty()) {
      IValueMeta v = new ValueMetaString(fileField);
      v.setLength(250);
      v.setPrecision(-1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (sheetField != null && !sheetField.isEmpty()) {
      IValueMeta v = new ValueMetaString(sheetField);
      v.setLength(250);
      v.setPrecision(-1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (sheetRowNumberField != null && !sheetRowNumberField.isEmpty()) {
      IValueMeta v = new ValueMetaInteger(sheetRowNumberField);
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (rowNumberField != null && !rowNumberField.isEmpty()) {
      IValueMeta v = new ValueMetaInteger(rowNumberField);
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    // Add additional fields
    //
    if (StringUtils.isNotEmpty(getShortFileFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getShortFileFieldName()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(getExtensionFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getExtensionFieldName()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(getPathFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getPathFieldName()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(getSizeFieldName())) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(getSizeFieldName()));
      v.setOrigin(name);
      v.setLength(9);
      row.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(getHiddenFieldName())) {
      IValueMeta v = new ValueMetaBoolean(variables.resolve(getHiddenFieldName()));
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    if (StringUtils.isNotEmpty(getLastModificationTimeFieldName())) {
      IValueMeta v = new ValueMetaDate(variables.resolve(getLastModificationTimeFieldName()));
      v.setOrigin(name);
      row.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(getUriNameFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getUriNameFieldName()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }

    if (StringUtils.isNotEmpty(getRootUriNameFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getRootUriNameFieldName()));
      v.setLength(100, -1);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  private String[] getFilesNames() {
    String[] fileName = new String[files.size()];
    for (int i = 0; i < files.size(); i++) {
      EIFile file = files.get(i);
      fileName[i] = file.getName();
    }
    return fileName;
  }

  private String[] getFilesMasks() {
    String[] fileMask = new String[files.size()];
    for (int i = 0; i < files.size(); i++) {
      EIFile file = files.get(i);
      fileMask[i] = file.getMask();
    }
    return fileMask;
  }

  private String[] getFilesExcludeMasks() {
    String[] excludeFileMask = new String[files.size()];
    for (int i = 0; i < files.size(); i++) {
      EIFile file = files.get(i);
      excludeFileMask[i] = file.getExcludeMask();
    }
    return excludeFileMask;
  }

  private String[] getFilesRequired() {
    String[] fileRequired = new String[files.size()];
    for (int i = 0; i < files.size(); i++) {
      EIFile file = files.get(i);
      fileRequired[i] = file.getRequired();
    }
    return fileRequired;
  }

  private boolean[] getFilesIncludeSubFolders() {
    boolean[] includeSub = new boolean[files.size()];
    for (int i = 0; i < files.size(); i++) {
      EIFile file = files.get(i);
      includeSub[i] = Const.toBoolean(file.getIncludeSubFolders());
    }
    return includeSub;
  }

  public String[] getFilePaths(IVariables variables) {
    return FileInputList.createFilePathList(
        variables,
        getFilesNames(),
        getFilesMasks(),
        getFilesExcludeMasks(),
        getFilesRequired(),
        getFilesIncludeSubFolders());
  }

  public FileInputList getFileList(IVariables variables) {
    return FileInputList.createFileList(
        variables,
        getFilesNames(),
        getFilesMasks(),
        getFilesExcludeMasks(),
        getFilesRequired(),
        getFilesIncludeSubFolders());
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

    for (ExcelInputField field : fields) {
      IValueMeta v;
      try {
        v = ValueMetaFactory.createValueMeta(field.getName(), field.getHopType());
      } catch (HopPluginException e) {
        v = new ValueMetaNone(field.getName());
      }
      row.addValueMeta(v);
    }

    return row;
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

  /**
   * @return Returns the acceptingField.
   */
  public String getAcceptingField() {
    return acceptingField;
  }

  /**
   * @param acceptingField The acceptingField to set.
   */
  public void setAcceptingField(String acceptingField) {
    this.acceptingField = acceptingField;
  }

  /**
   * @return Returns the acceptingFilenames.
   */
  public boolean isAcceptingFilenames() {
    return acceptingFilenames;
  }

  /**
   * @param acceptingFilenames The acceptingFilenames to set.
   */
  public void setAcceptingFilenames(boolean acceptingFilenames) {
    this.acceptingFilenames = acceptingFilenames;
  }

  /**
   * @return Returns the acceptingTransformName.
   */
  public String getAcceptingTransformName() {
    return acceptingTransformName;
  }

  /**
   * @param acceptingTransformName The acceptingTransformName to set.
   */
  public void setAcceptingTransformName(String acceptingTransformName) {
    this.acceptingTransformName = acceptingTransformName;
  }

  /**
   * @return the encoding
   */
  public String getEncoding() {
    return encoding;
  }

  /**
   * @param encoding the encoding to set
   */
  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /**
   * @param isaddresult The isaddresult to set.
   */
  public void setAddResultFile(boolean isaddresult) {
    this.addFilenamesToResult = isaddresult;
  }

  /**
   * @return Returns isaddresult.
   */
  public boolean isAddResultFile() {
    return addFilenamesToResult;
  }

  /**
   * Read all sheets if the sheet names are left blank.
   *
   * @return true if all sheets are read.
   */
  public boolean readAllSheets() {
    return sheets.isEmpty() || (sheets.size() == 1 && StringUtils.isEmpty(sheets.get(0).getName()));
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
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous transforms, forget about this!
      //
      if (!acceptingFilenames) {

        // Replace the filename ONLY (folder or filename)
        //
        for (EIFile file : files) {
          FileObject fileObject =
              HopVfs.getFileObject(variables.resolve(file.getName()), variables);
          file.setName(
              iResourceNaming.nameResource(fileObject, variables, Utils.isEmpty(file.getMask())));
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  public String[] getSheetsNames() {
    String[] names = new String[sheets.size()];
    for (int i = 0; i < sheets.size(); i++) {
      names[i] = sheets.get(i).getName();
    }
    return names;
  }

  public int[] getSheetsStartColumns() {
    int[] columns = new int[sheets.size()];
    for (int i = 0; i < sheets.size(); i++) {
      columns[i] = sheets.get(i).getStartColumn();
    }
    return columns;
  }

  public int[] getSheetsStartRows() {
    int[] rows = new int[sheets.size()];
    for (int i = 0; i < sheets.size(); i++) {
      rows[i] = sheets.get(i).getStartRow();
    }
    return rows;
  }

  public static class EIFile {
    @HopMetadataProperty(
        key = "name",
        injectionKey = "FILENAME",
        injectionKeyDescription = "ExcelInput.Injection.FILENAME")
    private String name;

    @HopMetadataProperty(
        key = "filemask",
        injectionKey = "FILEMASK",
        injectionKeyDescription = "ExcelInput.Injection.FILEMASK")
    private String mask;

    @HopMetadataProperty(
        key = "exclude_filemask",
        injectionKey = "EXCLUDE_FILEMASK",
        injectionKeyDescription = "ExcelInput.Injection.EXCLUDE_FILEMASK")
    private String excludeMask;

    @HopMetadataProperty(
        key = "file_required",
        injectionKey = "FILE_REQUIRED",
        injectionKeyDescription = "ExcelInput.Injection.FILE_REQUIRED")
    private String required;

    @HopMetadataProperty(
        key = "include_subfolders",
        injectionKey = "INCLUDE_SUBFOLDERS",
        injectionKeyDescription = "ExcelInput.Injection.INCLUDE_SUBFOLDERS")
    private String includeSubFolders;

    public EIFile() {}

    public EIFile(EIFile f) {
      this();
      this.name = f.name;
      this.mask = f.mask;
      this.excludeMask = f.excludeMask;
      this.required = f.required;
      this.includeSubFolders = f.includeSubFolders;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Gets mask
     *
     * @return value of mask
     */
    public String getMask() {
      return mask;
    }

    /**
     * Sets mask
     *
     * @param mask value of mask
     */
    public void setMask(String mask) {
      this.mask = mask;
    }

    /**
     * Gets excludeMask
     *
     * @return value of excludeMask
     */
    public String getExcludeMask() {
      return excludeMask;
    }

    /**
     * Sets excludeMask
     *
     * @param excludeMask value of excludeMask
     */
    public void setExcludeMask(String excludeMask) {
      this.excludeMask = excludeMask;
    }

    /**
     * Gets required
     *
     * @return value of required
     */
    public String getRequired() {
      return required;
    }

    /**
     * Sets required
     *
     * @param required value of required
     */
    public void setRequired(String required) {
      this.required = required;
    }

    /**
     * Gets includeSubFolders
     *
     * @return value of includeSubFolders
     */
    public String getIncludeSubFolders() {
      return includeSubFolders;
    }

    /**
     * Sets includeSubFolders
     *
     * @param includeSubFolders value of includeSubFolders
     */
    public void setIncludeSubFolders(String includeSubFolders) {
      this.includeSubFolders = includeSubFolders;
    }
  }

  public static class EISheet {
    @HopMetadataProperty(
        key = "name",
        injectionKey = "SHEET_NAME",
        injectionKeyDescription = "ExcelInput.Injection.SHEET_NAME")
    private String name;

    @HopMetadataProperty(
        key = "startrow",
        injectionKey = "SHEET_START_ROW",
        injectionKeyDescription = "ExcelInput.Injection.SHEET_START_ROW")
    private int startRow;

    @HopMetadataProperty(
        key = "startcol",
        injectionKey = "SHEET_START_COL",
        injectionKeyDescription = "ExcelInput.Injection.SHEET_START_COL")
    private int startColumn;

    public EISheet() {}

    public EISheet(EISheet s) {
      this.name = s.name;
      this.startRow = s.startRow;
      this.startColumn = s.startColumn;
    }

    /**
     * Gets name
     *
     * @return value of name
     */
    public String getName() {
      return name;
    }

    /**
     * Sets name
     *
     * @param name value of name
     */
    public void setName(String name) {
      this.name = name;
    }

    /**
     * Gets startRow
     *
     * @return value of startRow
     */
    public int getStartRow() {
      return startRow;
    }

    /**
     * Sets startRow
     *
     * @param startRow value of startRow
     */
    public void setStartRow(int startRow) {
      this.startRow = startRow;
    }

    /**
     * Gets startColumn
     *
     * @return value of startColumn
     */
    public int getStartColumn() {
      return startColumn;
    }

    /**
     * Sets startColumn
     *
     * @param startColumn value of startColumn
     */
    public void setStartColumn(int startColumn) {
      this.startColumn = startColumn;
    }
  }

  /**
   * Gets files
   *
   * @return value of files
   */
  public List<EIFile> getFiles() {
    return files;
  }

  /**
   * Sets files
   *
   * @param files value of files
   */
  public void setFiles(List<EIFile> files) {
    this.files = files;
  }

  /**
   * Gets fileField
   *
   * @return value of fileField
   */
  public String getFileField() {
    return fileField;
  }

  /**
   * Sets fileField
   *
   * @param fileField value of fileField
   */
  public void setFileField(String fileField) {
    this.fileField = fileField;
  }

  /**
   * Gets sheets
   *
   * @return value of sheets
   */
  public List<EISheet> getSheets() {
    return sheets;
  }

  /**
   * Sets sheets
   *
   * @param sheets value of sheets
   */
  public void setSheets(List<EISheet> sheets) {
    this.sheets = sheets;
  }

  /**
   * Gets sheetField
   *
   * @return value of sheetField
   */
  public String getSheetField() {
    return sheetField;
  }

  /**
   * Sets sheetField
   *
   * @param sheetField value of sheetField
   */
  public void setSheetField(String sheetField) {
    this.sheetField = sheetField;
  }

  /**
   * Gets startsWithHeader
   *
   * @return value of startsWithHeader
   */
  public boolean isStartsWithHeader() {
    return startsWithHeader;
  }

  /**
   * Sets startsWithHeader
   *
   * @param startsWithHeader value of startsWithHeader
   */
  public void setStartsWithHeader(boolean startsWithHeader) {
    this.startsWithHeader = startsWithHeader;
  }

  /**
   * Gets stopOnEmpty
   *
   * @return value of stopOnEmpty
   */
  public boolean isStopOnEmpty() {
    return stopOnEmpty;
  }

  /**
   * Sets stopOnEmpty
   *
   * @param stopOnEmpty value of stopOnEmpty
   */
  public void setStopOnEmpty(boolean stopOnEmpty) {
    this.stopOnEmpty = stopOnEmpty;
  }

  /**
   * Gets ignoreEmptyRows
   *
   * @return value of ignoreEmptyRows
   */
  public boolean isIgnoreEmptyRows() {
    return ignoreEmptyRows;
  }

  /**
   * Sets ignoreEmptyRows
   *
   * @param ignoreEmptyRows value of ignoreEmptyRows
   */
  public void setIgnoreEmptyRows(boolean ignoreEmptyRows) {
    this.ignoreEmptyRows = ignoreEmptyRows;
  }

  /**
   * Gets rowNumberField
   *
   * @return value of rowNumberField
   */
  public String getRowNumberField() {
    return rowNumberField;
  }

  /**
   * Sets rowNumberField
   *
   * @param rowNumberField value of rowNumberField
   */
  public void setRowNumberField(String rowNumberField) {
    this.rowNumberField = rowNumberField;
  }

  /**
   * Gets sheetRowNumberField
   *
   * @return value of sheetRowNumberField
   */
  public String getSheetRowNumberField() {
    return sheetRowNumberField;
  }

  /**
   * Sets sheetRowNumberField
   *
   * @param sheetRowNumberField value of sheetRowNumberField
   */
  public void setSheetRowNumberField(String sheetRowNumberField) {
    this.sheetRowNumberField = sheetRowNumberField;
  }

  /**
   * Gets rowLimit
   *
   * @return value of rowLimit
   */
  public long getRowLimit() {
    return rowLimit;
  }

  /**
   * Sets rowLimit
   *
   * @param rowLimit value of rowLimit
   */
  public void setRowLimit(long rowLimit) {
    this.rowLimit = rowLimit;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<ExcelInputField> getFields() {
    return fields;
  }

  /**
   * Sets fields
   *
   * @param fields value of fields
   */
  public void setFields(List<ExcelInputField> fields) {
    this.fields = fields;
  }

  /**
   * Gets warningFilesExtension
   *
   * @return value of warningFilesExtension
   */
  public String getWarningFilesExtension() {
    return warningFilesExtension;
  }

  /**
   * Sets warningFilesExtension
   *
   * @param warningFilesExtension value of warningFilesExtension
   */
  public void setWarningFilesExtension(String warningFilesExtension) {
    this.warningFilesExtension = warningFilesExtension;
  }

  /**
   * Gets isaddresult
   *
   * @return value of isaddresult
   */
  public boolean isAddFilenamesToResult() {
    return addFilenamesToResult;
  }

  /**
   * Sets isaddresult
   *
   * @param addFilenamesToResult value of isaddresult
   */
  public void setAddFilenamesToResult(boolean addFilenamesToResult) {
    this.addFilenamesToResult = addFilenamesToResult;
  }

  /**
   * Gets shortFileFieldName
   *
   * @return value of shortFileFieldName
   */
  public String getShortFileFieldName() {
    return shortFileFieldName;
  }

  /**
   * Sets shortFileFieldName
   *
   * @param shortFileFieldName value of shortFileFieldName
   */
  public void setShortFileFieldName(String shortFileFieldName) {
    this.shortFileFieldName = shortFileFieldName;
  }

  /**
   * Gets pathFieldName
   *
   * @return value of pathFieldName
   */
  public String getPathFieldName() {
    return pathFieldName;
  }

  /**
   * Sets pathFieldName
   *
   * @param pathFieldName value of pathFieldName
   */
  public void setPathFieldName(String pathFieldName) {
    this.pathFieldName = pathFieldName;
  }

  /**
   * Gets hiddenFieldName
   *
   * @return value of hiddenFieldName
   */
  public String getHiddenFieldName() {
    return hiddenFieldName;
  }

  /**
   * Sets hiddenFieldName
   *
   * @param hiddenFieldName value of hiddenFieldName
   */
  public void setHiddenFieldName(String hiddenFieldName) {
    this.hiddenFieldName = hiddenFieldName;
  }

  /**
   * Gets lastModificationTimeFieldName
   *
   * @return value of lastModificationTimeFieldName
   */
  public String getLastModificationTimeFieldName() {
    return lastModificationTimeFieldName;
  }

  /**
   * Sets lastModificationTimeFieldName
   *
   * @param lastModificationTimeFieldName value of lastModificationTimeFieldName
   */
  public void setLastModificationTimeFieldName(String lastModificationTimeFieldName) {
    this.lastModificationTimeFieldName = lastModificationTimeFieldName;
  }

  /**
   * Gets uriNameFieldName
   *
   * @return value of uriNameFieldName
   */
  public String getUriNameFieldName() {
    return uriNameFieldName;
  }

  /**
   * Sets uriNameFieldName
   *
   * @param uriNameFieldName value of uriNameFieldName
   */
  public void setUriNameFieldName(String uriNameFieldName) {
    this.uriNameFieldName = uriNameFieldName;
  }

  /**
   * Gets rootUriNameFieldName
   *
   * @return value of rootUriNameFieldName
   */
  public String getRootUriNameFieldName() {
    return rootUriNameFieldName;
  }

  /**
   * Sets rootUriNameFieldName
   *
   * @param rootUriNameFieldName value of rootUriNameFieldName
   */
  public void setRootUriNameFieldName(String rootUriNameFieldName) {
    this.rootUriNameFieldName = rootUriNameFieldName;
  }

  /**
   * Gets extensionFieldName
   *
   * @return value of extensionFieldName
   */
  public String getExtensionFieldName() {
    return extensionFieldName;
  }

  /**
   * Sets extensionFieldName
   *
   * @param extensionFieldName value of extensionFieldName
   */
  public void setExtensionFieldName(String extensionFieldName) {
    this.extensionFieldName = extensionFieldName;
  }

  /**
   * Gets sizeFieldName
   *
   * @return value of sizeFieldName
   */
  public String getSizeFieldName() {
    return sizeFieldName;
  }

  /**
   * Sets sizeFieldName
   *
   * @param sizeFieldName value of sizeFieldName
   */
  public void setSizeFieldName(String sizeFieldName) {
    this.sizeFieldName = sizeFieldName;
  }

  public String getSchemaDefinition() {
    return schemaDefinition;
  }

  public void setSchemaDefinition(String schemaDefinition) {
    this.schemaDefinition = schemaDefinition;
  }

  /**
   * Gets spreadSheetType
   *
   * @return value of spreadSheetType
   */
  public SpreadSheetType getSpreadSheetType() {
    return spreadSheetType;
  }

  /**
   * Sets spreadSheetType
   *
   * @param spreadSheetType value of spreadSheetType
   */
  public void setSpreadSheetType(SpreadSheetType spreadSheetType) {
    this.spreadSheetType = spreadSheetType;
  }
}
