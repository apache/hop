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

package org.apache.hop.pipeline.transforms.loadfileinput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Transform(
    id = "LoadFileInput",
    image = "loadfileinput.svg",
    name = "i18n::BaseTransform.TypeLongDesc.LoadFileInput",
    description = "i18n::BaseTransform.TypeTooltipDesc.LoadFileInput",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/loadfileinput.html")
public class LoadFileInputMeta extends BaseTransformMeta
    implements ITransformMeta<LoadFileInput, LoadFileInputData> {
  private static final String INCLUDE = "include";
  private static final String INCLUDE_FIELD = "include_field";
  private static final String ROWNUM = "rownum";
  private static final String ADDRESULTFILE = "addresultfile";
  private static final String IS_IGNORE_EMPTY_FILE = "IsIgnoreEmptyFile";
  private static final String IS_IGNORE_MISSING_PATH = "IsIgnoreMissingPath";
  private static final String ROWNUM_FIELD = "rownum_field";
  private static final String ENCODING = "encoding";
  private static final String NAME = "name";
  private static final String FILEMASK = "filemask";
  private static final String EXCLUDE_FILEMASK = "exclude_filemask";
  private static final String FILE_REQUIRED = "file_required";
  private static final String INCLUDE_SUBFOLDERS = "include_subfolders";
  private static final String LIMIT = "limit";
  private static final String IS_IN_FIELDS = "IsInFields";
  private static final String DYNAMIC_FILENAME_FIELD = "DynamicFilenameField";
  private static final String SHORT_FILE_FIELD_NAME = "shortFileFieldName";
  private static final String PATH_FIELD_NAME = "pathFieldName";
  private static final String HIDDEN_FIELD_NAME = "hiddenFieldName";
  private static final String LAST_MODIFICATION_TIME_FIELD_NAME = "lastModificationTimeFieldName";
  private static final String URI_NAME_FIELD_NAME = "uriNameFieldName";
  private static final String ROOT_URI_NAME_FIELD_NAME = "rootUriNameFieldName";
  private static final String EXTENSION_FIELD_NAME = "extensionFieldName";
  private static final String FILE = "file";
  private static final String FIELDS = "fields";

  private static final Class<?> PKG = LoadFileInputMeta.class; // For Translator

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };
  public static final String[] RequiredFilesCode = new String[] {"N", "Y"};

  private static final String YES = "Y";

  /** Array of filenames */
  private String[] fileName;

  /** Wildcard or filemask (regular expression) */
  private String[] fileMask;

  /** Wildcard or filemask to exclude (regular expression) */
  private String[] excludeFileMask;

  /** Flag indicating that we should include the filename in the output */
  private boolean includeFilename;

  /** The name of the field in the output containing the filename */
  private String filenameField;

  /** Flag indicating that a row number field should be included in the output */
  private boolean includeRowNumber;

  /** The name of the field in the output containing the row number */
  private String rowNumberField;

  /** The maximum number or lines to read */
  private long rowLimit;

  /** The fields to import... */
  private LoadFileInputField[] inputFields;

  /** The encoding to use for reading: null or empty string means system default encoding */
  private String encoding;

  /** Dynamic FilenameField */
  private String DynamicFilenameField;

  /** Is In fields */
  private boolean fileinfield;

  /** Flag: add result filename */
  private boolean addresultfile;

  /** Array of boolean values as string, indicating if a file is required. */
  private String[] fileRequired;

  /** Flag : do we ignore empty file? */
  private boolean ignoreEmptyFile;

  /** Flag : do we ignore missing path? */
  private boolean ignoreMissingPath;

  /** Array of boolean values as string, indicating if we need to fetch sub folders. */
  private String[] includeSubFolders;

  /** Additional fields */
  private String shortFileFieldName;

  private String pathFieldName;
  private String hiddenFieldName;
  private String lastModificationTimeFieldName;
  private String uriNameFieldName;
  private String rootUriNameFieldName;
  private String extensionFieldName;

  public LoadFileInputMeta() {
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

  public String[] getFileRequired() {
    return fileRequired;
  }

  public void setFileRequired(String[] fileRequired) {
    this.fileRequired = fileRequired;
  }

  /** @deprecated typo in method name */
  @Deprecated
  public String[] getExludeFileMask() {
    return excludeFileMask;
  }

  /** @return Returns the excludeFileMask. */
  public String[] getExcludeFileMask() {
    return excludeFileMask;
  }

  /** @param excludeFileMask The excludeFileMask to set. */
  public void setExcludeFileMask(String[] excludeFileMask) {
    this.excludeFileMask = excludeFileMask;
  }

  /** @deprecated doesn't following naming standards */
  @Deprecated
  public boolean addResultFile() {
    return addresultfile;
  }

  /** @return the add result filesname flag */
  public boolean getAddResultFile() {
    return addresultfile;
  }

  /** @return the IsIgnoreEmptyFile flag */
  public boolean isIgnoreEmptyFile() {
    return ignoreEmptyFile;
  }

  /** @param isIgnoreEmptyFile IsIgnoreEmptyFile to set */
  public void setIgnoreEmptyFile(boolean isIgnoreEmptyFile) {
    this.ignoreEmptyFile = isIgnoreEmptyFile;
  }

  /** @return the IsIgnoreMissingPath flag */
  public boolean isIgnoreMissingPath() {
    return ignoreMissingPath;
  }

  /** @param ignoreMissingPath ignoreMissingPath to set */
  public void setIgnoreMissingPath(boolean ignoreMissingPath) {
    this.ignoreMissingPath = ignoreMissingPath;
  }

  public void setAddResultFile(boolean addresultfile) {
    this.addresultfile = addresultfile;
  }

  /** @return Returns the input fields. */
  public LoadFileInputField[] getInputFields() {
    return inputFields;
  }

  /** @param inputFields The input fields to set. */
  public void setInputFields(LoadFileInputField[] inputFields) {
    this.inputFields = inputFields;
  }

  /************************************
   * get and set FilenameField
   *************************************/
  /** */
  public String getDynamicFilenameField() {
    return DynamicFilenameField;
  }

  /** */
  public void setDynamicFilenameField(String DynamicFilenameField) {
    this.DynamicFilenameField = DynamicFilenameField;
  }

  /************************************
   * get / set fileInFields
   *************************************/
  /** */
  public boolean getFileInFields() {
    return fileinfield;
  }

  /************************************
   * @deprecated doesn't follow standard naming
   *************************************/
  @Deprecated
  public boolean getIsInFields() {
    return fileinfield;
  }

  /** @deprecated doesn't follow standard naming */
  @Deprecated
  public void setIsInFields(boolean IsInFields) {
    this.fileinfield = IsInFields;
  }

  public void setFileInFields(boolean IsInFields) {
    this.fileinfield = IsInFields;
  }

  /** @return Returns the fileMask. */
  public String[] getFileMask() {
    return fileMask;
  }

  /** @param fileMask The fileMask to set. */
  public void setFileMask(String[] fileMask) {
    this.fileMask = fileMask;
  }

  /** @return Returns the fileName. */
  public String[] getFileName() {
    return fileName;
  }

  public String[] getIncludeSubFolders() {
    return includeSubFolders;
  }

  public void setIncludeSubFolders(String[] includeSubFoldersin) {
    for (int i = 0; i < includeSubFoldersin.length; i++) {
      this.includeSubFolders[i] = getRequiredFilesCode(includeSubFoldersin[i]);
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

  /** @param fileName The fileName to set. */
  public void setFileName(String[] fileName) {
    this.fileName = fileName;
  }

  /** @return Returns the filenameField. */
  public String getFilenameField() {
    return filenameField;
  }

  /** @param filenameField The filenameField to set. */
  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }

  /**
   * @return Returns the includeFilename.
   * @deprecated doesn't follow standard naming
   */
  @Deprecated
  public boolean includeFilename() {
    return includeFilename;
  }

  /** @return Returns the includeFilename. */
  public boolean getIncludeFilename() {
    return includeFilename;
  }

  /** @param includeFilename The includeFilename to set. */
  public void setIncludeFilename(boolean includeFilename) {
    this.includeFilename = includeFilename;
  }

  /**
   * @return Returns the includeRowNumber.
   * @deprecated doesn't follow standard naming
   */
  @Deprecated
  public boolean includeRowNumber() {
    return includeRowNumber;
  }

  /** @return Returns the includeRowNumber. */
  public boolean getIncludeRowNumber() {
    return includeRowNumber;
  }

  /** @param includeRowNumber The includeRowNumber to set. */
  public void setIncludeRowNumber(boolean includeRowNumber) {
    this.includeRowNumber = includeRowNumber;
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

  /** @return the encoding */
  public String getEncoding() {
    return encoding;
  }

  /** @param encoding the encoding to set */
  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public Object clone() {
    LoadFileInputMeta retval = (LoadFileInputMeta) super.clone();

    int nrFiles = fileName.length;
    int nrFields = inputFields.length;

    retval.allocate(nrFiles, nrFields);
    System.arraycopy(fileName, 0, retval.fileName, 0, nrFiles);
    System.arraycopy(fileMask, 0, retval.fileMask, 0, nrFiles);
    System.arraycopy(excludeFileMask, 0, retval.excludeFileMask, 0, nrFiles);
    System.arraycopy(fileRequired, 0, retval.fileRequired, 0, nrFiles);
    System.arraycopy(includeSubFolders, 0, retval.includeSubFolders, 0, nrFiles);

    for (int i = 0; i < nrFields; i++) {
      if (inputFields[i] != null) {
        retval.inputFields[i] = (LoadFileInputField) inputFields[i].clone();
      }
    }
    return retval;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    " + XmlHandler.addTagValue(INCLUDE, includeFilename));
    retval.append("    " + XmlHandler.addTagValue(INCLUDE_FIELD, filenameField));
    retval.append("    " + XmlHandler.addTagValue(ROWNUM, includeRowNumber));
    retval.append("    " + XmlHandler.addTagValue(ADDRESULTFILE, addresultfile));
    retval.append("    " + XmlHandler.addTagValue(IS_IGNORE_EMPTY_FILE, ignoreEmptyFile));
    retval.append("    " + XmlHandler.addTagValue(IS_IGNORE_MISSING_PATH, ignoreMissingPath));

    retval.append("    " + XmlHandler.addTagValue(ROWNUM_FIELD, rowNumberField));
    retval.append("    " + XmlHandler.addTagValue(ENCODING, encoding));

    retval.append("    <" + FILE + ">" + Const.CR);
    for (int i = 0; i < fileName.length; i++) {
      retval.append("      " + XmlHandler.addTagValue(NAME, fileName[i]));
      retval.append("      " + XmlHandler.addTagValue(FILEMASK, fileMask[i]));
      retval.append("      ").append(XmlHandler.addTagValue(EXCLUDE_FILEMASK, excludeFileMask[i]));
      retval.append("      ").append(XmlHandler.addTagValue(FILE_REQUIRED, fileRequired[i]));
      retval.append("      " + XmlHandler.addTagValue(INCLUDE_SUBFOLDERS, includeSubFolders[i]));
    }
    retval.append("      </" + FILE + ">" + Const.CR);

    retval.append("    <" + FIELDS + ">" + Const.CR);
    for (int i = 0; i < inputFields.length; i++) {
      LoadFileInputField field = inputFields[i];
      retval.append(field.getXml());
    }
    retval.append("      </" + FIELDS + ">" + Const.CR);
    retval.append("    " + XmlHandler.addTagValue(LIMIT, rowLimit));
    retval.append("    " + XmlHandler.addTagValue(IS_IN_FIELDS, fileinfield));
    retval.append("    " + XmlHandler.addTagValue(DYNAMIC_FILENAME_FIELD, DynamicFilenameField));
    retval.append("    ").append(XmlHandler.addTagValue(SHORT_FILE_FIELD_NAME, shortFileFieldName));
    retval.append("    ").append(XmlHandler.addTagValue(PATH_FIELD_NAME, pathFieldName));
    retval.append("    ").append(XmlHandler.addTagValue(HIDDEN_FIELD_NAME, hiddenFieldName));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                LAST_MODIFICATION_TIME_FIELD_NAME, lastModificationTimeFieldName));
    retval.append("    ").append(XmlHandler.addTagValue(URI_NAME_FIELD_NAME, uriNameFieldName));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue(ROOT_URI_NAME_FIELD_NAME, rootUriNameFieldName));
    retval.append("    ").append(XmlHandler.addTagValue(EXTENSION_FIELD_NAME, extensionFieldName));

    return retval.toString();
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      includeFilename = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, INCLUDE));
      filenameField = XmlHandler.getTagValue(transformNode, INCLUDE_FIELD);

      addresultfile = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, ADDRESULTFILE));
      ignoreEmptyFile =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, IS_IGNORE_EMPTY_FILE));
      ignoreMissingPath =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, IS_IGNORE_MISSING_PATH));

      includeRowNumber = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, ROWNUM));
      rowNumberField = XmlHandler.getTagValue(transformNode, ROWNUM_FIELD);
      encoding = XmlHandler.getTagValue(transformNode, ENCODING);

      Node filenode = XmlHandler.getSubNode(transformNode, FILE);
      Node fields = XmlHandler.getSubNode(transformNode, FIELDS);
      int nrFiles = XmlHandler.countNodes(filenode, NAME);
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFiles, nrFields);

      for (int i = 0; i < nrFiles; i++) {
        Node filenamenode = XmlHandler.getSubNodeByNr(filenode, NAME, i);
        Node filemasknode = XmlHandler.getSubNodeByNr(filenode, FILEMASK, i);
        Node excludefilemasknode = XmlHandler.getSubNodeByNr(filenode, EXCLUDE_FILEMASK, i);
        Node fileRequirednode = XmlHandler.getSubNodeByNr(filenode, FILE_REQUIRED, i);
        Node includeSubFoldersnode = XmlHandler.getSubNodeByNr(filenode, INCLUDE_SUBFOLDERS, i);
        fileName[i] = XmlHandler.getNodeValue(filenamenode);
        fileMask[i] = XmlHandler.getNodeValue(filemasknode);
        excludeFileMask[i] = XmlHandler.getNodeValue(excludefilemasknode);
        fileRequired[i] = XmlHandler.getNodeValue(fileRequirednode);
        includeSubFolders[i] = XmlHandler.getNodeValue(includeSubFoldersnode);
      }

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
        LoadFileInputField field = new LoadFileInputField(fnode);
        inputFields[i] = field;
      }

      // Is there a limit on the number of rows we process?
      rowLimit = Const.toLong(XmlHandler.getTagValue(transformNode, LIMIT), 0L);

      fileinfield = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, IS_IN_FIELDS));

      DynamicFilenameField = XmlHandler.getTagValue(transformNode, DYNAMIC_FILENAME_FIELD);
      shortFileFieldName = XmlHandler.getTagValue(transformNode, SHORT_FILE_FIELD_NAME);
      pathFieldName = XmlHandler.getTagValue(transformNode, PATH_FIELD_NAME);
      hiddenFieldName = XmlHandler.getTagValue(transformNode, HIDDEN_FIELD_NAME);
      lastModificationTimeFieldName =
          XmlHandler.getTagValue(transformNode, LAST_MODIFICATION_TIME_FIELD_NAME);
      uriNameFieldName = XmlHandler.getTagValue(transformNode, URI_NAME_FIELD_NAME);
      rootUriNameFieldName = XmlHandler.getTagValue(transformNode, ROOT_URI_NAME_FIELD_NAME);
      extensionFieldName = XmlHandler.getTagValue(transformNode, EXTENSION_FIELD_NAME);

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "LoadFileInputMeta.Exception.ErrorLoadingXml", e.toString()));
    }
  }

  public void allocate(int nrfiles, int nrFields) {
    fileName = new String[nrfiles];
    fileMask = new String[nrfiles];
    excludeFileMask = new String[nrfiles];
    fileRequired = new String[nrfiles];
    includeSubFolders = new String[nrfiles];
    inputFields = new LoadFileInputField[nrFields];
  }

  public void setDefault() {
    shortFileFieldName = null;
    pathFieldName = null;
    hiddenFieldName = null;
    lastModificationTimeFieldName = null;
    uriNameFieldName = null;
    rootUriNameFieldName = null;
    extensionFieldName = null;

    encoding = "";
    ignoreEmptyFile = false;
    ignoreMissingPath = false;
    includeFilename = false;
    filenameField = "";
    includeRowNumber = false;
    rowNumberField = "";
    addresultfile = true;

    int nrFiles = 0;
    int nrFields = 0;

    allocate(nrFiles, nrFields);

    for (int i = 0; i < nrFiles; i++) {
      fileName[i] = "filename" + (i + 1);
      fileMask[i] = "";
      excludeFileMask[i] = "";
      fileRequired[i] = RequiredFilesCode[0];
      includeSubFolders[i] = RequiredFilesCode[0];
    }

    for (int i = 0; i < nrFields; i++) {
      inputFields[i] = new LoadFileInputField("field" + (i + 1));
    }

    rowLimit = 0;

    fileinfield = false;
    DynamicFilenameField = null;
  }

  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (!getIsInFields()) {
      r.clear();
    }
    int i;
    for (i = 0; i < inputFields.length; i++) {
      LoadFileInputField field = inputFields[i];
      int type = field.getType();

      switch (field.getElementType()) {
        case LoadFileInputField.ELEMENT_TYPE_FILECONTENT:
          if (type == IValueMeta.TYPE_NONE) {
            type = IValueMeta.TYPE_STRING;
          }
          break;
        case LoadFileInputField.ELEMENT_TYPE_FILESIZE:
          if (type == IValueMeta.TYPE_NONE) {
            type = IValueMeta.TYPE_INTEGER;
          }
          break;
        default:
          break;
      }

      try {
        IValueMeta v = ValueMetaFactory.createValueMeta(variables.resolve(field.getName()), type);
        v.setLength(field.getLength());
        v.setPrecision(field.getPrecision());
        v.setConversionMask(field.getFormat());
        v.setCurrencySymbol(field.getCurrencySymbol());
        v.setDecimalSymbol(field.getDecimalSymbol());
        v.setGroupingSymbol(field.getGroupSymbol());
        v.setTrimType(field.getTrimType());
        v.setOrigin(name);
        r.addValueMeta(v);
      } catch (Exception e) {
        throw new HopTransformException(e);
      }
    }
    if (includeFilename) {
      IValueMeta v = new ValueMetaString(variables.resolve(filenameField));
      v.setLength(250);
      v.setPrecision(-1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (includeRowNumber) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(rowNumberField));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    // Add additional fields

    if (getShortFileNameField() != null && getShortFileNameField().length() > 0) {
      IValueMeta v = new ValueMetaString(variables.resolve(getShortFileNameField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (getExtensionField() != null && getExtensionField().length() > 0) {
      IValueMeta v = new ValueMetaString(variables.resolve(getExtensionField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (getPathField() != null && getPathField().length() > 0) {
      IValueMeta v = new ValueMetaString(variables.resolve(getPathField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (isHiddenField() != null && isHiddenField().length() > 0) {
      IValueMeta v = new ValueMetaBoolean(variables.resolve(isHiddenField()));
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (getLastModificationDateField() != null && getLastModificationDateField().length() > 0) {
      IValueMeta v = new ValueMetaDate(variables.resolve(getLastModificationDateField()));
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (getUriField() != null && getUriField().length() > 0) {
      IValueMeta v = new ValueMetaString(variables.resolve(getUriField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (getRootUriField() != null && getRootUriField().length() > 0) {
      IValueMeta v = new ValueMetaString(variables.resolve(getRootUriField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
  }

  public FileInputList getFiles(IVariables variables) {
    return FileInputList.createFileList(
        variables, fileName, fileMask, excludeFileMask, fileRequired, includeSubFolderBoolean());
  }

  private boolean[] includeSubFolderBoolean() {
    int len = fileName.length;
    boolean[] includeSubFolderBoolean = new boolean[len];
    for (int i = 0; i < len; i++) {
      includeSubFolderBoolean[i] = YES.equalsIgnoreCase(includeSubFolders[i]);
    }
    return includeSubFolderBoolean;
  }

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

    if (getIsInFields()) {
      // See if we get input...
      if (input.length == 0) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "LoadFileInputMeta.CheckResult.NoInputExpected"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "LoadFileInputMeta.CheckResult.NoInput"),
                transformMeta);
        remarks.add(cr);
      }

      if (Utils.isEmpty(getDynamicFilenameField())) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "LoadFileInputMeta.CheckResult.NoField"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "LoadFileInputMeta.CheckResult.FieldOk"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      FileInputList fileInputList = getFiles(variables);

      if (fileInputList == null || fileInputList.getFiles().size() == 0) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "LoadFileInputMeta.CheckResult.NoFiles"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "LoadFileInputMeta.CheckResult.FilesOk",
                    "" + fileInputList.getFiles().size()),
                transformMeta);
        remarks.add(cr);
      }
    }
  }

  /**
   * @param variables the variable variables to use
   * @param definitions
   * @param iResourceNaming
   * @param metadataProvider the metadataProvider in which non-hop metadata could reside.
   * @return the filename of the exported resource
   */
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming iResourceNaming,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      //
      if (!fileinfield) {
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

  public LoadFileInput createTransform(
      TransformMeta transformMeta,
      LoadFileInputData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new LoadFileInput(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public LoadFileInputData getTransformData() {
    return new LoadFileInputData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LoadFileInputMeta)) {
      return false;
    }
    LoadFileInputMeta that = (LoadFileInputMeta) o;

    if (ignoreEmptyFile != that.ignoreEmptyFile) {
      return false;
    }
    if (ignoreMissingPath != that.ignoreMissingPath) {
      return false;
    }
    if (addresultfile != that.addresultfile) {
      return false;
    }
    if (fileinfield != that.fileinfield) {
      return false;
    }
    if (includeFilename != that.includeFilename) {
      return false;
    }
    if (includeRowNumber != that.includeRowNumber) {
      return false;
    }
    if (rowLimit != that.rowLimit) {
      return false;
    }
    if (DynamicFilenameField != null
        ? !DynamicFilenameField.equals(that.DynamicFilenameField)
        : that.DynamicFilenameField != null) {
      return false;
    }
    if (encoding != null ? !encoding.equals(that.encoding) : that.encoding != null) {
      return false;
    }
    if (!Arrays.equals(excludeFileMask, that.excludeFileMask)) {
      return false;
    }
    if (extensionFieldName != null
        ? !extensionFieldName.equals(that.extensionFieldName)
        : that.extensionFieldName != null) {
      return false;
    }
    if (!Arrays.equals(fileMask, that.fileMask)) {
      return false;
    }
    if (!Arrays.equals(fileName, that.fileName)) {
      return false;
    }
    if (!Arrays.equals(fileRequired, that.fileRequired)) {
      return false;
    }
    if (filenameField != null
        ? !filenameField.equals(that.filenameField)
        : that.filenameField != null) {
      return false;
    }
    if (hiddenFieldName != null
        ? !hiddenFieldName.equals(that.hiddenFieldName)
        : that.hiddenFieldName != null) {
      return false;
    }
    if (!Arrays.equals(includeSubFolders, that.includeSubFolders)) {
      return false;
    }
    if (!Arrays.equals(inputFields, that.inputFields)) {
      return false;
    }
    if (lastModificationTimeFieldName != null
        ? !lastModificationTimeFieldName.equals(that.lastModificationTimeFieldName)
        : that.lastModificationTimeFieldName != null) {
      return false;
    }
    if (pathFieldName != null
        ? !pathFieldName.equals(that.pathFieldName)
        : that.pathFieldName != null) {
      return false;
    }
    if (rootUriNameFieldName != null
        ? !rootUriNameFieldName.equals(that.rootUriNameFieldName)
        : that.rootUriNameFieldName != null) {
      return false;
    }
    if (rowNumberField != null
        ? !rowNumberField.equals(that.rowNumberField)
        : that.rowNumberField != null) {
      return false;
    }
    if (shortFileFieldName != null
        ? !shortFileFieldName.equals(that.shortFileFieldName)
        : that.shortFileFieldName != null) {
      return false;
    }
    return !(uriNameFieldName != null
        ? !uriNameFieldName.equals(that.uriNameFieldName)
        : that.uriNameFieldName != null);
  }

  @Override
  public int hashCode() {
    int result = fileName != null ? Arrays.hashCode(fileName) : 0;
    result = 31 * result + (fileMask != null ? Arrays.hashCode(fileMask) : 0);
    result = 31 * result + (excludeFileMask != null ? Arrays.hashCode(excludeFileMask) : 0);
    result = 31 * result + (includeFilename ? 1 : 0);
    result = 31 * result + (filenameField != null ? filenameField.hashCode() : 0);
    result = 31 * result + (includeRowNumber ? 1 : 0);
    result = 31 * result + (rowNumberField != null ? rowNumberField.hashCode() : 0);
    result = 31 * result + (int) (rowLimit ^ (rowLimit >>> 32));
    result = 31 * result + (inputFields != null ? Arrays.hashCode(inputFields) : 0);
    result = 31 * result + (encoding != null ? encoding.hashCode() : 0);
    result = 31 * result + (DynamicFilenameField != null ? DynamicFilenameField.hashCode() : 0);
    result = 31 * result + (fileinfield ? 1 : 0);
    result = 31 * result + (addresultfile ? 1 : 0);
    result = 31 * result + (fileRequired != null ? Arrays.hashCode(fileRequired) : 0);
    result = 31 * result + (ignoreEmptyFile ? 1 : 0);
    result = 31 * result + (ignoreMissingPath ? 1 : 0);
    result = 31 * result + (includeSubFolders != null ? Arrays.hashCode(includeSubFolders) : 0);
    result = 31 * result + (shortFileFieldName != null ? shortFileFieldName.hashCode() : 0);
    result = 31 * result + (pathFieldName != null ? pathFieldName.hashCode() : 0);
    result = 31 * result + (hiddenFieldName != null ? hiddenFieldName.hashCode() : 0);
    result =
        31 * result
            + (lastModificationTimeFieldName != null
                ? lastModificationTimeFieldName.hashCode()
                : 0);
    result = 31 * result + (uriNameFieldName != null ? uriNameFieldName.hashCode() : 0);
    result = 31 * result + (rootUriNameFieldName != null ? rootUriNameFieldName.hashCode() : 0);
    result = 31 * result + (extensionFieldName != null ? extensionFieldName.hashCode() : 0);
    return result;
  }
}
