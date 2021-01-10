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

package org.apache.hop.pipeline.transforms.xml.getxmldata;

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
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Store run-time data on the getXMLData transform. */
@Transform(
    id = "getXMLData",
    image = "GXD.svg",
    name = "i18n::GetXMLData.name",
    description = "i18n::GetXMLData.description",
    categoryDescription = "i18n::GetXMLData.category",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/getxmldata.html")
public class GetXmlDataMeta extends BaseTransformMeta
    implements ITransformMeta<GetXmlData, GetXmlDataData> {
  private static final Class<?> PKG = GetXmlDataMeta.class; // For Translator

  private static final String YES = "Y";

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };
  public static final String[] RequiredFilesCode = new String[] {"N", "Y"};

  public static final String AT = "@";
  public static final String N0DE_SEPARATOR = "/";

  /** Array of filenames */
  private String[] fileName;

  /** Wildcard or filemask (regular expression) */
  private String[] fileMask;

  /** Array of boolean values as string, indicating if a file is required. */
  private String[] fileRequired;

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

  /** The XPath location to loop over */
  private String loopxpath;

  /** The fields to import... */
  private GetXmlDataField[] inputFields;

  /** The encoding to use for reading: null or empty string means system default encoding */
  private String encoding;

  /** Is In fields */
  private String xmlField;

  /** Is In fields */
  private boolean inFields;

  /** Is a File */
  private boolean IsAFile;

  /** Flag: add result filename * */
  private boolean addResultFile;

  /** Flag: set Namespace aware * */
  private boolean nameSpaceAware;

  /** Flag: set XML Validating * */
  private boolean validating;

  /** Flag : do we process use tokens? */
  private boolean usetoken;

  /** Flag : do we ignore empty files */
  private boolean IsIgnoreEmptyFile;

  /** Array of boolean values as string, indicating if we need to fetch sub folders. */
  private String[] includeSubFolders;

  /** Flag : do not fail if no file */
  private boolean doNotFailIfNoFile;

  /** Flag : ignore comments */
  private boolean ignorecomments;

  /** Flag : read url as source */
  private boolean readurl;

  // Given this path activates the streaming algorithm to process large files
  private String prunePath;

  /** Additional fields * */
  private String shortFileFieldName;

  private String pathFieldName;
  private String hiddenFieldName;
  private String lastModificationTimeFieldName;
  private String uriNameFieldName;
  private String rootUriNameFieldName;
  private String extensionFieldName;
  private String sizeFieldName;

  public GetXmlDataMeta() {
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

  /** @return the add result filesname flag */
  public boolean addResultFile() {
    return addResultFile;
  }

  /** @return the validating flag */
  public boolean isValidating() {
    return validating;
  }

  /** @param validating the validating flag to set */
  public void setValidating(boolean validating) {
    this.validating = validating;
  }

  /** @return the readurl flag */
  public boolean isReadUrl() {
    return readurl;
  }

  /** @param readurl the readurl flag to set */
  public void setReadUrl(boolean readurl) {
    this.readurl = readurl;
  }

  public void setAddResultFile(boolean addResultFile) {
    this.addResultFile = addResultFile;
  }

  /** @return Returns the input fields. */
  public GetXmlDataField[] getInputFields() {
    return inputFields;
  }

  /** @param inputFields The input fields to set. */
  public void setInputFields(GetXmlDataField[] inputFields) {
    this.inputFields = inputFields;
  }

  /** @return Returns the excludeFileMask. */
  public String[] getExludeFileMask() {
    return excludeFileMask;
  }

  /** @param excludeFileMask The excludeFileMask to set. */
  public void setExcludeFileMask(String[] excludeFileMask) {
    this.excludeFileMask = excludeFileMask;
  }

  /** Get XML field. */
  public String getXMLField() {
    return xmlField;
  }

  /** Set XML field. */
  public void setXMLField(String xmlField) {
    this.xmlField = xmlField;
  }

  /** Get the IsInFields. */
  public boolean isInFields() {
    return inFields;
  }

  /** @param inFields set the inFields. */
  public void setInFields(boolean inFields) {
    this.inFields = inFields;
  }

  /** @return Returns the fileMask. */
  public String[] getFileMask() {
    return fileMask;
  }

  /** @param fileMask The fileMask to set. */
  public void setFileMask(String[] fileMask) {
    this.fileMask = fileMask;
  }

  public String[] getFileRequired() {
    return fileRequired;
  }

  public void setFileRequired(String[] fileRequiredin) {
    for (int i = 0; i < fileRequiredin.length; i++) {
      this.fileRequired[i] = getRequiredFilesCode(fileRequiredin[i]);
    }
  }

  public void setIncludeSubFolders(String[] includeSubFoldersin) {
    for (int i = 0; i < includeSubFoldersin.length; i++) {
      this.includeSubFolders[i] = getRequiredFilesCode(includeSubFoldersin[i]);
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

  /** @return Returns the filenameField. */
  public String getFilenameField() {
    return filenameField;
  }

  /** @param filenameField The filenameField to set. */
  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }

  /** @return Returns the includeFilename. */
  public boolean includeFilename() {
    return includeFilename;
  }

  /** @param includeFilename The includeFilename to set. */
  public void setIncludeFilename(boolean includeFilename) {
    this.includeFilename = includeFilename;
  }

  /** @return Returns the includeRowNumber. */
  public boolean includeRowNumber() {
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

  /** @return Returns the LoopXPath */
  public String getLoopXPath() {
    return loopxpath;
  }

  /** @param loopxpath The loopxpath to set. */
  public void setLoopXPath(String loopxpath) {
    this.loopxpath = loopxpath;
  }

  /** @param usetoken the "use token" flag to set */
  public void setuseToken(boolean usetoken) {
    this.usetoken = usetoken;
  }

  /** @return the use token flag */
  public boolean isuseToken() {
    return usetoken;
  }

  /** @return the IsIgnoreEmptyFile flag */
  public boolean isIgnoreEmptyFile() {
    return IsIgnoreEmptyFile;
  }

  /** @param IsIgnoreEmptyFile the IsIgnoreEmptyFile to set */
  public void setIgnoreEmptyFile(boolean IsIgnoreEmptyFile) {
    this.IsIgnoreEmptyFile = IsIgnoreEmptyFile;
  }

  /** @return the doNotFailIfNoFile flag */
  public boolean isdoNotFailIfNoFile() {
    return doNotFailIfNoFile;
  }

  /** @param doNotFailIfNoFile the doNotFailIfNoFile to set */
  public void setdoNotFailIfNoFile(boolean doNotFailIfNoFile) {
    this.doNotFailIfNoFile = doNotFailIfNoFile;
  }

  /** @return the ignorecomments flag */
  public boolean isIgnoreComments() {
    return ignorecomments;
  }

  /** @param ignorecomments the ignorecomments to set */
  public void setIgnoreComments(boolean ignorecomments) {
    this.ignorecomments = ignorecomments;
  }

  /** @param nameSpaceAware the name variables aware flag to set */
  public void setNamespaceAware(boolean nameSpaceAware) {
    this.nameSpaceAware = nameSpaceAware;
  }

  /** @return the name variables aware flag */
  public boolean isNamespaceAware() {
    return nameSpaceAware;
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

  public boolean getIsAFile() {
    return IsAFile;
  }

  public void setIsAFile(boolean IsAFile) {
    this.IsAFile = IsAFile;
  }

  /** @return the prunePath */
  public String getPrunePath() {
    return prunePath;
  }

  public String[] getIncludeSubFolders() {
    return includeSubFolders;
  }

  /** @param prunePath the prunePath to set */
  public void setPrunePath(String prunePath) {
    this.prunePath = prunePath;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public Object clone() {
    GetXmlDataMeta retval = (GetXmlDataMeta) super.clone();

    int nrFiles = fileName.length;
    int nrFields = inputFields.length;

    retval.allocate(nrFiles, nrFields);

    for (int i = 0; i < nrFiles; i++) {
      retval.fileName[i] = fileName[i];
      retval.fileMask[i] = fileMask[i];
      retval.excludeFileMask[i] = excludeFileMask[i];
      retval.fileRequired[i] = fileRequired[i];
      retval.includeSubFolders[i] = includeSubFolders[i];
    }

    for (int i = 0; i < nrFields; i++) {
      if (inputFields[i] != null) {
        retval.inputFields[i] = (GetXmlDataField) inputFields[i].clone();
      }
    }
    return retval;
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      GetXmlDataData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new GetXmlData(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public GetXmlDataData getTransformData() {
    return new GetXmlDataData();
  }

  public String getXml() {
    StringBuffer retval = new StringBuffer(400);

    retval.append("    ").append(XmlHandler.addTagValue("include", includeFilename));
    retval.append("    ").append(XmlHandler.addTagValue("include_field", filenameField));
    retval.append("    ").append(XmlHandler.addTagValue("rownum", includeRowNumber));
    retval.append("    ").append(XmlHandler.addTagValue("addresultfile", addResultFile));
    retval.append("    ").append(XmlHandler.addTagValue("namespaceaware", nameSpaceAware));
    retval.append("    ").append(XmlHandler.addTagValue("ignorecomments", ignorecomments));
    retval.append("    ").append(XmlHandler.addTagValue("readurl", readurl));
    retval.append("    ").append(XmlHandler.addTagValue("validating", validating));
    retval.append("    " + XmlHandler.addTagValue("usetoken", usetoken));
    retval.append("    " + XmlHandler.addTagValue("IsIgnoreEmptyFile", IsIgnoreEmptyFile));
    retval.append("    " + XmlHandler.addTagValue("doNotFailIfNoFile", doNotFailIfNoFile));

    retval.append("    ").append(XmlHandler.addTagValue("rownum_field", rowNumberField));
    retval.append("    ").append(XmlHandler.addTagValue("encoding", encoding));

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

    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < inputFields.length; i++) {
      GetXmlDataField field = inputFields[i];
      retval.append(field.getXml());
    }
    retval.append("    </fields>").append(Const.CR);

    retval.append("    ").append(XmlHandler.addTagValue("limit", rowLimit));
    retval.append("    ").append(XmlHandler.addTagValue("loopxpath", loopxpath));
    retval.append("    ").append(XmlHandler.addTagValue("IsInFields", inFields));
    retval.append("    ").append(XmlHandler.addTagValue("IsAFile", IsAFile));
    retval.append("    ").append(XmlHandler.addTagValue("XmlField", xmlField));
    retval.append("    ").append(XmlHandler.addTagValue("prunePath", prunePath));
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
    return retval.toString();
  }

  public String getRequiredFilesDesc(String tt) {
    if (Utils.isEmpty(tt)) {
      return RequiredFilesDesc[0];
    }
    if (tt.equalsIgnoreCase(RequiredFilesCode[1])) {
      return RequiredFilesDesc[1];
    } else {
      return RequiredFilesDesc[0];
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

  private void readData(Node transformNode) throws HopXmlException {
    try {
      includeFilename = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "include"));
      filenameField = XmlHandler.getTagValue(transformNode, "include_field");

      addResultFile = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "addresultfile"));
      nameSpaceAware =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "namespaceaware"));
      ignorecomments =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "ignorecomments"));

      readurl = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "readurl"));
      validating = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "validating"));
      usetoken = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "usetoken"));
      IsIgnoreEmptyFile =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "IsIgnoreEmptyFile"));
      doNotFailIfNoFile =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "doNotFailIfNoFile"));

      includeRowNumber = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "rownum"));
      rowNumberField = XmlHandler.getTagValue(transformNode, "rownum_field");
      encoding = XmlHandler.getTagValue(transformNode, "encoding");

      Node filenode = XmlHandler.getSubNode(transformNode, "file");
      Node fields = XmlHandler.getSubNode(transformNode, "fields");
      int nrFiles = XmlHandler.countNodes(filenode, "name");
      int nrFields = XmlHandler.countNodes(fields, "field");

      allocate(nrFiles, nrFields);

      for (int i = 0; i < nrFiles; i++) {
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
        GetXmlDataField field = new GetXmlDataField(fnode);
        inputFields[i] = field;
      }

      // Is there a limit on the number of rows we process?
      rowLimit = Const.toLong(XmlHandler.getTagValue(transformNode, "limit"), 0L);
      // Do we skip rows before starting to read
      loopxpath = XmlHandler.getTagValue(transformNode, "loopxpath");

      inFields = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "IsInFields"));
      IsAFile = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "IsAFile"));

      xmlField = XmlHandler.getTagValue(transformNode, "XmlField");
      prunePath = XmlHandler.getTagValue(transformNode, "prunePath");

      shortFileFieldName = XmlHandler.getTagValue(transformNode, "shortFileFieldName");
      pathFieldName = XmlHandler.getTagValue(transformNode, "pathFieldName");
      hiddenFieldName = XmlHandler.getTagValue(transformNode, "hiddenFieldName");
      lastModificationTimeFieldName =
          XmlHandler.getTagValue(transformNode, "lastModificationTimeFieldName");
      uriNameFieldName = XmlHandler.getTagValue(transformNode, "uriNameFieldName");
      rootUriNameFieldName = XmlHandler.getTagValue(transformNode, "rootUriNameFieldName");
      extensionFieldName = XmlHandler.getTagValue(transformNode, "extensionFieldName");
      sizeFieldName = XmlHandler.getTagValue(transformNode, "sizeFieldName");
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "GetXMLDataMeta.Exception.ErrorLoadingXml", e.toString()));
    }
  }

  public void allocate(int nrfiles, int nrFields) {
    allocateFiles(nrfiles);
    inputFields = new GetXmlDataField[nrFields];
  }

  public void allocateFiles(int nrfiles) {
    fileName = new String[nrfiles];
    fileMask = new String[nrfiles];
    excludeFileMask = new String[nrfiles];
    fileRequired = new String[nrfiles];
    includeSubFolders = new String[nrfiles];
  }

  public void setDefault() {
    shortFileFieldName = null;
    pathFieldName = null;
    hiddenFieldName = null;
    lastModificationTimeFieldName = null;
    uriNameFieldName = null;
    rootUriNameFieldName = null;
    extensionFieldName = null;
    sizeFieldName = null;

    usetoken = false;
    IsIgnoreEmptyFile = false;
    doNotFailIfNoFile = true;
    includeFilename = false;
    filenameField = "";
    includeRowNumber = false;
    rowNumberField = "";
    IsAFile = false;
    addResultFile = false;
    nameSpaceAware = false;
    ignorecomments = false;
    readurl = false;
    validating = false;

    int nrFiles = 0;
    int nrFields = 0;
    loopxpath = "";

    allocate(nrFiles, nrFields);

    for (int i = 0; i < nrFiles; i++) {
      fileName[i] = "filename" + (i + 1);
      fileMask[i] = "";
      excludeFileMask[i] = "";
      fileRequired[i] = RequiredFilesCode[0];
      includeSubFolders[i] = RequiredFilesCode[0];
    }

    for (int i = 0; i < nrFields; i++) {
      inputFields[i] = new GetXmlDataField("field" + (i + 1));
    }

    rowLimit = 0;

    inFields = false;
    xmlField = "";
    prunePath = "";
  }

  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    int i;
    for (i = 0; i < inputFields.length; i++) {
      GetXmlDataField field = inputFields[i];

      int type = field.getType();
      if (type == IValueMeta.TYPE_NONE) {
        type = IValueMeta.TYPE_STRING;
      }
      try {
        IValueMeta v = ValueMetaFactory.createValueMeta(variables.resolve(field.getName()), type);
        v.setLength(field.getLength());
        v.setPrecision(field.getPrecision());
        v.setOrigin(name);
        v.setConversionMask(field.getFormat());
        v.setDecimalSymbol(field.getDecimalSymbol());
        v.setGroupingSymbol(field.getGroupSymbol());
        v.setCurrencySymbol(field.getCurrencySymbol());
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
    if (getSizeField() != null && getSizeField().length() > 0) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(getSizeField()));
      v.setOrigin(name);
      v.setLength(9);
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

    // See if we get input...
    if (input.length <= 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetXMLDataMeta.CheckResult.NoInputExpected"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "GetXMLDataMeta.CheckResult.NoInput"),
              transformMeta);
      remarks.add(cr);
    }

    // control Xpath
    if (getLoopXPath() == null || Utils.isEmpty(getLoopXPath())) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetXMLDataMeta.CheckResult.NoLoopXpath"),
              transformMeta);
      remarks.add(cr);
    }
    if (getInputFields().length <= 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetXMLDataMeta.CheckResult.NoInputField"),
              transformMeta);
      remarks.add(cr);
    }

    if (isInFields()) {
      if (Utils.isEmpty(getXMLField())) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "GetXMLDataMeta.CheckResult.NoField"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "GetXMLDataMeta.CheckResult.FieldOk"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      FileInputList fileInputList = getFiles(variables);
      // String files[] = getFiles();
      if (fileInputList == null || fileInputList.getFiles().size() == 0) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "GetXMLDataMeta.CheckResult.NoFiles"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "GetXMLDataMeta.CheckResult.FilesOk",
                    "" + fileInputList.getFiles().size()),
                transformMeta);
        remarks.add(cr);
      }
    }
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Since the exported transformation that runs this will reside in a ZIP file, we can't reference
   * files relatively. So what this does is turn the name of files into absolute paths OR it simply
   * includes the resource in the ZIP file. For now, we'll simply turn it into an absolute path and
   * pray that the file is on a shared drive or something like that.
   *
   * @param variables the variable variables to use
   * @param definitions
   * @param resourceNamingInterface
   * @param metadataProvider the metadata in which shared metadata could reside.
   * @return the filename of the exported resource
   */
  public String exportResources(
      IVariables variables,
      Map<String, ResourceDefinition> definitions,
      IResourceNaming resourceNamingInterface,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    try {
      // The object that we're modifying here is a copy of the original!
      // So let's change the filename from relative to absolute by grabbing the file object...
      // In case the name of the file comes from previous transforms, forget about this!
      //
      List<String> newFilenames = new ArrayList<>();

      if (!isInFields()) {
        FileInputList fileList = getFiles(variables);
        if (fileList.getFiles().size() > 0) {
          for (FileObject fileObject : fileList.getFiles()) {
            // From : ${Internal.Transformation.Filename.Directory}/../foo/bar.xml
            // To : /home/matt/test/files/foo/bar.xml
            //
            // If the file doesn't exist, forget about this effort too!
            //
            if (fileObject.exists()) {
              // Convert to an absolute path and add it to the list.
              //
              newFilenames.add(fileObject.getName().getPath());
            }
          }

          // Still here: set a new list of absolute filenames!
          //
          fileName = newFilenames.toArray(new String[newFilenames.size()]);
          fileMask = new String[newFilenames.size()]; // all null since converted to absolute path.
          fileRequired = new String[newFilenames.size()]; // all null, turn to "Y" :
          for (int i = 0; i < newFilenames.size(); i++) {
            fileRequired[i] = "Y";
          }
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /*
    @Override
    public TransformMetaInjection getTransformMetaInjectionInterface() {
      return new GetXmlDataMetaInjection( this );
    }
  */

  @Override
  public PipelineMeta.PipelineType[] getSupportedPipelineTypes() {
    return new PipelineMeta.PipelineType[] {PipelineMeta.PipelineType.Normal};
  }
}
