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

package org.apache.hop.pipeline.transforms.yamlinput;

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
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
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

@Transform(
    id = "YamlInput",
    image = "yamlinput.svg",
    name = "i18n::YamlInput.Name",
    description = "i18n::YamlInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/yamlinput.html")
public class YamlInputMeta extends BaseTransformMeta
    implements ITransformMeta<YamlInput, YamlInputData> {
  private static final Class<?> PKG = YamlInputMeta.class; // For Translator

  private static final String YES = "Y";

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };
  public static final String[] RequiredFilesCode = new String[] {"N", "Y"};

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      YamlInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new YamlInput(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  /** Array of filenames */
  private String[] fileName;

  /** Wildcard or filemask (regular expression) */
  private String[] fileMask;

  /** Array of boolean values as string, indicating if a file is required. */
  private String[] fileRequired;

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
  private YamlInputField[] inputFields;

  /** The encoding to use for reading: null or empty string means system default encoding */
  private String encoding;

  /** Is In fields */
  private String yamlField;

  /** Is In fields */
  private boolean inFields;

  /** Is a File */
  private boolean IsAFile;

  /** Flag: add result filename */
  private boolean addResultFile;

  /** Flag: set XML Validating */
  private boolean validating;

  /** Flag : do we ignore empty files */
  private boolean IsIgnoreEmptyFile;

  /** Array of boolean values as string, indicating if we need to fetch sub folders. */
  private String[] includeSubFolders;

  /** Flag : do not fail if no file */
  private boolean doNotFailIfNoFile;

  public YamlInputMeta() {
    super(); // allocate BaseTransformMeta
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

  public void setAddResultFile(boolean addResultFile) {
    this.addResultFile = addResultFile;
  }

  /** @return Returns the input fields. */
  public YamlInputField[] getInputFields() {
    return inputFields;
  }

  /** @param inputFields The input fields to set. */
  public void setInputFields(YamlInputField[] inputFields) {
    this.inputFields = inputFields;
  }

  /** Get XML field. */
  public String getYamlField() {
    return yamlField;
  }

  /** Set XML field. */
  public void setYamlField(String yamlField) {
    this.yamlField = yamlField;
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

  public String[] getIncludeSubFolders() {
    return includeSubFolders;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  @Override
  public Object clone() {
    YamlInputMeta retval = (YamlInputMeta) super.clone();

    int nrFiles = fileName.length;
    int nrFields = inputFields.length;

    retval.allocate(nrFiles, nrFields);

    System.arraycopy(fileName, 0, retval.fileName, 0, nrFiles);
    System.arraycopy(fileMask, 0, retval.fileMask, 0, nrFiles);
    System.arraycopy(fileRequired, 0, retval.fileRequired, 0, nrFiles);
    System.arraycopy(includeSubFolders, 0, retval.includeSubFolders, 0, nrFiles);

    for (int i = 0; i < nrFields; i++) {
      if (inputFields[i] != null) {
        retval.inputFields[i] = (YamlInputField) inputFields[i].clone();
      }
    }
    return retval;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(400);

    retval.append("    ").append(XmlHandler.addTagValue("include", includeFilename));
    retval.append("    ").append(XmlHandler.addTagValue("include_field", filenameField));
    retval.append("    ").append(XmlHandler.addTagValue("rownum", includeRowNumber));
    retval.append("    ").append(XmlHandler.addTagValue("addresultfile", addResultFile));
    retval.append("    ").append(XmlHandler.addTagValue("validating", validating));
    retval.append("    " + XmlHandler.addTagValue("IsIgnoreEmptyFile", IsIgnoreEmptyFile));
    retval.append("    " + XmlHandler.addTagValue("doNotFailIfNoFile", doNotFailIfNoFile));

    retval.append("    ").append(XmlHandler.addTagValue("rownum_field", rowNumberField));
    retval.append("    ").append(XmlHandler.addTagValue("encoding", encoding));

    retval.append("    <file>").append(Const.CR);
    for (int i = 0; i < fileName.length; i++) {
      retval.append("      ").append(XmlHandler.addTagValue("name", fileName[i]));
      retval.append("      ").append(XmlHandler.addTagValue("filemask", fileMask[i]));
      retval.append("      ").append(XmlHandler.addTagValue("file_required", fileRequired[i]));
      retval
          .append("      ")
          .append(XmlHandler.addTagValue("include_subfolders", includeSubFolders[i]));
    }
    retval.append("    </file>").append(Const.CR);

    retval.append("    <fields>").append(Const.CR);
    for (int i = 0; i < inputFields.length; i++) {
      YamlInputField field = inputFields[i];
      retval.append(field.getXml());
    }
    retval.append("    </fields>").append(Const.CR);

    retval.append("    ").append(XmlHandler.addTagValue("limit", rowLimit));
    retval.append("    ").append(XmlHandler.addTagValue("IsInFields", inFields));
    retval.append("    ").append(XmlHandler.addTagValue("IsAFile", IsAFile));
    retval.append("    ").append(XmlHandler.addTagValue("YamlField", yamlField));

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
      validating = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "validating"));
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
        Node fileRequirednode = XmlHandler.getSubNodeByNr(filenode, "file_required", i);
        Node includeSubFoldersnode = XmlHandler.getSubNodeByNr(filenode, "include_subfolders", i);
        fileName[i] = XmlHandler.getNodeValue(filenamenode);
        fileMask[i] = XmlHandler.getNodeValue(filemasknode);
        fileRequired[i] = XmlHandler.getNodeValue(fileRequirednode);
        includeSubFolders[i] = XmlHandler.getNodeValue(includeSubFoldersnode);
      }

      for (int i = 0; i < nrFields; i++) {
        Node fnode = XmlHandler.getSubNodeByNr(fields, "field", i);
        YamlInputField field = new YamlInputField(fnode);
        inputFields[i] = field;
      }

      // Is there a limit on the number of rows we process?
      rowLimit = Const.toLong(XmlHandler.getTagValue(transformNode, "limit"), 0L);
      inFields = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "IsInFields"));
      IsAFile = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "IsAFile"));
      yamlField = XmlHandler.getTagValue(transformNode, "YamlField");
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "YamlInputMeta.Exception.ErrorLoadingXml", e.toString()));
    }
  }

  public void allocate(int nrfiles, int nrFields) {
    fileName = new String[nrfiles];
    fileMask = new String[nrfiles];
    fileRequired = new String[nrfiles];
    includeSubFolders = new String[nrfiles];
    inputFields = new YamlInputField[nrFields];
  }

  @Override
  public void setDefault() {
    IsIgnoreEmptyFile = false;
    doNotFailIfNoFile = true;
    includeFilename = false;
    filenameField = "";
    includeRowNumber = false;
    rowNumberField = "";
    IsAFile = false;
    addResultFile = false;
    validating = false;

    int nrFiles = 0;
    int nrFields = 0;

    allocate(nrFiles, nrFields);

    for (int i = 0; i < nrFiles; i++) {
      fileName[i] = "filename" + (i + 1);
      fileMask[i] = "";
      fileRequired[i] = RequiredFilesCode[0];
      includeSubFolders[i] = RequiredFilesCode[0];
    }

    for (int i = 0; i < nrFields; i++) {
      inputFields[i] = new YamlInputField("field" + (i + 1));
    }

    rowLimit = 0;

    inFields = false;
    yamlField = "";
  }

  @Override
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
      YamlInputField field = inputFields[i];

      int type = field.getType();
      if (type == IValueMeta.TYPE_NONE) {
        type = IValueMeta.TYPE_STRING;
      }
      String valueName = variables.resolve(field.getName());
      IValueMeta v;
      try {
        v = ValueMetaFactory.createValueMeta(valueName, type);
      } catch (HopPluginException e) {
        v = new ValueMetaString(valueName);
      }
      v.setLength(field.getLength());
      v.setPrecision(field.getPrecision());
      v.setOrigin(name);
      v.setConversionMask(field.getFormat());
      v.setDecimalSymbol(field.getDecimalSymbol());
      v.setGroupingSymbol(field.getGroupSymbol());
      v.setCurrencySymbol(field.getCurrencySymbol());
      r.addValueMeta(v);
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
  }

  public FileInputList getFiles(IVariables variables) {
    return FileInputList.createFileList(
        variables, fileName, fileMask, fileRequired, includeSubFolderBoolean());
  }

  private boolean[] includeSubFolderBoolean() {
    int len = fileName.length;
    boolean[] includeSubFolderBoolean = new boolean[len];
    for (int i = 0; i < len; i++) {
      includeSubFolderBoolean[i] = YES.equalsIgnoreCase(includeSubFolders[i]);
    }
    return includeSubFolderBoolean;
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
    if (input.length <= 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "YamlInputMeta.CheckResult.NoInputExpected"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "YamlInputMeta.CheckResult.NoInput"),
              transformMeta);
      remarks.add(cr);
    }

    if (getInputFields().length <= 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "YamlInputMeta.CheckResult.NoInputField"),
              transformMeta);
      remarks.add(cr);
    }

    if (isInFields()) {
      if (Utils.isEmpty(getYamlField())) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "YamlInputMeta.CheckResult.NoField"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "YamlInputMeta.CheckResult.FieldOk"),
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
                BaseMessages.getString(PKG, "YamlInputMeta.CheckResult.NoFiles"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "YamlInputMeta.CheckResult.FilesOk", "" + fileInputList.getFiles().size()),
                transformMeta);
        remarks.add(cr);
      }
    }
  }

  @Override
  public YamlInputData getTransformData() {
    return new YamlInputData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Since the exported pipeline that runs this will reside in a ZIP file, we can't reference files
   * relatively. So what this does is turn the name of files into absolute paths OR it simply
   * includes the resource in the ZIP file. For now, we'll simply turn it into an absolute path and
   * pray that the file is on a shared drive or something like that.
   *
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
      List<String> newFilenames = new ArrayList<>();

      if (!isInFields()) {
        FileInputList fileList = getFiles(variables);
        if (fileList.getFiles().size() > 0) {
          for (FileObject fileObject : fileList.getFiles()) {
            // From : ${Internal.Pipeline.Filename.Directory}/../foo/bar.xml
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
}
