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

package org.apache.hop.pipeline.transforms.getfilesrowcount;

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
import org.apache.hop.core.row.value.ValueMetaInteger;
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

@Transform(
    id = "GetFilesRowsCount",
    image = "getfilesrowcount.svg",
    name = "i18n::BaseTransform.TypeLongDesc.GetFilesRowsCount",
    description = "i18n::BaseTransform.TypeTooltipDesc.GetFilesRowsCount",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/getfilesrowcount.html")
public class GetFilesRowsCountMeta extends BaseTransformMeta
    implements ITransformMeta<GetFilesRowsCount, GetFilesRowsCountData> {
  private static final Class<?> PKG = GetFilesRowsCountMeta.class; // For Translator

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };
  public static final String[] RequiredFilesCode = new String[] {"N", "Y"};
  private static final String NO = "N";
  private static final String YES = "Y";

  public static final String DEFAULT_ROWSCOUNT_FIELDNAME = "rowscount";

  /** Array of filenames */
  private String[] fileName;

  /** Wildcard or filemask (regular expression) */
  private String[] fileMask;

  /** Wildcard or filemask to exclude (regular expression) */
  private String[] excludeFileMask;

  /** Flag indicating that a row number field should be included in the output */
  private boolean includeFilesCount;

  /** The name of the field in the output containing the file number */
  private String filesCountFieldName;

  /** The name of the field in the output containing the row number */
  private String rowsCountFieldName;

  /** The row separator type */
  private String RowSeparatorFormat;

  /** The row separator */
  private String RowSeparator;

  /** file name from previous fields */
  private boolean filefield;

  private boolean isaddresult;

  private String outputFilenameField;

  /** Array of boolean values as string, indicating if a file is required. */
  private String[] fileRequired;

  /** Array of boolean values as string, indicating if we need to fetch sub folders. */
  private String[] includeSubFolders;

  /** Flag : check if a data is there right after separator */
  private boolean smartCount;

  public GetFilesRowsCountMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the row separator. */
  public String getRowSeparator() {
    return RowSeparator;
  }

  /** @param RowSeparatorin The RowSeparator to set. */
  public void setRowSeparator(String RowSeparatorin) {
    this.RowSeparator = RowSeparatorin;
  }

  /** @return Returns the row separator format. */
  public String getRowSeparatorFormat() {
    return RowSeparatorFormat;
  }

  /** @param isaddresult The isaddresult to set. */
  public void setAddResultFile(boolean isaddresult) {
    this.isaddresult = isaddresult;
  }

  /** @param smartCount The smartCount to set. */
  public void setSmartCount(boolean smartCount) {
    this.smartCount = smartCount;
  }

  /** @return Returns the excludeFileMask. Deprecated due to typo */
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

  /** @return Returns isaddresult. */
  public boolean isAddResultFile() {
    return isaddresult;
  }

  /** @return Returns smartCount. */
  public boolean isSmartCount() {
    return smartCount;
  }

  /** @return Returns the output filename_Field. Deprecated due to typo */
  @Deprecated
  public String setOutputFilenameField() {
    return outputFilenameField;
  }

  /** @return Returns the output filename_Field. */
  public String getOutputFilenameField() {
    return outputFilenameField;
  }

  /** @param outputFilenameField The output filename_field to set. */
  public void setOutputFilenameField(String outputFilenameField) {
    this.outputFilenameField = outputFilenameField;
  }

  /** @return Returns the File field. */
  public boolean isFileField() {
    return filefield;
  }

  /** @param filefield The file field to set. */
  public void setFileField(boolean filefield) {
    this.filefield = filefield;
  }

  /** @param RowSeparator_formatin The RowSeparator_format to set. */
  public void setRowSeparatorFormat(String RowSeparatorFormatin) {
    this.RowSeparatorFormat = RowSeparatorFormatin;
  }

  /** @return Returns the fileMask. */
  public String[] getFileMask() {
    return fileMask;
  }

  public void setFileRequired(String[] fileRequiredin) {
    for (int i = 0; i < fileRequiredin.length; i++) {
      this.fileRequired[i] = getRequiredFilesCode(fileRequiredin[i]);
    }
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

  /** @param fileMask The fileMask to set. */
  public void setFileMask(String[] fileMask) {
    this.fileMask = fileMask;
  }

  /** @return Returns the fileName. */
  public String[] getFileName() {
    return fileName;
  }

  /** @param fileName The fileName to set. */
  public void setFileName(String[] fileName) {
    this.fileName = fileName;
  }

  /** @return Returns the includeCountFiles. */
  public boolean includeCountFiles() {
    return includeFilesCount;
  }

  /** @param includeFilesCount The "includes files count" flag to set. */
  public void setIncludeCountFiles(boolean includeFilesCount) {
    this.includeFilesCount = includeFilesCount;
  }

  public String[] getFileRequired() {
    return this.fileRequired;
  }

  /** @return Returns the FilesCountFieldName. */
  public String getFilesCountFieldName() {
    return filesCountFieldName;
  }

  /** @return Returns the RowsCountFieldName. */
  public String getRowsCountFieldName() {
    return rowsCountFieldName;
  }

  /** @param filesCountFieldName The filesCountFieldName to set. */
  public void setFilesCountFieldName(String filesCountFieldName) {
    this.filesCountFieldName = filesCountFieldName;
  }

  /** @param rowsCountFieldName The rowsCountFieldName to set. */
  public void setRowsCountFieldName(String rowsCountFieldName) {
    this.rowsCountFieldName = rowsCountFieldName;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public Object clone() {
    GetFilesRowsCountMeta retval = (GetFilesRowsCountMeta) super.clone();

    int nrFiles = fileName.length;

    retval.allocate(nrFiles);
    System.arraycopy(fileName, 0, retval.fileName, 0, nrFiles);
    System.arraycopy(fileMask, 0, retval.fileMask, 0, nrFiles);
    System.arraycopy(excludeFileMask, 0, retval.excludeFileMask, 0, nrFiles);
    System.arraycopy(fileRequired, 0, retval.fileRequired, 0, nrFiles);
    System.arraycopy(includeSubFolders, 0, retval.includeSubFolders, 0, nrFiles);

    return retval;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append("    ").append(XmlHandler.addTagValue("files_count", includeFilesCount));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("files_count_fieldname", filesCountFieldName));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("rows_count_fieldname", rowsCountFieldName));
    retval.append("    ").append(XmlHandler.addTagValue("rowseparator_format", RowSeparatorFormat));
    retval.append("    ").append(XmlHandler.addTagValue("row_separator", RowSeparator));
    retval.append("    ").append(XmlHandler.addTagValue("isaddresult", isaddresult));
    retval.append("    ").append(XmlHandler.addTagValue("filefield", filefield));
    retval.append("    ").append(XmlHandler.addTagValue("filename_Field", outputFilenameField));
    retval.append("    ").append(XmlHandler.addTagValue("smartCount", smartCount));

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

    return retval.toString();
  }

  /**
   * Adjust old outdated values to new ones
   *
   * @param original The original value
   * @return The new/correct equivelant
   */
  private String scrubOldRowSeparator(String original) {
    if (original != null) {
      // Update old files to the new format
      if (original.equalsIgnoreCase("CR")) {
        return "LINEFEED";
      } else if (original.equalsIgnoreCase("LF")) {
        return "CARRIAGERETURN";
      }
    }
    return original;
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {

      includeFilesCount =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "files_count"));
      filesCountFieldName = XmlHandler.getTagValue(transformNode, "files_count_fieldname");
      rowsCountFieldName = XmlHandler.getTagValue(transformNode, "rows_count_fieldname");

      RowSeparatorFormat =
          scrubOldRowSeparator(XmlHandler.getTagValue(transformNode, "rowseparator_format"));
      RowSeparator = XmlHandler.getTagValue(transformNode, "row_separator");

      smartCount = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "smartCount"));

      String addresult = XmlHandler.getTagValue(transformNode, "isaddresult");
      if (Utils.isEmpty(addresult)) {
        isaddresult = true;
      } else {
        isaddresult = "Y".equalsIgnoreCase(addresult);
      }

      filefield = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "filefield"));
      outputFilenameField = XmlHandler.getTagValue(transformNode, "filename_Field");

      Node filenode = XmlHandler.getSubNode(transformNode, "file");
      int nrFiles = XmlHandler.countNodes(filenode, "name");
      allocate(nrFiles);

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

    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  public void allocate(int nrfiles) {
    fileName = new String[nrfiles];
    fileMask = new String[nrfiles];
    excludeFileMask = new String[nrfiles];
    fileRequired = new String[nrfiles];
    includeSubFolders = new String[nrfiles];
  }

  public void setDefault() {
    smartCount = false;
    outputFilenameField = "";
    filefield = false;
    isaddresult = true;
    includeFilesCount = false;
    filesCountFieldName = "";
    rowsCountFieldName = "rowscount";
    RowSeparatorFormat = "CR";
    RowSeparator = "";
    int nrFiles = 0;

    allocate(nrFiles);

    for (int i = 0; i < nrFiles; i++) {
      fileName[i] = "filename" + (i + 1);
      fileMask[i] = "";
      excludeFileMask[i] = "";
      fileRequired[i] = RequiredFilesCode[0];
      includeSubFolders[i] = RequiredFilesCode[0];
    }
  }

  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    IValueMeta v = new ValueMetaInteger(variables.resolve(rowsCountFieldName));
    v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
    v.setOrigin(name);
    r.addValueMeta(v);

    if (includeFilesCount) {
      v = new ValueMetaInteger(variables.resolve(filesCountFieldName));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
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
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetFilesRowsCountMeta.CheckResult.NoInputExpected"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "GetFilesRowsCountMeta.CheckResult.NoInput"),
              transformMeta);
      remarks.add(cr);
    }

    FileInputList fileInputList = getFiles(variables);

    if (fileInputList == null || fileInputList.getFiles().size() == 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetFilesRowsCountMeta.CheckResult.NoFiles"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "GetFilesRowsCountMeta.CheckResult.FilesOk",
                  "" + fileInputList.getFiles().size()),
              transformMeta);
      remarks.add(cr);
    }

    if ((RowSeparatorFormat.equals("CUSTOM")) && (RowSeparator == null)) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetFilesRowsCountMeta.CheckResult.NoSeparator"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "GetFilesRowsCountMeta.CheckResult.SeparatorOk"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public GetFilesRowsCount createTransform(
      TransformMeta transformMeta,
      GetFilesRowsCountData data,
      int cnr,
      PipelineMeta tr,
      Pipeline pipeline) {
    return new GetFilesRowsCount(transformMeta, this, data, cnr, tr, pipeline);
  }

  public GetFilesRowsCountData getTransformData() {
    return new GetFilesRowsCountData();
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
      if (!filefield) {
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
}
