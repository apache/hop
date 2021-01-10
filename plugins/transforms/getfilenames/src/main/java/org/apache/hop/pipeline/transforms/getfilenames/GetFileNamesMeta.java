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

package org.apache.hop.pipeline.transforms.getfilenames;

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
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Transform(
    id = "GetFileNames",
    image = "getfilenames.svg",
    name = "i18n::BaseTransform.TypeLongDesc.GetFileNames",
    description = "i18n::BaseTransform.TypeTooltipDesc.GetFileNames",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/getfilenames.html")
public class GetFileNamesMeta extends BaseTransformMeta
    implements ITransformMeta<GetFileNames, GetFileNamesData> {
  private static final Class<?> PKG = GetFileNamesMeta.class; // For Translator

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };
  public static final String[] RequiredFilesCode = new String[] {"N", "Y"};

  private static final String NO = "N";

  private static final String YES = "Y";

  /** Array of filenames */
  private String[] fileName;

  /** Wildcard or filemask (regular expression) */
  private String[] fileMask;

  /** Wildcard or filemask to exclude (regular expression) */
  private String[] excludeFileMask;

  /** Array of boolean values as string, indicating if a file is required. */
  private String[] fileRequired;

  /** Array of boolean values as string, indicating if we need to fetch sub folders. */
  private String[] includeSubFolders;

  /** Filter indicating file filter */
  private FileInputList.FileTypeFilter fileTypeFilter;

  /** The name of the field in the output containing the filename */
  private String filenameField;

  /** Flag indicating that a row number field should be included in the output */
  private boolean includeRowNumber;

  /** The name of the field in the output containing the row number */
  private String rowNumberField;

  private String dynamicFilenameField;

  private String dynamicWildcardField;
  private String dynamicExcludeWildcardField;

  /** file name from previous fields */
  private boolean filefield;

  private boolean dynamicIncludeSubFolders;

  private boolean isaddresult;

  /** The maximum number or lines to read */
  private long rowLimit;

  /** Flag : do not fail if no file */
  private boolean doNotFailIfNoFile;

  public GetFileNamesMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return the doNotFailIfNoFile flag */
  public boolean isdoNotFailIfNoFile() {
    return doNotFailIfNoFile;
  }

  /** @param doNotFailIfNoFile the doNotFailIfNoFile to set */
  public void setdoNotFailIfNoFile(boolean doNotFailIfNoFile) {
    this.doNotFailIfNoFile = doNotFailIfNoFile;
  }

  /** @return Returns the filenameField. */
  public String getFilenameField() {
    return filenameField;
  }

  /** @return Returns the rowNumberField. */
  public String getRowNumberField() {
    return rowNumberField;
  }

  /** @param dynamicFilenameField The dynamic filename field to set. */
  public void setDynamicFilenameField(String dynamicFilenameField) {
    this.dynamicFilenameField = dynamicFilenameField;
  }

  /** @param dynamicWildcardField The dynamic wildcard field to set. */
  public void setDynamicWildcardField(String dynamicWildcardField) {
    this.dynamicWildcardField = dynamicWildcardField;
  }

  /** @param rowNumberField The rowNumberField to set. */
  public void setRowNumberField(String rowNumberField) {
    this.rowNumberField = rowNumberField;
  }

  /** @return Returns the dynamic filename field (from previous transforms) */
  public String getDynamicFilenameField() {
    return dynamicFilenameField;
  }

  /** @return Returns the dynamic wildcard field (from previous transforms) */
  public String getDynamicWildcardField() {
    return dynamicWildcardField;
  }

  public String getDynamicExcludeWildcardField() {
    return this.dynamicExcludeWildcardField;
  }

  /** @param dynamicExcludeWildcardField The dynamic excludeWildcard field to set. */
  public void setDynamicExcludeWildcardField(String dynamicExcludeWildcardField) {
    this.dynamicExcludeWildcardField = dynamicExcludeWildcardField;
  }

  /** @return Returns the includeRowNumber. */
  public boolean includeRowNumber() {
    return includeRowNumber;
  }

  /** @return Returns the File field. */
  public boolean isFileField() {
    return filefield;
  }

  /** @param filefield The filefield to set. */
  public void setFileField(boolean filefield) {
    this.filefield = filefield;
  }

  public boolean isDynamicIncludeSubFolders() {
    return dynamicIncludeSubFolders;
  }

  public void setDynamicIncludeSubFolders(boolean dynamicIncludeSubFolders) {
    this.dynamicIncludeSubFolders = dynamicIncludeSubFolders;
  }

  /** @param includeRowNumber The includeRowNumber to set. */
  public void setIncludeRowNumber(boolean includeRowNumber) {
    this.includeRowNumber = includeRowNumber;
  }

  /** @param isaddresult The isaddresult to set. */
  public void setAddResultFile(boolean isaddresult) {
    this.isaddresult = isaddresult;
  }

  /** @return Returns isaddresult. */
  public boolean isAddResultFile() {
    return isaddresult;
  }

  /** @return Returns the fileMask. */
  public String[] getFileMask() {
    return fileMask;
  }

  /** @return Returns the fileRequired. */
  public String[] getFileRequired() {
    return fileRequired;
  }

  /** @param fileMask The fileMask to set. */
  public void setFileMask(String[] fileMask) {
    this.fileMask = fileMask;
  }

  /** @param excludeFileMask The excludeFileMask to set. */
  public void setExcludeFileMask(String[] excludeFileMask) {
    this.excludeFileMask = excludeFileMask;
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

  /** @param fileRequiredin The fileRequired to set. */
  public void setFileRequired(String[] fileRequiredin) {
    this.fileRequired = new String[fileRequiredin.length];
    for (int i = 0; i < fileRequiredin.length; i++) {
      this.fileRequired[i] = getRequiredFilesCode(fileRequiredin[i]);
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

  public void setIncludeSubFolders(String[] includeSubFoldersin) {
    this.includeSubFolders = new String[includeSubFoldersin.length];
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

  /** @return Returns the rowLimit. */
  public long getRowLimit() {
    return rowLimit;
  }

  /** @param rowLimit The rowLimit to set. */
  public void setRowLimit(long rowLimit) {
    this.rowLimit = rowLimit;
  }

  @Deprecated
  public void setFilterFileType(int filtertypevalue) {
    this.fileTypeFilter = FileInputList.FileTypeFilter.getByOrdinal(filtertypevalue);
  }

  public void setFilterFileType(FileInputList.FileTypeFilter filter) {
    this.fileTypeFilter = filter;
  }

  public FileInputList.FileTypeFilter getFileTypeFilter() {
    return fileTypeFilter;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  @Override
  public Object clone() {
    GetFileNamesMeta retval = (GetFileNamesMeta) super.clone();

    int nrfiles = fileName.length;

    retval.allocate(nrfiles);

    System.arraycopy(fileName, 0, retval.fileName, 0, nrfiles);
    System.arraycopy(fileMask, 0, retval.fileMask, 0, nrfiles);
    System.arraycopy(excludeFileMask, 0, retval.excludeFileMask, 0, nrfiles);
    System.arraycopy(fileRequired, 0, retval.fileRequired, 0, nrfiles);
    System.arraycopy(includeSubFolders, 0, retval.includeSubFolders, 0, nrfiles);

    return retval;
  }

  public void allocate(int nrfiles) {
    fileName = new String[nrfiles];
    fileMask = new String[nrfiles];
    excludeFileMask = new String[nrfiles];
    fileRequired = new String[nrfiles];
    includeSubFolders = new String[nrfiles];
  }

  @Override
  public void setDefault() {
    int nrfiles = 0;
    doNotFailIfNoFile = false;
    fileTypeFilter = FileInputList.FileTypeFilter.FILES_AND_FOLDERS;
    isaddresult = true;
    filefield = false;
    includeRowNumber = false;
    rowNumberField = "";
    dynamicFilenameField = "";
    dynamicWildcardField = "";
    dynamicIncludeSubFolders = false;
    dynamicExcludeWildcardField = "";

    allocate(nrfiles);

    for (int i = 0; i < nrfiles; i++) {
      fileName[i] = "filename" + (i + 1);
      fileMask[i] = "";
      excludeFileMask[i] = "";
      fileRequired[i] = NO;
      includeSubFolders[i] = NO;
    }
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

    // the filename
    IValueMeta filename = new ValueMetaString("filename");
    filename.setLength(500);
    filename.setPrecision(-1);
    filename.setOrigin(name);
    row.addValueMeta(filename);

    // the short filename
    IValueMeta shortFilename = new ValueMetaString("short_filename");
    shortFilename.setLength(500);
    shortFilename.setPrecision(-1);
    shortFilename.setOrigin(name);
    row.addValueMeta(shortFilename);

    // the path
    IValueMeta path = new ValueMetaString("path");
    path.setLength(500);
    path.setPrecision(-1);
    path.setOrigin(name);
    row.addValueMeta(path);

    // the type
    IValueMeta type = new ValueMetaString("type");
    type.setLength(500);
    type.setPrecision(-1);
    type.setOrigin(name);
    row.addValueMeta(type);

    // the exists
    IValueMeta exists = new ValueMetaBoolean("exists");
    exists.setOrigin(name);
    row.addValueMeta(exists);

    // the ishidden
    IValueMeta ishidden = new ValueMetaBoolean("ishidden");
    ishidden.setOrigin(name);
    row.addValueMeta(ishidden);

    // the isreadable
    IValueMeta isreadable = new ValueMetaBoolean("isreadable");
    isreadable.setOrigin(name);
    row.addValueMeta(isreadable);

    // the iswriteable
    IValueMeta iswriteable = new ValueMetaBoolean("iswriteable");
    iswriteable.setOrigin(name);
    row.addValueMeta(iswriteable);

    // the lastmodifiedtime
    IValueMeta lastmodifiedtime = new ValueMetaDate("lastmodifiedtime");
    lastmodifiedtime.setOrigin(name);
    row.addValueMeta(lastmodifiedtime);

    // the size
    IValueMeta size = new ValueMetaInteger("size");
    size.setOrigin(name);
    row.addValueMeta(size);

    // the extension
    IValueMeta extension = new ValueMetaString("extension");
    extension.setOrigin(name);
    row.addValueMeta(extension);

    // the uri
    IValueMeta uri = new ValueMetaString("uri");
    uri.setOrigin(name);
    row.addValueMeta(uri);

    // the rooturi
    IValueMeta rooturi = new ValueMetaString("rooturi");
    rooturi.setOrigin(name);
    row.addValueMeta(rooturi);

    if (includeRowNumber) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(rowNumberField));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append("    <filter>").append(Const.CR);
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("filterfiletype", fileTypeFilter.toString()));
    retval.append("    </filter>").append(Const.CR);
    retval.append("    ").append(XmlHandler.addTagValue("doNotFailIfNoFile", doNotFailIfNoFile));
    retval.append("    ").append(XmlHandler.addTagValue("rownum", includeRowNumber));
    retval.append("    ").append(XmlHandler.addTagValue("isaddresult", isaddresult));
    retval.append("    ").append(XmlHandler.addTagValue("filefield", filefield));
    retval.append("    ").append(XmlHandler.addTagValue("rownum_field", rowNumberField));
    retval.append("    ").append(XmlHandler.addTagValue("filename_Field", dynamicFilenameField));
    retval.append("    ").append(XmlHandler.addTagValue("wildcard_Field", dynamicWildcardField));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("exclude_wildcard_Field", dynamicExcludeWildcardField));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("dynamic_include_subfolders", dynamicIncludeSubFolders));
    retval.append("    ").append(XmlHandler.addTagValue("limit", rowLimit));

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

  private void readData(Node transformNode) throws HopXmlException {
    try {
      Node filternode = XmlHandler.getSubNode(transformNode, "filter");
      Node filterfiletypenode = XmlHandler.getSubNode(filternode, "filterfiletype");
      fileTypeFilter =
          FileInputList.FileTypeFilter.getByName(XmlHandler.getNodeValue(filterfiletypenode));
      doNotFailIfNoFile =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "doNotFailIfNoFile"));
      includeRowNumber = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "rownum"));
      isaddresult = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "isaddresult"));
      filefield = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "filefield"));
      rowNumberField = XmlHandler.getTagValue(transformNode, "rownum_field");
      dynamicFilenameField = XmlHandler.getTagValue(transformNode, "filename_Field");
      dynamicWildcardField = XmlHandler.getTagValue(transformNode, "wildcard_Field");
      dynamicExcludeWildcardField = XmlHandler.getTagValue(transformNode, "exclude_wildcard_Field");
      dynamicIncludeSubFolders =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "dynamic_include_subfolders"));

      // Is there a limit on the number of rows we process?
      rowLimit = Const.toLong(XmlHandler.getTagValue(transformNode, "limit"), 0L);

      Node filenode = XmlHandler.getSubNode(transformNode, "file");
      int nrfiles = XmlHandler.countNodes(filenode, "name");

      allocate(nrfiles);

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
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  private boolean[] includeSubFolderBoolean() {
    int len = fileName.length;
    boolean[] includeSubFolderBoolean = new boolean[len];
    for (int i = 0; i < len; i++) {
      includeSubFolderBoolean[i] = YES.equalsIgnoreCase(includeSubFolders[i]);
    }
    return includeSubFolderBoolean;
  }

  public String[] getIncludeSubFolders() {
    return includeSubFolders;
  }

  private FileInputList.FileTypeFilter[] buildFileTypeFiltersArray(String[] fileName) {
    FileInputList.FileTypeFilter[] filters = new FileInputList.FileTypeFilter[fileName.length];
    for (int i = 0; i < fileName.length; i++) {
      filters[i] = getFileTypeFilter();
    }
    return filters;
  }

  public String[] getFilePaths(IVariables variables) {
    return FileInputList.createFilePathList(
        variables,
        fileName,
        fileMask,
        excludeFileMask,
        fileRequired,
        includeSubFolderBoolean(),
        buildFileTypeFiltersArray(fileName));
  }

  public FileInputList getFileList(IVariables variables) {
    return FileInputList.createFileList(
        variables,
        fileName,
        fileMask,
        excludeFileMask,
        fileRequired,
        includeSubFolderBoolean(),
        buildFileTypeFiltersArray(fileName));
  }

  public FileInputList getDynamicFileList(
      IVariables variables,
      String[] filename,
      String[] filemask,
      String[] excludefilemask,
      String[] filerequired,
      boolean[] includesubfolders) {
    return FileInputList.createFileList(
        variables,
        filename,
        filemask,
        excludefilemask,
        filerequired,
        includesubfolders,
        buildFileTypeFiltersArray(filename));
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
    if (filefield) {
      if (input.length > 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "GetFileNamesMeta.CheckResult.InputOk"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "GetFileNamesMeta.CheckResult.InputErrorKo"),
                transformMeta);
      }
      remarks.add(cr);

      if (Utils.isEmpty(dynamicFilenameField)) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "GetFileNamesMeta.CheckResult.FolderFieldnameMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "GetFileNamesMeta.CheckResult.FolderFieldnameOk"),
                transformMeta);
      }
      remarks.add(cr);

    } else {

      if (input.length > 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "GetFileNamesMeta.CheckResult.NoInputError"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "GetFileNamesMeta.CheckResult.NoInputOk"),
                transformMeta);
      }

      remarks.add(cr);

      // check specified file names
      FileInputList fileList = getFileList(variables);
      if (fileList.nrOfFiles() == 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "GetFileNamesMeta.CheckResult.ExpectedFilesError"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "GetFileNamesMeta.CheckResult.ExpectedFilesOk", "" + fileList.nrOfFiles()),
                transformMeta);
      }
      remarks.add(cr);
    }
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, TransformMeta transformMeta) {
    List<ResourceReference> references = new ArrayList<>(5);
    ResourceReference reference = new ResourceReference(transformMeta);
    references.add(reference);

    String[] files = getFilePaths(variables);
    if (files != null) {
      for (int i = 0; i < files.length; i++) {
        reference.getEntries().add(new ResourceEntry(files[i], ResourceType.FILE));
      }
    }
    return references;
  }

  @Override
  public GetFileNames createTransform(
      TransformMeta transformMeta,
      GetFileNamesData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new GetFileNames(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public GetFileNamesData getTransformData() {
    return new GetFileNamesData();
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
      if (!filefield) {

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
}
