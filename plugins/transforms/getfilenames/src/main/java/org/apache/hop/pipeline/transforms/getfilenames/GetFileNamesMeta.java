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
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
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
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Transform(
    id = "GetFileNames",
    image = "getfilenames.svg",
    name = "i18n::BaseTransform.TypeLongDesc.GetFileNames",
    description = "i18n::BaseTransform.TypeTooltipDesc.GetFileNames",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::GetFileNamesMeta.keyword",
    documentationUrl = "/pipeline/transforms/getfilenames.html")
public class GetFileNamesMeta extends BaseTransformMeta
    implements ITransformMeta<GetFileNames, GetFileNamesData> {
  private static final Class<?> PKG = GetFileNamesMeta.class; // For Translator

  private static final String YES = "Y";

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };
  public static final String[] RequiredFilesCode = new String[] {"N", "Y"};

  /** Filter indicating file filter */
  private FileInputList.FileTypeFilter fileTypeFilter;

  @HopMetadataProperty(key = "file", injectionKeyDescription = "GetFileNames.Injection.File.Label")
  private List<FileItem> filesList;

  @HopMetadataProperty(
      key = "filter",
      injectionKeyDescription = "GetFileNames.Injection.FilterItemType.Label")
  private List<FilterItem> filterItemList;

  /** Flag indicating that a row number field should be included in the output */
  @HopMetadataProperty(
      key = "rownum",
      injectionKeyDescription = "GetFileNames.Injection.InclRownum.Label")
  private boolean includeRowNumber;

  /** The name of the field in the output containing the row number */
  @HopMetadataProperty(
      key = "rownum_field",
      injectionKeyDescription = "GetFileNames.Injection.InclRownumField.Label")
  private String rowNumberField;

  @HopMetadataProperty(
      key = "filename_Field",
      injectionKeyDescription = "GetFileNames.Injection.FilenameField.Label")
  private String dynamicFilenameField;

  @HopMetadataProperty(
      key = "wildcard_Field",
      injectionKeyDescription = "GetFileNames.Injection.WildcardField.Label")
  private String dynamicWildcardField;

  @HopMetadataProperty(
      key = "exclude_wildcard_Field",
      injectionKeyDescription = "GetFileNames.Injection.ExcludeWildcardField.Label")
  private String dynamicExcludeWildcardField;

  /** file name from previous fields */
  @HopMetadataProperty(
      key = "filefield",
      injectionKeyDescription = "GetFileNames.Injection.FilenameInField.Label")
  private boolean fileField;

  @HopMetadataProperty(
      key = "dynamic_include_subfolders",
      injectionKeyDescription = "GetFileNames.Injection.IncludeSubFolder.Label")
  private boolean dynamicIncludeSubFolders;

  @HopMetadataProperty(
      key = "isaddresult",
      injectionKeyDescription = "GetFileNames.Injection.AddResult.Label")
  private boolean addResultFile;

  /** The maximum number or lines to read */
  @HopMetadataProperty(
      key = "limit",
      injectionKeyDescription = "GetFileNames.Injection.Limit.Label")
  private long rowLimit;

  /** Flag : do not fail if no file */
  @HopMetadataProperty(injectionKeyDescription = "GetFileNames.Injection.DoNotFailIfNoFile.Label")
  private boolean doNotFailIfNoFile;

  /** Flag : raise an exception if no file */
  @HopMetadataProperty(
      injectionKeyDescription = "GetFileNames.Injection.RaiseAnExceptionIfNoFiles.Label")
  private boolean raiseAnExceptionIfNoFile;

  public GetFileNamesMeta() {
    super(); // allocate BaseTransformMeta
    filesList = new ArrayList<>();
    filterItemList = new ArrayList<>();
  }

  public List<FileItem> getFilesList() {
    return filesList;
  }

  public void setFilesList(List<FileItem> filesList) {
    this.filesList = filesList;
  }

  public List<FilterItem> getFilterItemList() {
    return filterItemList;
  }

  public void setFilterItemList(List<FilterItem> filterItemList) {
    this.filterItemList = filterItemList;
  }

  /** @return the doNotFailIfNoFile flag */
  public boolean isDoNotFailIfNoFile() {
    return doNotFailIfNoFile;
  }

  /** @param doNotFailIfNoFile the doNotFailIfNoFile to set */
  public void setDoNotFailIfNoFile(boolean doNotFailIfNoFile) {
    this.doNotFailIfNoFile = doNotFailIfNoFile;
  }

  /** @return the raiseAnExceptionIfNoFile flag */
  public boolean isRaiseAnExceptionIfNoFile() {
    return raiseAnExceptionIfNoFile;
  }

  /** @param raiseAnExceptionIfNoFile the raiseAnExceptionIfNoFile to set */
  public void setRaiseAnExceptionIfNoFile(boolean raiseAnExceptionIfNoFile) {
    this.raiseAnExceptionIfNoFile = raiseAnExceptionIfNoFile;
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
  public boolean isIncludeRowNumber() {
    return includeRowNumber;
  }

  /** @return Returns the File field. */
  public boolean isFileField() {
    return fileField;
  }

  /** @param filefield The filefield to set. */
  public void setFileField(boolean fileField) {
    this.fileField = fileField;
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
  public void setAddResultFile(boolean addResultFile) {
    this.addResultFile = addResultFile;
  }

  /** @return Returns isaddresult. */
  public boolean isAddResultFile() {
    return addResultFile;
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

  /** @return Returns the rowLimit. */
  public long getRowLimit() {
    return rowLimit;
  }

  /** @param rowLimit The rowLimit to set. */
  public void setRowLimit(long rowLimit) {
    this.rowLimit = rowLimit;
  }

  @Override
  public Object clone() {
    GetFileNamesMeta retval = (GetFileNamesMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    doNotFailIfNoFile = false;
    fileTypeFilter = FileInputList.FileTypeFilter.FILES_AND_FOLDERS;
    addResultFile = true;
    fileField = false;
    includeRowNumber = false;
    rowNumberField = "";
    dynamicFilenameField = "";
    dynamicWildcardField = "";
    dynamicIncludeSubFolders = false;
    dynamicExcludeWildcardField = "";
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

  private FileInputList.FileTypeFilter[] buildFileTypeFiltersArray() {
    FileInputList.FileTypeFilter[] filters = new FileInputList.FileTypeFilter[filesList.size()];
    FileInputList.FileTypeFilter elementTypeToGet =
        FileInputList.FileTypeFilter.getByName(
            getFilterItemList().get(0).getFileTypeFilterSelection());

    for (int i = 0; i < filesList.size(); i++) {
      filters[i] = elementTypeToGet;
    }
    return filters;
  }

  public String[] getFilePaths(IVariables variables) {
    return FileInputList.createFilePathList(
        variables,
        buildFilenamesArray(),
        buildMasksArray(),
        buildExcludeMasksArray(),
        buildFileRequiredArray(),
        includeSubFolderBoolean(),
        buildFileTypeFiltersArray());
  }

  public FileInputList getFileList(IVariables variables) {
    return FileInputList.createFileList(
        variables,
        buildFilenamesArray(),
        buildMasksArray(),
        buildExcludeMasksArray(),
        buildFileRequiredArray(),
        includeSubFolderBoolean(),
        buildFileTypeFiltersArray());
  }

  public FileInputList getDynamicFileList(
      IVariables variables,
      String[] filename,
      String[] fileMask,
      String[] excludeFilemask,
      String[] fileRequired,
      boolean[] includeSubFolders) {
    return FileInputList.createFileList(
        variables,
        filename,
        fileMask,
        excludeFilemask,
        fileRequired,
        includeSubFolders,
        buildFileTypeFiltersArray());
  }

  protected String[] buildFilenamesArray() {

    String[] fileNames = new String[filesList.size()];
    for (int i = 0; i < filesList.size(); i++) {
      fileNames[i] = filesList.get(i).getFileName();
    }
    return fileNames;
  }

  protected String[] buildMasksArray() {

    String[] fileMasks = new String[filesList.size()];
    for (int i = 0; i < filesList.size(); i++) {
      fileMasks[i] = filesList.get(i).getFileMask();
    }
    return fileMasks;
  }

  protected String[] buildExcludeMasksArray() {
    String[] excludeMasks = new String[filesList.size()];
    for (int i = 0; i < filesList.size(); i++) {
      excludeMasks[i] = filesList.get(i).getExcludeFileMask();
    }
    return excludeMasks;
  }

  protected String[] buildFileRequiredArray() {
    String[] required = new String[filesList.size()];
    for (int i = 0; i < filesList.size(); i++) {
      required[i] = filesList.get(i).getFileRequired();
    }
    return required;
  }

  private boolean[] includeSubFolderBoolean() {
    boolean[] includeSubFolderBoolean = new boolean[filesList.size()];
    for (int i = 0; i < filesList.size(); i++) {
      includeSubFolderBoolean[i] = YES.equalsIgnoreCase(filesList.get(i).getIncludeSubFolders());
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
    if (fileField) {
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
      if (!fileField) {

        // Replace the filename ONLY (folder or filename)
        //
        for (int i = 0; i < filesList.size(); i++) {
          FileItem fi = filesList.get(i);
          FileObject fileObject = HopVfs.getFileObject(variables.resolve(fi.getFileName()));
          fi.setFileName(
              iResourceNaming.nameResource(fileObject, variables, Utils.isEmpty(fi.getFileMask())));
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }
}
