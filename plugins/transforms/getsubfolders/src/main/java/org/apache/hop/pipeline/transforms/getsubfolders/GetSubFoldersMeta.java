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

package org.apache.hop.pipeline.transforms.getsubfolders;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
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
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;

@Transform(
    id = "GetSubFolders",
    image = "getsubfolders.svg",
    name = "i18n::GetSubFolders.Name",
    description = "i18n::GetSubFolders.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::GetSubFoldersMeta.keyword",
    documentationUrl = "/pipeline/transforms/getsubfolders.html")
public class GetSubFoldersMeta extends BaseTransformMeta<GetSubFolders, GetSubFoldersData> {
  private static final Class<?> PKG = GetSubFoldersMeta.class;

  /** The files/folders to get subfolders for */
  @HopMetadataProperty(key = "file")
  private List<GSFile> files;

  /** Flag indicating that a row number field should be included in the output */
  @HopMetadataProperty(key = "rownum")
  private boolean includeRowNumber;

  /** The name of the field in the output containing the row number */
  @HopMetadataProperty(key = "rownum_field")
  private String rowNumberField;

  /** The name of the field in the output containing the foldername */
  @HopMetadataProperty(key = "foldername_field")
  private String dynamicFolderNameField;

  /** folder name from previous fields */
  @HopMetadataProperty(key = "foldername_dynamic")
  private boolean folderNameDynamic;

  /** The maximum number or lines to read */
  @HopMetadataProperty(key = "limit")
  private long rowLimit;

  public GetSubFoldersMeta() {
    super();
    files = new ArrayList<>();
  }

  public GetSubFoldersMeta(GetSubFoldersMeta m) {
    this();
    this.includeRowNumber = m.includeRowNumber;
    this.rowNumberField = m.rowNumberField;
    this.dynamicFolderNameField = m.dynamicFolderNameField;
    this.folderNameDynamic = m.folderNameDynamic;
    this.rowLimit = m.rowLimit;
    m.files.forEach(f -> this.files.add(new GSFile(f)));
  }

  @Override
  public GetSubFoldersMeta clone() {
    return new GetSubFoldersMeta(this);
  }

  @Override
  public void setDefault() {
    folderNameDynamic = false;
    includeRowNumber = false;
    rowNumberField = "";
    dynamicFolderNameField = "";
  }

  @Override
  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    // the folderName
    IValueMeta folderNameValueMeta = new ValueMetaString("folderName");
    folderNameValueMeta.setLength(500);
    folderNameValueMeta.setPrecision(-1);
    folderNameValueMeta.setOrigin(name);
    row.addValueMeta(folderNameValueMeta);

    // the short folderName
    IValueMeta shortFolderNameValueMeta = new ValueMetaString("short_folderName");
    shortFolderNameValueMeta.setLength(500);
    shortFolderNameValueMeta.setPrecision(-1);
    shortFolderNameValueMeta.setOrigin(name);
    row.addValueMeta(shortFolderNameValueMeta);

    // the path
    IValueMeta pathValueMeta = new ValueMetaString("path");
    pathValueMeta.setLength(500);
    pathValueMeta.setPrecision(-1);
    pathValueMeta.setOrigin(name);
    row.addValueMeta(pathValueMeta);

    // the is hidden
    IValueMeta isHiddenValueMeta = new ValueMetaBoolean("ishidden");
    isHiddenValueMeta.setOrigin(name);
    row.addValueMeta(isHiddenValueMeta);

    // the is readable?
    IValueMeta isReadableValueMeta = new ValueMetaBoolean("isreadable");
    isReadableValueMeta.setOrigin(name);
    row.addValueMeta(isReadableValueMeta);

    // the is writeable
    IValueMeta isWriteableValueMeta = new ValueMetaBoolean("iswriteable");
    isWriteableValueMeta.setOrigin(name);
    row.addValueMeta(isWriteableValueMeta);

    // the last modified time
    IValueMeta lastModifiedTimeValueMeta = new ValueMetaDate("lastmodifiedtime");
    lastModifiedTimeValueMeta.setOrigin(name);
    row.addValueMeta(lastModifiedTimeValueMeta);

    // the uri
    IValueMeta uriValueMeta = new ValueMetaString("uri");
    uriValueMeta.setOrigin(name);
    row.addValueMeta(uriValueMeta);

    // the root uri
    IValueMeta rootUriValueMeta = new ValueMetaString("rooturi");
    rootUriValueMeta.setOrigin(name);
    row.addValueMeta(rootUriValueMeta);

    // children
    IValueMeta childrenValueMeta = new ValueMetaInteger(variables.resolve("childrens"));
    childrenValueMeta.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
    childrenValueMeta.setOrigin(name);
    row.addValueMeta(childrenValueMeta);

    if (includeRowNumber) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(rowNumberField));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  public String[] getFilesNames() {
    String[] names = new String[files.size()];
    for (int i = 0; i < names.length; i++) {
      names[i] = files.get(i).getName();
    }
    return names;
  }

  public String[] getFilesRequired() {
    String[] required = new String[files.size()];
    for (int i = 0; i < required.length; i++) {
      required[i] = files.get(i).isRequired() ? "Y" : "N";
    }
    return required;
  }

  public FileInputList getFolderList(IVariables variables) {
    return FileInputList.createFolderList(variables, getFilesNames(), getFilesRequired());
  }

  public FileInputList getDynamicFolderList(
      IVariables variables, String[] folderName, String[] folderRequired) {
    return FileInputList.createFolderList(variables, folderName, folderRequired);
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
    if (folderNameDynamic) {
      if (input.length > 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "GetSubFoldersMeta.CheckResult.InputOk"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "GetSubFoldersMeta.CheckResult.InputErrorKo"),
                transformMeta);
      }
      remarks.add(cr);

      if (Utils.isEmpty(dynamicFolderNameField)) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "GetSubFoldersMeta.CheckResult.FolderFieldnameMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "GetSubFoldersMeta.CheckResult.FolderFieldnameOk"),
                transformMeta);
      }

      remarks.add(cr);
    } else {
      if (input.length > 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "GetSubFoldersMeta.CheckResult.NoInputError"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "GetSubFoldersMeta.CheckResult.NoInputOk"),
                transformMeta);
      }
      remarks.add(cr);
      // check specified folder names
      FileInputList fileList = getFolderList(variables);
      if (fileList.nrOfFiles() == 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "GetSubFoldersMeta.CheckResult.ExpectedFoldersError"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "GetSubFoldersMeta.CheckResult.ExpectedFilesOk",
                    "" + fileList.nrOfFiles()),
                transformMeta);
        remarks.add(cr);
      }
    }
  }

  /**
   * Since the exported pipeline that runs this will reside in a ZIP file, we can't reference files
   * relatively. So what this does is turn the name of files into absolute paths OR it simply
   * includes the resource in the ZIP file. For now, we'll simply turn it into an absolute path and
   * pray that the file is on a shared drive or something like that.
   *
   * @param variables the variable variables to use
   * @param definitions The definitions to use
   * @param iResourceNaming The naming method
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
      if (!folderNameDynamic) {
        for (GSFile file : files) {
          FileObject fileObject = HopVfs.getFileObject(variables.resolve(file.getName()));
          file.setName(iResourceNaming.nameResource(fileObject, variables, true));
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  public static final class GSFile {
    @HopMetadataProperty(key = "name")
    private String name;

    @HopMetadataProperty(key = "file_required")
    private boolean required;

    public GSFile() {}

    public GSFile(GSFile f) {
      this.name = f.name;
      this.required = f.required;
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
     * Gets required
     *
     * @return value of required
     */
    public boolean isRequired() {
      return required;
    }

    /**
     * Sets required
     *
     * @param required value of required
     */
    public void setRequired(boolean required) {
      this.required = required;
    }
  }

  /**
   * Gets files
   *
   * @return value of files
   */
  public List<GSFile> getFiles() {
    return files;
  }

  /**
   * Sets files
   *
   * @param files value of files
   */
  public void setFiles(List<GSFile> files) {
    this.files = files;
  }

  /**
   * Gets includeRowNumber
   *
   * @return value of includeRowNumber
   */
  public boolean isIncludeRowNumber() {
    return includeRowNumber;
  }

  /**
   * Sets includeRowNumber
   *
   * @param includeRowNumber value of includeRowNumber
   */
  public void setIncludeRowNumber(boolean includeRowNumber) {
    this.includeRowNumber = includeRowNumber;
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
   * Gets dynamicFolderNameField
   *
   * @return value of dynamicFolderNameField
   */
  public String getDynamicFolderNameField() {
    return dynamicFolderNameField;
  }

  /**
   * Sets dynamicFolderNameField
   *
   * @param dynamicFolderNameField value of dynamicFolderNameField
   */
  public void setDynamicFolderNameField(String dynamicFolderNameField) {
    this.dynamicFolderNameField = dynamicFolderNameField;
  }

  /**
   * Gets folderNameDynamic
   *
   * @return value of folderNameDynamic
   */
  public boolean isFolderNameDynamic() {
    return folderNameDynamic;
  }

  /**
   * Sets folderNameDynamic
   *
   * @param folderNameDynamic value of folderNameDynamic
   */
  public void setFolderNameDynamic(boolean folderNameDynamic) {
    this.folderNameDynamic = folderNameDynamic;
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
}
