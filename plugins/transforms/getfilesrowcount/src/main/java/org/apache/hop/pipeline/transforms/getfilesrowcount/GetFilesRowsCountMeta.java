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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;

@Transform(
    id = "GetFilesRowsCount",
    image = "getfilesrowcount.svg",
    name = "i18n::GetFilesRowsCount.Name",
    description = "i18n::GetFilesRowsCount.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::GetFilesRowsCountMeta.keyword",
    documentationUrl = "/pipeline/transforms/getfilesrowcount.html")
public class GetFilesRowsCountMeta
    extends BaseTransformMeta<GetFilesRowsCount, GetFilesRowsCountData> {
  private static final Class<?> PKG = GetFilesRowsCountMeta.class;

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };
  public static final String[] RequiredFilesCode = new String[] {"N", "Y"};
  private static final String YES = "Y";

  @HopMetadataProperty(key = "file")
  private List<GCFile> files;

  /** Flag indicating that a row number field should be included in the output */
  @HopMetadataProperty(key = "files_count")
  private boolean includeFilesCount;

  /** The name of the field in the output containing the file number */
  @HopMetadataProperty(key = "files_count_fieldname")
  private String filesCountFieldName;

  /** The name of the field in the output containing the row number */
  @HopMetadataProperty(key = "rows_count_fieldname")
  private String rowsCountFieldName;

  /** The row separator type */
  @HopMetadataProperty(key = "rowseparator_format", storeWithCode = true)
  private SeparatorFormat rowSeparatorFormat;

  /** The row separator */
  @HopMetadataProperty(key = "row_separator")
  private String rowSeparator;

  /** file name from previous fields */
  @HopMetadataProperty(key = "filefield")
  private boolean fileFromField;

  @HopMetadataProperty(key = "isaddresult")
  private boolean addResultFilename;

  @HopMetadataProperty(key = "filename_Field")
  private String outputFilenameField;

  /** Flag : check if a data is there right after separator */
  @HopMetadataProperty(key = "smartCount")
  private boolean smartCount;

  public GetFilesRowsCountMeta() {
    super();
    this.files = new ArrayList<>();
  }

  public GetFilesRowsCountMeta(GetFilesRowsCountMeta m) {
    this();
    this.includeFilesCount = m.includeFilesCount;
    this.filesCountFieldName = m.filesCountFieldName;
    this.rowsCountFieldName = m.rowsCountFieldName;
    this.rowSeparatorFormat = m.rowSeparatorFormat;
    this.rowSeparator = m.rowSeparator;
    this.fileFromField = m.fileFromField;
    this.addResultFilename = m.addResultFilename;
    this.outputFilenameField = m.outputFilenameField;
    this.smartCount = m.smartCount;
    m.files.forEach(f -> this.files.add(new GCFile(f)));
  }

  @Override
  public GetFilesRowsCountMeta clone() {
    return new GetFilesRowsCountMeta(this);
  }

  @Override
  public void setDefault() {
    smartCount = false;
    outputFilenameField = "";
    fileFromField = false;
    addResultFilename = true;
    includeFilesCount = false;
    filesCountFieldName = "";
    rowsCountFieldName = "rowsCount";
    rowSeparatorFormat = SeparatorFormat.CR;
    rowSeparator = "";
  }

  @Override
  public void getFields(
      IRowMeta r,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
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

  public String[] getFilesNames() {
    return files.stream().map(GCFile::getName).toArray(String[]::new);
  }

  public String[] getFilesMasks() {
    return files.stream().map(GCFile::getMask).toArray(String[]::new);
  }

  public String[] getFilesExcludeMasks() {
    return files.stream().map(GCFile::getExcludeMask).toArray(String[]::new);
  }

  public String[] getFilesRequired() {
    return files.stream().map(f -> f.isRequired() ? "Y" : "N").toArray(String[]::new);
  }

  public boolean[] getFilesSubFolderIncluded() {
    boolean[] b = new boolean[files.size()];
    for (int i = 0; i < b.length; i++) {
      b[i] = files.get(i).isIncludeSubFolder();
    }
    return b;
  }

  public FileInputList getFiles(IVariables variables) {
    return FileInputList.createFileList(
        variables,
        getFilesNames(),
        getFilesMasks(),
        getFilesExcludeMasks(),
        getFilesRequired(),
        getFilesSubFolderIncluded());
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
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetFilesRowsCountMeta.CheckResult.NoInputExpected"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "GetFilesRowsCountMeta.CheckResult.NoInput"),
              transformMeta);
      remarks.add(cr);
    }

    FileInputList fileInputList = getFiles(variables);

    if (fileInputList == null || fileInputList.getFiles().isEmpty()) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetFilesRowsCountMeta.CheckResult.NoFiles"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG,
                  "GetFilesRowsCountMeta.CheckResult.FilesOk",
                  "" + fileInputList.getFiles().size()),
              transformMeta);
      remarks.add(cr);
    }

    if ((rowSeparatorFormat.equals("CUSTOM")) && (rowSeparator == null)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetFilesRowsCountMeta.CheckResult.NoSeparator"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "GetFilesRowsCountMeta.CheckResult.SeparatorOk"),
              transformMeta);
      remarks.add(cr);
    }
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
      if (!fileFromField) {
        for (GCFile file : files) {
          FileObject fileObject = HopVfs.getFileObject(variables.resolve(file.getName()));
          file.setName(
              iResourceNaming.nameResource(
                  fileObject, variables, StringUtils.isEmpty(file.getMask())));
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  /**
   * Gets files
   *
   * @return value of files
   */
  public List<GCFile> getFiles() {
    return files;
  }

  /**
   * Sets files
   *
   * @param files value of files
   */
  public void setFiles(List<GCFile> files) {
    this.files = files;
  }

  /**
   * Gets includeFilesCount
   *
   * @return value of includeFilesCount
   */
  public boolean isIncludeFilesCount() {
    return includeFilesCount;
  }

  /**
   * Sets includeFilesCount
   *
   * @param includeFilesCount value of includeFilesCount
   */
  public void setIncludeFilesCount(boolean includeFilesCount) {
    this.includeFilesCount = includeFilesCount;
  }

  /**
   * Gets filesCountFieldName
   *
   * @return value of filesCountFieldName
   */
  public String getFilesCountFieldName() {
    return filesCountFieldName;
  }

  /**
   * Sets filesCountFieldName
   *
   * @param filesCountFieldName value of filesCountFieldName
   */
  public void setFilesCountFieldName(String filesCountFieldName) {
    this.filesCountFieldName = filesCountFieldName;
  }

  /**
   * Gets rowsCountFieldName
   *
   * @return value of rowsCountFieldName
   */
  public String getRowsCountFieldName() {
    return rowsCountFieldName;
  }

  /**
   * Sets rowsCountFieldName
   *
   * @param rowsCountFieldName value of rowsCountFieldName
   */
  public void setRowsCountFieldName(String rowsCountFieldName) {
    this.rowsCountFieldName = rowsCountFieldName;
  }

  /**
   * Gets rowSeparatorFormat
   *
   * @return value of rowSeparatorFormat
   */
  public SeparatorFormat getRowSeparatorFormat() {
    return rowSeparatorFormat;
  }

  /**
   * Sets rowSeparatorFormat
   *
   * @param rowSeparatorFormat value of rowSeparatorFormat
   */
  public void setRowSeparatorFormat(SeparatorFormat rowSeparatorFormat) {
    this.rowSeparatorFormat = rowSeparatorFormat;
  }

  /**
   * Gets rowSeparator
   *
   * @return value of rowSeparator
   */
  public String getRowSeparator() {
    return rowSeparator;
  }

  /**
   * Sets rowSeparator
   *
   * @param rowSeparator value of rowSeparator
   */
  public void setRowSeparator(String rowSeparator) {
    this.rowSeparator = rowSeparator;
  }

  /**
   * Gets fileFromField
   *
   * @return value of fileFromField
   */
  public boolean isFileFromField() {
    return fileFromField;
  }

  /**
   * Sets fileFromField
   *
   * @param fileFromField value of fileFromField
   */
  public void setFileFromField(boolean fileFromField) {
    this.fileFromField = fileFromField;
  }

  /**
   * Gets addResultFilename
   *
   * @return value of addResultFilename
   */
  public boolean isAddResultFilename() {
    return addResultFilename;
  }

  /**
   * Sets addResultFilename
   *
   * @param addResultFilename value of addResultFilename
   */
  public void setAddResultFilename(boolean addResultFilename) {
    this.addResultFilename = addResultFilename;
  }

  /**
   * Gets outputFilenameField
   *
   * @return value of outputFilenameField
   */
  public String getOutputFilenameField() {
    return outputFilenameField;
  }

  /**
   * Sets outputFilenameField
   *
   * @param outputFilenameField value of outputFilenameField
   */
  public void setOutputFilenameField(String outputFilenameField) {
    this.outputFilenameField = outputFilenameField;
  }

  /**
   * Gets smartCount
   *
   * @return value of smartCount
   */
  public boolean isSmartCount() {
    return smartCount;
  }

  /**
   * Sets smartCount
   *
   * @param smartCount value of smartCount
   */
  public void setSmartCount(boolean smartCount) {
    this.smartCount = smartCount;
  }

  public static final class GCFile {
    @HopMetadataProperty(key = "name")
    private String name;

    @HopMetadataProperty(key = "filemask")
    private String mask;

    @HopMetadataProperty(key = "exclude_filemask")
    private String excludeMask;

    @HopMetadataProperty(key = "file_required")
    private boolean required;

    @HopMetadataProperty(key = "include_subfolders")
    private boolean includeSubFolder;

    public GCFile() {}

    public GCFile(GCFile f) {
      this();
      this.name = f.name;
      this.mask = f.mask;
      this.excludeMask = f.excludeMask;
      this.required = f.required;
      this.includeSubFolder = f.includeSubFolder;
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

    /**
     * Gets includeSubFolder
     *
     * @return value of includeSubFolder
     */
    public boolean isIncludeSubFolder() {
      return includeSubFolder;
    }

    /**
     * Sets includeSubFolder
     *
     * @param includeSubFolder value of includeSubFolder
     */
    public void setIncludeSubFolder(boolean includeSubFolder) {
      this.includeSubFolder = includeSubFolder;
    }
  }

  public enum SeparatorFormat implements IEnumHasCodeAndDescription {
    CR(
        "CARRIAGERETURN",
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.CR.Label")),
    LF(
        "LINEFEED",
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.LF.Label")),
    CRLF(
        "CRLF",
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.CRLF.Label")),
    TAB("TAB", BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.TAB.Label")),
    CUSTOM(
        "CUSTOM",
        BaseMessages.getString(PKG, "GetFilesRowsCountDialog.RowSeparatorFormat.CUSTOM.Label"));
    private final String code;
    private final String description;

    SeparatorFormat(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(SeparatorFormat.class);
    }

    public static SeparatorFormat lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(SeparatorFormat.class, description, CR);
    }

    /**
     * Gets code
     *
     * @return value of code
     */
    @Override
    public String getCode() {
      return code;
    }

    /**
     * Gets description
     *
     * @return value of description
     */
    @Override
    public String getDescription() {
      return description;
    }
  }
}
