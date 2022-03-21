/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.tika;

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
import org.apache.hop.core.row.value.ValueMetaString;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Transform(
    id = "Tika",
    image = "tika.svg",
    name = "i18n::Tika.Name",
    description = "i18n::Tika.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::Tika.Keywords")
public class TikaMeta extends BaseTransformMeta<Tika, TikaData> {
  private static final Class<?> PKG = TikaMeta.class; // for Translator

  /** The list of files to read */
  @HopMetadataProperty(
      groupKey = "files",
      key = "file",
      injectionGroupDescription = "TikaMeta.Injection.Files",
      injectionKeyDescription = "TikaMeta.Injection.File")
  private List<TikaFile> files;

  /** The name of the field in the output containing the filename */
  @HopMetadataProperty(
      key = "include-filename-field",
      injectionKeyDescription = "TikaMeta.Injection.FilenameField")
  private String filenameField;

  /** The name of the field in the output containing the row number */
  @HopMetadataProperty(
      key = "include-row-number-field",
      injectionKeyDescription = "TikaMeta.Injection.RowNumberField")
  private String rowNumberField;

  /** The maximum number or lines to read */
  @HopMetadataProperty(key = "row-limit", injectionKeyDescription = "TikaMeta.Injection.RowLimit")
  private long rowLimit;

  @HopMetadataProperty(
      key = "output-format",
      injectionKeyDescription = "TikaMeta.Injection.OutputFormat")
  private String outputFormat;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @HopMetadataProperty(injectionKeyDescription = "TikaMeta.Injection.Encoding")
  private String encoding;

  /** Dynamic FilenameField */
  @HopMetadataProperty(
      key = "dynamic-filename-field",
      injectionKeyDescription = "TikaMeta.Injection.DynamicFilenameField")
  private String dynamicFilenameField;

  /** Is In fields */
  @HopMetadataProperty(
      key = "file-in-field",
      injectionKeyDescription = "TikaMeta.Injection.FileInField")
  private boolean fileInField;

  /** Flag: add result filename */
  @HopMetadataProperty(
      key = "add-result-file",
      injectionKeyDescription = "TikaMeta.Injection.AddResultFile")
  private boolean addingResultFile;

  /** Flag : do we ignore empty file? */
  @HopMetadataProperty(
      key = "ignore-empty-file",
      injectionKeyDescription = "TikaMeta.Injection.IgnoreEmptyFile")
  private boolean ignoreEmptyFile;

  /** Content field name */
  @HopMetadataProperty(
      key = "content-field",
      injectionKeyDescription = "TikaMeta.Injection.ContentFieldName")
  private String contentFieldName;

  /** file size field name */
  @HopMetadataProperty(
      key = "file-size-field",
      injectionKeyDescription = "TikaMeta.Injection.FileSizeFieldName")
  private String fileSizeFieldName;

  /** Additional fields */
  @HopMetadataProperty(
      key = "short-filename-field",
      injectionKeyDescription = "TikaMeta.Injection.ShortFileFieldName")
  private String shortFileFieldName;

  @HopMetadataProperty(
      key = "path-field",
      injectionKeyDescription = "TikaMeta.Injection.PathFieldName")
  private String pathFieldName;

  @HopMetadataProperty(
      key = "hidden-field",
      injectionKeyDescription = "TikaMeta.Injection.HiddenFlagFieldName")
  private String hiddenFieldName;

  @HopMetadataProperty(
      key = "last-modification-time-field",
      injectionKeyDescription = "TikaMeta.Injection.LastModDateFieldName")
  private String lastModificationTimeFieldName;

  @HopMetadataProperty(
      key = "uri-field",
      injectionKeyDescription = "TikaMeta.Injection.UriFieldName")
  private String uriFieldName;

  @HopMetadataProperty(
      key = "root-uri-field",
      injectionKeyDescription = "TikaMeta.Injection.RootUriFieldName")
  private String rootUriNameFieldName;

  @HopMetadataProperty(
      key = "extension-field",
      injectionKeyDescription = "TikaMeta.Injection.ExtensionFieldName")
  private String extensionFieldName;

  @HopMetadataProperty(
      key = "metadata-field",
      injectionKeyDescription = "TikaMeta.Injection.MetadataFieldName")
  private String metadataFieldName;

  public TikaMeta() {
    this.files = new ArrayList<>();
    this.contentFieldName = "content";
    this.fileSizeFieldName = "fileSize";
    this.outputFormat = "Plain text";
    this.metadataFieldName = "metadata";
    this.filenameField = "filename";
  }

  public TikaMeta(TikaMeta meta) {
    this();
    for (TikaFile file : meta.files) {
      this.files.add(new TikaFile(file));
    }
    this.contentFieldName = meta.contentFieldName;
    this.fileSizeFieldName = meta.fileSizeFieldName;
    this.filenameField = meta.filenameField;
    this.rowNumberField = meta.rowNumberField;
    this.rowLimit = meta.rowLimit;
    this.outputFormat = meta.outputFormat;
    this.encoding = meta.encoding;
    this.dynamicFilenameField = meta.dynamicFilenameField;
    this.fileInField = meta.fileInField;
    this.addingResultFile = meta.addingResultFile;
    this.ignoreEmptyFile = meta.ignoreEmptyFile;
    this.shortFileFieldName = meta.shortFileFieldName;
    this.pathFieldName = meta.pathFieldName;
    this.hiddenFieldName = meta.hiddenFieldName;
    this.lastModificationTimeFieldName = meta.lastModificationTimeFieldName;
    this.uriFieldName = meta.uriFieldName;
    this.rootUriNameFieldName = meta.rootUriNameFieldName;
    this.extensionFieldName = meta.extensionFieldName;
    this.metadataFieldName = meta.metadataFieldName;
  }

  public TikaMeta clone() {
    return new TikaMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta rowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (!isFileInField()) {
      rowMeta.clear();
    }
    if (StringUtils.isNotEmpty(contentFieldName)) {
      rowMeta.addValueMeta(new ValueMetaString(contentFieldName));
    }
    if (StringUtils.isNotEmpty(fileSizeFieldName)) {
      rowMeta.addValueMeta(new ValueMetaInteger(fileSizeFieldName));
    }
    if (StringUtils.isNotEmpty(metadataFieldName)) {
      rowMeta.addValueMeta(new ValueMetaString(variables.resolve(metadataFieldName)));
    }
    if (StringUtils.isNotEmpty(filenameField)) {
      IValueMeta v = new ValueMetaString(variables.resolve(filenameField));
      v.setLength(250);
      v.setPrecision(-1);
      v.setOrigin(name);
      rowMeta.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(rowNumberField)) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(rowNumberField));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      rowMeta.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(shortFileFieldName)) {
      IValueMeta v = new ValueMetaString(variables.resolve(shortFileFieldName));
      v.setLength(100, -1);
      v.setOrigin(name);
      rowMeta.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(extensionFieldName)) {
      IValueMeta v = new ValueMetaString(variables.resolve(extensionFieldName));
      v.setLength(100, -1);
      v.setOrigin(name);
      rowMeta.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(pathFieldName)) {
      IValueMeta v = new ValueMetaString(variables.resolve(pathFieldName));
      v.setLength(100, -1);
      v.setOrigin(name);
      rowMeta.addValueMeta(v);
    }

    if (StringUtils.isNotEmpty(hiddenFieldName)) {
      IValueMeta v = new ValueMetaString(variables.resolve(hiddenFieldName));
      v.setOrigin(name);
      rowMeta.addValueMeta(v);
    }

    if (StringUtils.isNotEmpty(lastModificationTimeFieldName)) {
      IValueMeta v = new ValueMetaString(variables.resolve(lastModificationTimeFieldName));
      v.setOrigin(name);
      rowMeta.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(uriFieldName)) {
      IValueMeta v = new ValueMetaString(variables.resolve(uriFieldName));
      v.setLength(100, -1);
      v.setOrigin(name);
      rowMeta.addValueMeta(v);
    }
    if (StringUtils.isNotEmpty(rootUriNameFieldName)) {
      IValueMeta v = new ValueMetaString(variables.resolve(rootUriNameFieldName));
      v.setLength(100, -1);
      v.setOrigin(name);
      rowMeta.addValueMeta(v);
    }
  }

  private String[] getFileNames() {
    String[] fileNames = new String[files.size()];
    for (int i = 0; i < files.size(); i++) {
      fileNames[i] = files.get(i).getName();
    }
    return fileNames;
  }

  private String[] getFileMasks() {
    String[] fileMasks = new String[files.size()];
    for (int i = 0; i < files.size(); i++) {
      fileMasks[i] = files.get(i).getMask();
    }
    return fileMasks;
  }

  private String[] getExcludeFileMasks() {
    String[] excludeFileMasks = new String[files.size()];
    for (int i = 0; i < files.size(); i++) {
      excludeFileMasks[i] = files.get(i).getExcludeMask();
    }
    return excludeFileMasks;
  }

  private String[] getFileRequired() {
    String[] required = new String[files.size()];
    for (int i = 0; i < files.size(); i++) {
      required[i] = files.get(i).isRequired() ? "Y" : "N";
    }
    return required;
  }

  private boolean[] getIncludeSubFolders() {
    boolean[] included = new boolean[files.size()];
    for (int i = 0; i < files.size(); i++) {
      included[i] = files.get(i).isIncludingSubFolders();
    }
    return included;
  }

  public FileInputList getFiles(IVariables variables) {
    return FileInputList.createFileList(
        variables,
        getFileNames(),
        getFileMasks(),
        getExcludeFileMasks(),
        getFileRequired(),
        getIncludeSubFolders());
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
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "TikaMeta.CheckResult.NoInputExpected"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "TikaMeta.CheckResult.NoInput"),
              transformMeta);
      remarks.add(cr);
    }

    if (isFileInField()) {
      if (StringUtils.isEmpty(getDynamicFilenameField())) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "TikaMeta.CheckResult.NoField"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "TikaMeta.CheckResult.FieldOk"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      FileInputList fileInputList = getFiles(variables);

      if (fileInputList == null || fileInputList.getFiles().size() == 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "TikaMeta.CheckResult.NoFiles"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "TikaMeta.CheckResult.FilesOk", "" + fileInputList.getFiles().size()),
                transformMeta);
        remarks.add(cr);
      }
    }
  }

  /**
   * @param variables the variables to use
   * @param definitions
   * @param iResourceNaming
   * @param metadataProvider the provider of Hop metadata
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
      //
      if (!fileInField) {
        for (TikaFile file : files) {
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

  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * Gets files
   *
   * @return value of files
   */
  public List<TikaFile> getFiles() {
    return files;
  }

  /** @param files The files to set */
  public void setFiles(List<TikaFile> files) {
    this.files = files;
  }

  /**
   * Gets contentFieldName
   *
   * @return value of contentFieldName
   */
  public String getContentFieldName() {
    return contentFieldName;
  }

  /** @param contentFieldName The contentFieldName to set */
  public void setContentFieldName(String contentFieldName) {
    this.contentFieldName = contentFieldName;
  }

  /**
   * Gets fileSizeFieldName
   *
   * @return value of fileSizeFieldName
   */
  public String getFileSizeFieldName() {
    return fileSizeFieldName;
  }

  /** @param fileSizeFieldName The fileSizeFieldName to set */
  public void setFileSizeFieldName(String fileSizeFieldName) {
    this.fileSizeFieldName = fileSizeFieldName;
  }

  /**
   * Gets filenameField
   *
   * @return value of filenameField
   */
  public String getFilenameField() {
    return filenameField;
  }

  /** @param filenameField The filenameField to set */
  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }

  /**
   * Gets rowNumberField
   *
   * @return value of rowNumberField
   */
  public String getRowNumberField() {
    return rowNumberField;
  }

  /** @param rowNumberField The rowNumberField to set */
  public void setRowNumberField(String rowNumberField) {
    this.rowNumberField = rowNumberField;
  }

  /**
   * Gets rowLimit
   *
   * @return value of rowLimit
   */
  public long getRowLimit() {
    return rowLimit;
  }

  /** @param rowLimit The rowLimit to set */
  public void setRowLimit(long rowLimit) {
    this.rowLimit = rowLimit;
  }

  /**
   * Gets outputFormat
   *
   * @return value of outputFormat
   */
  public String getOutputFormat() {
    return outputFormat;
  }

  /** @param outputFormat The outputFormat to set */
  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  /**
   * Gets encoding
   *
   * @return value of encoding
   */
  public String getEncoding() {
    return encoding;
  }

  /** @param encoding The encoding to set */
  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /**
   * Gets dynamicFilenameField
   *
   * @return value of dynamicFilenameField
   */
  public String getDynamicFilenameField() {
    return dynamicFilenameField;
  }

  /** @param dynamicFilenameField The dynamicFilenameField to set */
  public void setDynamicFilenameField(String dynamicFilenameField) {
    this.dynamicFilenameField = dynamicFilenameField;
  }

  /**
   * Gets fileInField
   *
   * @return value of fileInField
   */
  public boolean isFileInField() {
    return fileInField;
  }

  /** @param fileInField The fileInField to set */
  public void setFileInField(boolean fileInField) {
    this.fileInField = fileInField;
  }

  /**
   * Gets addingResultFile
   *
   * @return value of addingResultFile
   */
  public boolean isAddingResultFile() {
    return addingResultFile;
  }

  /** @param addingResultFile The addingResultFile to set */
  public void setAddingResultFile(boolean addingResultFile) {
    this.addingResultFile = addingResultFile;
  }

  /**
   * Gets ignoreEmptyFile
   *
   * @return value of ignoreEmptyFile
   */
  public boolean isIgnoreEmptyFile() {
    return ignoreEmptyFile;
  }

  /** @param ignoreEmptyFile The ignoreEmptyFile to set */
  public void setIgnoreEmptyFile(boolean ignoreEmptyFile) {
    this.ignoreEmptyFile = ignoreEmptyFile;
  }

  /**
   * Gets shortFileFieldName
   *
   * @return value of shortFileFieldName
   */
  public String getShortFileFieldName() {
    return shortFileFieldName;
  }

  /** @param shortFileFieldName The shortFileFieldName to set */
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

  /** @param pathFieldName The pathFieldName to set */
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

  /** @param hiddenFieldName The hiddenFieldName to set */
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

  /** @param lastModificationTimeFieldName The lastModificationTimeFieldName to set */
  public void setLastModificationTimeFieldName(String lastModificationTimeFieldName) {
    this.lastModificationTimeFieldName = lastModificationTimeFieldName;
  }

  /**
   * Gets uri field name
   *
   * @return value of uriFieldName
   */
  public String getUriFieldName() {
    return uriFieldName;
  }

  /** @param uriFieldName The uriFieldName to set */
  public void setUriFieldName(String uriFieldName) {
    this.uriFieldName = uriFieldName;
  }

  /**
   * Gets rootUriNameFieldName
   *
   * @return value of rootUriNameFieldName
   */
  public String getRootUriNameFieldName() {
    return rootUriNameFieldName;
  }

  /** @param rootUriNameFieldName The rootUriNameFieldName to set */
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

  /** @param extensionFieldName The extensionFieldName to set */
  public void setExtensionFieldName(String extensionFieldName) {
    this.extensionFieldName = extensionFieldName;
  }

  /**
   * Gets metadataFieldName
   *
   * @return value of metadataFieldName
   */
  public String getMetadataFieldName() {
    return metadataFieldName;
  }

  /** @param metadataFieldName The metadataFieldName to set */
  public void setMetadataFieldName(String metadataFieldName) {
    this.metadataFieldName = metadataFieldName;
  }
}
