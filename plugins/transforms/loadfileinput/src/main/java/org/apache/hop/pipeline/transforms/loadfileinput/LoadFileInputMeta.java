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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.fileinput.InputFile;
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
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileInputAdditionalFields;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

@Transform(
    id = "LoadFileInput",
    image = "loadfileinput.svg",
    name = "i18n::LoadFileInput.Name",
    description = "i18n::LoadFileInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::LoadFileInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/loadfileinput.html")
@Getter
@Setter
public class LoadFileInputMeta extends BaseTransformMeta<LoadFileInput, LoadFileInputData> {
  private static final Class<?> PKG = LoadFileInputMeta.class;

  /** Flag indicating that we should include the filename in the output */
  @HopMetadataProperty(
      key = "include",
      injectionKey = "INCLUDE",
      injectionKeyDescription = "LoadFileInputMeta.Injection.INCLUDE")
  private boolean includeFilename;

  /** The name of the field in the output containing the filename */
  @HopMetadataProperty(
      key = "include_field",
      injectionKey = "INCLUDE_FIELD",
      injectionKeyDescription = "LoadFileInputMeta.Injection.INCLUDE_FIELD")
  private String filenameField;

  /** Flag indicating that a row number field should be included in the output */
  @HopMetadataProperty(
      key = "rownum",
      injectionKey = "ROWNUM",
      injectionKeyDescription = "LoadFileInputMeta.Injection.ROWNUM")
  private boolean includeRowNumber;

  /** The name of the field in the output containing the row number */
  @HopMetadataProperty(
      key = "rownum_field",
      injectionKey = "ROWNUM_FIELD",
      injectionKeyDescription = "LoadFileInputMeta.Injection.ROWNUM_FIELD")
  private String rowNumberField;

  /** The maximum number of lines to read */
  @HopMetadataProperty(
      key = "limit",
      injectionKey = "LIMIT",
      injectionKeyDescription = "LoadFileInputMeta.Injection.LIMIT")
  private long rowLimit;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @HopMetadataProperty(
      key = "encoding",
      injectionKey = "ENCODING",
      injectionKeyDescription = "LoadFileInputMeta.Injection.ENCODING")
  private String encoding;

  /** Dynamic FilenameField */
  @HopMetadataProperty(
      key = "DynamicFilenameField",
      injectionKey = "DYNAMIC_FILENAME_FIELD",
      injectionKeyDescription = "LoadFileInputMeta.Injection.DYNAMIC_FILENAME_FIELD")
  private String dynamicFilenameField;

  /** Is In fields */
  @HopMetadataProperty(
      key = "IsInFields",
      injectionKey = "IS_IN_FIELDS",
      injectionKeyDescription = "LoadFileInputMeta.Injection.IS_IN_FIELDS")
  private boolean fileInField;

  /** Flag: add result filename */
  @HopMetadataProperty(
      key = "addresultfile",
      injectionKey = "ADD_RESULT_FILE",
      injectionKeyDescription = "LoadFileInputMeta.Injection.ADD_RESULT_FILE")
  private boolean addingResultFile;

  /** Flag : do we ignore empty file? */
  @HopMetadataProperty(
      key = "IsIgnoreEmptyFile",
      injectionKey = "IGNORE_EMPTY_FILE",
      injectionKeyDescription = "LoadFileInputMeta.Injection.IGNORE_EMPTY_FILE")
  private boolean ignoreEmptyFile;

  /** Flag : do we ignore missing path? */
  @HopMetadataProperty(
      key = "IsIgnoreMissingPath",
      injectionKey = "IGNORE_MISSING_PATH",
      injectionKeyDescription = "LoadFileInputMeta.Injection.IGNORE_MISSING_PATH")
  private boolean ignoreMissingPath;

  @HopMetadataProperty(
      inline = true,
      childKeysToIgnore = {"sizeFieldName"})
  private BaseFileInputAdditionalFields additionalFields;

  @HopMetadataProperty(
      key = "file",
      groupKey = "files",
      injectionGroupKey = "FILES",
      injectionGroupDescription = "LoadFileInputMeta.Injection.FILES",
      childKeysToIgnore = {"type_filter"})
  private List<InputFile> inputFiles;

  /** The fields to import... */
  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionGroupKey = "FIELDS",
      injectionGroupDescription = "LoadFileInputMeta.Injection.FIELDS")
  private List<LoadFileInputField> inputFields;

  public LoadFileInputMeta() {
    super();
    additionalFields = new BaseFileInputAdditionalFields();
    inputFiles = new ArrayList<>();
    inputFields = new ArrayList<>();
    encoding = "";
    ignoreEmptyFile = false;
    ignoreMissingPath = false;
    includeFilename = false;
    filenameField = "";
    includeRowNumber = false;
    rowNumberField = "";
    addingResultFile = true;
    rowLimit = 0;
    fileInField = false;
    dynamicFilenameField = null;
  }

  public LoadFileInputMeta(LoadFileInputMeta m) {
    this();
    this.addingResultFile = m.addingResultFile;
    this.dynamicFilenameField = m.dynamicFilenameField;
    this.encoding = m.encoding;
    this.fileInField = m.fileInField;
    this.filenameField = m.filenameField;
    this.ignoreEmptyFile = m.ignoreEmptyFile;
    this.ignoreMissingPath = m.ignoreMissingPath;
    this.includeFilename = m.includeFilename;
    this.includeRowNumber = m.includeRowNumber;
    this.rowLimit = m.rowLimit;
    this.rowNumberField = m.rowNumberField;
    this.additionalFields = new BaseFileInputAdditionalFields(m.additionalFields);
    m.inputFields.forEach(field -> this.inputFields.add(new LoadFileInputField(field)));
    m.inputFiles.forEach(file -> this.inputFiles.add(new InputFile(file)));
  }

  @Override
  public Object clone() {
    return new LoadFileInputMeta(this);
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
    if (!isFileInField()) {
      r.clear();
    }
    for (LoadFileInputField field : inputFields) {
      try {
        IValueMeta v = field.createValueMeta(variables);
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
    if (!Utils.isEmpty(getAdditionalFields().getShortFilenameField())) {
      IValueMeta v =
          new ValueMetaString(variables.resolve(getAdditionalFields().getShortFilenameField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (!Utils.isEmpty(getAdditionalFields().getExtensionField())) {
      IValueMeta v =
          new ValueMetaString(variables.resolve(getAdditionalFields().getExtensionField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (!Utils.isEmpty(getAdditionalFields().getPathField())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getAdditionalFields().getPathField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (!Utils.isEmpty(getAdditionalFields().getHiddenField())) {
      IValueMeta v =
          new ValueMetaBoolean(variables.resolve(getAdditionalFields().getHiddenField()));
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (!Utils.isEmpty(getAdditionalFields().getLastModificationField())) {
      IValueMeta v =
          new ValueMetaDate(variables.resolve(getAdditionalFields().getLastModificationField()));
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (!Utils.isEmpty(getAdditionalFields().getUriField())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getAdditionalFields().getUriField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (!Utils.isEmpty(getAdditionalFields().getRootUriField())) {
      IValueMeta v =
          new ValueMetaString(variables.resolve(getAdditionalFields().getRootUriField()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
  }

  public FileInputList getFiles(IVariables variables) {
    return FileInputList.createFileList(variables, inputFiles);
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

    if (isFileInField()) {
      // See if we get input...
      if (input.length == 0) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "LoadFileInputMeta.CheckResult.NoInputExpected"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "LoadFileInputMeta.CheckResult.NoInput"),
                transformMeta);
        remarks.add(cr);
      }

      if (Utils.isEmpty(getDynamicFilenameField())) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "LoadFileInputMeta.CheckResult.NoField"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "LoadFileInputMeta.CheckResult.FieldOk"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      FileInputList fileInputList = getFiles(variables);

      if (fileInputList == null || fileInputList.getFiles().isEmpty()) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "LoadFileInputMeta.CheckResult.NoFiles"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
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
   * @param definitions The definitions to use
   * @param iResourceNaming The resource naming interface
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
      //
      if (!fileInField) {
        for (InputFile inputFile : inputFiles) {
          FileObject fileObject = HopVfs.getFileObject(variables.resolve(inputFile.getFileName()));
          inputFile.setFileName(
              iResourceNaming.nameResource(
                  fileObject, variables, Utils.isEmpty(inputFile.getFileMask())));
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Override
  public void convertLegacyXml(Node node) throws HopException {
    Node fileNode = XmlHandler.getSubNode(node, "file");
    if (fileNode != null) {
      // Load all file names here...
      int nrFiles = XmlHandler.countNodes(fileNode, "name");
      for (int i = 0; i < nrFiles; i++) {
        InputFile inputFile = new InputFile();
        inputFile.setFileName(
            XmlHandler.getNodeValue(XmlHandler.getSubNodeByNr(fileNode, "name", i)));
        inputFile.setFileMask(
            XmlHandler.getNodeValue(XmlHandler.getSubNodeByNr(fileNode, "filemask", i)));
        inputFile.setExcludeFileMask(
            XmlHandler.getNodeValue(XmlHandler.getSubNodeByNr(fileNode, "exclude_filemask", i)));
        inputFile.setFileRequired(
            Const.toBoolean(
                XmlHandler.getNodeValue(XmlHandler.getSubNodeByNr(fileNode, "file_required", i))));
        inputFile.setIncludeSubFolders(
            Const.toBoolean(
                XmlHandler.getNodeValue(
                    XmlHandler.getSubNodeByNr(fileNode, "include_subfolders", i))));
        inputFiles.add(inputFile);
      }
    }
  }
}
