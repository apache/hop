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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;

/** Store run-time data on the getXMLData transform. */
@Transform(
    id = "getXMLData",
    image = "GXD.svg",
    name = "i18n::GetXMLData.name",
    description = "i18n::GetXMLData.description",
    categoryDescription = "i18n::GetXMLData.category",
    keywords = "i18n::GetXmlDataMeta.keyword",
    documentationUrl = "/pipeline/transforms/getdatafromxml.html")
@Getter
@Setter
public class GetXmlDataMeta extends BaseTransformMeta<GetXmlData, GetXmlDataData> {
  private static final Class<?> PKG = GetXmlDataMeta.class;

  private static final String YES = "Y";

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };
  public static final String[] RequiredFilesCode = new String[] {"N", "Y"};

  public static final String AT = "@";
  public static final String N0DE_SEPARATOR = "/";
  public static final String CONST_SPACES = "      ";
  public static final String CONST_FIELD = "field";

  @HopMetadataProperty(
      key = "file",
      injectionKeyDescription = "GetXmlDataMeta.Injection.File.Label",
      injectionGroupKey = "files",
      injectionGroupDescription = "GetXmlDataMeta.Injection.Group.FileTab.Label",
      inlineListTags = {
        "name",
        "filemask",
        "exclude_filemask",
        "file_required",
        "include_subfolders"
      })
  private List<GetXmlFileItem> filesList;

  /** Flag indicating that we should include the filename in the output */
  @HopMetadataProperty(
      key = "include",
      injectionKeyDescription = "GetXmlDataMeta.Injection.IncludeFileName")
  private boolean includeFilename;

  /** The name of the field in the output containing the filename */
  @HopMetadataProperty(
      key = "include_field",
      injectionKeyDescription = "GetXmlDataMeta.Injection.FilenameField")
  private String filenameField;

  /** Flag indicating that a row number field should be included in the output */
  @HopMetadataProperty(
      key = "rownum",
      injectionKeyDescription = "GetXmlDataMeta.Injection.IncludeRowNumber")
  private boolean includeRowNumber;

  /** The name of the field in the output containing the row number */
  @HopMetadataProperty(
      key = "rownum_field",
      injectionKeyDescription = "GetXmlDataMeta.Injection.RowNumberField")
  private String rowNumberField;

  /** The maximum number or lines to read */
  @HopMetadataProperty(key = "limit", injectionKeyDescription = "GetXmlDataMeta.Injection.RowLimit")
  private long rowLimit;

  /** The XPath location to loop over */
  @HopMetadataProperty(
      key = "loopxpath",
      injectionKeyDescription = "GetXmlDataMeta.Injection.LoopXPath")
  private String loopXPath;

  @HopMetadataProperty(
      key = "field",
      groupKey = "fields",
      injectionKeyDescription = "GetXmlDataMeta.Injection.Fields.Label",
      injectionGroupDescription = "GetXmlDataMeta.Injection.Group.Fields.Label")
  private List<GetXmlDataField> inputFields;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @HopMetadataProperty(injectionKeyDescription = "GetXmlDataMeta.Injection.Encoding")
  private String encoding;

  /** Is In fields */
  @HopMetadataProperty(
      key = "XmlField",
      injectionKeyDescription = "GetXmlDataMeta.Injection.XmlField")
  private String xmlField;

  /** Is In fields */
  @HopMetadataProperty(
      key = "IsInFields",
      injectionKeyDescription = "GetXmlDataMeta.Injection.InFields")
  private boolean inFields;

  /** Is a File */
  @HopMetadataProperty(
      key = "IsAFile",
      injectionKeyDescription = "GetXmlDataMeta.Injection.IsAFile")
  private boolean aFile;

  /** Flag: add result filename * */
  @HopMetadataProperty(
      key = "addresultfile",
      injectionKeyDescription = "GetXmlDataMeta.Injection.AddResultFile")
  private boolean addResultFile;

  /** Flag: set Namespace aware * */
  @HopMetadataProperty(
      key = "namespaceaware",
      injectionKeyDescription = "GetXmlDataMeta.Injection.NameSpaceAware")
  private boolean nameSpaceAware;

  /** Flag: set XML Validating * */
  @HopMetadataProperty(injectionKeyDescription = "GetXmlDataMeta.Injection.Validating")
  private boolean validating;

  /** Flag : do we process use tokens? */
  @HopMetadataProperty(
      key = "usetoken",
      injectionKeyDescription = "GetXmlDataMeta.Injection.Usetoken")
  private boolean useToken;

  /** Flag : do we ignore empty files */
  @HopMetadataProperty(
      key = "IsIgnoreEmptyFile",
      injectionKeyDescription = "GetXmlDataMeta.Injection.IsIgnoreEmptyFile")
  private boolean ignoreEmptyFile;

  /** Flag : do not fail if no file */
  @HopMetadataProperty(injectionKeyDescription = "GetXmlDataMeta.Injection.DoNotFailIfNoFile")
  private boolean doNotFailIfNoFile;

  /** Flag : ignore comments */
  @HopMetadataProperty(
      key = "ignorecomments",
      injectionKeyDescription = "GetXmlDataMeta.Injection.IgnoreComments")
  private boolean ignoreComments;

  /** Flag : read url as source */
  @HopMetadataProperty(
      key = "readurl",
      injectionKeyDescription = "GetXmlDataMeta.Injection.ReadUrl")
  private boolean readUrl;

  // Given this path activates the streaming algorithm to process large files
  @HopMetadataProperty(injectionKeyDescription = "GetXmlDataMeta.Injection.PrunePath")
  private String prunePath;

  /** Additional fields * */
  @HopMetadataProperty(
      injectionKeyDescription = "GetXmlDataMeta.Injection.PrunePath",
      injectionGroupKey = "AdditionalFields",
      injectionGroupDescription = "GetXmlDataMeta.Injection.Group.Additional.Label")
  private String shortFileFieldName;

  @HopMetadataProperty(
      injectionKeyDescription = "GetXmlDataMeta.Injection.PathFieldName",
      injectionGroupKey = "AdditionalFields",
      injectionGroupDescription = "GetXmlDataMeta.Injection.Group.Additional.Label")
  private String pathFieldName;

  @HopMetadataProperty(
      injectionKeyDescription = "GetXmlDataMeta.Injection.HiddenFieldName",
      injectionGroupKey = "AdditionalFields",
      injectionGroupDescription = "GetXmlDataMeta.Injection.Group.Additional.Label")
  private String hiddenFieldName;

  @HopMetadataProperty(
      injectionKeyDescription = "GetXmlDataMeta.Injection.LastModificationTimeFieldName",
      injectionGroupKey = "AdditionalFields",
      injectionGroupDescription = "GetXmlDataMeta.Injection.Group.Additional.Label")
  private String lastModificationTimeFieldName;

  @HopMetadataProperty(
      injectionKeyDescription = "GetXmlDataMeta.Injection.UriNameFieldName",
      injectionGroupKey = "AdditionalFields",
      injectionGroupDescription = "GetXmlDataMeta.Injection.Group.Additional.Label")
  private String uriNameFieldName;

  @HopMetadataProperty(
      injectionKeyDescription = "GetXmlDataMeta.Injection.RootUriNameFieldName",
      injectionGroupKey = "AdditionalFields",
      injectionGroupDescription = "GetXmlDataMeta.Injection.Group.Additional.Label")
  private String rootUriNameFieldName;

  @HopMetadataProperty(
      injectionKeyDescription = "GetXmlDataMeta.Injection.ExtensionFieldName",
      injectionGroupKey = "AdditionalFields",
      injectionGroupDescription = "GetXmlDataMeta.Injection.Group.Additional.Label")
  private String extensionFieldName;

  @HopMetadataProperty(
      injectionKeyDescription = "GetXmlDataMeta.Injection.SizeFieldName",
      injectionGroupKey = "AdditionalFields",
      injectionGroupDescription = "GetXmlDataMeta.Injection.Group.Additional.Label")
  private String sizeFieldName;

  public GetXmlDataMeta() {
    super(); // allocate BaseTransformMeta
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

  @Override
  public void setDefault() {
    shortFileFieldName = null;
    pathFieldName = null;
    hiddenFieldName = null;
    lastModificationTimeFieldName = null;
    uriNameFieldName = null;
    rootUriNameFieldName = null;
    extensionFieldName = null;
    sizeFieldName = null;

    useToken = false;
    ignoreEmptyFile = false;
    doNotFailIfNoFile = true;
    includeFilename = false;
    filenameField = "";
    includeRowNumber = false;
    rowNumberField = "";
    aFile = false;
    addResultFile = false;
    nameSpaceAware = false;
    ignoreComments = false;
    readUrl = false;
    validating = false;
    loopXPath = "";

    filesList = new ArrayList<>();
    inputFields = new ArrayList<>();

    rowLimit = 0;

    inFields = false;
    xmlField = "";
    prunePath = "";
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
    for (i = 0; i < inputFields.size(); i++) {
      GetXmlDataField field = inputFields.get(i);

      int type = ValueMetaBase.getType(field.getType());
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

    if (!Utils.isEmpty(getShortFileFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getShortFileFieldName()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (!Utils.isEmpty(getExtensionFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getExtensionFieldName()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (!Utils.isEmpty(getPathFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getPathFieldName()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (!Utils.isEmpty(getSizeFieldName())) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(getSizeFieldName()));
      v.setOrigin(name);
      v.setLength(9);
      r.addValueMeta(v);
    }
    if (!Utils.isEmpty(getHiddenFieldName())) {
      IValueMeta v = new ValueMetaBoolean(variables.resolve(getHiddenFieldName()));
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (!Utils.isEmpty(getLastModificationTimeFieldName())) {
      IValueMeta v = new ValueMetaDate(variables.resolve(getLastModificationTimeFieldName()));
      v.setOrigin(name);
      r.addValueMeta(v);
    }
    if (!Utils.isEmpty(getUriNameFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getUriNameFieldName()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }

    if (!Utils.isEmpty(getRootUriNameFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(getRootUriNameFieldName()));
      v.setLength(100, -1);
      v.setOrigin(name);
      r.addValueMeta(v);
    }
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

  public FileInputList getFiles(IVariables variables) {
    return FileInputList.createFileList(
        variables,
        buildFilenamesArray(),
        buildMasksArray(),
        buildExcludeMasksArray(),
        buildFileRequiredArray(),
        includeSubFolderBoolean());
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
              BaseMessages.getString(PKG, "GetXMLDataMeta.CheckResult.NoInputExpected"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "GetXMLDataMeta.CheckResult.NoInput"),
              transformMeta);
      remarks.add(cr);
    }

    // control Xpath
    if (getLoopXPath() == null || Utils.isEmpty(getLoopXPath())) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetXMLDataMeta.CheckResult.NoLoopXpath"),
              transformMeta);
      remarks.add(cr);
    }
    if (getInputFields().size() <= 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "GetXMLDataMeta.CheckResult.NoInputField"),
              transformMeta);
      remarks.add(cr);
    }

    if (isInFields()) {
      if (Utils.isEmpty(getXmlField())) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "GetXMLDataMeta.CheckResult.NoField"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "GetXMLDataMeta.CheckResult.FieldOk"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      FileInputList fileInputList = getFiles(variables);
      if (fileInputList == null || fileInputList.getFiles().isEmpty()) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "GetXMLDataMeta.CheckResult.NoFiles"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG,
                    "GetXMLDataMeta.CheckResult.FilesOk",
                    "" + fileInputList.getFiles().size()),
                transformMeta);
        remarks.add(cr);
      }
    }
  }

  @Override
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
  @Override
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
        if (!fileList.getFiles().isEmpty()) {
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
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  @Override
  public PipelineMeta.PipelineType[] getSupportedPipelineTypes() {
    return new PipelineMeta.PipelineType[] {PipelineMeta.PipelineType.Normal};
  }
}
