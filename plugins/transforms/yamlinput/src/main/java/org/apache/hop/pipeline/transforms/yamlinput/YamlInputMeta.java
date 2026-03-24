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
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.IResourceNaming;
import org.apache.hop.resource.ResourceDefinition;
import org.w3c.dom.Node;

@Transform(
    id = "YamlInput",
    image = "yamlinput.svg",
    name = "i18n::YamlInput.Name",
    description = "i18n::YamlInput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    keywords = "i18n::YamlInputMeta.keyword",
    documentationUrl = "/pipeline/transforms/yamlinput.html")
@Getter
@Setter
public class YamlInputMeta extends BaseTransformMeta<YamlInput, YamlInputData> {
  private static final Class<?> PKG = YamlInputMeta.class;

  private static final String YES = "Y";

  public static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };
  public static final String[] RequiredFilesCode = new String[] {"N", "Y"};
  public static final String CONST_FIELD = "field";

  @Getter
  @Setter
  public static class YamlFile {
    @HopMetadataProperty(key="name", injectionKey = "filename")
    private String filename;
    @HopMetadataProperty(key="filemask")
    private String fileMask;
    @HopMetadataProperty(key="file_required")
    private boolean fileRequired;
    @HopMetadataProperty(key="include_subfolders")
    private boolean includingSubFolders;

    public YamlFile() {
      filename = "";
      fileMask = "";
      fileRequired = false;
    }

    public YamlFile(YamlFile f) {
      this();
      this.filename = f.filename;
      this.fileMask = f.fileMask;
      this.fileRequired = f.fileRequired;
      this.includingSubFolders = f.includingSubFolders;
    }
  }

  @HopMetadataProperty(key="")
  private List<YamlFile> yamlFiles;

  /** Flag indicating that we should include the filename in the output */
  @HopMetadataProperty(key="include")
  private boolean includeFilename;

  /** The name of the field in the output containing the filename */
  @HopMetadataProperty(key="include_field")
  private String filenameField;

  /** Flag indicating that a row number field should be included in the output */
  @HopMetadataProperty(key="rownum")
  private boolean includeRowNumber;

  /** The name of the field in the output containing the row number */
  @HopMetadataProperty(key="rownum_field")
  private String rowNumberField;

  /** The maximum number or lines to read */
  @HopMetadataProperty(key="limit")
  private long rowLimit;

  /** The fields to import... */
  @HopMetadataProperty(key="field", groupKey="fields")
  private List<YamlInputField> inputFields;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @HopMetadataProperty(key="encoding")
  private String encoding;

  /** Is In fields */
  @HopMetadataProperty(key="YamlField")
  private String yamlField;

  /** Is In fields */
  @HopMetadataProperty(key="IsInFields")
  private boolean inFields;

  /** Is a File */
  @HopMetadataProperty(key="IsAFile")
  private boolean sourceFile;

  /** Flag: add result filename */
  @HopMetadataProperty(key="addresultfile")
  private boolean addingResultFile;

  /** Flag: set XML Validating */
  @HopMetadataProperty(key="validating")
  private boolean validating;

  /** Flag : do we ignore empty files */
  @HopMetadataProperty(key="IsIgnoreEmptyFile")
  private boolean ignoringEmptyFile;

    /** Flag : do not fail if no file */
  @HopMetadataProperty(key="doNotFailIfNoFile")
  private boolean doNotFailIfNoFile;

  public YamlInputMeta() {
    super();
    this.yamlFiles = new ArrayList<>();
    this.inputFields = new ArrayList<>();

    doNotFailIfNoFile = true;
    filenameField = "";
    rowNumberField = "";
    rowLimit = 0;
    yamlField = "";
  }

  public YamlInputMeta(YamlInputMeta m) {
    this();
    this.includeFilename = m.includeFilename;
    this.filenameField = m.filenameField;
    this.includeRowNumber = m.includeRowNumber;
    this.rowNumberField = m.rowNumberField;
    this.rowLimit = m.rowLimit;
    this.encoding = m.encoding;
    this.yamlField = m.yamlField;
    this.inFields = m.inFields;
    this.sourceFile = m.sourceFile;
    this.addingResultFile = m.addingResultFile;
    this.validating = m.validating;
    this.ignoringEmptyFile = m.ignoringEmptyFile;
    this.doNotFailIfNoFile = m.doNotFailIfNoFile;
    m.yamlFiles.forEach(y->this.yamlFiles.add(new YamlFile(y)));
    m.inputFields.forEach(f->this.inputFields.add(new YamlInputField(f)));

  }

  @Override
  public YamlInputMeta clone() {
    return new YamlInputMeta(this);
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
      YamlInputField field = inputFields.get(i);

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

/*
  public FileInputList getFiles(IVariables variables) {
    FileInputList list = new FileInputList();
    for (YamlFile file : yamlFiles) {}
  }
*/

  //
  //    return FileInputList.createFileList(
  //        variables, , includeSubFolderBoolean());
  //  }
  /*
  private boolean[] includeSubFolderBoolean() {
    int len = fileName.length;
    boolean[] includeSubFolderBoolean = new boolean[len];
    for (int i = 0; i < len; i++) {
      includeSubFolderBoolean[i] = YES.equalsIgnoreCase(includeSubFolders[i]);
    }
    return includeSubFolderBoolean;
  }*/

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
              BaseMessages.getString(PKG, "YamlInputMeta.CheckResult.NoInputExpected"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "YamlInputMeta.CheckResult.NoInput"),
              transformMeta);
      remarks.add(cr);
    }

    if (getInputFields().size() <= 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "YamlInputMeta.CheckResult.NoInputField"),
              transformMeta);
      remarks.add(cr);
    }

    if (isInFields()) {
      if (Utils.isEmpty(getYamlField())) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "YamlInputMeta.CheckResult.NoField"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "YamlInputMeta.CheckResult.FieldOk"),
                transformMeta);
        remarks.add(cr);
      }
    } else {
      FileInputList fileInputList = getFiles(variables);
      if (fileInputList == null || fileInputList.getFiles().isEmpty()) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "YamlInputMeta.CheckResult.NoFiles"),
                transformMeta);
        remarks.add(cr);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(
                    PKG, "YamlInputMeta.CheckResult.FilesOk", "" + fileInputList.getFiles().size()),
                transformMeta);
        remarks.add(cr);
      }
    }
  }

  private FileInputList getFiles(IVariables variables) {
    // TODO
    return null;
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
   * @param definitions The definitions to use.
   * @param iResourceNaming The resource naming method.
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
        if (!fileList.getFiles().isEmpty()) {
          for (FileObject fileObject : fileList.getFiles()) {
            // If the file doesn't exist, forget about this effort too!
            //
            if (fileObject.exists()) {
              // Convert to an absolute path and add it to the list.
              //
              newFilenames.add(fileObject.getName().getPath());
            }
          }
/*
          // Still here: set a new list of absolute filenames!
          //
          fileName = newFilenames.toArray(new String[newFilenames.size()]);
          fileMask = new String[newFilenames.size()]; // all null since converted to absolute path.
          fileRequired = new String[newFilenames.size()]; // all null, turn to "Y" :
          for (int i = 0; i < newFilenames.size(); i++) {
            fileRequired[i] = "Y";
          }*/
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }
}
