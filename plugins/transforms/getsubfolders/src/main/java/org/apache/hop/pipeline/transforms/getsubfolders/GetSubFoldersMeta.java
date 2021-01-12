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
import org.w3c.dom.Node;

import java.util.List;
import java.util.Map;

/**
 * @author Samatar
 * @since 18-July-2008
 */
@Transform(
    id = "GetSubFolders",
    image = "getsubfolders.svg",
    name = "i18n::BaseTransform.TypeLongDesc.GetSubFolders",
    description = "i18n::BaseTransform.TypeTooltipDesc.GetSubFolders",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Input",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/getsubfolders.html")
public class GetSubFoldersMeta extends BaseTransformMeta
    implements ITransformMeta<GetSubFolders, GetSubFoldersData> {
  private static final Class<?> PKG = GetSubFoldersMeta.class; // For Translator

  public static final String[] RequiredFoldersDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };
  public static final String[] RequiredFoldersCode = new String[] {"N", "Y"};

  public static final String NO = "N";

  /** Array of filenames */
  private String[] folderName;

  /** Array of boolean values as string, indicating if a file is required. */
  private String[] folderRequired;

  /** Flag indicating that a row number field should be included in the output */
  private boolean includeRowNumber;

  /** The name of the field in the output containing the row number */
  private String rowNumberField;

  /** The name of the field in the output containing the foldername */
  private String dynamicFoldernameField;

  /** folder name from previous fields */
  private boolean isFoldernameDynamic;

  /** The maximum number or lines to read */
  private long rowLimit;

  public GetSubFoldersMeta() {
    super(); // allocate BaseTransformMeta
  }

  public String getRequiredFilesDesc(String tt) {
    if (Utils.isEmpty(tt)) {
      return RequiredFoldersDesc[0];
    }
    if (tt.equalsIgnoreCase(RequiredFoldersCode[1])) {
      return RequiredFoldersDesc[1];
    } else {
      return RequiredFoldersDesc[0];
    }
  }

  /** @return Returns the rowNumberField. */
  public String getRowNumberField() {
    return rowNumberField;
  }

  /** @param dynamicFoldernameField The dynamic foldername field to set. */
  public void setDynamicFoldernameField(String dynamicFoldernameField) {
    this.dynamicFoldernameField = dynamicFoldernameField;
  }

  /** @param rowNumberField The rowNumberField to set. */
  public void setRowNumberField(String rowNumberField) {
    this.rowNumberField = rowNumberField;
  }

  /** @return Returns the dynamic folder field (from previous transforms) */
  public String getDynamicFoldernameField() {
    return dynamicFoldernameField;
  }

  /** @return Returns the includeRowNumber. */
  public boolean includeRowNumber() {
    return includeRowNumber;
  }

  /** @return Returns the dynamic foldername flag. */
  public boolean isFoldernameDynamic() {
    return isFoldernameDynamic;
  }

  /** @param isFoldernameDynamic The isFoldernameDynamic to set. */
  public void setFolderField(boolean isFoldernameDynamic) {
    this.isFoldernameDynamic = isFoldernameDynamic;
  }

  /** @param includeRowNumber The includeRowNumber to set. */
  public void setIncludeRowNumber(boolean includeRowNumber) {
    this.includeRowNumber = includeRowNumber;
  }

  /** @return Returns the folderRequired. */
  public String[] getFolderRequired() {
    return folderRequired;
  }

  public String getRequiredFoldersCode(String tt) {
    if (tt == null) {
      return RequiredFoldersCode[0];
    }
    if (tt.equals(RequiredFoldersDesc[1])) {
      return RequiredFoldersCode[1];
    } else {
      return RequiredFoldersCode[0];
    }
  }

  /** @param folderRequiredin The folderRequired to set. */
  public void setFolderRequired(String[] folderRequiredin) {
    this.folderRequired = new String[folderRequiredin.length];
    for (int i = 0; i < folderRequiredin.length; i++) {
      this.folderRequired[i] = getRequiredFoldersCode(folderRequiredin[i]);
    }
  }

  /** @return Returns the folderName. */
  public String[] getFolderName() {
    return folderName;
  }

  /** @param folderName The folderName to set. */
  public void setFolderName(String[] folderName) {
    this.folderName = folderName;
  }

  /** @return Returns the rowLimit. */
  public long getRowLimit() {
    return rowLimit;
  }

  /** @param rowLimit The rowLimit to set. */
  public void setRowLimit(long rowLimit) {
    this.rowLimit = rowLimit;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public Object clone() {
    GetSubFoldersMeta retval = (GetSubFoldersMeta) super.clone();

    int nrfiles = folderName.length;

    retval.allocate(nrfiles);

    System.arraycopy(folderName, 0, retval.folderName, 0, nrfiles);
    System.arraycopy(folderRequired, 0, retval.folderRequired, 0, nrfiles);

    return retval;
  }

  public void allocate(int nrfiles) {
    folderName = new String[nrfiles];
    folderRequired = new String[nrfiles];
  }

  public void setDefault() {
    int nrfiles = 0;
    isFoldernameDynamic = false;
    includeRowNumber = false;
    rowNumberField = "";
    dynamicFoldernameField = "";

    allocate(nrfiles);

    for (int i = 0; i < nrfiles; i++) {
      folderName[i] = "folderName" + (i + 1);
      folderRequired[i] = NO;
    }
  }

  public void getFields(
      IRowMeta row,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {

    // the folderName
    IValueMeta folderName = new ValueMetaString("folderName");
    folderName.setLength(500);
    folderName.setPrecision(-1);
    folderName.setOrigin(name);
    row.addValueMeta(folderName);

    // the short folderName
    IValueMeta shortFolderName = new ValueMetaString("short_folderName");
    shortFolderName.setLength(500);
    shortFolderName.setPrecision(-1);
    shortFolderName.setOrigin(name);
    row.addValueMeta(shortFolderName);

    // the path
    IValueMeta path = new ValueMetaString("path");
    path.setLength(500);
    path.setPrecision(-1);
    path.setOrigin(name);
    row.addValueMeta(path);

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

    // the uri
    IValueMeta uri = new ValueMetaString("uri");
    uri.setOrigin(name);
    row.addValueMeta(uri);

    // the rooturi
    IValueMeta rooturi = new ValueMetaString("rooturi");
    rooturi.setOrigin(name);
    row.addValueMeta(rooturi);

    // childrens
    IValueMeta childrens = new ValueMetaInteger(variables.resolve("childrens"));
    childrens.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
    childrens.setOrigin(name);
    row.addValueMeta(childrens);

    if (includeRowNumber) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(rowNumberField));
      v.setLength(IValueMeta.DEFAULT_INTEGER_LENGTH, 0);
      v.setOrigin(name);
      row.addValueMeta(v);
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append("    ").append(XmlHandler.addTagValue("rownum", includeRowNumber));
    retval.append("    ").append(XmlHandler.addTagValue("foldername_dynamic", isFoldernameDynamic));
    retval.append("    ").append(XmlHandler.addTagValue("rownum_field", rowNumberField));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("foldername_field", dynamicFoldernameField));
    retval.append("    ").append(XmlHandler.addTagValue("limit", rowLimit));
    retval.append("    <file>").append(Const.CR);

    for (int i = 0; i < folderName.length; i++) {
      retval.append("      ").append(XmlHandler.addTagValue("name", folderName[i]));
      retval.append("      ").append(XmlHandler.addTagValue("file_required", folderRequired[i]));
    }
    retval.append("    </file>").append(Const.CR);

    return retval.toString();
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      includeRowNumber = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "rownum"));
      isFoldernameDynamic =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "foldername_dynamic"));
      rowNumberField = XmlHandler.getTagValue(transformNode, "rownum_field");
      dynamicFoldernameField = XmlHandler.getTagValue(transformNode, "foldername_field");

      // Is there a limit on the number of rows we process?
      rowLimit = Const.toLong(XmlHandler.getTagValue(transformNode, "limit"), 0L);

      Node filenode = XmlHandler.getSubNode(transformNode, "file");
      int nrfiles = XmlHandler.countNodes(filenode, "name");

      allocate(nrfiles);

      for (int i = 0; i < nrfiles; i++) {
        Node folderNamenode = XmlHandler.getSubNodeByNr(filenode, "name", i);
        Node folderRequirednode = XmlHandler.getSubNodeByNr(filenode, "file_required", i);
        folderName[i] = XmlHandler.getNodeValue(folderNamenode);
        folderRequired[i] = XmlHandler.getNodeValue(folderRequirednode);
      }
    } catch (Exception e) {
      throw new HopXmlException("Unable to load transform info from XML", e);
    }
  }

  public FileInputList getFolderList(IVariables variables) {
    return FileInputList.createFolderList(variables, folderName, folderRequired);
  }

  public FileInputList getDynamicFolderList(
      IVariables variables, String[] folderName, String[] folderRequired) {
    return FileInputList.createFolderList(variables, folderName, folderRequired);
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
    if (isFoldernameDynamic) {
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

      if (Utils.isEmpty(dynamicFoldernameField)) {
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

  public GetSubFolders createTransform(
      TransformMeta transformMeta,
      GetSubFoldersData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new GetSubFolders(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public GetSubFoldersData getTransformData() {
    return new GetSubFoldersData();
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
      if (!isFoldernameDynamic) {
        for (int i = 0; i < folderName.length; i++) {
          FileObject fileObject = HopVfs.getFileObject(variables.resolve(folderName[i]));
          folderName[i] = iResourceNaming.nameResource(fileObject, variables, true);
        }
      }
      return null;
    } catch (Exception e) {
      throw new HopException(e);
    }
  }
}
