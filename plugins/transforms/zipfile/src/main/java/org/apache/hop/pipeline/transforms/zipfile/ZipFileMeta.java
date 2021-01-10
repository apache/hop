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

package org.apache.hop.pipeline.transforms.zipfile;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "ZipFile",
    image = "zipfile.svg",
    name = "i18n::ZipFile.Name",
    description = "i18n::ZipFile.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/zipfile.html")
public class ZipFileMeta extends BaseTransformMeta implements ITransformMeta<ZipFile, ZipFileData> {
  private static final Class<?> PKG = ZipFileMeta.class; // For Translator

  /** dynamic filename */
  private String sourcefilenamefield;

  private String targetfilenamefield;
  private String baseFolderField;

  private String movetofolderfield;

  private boolean addresultfilenames;
  private boolean overwritezipentry;
  private boolean createparentfolder;

  private boolean keepsourcefolder;

  /** Operations type */
  private int operationType;

  /** The operations description */
  public static final String[] operationTypeDesc = {
    BaseMessages.getString(PKG, "ZipFileMeta.operationType.DoNothing"),
    BaseMessages.getString(PKG, "ZipFileMeta.operationType.Move"),
    BaseMessages.getString(PKG, "ZipFileMeta.operationType.Delete")
  };

  /** The operations type codes */
  public static final String[] operationTypeCode = {"", "move", "delete"};

  public static final int OPERATION_TYPE_NOTHING = 0;

  public static final int OPERATION_TYPE_MOVE = 1;

  public static final int OPERATION_TYPE_DELETE = 2;

  public ZipFileMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public ITransform createTransform(
      TransformMeta transformMeta,
      ZipFileData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ZipFile(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  /** @return Returns the sourcefilenamefield. */
  public String getDynamicSourceFileNameField() {
    return sourcefilenamefield;
  }

  /** @param sourcefilenamefield The sourcefilenamefield to set. */
  public void setDynamicSourceFileNameField(String sourcefilenamefield) {
    this.sourcefilenamefield = sourcefilenamefield;
  }

  /** @return Returns the baseFolderField. */
  public String getBaseFolderField() {
    return baseFolderField;
  }

  /** @param baseFolderField The baseFolderField to set. */
  public void setBaseFolderField(String baseFolderField) {
    this.baseFolderField = baseFolderField;
  }

  /** @return Returns the movetofolderfield. */
  public String getMoveToFolderField() {
    return movetofolderfield;
  }

  /** @param movetofolderfield The movetofolderfield to set. */
  public void setMoveToFolderField(String movetofolderfield) {
    this.movetofolderfield = movetofolderfield;
  }

  /** @return Returns the targetfilenamefield. */
  public String getDynamicTargetFileNameField() {
    return targetfilenamefield;
  }

  /** @param targetfilenamefield The targetfilenamefield to set. */
  public void setDynamicTargetFileNameField(String targetfilenamefield) {
    this.targetfilenamefield = targetfilenamefield;
  }

  public boolean isaddTargetFileNametoResult() {
    return addresultfilenames;
  }

  public boolean isOverwriteZipEntry() {
    return overwritezipentry;
  }

  public boolean isCreateParentFolder() {
    return createparentfolder;
  }

  public boolean isKeepSouceFolder() {
    return keepsourcefolder;
  }

  public void setKeepSouceFolder(boolean value) {
    keepsourcefolder = value;
  }

  public void setaddTargetFileNametoResult(boolean addresultfilenames) {
    this.addresultfilenames = addresultfilenames;
  }

  public void setOverwriteZipEntry(boolean overwritezipentry) {
    this.overwritezipentry = overwritezipentry;
  }

  public void setCreateParentFolder(boolean createparentfolder) {
    this.createparentfolder = createparentfolder;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode);
  }

  public Object clone() {
    ZipFileMeta retval = (ZipFileMeta) super.clone();

    return retval;
  }

  public void setDefault() {
    addresultfilenames = false;
    overwritezipentry = false;
    createparentfolder = false;
    keepsourcefolder = false;
    operationType = OPERATION_TYPE_NOTHING;
  }

  private static String getOperationTypeCode(int i) {
    if (i < 0 || i >= operationTypeCode.length) {
      return operationTypeCode[0];
    }
    return operationTypeCode[i];
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    " + XmlHandler.addTagValue("sourcefilenamefield", sourcefilenamefield));
    retval.append("    " + XmlHandler.addTagValue("targetfilenamefield", targetfilenamefield));
    retval.append("    " + XmlHandler.addTagValue("baseFolderField", baseFolderField));
    retval.append(
        "    " + XmlHandler.addTagValue("operation_type", getOperationTypeCode(operationType)));
    retval.append("    ").append(XmlHandler.addTagValue("addresultfilenames", addresultfilenames));
    retval.append("    ").append(XmlHandler.addTagValue("overwritezipentry", overwritezipentry));
    retval.append("    ").append(XmlHandler.addTagValue("createparentfolder", createparentfolder));
    retval.append("    ").append(XmlHandler.addTagValue("keepsourcefolder", keepsourcefolder));
    retval.append("    " + XmlHandler.addTagValue("movetofolderfield", movetofolderfield));
    return retval.toString();
  }

  private void readData(Node transformNode) throws HopXmlException {
    try {
      sourcefilenamefield = XmlHandler.getTagValue(transformNode, "sourcefilenamefield");
      targetfilenamefield = XmlHandler.getTagValue(transformNode, "targetfilenamefield");
      baseFolderField = XmlHandler.getTagValue(transformNode, "baseFolderField");
      operationType =
          getOperationTypeByCode(
              Const.NVL(XmlHandler.getTagValue(transformNode, "operation_type"), ""));
      addresultfilenames =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "addresultfilenames"));
      overwritezipentry =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "overwritezipentry"));
      createparentfolder =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "createparentfolder"));
      keepsourcefolder =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "keepsourcefolder"));
      movetofolderfield = XmlHandler.getTagValue(transformNode, "movetofolderfield");

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ZipFileMeta.Exception.UnableToReadTransformMeta"), e);
    }
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
    String errorMessage = "";

    // source filename
    if (Utils.isEmpty(sourcefilenamefield)) {
      errorMessage = BaseMessages.getString(PKG, "ZipFileMeta.CheckResult.SourceFileFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "ZipFileMeta.CheckResult.TargetFileFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ZipFileMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ZipFileMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public ZipFileData getTransformData() {
    return new ZipFileData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  public int getOperationType() {
    return operationType;
  }

  public static int getOperationTypeByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < operationTypeDesc.length; i++) {
      if (operationTypeDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    // If this fails, try to match using the code.
    return getOperationTypeByCode(tt);
  }

  public void setOperationType(int operationType) {
    this.operationType = operationType;
  }

  public static String getOperationTypeDesc(int i) {
    if (i < 0 || i >= operationTypeDesc.length) {
      return operationTypeDesc[0];
    }
    return operationTypeDesc[i];
  }

  private static int getOperationTypeByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < operationTypeCode.length; i++) {
      if (operationTypeCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }
}
