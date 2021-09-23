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

package org.apache.hop.pipeline.transforms.processfiles;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import java.util.List;

@Transform(
    id = "ProcessFiles",
    image = "processfiles.svg",
    description = "i18n::ProcessFiles.Description",
    name = "i18n::ProcessFiles.Name",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    documentationUrl = "/pipeline/transforms/processfiles.html")
public class ProcessFilesMeta extends BaseTransformMeta<ProcessFiles, ProcessFilesData> {
  private static final Class<?> PKG = ProcessFilesMeta.class; // For Translator

  @HopMetadataProperty(
      key = "addresultfilenames",
      injectionKeyDescription = "ProcessFiles.Injection.AddResultFilenames")
  private boolean addResultFilenames;

  @HopMetadataProperty(
      key = "overwritetargetfile",
      injectionKeyDescription = "ProcessFiles.Injection.OverwriteTargetFile")
  private boolean overwriteTargetFile;

  @HopMetadataProperty(
      key = "createparentfolder",
      injectionKeyDescription = "ProcessFiles.Injection.CreateParentFolder")
  private boolean createParentFolder;

  @HopMetadataProperty(
      key = "simulate",
      injectionKeyDescription = "ProcessFiles.Injection.Simulate")
  public boolean simulate;

  @HopMetadataProperty(
      key = "sourcefilenamefield",
      injectionKeyDescription = "ProcessFiles.Injection.SourceFilenameField")
  private String sourceFilenameField;

  @HopMetadataProperty(
      key = "targetfilenamefield",
      injectionKeyDescription = "ProcessFiles.Injection.TargetFilenameField")
  private String targetFilenameField;

  /** Operations type */
  private int operationType;

  @HopMetadataProperty(
      key = "operation_type",
      injectionKeyDescription = "ProcessFiles.Injection.OperationType")
  private String operationTypeMeta;

  /** The operations description */
  public static final String[] operationTypeDesc = {
    BaseMessages.getString(PKG, "ProcessFilesMeta.operationType.Copy"),
    BaseMessages.getString(PKG, "ProcessFilesMeta.operationType.Move"),
    BaseMessages.getString(PKG, "ProcessFilesMeta.operationType.Delete")
  };

  /** The operations type codes */
  public static final String[] operationTypeCode = {"copy", "move", "delete"};

  public static final int OPERATION_TYPE_COPY = 0;

  public static final int OPERATION_TYPE_MOVE = 1;

  public static final int OPERATION_TYPE_DELETE = 2;

  public ProcessFilesMeta() {
    super(); // allocate BaseTransformMeta
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
    this.operationTypeMeta = getOperationTypeCode(operationType);
  }

  public static String getOperationTypeDesc(int i) {
    if (i < 0 || i >= operationTypeDesc.length) {
      return operationTypeDesc[0];
    }
    return operationTypeDesc[i];
  }

  /** @return Returns the sourcefilenamefield. */
  public String getSourceFilenameField() {
    return sourceFilenameField;
  }

  /** @param sourceFilenameFieldield The sourcefilenamefield to set. */
  public void setSourceFilenameField(String sourceFilenameFieldield) {
    this.sourceFilenameField = sourceFilenameFieldield;
  }

  /** @return Returns the targetfilenamefield. */
  public String getTargetFilenameField() {
    return targetFilenameField;
  }

  /** @param targetFilenameField The targetFilenameField to set. */
  public void setTargetFilenameField(String targetFilenameField) {
    this.targetFilenameField = targetFilenameField;
  }

  public boolean isOverwriteTargetFile() {
    return overwriteTargetFile;
  }

  public boolean isCreateParentFolder() {
    return createParentFolder;
  }

  public void setAddResultFilenames(boolean addresultfilenames) {
    this.addResultFilenames = addresultfilenames;
  }

  public void setOverwriteTargetFile(boolean overwritetargetfile) {
    this.overwriteTargetFile = overwritetargetfile;
  }

  public void setCreateParentFolder(boolean createparentfolder) {
    this.createParentFolder = createparentfolder;
  }

  public void setSimulate(boolean simulate) {
    this.simulate = simulate;
  }

  public boolean isSimulate() {
    return this.simulate;
  }

  public boolean isAddResultFilenames() {
    return addResultFilenames;
  }

  public String getOperationTypeMeta() {
    return operationTypeMeta;
  }

  public void setOperationTypeMeta(String operationTypeMeta) {
    this.operationTypeMeta = operationTypeMeta;
    setOperationType(getOperationTypeByCode(operationTypeMeta));
  }

  @Override
  public void setDefault() {
    addResultFilenames = false;
    overwriteTargetFile = false;
    createParentFolder = false;
    simulate = true;
    operationType = OPERATION_TYPE_COPY;
  }

  private static String getOperationTypeCode(int i) {
    if (i < 0 || i >= operationTypeCode.length) {
      return operationTypeCode[0];
    }
    return operationTypeCode[i];
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
    String errorMessage = "";

    // source filename
    if (Utils.isEmpty(sourceFilenameField)) {
      errorMessage =
          BaseMessages.getString(PKG, "ProcessFilesMeta.CheckResult.SourceFileFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "ProcessFilesMeta.CheckResult.TargetFileFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (operationType != OPERATION_TYPE_DELETE && Utils.isEmpty(targetFilenameField)) {
      errorMessage =
          BaseMessages.getString(PKG, "ProcessFilesMeta.CheckResult.TargetFileFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "ProcessFilesMeta.CheckResult.SourceFileFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ProcessFilesMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ProcessFilesMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
