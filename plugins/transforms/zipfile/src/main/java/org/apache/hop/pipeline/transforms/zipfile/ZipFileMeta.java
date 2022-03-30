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
    id = "ZipFile",
    image = "zipfile.svg",
    name = "i18n::ZipFile.Name",
    description = "i18n::ZipFile.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    keywords = "i18n::ZipFileMeta.keyword",
    documentationUrl = "/pipeline/transforms/zipfile.html")
public class ZipFileMeta extends BaseTransformMeta<ZipFile, ZipFileData> {
  private static final Class<?> PKG = ZipFileMeta.class; // For Translator

  /** dynamic filename */
  @HopMetadataProperty(key = "sourcefilenamefield", injectionKeyDescription = "")
  private String sourceFilenameField;

  @HopMetadataProperty(key = "targetfilenamefield", injectionKeyDescription = "")
  private String targetFilenameField;

  @HopMetadataProperty(key = "baseFolderField", injectionKeyDescription = "")
  private String baseFolderField;

  @HopMetadataProperty(key = "movetofolderfield", injectionKeyDescription = "")
  private String moveToFolderField;

  @HopMetadataProperty(key = "addresultfilenames", injectionKeyDescription = "")
  private boolean addResultFilenames;

  @HopMetadataProperty(key = "overwritezipentry", injectionKeyDescription = "")
  private boolean overwriteZipEntry;

  @HopMetadataProperty(key = "createparentfolder", injectionKeyDescription = "")
  private boolean createParentFolder;

  @HopMetadataProperty(key = "keepsourcefolder", injectionKeyDescription = "")
  private boolean keepSourceFolder;

  @HopMetadataProperty(key = "operation_type", injectionKeyDescription = "")
  private String operationTypeMeta;

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

  /** @return Returns the sourcefilenamefield. */
  public String getSourceFilenameField() {
    return sourceFilenameField;
  }

  /** @param sourceFilenameField The sourcefilenamefield to set. */
  public void setSourceFilenameField(String sourceFilenameField) {
    this.sourceFilenameField = sourceFilenameField;
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
    return moveToFolderField;
  }

  /** @param movetofolderfield The movetofolderfield to set. */
  public void setMoveToFolderField(String movetofolderfield) {
    this.moveToFolderField = movetofolderfield;
  }

  /** @return Returns the targetfilenamefield. */
  public String getTargetFilenameField() {
    return targetFilenameField;
  }

  /** @param targetFilenameField The targetfilenamefield to set. */
  public void setTargetFilenameField(String targetFilenameField) {
    this.targetFilenameField = targetFilenameField;
  }

  /** @return Returns if the fieldnames are added to the result */
  public boolean isAddResultFilenames() {
    return addResultFilenames;
  }

  /** @return Return if destination file may be overwritten */
  public boolean isOverwriteZipEntry() {
    return overwriteZipEntry;
  }

  /** @return Return if parent folder should be created */
  public boolean isCreateParentFolder() {
    return createParentFolder;
  }

  /** @return Return if sourcefolder has to be kept */
  public boolean isKeepSourceFolder() {
    return keepSourceFolder;
  }

  /** @param value Set to true if sourcefolder has to be kept */
  public void setKeepSourceFolder(boolean value) {
    keepSourceFolder = value;
  }

  /** @param addResultFilenames Add the result filenames to the result */
  public void setAddResultFilenames(boolean addResultFilenames) {
    this.addResultFilenames = addResultFilenames;
  }

  /** @param overwriteZipEntry Overwrite the destination zipfile if exists */
  public void setOverwriteZipEntry(boolean overwriteZipEntry) {
    this.overwriteZipEntry = overwriteZipEntry;
  }

  /** @param createParentFolder Create parent folder if it does nto exist */
  public void setCreateParentFolder(boolean createParentFolder) {
    this.createParentFolder = createParentFolder;
  }

  /** @return The opration type saved in the serialization */
  public String getOperationTypeMeta() {
    return getOperationTypeCode(operationType);
  }

  /** @param operationTypeMeta OperationCode used by the serialization */
  public void setOperationTypeMeta(String operationTypeMeta) {
    this.operationTypeMeta = operationTypeMeta;
    this.operationType = getOperationTypeByCode(operationTypeMeta);
  }

  /** @return The Integer value of the operation */
  public int getOperationType() {
    return operationType;
  }

  /**
   * @param operationType Integer value of the operation see {@link #getOperationTypeByCode(String)}
   */
  public void setOperationType(int operationType) {
    this.operationType = operationType;
  }

  @Override
  public Object clone() {
    ZipFileMeta retval = (ZipFileMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    addResultFilenames = false;
    overwriteZipEntry = false;
    createParentFolder = false;
    keepSourceFolder = false;
    operationType = OPERATION_TYPE_NOTHING;
  }

  private static String getOperationTypeCode(int i) {
    if (i < 0 || i >= operationTypeCode.length) {
      return operationTypeCode[0];
    }
    return operationTypeCode[i];
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
      errorMessage = BaseMessages.getString(PKG, "ZipFileMeta.CheckResult.SourceFileFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "ZipFileMeta.CheckResult.TargetFileFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ZipFileMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ZipFileMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
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
