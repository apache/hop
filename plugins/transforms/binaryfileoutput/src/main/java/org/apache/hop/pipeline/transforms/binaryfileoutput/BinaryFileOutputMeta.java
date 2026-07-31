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

package org.apache.hop.pipeline.transforms.binaryfileoutput;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
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

@Transform(
    id = "BinaryFileOutput",
    image = "binaryfileoutput.svg",
    name = "i18n::BinaryFileOutput.Name",
    description = "i18n::BinaryFileOutput.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output",
    keywords = "i18n::BinaryFileOutputMeta.keyword",
    documentationUrl = "/pipeline/transforms/binaryfileoutput.html")
@Getter
@Setter
public class BinaryFileOutputMeta
    extends BaseTransformMeta<BinaryFileOutput, BinaryFileOutputData> {
  private static final Class<?> PKG = BinaryFileOutputMeta.class;

  /** Field containing the binary content to write */
  @HopMetadataProperty(
      key = "binaryfield",
      injectionKey = "BINARY_FIELD",
      injectionKeyDescription = "BinaryFileOutput.Injection.BinaryField")
  private String binaryField;

  /** Field containing the target filename */
  @HopMetadataProperty(
      key = "filenamefield",
      injectionKey = "FILENAME_FIELD",
      injectionKeyDescription = "BinaryFileOutput.Injection.FilenameField")
  private String filenameField;

  /** Create parent folder if it does not exist */
  @HopMetadataProperty(
      key = "createparentfolder",
      injectionKey = "CREATE_PARENT_FOLDER",
      injectionKeyDescription = "BinaryFileOutput.Injection.CreateParentFolder")
  private boolean createParentFolder;

  /** Overwrite the target file when it already exists */
  @HopMetadataProperty(
      key = "overwritefile",
      injectionKey = "OVERWRITE_FILE",
      injectionKeyDescription = "BinaryFileOutput.Injection.OverwriteFile")
  private boolean overwriteFile;

  /** Add written filenames to the result filenames */
  @HopMetadataProperty(
      key = "addresultfilenames",
      injectionKey = "ADD_RESULT_FILENAMES",
      injectionKeyDescription = "BinaryFileOutput.Injection.AddResultFilenames")
  private boolean addResultFilenames;

  public BinaryFileOutputMeta() {
    super();
  }

  @Override
  public Object clone() {
    return super.clone();
  }

  @Override
  public void setDefault() {
    binaryField = null;
    filenameField = null;
    createParentFolder = true;
    overwriteFile = true;
    addResultFilenames = false;
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

    if (Utils.isEmpty(binaryField)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "BinaryFileOutputMeta.CheckResult.BinaryFieldMissing"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "BinaryFileOutputMeta.CheckResult.BinaryFieldOK"),
              transformMeta);
      remarks.add(cr);
    }

    if (Utils.isEmpty(filenameField)) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "BinaryFileOutputMeta.CheckResult.FilenameFieldMissing"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "BinaryFileOutputMeta.CheckResult.FilenameFieldOK"),
              transformMeta);
      remarks.add(cr);
    }

    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "BinaryFileOutputMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "BinaryFileOutputMeta.CheckResult.NoInputReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
