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

package org.apache.hop.pipeline.transforms.filelocked;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;

/** Check if a file is locked * */
@Transform(
    id = "FileLocked",
    image = "filelocked.svg",
    name = "i18n::BaseTransform.TypeLongDesc.FileLocked",
    description = "i18n::BaseTransform.TypeTooltipDesc.FileLocked",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    documentationUrl =
        "https://hop.apache.org/manual/latest/pipeline/transforms/checkfilelocked.html")
public class FileLockedMeta extends BaseTransformMeta
    implements ITransformMeta<FileLocked, FileLockedData> {
  private static final Class<?> PKG = FileLockedMeta.class; // For Translator

  @HopMetadataProperty(
      key = "addresultfilenames",
      injectionKeyDescription = "FileLockedDialog.AddResult.Label")
  private boolean addresultfilenames;

  /** dynamic filename */
  @HopMetadataProperty(
      key = "filenamefield",
      injectionKeyDescription = "FileLockedDialog.FileName.Label")
  private String filenamefield;

  /** function result: new value name */
  @HopMetadataProperty(
      key = "resultfieldname",
      injectionKeyDescription = "FileLockedDialog.ResultField.Label")
  private String resultfieldname;

  public FileLockedMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the filenamefield. */
  public String getFilenamefield() {
    return filenamefield;
  }

  /** @param filenamefield The filenamefield to set. */
  public void setFilenamefield(String filenamefield) {
    this.filenamefield = filenamefield;
  }

  /** @return Returns the resultName. */
  public String getResultfieldname() {
    return resultfieldname;
  }

  /** @param resultfieldname The resultfieldname to set. */
  public void setResultfieldname(String resultfieldname) {
    this.resultfieldname = resultfieldname;
  }

  public boolean isAddresultfilenames() {
    return addresultfilenames;
  }

  public void setAddresultfilenames(boolean addresultfilenames) {
    this.addresultfilenames = addresultfilenames;
  }

  @Override
  public Object clone() {
    FileLockedMeta retval = (FileLockedMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    resultfieldname = "result";
    addresultfilenames = false;
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (!Utils.isEmpty(resultfieldname)) {
      IValueMeta v = new ValueMetaBoolean(resultfieldname);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
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

    if (Utils.isEmpty(resultfieldname)) {
      errorMessage = BaseMessages.getString(PKG, "FileLockedMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "FileLockedMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(filenamefield)) {
      errorMessage = BaseMessages.getString(PKG, "FileLockedMeta.CheckResult.FileFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "FileLockedMeta.CheckResult.FileFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "FileLockedMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "FileLockedMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public FileLocked createTransform(
      TransformMeta transformMeta,
      FileLockedData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new FileLocked(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public FileLockedData getTransformData() {
    return new FileLockedData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
