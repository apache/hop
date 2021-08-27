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

package org.apache.hop.pipeline.transforms.changefileencoding;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
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

@Transform(
    id = "ChangeFileEncoding",
    image = "changefileencoding.svg",
    name = "i18n::ChangeFileEncoding.Name",
    description = "i18n::ChangeFileEncoding.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    documentationUrl =
        "https://hop.apache.org/manual/latest/pipeline/transforms/changefileencoding.html")
public class ChangeFileEncodingMeta extends BaseTransformMeta
    implements ITransformMeta<ChangeFileEncoding, ChangeFileEncodingData> {

  private static final Class<?> PKG = ChangeFileEncoding.class; // For Translator

  @HopMetadataProperty(
      key = "addsourceresultfilenames",
      injectionKeyDescription = "ChangeFileEncoding.Injection.AddSourceResultFilenames")
  private boolean addSourceResultFilenames;

  @HopMetadataProperty(
      key = "addtargetresultfilenames",
      injectionKeyDescription = "ChangeFileEncoding.Injection.AddTargetResultFilenames")
  private boolean addTargetResultFilenames;

  /** dynamic filename */
  @HopMetadataProperty(
      key = "filenamefield",
      injectionKeyDescription = "ChangeFileEncoding.Injection.FilenameField")
  private String filenameField;

  @HopMetadataProperty(
      key = "targetfilenamefield",
      injectionKeyDescription = "ChangeFileEncoding.Injection.TargetFilenameField")
  private String targetFilenameField;

  @HopMetadataProperty(
      key = "targetencoding",
      injectionKeyDescription = "ChangeFileEncoding.Injection.TargetEncoding")
  private String targetEncoding;

  @HopMetadataProperty(
      key = "sourceencoding",
      injectionKeyDescription = "ChangeFileEncoding.Injection.SourceEncoding")
  private String sourceEncoding;

  @HopMetadataProperty(
      key = "createparentfolder",
      injectionKeyDescription = "ChangeFileEncoding.Injection.CreateParentFolder")
  private boolean createParentFolder;

  public ChangeFileEncodingMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the filenamefield. */
  public String getFilenameField() {
    return filenameField;
  }

  /** @param filenamefield The filenamefield to set. */
  public void setFilenameField(String filenamefield) {
    this.filenameField = filenamefield;
  }

  /** @return Returns the targetfilenamefield. */
  public String getTargetFilenameField() {
    return targetFilenameField;
  }

  /** @param targetfilenamefield The targetfilenamefield to set. */
  public void setTargetFilenameField(String targetfilenamefield) {
    this.targetFilenameField = targetfilenamefield;
  }

  /** @return Returns the sourceencoding. */
  public String getSourceEncoding() {
    return sourceEncoding;
  }

  /** @param encoding The sourceencoding to set. */
  public void setSourceEncoding(String encoding) {
    this.sourceEncoding = encoding;
  }

  /** @return Returns the targetencoding. */
  public String getTargetEncoding() {
    return targetEncoding;
  }

  /** @param encoding The targetencoding to set. */
  public void setTargetEncoding(String encoding) {
    this.targetEncoding = encoding;
  }

  public boolean isAddSourceResultFilenames() {
    return addSourceResultFilenames;
  }

  public void setAddSourceResultFilenames(boolean addresultfilenames) {
    this.addSourceResultFilenames = addresultfilenames;
  }

  public boolean isAddTargetResultFilenames() {
    return addTargetResultFilenames;
  }

  public void setAddTargetResultFilenames(boolean addresultfilenames) {
    this.addTargetResultFilenames = addresultfilenames;
  }

  public boolean isCreateParentFolder() {
    return createParentFolder;
  }

  public void setCreateParentFolder(boolean createparentfolder) {
    this.createParentFolder = createparentfolder;
  }

  @Override
  public Object clone() {
    ChangeFileEncodingMeta retval = (ChangeFileEncodingMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    addSourceResultFilenames = false;
    addTargetResultFilenames = false;
    targetFilenameField = null;
    sourceEncoding = System.getProperty("file.encoding");
    targetEncoding = null;
    createParentFolder = false;
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

    if (Utils.isEmpty(filenameField)) {
      errorMessage =
          BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.FileFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.FileFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(targetFilenameField)) {
      errorMessage =
          BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.TargetFileFieldMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.TargetFileFieldOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    String realSourceEncoding = variables.resolve(getSourceEncoding());
    if (Utils.isEmpty(realSourceEncoding)) {
      errorMessage =
          BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.SourceEncodingMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.SourceEncodingOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    String realTargetEncoding = variables.resolve(getTargetEncoding());
    if (Utils.isEmpty(realTargetEncoding)) {
      errorMessage =
          BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.TargetEncodingMissing");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.TargetEncodingOK");
      cr = new CheckResult(ICheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ChangeFileEncodingMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  @Override
  public ChangeFileEncoding createTransform(
      TransformMeta transformMeta,
      ChangeFileEncodingData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ChangeFileEncoding(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  @Override
  public ChangeFileEncodingData getTransformData() {
    return new ChangeFileEncodingData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }
}
