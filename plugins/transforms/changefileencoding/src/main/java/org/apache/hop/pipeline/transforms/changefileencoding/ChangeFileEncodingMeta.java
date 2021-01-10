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
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "ChangeFileEncoding",
    image = "changefileencoding.svg",
    name = "i18n::ChangeFileEncoding.Name",
    description = "i18n::ChangeFileEncoding.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/changefileencoding.html")
public class ChangeFileEncodingMeta extends BaseTransformMeta
    implements ITransformMeta<ChangeFileEncoding, ChangeFileEncodingData> {

  private static final Class<?> PKG = ChangeFileEncoding.class; // For Translator

  private boolean addsourceresultfilenames;
  private boolean addtargetresultfilenames;

  /** dynamic filename */
  private String filenamefield;

  private String targetfilenamefield;
  private String targetencoding;
  private String sourceencoding;
  private boolean createparentfolder;

  public ChangeFileEncodingMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the filenamefield. */
  public String getDynamicFilenameField() {
    return filenamefield;
  }

  /** @param filenamefield The filenamefield to set. */
  public void setDynamicFilenameField(String filenamefield) {
    this.filenamefield = filenamefield;
  }

  /** @return Returns the targetfilenamefield. */
  public String getTargetFilenameField() {
    return targetfilenamefield;
  }

  /** @param targetfilenamefield The targetfilenamefield to set. */
  public void setTargetFilenameField(String targetfilenamefield) {
    this.targetfilenamefield = targetfilenamefield;
  }

  /** @return Returns the sourceencoding. */
  public String getSourceEncoding() {
    return sourceencoding;
  }

  /** @param encoding The sourceencoding to set. */
  public void setSourceEncoding(String encoding) {
    this.sourceencoding = encoding;
  }

  /** @return Returns the targetencoding. */
  public String getTargetEncoding() {
    return targetencoding;
  }

  /** @param encoding The targetencoding to set. */
  public void setTargetEncoding(String encoding) {
    this.targetencoding = encoding;
  }

  public boolean addSourceResultFilenames() {
    return addsourceresultfilenames;
  }

  public void setaddSourceResultFilenames(boolean addresultfilenames) {
    this.addsourceresultfilenames = addresultfilenames;
  }

  public boolean addTargetResultFilenames() {
    return addtargetresultfilenames;
  }

  public void setaddTargetResultFilenames(boolean addresultfilenames) {
    this.addtargetresultfilenames = addresultfilenames;
  }

  public boolean isCreateParentFolder() {
    return createparentfolder;
  }

  public void setCreateParentFolder(boolean createparentfolder) {
    this.createparentfolder = createparentfolder;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public Object clone() {
    ChangeFileEncodingMeta retval = (ChangeFileEncodingMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    addsourceresultfilenames = false;
    addtargetresultfilenames = false;
    targetfilenamefield = null;
    sourceencoding = System.getProperty("file.encoding");
    targetencoding = null;
    createparentfolder = false;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    " + XmlHandler.addTagValue("filenamefield", filenamefield));
    retval.append("    " + XmlHandler.addTagValue("targetfilenamefield", targetfilenamefield));
    retval.append("    " + XmlHandler.addTagValue("sourceencoding", sourceencoding));
    retval.append("    " + XmlHandler.addTagValue("targetencoding", targetencoding));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("addsourceresultfilenames", addsourceresultfilenames));
    retval
        .append("    ")
        .append(XmlHandler.addTagValue("addtargetresultfilenames", addtargetresultfilenames));
    retval.append("    ").append(XmlHandler.addTagValue("createparentfolder", createparentfolder));

    return retval.toString();
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      filenamefield = XmlHandler.getTagValue(transformNode, "filenamefield");
      targetfilenamefield = XmlHandler.getTagValue(transformNode, "targetfilenamefield");
      sourceencoding = XmlHandler.getTagValue(transformNode, "sourceencoding");
      targetencoding = XmlHandler.getTagValue(transformNode, "targetencoding");
      addsourceresultfilenames =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "addsourceresultfilenames"));
      addtargetresultfilenames =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "addtargetresultfilenames"));
      createparentfolder =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "createparentfolder"));

    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ChangeFileEncodingMeta.Exception.UnableToReadTransformMeta"),
          e);
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

    if (Utils.isEmpty(filenamefield)) {
      errorMessage =
          BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.FileFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.FileFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(targetfilenamefield)) {
      errorMessage =
          BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.TargetFileFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.TargetFileFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    String realSourceEncoding = variables.resolve(getSourceEncoding());
    if (Utils.isEmpty(realSourceEncoding)) {
      errorMessage =
          BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.SourceEncodingMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.SourceEncodingOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    String realTargetEncoding = variables.resolve(getTargetEncoding());
    if (Utils.isEmpty(realTargetEncoding)) {
      errorMessage =
          BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.TargetEncodingMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.TargetEncodingOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "ChangeFileEncodingMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "ChangeFileEncodingMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public ChangeFileEncoding createTransform(
      TransformMeta transformMeta,
      ChangeFileEncodingData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new ChangeFileEncoding(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public ChangeFileEncodingData getTransformData() {
    return new ChangeFileEncodingData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
