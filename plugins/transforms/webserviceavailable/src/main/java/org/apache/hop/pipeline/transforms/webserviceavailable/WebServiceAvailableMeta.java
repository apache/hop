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

package org.apache.hop.pipeline.transforms.webserviceavailable;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
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
    id = "WebServiceAvailable",
    image = "webserviceavailable.svg",
    name = "i18n::BaseTransform.TypeLongDesc.WebServiceAvailable",
    description = "i18n::BaseTransform.TypeTooltipDesc.WebServiceAvailable",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Transform",
    documentationUrl =
        "https://hop.apache.org/manual/latest/plugins/transforms/webserviceavailable.html")
public class WebServiceAvailableMeta extends BaseTransformMeta
    implements ITransformMeta<WebServiceAvailable, WebServiceAvailableData> {
  private static final Class<?> PKG = WebServiceAvailableMeta.class; // For Translator

  /** dynamic filename */
  private String urlField;

  /** function result: new value name */
  private String resultfieldname;

  private String connectTimeOut;

  private String readTimeOut;

  public WebServiceAvailableMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the urlField. */
  public String getURLField() {
    return urlField;
  }

  /** @param urlField The urlField to set. */
  public void setURLField(String urlField) {
    this.urlField = urlField;
  }

  public void setConnectTimeOut(String timeout) {
    this.connectTimeOut = timeout;
  }

  public String getConnectTimeOut() {
    return connectTimeOut;
  }

  public void setReadTimeOut(String timeout) {
    this.readTimeOut = timeout;
  }

  public String getReadTimeOut() {
    return readTimeOut;
  }

  /** @return Returns the resultName. */
  public String getResultFieldName() {
    return resultfieldname;
  }

  /** @param resultfieldname The resultfieldname to set. */
  public void setResultFieldName(String resultfieldname) {
    this.resultfieldname = resultfieldname;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public Object clone() {
    WebServiceAvailableMeta retval = (WebServiceAvailableMeta) super.clone();

    return retval;
  }

  public void setDefault() {
    resultfieldname = "result";
    connectTimeOut = "0";
    readTimeOut = "0";
  }

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

  public String getXml() {
    StringBuilder retval = new StringBuilder();

    retval.append("    " + XmlHandler.addTagValue("urlField", urlField));
    retval.append("    " + XmlHandler.addTagValue("readTimeOut", readTimeOut));
    retval.append("    " + XmlHandler.addTagValue("connectTimeOut", connectTimeOut));
    retval.append("    " + XmlHandler.addTagValue("resultfieldname", resultfieldname));
    return retval.toString();
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      urlField = XmlHandler.getTagValue(transformNode, "urlField");
      connectTimeOut = XmlHandler.getTagValue(transformNode, "connectTimeOut");
      readTimeOut = XmlHandler.getTagValue(transformNode, "readTimeOut");
      resultfieldname = XmlHandler.getTagValue(transformNode, "resultfieldname");
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(
              PKG, "WebServiceAvailableMeta.Exception.UnableToReadTransformMeta"),
          e);
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

    if (Utils.isEmpty(resultfieldname)) {
      errorMessage =
          BaseMessages.getString(PKG, "WebServiceAvailableMeta.CheckResult.ResultFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage =
          BaseMessages.getString(PKG, "WebServiceAvailableMeta.CheckResult.ResultFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    if (Utils.isEmpty(urlField)) {
      errorMessage =
          BaseMessages.getString(PKG, "WebServiceAvailableMeta.CheckResult.URLFieldMissing");
      cr = new CheckResult(CheckResult.TYPE_RESULT_ERROR, errorMessage, transformMeta);
      remarks.add(cr);
    } else {
      errorMessage = BaseMessages.getString(PKG, "WebServiceAvailableMeta.CheckResult.URLFieldOK");
      cr = new CheckResult(CheckResult.TYPE_RESULT_OK, errorMessage, transformMeta);
      remarks.add(cr);
    }
    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(
                  PKG, "WebServiceAvailableMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "WebServiceAvailableMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
  }

  public WebServiceAvailable createTransform(
      TransformMeta transformMeta,
      WebServiceAvailableData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new WebServiceAvailable(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public WebServiceAvailableData getTransformData() {
    return new WebServiceAvailableData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
