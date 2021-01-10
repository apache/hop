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

package org.apache.hop.pipeline.transforms.http;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
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

/*
 * Created on 10-dec-2006
 *
 */
@Transform(
    id = "Http",
    image = "http.svg",
    name = "i18n::BaseTransform.TypeLongDesc.HTTP",
    description = "i18n::BaseTransform.TypeTooltipDesc.HTTP",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/getvariable.html")
public class HttpMeta extends BaseTransformMeta implements ITransformMeta<Http, HttpData> {
  private static final Class<?> PKG = HttpMeta.class; // For Translator

  // the timeout for waiting for data (milliseconds)
  public static final int DEFAULT_SOCKET_TIMEOUT = 10000;

  // the timeout until a connection is established (milliseconds)
  public static final int DEFAULT_CONNECTION_TIMEOUT = 10000;

  // the time to wait till a connection is closed (milliseconds)? -1 is no not close.
  public static final int DEFAULT_CLOSE_CONNECTIONS_TIME = -1;

  private String socketTimeout;
  private String connectionTimeout;
  private String closeIdleConnectionsTime;

  /** URL / service to be called */
  private String url;

  /** function arguments : fieldname */
  private String[] argumentField;

  /** IN / OUT / INOUT */
  private String[] argumentParameter;

  /** function result: new value name */
  private String fieldName;

  /** The encoding to use for retrieval of the data */
  private String encoding;

  private boolean urlInField;

  private String urlField;

  private String proxyHost;

  private String proxyPort;

  private String httpLogin;

  private String httpPassword;

  private String resultCodeFieldName;
  private String responseTimeFieldName;
  private String responseHeaderFieldName;

  private String[] headerParameter;
  private String[] headerField;

  public HttpMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the connectionTimeout. */
  public String getConnectionTimeout() {
    return connectionTimeout;
  }

  /** @param connectionTimeout The connectionTimeout to set. */
  public void setConnectionTimeout(String connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
  }

  /** @return Returns the closeIdleConnectionsTime. */
  public String getCloseIdleConnectionsTime() {
    return closeIdleConnectionsTime;
  }

  /** @param closeIdleConnectionsTime The connectionTimeout to set. */
  public void setCloseIdleConnectionsTime(String closeIdleConnectionsTime) {
    this.closeIdleConnectionsTime = closeIdleConnectionsTime;
  }

  /** @return Returns the socketTimeout. */
  public String getSocketTimeout() {
    return socketTimeout;
  }

  /** @param socketTimeout The socketTimeout to set. */
  public void setSocketTimeout(String socketTimeout) {
    this.socketTimeout = socketTimeout;
  }

  /** @return Returns the argument. */
  public String[] getArgumentField() {
    return argumentField;
  }

  /** @param argument The argument to set. */
  public void setArgumentField(String[] argument) {
    this.argumentField = argument;
  }

  /** @return Returns the headerFields. */
  public String[] getHeaderField() {

    return headerField;
  }

  /** @param headerField The headerField to set. */
  public void setHeaderField(String[] headerField) {

    this.headerField = headerField;
  }

  /** @return Returns the argumentDirection. */
  public String[] getArgumentParameter() {
    return argumentParameter;
  }

  /** @param argumentDirection The argumentDirection to set. */
  public void setArgumentParameter(String[] argumentDirection) {
    this.argumentParameter = argumentDirection;
  }

  /** @return Returns the headerParameter. */
  public String[] getHeaderParameter() {
    return headerParameter;
  }

  /** @param headerParameter The headerParameter to set. */
  public void setHeaderParameter(String[] headerParameter) {
    this.headerParameter = headerParameter;
  }

  /** @return Returns the procedure. */
  public String getUrl() {
    return url;
  }

  /** @param procedure The procedure to set. */
  public void setUrl(String procedure) {
    this.url = procedure;
  }

  /** @return Returns the resultName. */
  public String getFieldName() {
    return fieldName;
  }

  /** @param resultName The resultName to set. */
  public void setFieldName(String resultName) {
    this.fieldName = resultName;
  }

  /** @return Is the url coded in a field? */
  public boolean isUrlInField() {
    return urlInField;
  }

  /** @param urlInField Is the url coded in a field? */
  public void setUrlInField(boolean urlInField) {
    this.urlInField = urlInField;
  }

  /** @return The field name that contains the url. */
  public String getUrlField() {
    return urlField;
  }

  /** @param urlField name of the field that contains the url */
  public void setUrlField(String urlField) {
    this.urlField = urlField;
  }

  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  public void allocate(int nrargs, int nrqueryparams) {
    argumentField = new String[nrargs];
    argumentParameter = new String[nrargs];
    headerField = new String[nrqueryparams];
    headerParameter = new String[nrqueryparams];
  }

  public Object clone() {
    HttpMeta retval = (HttpMeta) super.clone();
    int nrargs = argumentField.length;
    int nrheaderparams = headerField.length;

    retval.allocate(nrargs, nrheaderparams);

    System.arraycopy(argumentField, 0, retval.argumentField, 0, nrargs);
    System.arraycopy(argumentParameter, 0, retval.argumentParameter, 0, nrargs);
    System.arraycopy(headerField, 0, retval.headerField, 0, nrheaderparams);
    System.arraycopy(headerParameter, 0, retval.headerParameter, 0, nrheaderparams);

    return retval;
  }

  public void setDefault() {
    socketTimeout = String.valueOf(DEFAULT_SOCKET_TIMEOUT);
    connectionTimeout = String.valueOf(DEFAULT_CONNECTION_TIMEOUT);
    closeIdleConnectionsTime = String.valueOf(DEFAULT_CLOSE_CONNECTIONS_TIME);
    int i;
    int nrargs;
    int nrquery;
    nrargs = 0;
    nrquery = 0;

    allocate(nrargs, nrquery);

    for (i = 0; i < nrargs; i++) {
      argumentField[i] = "arg" + i;
      argumentParameter[i] = "arg";
    }

    for (i = 0; i < nrquery; i++) {
      headerField[i] = "header" + i;
      headerParameter[i] = "header";
    }

    fieldName = "result";
    resultCodeFieldName = "";
    responseTimeFieldName = "";
    responseHeaderFieldName = "";
    encoding = "UTF-8";
  }

  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (!Utils.isEmpty(fieldName)) {
      IValueMeta v = new ValueMetaString(fieldName);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    if (!Utils.isEmpty(resultCodeFieldName)) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(resultCodeFieldName));
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    if (!Utils.isEmpty(responseTimeFieldName)) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(responseTimeFieldName));
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    String headerFieldName = variables.resolve(responseHeaderFieldName);
    if (!Utils.isEmpty(headerFieldName)) {
      IValueMeta v = new ValueMetaString(headerFieldName);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append("    ").append(XmlHandler.addTagValue("url", url));
    retval.append("    " + XmlHandler.addTagValue("urlInField", urlInField));
    retval.append("    " + XmlHandler.addTagValue("urlField", urlField));
    retval.append("    " + XmlHandler.addTagValue("encoding", encoding));
    retval.append("    " + XmlHandler.addTagValue("httpLogin", httpLogin));
    retval.append(
        "    "
            + XmlHandler.addTagValue(
                "httpPassword", Encr.encryptPasswordIfNotUsingVariables(httpPassword)));
    retval.append("    " + XmlHandler.addTagValue("proxyHost", proxyHost));
    retval.append("    " + XmlHandler.addTagValue("proxyPort", proxyPort));
    retval.append("    " + XmlHandler.addTagValue("socketTimeout", socketTimeout));
    retval.append("    " + XmlHandler.addTagValue("connectionTimeout", connectionTimeout));
    retval.append(
        "    " + XmlHandler.addTagValue("closeIdleConnectionsTime", closeIdleConnectionsTime));

    retval.append("    <lookup>").append(Const.CR);

    for (int i = 0; i < argumentField.length; i++) {
      retval.append("      <arg>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("name", argumentField[i]));
      retval.append("        ").append(XmlHandler.addTagValue("parameter", argumentParameter[i]));
      retval.append("      </arg>").append(Const.CR);
    }
    for (int i = 0; i < headerField.length; i++) {
      retval.append("      <header>" + Const.CR);
      retval.append("        " + XmlHandler.addTagValue("name", headerField[i]));
      retval.append("        " + XmlHandler.addTagValue("parameter", headerParameter[i]));
      retval.append("      </header>" + Const.CR);
    }

    retval.append("    </lookup>").append(Const.CR);

    retval.append("    <result>").append(Const.CR);
    retval.append("      ").append(XmlHandler.addTagValue("name", fieldName));
    retval.append("      ").append(XmlHandler.addTagValue("code", resultCodeFieldName));
    retval.append("      ").append(XmlHandler.addTagValue("response_time", responseTimeFieldName));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("response_header", responseHeaderFieldName));
    retval.append("    </result>").append(Const.CR);

    return retval.toString();
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      int nrargs;

      url = XmlHandler.getTagValue(transformNode, "url");
      urlInField = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "urlInField"));
      urlField = XmlHandler.getTagValue(transformNode, "urlField");
      encoding = XmlHandler.getTagValue(transformNode, "encoding");
      httpLogin = XmlHandler.getTagValue(transformNode, "httpLogin");
      httpPassword =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(transformNode, "httpPassword"));
      proxyHost = XmlHandler.getTagValue(transformNode, "proxyHost");
      proxyPort = XmlHandler.getTagValue(transformNode, "proxyPort");

      socketTimeout = XmlHandler.getTagValue(transformNode, "socketTimeout");
      connectionTimeout = XmlHandler.getTagValue(transformNode, "connectionTimeout");
      closeIdleConnectionsTime = XmlHandler.getTagValue(transformNode, "closeIdleConnectionsTime");

      Node lookup = XmlHandler.getSubNode(transformNode, "lookup");
      nrargs = XmlHandler.countNodes(lookup, "arg");

      int nrheaders = XmlHandler.countNodes(lookup, "header");
      allocate(nrargs, nrheaders);

      for (int i = 0; i < nrargs; i++) {
        Node anode = XmlHandler.getSubNodeByNr(lookup, "arg", i);

        argumentField[i] = XmlHandler.getTagValue(anode, "name");
        argumentParameter[i] = XmlHandler.getTagValue(anode, "parameter");
      }

      for (int i = 0; i < nrheaders; i++) {
        Node anode = XmlHandler.getSubNodeByNr(lookup, "header", i);
        headerField[i] = XmlHandler.getTagValue(anode, "name");
        headerParameter[i] = XmlHandler.getTagValue(anode, "parameter");
      }

      fieldName = XmlHandler.getTagValue(transformNode, "result", "name");
      resultCodeFieldName = XmlHandler.getTagValue(transformNode, "result", "code");
      responseTimeFieldName = XmlHandler.getTagValue(transformNode, "result", "response_time");
      responseHeaderFieldName = XmlHandler.getTagValue(transformNode, "result", "response_header");
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "HTTPMeta.Exception.UnableToReadTransformMeta"), e);
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

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "HTTPMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "HTTPMeta.CheckResult.NoInpuReceived"),
              transformMeta);
      remarks.add(cr);
    }
    // check Url
    if (urlInField) {
      if (Utils.isEmpty(urlField)) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "HTTPMeta.CheckResult.UrlfieldMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "HTTPMeta.CheckResult.UrlfieldOk"),
                transformMeta);
      }

    } else {
      if (Utils.isEmpty(url)) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "HTTPMeta.CheckResult.UrlMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "HTTPMeta.CheckResult.UrlOk"),
                transformMeta);
      }
    }
    remarks.add(cr);
  }

  public Http createTransform(
      TransformMeta transformMeta,
      HttpData data,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new Http(transformMeta, this, data, cnr, pipelineMeta, pipeline);
  }

  public HttpData getTransformData() {
    return new HttpData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  /** @return the encoding */
  public String getEncoding() {
    return encoding;
  }

  /** @param encoding the encoding to set */
  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /**
   * ISetter
   *
   * @param proxyHost
   */
  public void setProxyHost(String proxyHost) {
    this.proxyHost = proxyHost;
  }

  /**
   * IGetter
   *
   * @return
   */
  public String getProxyHost() {
    return proxyHost;
  }

  /**
   * ISetter
   *
   * @param proxyPort
   */
  public void setProxyPort(String proxyPort) {
    this.proxyPort = proxyPort;
  }

  /**
   * IGetter
   *
   * @return
   */
  public String getProxyPort() {
    return this.proxyPort;
  }

  /**
   * ISetter
   *
   * @param httpLogin
   */
  public void setHttpLogin(String httpLogin) {
    this.httpLogin = httpLogin;
  }

  /**
   * IGetter
   *
   * @return
   */
  public String getHttpLogin() {
    return httpLogin;
  }

  /**
   * ISetter
   *
   * @param httpPassword
   */
  public void setHttpPassword(String httpPassword) {
    this.httpPassword = httpPassword;
  }

  /** @return */
  public String getHttpPassword() {
    return httpPassword;
  }

  /** @return the resultCodeFieldName */
  public String getResultCodeFieldName() {
    return resultCodeFieldName;
  }

  /** @param resultCodeFieldName the resultCodeFieldName to set */
  public void setResultCodeFieldName(String resultCodeFieldName) {
    this.resultCodeFieldName = resultCodeFieldName;
  }

  public String getResponseTimeFieldName() {
    return responseTimeFieldName;
  }

  public void setResponseTimeFieldName(String responseTimeFieldName) {
    this.responseTimeFieldName = responseTimeFieldName;
  }

  public String getResponseHeaderFieldName() {
    return responseHeaderFieldName;
  }

  public void setResponseHeaderFieldName(String responseHeaderFieldName) {
    this.responseHeaderFieldName = responseHeaderFieldName;
  }
}
