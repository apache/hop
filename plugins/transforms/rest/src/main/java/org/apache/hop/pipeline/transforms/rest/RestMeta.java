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

package org.apache.hop.pipeline.transforms.rest;

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

@Transform(
    id = "Rest",
    image = "rest.svg",
    name = "i18n::BaseTransform.TypeLongDesc.Rest",
    description = "i18n::BaseTransform.TypeTooltipDesc.Rest",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Lookup",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/transforms/rest.html")
public class RestMeta extends BaseTransformMeta implements ITransformMeta<Rest, RestData> {
  private static final Class<?> PKG = RestMeta.class; // For Translator

  public static final String[] APPLICATION_TYPES =
      new String[] {
        "TEXT PLAIN",
        "XML",
        "JSON",
        "OCTET STREAM",
        "XHTML",
        "FORM URLENCODED",
        "ATOM XML",
        "SVG XML",
        "TEXT XML"
      };
  public static final String APPLICATION_TYPE_TEXT_PLAIN = "TEXT PLAIN";
  public static final String APPLICATION_TYPE_XML = "XML";
  public static final String APPLICATION_TYPE_JSON = "JSON";
  public static final String APPLICATION_TYPE_OCTET_STREAM = "OCTET STREAM";
  public static final String APPLICATION_TYPE_XHTML = "XHTML";
  public static final String APPLICATION_TYPE_FORM_URLENCODED = "FORM URLENCODED";
  public static final String APPLICATION_TYPE_ATOM_XML = "ATOM XML";
  public static final String APPLICATION_TYPE_SVG_XML = "SVG XML";
  public static final String APPLICATION_TYPE_TEXT_XML = "TEXT XML";

  private String applicationType;

  public static final String[] HTTP_METHODS =
      new String[] {"GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH"};

  public static final String HTTP_METHOD_GET = "GET";
  public static final String HTTP_METHOD_POST = "POST";
  public static final String HTTP_METHOD_PUT = "PUT";
  public static final String HTTP_METHOD_DELETE = "DELETE";
  public static final String HTTP_METHOD_HEAD = "HEAD";
  public static final String HTTP_METHOD_OPTIONS = "OPTIONS";
  public static final String HTTP_METHOD_PATCH = "PATCH";

  /** URL / service to be called */
  private String url;

  private boolean urlInField;
  private String urlField;

  /** headers name */
  private String[] headerField;

  private String[] headerName;

  /** Query parameters name */
  private String[] parameterField;

  private String[] parameterName;

  /** Matrix parameters name */
  private String[] matrixParameterField;

  private String[] matrixParameterName;

  /** function result: new value name */
  private String fieldName;

  private String resultCodeFieldName;
  private String responseTimeFieldName;
  private String responseHeaderFieldName;

  /** proxy */
  private String proxyHost;

  private String proxyPort;
  private String httpLogin;
  private String httpPassword;
  private boolean preemptive;

  /** Body fieldname */
  private String bodyField;

  /** HTTP Method */
  private String method;

  private boolean dynamicMethod;
  private String methodFieldName;

  /** Trust store */
  private String trustStoreFile;

  private String trustStorePassword;

  public RestMeta() {
    super(); // allocate BaseTransformMeta
  }

  /** @return Returns the method. */
  public String getMethod() {
    return method;
  }

  /** @param value The method to set. */
  public void setMethod(String value) {
    this.method = value;
  }

  /** @return Returns the bodyField. */
  public String getBodyField() {
    return bodyField;
  }

  /** @param value The bodyField to set. */
  public void setBodyField(String value) {
    this.bodyField = value;
  }

  /** @return Returns the headerName. */
  public String[] getHeaderName() {
    return headerName;
  }

  /** @param value The headerName to set. */
  public void setHeaderName(String[] value) {
    this.headerName = value;
  }

  /** @return Returns the parameterField. */
  public String[] getParameterField() {
    return parameterField;
  }

  /** @param value The parameterField to set. */
  public void setParameterField(String[] value) {
    this.parameterField = value;
  }

  /** @return Returns the parameterName. */
  public String[] getParameterName() {
    return parameterName;
  }

  /** @param value The parameterName to set. */
  public void setParameterName(String[] value) {
    this.parameterName = value;
  }

  /** @return Returns the matrixParameterField. */
  public String[] getMatrixParameterField() {
    return matrixParameterField;
  }

  /** @param value The matrixParameterField to set. */
  public void setMatrixParameterField(String[] value) {
    this.matrixParameterField = value;
  }

  /** @return Returns the matrixParameterName. */
  public String[] getMatrixParameterName() {
    return matrixParameterName;
  }

  /** @param value The matrixParameterName to set. */
  public void setMatrixParameterName(String[] value) {
    this.matrixParameterName = value;
  }

  /** @return Returns the headerField. */
  public String[] getHeaderField() {
    return headerField;
  }

  /** @param value The headerField to set. */
  public void setHeaderField(String[] value) {
    this.headerField = value;
  }

  /** @return Returns the procedure. */
  public String getUrl() {
    return url;
  }

  /** @param procedure The procedure to set. */
  public void setUrl(String procedure) {
    this.url = procedure;
  }

  /** @return Is the url coded in a field? */
  public boolean isUrlInField() {
    return urlInField;
  }

  /** @param urlInField Is the url coded in a field? */
  public void setUrlInField(boolean urlInField) {
    this.urlInField = urlInField;
  }

  /** @return Is preemptive? */
  public boolean isPreemptive() {
    return preemptive;
  }

  /** @param preemptive Ispreemptive? */
  public void setPreemptive(boolean preemptive) {
    this.preemptive = preemptive;
  }

  /** @return Is the method defined in a field? */
  public boolean isDynamicMethod() {
    return dynamicMethod;
  }

  /** @param dynamicMethod If the method is defined in a field? */
  public void setDynamicMethod(boolean dynamicMethod) {
    this.dynamicMethod = dynamicMethod;
  }

  /** @return methodFieldName */
  public String getMethodFieldName() {
    return methodFieldName;
  }

  /** @param methodFieldName */
  public void setMethodFieldName(String methodFieldName) {
    this.methodFieldName = methodFieldName;
  }

  /** @return The field name that contains the url. */
  public String getUrlField() {
    return urlField;
  }

  /** @param urlField name of the field that contains the url */
  public void setUrlField(String urlField) {
    this.urlField = urlField;
  }

  /** @return Returns the resultName. */
  public String getFieldName() {
    return fieldName;
  }

  /** @param resultName The resultName to set. */
  public void setFieldName(String resultName) {
    this.fieldName = resultName;
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    readData(transformNode, metadataProvider);
  }

  @Deprecated
  public void allocate(int nrheaders, int nrparamers) {
    allocate(nrheaders, nrparamers, 0);
  }

  public void allocate(int nrheaders, int nrparamers, int nrmatrixparameters) {
    headerField = new String[nrheaders];
    headerName = new String[nrheaders];
    parameterField = new String[nrparamers];
    parameterName = new String[nrparamers];
    matrixParameterField = new String[nrmatrixparameters];
    matrixParameterName = new String[nrmatrixparameters];
  }

  @Override
  public Object clone() {
    RestMeta retval = (RestMeta) super.clone();

    int nrheaders = headerName.length;
    int nrparameters = parameterField.length;
    int nrmatrixparameters = matrixParameterField.length;

    retval.allocate(nrheaders, nrparameters, nrmatrixparameters);
    System.arraycopy(headerField, 0, retval.headerField, 0, nrheaders);
    System.arraycopy(headerName, 0, retval.headerName, 0, nrheaders);
    System.arraycopy(parameterField, 0, retval.parameterField, 0, nrparameters);
    System.arraycopy(parameterName, 0, retval.parameterName, 0, nrparameters);
    System.arraycopy(matrixParameterField, 0, retval.matrixParameterField, 0, nrmatrixparameters);
    System.arraycopy(matrixParameterName, 0, retval.matrixParameterName, 0, nrmatrixparameters);

    return retval;
  }

  @Override
  public Rest createTransform(
      TransformMeta transformMeta,
      RestData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new Rest(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public void setDefault() {
    allocate(0, 0, 0);

    this.fieldName = "result";
    this.resultCodeFieldName = "";
    this.responseTimeFieldName = "";
    this.responseHeaderFieldName = "";
    this.method = HTTP_METHOD_GET;
    this.dynamicMethod = false;
    this.methodFieldName = null;
    this.preemptive = false;
    this.trustStoreFile = null;
    this.trustStorePassword = null;
    this.applicationType = APPLICATION_TYPE_TEXT_PLAIN;
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
    if (!Utils.isEmpty(fieldName)) {
      IValueMeta v = new ValueMetaString(variables.resolve(fieldName));
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

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder();
    retval.append("    ").append(XmlHandler.addTagValue("applicationType", applicationType));
    retval.append("    ").append(XmlHandler.addTagValue("method", method));
    retval.append("    ").append(XmlHandler.addTagValue("url", url));
    retval.append("    ").append(XmlHandler.addTagValue("urlInField", urlInField));
    retval.append("    ").append(XmlHandler.addTagValue("dynamicMethod", dynamicMethod));
    retval.append("    ").append(XmlHandler.addTagValue("methodFieldName", methodFieldName));

    retval.append("    ").append(XmlHandler.addTagValue("urlField", urlField));
    retval.append("    ").append(XmlHandler.addTagValue("bodyField", bodyField));
    retval.append("    ").append(XmlHandler.addTagValue("httpLogin", httpLogin));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "httpPassword", Encr.encryptPasswordIfNotUsingVariables(httpPassword)));

    retval.append("    ").append(XmlHandler.addTagValue("proxyHost", proxyHost));
    retval.append("    ").append(XmlHandler.addTagValue("proxyPort", proxyPort));
    retval.append("    ").append(XmlHandler.addTagValue("preemptive", preemptive));

    retval.append("    ").append(XmlHandler.addTagValue("trustStoreFile", trustStoreFile));
    retval
        .append("    ")
        .append(
            XmlHandler.addTagValue(
                "trustStorePassword", Encr.encryptPasswordIfNotUsingVariables(trustStorePassword)));

    retval.append("    <headers>").append(Const.CR);
    for (int i = 0, len = (headerName != null ? headerName.length : 0); i < len; i++) {
      retval.append("      <header>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("field", headerField[i]));
      retval.append("        ").append(XmlHandler.addTagValue("name", headerName[i]));
      retval.append("        </header>").append(Const.CR);
    }
    retval.append("      </headers>").append(Const.CR);

    retval.append("    <parameters>").append(Const.CR);
    for (int i = 0, len = (parameterName != null ? parameterName.length : 0); i < len; i++) {
      retval.append("      <parameter>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("field", parameterField[i]));
      retval.append("        ").append(XmlHandler.addTagValue("name", parameterName[i]));
      retval.append("        </parameter>").append(Const.CR);
    }
    retval.append("      </parameters>").append(Const.CR);

    retval.append("    <matrixParameters>").append(Const.CR);
    for (int i = 0, len = (matrixParameterName != null ? matrixParameterName.length : 0);
        i < len;
        i++) {
      retval.append("      <matrixParameter>").append(Const.CR);
      retval.append("        ").append(XmlHandler.addTagValue("field", matrixParameterField[i]));
      retval.append("        ").append(XmlHandler.addTagValue("name", matrixParameterName[i]));
      retval.append("        </matrixParameter>").append(Const.CR);
    }
    retval.append("      </matrixParameters>").append(Const.CR);

    retval.append("    <result>").append(Const.CR);
    retval.append("      ").append(XmlHandler.addTagValue("name", fieldName));
    retval.append("      ").append(XmlHandler.addTagValue("code", resultCodeFieldName));
    retval.append("      ").append(XmlHandler.addTagValue("response_time", responseTimeFieldName));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("response_header", responseHeaderFieldName));
    retval.append("      </result>").append(Const.CR);

    return retval.toString();
  }

  private void readData(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      applicationType = XmlHandler.getTagValue(transformNode, "applicationType");
      method = XmlHandler.getTagValue(transformNode, "method");
      url = XmlHandler.getTagValue(transformNode, "url");
      urlInField = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "urlInField"));
      methodFieldName = XmlHandler.getTagValue(transformNode, "methodFieldName");

      dynamicMethod = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "dynamicMethod"));
      urlField = XmlHandler.getTagValue(transformNode, "urlField");
      bodyField = XmlHandler.getTagValue(transformNode, "bodyField");
      httpLogin = XmlHandler.getTagValue(transformNode, "httpLogin");
      httpPassword =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(transformNode, "httpPassword"));

      proxyHost = XmlHandler.getTagValue(transformNode, "proxyHost");
      proxyPort = XmlHandler.getTagValue(transformNode, "proxyPort");
      preemptive = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, "preemptive"));

      trustStoreFile = XmlHandler.getTagValue(transformNode, "trustStoreFile");
      trustStorePassword =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(transformNode, "trustStorePassword"));

      Node headernode = XmlHandler.getSubNode(transformNode, "headers");
      int nrheaders = XmlHandler.countNodes(headernode, "header");
      Node paramnode = XmlHandler.getSubNode(transformNode, "parameters");
      int nrparameters = XmlHandler.countNodes(paramnode, "parameter");
      Node matrixparamnode = XmlHandler.getSubNode(transformNode, "matrixParameters");
      int nrmatrixparameters = XmlHandler.countNodes(matrixparamnode, "matrixParameter");

      allocate(nrheaders, nrparameters, nrmatrixparameters);
      for (int i = 0; i < nrheaders; i++) {
        Node anode = XmlHandler.getSubNodeByNr(headernode, "header", i);
        headerField[i] = XmlHandler.getTagValue(anode, "field");
        headerName[i] = XmlHandler.getTagValue(anode, "name");
      }
      for (int i = 0; i < nrparameters; i++) {
        Node anode = XmlHandler.getSubNodeByNr(paramnode, "parameter", i);
        parameterField[i] = XmlHandler.getTagValue(anode, "field");
        parameterName[i] = XmlHandler.getTagValue(anode, "name");
      }
      for (int i = 0; i < nrmatrixparameters; i++) {
        Node anode = XmlHandler.getSubNodeByNr(matrixparamnode, "matrixParameter", i);
        matrixParameterField[i] = XmlHandler.getTagValue(anode, "field");
        matrixParameterName[i] = XmlHandler.getTagValue(anode, "name");
      }

      fieldName = XmlHandler.getTagValue(transformNode, "result", "name"); // Optional, can be null
      resultCodeFieldName =
          XmlHandler.getTagValue(transformNode, "result", "code"); // Optional, can be null
      responseTimeFieldName =
          XmlHandler.getTagValue(transformNode, "result", "response_time"); // Optional, can be null
      responseHeaderFieldName =
          XmlHandler.getTagValue(
              transformNode, "result", "response_header"); // Optional, can be null
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "RestMeta.Exception.UnableToReadTransformMeta"), e);
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

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "RestMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "RestMeta.CheckResult.NoInpuReceived"),
              transformMeta);
    }
    remarks.add(cr);

    // check Url
    if (urlInField) {
      if (Utils.isEmpty(urlField)) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "RestMeta.CheckResult.UrlfieldMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "RestMeta.CheckResult.UrlfieldOk"),
                transformMeta);
      }

    } else {
      if (Utils.isEmpty(url)) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "RestMeta.CheckResult.UrlMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "RestMeta.CheckResult.UrlOk"),
                transformMeta);
      }
    }
    remarks.add(cr);

    // Check method
    if (dynamicMethod) {
      if (Utils.isEmpty(methodFieldName)) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "RestMeta.CheckResult.MethodFieldMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "RestMeta.CheckResult.MethodFieldOk"),
                transformMeta);
      }

    } else {
      if (Utils.isEmpty(method)) {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "RestMeta.CheckResult.MethodMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                CheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "RestMeta.CheckResult.MethodOk"),
                transformMeta);
      }
    }
    remarks.add(cr);
  }

  @Override
  public RestData getTransformData() {
    return new RestData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  /** @return the resultCodeFieldName */
  public String getResultCodeFieldName() {
    return resultCodeFieldName;
  }

  /** @param resultCodeFieldName the resultCodeFieldName to set */
  public void setResultCodeFieldName(String resultCodeFieldName) {
    this.resultCodeFieldName = resultCodeFieldName;
  }

  /**
   * Setter
   *
   * @param proxyHost
   */
  public void setProxyHost(String proxyHost) {
    this.proxyHost = proxyHost;
  }

  /**
   * Getter
   *
   * @return
   */
  public String getProxyHost() {
    return proxyHost;
  }

  /**
   * Setter
   *
   * @param proxyPort
   */
  public void setProxyPort(String proxyPort) {
    this.proxyPort = proxyPort;
  }

  /**
   * Getter
   *
   * @return
   */
  public String getProxyPort() {
    return this.proxyPort;
  }

  /**
   * Setter
   *
   * @param applicationType
   */
  public void setApplicationType(String applicationType) {
    this.applicationType = applicationType;
  }

  /**
   * Getter
   *
   * @return
   */
  public String getApplicationType() {
    return applicationType;
  }

  /**
   * Setter
   *
   * @param httpLogin
   */
  public void setHttpLogin(String httpLogin) {
    this.httpLogin = httpLogin;
  }

  /**
   * Getter
   *
   * @return
   */
  public String getHttpLogin() {
    return httpLogin;
  }

  /**
   * Setter
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

  /**
   * Setter
   *
   * @param trustStoreFile
   */
  public void setTrustStoreFile(String trustStoreFile) {
    this.trustStoreFile = trustStoreFile;
  }

  /** @return trustStoreFile */
  public String getTrustStoreFile() {
    return trustStoreFile;
  }

  /**
   * Setter
   *
   * @param trustStorePassword
   */
  public void setTrustStorePassword(String trustStorePassword) {
    this.trustStorePassword = trustStorePassword;
  }

  /** @return trustStorePassword */
  public String getTrustStorePassword() {
    return trustStorePassword;
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

  public static boolean isActiveBody(String method) {
    if (Utils.isEmpty(method)) {
      return false;
    }
    return (method.equals(HTTP_METHOD_POST)
        || method.equals(HTTP_METHOD_PUT)
        || method.equals(HTTP_METHOD_PATCH));
  }

  public static boolean isActiveParameters(String method) {
    if (Utils.isEmpty(method)) {
      return false;
    }
    return (method.equals(HTTP_METHOD_POST)
        || method.equals(HTTP_METHOD_PUT)
        || method.equals(HTTP_METHOD_PATCH)
        || method.equals(HTTP_METHOD_DELETE));
  }
}
