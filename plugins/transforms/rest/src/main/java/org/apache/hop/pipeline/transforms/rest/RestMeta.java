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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.fields.HeaderField;
import org.apache.hop.pipeline.transforms.rest.fields.MatrixParameterField;
import org.apache.hop.pipeline.transforms.rest.fields.ParameterField;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;

@Setter
@Getter
@Transform(
    id = "Rest",
    image = "rest.svg",
    name = "i18n::Rest.Name",
    description = "i18n::Rest.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    keywords = "i18n::RestMeta.keyword",
    documentationUrl = "/pipeline/transforms/rest.html")
public class RestMeta extends BaseTransformMeta<Rest, RestData> {
  private static final Class<?> PKG = RestMeta.class;

  public static final String APPLICATION_TYPE_TEXT_PLAIN = "TEXT PLAIN";
  public static final String APPLICATION_TYPE_XML = "XML";
  public static final String APPLICATION_TYPE_JSON = "JSON";
  public static final String APPLICATION_TYPE_OCTET_STREAM = "OCTET STREAM";
  public static final String APPLICATION_TYPE_XHTML = "XHTML";
  public static final String APPLICATION_TYPE_FORM_URLENCODED = "FORM URLENCODED";
  public static final String APPLICATION_TYPE_ATOM_XML = "ATOM XML";
  public static final String APPLICATION_TYPE_SVG_XML = "SVG XML";
  public static final String APPLICATION_TYPE_TEXT_XML = "TEXT XML";
  public static final String HTTP_METHOD_GET = "GET";
  public static final String HTTP_METHOD_POST = "POST";
  public static final String HTTP_METHOD_PUT = "PUT";
  public static final String HTTP_METHOD_DELETE = "DELETE";
  public static final String HTTP_METHOD_HEAD = "HEAD";
  public static final String HTTP_METHOD_OPTIONS = "OPTIONS";
  public static final String HTTP_METHOD_PATCH = "PATCH";

  public static final String[] APPLICATION_TYPES =
      new String[] {
        APPLICATION_TYPE_TEXT_PLAIN,
        APPLICATION_TYPE_XML,
        APPLICATION_TYPE_JSON,
        APPLICATION_TYPE_OCTET_STREAM,
        APPLICATION_TYPE_XHTML,
        APPLICATION_TYPE_FORM_URLENCODED,
        APPLICATION_TYPE_ATOM_XML,
        APPLICATION_TYPE_SVG_XML,
        APPLICATION_TYPE_TEXT_XML
      };

  @HopMetadataProperty(key = "applicationType", injectionKey = "APPLICATION_TYPE")
  private String applicationType;

  public static final String[] HTTP_METHODS =
      new String[] {
        HTTP_METHOD_GET,
        HTTP_METHOD_POST,
        HTTP_METHOD_PUT,
        HTTP_METHOD_DELETE,
        HTTP_METHOD_HEAD,
        HTTP_METHOD_OPTIONS,
        HTTP_METHOD_PATCH
      };

  /** The default timeout until a connection is established (milliseconds) */
  public static final int DEFAULT_CONNECTION_TIMEOUT = 10000;

  /** The default timeout for waiting for reading data (milliseconds) */
  public static final int DEFAULT_READ_TIMEOUT = 10000;

  @HopMetadataProperty(key = "connection_name", injectionKey = "CONNECTION_NAME")
  private String connectionName;

  @HopMetadataProperty(key = "url", injectionKey = "URL")
  private String url;

  @HopMetadataProperty(key = "urlInField", injectionKey = "URL_IN_FIELD")
  private boolean urlInField;

  @HopMetadataProperty(key = "urlField", injectionKey = "URL_IN_FIELD")
  private String urlField;

  @HopMetadataProperty(key = "proxyHost", injectionKey = "PROXY_HOST")
  private String proxyHost;

  @HopMetadataProperty(key = "proxyPort", injectionKey = "PROXY_PORT")
  private String proxyPort;

  @HopMetadataProperty(key = "httpLogin", injectionKey = "HTTP_LOGIN")
  private String httpLogin;

  @HopMetadataProperty(key = "httpPassword", injectionKey = "HTTP_PASSWORD", password = true)
  private String httpPassword;

  @HopMetadataProperty(key = "preemptive", injectionKey = "PREEMPTIVE")
  private boolean preemptive;

  @HopMetadataProperty(key = "bodyField", injectionKey = "BODY_FIELD")
  private String bodyField;

  @HopMetadataProperty(key = "method", injectionKey = "METHOD")
  private String method;

  @HopMetadataProperty(key = "dynamicMethod", injectionKey = "DYMAMIC_METHOD")
  private boolean dynamicMethod;

  @HopMetadataProperty(key = "methodFieldName", injectionKey = "METHOD_FIELD_NAME")
  private String methodFieldName;

  @HopMetadataProperty(key = "trustStoreFile", injectionKey = "TRUSTSTORE_FILE")
  private String trustStoreFile;

  @HopMetadataProperty(
      key = "trustStorePassword",
      injectionKey = "TRUSTSTORE_PASSWORD",
      password = true)
  private String trustStorePassword;

  @HopMetadataProperty(key = "connectionTimeout", injectionKey = "CONNECTION_TIMEOUT")
  private String connectionTimeout;

  @HopMetadataProperty(key = "readTimeout", injectionKey = "READ_TIMEOUT")
  private String readTimeout;

  @HopMetadataProperty(key = "ignoreSsl", injectionKey = "IGNORE_SSL")
  private boolean ignoreSsl;

  @HopMetadataProperty(
      key = "header",
      groupKey = "headers",
      injectionKey = "HEADERS",
      injectionGroupKey = "HEADER")
  private List<HeaderField> headerFields;

  @HopMetadataProperty(
      key = "parameter",
      injectionKey = "PARAMETER",
      groupKey = "parameters",
      injectionGroupKey = "PARAMETERS")
  private List<ParameterField> parameterFields;

  @HopMetadataProperty(
      key = "matrixParameter",
      injectionKey = "MATRIX_PARAMETER",
      groupKey = "matrixParameters",
      injectionGroupKey = "MATRIX_PARAMETERS")
  private List<MatrixParameterField> matrixParameterFields;

  @HopMetadataProperty(key = "result", injectionKey = "RESULT")
  private ResultField resultField;

  public RestMeta() {
    super(); // allocate BaseTransformMeta
    headerFields = new ArrayList<>();
    parameterFields = new ArrayList<>();
    matrixParameterFields = new ArrayList<>();
    resultField = new ResultField();
  }

  @Override
  public Object clone() {
    RestMeta retval = (RestMeta) super.clone();

    return retval;
  }

  @Override
  public void setDefault() {
    headerFields = new ArrayList<>();
    parameterFields = new ArrayList<>();
    matrixParameterFields = new ArrayList<>();
    resultField = new ResultField();

    this.method = HTTP_METHOD_GET;
    this.dynamicMethod = false;
    this.methodFieldName = null;
    this.preemptive = false;
    this.trustStoreFile = null;
    this.trustStorePassword = null;
    this.applicationType = APPLICATION_TYPE_TEXT_PLAIN;
    this.readTimeout = String.valueOf(DEFAULT_READ_TIMEOUT);
    this.connectionTimeout = String.valueOf(DEFAULT_CONNECTION_TIMEOUT);
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (!Utils.isEmpty(resultField.getFieldName())) {
      IValueMeta v = new ValueMetaString(variables.resolve(resultField.getFieldName()));
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }

    if (!Utils.isEmpty(resultField.getCode())) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(resultField.getCode()));
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    if (!Utils.isEmpty(resultField.getResponseTime())) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(resultField.getResponseTime()));
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    String headerFieldName = variables.resolve(resultField.getResponseHeader());
    if (!Utils.isEmpty(headerFieldName)) {
      IValueMeta v = new ValueMetaString(headerFieldName);
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

    // See if we have input streams leading to this transform!
    if (input.length > 0) {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_OK,
              BaseMessages.getString(PKG, "RestMeta.CheckResult.ReceivingInfoFromOtherTransforms"),
              transformMeta);
    } else {
      cr =
          new CheckResult(
              ICheckResult.TYPE_RESULT_ERROR,
              BaseMessages.getString(PKG, "RestMeta.CheckResult.NoInpuReceived"),
              transformMeta);
    }
    remarks.add(cr);

    // check Url
    if (urlInField) {
      if (Utils.isEmpty(urlField)) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "RestMeta.CheckResult.UrlfieldMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "RestMeta.CheckResult.UrlfieldOk"),
                transformMeta);
      }

    } else {
      if (Utils.isEmpty(url)) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "RestMeta.CheckResult.UrlMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
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
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "RestMeta.CheckResult.MethodFieldMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "RestMeta.CheckResult.MethodFieldOk"),
                transformMeta);
      }

    } else {
      if (Utils.isEmpty(method)) {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_ERROR,
                BaseMessages.getString(PKG, "RestMeta.CheckResult.MethodMissing"),
                transformMeta);
      } else {
        cr =
            new CheckResult(
                ICheckResult.TYPE_RESULT_OK,
                BaseMessages.getString(PKG, "RestMeta.CheckResult.MethodOk"),
                transformMeta);
      }
    }
    remarks.add(cr);
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  public static boolean isActiveBody(String method) {
    if (Utils.isEmpty(method)) {
      return false;
    }
    return (method.equals(HTTP_METHOD_POST)
        || method.equals(HTTP_METHOD_PUT)
        || method.equals(HTTP_METHOD_PATCH)
        || method.equals(HTTP_METHOD_DELETE));
  }

  public static boolean isActiveParameters(String method) {
    if (Utils.isEmpty(method)) {
      return false;
    }
    return (method.equals(HTTP_METHOD_GET)
        || method.equals(HTTP_METHOD_POST)
        || method.equals(HTTP_METHOD_PUT)
        || method.equals(HTTP_METHOD_PATCH)
        || method.equals(HTTP_METHOD_DELETE));
  }
}
