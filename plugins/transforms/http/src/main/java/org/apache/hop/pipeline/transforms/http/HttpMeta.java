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

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
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

@Transform(
    id = "Http",
    image = "http.svg",
    name = "i18n::HTTP.Name",
    description = "i18n::HTTP.Description",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Utility",
    keywords = "i18n::HttpMeta.keyword",
    documentationUrl = "/pipeline/transforms/http.html")
@Getter
@Setter
public class HttpMeta extends BaseTransformMeta<Http, HttpData> {
  private static final Class<?> PKG = HttpMeta.class;

  // the timeout for waiting for data (milliseconds)
  public static final int DEFAULT_SOCKET_TIMEOUT = 10000;

  // the timeout until a connection is established (milliseconds)
  public static final int DEFAULT_CONNECTION_TIMEOUT = 10000;

  // the time to wait till a connection is closed (milliseconds)? -1 is no not close.
  public static final int DEFAULT_CLOSE_CONNECTIONS_TIME = -1;
  public static final String CONST_HEADER = "header";
  public static final String CONST_RESULT = "result";
  public static final String CONST_SPACES_LONG = "        ";
  public static final String CONST_SPACES = "      ";
  public static final String CONST_PARAMETER = "parameter";

  @HopMetadataProperty(
      key = "lookup",
      injectionKey = "LOOKUP",
      injectionKeyDescription = "HttpMeta.Injection.Lookup")
  private LookupParameters lookupParameters;

  @HopMetadataProperty(
      key = "socketTimeout",
      injectionKey = "SOCKET_TIMEOUT",
      injectionKeyDescription = "HttpMeta.Injection.SOCKET_TIMEOUT")
  private String socketTimeout;

  @HopMetadataProperty(
      key = "connectionTimeout",
      injectionKey = "CONNECTION_TIMEOUT",
      injectionKeyDescription = "HttpMeta.Injection.CONNECTION_TIMEOUT")
  private String connectionTimeout;

  @HopMetadataProperty(
      key = "closeIdleConnectionsTime",
      injectionKey = "CLOSE_IDLE_CONNECTIONS_TIME",
      injectionKeyDescription = "HttpMeta.Injection.CLOSE_IDLE_CONNECTIONS_TIME")
  private String closeIdleConnectionsTime;

  /** URL / service to be called */
  @HopMetadataProperty(
      key = "url",
      injectionKey = "URL",
      injectionKeyDescription = "HttpMeta.Injection.URL")
  private String url;

  /** The encoding to use for retrieval of the data */
  @HopMetadataProperty(
      key = "encoding",
      injectionKey = "ENCODING",
      injectionKeyDescription = "HttpMeta.Injection.ENCODING")
  private String encoding;

  @HopMetadataProperty(
      key = "urlInField",
      injectionKey = "URL_IN_FIELD",
      injectionKeyDescription = "HttpMeta.Injection.URL_IN_FIELD")
  private boolean urlInField;

  @HopMetadataProperty(
      key = "ignoreSsl",
      injectionKey = "IGNORE_SSL",
      injectionKeyDescription = "HttpMeta.Injection.IGNORE_SSL")
  private boolean ignoreSsl;

  @HopMetadataProperty(
      key = "urlField",
      injectionKey = "URL_FIELD",
      injectionKeyDescription = "HttpMeta.Injection.URL_FIELD")
  private String urlField;

  @HopMetadataProperty(
      key = "proxyHost",
      injectionKey = "PROXY_HOST",
      injectionKeyDescription = "HttpMeta.Injection.PROXY_HOST")
  private String proxyHost;

  @HopMetadataProperty(
      key = "proxyPort",
      injectionKey = "PROXY_PORT",
      injectionKeyDescription = "HttpMeta.Injection.PROXY_PORT")
  private String proxyPort;

  @HopMetadataProperty(
      key = "httpLogin",
      injectionKey = "HTTP_LOGIN",
      injectionKeyDescription = "HttpMeta.Injection.HTTP_LOGIN")
  private String httpLogin;

  @HopMetadataProperty(
      key = "httpPassword",
      injectionKey = "HTTP_PASSWORD",
      injectionKeyDescription = "HttpMeta.Injection.HTTP_PASSWORD",
      password = true)
  private String httpPassword;

  @HopMetadataProperty(
      key = "result",
      injectionKey = "RESULT",
      injectionKeyDescription = "HttpMeta.Injection.RESULT")
  private ResultFields resultFields;

  public HttpMeta() {
    super(); // allocate BaseTransformMeta
    this.lookupParameters = new LookupParameters();
    this.resultFields = new ResultFields();
    this.socketTimeout = String.valueOf(DEFAULT_SOCKET_TIMEOUT);
    this.connectionTimeout = String.valueOf(DEFAULT_CONNECTION_TIMEOUT);
    this.closeIdleConnectionsTime = String.valueOf(DEFAULT_CLOSE_CONNECTIONS_TIME);
    this.resultFields.fieldName = CONST_RESULT;

    this.encoding = Const.UTF_8;
  }

  public HttpMeta(HttpMeta m) {
    this();
    this.closeIdleConnectionsTime = m.closeIdleConnectionsTime;
    this.connectionTimeout = m.connectionTimeout;
    this.encoding = m.encoding;
    this.httpLogin = m.httpLogin;
    this.httpPassword = m.httpPassword;
    this.ignoreSsl = m.ignoreSsl;
    this.proxyHost = m.proxyHost;
    this.proxyPort = m.proxyPort;
    this.socketTimeout = m.socketTimeout;
    this.url = m.url;
    this.urlField = m.urlField;
    this.urlInField = m.urlInField;
    this.lookupParameters = new LookupParameters(m.lookupParameters);
    this.resultFields = new ResultFields(m.resultFields);
  }

  @Override
  public Object clone() {
    return new HttpMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (!Utils.isEmpty(resultFields.fieldName)) {
      IValueMeta v = new ValueMetaString(resultFields.fieldName);
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    if (!Utils.isEmpty(resultFields.resultCodeFieldName)) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(resultFields.resultCodeFieldName));
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    if (!Utils.isEmpty(resultFields.responseTimeFieldName)) {
      IValueMeta v = new ValueMetaInteger(variables.resolve(resultFields.responseTimeFieldName));
      v.setOrigin(name);
      inputRowMeta.addValueMeta(v);
    }
    String headerFieldName = variables.resolve(resultFields.responseHeaderFieldName);
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

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

  @Getter
  @Setter
  public static class QueryParameter {
    @HopMetadataProperty(
        key = "name",
        injectionKey = "ARGUMENT_FIELD",
        injectionKeyDescription = "HttpMeta.Injection.ARGUMENT_FIELD")
    private String field;

    @HopMetadataProperty(
        key = "parameter",
        injectionKey = "ARGUMENT_PARAMETER",
        injectionKeyDescription = "HttpMeta.Injection.ARGUMENT_PARAMETER")
    private String parameter;

    public QueryParameter() {}

    public QueryParameter(String field, String parameter) {
      this.field = field;
      this.parameter = parameter;
    }

    public QueryParameter(QueryParameter p) {
      this.field = p.field;
      this.parameter = p.parameter;
    }
  }

  @Getter
  @Setter
  public static class HeaderParameter {
    @HopMetadataProperty(
        key = "name",
        injectionKey = "HEADER_FIELD",
        injectionKeyDescription = "HttpMeta.Injection.HEADER_FIELD")
    private String field;

    @HopMetadataProperty(
        key = "parameter",
        injectionKey = "HEADER_PARAMETER",
        injectionKeyDescription = "HttpMeta.Injection.HEADER_PARAMETER")
    private String parameter;

    public HeaderParameter() {}

    public HeaderParameter(String field, String parameter) {
      this.field = field;
      this.parameter = parameter;
    }

    public HeaderParameter(HeaderParameter p) {
      this.field = p.field;
      this.parameter = p.parameter;
    }
  }

  @Getter
  @Setter
  public static class LookupParameters {
    @HopMetadataProperty(
        key = "arg",
        injectionKey = "ARG",
        injectionKeyDescription = "HttpMeta.Injection.ARG")
    private List<QueryParameter> queryParameters;

    @HopMetadataProperty(
        key = "header",
        injectionKey = "HEADER",
        injectionKeyDescription = "HttpMeta.Injection.HEADER")
    private List<HeaderParameter> headers;

    public LookupParameters() {
      this.queryParameters = new ArrayList<>();
      this.headers = new ArrayList<>();
    }

    public LookupParameters(LookupParameters p) {
      this();
      p.headers.forEach(h -> this.headers.add(new HeaderParameter(h)));
      p.queryParameters.forEach(q -> this.queryParameters.add(new QueryParameter(q)));
    }
  }

  @Getter
  @Setter
  public static class ResultFields {
    /** function result: new value name */
    @HopMetadataProperty(
        key = "name",
        injectionKey = "RESULT_FIELD_NAME",
        injectionKeyDescription = "HttpMeta.Injection.RESULT_FIELD_NAME")
    private String fieldName;

    @HopMetadataProperty(
        key = "code",
        injectionKey = "RESULT_CODE_FIELD_NAME",
        injectionKeyDescription = "HttpMeta.Injection.RESULT_CODE_FIELD_NAME")
    private String resultCodeFieldName;

    @HopMetadataProperty(
        key = "response_time",
        injectionKey = "RESPONSE_TIME_FIELD_NAME",
        injectionKeyDescription = "HttpMeta.Injection.RESPONSE_TIME_FIELD_NAME")
    private String responseTimeFieldName;

    @HopMetadataProperty(
        key = "response_header",
        injectionKey = "RESPONSE_HEADER_FIELD_NAME",
        injectionKeyDescription = "HttpMeta.Injection.RESPONSE_HEADER_FIELD_NAME")
    private String responseHeaderFieldName;

    public ResultFields() {
      this.fieldName = "";
      this.resultCodeFieldName = "";
      this.responseTimeFieldName = "";
      this.responseHeaderFieldName = "";
    }

    public ResultFields(ResultFields r) {
      this();
      this.fieldName = r.fieldName;
      this.responseHeaderFieldName = r.responseHeaderFieldName;
      this.responseTimeFieldName = r.responseTimeFieldName;
      this.resultCodeFieldName = r.resultCodeFieldName;
    }
  }
}
