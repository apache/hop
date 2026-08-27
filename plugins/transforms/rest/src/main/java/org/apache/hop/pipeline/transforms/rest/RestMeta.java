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
import java.util.Set;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.common.RestConst;
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
    documentationUrl = "/pipeline/transforms/rest.html",
    classLoaderGroup = "rest")
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

  /**
   * Well-known methods that never carry a request body. Any other method — including a custom verb
   * such as LIST or PURGE (issue #4770) — is allowed to send one.
   */
  private static final Set<String> BODY_LESS_METHODS =
      Set.of(HTTP_METHOD_GET, HTTP_METHOD_HEAD, HTTP_METHOD_OPTIONS);

  /** Well-known methods that take no query or matrix parameters. */
  private static final Set<String> PARAMETER_LESS_METHODS =
      Set.of(HTTP_METHOD_HEAD, HTTP_METHOD_OPTIONS);

  /** A valid HTTP method token, per RFC 9110 §5.6.2 (a {@code token} production). */
  private static final Pattern HTTP_METHOD_TOKEN = Pattern.compile("[!#$%&'*+\\-.^_`|~0-9A-Za-z]+");

  /** The default timeout until a connection is established (milliseconds) */
  public static final int DEFAULT_CONNECTION_TIMEOUT = 10000;

  /** The default timeout for waiting for reading data (milliseconds) */
  public static final int DEFAULT_READ_TIMEOUT = 10000;

  @HopMetadataProperty(
      key = "connection_name",
      injectionKey = "CONNECTION_NAME",
      hopMetadataPropertyType = HopMetadataPropertyType.REST_CONNECTION)
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

  /**
   * Stored inverted, so that "not stored" means preemptive. Issue #4196: the old {@code preemptive}
   * key was serialized and had a checkbox, but nothing ever read it — the credentials always went
   * out on the first request. Every pipeline ever saved therefore carries {@code
   * <preemptive>N</preemptive>}, the default of a control that did nothing, rather than a choice
   * anyone made. Reading that key now would flip all of them to challenge-response and break every
   * server that answers an unauthenticated request with something other than a 401.
   *
   * <p>So the old key is gone rather than repurposed: an existing pipeline loses a value that never
   * meant anything and keeps the behaviour it has always had. Read this through {@link
   * #isPreemptive()}.
   */
  @HopMetadataProperty(
      key = "non_preemptive_basic_auth",
      injectionKey = "NON_PREEMPTIVE_BASIC_AUTH")
  private boolean nonPreemptiveBasicAuth;

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

  /**
   * retry config retryTimes=0 retryDelayMs=500ms retryStatusCode=[429, 500, 502, 503, 504]
   * retryMethods=[post,get,delete,put,head,option,patch]
   */
  /*--------------------------------------------------------------------
  | retry config(retryTimes=0,retryDelayMs=500ms)
  | retryStatusCode=[429, 500, 502, 503, 504]
  | retryMethods=[get,delete,put]
  --------------------------------------------------------------------- */
  @HopMetadataProperty(key = "retryTimes", injectionKey = "RETRY_TIMES")
  private Integer retryTimes;

  @HopMetadataProperty(key = "retryDelayMs", injectionKey = "RETRY_DELAY_MS")
  private Long retryDelayMs;

  @HopMetadataProperty(
      key = "retryStatusCode",
      injectionKey = "RETRY_STATUS_CODE",
      groupKey = "retryStatusCodes",
      injectionGroupKey = "RETRY_STATUS_CODES")
  private List<String> retryStatusCodes;

  @HopMetadataProperty(
      key = "retryMethod",
      injectionKey = "RETRY_METHOD",
      groupKey = "retryMethods",
      injectionGroupKey = "RETRY_METHODS")
  private List<String> retryMethods;

  @HopMetadataProperty(key = "paginationEnabled", injectionKey = "PAGINATION_ENABLED")
  private boolean paginationEnabled;

  @HopMetadataProperty(key = "maxPagesLoops", injectionKey = "MAX_PAGES_LOOPS")
  private int maxPagesLoops;

  /**
   * Optional JsonPath ({@link #APPLICATION_TYPE_JSON}) or XPath ({@link #APPLICATION_TYPE_XML})
   * that selects an array or node-set; each matched element becomes one outgoing row instead of
   * buffering whole responses.
   */
  @HopMetadataProperty(key = "resultSplitPath", injectionKey = "RESULT_SPLIT_PATH")
  private String resultSplitPath;

  /**
   * Emit a row per record as the response arrives, instead of reading the whole body first (issue
   * #2746). For a response that is very large, or one that never ends, buffering it is either
   * wasteful or fatal.
   */
  @HopMetadataProperty(key = "streamingEnabled", injectionKey = "STREAMING_ENABLED")
  private boolean streamingEnabled;

  @HopMetadataProperty(key = "streamingFormat", injectionKey = "STREAMING_FORMAT")
  private RestStreamingFormat streamingFormat = RestStreamingFormat.NDJSON;

  /**
   * Optional output field for the SSE {@code event:} type. Named like every other optional output
   * on this transform: leave it empty and the column is not added at all. The record itself stays
   * in the result field rather than being wrapped in an envelope, so a payload that is already JSON
   * can go straight into a JSON Input transform without being unwrapped first.
   */
  @HopMetadataProperty(key = "streamingEventNameField", injectionKey = "STREAMING_EVENT_NAME_FIELD")
  private String streamingEventNameField;

  /** Optional output field for the SSE {@code id:} of each event. */
  @HopMetadataProperty(key = "streamingEventIdField", injectionKey = "STREAMING_EVENT_ID_FIELD")
  private String streamingEventIdField;

  public RestMeta() {
    super(); // allocate BaseTransformMeta
    headerFields = new ArrayList<>();
    parameterFields = new ArrayList<>();
    matrixParameterFields = new ArrayList<>();
    resultField = new ResultField();

    this.retryStatusCodes = new ArrayList<>();
    this.retryMethods = new ArrayList<>();
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
    // A new transform authenticates preemptively, which is what this transform has always done.
    this.nonPreemptiveBasicAuth = false;
    this.trustStoreFile = null;
    this.trustStorePassword = null;
    this.applicationType = APPLICATION_TYPE_TEXT_PLAIN;
    this.readTimeout = String.valueOf(DEFAULT_READ_TIMEOUT);
    this.connectionTimeout = String.valueOf(DEFAULT_CONNECTION_TIMEOUT);

    // retry config.
    this.retryTimes = RestConst.DEFAULT_RETRY_TIMES;
    this.retryDelayMs = RestConst.DEFAULT_RETRY_DELAY_MS;
    this.retryStatusCodes.addAll(RestConst.retryStatusCodes());
    this.retryMethods.addAll(RestConst.retryMethods());

    this.paginationEnabled = false;
    this.maxPagesLoops = RestConst.DEFAULT_MAX_PAGES_LOOPS;
    this.resultSplitPath = null;
    this.streamingEnabled = false;
    this.streamingFormat = RestStreamingFormat.NDJSON;
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
      // A binary result carries the response bytes verbatim; decoding them to a String would
      // corrupt any non-text payload (issue #3746).
      IValueMeta v =
          resultField.isBinary()
              ? new ValueMetaBinary(variables.resolve(resultField.getFieldName()))
              : new ValueMetaString(variables.resolve(resultField.getFieldName()));
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

    // Only when streaming: without it these would be columns that are always null.
    if (streamingEnabled) {
      String eventNameField = variables.resolve(streamingEventNameField);
      if (!Utils.isEmpty(eventNameField)) {
        IValueMeta v = new ValueMetaString(eventNameField);
        v.setOrigin(name);
        inputRowMeta.addValueMeta(v);
      }
      String eventIdField = variables.resolve(streamingEventIdField);
      if (!Utils.isEmpty(eventIdField)) {
        IValueMeta v = new ValueMetaString(eventIdField);
        v.setOrigin(name);
        inputRowMeta.addValueMeta(v);
      }
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

  /**
   * Whether Basic credentials go out on the first request rather than waiting for a 401 challenge.
   * This is the form the dialog and the transform work in; the metadata stores its opposite, see
   * {@link #nonPreemptiveBasicAuth}.
   */
  public boolean isPreemptive() {
    return !nonPreemptiveBasicAuth;
  }

  public void setPreemptive(boolean preemptive) {
    this.nonPreemptiveBasicAuth = !preemptive;
  }

  public static boolean isActiveBody(String method) {
    if (Utils.isEmpty(method)) {
      return false;
    }
    return !BODY_LESS_METHODS.contains(method);
  }

  public static boolean isActiveParameters(String method) {
    if (Utils.isEmpty(method)) {
      return false;
    }
    return !PARAMETER_LESS_METHODS.contains(method);
  }

  /**
   * Canonicalizes a method for use on the wire: trims it, and upper-cases it only when it names one
   * of the well-known verbs. HTTP method tokens are case-sensitive, so a custom verb is passed
   * through exactly as the user typed it.
   *
   * @param method the raw method, possibly null
   * @return the canonicalized method, or null if the input was null
   */
  public static String normalizeMethod(String method) {
    if (method == null) {
      return null;
    }
    String trimmed = method.trim();
    for (String known : HTTP_METHODS) {
      if (known.equalsIgnoreCase(trimmed)) {
        return known;
      }
    }
    return trimmed;
  }

  /**
   * Checks that a method is a valid HTTP method token as defined by RFC 9110 §5.6.2. This is
   * enforced because the method can come straight from an input field: a value containing spaces or
   * CR/LF would otherwise be spliced into the request line.
   *
   * @param method the method to validate
   * @return true if the method is a usable HTTP method token
   */
  public static boolean isValidMethodToken(String method) {
    return method != null && HTTP_METHOD_TOKEN.matcher(method).matches();
  }
}
