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

import static org.apache.hop.core.Const.NVL;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlParserFactoryProducer;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageHttpIoEmitter;
import org.apache.hop.lineage.model.HttpDirection;
import org.apache.hop.lineage.model.HttpLineagePayload;
import org.apache.hop.metadata.rest.RestConnection;
import org.apache.hop.metadata.rest.RestPaginationType;
import org.apache.hop.metadata.rest.client.RestAuthType;
import org.apache.hop.metadata.rest.client.RestAuthenticator;
import org.apache.hop.metadata.rest.client.RestClientFactory;
import org.apache.hop.metadata.rest.client.RestClientSettings;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.common.RestConst;
import org.json.simple.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class Rest extends BaseTransform<RestMeta, RestData> {
  private static final Class<?> PKG = RestMeta.class;
  public static final String CONST_REST_EXCEPTION_ERROR_FINDING_FIELD =
      "Rest.Exception.ErrorFindingField";
  private String baseUrl = "";
  private RestConnection connection;
  private RestClientSettings clientSettings;
  private RestAuthenticator authenticator;

  private static final Configuration JSON_PATH_CONFIGURATION =
      Configuration.builder()
          .options(Option.DEFAULT_PATH_LEAF_TO_NULL, Option.SUPPRESS_EXCEPTIONS)
          .build();

  private final ObjectMapper paginationJsonMapper = new ObjectMapper();

  /**
   * Outcome of one HTTP invocation (body, timing, serialization of headers needed for paging and
   * output fields).
   */
  protected static final class RestExchangeResult {
    final String body;

    /**
     * Raw response body, set instead of {@link #body} when the result field is Binary (issue
     * #3746). Paging is rejected in that case, so paging only ever reads {@link #body}.
     */
    final byte[] bodyBytes;

    final int status;
    final long responseTimeMs;
    final String headerJson;
    final Map<String, List<String>> headers;

    /** Effective request URL for this exchange (after paging merge), used for Link-header dedup. */
    final String requestUrl;

    /** SSE framing for this record, when streaming and the user asked for it. */
    String eventName;

    String eventId;

    RestExchangeResult(
        String body,
        byte[] bodyBytes,
        int status,
        long responseTimeMs,
        String headerJson,
        Map<String, List<String>> headers,
        String requestUrl) {
      this.body = body;
      this.bodyBytes = bodyBytes;
      this.status = status;
      this.responseTimeMs = responseTimeMs;
      this.headerJson = headerJson;
      this.headers = headers;
      this.requestUrl = requestUrl;
    }
  }

  /** Mutable paging token state for {@link RestPaginationType} semantics. */
  private static final class PaginationState {
    int pageNumber = 1;
    long offset = 0;

    /** Next full URL supplied by RFC 5988 Link header paging. */
    String linkNextUrl;

    String cursorToken;
    int effectiveLimit = 100;
  }

  public Rest(
      TransformMeta transformMeta,
      RestMeta meta,
      RestData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  /* for unit test*/
  Map<String, String> createMultivalueMap(String paramName, String paramValue) {
    Map<String, String> queryParams = new LinkedHashMap<>();
    queryParams.put(paramName, encodeQueryValue(paramValue));
    return queryParams;
  }

  /** Percent-encodes a value for use in a query string. */
  private static String encodeQueryValue(String value) {
    return URLEncoder.encode(NVL(value, ""), StandardCharsets.UTF_8);
  }

  /**
   * Percent-encodes a value for use inside a path segment, as matrix parameters are. The form
   * encoding of {@link #encodeQueryValue} would be wrong here: a {@code +} is a literal plus in a
   * path, not a space.
   */
  private static String encodePathValue(String value) {
    return encodeQueryValue(value).replace("+", "%20");
  }

  /** Resolves incoming-row URL substitution and HTTP method defaults for this row. */
  protected void applyDynamicRowUrlAndMethod(Object[] rowData) throws HopException {
    if (meta.isUrlInField()) {
      if (!Utils.isEmpty(data.connectionName)) {
        data.realUrl =
            resolveAgainstBase(baseUrl, data.inputRowMeta.getString(rowData, data.indexOfUrlField));
      } else {
        data.realUrl = data.inputRowMeta.getString(rowData, data.indexOfUrlField);
      }
    }

    if (meta.isDynamicMethod()) {
      data.method = checkMethod(data.inputRowMeta.getString(rowData, data.indexOfMethod));
    }
  }

  /**
   * Canonicalizes an HTTP method and rejects anything that is not a valid method token. Custom
   * verbs are allowed through (issue #4770), but a value carrying spaces or CR/LF is not: the
   * method may come straight from an input field, and such a value would be spliced into the
   * request line.
   */
  private String checkMethod(String rawMethod) throws HopException {
    String httpMethod = RestMeta.normalizeMethod(rawMethod);
    if (Utils.isEmpty(httpMethod)) {
      throw new HopException(BaseMessages.getString(PKG, "Rest.Error.MethodMissing"));
    }
    if (!RestMeta.isValidMethodToken(httpMethod)) {
      throw new HopException(BaseMessages.getString(PKG, "Rest.Error.InvalidMethod", httpMethod));
    }
    return httpMethod;
  }

  /** Whether paging can run without degrading back to legacy single-call semantics. */
  protected boolean supportsPaging() {
    return meta.isPaginationEnabled()
        && connection != null
        && connection.getPaginationType() != null
        && connection.getPaginationType() != RestPaginationType.NONE;
  }

  /**
   * Merges additional query-string parameters onto an absolute REST URL intended for paging tokens.
   */
  protected String mergePagingQueriesIntoResolvedUrl(
      String baseUrlResolved, LinkedHashMap<String, String> pagingQueries) {
    if (pagingQueries == null || pagingQueries.isEmpty()) {
      return baseUrlResolved;
    }
    try {
      URIBuilder ub = new URIBuilder(baseUrlResolved);
      for (Map.Entry<String, String> e : pagingQueries.entrySet()) {
        if (!Utils.isEmpty(e.getKey())) {
          ub.addParameter(e.getKey(), e.getValue() == null ? "" : e.getValue());
        }
      }
      return ub.build().toString();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Not a valid URL: " + baseUrlResolved, e);
    }
  }

  /**
   * Joins a REST connection's base URL with the transform's own URL value.
   *
   * <p>These used to be concatenated as raw strings, so a trailing or leading slash produced a
   * doubled separator, a missing one glued two path segments together, and an absolute URL in the
   * field yielded something malformed. A value that carries its own scheme is treated as absolute
   * and the base is ignored.
   */
  protected static String resolveAgainstBase(String base, String value) {
    String url = NVL(value, "");
    if (Utils.isEmpty(base) || hasScheme(url)) {
      return url;
    }
    if (url.isEmpty()) {
      return base;
    }
    boolean baseEndsWithSlash = base.endsWith("/");
    boolean valueStartsWithSlash = url.startsWith("/");
    if (baseEndsWithSlash && valueStartsWithSlash) {
      return base + url.substring(1);
    }
    if (!baseEndsWithSlash && !valueStartsWithSlash) {
      return base + "/" + url;
    }
    return base + url;
  }

  /**
   * True when the value is an absolute URL rather than a path to hang off the base URL. The scheme
   * has to be followed by {@code ://}: requiring only a colon would read {@code localhost:8080/x}
   * as scheme {@code localhost} instead of a host and port.
   */
  private static boolean hasScheme(String url) {
    int separator = url.indexOf("://");
    if (separator <= 0) {
      return false;
    }
    // A scheme is ALPHA *( ALPHA / DIGIT / "+" / "-" / "." ).
    if (!Character.isLetter(url.charAt(0))) {
      return false;
    }
    for (int i = 1; i < separator; i++) {
      char c = url.charAt(i);
      if (!Character.isLetterOrDigit(c) && c != '+' && c != '-' && c != '.') {
        return false;
      }
    }
    return true;
  }

  /**
   * Masks the value of security-sensitive headers so credentials are not written to the log when
   * header values are logged at debug level.
   */
  private static String maskHeaderValue(String name, String value) {
    if (name != null
        && ("Authorization".equalsIgnoreCase(name)
            || "Proxy-Authorization".equalsIgnoreCase(name)
            || "Cookie".equalsIgnoreCase(name))) {
      return "********";
    }
    return value;
  }

  /**
   * Executes a single REST exchange and bundles the deserialized pieces required for paging and row
   * assembly.
   */
  @SuppressWarnings("java:S5527")
  protected RestExchangeResult invokeRestExchange(
      Object[] rowData,
      String uriOverrideFull,
      LinkedHashMap<String, String> pagingQueries,
      LinkedHashMap<String, String> pagingBodyParams,
      LinkedHashMap<String, String> pagingHeaderParams)
      throws HopException {

    applyDynamicRowUrlAndMethod(rowData);

    String mergeBase = Utils.isEmpty(uriOverrideFull) ? data.realUrl : uriOverrideFull;
    LinkedHashMap<String, String> query =
        pagingQueries != null ? pagingQueries : new LinkedHashMap<>();
    String effectiveBase = mergePagingQueriesIntoResolvedUrl(mergeBase, query);

    long startTime;
    HttpExchange response;
    final long httpLineageT0 = System.currentTimeMillis();
    final long httpVolIn0 = dataVolumeIn != null ? dataVolumeIn : 0L;
    final long httpVolOut0 = dataVolumeOut != null ? dataVolumeOut : 0L;
    try {
      CloseableHttpClient client = getClient();
      if (isDetailed()) {
        if (connection != null) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "Rest.Log.UsingConnection", meta.getConnectionName(), NVL(baseUrl, "")));
        } else {
          logDetailed(BaseMessages.getString(PKG, "Rest.Log.NoConnection"));
        }
      }

      // One request for both paths. The connection used to build its own target internally, which
      // is why the matrix and query parameters had to be baked into the URL beforehand (#7621).
      String requestUri = buildRequestUri(effectiveBase, rowData);
      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG, "Rest.Log.Timeouts", data.realConnectionTimeout, data.realReadTimeout));
      }

      Map<String, String> headerMap = new LinkedHashMap<>();

      boolean acceptHeaderProvided = false;
      String contentType = null;
      if (data.useHeaders) {
        for (int i = 0; i < data.nrheader; i++) {
          String value = data.inputRowMeta.getString(rowData, data.indexOfHeaderFields[i]);

          // Content-Length is a restricted header computed by the HTTP client when the request
          // entity is buffered. Setting it manually throws "Content-Length already defined" on
          // some connectors, so we ignore any user-supplied value and let the client manage it
          // (issue #7621).
          if ("Content-Length".equalsIgnoreCase(data.headerNames[i])) {
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      "Rest.Log.HeaderValue",
                      data.headerNames[i],
                      maskHeaderValue(data.headerNames[i], value)));
            }
            continue;
          }

          headerMap.put(data.headerNames[i], value);
          if ("Content-Type".equals(data.headerNames[i])) {
            contentType = value;
          }
          if ("Accept".equalsIgnoreCase(data.headerNames[i])) {
            acceptHeaderProvided = true;
          }
          if (isDebug()) {
            logDebug(
                BaseMessages.getString(
                    PKG,
                    "Rest.Log.HeaderValue",
                    data.headerNames[i],
                    maskHeaderValue(data.headerNames[i], value)));
          }
        }
      }

      if (!acceptHeaderProvided
          && data.streaming
          && data.streamingFormat == RestStreamingFormat.SSE) {
        // An event-stream endpoint is entitled to answer something else, or nothing, when asked
        // for application/json. The user can still override this on the Headers tab.
        headerMap.put("Accept", "text/event-stream");
      } else if (!acceptHeaderProvided && data.mediaType != null) {
        headerMap.put("Accept", data.mediaType.getMimeType());
      }

      /* The single place authentication is applied, for both the connection and the transform's
       * own credentials. It has to happen against this map rather than the invocation builder,
       * A row that supplied its own Authorization keeps it. */
      authenticator().applyRequestHeaders(headerMap, requestUri);

      if (pagingHeaderParams != null && !pagingHeaderParams.isEmpty()) {
        for (Map.Entry<String, String> e : pagingHeaderParams.entrySet()) {
          if (!Utils.isEmpty(e.getKey())) {
            headerMap.put(e.getKey(), e.getValue() == null ? "" : e.getValue());
          }
        }
      }

      // The request entity is either a String or a byte[]. A Binary body field is passed on as raw
      // bytes: routing it through getString() decodes it with a charset and Jersey then re-encodes
      // it, which mangles every byte that is not valid in that charset (issue #3746).
      Object entity = null;
      if (data.useBody) {
        if (data.binaryBody) {
          entity = data.inputRowMeta.getBinary(rowData, data.indexOfBodyField);
          if (isDebug()) {
            logDebug(
                BaseMessages.getString(
                    PKG,
                    "Rest.Log.BinaryBodyValue",
                    entity == null ? 0 : ((byte[]) entity).length));
          }
        } else {
          entity = NVL(data.inputRowMeta.getString(rowData, data.indexOfBodyField), null);
          if (isDebug()) {
            logDebug(BaseMessages.getString(PKG, "Rest.Log.BodyValue", entity));
          }
        }
      }
      if (pagingBodyParams != null && !pagingBodyParams.isEmpty()) {
        if (data.binaryBody) {
          // Merging paging parameters means rewriting the body as JSON or form data, which cannot
          // be done to an opaque byte payload. Send the bytes unchanged and say so.
          logBasic(BaseMessages.getString(PKG, "Rest.Log.PagingBodyParamsIgnoredForBinaryBody"));
        } else {
          String contentTypeForMerge = contentType;
          if (Utils.isEmpty(contentTypeForMerge) && data.mediaType != null) {
            contentTypeForMerge = data.mediaType.toString();
          }
          entity =
              PagingBodyMerge.merge(
                  NVL((String) entity, ""), pagingBodyParams, contentTypeForMerge);
          if (isDebug()) {
            logDebug(BaseMessages.getString(PKG, "Rest.Log.BodyValue", entity));
          }
        }
      }

      final String finalRequestUri = requestUri;
      final Object finalEntity = entity;
      final String finalContentType = contentType;

      if (isDebug()) {
        logDebug(
            BaseMessages.getString(
                PKG,
                "Rest.Log.RequestContentType",
                contentType != null ? contentType : String.valueOf(data.mediaType)));
      }

      startTime = System.currentTimeMillis();
      response =
          executeWithRetry(
              () -> {
                try {
                  return executeRequest(
                      finalRequestUri, headerMap, finalEntity, finalContentType, rowData);
                } catch (HopException e) {
                  throw new HopRuntimeException(e);
                }
              });

      long responseTime = System.currentTimeMillis() - startTime;

      int status = response.status();
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "Rest.Log.ResponseCode", status));
        logDetailed(BaseMessages.getString(PKG, "Rest.Log.ResponseTime", responseTime, requestUri));
        if (status >= 400) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "Rest.Log.ResponseError", data.method, requestUri, status));
        }
      }

      Map<String, List<String>> headers = response.headers();
      String headerString = buildHeaderJson(headers);
      trackResponseBytes(response);

      // A binary result skips String decoding entirely: the bytes go into the row untouched.
      if (data.binaryResult) {
        byte[] bodyBytes = response.body();
        if (isRowLevel()) {
          logRowlevel(
              BaseMessages.getString(
                  PKG, "Rest.Log.BinaryResponseBody", bodyBytes == null ? 0 : bodyBytes.length));
        }
        emitHttpLineage(httpLineageT0, httpVolIn0, httpVolOut0, status, true, null);
        return new RestExchangeResult(
            null, bodyBytes, status, responseTime, headerString, headers, effectiveBase);
      }

      String body = new String(response.body(), resolveCharset(response.contentType()));
      if (isRowLevel()) {
        logRowlevel(BaseMessages.getString(PKG, "Rest.Log.ResponseBody", body));
      }

      emitHttpLineage(httpLineageT0, httpVolIn0, httpVolOut0, status, true, null);
      return new RestExchangeResult(
          body, null, status, responseTime, headerString, headers, effectiveBase);
    } catch (Exception e) {
      emitHttpLineage(httpLineageT0, httpVolIn0, httpVolOut0, null, false, e.getMessage());
      throw new HopException(
          BaseMessages.getString(PKG, "Rest.Error.CanNotReadURL", NVL(data.realUrl, effectiveBase)),
          e);
    }
    // The client is not closed here: it is shared by every row and released on dispose().
  }

  /**
   * A response, fully materialised. HttpClient5 releases the connection as soon as the exchange
   * returns, so the body has to be read before then rather than lazily like a JAX-RS Response.
   */
  private record HttpExchange(
      int status, byte[] body, Map<String, List<String>> headers, ContentType contentType) {}

  /** Reads an in-flight response into memory so it can outlive the exchange. */
  private static HttpExchange materialize(ClassicHttpResponse response) throws IOException {
    HttpEntity entity = response.getEntity();
    byte[] body = entity == null ? new byte[0] : EntityUtils.toByteArray(entity);
    ContentType contentType = null;
    if (entity != null && entity.getContentType() != null) {
      try {
        contentType = ContentType.parse(entity.getContentType());
      } catch (Exception ignored) {
        // Malformed Content-Type: fall back to the default charset below.
      }
    }
    Map<String, List<String>> headers = new LinkedHashMap<>();
    for (Header header : response.getHeaders()) {
      headers.computeIfAbsent(header.getName(), k -> new ArrayList<>()).add(header.getValue());
    }
    return new HttpExchange(response.getCode(), body, headers, contentType);
  }

  /** Applies the configured matrix and query parameters for this row to the request URL. */
  private String buildRequestUri(String base, Object[] rowData) throws HopException {
    StringBuilder matrix = new StringBuilder();
    if (data.useMatrixParams) {
      for (int i = 0; i < data.nrMatrixParams; i++) {
        String value = data.inputRowMeta.getString(rowData, data.indexOfMatrixParamFields[i]);
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG, "Rest.Log.matrixParameterValue", data.matrixParamNames[i], value));
        }
        matrix
            .append(';')
            .append(data.matrixParamNames[i])
            .append('=')
            .append(encodePathValue(value));
      }
    }
    // Matrix parameters belong to the last path segment, so they go in ahead of any query string.
    String withMatrix = base;
    if (matrix.length() > 0) {
      int query = base.indexOf('?');
      withMatrix =
          query < 0 ? base + matrix : base.substring(0, query) + matrix + base.substring(query);
    }
    try {
      URIBuilder builder = new URIBuilder(withMatrix);
      if (data.useParams) {
        for (int i = 0; i < data.nrParams; i++) {
          String value = data.inputRowMeta.getString(rowData, data.indexOfParamFields[i]);
          if (isDebug()) {
            logDebug(
                BaseMessages.getString(
                    PKG, "Rest.Log.queryParameterValue", data.paramNames[i], value));
          }
          builder.addParameter(data.paramNames[i], value);
        }
      }
      return builder.build().toString();
    } catch (URISyntaxException e) {
      throw new HopException("Not a valid URL: " + withMatrix, e);
    }
  }

  /**
   * Reads a streaming response, emitting one row per record as it arrives (issue #2746).
   *
   * <p>The whole point is not to hold the body: a bulk export can be larger than memory, and an
   * event feed never ends at all. The connection stays checked out for as long as the stream runs,
   * which is the trade this option makes.
   *
   * @return a summary exchange carrying the status and headers; its body is empty because the
   *     records have already gone downstream
   */
  private HttpExchange streamRecords(
      ClassicHttpResponse response, Object[] rowData, HttpUriRequestBase request)
      throws IOException {
    Map<String, List<String>> headers = new LinkedHashMap<>();
    for (Header header : response.getHeaders()) {
      headers.computeIfAbsent(header.getName(), k -> new ArrayList<>()).add(header.getValue());
    }
    int status = response.getCode();
    HttpEntity entity = response.getEntity();

    if (status < 200 || status >= 300) {
      // Not a silent zero-row success: without this the transform finishes green on a 401, a 404
      // or a 500, and there is nothing anywhere to say what happened.
      String body = "";
      if (entity != null) {
        try {
          body = EntityUtils.toString(entity, StandardCharsets.UTF_8);
        } catch (Exception ignored) {
          // Nothing readable; the status alone still has to surface.
        }
      }
      throw new IOException(
          "The streaming request failed with status "
              + status
              + (body.isBlank()
                  ? ""
                  : ": " + (body.length() > 500 ? body.substring(0, 500) + "..." : body)));
    }

    if (entity != null) {
      ContentType type = null;
      try {
        type = entity.getContentType() == null ? null : ContentType.parse(entity.getContentType());
      } catch (Exception ignored) {
        // Malformed Content-Type: fall back to UTF-8 below.
      }
      String headerJson = buildHeaderJson(headers);
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(entity.getContent(), resolveCharset(type)))) {
        if (data.streamingFormat == RestStreamingFormat.SSE) {
          readServerSentEvents(reader, rowData, status, headers, headerJson, request);
        } else {
          readNewlineDelimited(reader, rowData, status, headers, headerJson, request);
        }
      }
    }
    return new HttpExchange(status, new byte[0], headers, null);
  }

  /** One record per non-blank line. */
  private void readNewlineDelimited(
      BufferedReader reader,
      Object[] rowData,
      int status,
      Map<String, List<String>> headers,
      String headerJson,
      HttpUriRequestBase request)
      throws IOException {
    String line;
    while ((line = reader.readLine()) != null) {
      if (isStopped()) {
        abortStreaming(request);
        return;
      }
      if (!line.isBlank()) {
        emitStreamedRecord(line, rowData, status, headers, headerJson, null, null);
      }
    }
  }

  /**
   * WHATWG {@code text/event-stream}: a blank line ends an event, and the record is the joined
   * {@code data:} fields. The framing fields ({@code event}, {@code id}, {@code retry}) and comment
   * lines are skipped so that what lands in the row is the payload.
   */
  private void readServerSentEvents(
      BufferedReader reader,
      Object[] rowData,
      int status,
      Map<String, List<String>> headers,
      String headerJson,
      HttpUriRequestBase request)
      throws IOException {
    StringBuilder event = new StringBuilder();
    String eventName = null;
    String eventId = null;
    String line;
    while ((line = reader.readLine()) != null) {
      if (isStopped()) {
        abortStreaming(request);
        return;
      }
      if (line.isEmpty()) {
        if (event.length() > 0) {
          emitStreamedRecord(
              event.toString(), rowData, status, headers, headerJson, eventName, eventId);
          event.setLength(0);
          eventName = null;
        }
        // The id deliberately persists: the spec calls it the "last event ID", the point a
        // consumer would resume from. Only the type and the data reset per event.
        continue;
      }
      if (line.startsWith(":")) {
        continue; // a comment line, not a field
      }
      String field = line;
      String value = "";
      int colon = line.indexOf(':');
      if (colon >= 0) {
        field = line.substring(0, colon);
        value = line.substring(colon + 1);
        if (value.startsWith(" ")) {
          // The spec strips exactly one leading space after the colon.
          value = value.substring(1);
        }
      }
      switch (field) {
        case "data" -> {
          if (event.length() > 0) {
            event.append('\n');
          }
          event.append(value);
        }
        case "event" -> eventName = value;
        case "id" -> eventId = value;
        default -> {
          // "retry" tells a client how long to wait before reconnecting. Hop does not reconnect,
          // so it would only ever be a column holding the same number. Ignored on purpose.
        }
      }
    }
    if (event.length() > 0) {
      // A final event with no trailing blank line still counts.
      emitStreamedRecord(
          event.toString(), rowData, status, headers, headerJson, eventName, eventId);
    }
  }

  /**
   * Ends a stream the pipeline no longer wants. Simply returning from the read loop is not enough:
   * the client then tries to drain whatever is left of the entity so the connection can be reused,
   * and on a feed that never ends that never returns — the transform sits in "Halting" forever.
   * Aborting discards the connection instead.
   */
  private void abortStreaming(HttpUriRequestBase request) {
    if (request != null) {
      request.abort();
    }
  }

  private void emitStreamedRecord(
      String record,
      Object[] rowData,
      int status,
      Map<String, List<String>> headers,
      String headerJson,
      String eventName,
      String eventId)
      throws IOException {
    RestExchangeResult exchange =
        new RestExchangeResult(record, null, status, 0L, headerJson, headers, data.realUrl);
    exchange.eventName = eventName;
    exchange.eventId = eventId;
    try {
      Object[] outputRow = assembleResultRow(rowData == null ? null : rowData.clone(), exchange);
      putRow(data.outputRowMeta, outputRow);
    } catch (HopException e) {
      throw new IOException("Unable to emit a streamed record", e);
    }
  }

  /** Serializes the response headers to the JSON string exposed by the response-header field. */
  private String buildHeaderJson(Map<String, List<String>> headers) {
    JSONObject json = new JSONObject();
    for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
      String name = entry.getKey();
      List<String> value = entry.getValue();
      if (value.size() > 1) {
        json.put(name, value);
      } else {
        json.put(name, value.get(0));
      }
    }
    return json.toJSONString();
  }

  /**
   * Streaming reads the body once, as it arrives, which rules out the options that need the whole
   * body in hand. Saying so here beats a confusing empty result or a silently ignored setting.
   */
  private void rejectUnsupportedStreamingCombinations() throws HopException {
    if (!data.streaming) {
      return;
    }
    String conflict = null;
    if (supportsPaging()) {
      // Paging re-reads a response to find the next page, and re-requests; a consumed stream
      // cannot give either.
      conflict = "pagination";
    } else if (data.binaryResult) {
      conflict = "a binary result field";
    } else if (!Utils.isEmpty(meta.getResultSplitPath())) {
      // The split path is a JsonPath or XPath over a complete document.
      conflict = "a result split path";
    }
    if (conflict != null) {
      throw new HopException(
          BaseMessages.getString(PKG, "Rest.Exception.StreamingConflict", conflict));
    }
  }

  /**
   * Every option that takes its value from an incoming field needs an incoming field. Without a hop
   * there is nothing to read them from, so say which option is the problem rather than let the
   * field lookup fail later with "error finding field".
   */
  private void rejectFieldDrivenOptionsWithoutInput() throws HopException {
    if (data.readsRows) {
      return;
    }
    String option = null;
    if (meta.isUrlInField()) {
      option = "Accept URL from field";
    } else if (meta.isDynamicMethod()) {
      option = "Get Method from field";
    } else if (!Utils.isEmpty(meta.getBodyField())) {
      option = "Body field";
    } else if (!Utils.isEmpty(meta.getHeaderFields())) {
      option = "Headers";
    } else if (!Utils.isEmpty(meta.getParameterFields())) {
      option = "Query parameters";
    } else if (!Utils.isEmpty(meta.getMatrixParameterFields())) {
      option = "Matrix parameters";
    }
    if (option != null) {
      throw new HopException(
          BaseMessages.getString(PKG, "Rest.Exception.FieldOptionWithoutInput", option));
    }
  }

  protected Object[] assembleResultRow(Object[] baseRowMaybeNull, RestExchangeResult exchange)
      throws HopException {

    Object[] newRow = baseRowMaybeNull;
    int returnFieldsOffset = data.inputRowMeta.size();
    String body = exchange.body;
    int status = exchange.status;
    long responseTime = exchange.responseTimeMs;
    String headerString = exchange.headerJson;

    if (!Utils.isEmpty(data.resultFieldName)) {
      // The Binary result field carries the bytes as they arrived (issue #3746).
      newRow =
          RowDataUtil.addValueData(
              newRow, returnFieldsOffset, data.binaryResult ? exchange.bodyBytes : body);
      returnFieldsOffset++;
    }
    if (!Utils.isEmpty(data.resultCodeFieldName)) {
      newRow = RowDataUtil.addValueData(newRow, returnFieldsOffset, (long) status);
      returnFieldsOffset++;
    }
    if (!Utils.isEmpty(data.resultResponseFieldName)) {
      newRow = RowDataUtil.addValueData(newRow, returnFieldsOffset, responseTime);
      returnFieldsOffset++;
    }
    if (!Utils.isEmpty(data.resultHeaderFieldName)) {
      newRow = RowDataUtil.addValueData(newRow, returnFieldsOffset, headerString);
      returnFieldsOffset++;
    }

    // Same order as RestMeta.getFields declares them, or the values land in the wrong columns.
    if (data.streaming) {
      if (!Utils.isEmpty(data.streamingEventNameField)) {
        newRow = RowDataUtil.addValueData(newRow, returnFieldsOffset, exchange.eventName);
        returnFieldsOffset++;
      }
      if (!Utils.isEmpty(data.streamingEventIdField)) {
        newRow = RowDataUtil.addValueData(newRow, returnFieldsOffset, exchange.eventId);
      }
    }
    return newRow;
  }

  /**
   * Legacy single-exchange flow (one output row built from exactly one REST response unless paging
   * mode at {@link RestMeta#paginationEnabled}) is delegated through here by {@link #processRow()}
   * when paging is inactive.
   */
  @SuppressWarnings("java:S5527")
  protected Object[] callRest(Object[] rowData) throws HopException {
    RestExchangeResult exchange =
        invokeRestExchange(
            rowData, null, new LinkedHashMap<>(), new LinkedHashMap<>(), new LinkedHashMap<>());
    if (data.streaming) {
      // The rows went downstream as the response arrived; there is no summary row to add here.
      return null;
    }
    return assembleResultRow(rowData == null ? null : rowData.clone(), exchange);
  }

  private static String normalizeUrlForPagingDedup(String url) {
    if (Utils.isEmpty(url)) {
      return "";
    }
    try {
      return URI.create(url.trim()).normalize().toASCIIString();
    } catch (Exception e) {
      return url.trim();
    }
  }

  /** Follows paging instructions on the REST connection metadata for one incoming Hop row. */
  protected void runPaginationLoop(Object[] rowData) throws HopException {
    RestPaginationType pagingType =
        connection != null ? connection.getPaginationType() : RestPaginationType.NONE;
    PaginationState state = new PaginationState();
    state.effectiveLimit = resolvePagingLimit(connection);

    String resolvedSplitPath = resolveSplitPathOrPagingExpression(meta.getResultSplitPath());
    int maxLoops =
        meta.getMaxPagesLoops() > 0 ? meta.getMaxPagesLoops() : RestConst.DEFAULT_MAX_PAGES_LOOPS;

    LinkedHashSet<String> linkPagingFetchedUrls = new LinkedHashSet<>();

    applyDynamicRowUrlAndMethod(rowData);

    boolean hasMore = true;

    int loopIdx = 0;
    int totalEmitted = 0;
    RestExchangeResult firstExchange = null;
    while (hasMore && loopIdx < maxLoops) {
      String uriOv =
          usesLinkStylePaging(pagingType) ? (loopIdx == 0 ? null : state.linkNextUrl) : null;

      if (usesLinkStylePaging(pagingType) && uriOv != null) {
        String nextKey = normalizeUrlForPagingDedup(uriOv);
        if (linkPagingFetchedUrls.contains(nextKey)) {
          logBasic(BaseMessages.getString(PKG, "Rest.Log.LinkPaginationStoppedRepeatedUrl", uriOv));
          break;
        }
      }

      LinkedHashMap<String, String> pageQs = buildPagingQuery(connection, pagingType, state);
      LinkedHashMap<String, String> pageBody = buildPagingBody(connection, pagingType, state);
      LinkedHashMap<String, String> pageHeaders = buildPagingHeaders(connection, pagingType, state);

      RestExchangeResult ex = invokeRestExchange(rowData, uriOv, pageQs, pageBody, pageHeaders);
      logBasic(
          BaseMessages.getString(
              PKG, "Rest.Log.PaginationFetchedPage", loopIdx + 1, NVL(ex.requestUrl, "")));
      if (firstExchange == null) {
        firstExchange = ex;
      }
      if (usesLinkStylePaging(pagingType)) {
        String fetchedKey = normalizeUrlForPagingDedup(ex.requestUrl);
        if (!linkPagingFetchedUrls.add(fetchedKey)) {
          logBasic(
              BaseMessages.getString(
                  PKG, "Rest.Log.LinkPaginationStoppedRepeatedUrl", NVL(ex.requestUrl, "")));
          break;
        }
      }
      totalEmitted += emitPagedResultRows(rowData, resolvedSplitPath, ex);

      hasMore =
          continuePaginationAfterExchange(connection, pagingType, state, resolvedSplitPath, ex);
      loopIdx++;
      if (!hasMore || loopIdx >= maxLoops && isDetailed()) {
        if (loopIdx >= maxLoops && hasMore && isDetailed()) {
          logDetailed(
              "REST pagination stopped after safeguard maxPagesLoops=" + maxLoops + " iterations.");
        }
      }
    }

    enforcePagingSplitEmittedSomething(resolvedSplitPath, firstExchange, totalEmitted);
  }

  private static int resolvePagingLimit(RestConnection connection) {
    if (connection == null) {
      return 100;
    }
    RestPaginationType type = connection.getPaginationType();
    if (RestPaginationType.OFFSET_LIMIT.equals(type)
        || RestPaginationType.BODY_CURSOR.equals(type)
        || RestPaginationType.HEADER_CURSOR.equals(type)) {
      int lim = connection.getDefaultLimit();
      return lim > 0 ? lim : 100;
    }
    return 100;
  }

  private LinkedHashMap<String, String> buildPagingQuery(
      RestConnection conn, RestPaginationType type, PaginationState state) {
    LinkedHashMap<String, String> q = new LinkedHashMap<>();
    if (conn == null || type == null || RestPaginationType.NONE.equals(type)) {
      return q;
    }
    switch (type) {
      case OFFSET_LIMIT -> {
        String offKey = NVL(resolve(conn.getOffsetParamName()), "offset");
        String limKey = NVL(resolve(conn.getLimitParamName()), "limit");
        q.put(offKey, Long.toString(state.offset));
        q.put(limKey, Integer.toString(state.effectiveLimit));
      }
      case PAGE_NUMBER ->
          q.put(NVL(resolve(conn.getPageParamName()), "page"), Integer.toString(state.pageNumber));
      case CURSOR -> {
        if (!Utils.isEmpty(state.cursorToken)) {
          q.put(NVL(resolve(conn.getPageParamName()), "cursor"), state.cursorToken);
        }
      }
      case BODY_CURSOR -> appendCursorBatchToQueryIfGet(conn, state, q);
      case HEADER_CURSOR, LINK_HEADER, BODY_NEXT_URL, NONE -> {
        /* header, URL, or body carries paging tokens */
      }
      default -> {
        /* exhaustive */
      }
    }
    return q;
  }

  private LinkedHashMap<String, String> buildPagingBody(
      RestConnection conn, RestPaginationType type, PaginationState state) {
    LinkedHashMap<String, String> body = new LinkedHashMap<>();
    if (conn == null || type == null || !RestPaginationType.BODY_CURSOR.equals(type)) {
      return body;
    }
    if (isGetPagingMethod()) {
      return body;
    }
    appendCursorBatchParams(conn, state, body);
    return body;
  }

  private LinkedHashMap<String, String> buildPagingHeaders(
      RestConnection conn, RestPaginationType type, PaginationState state) {
    LinkedHashMap<String, String> headers = new LinkedHashMap<>();
    if (conn == null || type == null || !RestPaginationType.HEADER_CURSOR.equals(type)) {
      return headers;
    }
    appendCursorBatchParams(conn, state, headers);
    return headers;
  }

  private void appendCursorBatchToQueryIfGet(
      RestConnection conn, PaginationState state, LinkedHashMap<String, String> query) {
    if (!isGetPagingMethod()) {
      return;
    }
    appendCursorBatchParams(conn, state, query);
  }

  private void appendCursorBatchParams(
      RestConnection conn, PaginationState state, LinkedHashMap<String, String> target) {
    if (state.effectiveLimit > 0) {
      target.put(
          NVL(resolve(conn.getLimitParamName()), "limit"), Integer.toString(state.effectiveLimit));
    }
    if (!Utils.isEmpty(state.cursorToken)) {
      target.put(NVL(resolve(conn.getPageParamName()), "cursor"), state.cursorToken);
    }
  }

  private boolean isGetPagingMethod() {
    return RestMeta.HTTP_METHOD_GET.equals(NVL(data.method, ""));
  }

  private static boolean usesLinkStylePaging(RestPaginationType pagingType) {
    return RestPaginationType.LINK_HEADER.equals(pagingType)
        || RestPaginationType.BODY_NEXT_URL.equals(pagingType);
  }

  private boolean continuePaginationAfterExchange(
      RestConnection conn,
      RestPaginationType pagingType,
      PaginationState state,
      String resolvedSplitPath,
      RestExchangeResult ex)
      throws HopException {

    if (conn == null || pagingType == null || RestPaginationType.NONE.equals(pagingType)) {
      return false;
    }

    boolean httpOk = ex.status >= 200 && ex.status < 300;
    boolean hadPayload = exchangeHadPayload(ex, resolvedSplitPath);

    if (!httpOk) {
      return false;
    }

    switch (pagingType) {
      case LINK_HEADER -> {
        state.linkNextUrl = extractRelNextUri(ex.headers);
        return !Utils.isEmpty(state.linkNextUrl);
      }
      case BODY_NEXT_URL -> {
        state.linkNextUrl = extractNextPageUrlFromBody(conn, ex.body).orElse(null);
        return !Utils.isEmpty(state.linkNextUrl);
      }
      case OFFSET_LIMIT -> {
        if (!hadPayload) {
          return false;
        }
        state.offset += state.effectiveLimit;
        return true;
      }
      case PAGE_NUMBER -> {
        if (!hadPayload) {
          return false;
        }
        state.pageNumber++;
        return true;
      }
      case CURSOR -> {
        if (!hadPayload) {
          return false;
        }
        return extractCursorForNext(conn, ex.body)
            .map(
                tok -> {
                  state.cursorToken = tok;
                  return true;
                })
            .orElse(false);
      }
      case BODY_CURSOR, HEADER_CURSOR -> {
        return extractCursorForNext(conn, ex.body)
            .map(
                tok -> {
                  state.cursorToken = tok;
                  return true;
                })
            .orElse(false);
      }
      case NONE -> {
        return false;
      }
      default -> {
        return false;
      }
    }
  }

  /**
   * @return whether this response appears to carry at least one item (whole body when no
   *     split-path, or extracted elements when splitting).
   */
  private boolean exchangeHadPayload(RestExchangeResult ex, String resolvedSplitPath)
      throws HopException {
    List<String> items =
        Utils.isEmpty(resolvedSplitPath) ? null : splitResultItems(ex.body, resolvedSplitPath);
    if (items != null) {
      return !items.isEmpty();
    }
    return !Utils.isEmpty(NVL(ex.body, "").trim());
  }

  /** Uses RFC 5988 / RFC 8288 Web Link parsing ({@code LinkHeaderPaging}) for {@code rel=next}. */
  private static String extractRelNextUri(Map<String, List<String>> headers) {
    return LinkHeaderPaging.findFirstUriWithRelNext(headers);
  }

  /**
   * Resolves %%VAR%% and ${VAR} for JsonPath / XPath paging expressions without applying Hop's hex
   * notation ($[hh],…) from {@link StringUtil#environmentSubstitute}, which would corrupt common
   * JsonPath literals such as "$[*]" (treated as a malformed hex escape).
   */
  private String resolveSplitPathOrPagingExpression(String literal) {
    if (literal == null) {
      return null;
    }
    if (Utils.isEmpty(literal)) {
      return literal;
    }
    Map<String, String> map = substitutionMapSnapshot();
    String afterWindows = StringUtil.substituteWindows(literal, map);
    return StringUtil.substituteUnix(afterWindows, map);
  }

  private HashMap<String, String> substitutionMapSnapshot() {
    HashMap<String, String> map = new HashMap<>();
    for (String name : getVariableNames()) {
      String v = getVariable(name);
      if (v != null) {
        map.put(name, v);
      }
    }
    return map;
  }

  /**
   * Paging plus result split emits nothing for many failure modes (wrong JsonPath, error JSON body,
   * auth failures returning objects, etc.). Failing loudly avoids silent pipelines with zero
   * output.
   *
   * <p>Legitimate empty-collection cases (no rows, no error): HTTP 2xx with a top-level {@code []},
   * or JSON where the split path points at an existing but empty array (e.g. Shopify {@code
   * {"orders":[]}} with split {@code $.orders[*]}).
   */
  private void enforcePagingSplitEmittedSomething(
      String resolvedSplitPath, RestExchangeResult firstExchange, int totalEmitted)
      throws HopException {
    if (!meta.isPaginationEnabled() || Utils.isEmpty(resolvedSplitPath)) {
      return;
    }
    if (totalEmitted > 0) {
      return;
    }
    if (firstExchange == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "Rest.Error.PagingSplitProducedZeroRowsNoResponse"));
    }
    boolean httpOk = firstExchange.status >= 200 && firstExchange.status < 300;
    String bodyTrim = NVL(firstExchange.body, "").trim();
    if (firstExchange.status == 401 || firstExchange.status == 403) {
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "Rest.Error.PagingAuthFailed",
              Integer.toString(firstExchange.status),
              abbreviateBodyForLog(firstExchange.body, 900)));
    }
    if (httpOk && isLegitimateEmptyPagedSplit(bodyTrim, resolvedSplitPath)) {
      logBasic(
          BaseMessages.getString(
              PKG, "Rest.Log.PagingEmptyCollectionFirstPage", NVL(resolvedSplitPath, "")));
      return;
    }

    throw new HopException(
        BaseMessages.getString(
            PKG,
            "Rest.Error.PagingSplitProducedZeroRows",
            Integer.toString(firstExchange.status),
            NVL(resolvedSplitPath, ""),
            abbreviateBodyForLog(firstExchange.body, 900)));
  }

  /**
   * True when HTTP succeeded but the split path targets an empty collection (not a missing/wrong
   * path). Supports top-level {@code []} and wrapped arrays such as {@code {"orders":[]}}.
   */
  boolean isLegitimateEmptyPagedSplit(String bodyTrim, String resolvedSplitPath) {
    if (Utils.isEmpty(bodyTrim) || Utils.isEmpty(resolvedSplitPath)) {
      return false;
    }
    if ("[]".equals(bodyTrim)) {
      return true;
    }
    // Response may be JSON even when the request Content-Type is form-urlencoded (e.g. Slack).
    if (!shouldParseResponseAsJson(
        NVL(resolve(meta.getApplicationType()), ""), resolvedSplitPath)) {
      return false;
    }
    try {
      String parentPath = resolvedSplitPath.replaceAll("\\[\\*\\]\\s*$", "");
      String jsonPathToProbe =
          parentPath.equals(resolvedSplitPath) ? resolvedSplitPath : parentPath;
      Object atPath = JsonPath.using(JSON_PATH_CONFIGURATION).parse(bodyTrim).read(jsonPathToProbe);
      if (atPath == null) {
        return false;
      }
      if (atPath instanceof Iterable<?> iterable) {
        return !iterable.iterator().hasNext();
      }
      // Single object at path without [*] — zero rows means nothing to split, still valid.
      return true;
    } catch (Exception ignored) {
      return false;
    }
  }

  /**
   * Whether a response body should be parsed as JSON for pagination/split. Application type is the
   * <em>request</em> Content-Type; APIs such as Slack POST form bodies and still return JSON.
   * JsonPath expressions (starting with {@code $}) imply a JSON response regardless of request
   * type.
   */
  static boolean shouldParseResponseAsJson(String applicationType, String pathOrExpression) {
    if (RestMeta.APPLICATION_TYPE_JSON.equals(NVL(applicationType, ""))) {
      return true;
    }
    return pathOrExpression != null && pathOrExpression.trim().startsWith("$");
  }

  private int emitPagedResultRows(
      Object[] rowTemplate, String resolvedSplitPath, RestExchangeResult ex) throws HopException {

    String splitPathResolved = Utils.isEmpty(resolvedSplitPath) ? null : resolvedSplitPath;
    if (splitPathResolved == null) {
      putRow(data.outputRowMeta, assembleResultRow(rowTemplate.clone(), ex));
      return 1;
    }

    List<String> chunks = splitResultItems(ex.body, splitPathResolved);
    if (chunks.isEmpty()) {
      return 0;
    }

    int emitted = 0;
    for (String sliceBody : chunks) {
      RestExchangeResult slice =
          new RestExchangeResult(
              sliceBody,
              null,
              ex.status,
              ex.responseTimeMs,
              ex.headerJson,
              ex.headers,
              ex.requestUrl);
      putRow(data.outputRowMeta, assembleResultRow(rowTemplate.clone(), slice));
      emitted++;
    }
    return emitted;
  }

  private List<String> splitResultItems(String body, String jsonOrXPathExpr) throws HopException {
    String appType = NVL(resolve(meta.getApplicationType()), "");

    List<String> out = new ArrayList<>();
    try {
      // Prefer JsonPath when the expression is JsonPath or the request type is JSON. Request
      // Content-Type (e.g. FORM URLENCODED) does not dictate the response payload format.
      if (shouldParseResponseAsJson(appType, jsonOrXPathExpr)) {
        Object raw =
            JsonPath.using(JSON_PATH_CONFIGURATION).parse(NVL(body, "{}")).read(jsonOrXPathExpr);
        appendStructuredJsonPieces(out, raw);
        return out;
      }
      if (RestMeta.APPLICATION_TYPE_XML.equals(appType)
          || RestMeta.APPLICATION_TYPE_TEXT_XML.equals(appType)
          || RestMeta.APPLICATION_TYPE_ATOM_XML.equals(appType)
          || RestMeta.APPLICATION_TYPE_SVG_XML.equals(appType)
          || RestMeta.APPLICATION_TYPE_XHTML.equals(appType)) {

        DocumentBuilderFactory df = documentBuilderFactory();
        DocumentBuilder db = df.newDocumentBuilder();
        Document doc;
        try (StringReader sr = new StringReader(NVL(body, ""))) {
          doc = db.parse(new InputSource(sr));
        }
        XPathFactory xpf = XPathFactory.newInstance();
        XPath xp = xpf.newXPath();
        XPathExpression expr = xp.compile(jsonOrXPathExpr);
        NodeList nl = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);

        TransformerFactory tf = TransformerFactory.newInstance();
        try {
          tf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        } catch (Exception ignored) {
          // Older JDKs ignore unknown features gracefully.
        }
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");

        for (int i = 0; i < nl.getLength(); i++) {
          StringWriter sw = new StringWriter();
          transformer.transform(new DOMSource(nl.item(i)), new StreamResult(sw));
          String piece = sw.toString();
          if (!Utils.isEmpty(piece.trim())) {
            out.add(piece);
          }
        }
        return out;
      }

      throw new HopException(
          "REST resultSplitPath requires a JsonPath (starting with $) or XML application type, got '"
              + appType
              + "'.");

    } catch (HopException he) {
      throw he;
    } catch (Exception e) {
      throw new HopException("Unable to split REST response payload", e);
    }
  }

  private DocumentBuilderFactory documentBuilderFactory() throws Exception {
    DocumentBuilderFactory dbf = XmlParserFactoryProducer.createSecureDocBuilderFactory();
    dbf.setNamespaceAware(true);
    return dbf;
  }

  private void appendStructuredJsonPieces(List<String> out, Object raw) throws Exception {
    if (raw == null) {
      return;
    }
    if (raw instanceof Iterable<?> it) {
      for (Object elem : it) {
        stringifyJsonPiece(out, elem);
      }
      return;
    }
    stringifyJsonPiece(out, raw);
  }

  private void stringifyJsonPiece(List<String> out, Object o) throws Exception {
    if (o == null) {
      return;
    }
    try {
      out.add(paginationJsonMapper.writeValueAsString(o));
    } catch (Exception e) {
      out.add(o.toString());
    }
  }

  private java.util.Optional<String> extractCursorForNext(RestConnection conn, String body) {
    if (conn == null) {
      return java.util.Optional.empty();
    }
    String appType = NVL(resolve(meta.getApplicationType()), "");
    try {
      String cursorJsonPath = conn.getCursorJsonPath();
      if (shouldParseResponseAsJson(appType, resolve(cursorJsonPath))
          && !Utils.isEmpty(cursorJsonPath)) {
        Object raw =
            JsonPath.using(JSON_PATH_CONFIGURATION)
                .parse(NVL(body, "{}"))
                .read(resolve(cursorJsonPath));
        if (raw == null) {
          return java.util.Optional.empty();
        }
        String s = raw.toString().trim();
        return s.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(s);
      }

      if (RestMeta.APPLICATION_TYPE_XML.equals(appType)
          || RestMeta.APPLICATION_TYPE_TEXT_XML.equals(appType)
          || RestMeta.APPLICATION_TYPE_ATOM_XML.equals(appType)) {

        if (Utils.isEmpty(conn.getCursorXPath())) {
          return java.util.Optional.empty();
        }
        DocumentBuilderFactory df = documentBuilderFactory();
        Document doc;
        try (StringReader sr = new StringReader(NVL(body, ""))) {
          doc = df.newDocumentBuilder().parse(new InputSource(sr));
        }
        XPathFactory xpf = XPathFactory.newInstance();
        XPathExpression expr =
            xpf.newXPath().compile(resolveSplitPathOrPagingExpression(conn.getCursorXPath()));
        String val = expr.evaluate(doc);
        String s = val == null ? "" : val.trim();
        return s.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(s);
      }
    } catch (Exception ignored) {
      /* JsonPath suppressed option may yield null upstream */
    }
    return java.util.Optional.empty();
  }

  private java.util.Optional<String> extractNextPageUrlFromBody(RestConnection conn, String body) {
    if (conn == null) {
      return java.util.Optional.empty();
    }
    String appType = NVL(resolve(meta.getApplicationType()), "");
    try {
      String nextPageUrlJsonPath = conn.getNextPageUrlJsonPath();
      if (shouldParseResponseAsJson(appType, resolve(nextPageUrlJsonPath))
          && !Utils.isEmpty(nextPageUrlJsonPath)) {
        Object raw =
            JsonPath.using(JSON_PATH_CONFIGURATION)
                .parse(NVL(body, "{}"))
                .read(resolveSplitPathOrPagingExpression(nextPageUrlJsonPath));
        return normalizeNextPageUrl(raw);
      }

      if (RestMeta.APPLICATION_TYPE_XML.equals(appType)
          || RestMeta.APPLICATION_TYPE_TEXT_XML.equals(appType)
          || RestMeta.APPLICATION_TYPE_ATOM_XML.equals(appType)) {
        if (Utils.isEmpty(conn.getNextPageUrlXPath())) {
          return java.util.Optional.empty();
        }
        DocumentBuilderFactory df = documentBuilderFactory();
        Document doc;
        try (StringReader sr = new StringReader(NVL(body, ""))) {
          doc = df.newDocumentBuilder().parse(new InputSource(sr));
        }
        XPathFactory xpf = XPathFactory.newInstance();
        XPathExpression expr =
            xpf.newXPath().compile(resolveSplitPathOrPagingExpression(conn.getNextPageUrlXPath()));
        return normalizeNextPageUrl(expr.evaluate(doc));
      }
    } catch (Exception ignored) {
      /* suppressed JsonPath / XPath failures */
    }
    return java.util.Optional.empty();
  }

  private static java.util.Optional<String> normalizeNextPageUrl(Object raw) {
    if (raw == null) {
      return java.util.Optional.empty();
    }
    String s = raw.toString().trim();
    return s.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(s);
  }

  private HttpExchange executeWithRetry(Supplier<HttpExchange> requestSupplier)
      throws HopException {
    int maxRetries =
        meta.getRetryTimes() != null ? meta.getRetryTimes() : RestConst.DEFAULT_RETRY_TIMES;
    long baseDelay =
        meta.getRetryDelayMs() != null ? meta.getRetryDelayMs() : RestConst.DEFAULT_RETRY_DELAY_MS;
    List<String> retryMethods = meta.getRetryMethods();
    Exception lastException = null;

    if (retryMethods == null || retryMethods.isEmpty() || !retryMethods.contains(data.method)) {
      return requestSupplier.get();
    }

    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      HttpExchange response = null;
      try {
        response = requestSupplier.get();
        int status = response.status();

        if (!shouldRetry(String.valueOf(status))) {
          return response;
        }

        logRetry(attempt, maxRetries, status);
        if (attempt >= maxRetries) {
          throw new HopException("Request failed after retries, status: " + status);
        }
      } catch (Exception e) {
        lastException = e;
        if (attempt == maxRetries) {
          throw new HopException("Request failed after retries", e);
        }
      }

      // Nothing to release: the exchange was materialised into memory before the connection
      // was returned to the pool.
      sleepBeforeRetry(attempt, baseDelay);
    }

    throw new HopException("Request failed after retries", lastException);
  }

  /**
   * Sleeps for a computed backoff duration before performing the next retry.
   *
   * @param attempt the current retry attempt, starting from 0
   * @param delay the base delay in milliseconds
   */
  private void sleepBeforeRetry(int attempt, long delay) {
    try {
      Thread.sleep(computeRetryDelay(attempt, delay));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Determines whether the current request should be retried.
   *
   * @param status the HTTP response status code as a string
   * @return {@code true} if the request should be retried, {@code false} otherwise
   */
  private boolean shouldRetry(String status) {
    return meta.getRetryStatusCodes().contains(status);
  }

  /** Logs retry attempt information at detailed log level. */
  private void logRetry(int attempt, int maxRetries, int status) {
    if (isDetailed()) {
      logDetailed(
          "Retry rest {0}/{1}, status: {2}, method: {3}",
          attempt + 1, maxRetries, status, meta.getMethod());
    }
  }

  /**
   * Computes the retry delay using an exponential backoff strategy with equal jitter.
   *
   * <pre>
   *   delay = 100
   *   attempt = 0 → delay     -> 50 ~ 149 ms
   *   attempt = 1 → delay * 2 -> 100 ~ 199 ms
   *   attempt = 2 → delay * 4 -> 200 ~ 299 ms
   * </pre>
   *
   * @param attempt the current retry attempt, starting from 0
   * @param delay the base delay in milliseconds
   * @return the computed retry delay in milliseconds
   */
  private long computeRetryDelay(int attempt, long delay) {
    long maxDelay = 30_000;

    long expDelay = delay * (1L << attempt);
    long capped = Math.min(expDelay, maxDelay);

    long jitter = ThreadLocalRandom.current().nextLong(delay);
    return capped / 2 + jitter;
  }

  /**
   * Issues the request. The entity is either a String or a byte[]; a byte[] goes to the wire
   * untouched, so a binary body survives (issue #3746).
   *
   * <p>Any verb is passed through verbatim (issue #4770): HttpClient5 writes the method token
   * straight into the request line, so LIST, PURGE, PROPFIND and friends need nothing special.
   */
  private HttpExchange executeRequest(
      String requestUri,
      Map<String, String> headers,
      Object entity,
      String contentType,
      Object[] rowData)
      throws HopException {
    // A body-less POST/PUT/PATCH/DELETE must still carry a Content-Length header (issue #7621),
    // so a null body becomes an empty entity rather than no entity at all.
    Object body = entity == null ? (data.binaryBody ? new byte[0] : "") : entity;
    try {
      HttpUriRequestBase request = new HttpUriRequestBase(data.method, URI.create(requestUri));
      headers.forEach(request::addHeader);

      if (RestMeta.isActiveBody(data.method)) {
        ContentType type = contentType != null ? ContentType.parse(contentType) : data.mediaType;
        trackRequestBytes(body, resolveCharset(type));
        request.setEntity(
            body instanceof byte[] bytes
                ? new ByteArrayEntity(bytes, type)
                : new StringEntity((String) body, type));
      }

      if (isDetailed()) {
        logDetailed(describeRequest(request, body));
      }

      if (data.streaming) {
        // Rows are emitted from inside the response handler, while the connection is still open.
        try {
          return data.client.execute(
              request, response -> streamRecords(response, rowData, request));
        } catch (Exception e) {
          if (isStopped()) {
            // The abort below is how a stop actually takes effect, and it surfaces here as a
            // failure. Expected, not an error: report what was read and let the transform end.
            return new HttpExchange(0, new byte[0], new LinkedHashMap<>(), null);
          }
          throw e;
        }
      }
      return data.client.execute(request, Rest::materialize);
    } catch (Exception e) {
      throw new HopException("Request could not be processed", e);
    }
  }

  /**
   * Renders the request the way a browser console or Postman would: the request line, every header,
   * then the body. The per-header and per-parameter lines elsewhere in this class say what was
   * configured; this says what actually goes out, in one block you can read or paste. It replaces
   * the one-line "Sending [method] request to [url]" that used to be logged here, which this is a
   * superset of.
   *
   * <p>It is built from the {@code HttpUriRequestBase} rather than from the transform's own fields,
   * so it also shows the headers Hop adds for you — the {@code Authorization} an attached REST
   * connection contributes, {@code Accept}, {@code Content-Type}, a paging cursor header — none of
   * which appear anywhere else in the log.
   *
   * <p>Credentials are masked. Debug logging routinely ends up in tickets and CI output, and a
   * verbatim dump of an {@code Authorization} header is a credential leak.
   */
  /** Enough to read a request by, without pouring a multi-megabyte upload into the log per row. */
  private static final int MAX_LOGGED_BODY_CHARS = 4096;

  private String describeRequest(HttpUriRequestBase request, Object body) {
    StringBuilder text = new StringBuilder(256);
    text.append(BaseMessages.getString(PKG, "Rest.Log.FullRequest")).append(Const.CR);
    text.append(request.getMethod()).append(' ').append(request.getRequestUri()).append(Const.CR);

    // Host and Content-Type never appear in getHeaders(): the client derives the first from the
    // route and the second from the entity, both at send time. Leaving them out would make this a
    // misleading picture of the request rather than a faithful one.
    if (request.getAuthority() != null) {
      text.append("Host: ").append(request.getAuthority().toString()).append(Const.CR);
    }
    HttpEntity requestEntity = request.getEntity();
    if (requestEntity != null && requestEntity.getContentType() != null) {
      text.append("Content-Type: ").append(requestEntity.getContentType()).append(Const.CR);
    }

    for (Header header : request.getHeaders()) {
      text.append(header.getName())
          .append(": ")
          .append(maskHeaderValue(header.getName(), header.getValue()))
          .append(Const.CR);
    }

    if (RestMeta.isActiveBody(data.method)) {
      text.append(Const.CR);
      if (body instanceof byte[] bytes) {
        // Binary bodies are not text and must not be decoded just to be logged (issue #3746).
        text.append(BaseMessages.getString(PKG, "Rest.Log.FullRequest.BinaryBody", bytes.length));
      } else {
        String text0 = String.valueOf(body);
        if (text0.length() > MAX_LOGGED_BODY_CHARS) {
          text.append(text0, 0, MAX_LOGGED_BODY_CHARS)
              .append(
                  BaseMessages.getString(
                      PKG, "Rest.Log.FullRequest.BodyTruncated", text0.length()));
        } else {
          text.append(text0);
        }
      }
    }
    return text.toString();
  }

  private void emitHttpLineage(
      long startedAt,
      long volumeInBefore,
      long volumeOutBefore,
      Integer statusCode,
      boolean success,
      String message) {
    long reqDelta = (dataVolumeOut != null ? dataVolumeOut : 0L) - volumeOutBefore;
    long respDelta = (dataVolumeIn != null ? dataVolumeIn : 0L) - volumeInBefore;
    String url = data.realUrl;
    if (Utils.isEmpty(url)) {
      url = null;
    }
    LineageHttpIoEmitter.emitTransformHttpIo(
        this,
        new HttpLineagePayload(
            HttpDirection.CLIENT,
            data.method,
            url,
            statusCode,
            reqDelta > 0 ? reqDelta : null,
            respDelta > 0 ? respDelta : null,
            System.currentTimeMillis() - startedAt,
            success,
            message));
  }

  /**
   * Counts the request payload for lineage. A byte[] entity is measured directly: re-encoding it
   * through a charset would report the wrong size as well as corrupt it (issue #3746).
   */
  private void trackRequestBytes(Object entity, Charset charset) {
    if (entity instanceof byte[] bytes) {
      if (bytes.length > 0) {
        dataVolumeOut = (dataVolumeOut != null ? dataVolumeOut : 0L) + bytes.length;
      }
      return;
    }
    trackRequestBytes((String) entity, charset);
  }

  private void trackRequestBytes(String entityString, Charset charset) {
    if (entityString == null) {
      return;
    }

    byte[] requestBytes = entityString.getBytes(charset);
    if (requestBytes.length > 0) {
      dataVolumeOut = (dataVolumeOut != null ? dataVolumeOut : 0L) + requestBytes.length;
    }
  }

  private void trackResponseBytes(HttpExchange response) {
    long responseBytes = response.body().length;
    if (responseBytes > 0) {
      dataVolumeIn = (dataVolumeIn != null ? dataVolumeIn : 0L) + responseBytes;
    }
  }

  private Charset resolveCharset(String mediaTypeValue) {
    if (!Utils.isEmpty(mediaTypeValue)) {
      try {
        return resolveCharset(ContentType.parse(mediaTypeValue));
      } catch (Exception ignored) {
        // Fall back to UTF-8 below if the header value is malformed.
      }
    }
    return StandardCharsets.UTF_8;
  }

  private Charset resolveCharset(ContentType mediaType) {
    if (mediaType != null && mediaType.getCharset() != null) {
      return mediaType.getCharset();
    }
    return StandardCharsets.UTF_8;
  }

  private static String abbreviateBodyForLog(String body, int maxChars) {
    String s = NVL(body, "").replaceAll("\\s+", " ").trim();
    if (s.length() <= maxChars) {
      return s.isEmpty() ? "(empty)" : s;
    }
    return s.substring(0, maxChars) + "...";
  }

  /**
   * Resolves the client configuration from whichever source describes this transform: the selected
   * REST connection, or the transform's own fields.
   */
  protected RestClientSettings createClientSettings() throws HopException {
    if (connection != null) {
      warnAboutSupersededFields();
      return connection.createClientSettings();
    }
    return createTransformClientSettings();
  }

  /**
   * A selected REST connection describes the whole client, so the transform's own connection fields
   * are ignored. They are not cleared — the connection can be deselected again — but a pipeline
   * that set them before is entitled to know they stopped being read.
   */
  private void warnAboutSupersededFields() {
    List<String> ignored = new ArrayList<>();
    if (!Utils.isEmpty(meta.getProxyHost())) {
      ignored.add("proxy");
    }
    if (!Utils.isEmpty(meta.getHttpLogin()) || !Utils.isEmpty(meta.getHttpPassword())) {
      ignored.add("HTTP authentication");
    }
    if (!Utils.isEmpty(meta.getTrustStoreFile()) || meta.isIgnoreSsl()) {
      ignored.add("SSL");
    }
    if (!Utils.isEmpty(meta.getConnectionTimeout()) || !Utils.isEmpty(meta.getReadTimeout())) {
      ignored.add("timeouts");
    }
    if (!ignored.isEmpty()) {
      logBasic(
          BaseMessages.getString(
              PKG,
              "Rest.Log.ConnectionSupersedesTransformFields",
              data.connectionName,
              String.join(", ", ignored)));
    }
  }

  /** Client configuration taken from the transform's own fields, with no connection selected. */
  private RestClientSettings createTransformClientSettings() throws HopException {
    RestClientSettings settings = new RestClientSettings();

    // Only apply a timeout that is actually configured. An empty field resolves to -1, which used
    // to be passed straight through here while the connection path left it unset; both now mean
    // the same thing, which is Jersey's default of no timeout.
    if (data.realConnectionTimeout >= 0) {
      settings.setConnectTimeout(data.realConnectionTimeout);
    }
    if (data.realReadTimeout >= 0) {
      settings.setReadTimeout(data.realReadTimeout);
    }

    // PROXY CONFIGURATION
    if (!Utils.isEmpty(data.realProxyHost)) {
      settings.setProxyHost(data.realProxyHost);
      settings.setProxyPort(data.realProxyPort);
    }

    // HTTP BASIC AUTHENTICATION
    if (!Utils.isEmpty(data.realHttpLogin) || !Utils.isEmpty(data.realHttpPassword)) {
      settings.setAuthType(RestAuthType.BASIC);
      settings.setBasicUsername(data.realHttpLogin);
      settings.setBasicPassword(data.realHttpPassword);
      settings.setBasicPreemptive(meta.isPreemptive());
      // Only a static URL gives an origin to bind the credentials to. With the URL coming from an
      // input field and no base URL, there is nothing to check against, and the credentials go to
      // whatever host the row names — which is what this transform has always done.
      if (!meta.isUrlInField()) {
        // Not data.realUrl: that is only resolved once the first row arrives, and the settings are
        // built in init().
        settings.setAuthOrigin(resolve(meta.getUrl()));
      }
    }

    // SSL TRUST STORE CONFIGURATION
    if (meta.isIgnoreSsl()) {
      settings.setSslContext(trustAllSslContext());
    } else if (!Utils.isEmpty(data.trustStoreFile)) {
      settings.setSslContext(trustStoreSslContext());
    }
    if (settings.getSslContext() != null) {
      settings.setPermissiveHostnameVerifier(true);
    }
    return settings;
  }

  /**
   * The HTTP client for this transform copy, created on first use. A client is bound to a
   * configuration rather than to a URL, so the same one serves every row even when the endpoint
   * comes from an input field.
   */
  protected CloseableHttpClient getClient() throws HopException {
    if (data.client == null) {
      data.client = createClient();
    }
    return data.client;
  }

  /** Overridable so tests can supply a client without reaching the network. */
  protected CloseableHttpClient createClient() throws HopException {
    return RestClientFactory.createClient(clientSettings());
  }

  /** The resolved client configuration, built once per transform copy. */
  private RestClientSettings clientSettings() throws HopException {
    if (clientSettings == null) {
      clientSettings = createClientSettings();
    }
    return clientSettings;
  }

  /**
   * The authenticator that goes with the resolved configuration. It has its own accessor rather
   * than being set while the client is built: a caller that supplies its own client would otherwise
   * never get one.
   */
  private RestAuthenticator authenticator() throws HopException {
    if (authenticator == null) {
      authenticator = new RestAuthenticator(clientSettings());
    }
    return authenticator;
  }

  private SSLContext trustAllSslContext() throws HopException {
    try {
      return HttpClientManager.getTrustAllSslContext();
    } catch (NoSuchAlgorithmException e) {
      throw new HopException(BaseMessages.getString(PKG, "Rest.Error.NoSuchAlgorithm"), e);
    } catch (KeyManagementException e) {
      throw new HopException(BaseMessages.getString(PKG, "Rest.Error.KeyManagementException"), e);
    }
  }

  private SSLContext trustStoreSslContext() throws HopException {
    try (FileInputStream trustFileStream = new FileInputStream(data.trustStoreFile)) {
      return HttpClientManager.getSslContextWithTrustStoreFile(
          trustFileStream, data.trustStorePassword);
    } catch (NoSuchAlgorithmException e) {
      throw new HopException(BaseMessages.getString(PKG, "Rest.Error.NoSuchAlgorithm"), e);
    } catch (KeyStoreException e) {
      throw new HopException(BaseMessages.getString(PKG, "Rest.Error.KeyStoreException"), e);
    } catch (CertificateException e) {
      throw new HopException(BaseMessages.getString(PKG, "Rest.Error.CertificateException"), e);
    } catch (FileNotFoundException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "Rest.Error.FileNotFound", data.trustStoreFile), e);
    } catch (IOException e) {
      throw new HopException(BaseMessages.getString(PKG, "Rest.Error.IOException"), e);
    } catch (KeyManagementException e) {
      throw new HopException(BaseMessages.getString(PKG, "Rest.Error.KeyManagementException"), e);
    }
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r;

    if (data.readsRows) {
      r = getRow(); // Get row from input rowset & set row busy!
      if (r == null) {
        // no more input to be expected...
        setOutputDone();
        return false;
      }
    } else {
      // No incoming hop: the transform is a starting point, so it makes its request once against
      // an empty row rather than not at all.
      r = RowDataUtil.allocateRowData(0);
      incrementLinesRead();
    }

    if (first) {
      first = false;
      data.inputRowMeta = data.readsRows ? getInputRowMeta() : new RowMeta();
      rejectFieldDrivenOptionsWithoutInput();
      rejectUnsupportedStreamingCombinations();
      data.outputRowMeta = data.inputRowMeta.clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // Let's set URL
      if (meta.isUrlInField()) {
        if (Utils.isEmpty(meta.getUrlField())) {
          logError(BaseMessages.getString(PKG, "Rest.Log.NoField"));
          throw new HopException(BaseMessages.getString(PKG, "Rest.Log.NoField"));
        }
        // cache the position of the field
        if (data.indexOfUrlField < 0) {
          String realUrlfieldName = resolve(meta.getUrlField());
          data.indexOfUrlField = data.inputRowMeta.indexOfValue(realUrlfieldName);
          if (data.indexOfUrlField < 0) {
            // The field is unreachable !
            throw new HopException(
                BaseMessages.getString(
                    PKG, CONST_REST_EXCEPTION_ERROR_FINDING_FIELD, realUrlfieldName));
          }
        }
      } else {
        // Static URL
        if (!Utils.isEmpty(data.connectionName)) {
          data.realUrl = resolveAgainstBase(baseUrl, resolve(meta.getUrl()));
        } else {
          data.realUrl = resolve(meta.getUrl());
        }
      }
      // Check Method
      if (meta.isDynamicMethod()) {
        String field = resolve(meta.getMethodFieldName());
        if (Utils.isEmpty(field)) {
          throw new HopException(BaseMessages.getString(PKG, "Rest.Exception.MethodFieldMissing"));
        }
        data.indexOfMethod = data.inputRowMeta.indexOfValue(field);
        if (data.indexOfMethod < 0) {
          // The field is unreachable !
          throw new HopException(
              BaseMessages.getString(PKG, CONST_REST_EXCEPTION_ERROR_FINDING_FIELD, field));
        }
      }
      // set Headers
      if (!Utils.isEmpty(meta.getHeaderFields())) {
        data.nrheader = meta.getHeaderFields().size();
        data.indexOfHeaderFields = new int[meta.getHeaderFields().size()];
        data.headerNames = new String[meta.getHeaderFields().size()];
        for (int i = 0; i < meta.getHeaderFields().size(); i++) {
          // split into body / header
          data.headerNames[i] = resolve(meta.getHeaderFields().get(i).getName());
          String field = resolve(meta.getHeaderFields().get(i).getHeaderField());
          if (Utils.isEmpty(field)) {
            throw new HopException(BaseMessages.getString(PKG, "Rest.Exception.HeaderFieldEmpty"));
          }
          data.indexOfHeaderFields[i] = data.inputRowMeta.indexOfValue(field);
          if (data.indexOfHeaderFields[i] < 0) {
            throw new HopException(
                BaseMessages.getString(PKG, CONST_REST_EXCEPTION_ERROR_FINDING_FIELD, field));
          }
        }
        data.useHeaders = true;
      }
      if (RestMeta.isActiveParameters(meta.getMethod())) {
        // Parameters
        int nrparams = meta.getParameterFields() == null ? 0 : meta.getParameterFields().size();
        if (nrparams > 0) {
          data.nrParams = nrparams;
          data.paramNames = new String[nrparams];
          data.indexOfParamFields = new int[nrparams];
          for (int i = 0; i < nrparams; i++) {
            data.paramNames[i] = resolve(meta.getParameterFields().get(i).getName());
            String field = resolve(meta.getParameterFields().get(i).getHeaderField());
            if (Utils.isEmpty(field)) {
              throw new HopException(BaseMessages.getString(PKG, "Rest.Exception.ParamFieldEmpty"));
            }
            data.indexOfParamFields[i] = data.inputRowMeta.indexOfValue(field);
            if (data.indexOfParamFields[i] < 0) {
              throw new HopException(
                  BaseMessages.getString(PKG, CONST_REST_EXCEPTION_ERROR_FINDING_FIELD, field));
            }
          }
          data.useParams = true;
        }
        int nrmatrixparams =
            meta.getMatrixParameterFields() == null ? 0 : meta.getMatrixParameterFields().size();
        if (nrmatrixparams > 0) {
          data.nrMatrixParams = nrmatrixparams;
          data.matrixParamNames = new String[nrmatrixparams];
          data.indexOfMatrixParamFields = new int[nrmatrixparams];
          for (int i = 0; i < nrmatrixparams; i++) {
            data.matrixParamNames[i] = resolve(meta.getMatrixParameterFields().get(i).getName());
            String field = resolve(meta.getMatrixParameterFields().get(i).getHeaderField());
            if (Utils.isEmpty(field)) {
              throw new HopException(
                  BaseMessages.getString(PKG, "Rest.Exception.MatrixParamFieldEmpty"));
            }
            data.indexOfMatrixParamFields[i] = data.inputRowMeta.indexOfValue(field);
            if (data.indexOfMatrixParamFields[i] < 0) {
              throw new HopException(
                  BaseMessages.getString(PKG, CONST_REST_EXCEPTION_ERROR_FINDING_FIELD, field));
            }
          }
          data.useMatrixParams = true;
        }
      }

      // Do we need to set body
      if (RestMeta.isActiveBody(meta.getMethod())) {
        String field = resolve(meta.getBodyField());
        if (!Utils.isEmpty(field)) {
          data.indexOfBodyField = data.inputRowMeta.indexOfValue(field);
          if (data.indexOfBodyField < 0) {
            throw new HopException(
                BaseMessages.getString(PKG, CONST_REST_EXCEPTION_ERROR_FINDING_FIELD, field));
          }
          data.useBody = true;
          // Keyed off the declared type, not the storage type: Hop's lazy conversion gives plain
          // String fields a binary storage type, and those must keep going through getString().
          data.binaryBody =
              data.inputRowMeta.getValueMeta(data.indexOfBodyField).getType()
                  == IValueMeta.TYPE_BINARY;
          if (data.binaryBody && isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "Rest.Log.BinaryBodyField", field));
          }
        }
      }
    } // end if first
    try {
      if (supportsPaging()) {
        runPaginationLoop(r);
      } else {
        if (meta.isPaginationEnabled() && isDetailed()) {
          logDetailed(
              "REST pagination is configured on this transform but is inactive "
                  + "(needs a REST connection with a non-NONE pagination type). Using legacy single-request behaviour.");
        }
        Object[] outputRowData = callRest(r);
        if (outputRowData != null) {
          // Null means the rows were already emitted while the response streamed in.
          putRow(data.outputRowMeta, outputRowData);
        }
      }
      if (checkFeedback(getLinesRead()) && isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "Rest.LineNumber") + getLinesRead());
      }
    } catch (HopException e) {
      boolean sendToErrorRow = false;
      String errorMessage = null;
      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(BaseMessages.getString(PKG, "Rest.ErrorInTransformRunning") + e.getMessage());
        setErrors(1);
        logError(Const.getStackTracker(e));
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(data.inputRowMeta, r, 1, errorMessage, null, "Rest001");
      }
    }

    if (!data.readsRows) {
      // One request, one output row, done.
      setOutputDone();
      return false;
    }
    return true;
  }

  @Override
  public boolean init() {

    if (super.init()) {

      // Decided from the pipeline layout rather than from what arrives at runtime: a hop that
      // happens to carry zero rows must stay a no-op, while no hop at all means this transform
      // starts the work itself.
      data.readsRows = !Utils.isEmpty(getPipelineMeta().findPreviousTransforms(getTransformMeta()));

      // use the information from the selection line if we have one.
      data.connectionName = resolve(meta.getConnectionName());
      if (!Utils.isEmpty(data.connectionName)) {
        try {
          this.connection =
              metadataProvider.getSerializer(RestConnection.class).load(data.connectionName);
          if (this.connection != null) {
            this.connection.setVariables(this);
          }
          baseUrl = resolve(connection.getBaseUrl());

        } catch (Exception e) {
          throw new HopRuntimeException(
              "REST connection " + meta.getConnectionName() + " could not be found");
        }
      }

      data.resultFieldName = resolve(meta.getResultField().getFieldName());
      data.resultCodeFieldName = resolve(meta.getResultField().getCode());
      data.resultResponseFieldName = resolve(meta.getResultField().getResponseTime());
      data.resultHeaderFieldName = resolve(meta.getResultField().getResponseHeader());
      data.binaryResult = meta.getResultField().isBinary();
      data.streaming = meta.isStreamingEnabled();
      data.streamingFormat =
          meta.getStreamingFormat() == null
              ? RestStreamingFormat.NDJSON
              : meta.getStreamingFormat();
      data.streamingEventNameField = resolve(meta.getStreamingEventNameField());
      data.streamingEventIdField = resolve(meta.getStreamingEventIdField());

      // Paging needs to read the response body as text to find the next page token, split the
      // result or follow a Link header. None of that is possible on an opaque byte payload.
      if (data.binaryResult && meta.isPaginationEnabled()) {
        logError(BaseMessages.getString(PKG, "Rest.Error.BinaryResultWithPagination"));
        return false;
      }

      data.realConnectionTimeout = Const.toInt(resolve(meta.getConnectionTimeout()), -1);
      data.realReadTimeout = Const.toInt(resolve(meta.getReadTimeout()), -1);

      // get authentication settings once
      data.realProxyHost = resolve(meta.getProxyHost());
      data.realProxyPort = Const.toInt(resolve(meta.getProxyPort()), 8080);
      data.realHttpLogin = resolve(meta.getHttpLogin());
      data.realHttpPassword =
          Encr.decryptPasswordOptionallyEncrypted(resolve(meta.getHttpPassword()));

      if (!meta.isDynamicMethod()) {
        try {
          data.method = checkMethod(resolve(meta.getMethod()));
        } catch (HopException e) {
          logError(e.getMessage());
          return false;
        }
      }

      data.trustStoreFile = resolve(meta.getTrustStoreFile());
      data.trustStorePassword = resolve(meta.getTrustStorePassword());

      String applicationType = NVL(meta.getApplicationType(), "");
      switch (applicationType) {
        case RestMeta.APPLICATION_TYPE_XML -> data.mediaType = ContentType.APPLICATION_XML;
        case RestMeta.APPLICATION_TYPE_JSON -> data.mediaType = ContentType.APPLICATION_JSON;
        case RestMeta.APPLICATION_TYPE_OCTET_STREAM ->
            data.mediaType = ContentType.APPLICATION_OCTET_STREAM;
        case RestMeta.APPLICATION_TYPE_XHTML -> data.mediaType = ContentType.APPLICATION_XHTML_XML;
        case RestMeta.APPLICATION_TYPE_FORM_URLENCODED ->
            data.mediaType = ContentType.APPLICATION_FORM_URLENCODED;
        case RestMeta.APPLICATION_TYPE_ATOM_XML ->
            data.mediaType = ContentType.APPLICATION_ATOM_XML;
        case RestMeta.APPLICATION_TYPE_SVG_XML -> data.mediaType = ContentType.APPLICATION_SVG_XML;
        case RestMeta.APPLICATION_TYPE_TEXT_XML -> data.mediaType = ContentType.TEXT_XML;
        default -> data.mediaType = ContentType.TEXT_PLAIN;
      }
      try {
        // Resolve the client configuration now so a bad trust store or an unreadable connection
        // fails the transform at startup rather than on the first row.
        clientSettings();
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "Rest.Error.Config"), e);
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  public void dispose() {
    if (data.client != null) {
      try {
        data.client.close();
      } catch (IOException e) {
        logDebug("Error closing the REST client", e);
      }
      data.client = null;
    }
    clientSettings = null;
    data.headerNames = null;
    data.indexOfHeaderFields = null;
    data.paramNames = null;
    super.dispose();
  }
}
