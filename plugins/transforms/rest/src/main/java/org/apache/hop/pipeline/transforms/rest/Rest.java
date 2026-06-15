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
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageHttpIoEmitter;
import org.apache.hop.lineage.model.HttpDirection;
import org.apache.hop.lineage.model.HttpLineagePayload;
import org.apache.hop.metadata.rest.RestConnection;
import org.apache.hop.metadata.rest.RestPaginationType;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.common.RestConst;
import org.glassfish.jersey.apache5.connector.Apache5ConnectorProvider;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.HttpUrlConnectorProvider;
import org.glassfish.jersey.client.RequestEntityProcessing;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.uri.UriComponent;
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
    final int status;
    final long responseTimeMs;
    final String headerJson;
    final MultivaluedMap<String, Object> headers;

    /** Effective request URL for this exchange (after paging merge), used for Link-header dedup. */
    final String requestUrl;

    RestExchangeResult(
        String body,
        int status,
        long responseTimeMs,
        String headerJson,
        MultivaluedMap<String, Object> headers,
        String requestUrl) {
      this.body = body;
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

  protected ClientBuilder createClientBuilder() {
    return ClientBuilder.newBuilder();
  }

  /* for unit test*/
  MultivaluedHashMap createMultivalueMap(String paramName, String paramValue) {
    MultivaluedHashMap queryParams = new MultivaluedHashMap();
    queryParams.add(paramName, UriComponent.encode(paramValue, UriComponent.Type.QUERY_PARAM));
    return queryParams;
  }

  /** Resolves incoming-row URL substitution and HTTP method defaults for this row. */
  protected void applyDynamicRowUrlAndMethod(Object[] rowData) throws HopException {
    if (meta.isUrlInField()) {
      if (!Utils.isEmpty(data.connectionName)) {
        data.realUrl = baseUrl + data.inputRowMeta.getString(rowData, data.indexOfUrlField);
      } else {
        data.realUrl = data.inputRowMeta.getString(rowData, data.indexOfUrlField);
      }
    }

    if (meta.isDynamicMethod()) {
      data.method = data.inputRowMeta.getString(rowData, data.indexOfMethod);
      if (Utils.isEmpty(data.method)) {
        throw new HopException(BaseMessages.getString(PKG, "Rest.Error.MethodMissing"));
      }
    }
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
    UriBuilder ub = UriBuilder.fromUri(URI.create(baseUrlResolved));
    for (Map.Entry<String, String> e : pagingQueries.entrySet()) {
      if (!Utils.isEmpty(e.getKey())) {
        ub.queryParam(e.getKey(), e.getValue() == null ? "" : e.getValue());
      }
    }
    return ub.build().toString();
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

    WebTarget webResource = null;
    Client client = null;
    Invocation.Builder invocationBuilder = null;
    long startTime;
    Response response;
    final long httpLineageT0 = System.currentTimeMillis();
    final long httpVolIn0 = dataVolumeIn != null ? dataVolumeIn : 0L;
    final long httpVolOut0 = dataVolumeOut != null ? dataVolumeOut : 0L;
    try {
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "Rest.Log.ConnectingToURL", effectiveBase));
      }
      if (!StringUtils.isEmpty(meta.getConnectionName())) {
        invocationBuilder =
            connection.getInvocationBuilder(effectiveBase, data.realProxyHost, data.realProxyPort);
      } else {
        ClientBuilder clientBuilder = createClientBuilder();
        clientBuilder
            .withConfig(data.config)
            .property(HttpUrlConnectorProvider.SET_METHOD_WORKAROUND, true);

        if (meta.isIgnoreSsl() || !Utils.isEmpty(data.trustStoreFile)) {
          clientBuilder.hostnameVerifier((s1, s2) -> true);
          clientBuilder.sslContext(data.sslContext);
        }

        client = clientBuilder.build();
        if (data.basicAuthentication != null) {
          client.register(data.basicAuthentication);
        }
        webResource = client.target(effectiveBase);

        if (data.useMatrixParams) {
          UriBuilder builder = webResource.getUriBuilder();
          for (int i = 0; i < data.nrMatrixParams; i++) {
            String value = data.inputRowMeta.getString(rowData, data.indexOfMatrixParamFields[i]);
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "Rest.Log.matrixParameterValue", data.matrixParamNames[i], value));
            }
            builder =
                builder.matrixParam(
                    data.matrixParamNames[i],
                    UriComponent.encode(value, UriComponent.Type.QUERY_PARAM));
          }
          webResource = client.target(builder.build());
        }

        if (data.useParams) {
          for (int i = 0; i < data.nrParams; i++) {
            String value = data.inputRowMeta.getString(rowData, data.indexOfParamFields[i]);
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "Rest.Log.queryParameterValue", data.paramNames[i], value));
            }
            webResource = webResource.queryParam(data.paramNames[i], value);
          }
        }
        if (isDebug()) {
          logDebug(BaseMessages.getString(PKG, "Rest.Log.ConnectingToURL", webResource.getUri()));
        }
        invocationBuilder = webResource.request();
      }
      if (invocationBuilder == null) {
        throw new HopException("Invocation builder not initialized");
      }

      MultivaluedMap<String, Object> headerMap = new MultivaluedHashMap<>();
      addApiKeyHeaderIfAbsent(headerMap);

      boolean acceptHeaderProvided = false;
      String contentType = null;
      if (data.useHeaders) {
        for (int i = 0; i < data.nrheader; i++) {
          String value = data.inputRowMeta.getString(rowData, data.indexOfHeaderFields[i]);

          headerMap.putSingle(data.headerNames[i], value);
          if ("Content-Type".equals(data.headerNames[i])) {
            contentType = value;
          }
          if ("Accept".equalsIgnoreCase(data.headerNames[i])) {
            acceptHeaderProvided = true;
          }
          if (isDebug()) {
            logDebug(
                BaseMessages.getString(PKG, "Rest.Log.HeaderValue", data.headerNames[i], value));
          }
        }
      }

      if (!acceptHeaderProvided && data.mediaType != null) {
        headerMap.putSingle("Accept", data.mediaType);
      }

      /* Jersey Invocation.Builder.headers(MultivaluedMap) replaces all headers — so Bearer / API-key
       * set inside RestConnection.getInvocationBuilder(...) would be stripped. Merge connection auth
       * into outbound headers unless the caller already supplied Authorization. */
      if (!StringUtils.isEmpty(meta.getConnectionName()) && connection != null) {
        connection.applyBearerAndApiKeyHeaders(invocationBuilder, headerMap);
      }

      if (pagingHeaderParams != null && !pagingHeaderParams.isEmpty()) {
        for (Map.Entry<String, String> e : pagingHeaderParams.entrySet()) {
          if (!Utils.isEmpty(e.getKey())) {
            headerMap.putSingle(e.getKey(), e.getValue() == null ? "" : e.getValue());
          }
        }
      }

      String entityString = null;
      if (data.useBody) {
        entityString = NVL(data.inputRowMeta.getString(rowData, data.indexOfBodyField), null);
        if (isDebug()) {
          logDebug(BaseMessages.getString(PKG, "Rest.Log.BodyValue", entityString));
        }
      }
      if (pagingBodyParams != null && !pagingBodyParams.isEmpty()) {
        String contentTypeForMerge = contentType;
        if (Utils.isEmpty(contentTypeForMerge) && data.mediaType != null) {
          contentTypeForMerge = data.mediaType.toString();
        }
        entityString =
            PagingBodyMerge.merge(NVL(entityString, ""), pagingBodyParams, contentTypeForMerge);
        if (isDebug()) {
          logDebug(BaseMessages.getString(PKG, "Rest.Log.BodyValue", entityString));
        }
      }

      invocationBuilder.headers(headerMap);
      final Invocation.Builder finalInvocationBuilder = invocationBuilder;
      final String finalEntityString = entityString;
      final String finalContentType = contentType;

      startTime = System.currentTimeMillis();
      response =
          executeWithRetry(
              () -> {
                try {
                  return executeRequest(
                      finalInvocationBuilder, finalEntityString, finalContentType);
                } catch (HopException e) {
                  throw new HopRuntimeException(e);
                }
              });

      if (response != null) {
        response.bufferEntity();
      }
      long responseTime = System.currentTimeMillis() - startTime;

      int status = response.getStatus();

      if (response.hasEntity()) {
        response.bufferEntity();
      }

      String body;
      try {
        body = response.readEntity(String.class);
      } catch (ProcessingException ex) {
        String errorMessage = ex.getMessage();
        if (errorMessage != null
            && errorMessage.contains("Too many \"Content-Type\" header values")) {
          throw new HopException(
              BaseMessages.getString(
                  PKG, "Rest.Error.DuplicateContentType", effectiveBase, errorMessage),
              ex);
        }
        body = "";
        if (response.hasEntity()) {
          try (InputStream stream = response.readEntity(InputStream.class)) {
            if (stream != null) {
              body = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            }
          } catch (Exception ioEx) {
            if (isDetailed()) {
              logDetailed("Unable to read response entity as String", ioEx);
            }
            throw new HopException(
                BaseMessages.getString(PKG, "Rest.Error.CanNotReadResponse", effectiveBase), ex);
          }
        }
      } catch (Exception ex) {
        body = "";
        if (response.hasEntity()) {
          try (InputStream stream = response.readEntity(InputStream.class)) {
            if (stream != null) {
              body = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            }
          } catch (Exception ioEx) {
            if (isDetailed()) {
              logDetailed("Unable to read response entity as String", ioEx);
            }
            throw new HopException(
                BaseMessages.getString(PKG, "Rest.Error.CanNotReadResponse", effectiveBase), ex);
          }
        }
      }
      trackResponseBytes(response, body);

      MultivaluedMap<String, Object> headers = searchForHeaders(response);
      JSONObject json = new JSONObject();
      for (Map.Entry<String, List<Object>> entry : headers.entrySet()) {
        String name = entry.getKey();
        List<Object> value = entry.getValue();
        if (value.size() > 1) {
          json.put(name, value);
        } else {
          json.put(name, value.get(0));
        }
      }
      String headerString = json.toJSONString();

      emitHttpLineage(httpLineageT0, httpVolIn0, httpVolOut0, status, true, null);
      return new RestExchangeResult(
          body, status, responseTime, headerString, headers, effectiveBase);
    } catch (Exception e) {
      emitHttpLineage(httpLineageT0, httpVolIn0, httpVolOut0, null, false, e.getMessage());
      throw new HopException(
          BaseMessages.getString(PKG, "Rest.Error.CanNotReadURL", NVL(data.realUrl, effectiveBase)),
          e);
    } finally {
      if (client != null) {
        client.close();
      }
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
      newRow = RowDataUtil.addValueData(newRow, returnFieldsOffset, body);
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
  private static String extractRelNextUri(MultivaluedMap<String, Object> headers) {
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
    if (!RestMeta.APPLICATION_TYPE_JSON.equals(NVL(resolve(meta.getApplicationType()), ""))) {
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
              sliceBody, ex.status, ex.responseTimeMs, ex.headerJson, ex.headers, ex.requestUrl);
      putRow(data.outputRowMeta, assembleResultRow(rowTemplate.clone(), slice));
      emitted++;
    }
    return emitted;
  }

  private List<String> splitResultItems(String body, String jsonOrXPathExpr) throws HopException {
    String appType = NVL(resolve(meta.getApplicationType()), "");

    List<String> out = new ArrayList<>();
    try {
      if (RestMeta.APPLICATION_TYPE_JSON.equals(appType)) {
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
          "REST resultSplitPath requires application type XML or JSON, got '" + appType + "'.");

    } catch (HopException he) {
      throw he;
    } catch (Exception e) {
      throw new HopException("Unable to split REST response payload", e);
    }
  }

  private DocumentBuilderFactory documentBuilderFactory() throws Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
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
      if (RestMeta.APPLICATION_TYPE_JSON.equals(appType)) {
        if (Utils.isEmpty(conn.getCursorJsonPath())) {
          return java.util.Optional.empty();
        }
        Object raw =
            JsonPath.using(JSON_PATH_CONFIGURATION)
                .parse(NVL(body, "{}"))
                .read(resolve(conn.getCursorJsonPath()));
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
      if (RestMeta.APPLICATION_TYPE_JSON.equals(appType)) {
        if (Utils.isEmpty(conn.getNextPageUrlJsonPath())) {
          return java.util.Optional.empty();
        }
        Object raw =
            JsonPath.using(JSON_PATH_CONFIGURATION)
                .parse(NVL(body, "{}"))
                .read(resolveSplitPathOrPagingExpression(conn.getNextPageUrlJsonPath()));
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

  private Response executeWithRetry(Supplier<Response> requestSupplier) throws HopException {
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
      Response response = null;
      try {
        response = requestSupplier.get();
        int status = response.getStatus();

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

      if (response != null) {
        response.close();
      }
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

  private Response executeRequest(
      Invocation.Builder invocationBuilder, String entityString, String contentType)
      throws HopException {
    try {
      switch (data.method) {
        case RestMeta.HTTP_METHOD_GET -> {
          return invocationBuilder.get(Response.class);
        }
        case RestMeta.HTTP_METHOD_POST -> {
          trackRequestBytes(
              entityString,
              contentType != null ? resolveCharset(contentType) : resolveCharset(data.mediaType));
          if (null != contentType) {
            return invocationBuilder.post(Entity.entity(entityString, contentType));
          } else {
            return invocationBuilder.post(Entity.entity(entityString, data.mediaType));
          }
        }
        case RestMeta.HTTP_METHOD_PUT -> {
          trackRequestBytes(
              entityString,
              contentType != null ? resolveCharset(contentType) : resolveCharset(data.mediaType));
          if (null != contentType) {
            return invocationBuilder.put(Entity.entity(entityString, contentType));
          } else {
            return invocationBuilder.put(Entity.entity(entityString, data.mediaType));
          }
        }
        case RestMeta.HTTP_METHOD_DELETE -> {
          trackRequestBytes(
              entityString,
              contentType != null ? resolveCharset(contentType) : resolveCharset(data.mediaType));
          Invocation invocation =
              invocationBuilder.build("DELETE", Entity.entity(entityString, data.mediaType));
          return invocation.invoke();
        }
        case RestMeta.HTTP_METHOD_HEAD -> {
          return invocationBuilder.head();
        }
        case RestMeta.HTTP_METHOD_OPTIONS -> {
          return invocationBuilder.options();
        }
        case RestMeta.HTTP_METHOD_PATCH -> {
          trackRequestBytes(
              entityString,
              contentType != null ? resolveCharset(contentType) : resolveCharset(data.mediaType));
          if (null != contentType) {
            return invocationBuilder.method(
                RestMeta.HTTP_METHOD_PATCH, Entity.entity(entityString, contentType));
          } else {
            return invocationBuilder.method(
                RestMeta.HTTP_METHOD_PATCH, Entity.entity(entityString, data.mediaType));
          }
        }
        default ->
            throw new HopException(
                BaseMessages.getString(PKG, "Rest.Error.UnknownMethod", data.method));
      }
    } catch (Exception e) {
      throw new HopException("Request could not be processed", e);
    }
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

  private void trackRequestBytes(String entityString, Charset charset) {
    if (entityString == null) {
      return;
    }

    byte[] requestBytes = entityString.getBytes(charset);
    if (requestBytes.length > 0) {
      dataVolumeOut = (dataVolumeOut != null ? dataVolumeOut : 0L) + requestBytes.length;
    }
  }

  private void trackResponseBytes(Response response, String body) {
    long responseBytes = response.getLength();
    if (responseBytes < 0 && body != null) {
      responseBytes = body.getBytes(resolveCharset(response.getMediaType())).length;
    }
    if (responseBytes > 0) {
      dataVolumeIn = (dataVolumeIn != null ? dataVolumeIn : 0L) + responseBytes;
    }
  }

  private Charset resolveCharset(String mediaTypeValue) {
    if (!Utils.isEmpty(mediaTypeValue)) {
      try {
        return resolveCharset(MediaType.valueOf(mediaTypeValue));
      } catch (Exception ignored) {
        // Fall back to UTF-8 below if the header value is malformed.
      }
    }
    return StandardCharsets.UTF_8;
  }

  private Charset resolveCharset(MediaType mediaType) {
    if (mediaType != null) {
      Map<String, String> parameters = mediaType.getParameters();
      String charsetName = parameters.get("charset");
      if (!Utils.isEmpty(charsetName)) {
        try {
          return Charset.forName(charsetName);
        } catch (Exception ignored) {
          // Fall back to UTF-8 below if the charset is unknown.
        }
      }
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
   * set the Authentication/Authorization header from the connection first, if available. this
   * transform's headers will override this value if available.
   *
   * @param headers the mutable map containing HTTP headers that will be sent with the request
   */
  private void addApiKeyHeaderIfAbsent(MultivaluedMap<String, Object> headers) {
    if (connection == null) {
      return;
    }

    if (!"API Key".equalsIgnoreCase(connection.getAuthType())) {
      return;
    }

    String headerName = resolve(connection.getAuthorizationHeaderName());
    String headerValue = resolve(connection.getAuthorizationHeaderValue());
    String prefix = resolve(connection.getAuthorizationPrefix());

    if (Utils.isEmpty(headerName) || Utils.isEmpty(headerValue)) {
      return;
    }

    if (!Utils.isEmpty(prefix)) {
      headerValue = prefix + " " + headerValue;
    }

    // Only add header if not already present
    if (!headers.containsKey(headerName)) {
      headers.putSingle(headerName, headerValue);
    }
  }

  private void setConfig() throws HopException {
    if (data.config == null) {
      // Use ApacheHttpClient for supporting proxy authentication.
      data.config = new ClientConfig();
      data.config.connectorProvider(new Apache5ConnectorProvider());
      data.config.property(
          ClientProperties.REQUEST_ENTITY_PROCESSING, RequestEntityProcessing.BUFFERED);
      data.config.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);

      data.config.property(ClientProperties.READ_TIMEOUT, data.realReadTimeout);
      data.config.property(ClientProperties.CONNECT_TIMEOUT, data.realConnectionTimeout);

      // PROXY CONFIGURATION
      if (!Utils.isEmpty(data.realProxyHost)) {
        data.config.property(
            ClientProperties.PROXY_URI, "http://" + data.realProxyHost + ":" + data.realProxyPort);
      }
      // HTTP BASIC AUTHENTICATION
      if (StringUtils.isEmpty(meta.getConnectionName())) {
        if (!Utils.isEmpty(data.realHttpLogin) || !Utils.isEmpty(data.realHttpPassword)) {
          data.basicAuthentication =
              HttpAuthenticationFeature.basicBuilder()
                  .credentials(data.realHttpLogin, data.realHttpPassword)
                  .build();
        }
      }
      // SSL TRUST STORE CONFIGURATION
      if (!Utils.isEmpty(data.trustStoreFile) && !meta.isIgnoreSsl()) {
        setTrustStoreFile();
      }
      if (meta.isIgnoreSsl()) {
        setTrustAll();
      }
    }
  }

  private void setTrustAll() throws HopException {
    try {
      SSLContext ctx = HttpClientManager.getTrustAllSslContext();

      data.sslContext = ctx;
    } catch (NoSuchAlgorithmException e) {
      throw new HopException(BaseMessages.getString(PKG, "Rest.Error.NoSuchAlgorithm"), e);
    } catch (KeyManagementException e) {
      throw new HopException(BaseMessages.getString(PKG, "Rest.Error.KeyManagementException"), e);
    }
  }

  private void setTrustStoreFile() throws HopException {
    try (FileInputStream trustFileStream = new FileInputStream(data.trustStoreFile)) {

      SSLContext ctx =
          HttpClientManager.getSslContextWithTrustStoreFile(
              trustFileStream, data.trustStorePassword);

      data.sslContext = ctx;
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

  protected MultivaluedMap<String, Object> searchForHeaders(Response response) {
    return response.getHeaders();
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // Get row from input rowset & set row busy!

    if (r == null) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }
    if (first) {
      first = false;
      data.inputRowMeta = getInputRowMeta();
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
          data.realUrl = baseUrl + NVL(resolve(meta.getUrl()), "");
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
        putRow(data.outputRowMeta, outputRowData);
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
        putError(getInputRowMeta(), r, 1, errorMessage, null, "Rest001");
      }
    }
    return true;
  }

  @Override
  public boolean init() {

    if (super.init()) {

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

      data.realConnectionTimeout = Const.toInt(resolve(meta.getConnectionTimeout()), -1);
      data.realReadTimeout = Const.toInt(resolve(meta.getReadTimeout()), -1);

      // get authentication settings once
      data.realProxyHost = resolve(meta.getProxyHost());
      data.realProxyPort = Const.toInt(resolve(meta.getProxyPort()), 8080);
      data.realHttpLogin = resolve(meta.getHttpLogin());
      data.realHttpPassword =
          Encr.decryptPasswordOptionallyEncrypted(resolve(meta.getHttpPassword()));

      if (!meta.isDynamicMethod()) {
        data.method = resolve(meta.getMethod());
        if (Utils.isEmpty(data.method)) {
          logError(BaseMessages.getString(PKG, "Rest.Error.MethodMissing"));
          return false;
        }
      }

      data.trustStoreFile = resolve(meta.getTrustStoreFile());
      data.trustStorePassword = resolve(meta.getTrustStorePassword());

      String applicationType = NVL(meta.getApplicationType(), "");
      switch (applicationType) {
        case RestMeta.APPLICATION_TYPE_XML -> data.mediaType = MediaType.APPLICATION_XML_TYPE;
        case RestMeta.APPLICATION_TYPE_JSON -> data.mediaType = MediaType.APPLICATION_JSON_TYPE;
        case RestMeta.APPLICATION_TYPE_OCTET_STREAM ->
            data.mediaType = MediaType.APPLICATION_OCTET_STREAM_TYPE;
        case RestMeta.APPLICATION_TYPE_XHTML ->
            data.mediaType = MediaType.APPLICATION_XHTML_XML_TYPE;
        case RestMeta.APPLICATION_TYPE_FORM_URLENCODED ->
            data.mediaType = MediaType.APPLICATION_FORM_URLENCODED_TYPE;
        case RestMeta.APPLICATION_TYPE_ATOM_XML ->
            data.mediaType = MediaType.APPLICATION_ATOM_XML_TYPE;
        case RestMeta.APPLICATION_TYPE_SVG_XML ->
            data.mediaType = MediaType.APPLICATION_SVG_XML_TYPE;
        case RestMeta.APPLICATION_TYPE_TEXT_XML -> data.mediaType = MediaType.TEXT_XML_TYPE;
        default -> data.mediaType = MediaType.TEXT_PLAIN_TYPE;
      }
      try {
        setConfig();
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
    data.config = null;
    data.headerNames = null;
    data.indexOfHeaderFields = null;
    data.paramNames = null;
    super.dispose();
  }
}
