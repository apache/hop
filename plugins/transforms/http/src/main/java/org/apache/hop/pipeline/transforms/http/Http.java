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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.auth.AuthCache;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.auth.BasicAuthCache;
import org.apache.hc.client5.http.impl.auth.BasicScheme;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageHttpIoEmitter;
import org.apache.hop.lineage.model.HttpDirection;
import org.apache.hop.lineage.model.HttpLineagePayload;
import org.apache.hop.metadata.rest.RestConnection;
import org.apache.hop.metadata.rest.client.RestClientFactory;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.jetbrains.annotations.NotNull;
import org.json.simple.JSONObject;

/** Retrieves data from an Http endpoint */
public class Http extends BaseTransform<HttpMeta, HttpData> {

  private static final Class<?> PKG = HttpMeta.class;
  public static final String CONST_HTTP_EXCEPTION_ERROR_FINDING_FIELD =
      "HTTP.Exception.ErrorFindingField";

  public Http(
      TransformMeta transformMeta,
      HttpMeta meta,
      HttpData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private Object[] execHttp(IRowMeta rowMeta, Object[] row) throws HopException {
    if (first) {
      first = false;
      int nrQueryParameters = meta.getLookupParameters().getQueryParameters().size();
      data.argNrs = new int[nrQueryParameters];

      for (int i = 0; i < nrQueryParameters; i++) {
        HttpMeta.QueryParameter queryParameter =
            meta.getLookupParameters().getQueryParameters().get(i);

        data.argNrs[i] = rowMeta.indexOfValue(queryParameter.getField());
        if (data.argNrs[i] < 0) {
          logError(
              BaseMessages.getString(PKG, "HTTP.Log.ErrorFindingField")
                  + queryParameter.getField()
                  + "]");
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "HTTP.Exception.CouldnotFindField", queryParameter.getField()));
        }
      }
    }

    return callHttpService(rowMeta, row);
  }

  @VisibleForTesting
  Object[] callHttpService(IRowMeta rowMeta, Object[] rowData) throws HopException {
    // Prepare Http get
    URI uri = null;
    long lineageStart = System.currentTimeMillis();
    long volumeInBefore = dataVolumeIn != null ? dataVolumeIn : 0L;
    data.lastHttpResponseBodyBytes = 0;
    String urlForLineage = data.realUrl;
    Integer lineageStatus = null;
    boolean lineageOk = false;
    String lineageErr = null;
    try {
      URIBuilder uriBuilder = constructUrlBuilder(rowMeta, rowData);

      uri = uriBuilder.build();
      urlForLineage = uri.toString();
      HttpGet method = new HttpGet(uri);

      // Add Custom Http headers
      addHeadersToMethod(rowData, method);
      addConnectionAuthentication(method, uri);

      Object[] newRow = null;
      if (rowData != null) {
        newRow = rowData.clone();
      }
      // Execute request
      CloseableHttpResponse httpResponse = null;
      try {
        // used for calculating the responseTime
        long startTime = System.currentTimeMillis();

        // Preemptive Basic auth: AuthCache must be keyed to the origin host (same as the request
        // target). Proxy routing is configured on the client via a route planner — do not pass the
        // proxy as HttpHost here (breaks credentials + preemptive cache matching in HttpClient 5).
        HttpHost target = HttpClientManager.createHttpHost(uri);
        CloseableHttpClient httpClient = httpClient(target);

        httpResponse =
            httpClient.execute(
                target,
                method,
                data.restConnection == null
                    ? buildHttpClientContext(target, data.realHttpLogin, data.realHttpPassword)
                    : HttpClientContext.create());

        // calculate the responseTime
        long responseTime = System.currentTimeMillis() - startTime;
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "HTTP.Log.ResponseTime", responseTime, uri));
        }
        int statusCode = requestStatusCode(httpResponse);
        lineageStatus = statusCode;
        // The status code
        if (isDebug()) {
          logDebug(BaseMessages.getString(PKG, "HTTP.Log.ResponseStatusCode", "" + statusCode));
        }

        String body;
        body = handResponse(statusCode, httpResponse);

        String headerString = extractHeaderString(searchForHeaders(httpResponse));

        int returnFieldsOffset = rowMeta.size();
        if (!Utils.isEmpty(meta.getResultFields().getFieldName())) {
          newRow = RowDataUtil.addValueData(newRow, returnFieldsOffset, body);
          returnFieldsOffset++;
        }

        if (!Utils.isEmpty(meta.getResultFields().getResultCodeFieldName())) {
          newRow = RowDataUtil.addValueData(newRow, returnFieldsOffset, (long) statusCode);
          returnFieldsOffset++;
        }
        if (!Utils.isEmpty(meta.getResultFields().getResponseTimeFieldName())) {
          newRow = RowDataUtil.addValueData(newRow, returnFieldsOffset, responseTime);
          returnFieldsOffset++;
        }
        if (!Utils.isEmpty(meta.getResultFields().getResponseHeaderFieldName())) {
          newRow = RowDataUtil.addValueData(newRow, returnFieldsOffset, headerString);
        }
      } finally {
        if (httpResponse != null) {
          httpResponse.close();
        }
      }
      lineageOk = true;
      return newRow;
    } catch (UnknownHostException uhe) {
      lineageErr = uhe.getMessage();
      throw new HopException(
          BaseMessages.getString(PKG, "HTTP.Error.UnknownHostException", uhe.getMessage()));
    } catch (Exception e) {
      lineageErr = e.getMessage();
      throw new HopException(BaseMessages.getString(PKG, "HTTP.Log.UnableGetResult", uri), e);
    } finally {
      long respDelta = (dataVolumeIn != null ? dataVolumeIn : 0L) - volumeInBefore;
      if (respDelta <= 0 && data.lastHttpResponseBodyBytes > 0) {
        respDelta = data.lastHttpResponseBodyBytes;
      }
      LineageHttpIoEmitter.emitTransformHttpIo(
          this,
          new HttpLineagePayload(
              HttpDirection.CLIENT,
              "GET",
              urlForLineage,
              lineageStatus,
              null,
              respDelta > 0 ? respDelta : null,
              System.currentTimeMillis() - lineageStart,
              lineageOk,
              lineageErr));
    }
  }

  private static String extractHeaderString(Header[] headers) {
    JSONObject json = new JSONObject();
    for (Header header : headers) {
      Object previousValue = json.get(header.getName());
      if (previousValue == null) {
        json.put(header.getName(), header.getValue());
      } else if (previousValue instanceof List) {
        List<String> list = (List<String>) previousValue;
        list.add(header.getValue());
      } else {
        ArrayList<String> list = new ArrayList<>();
        list.add((String) previousValue);
        list.add(header.getValue());
        json.put(header.getName(), list);
      }
    }
    return json.toJSONString();
  }

  private String handResponse(int statusCode, CloseableHttpResponse httpResponse)
      throws HopTransformException, IOException {
    String body;
    switch (statusCode) {
      case HttpURLConnection.HTTP_UNAUTHORIZED:
        throw new HopTransformException(
            BaseMessages.getString(PKG, "HTTP.Exception.Authentication", data.realUrl));
      case -1:
        throw new HopTransformException(
            BaseMessages.getString(PKG, "HTTP.Exception.IllegalStatusCode", data.realUrl));
      case HttpURLConnection.HTTP_NO_CONTENT:
        body = "";
        break;
      default:
        HttpEntity entity = httpResponse.getEntity();
        if (entity != null) {
          body = readResponseBody(entity);
        } else {
          body = "";
        }
        break;
    }
    return body;
  }

  private String readResponseBody(HttpEntity entity) throws IOException {
    byte[] bodyBytes = EntityUtils.toByteArray(entity);
    data.lastHttpResponseBodyBytes = bodyBytes.length;
    dataVolumeIn = (dataVolumeIn != null ? dataVolumeIn : 0L) + bodyBytes.length;

    ByteArrayEntity countedEntity =
        new ByteArrayEntity(
            bodyBytes, org.apache.hc.core5.http.ContentType.APPLICATION_OCTET_STREAM);

    try {
      return StringUtils.isEmpty(meta.getEncoding())
          ? EntityUtils.toString(countedEntity)
          : EntityUtils.toString(countedEntity, meta.getEncoding());
    } catch (ParseException e) {
      throw new IOException("Unable to parse HTTP response body", e);
    }
  }

  /**
   * The client to execute with: the one built from the selected REST connection, or a client
   * configured from this transform's own fields.
   */
  private CloseableHttpClient httpClient(HttpHost target) {
    if (data.restConnectionClient != null) {
      return data.restConnectionClient;
    }
    return createClientBuilder(target).build();
  }

  /**
   * Bearer, API-key and preemptive Basic authentication are request headers rather than answers to
   * a challenge, so the connection writes them onto every request itself.
   */
  private void addConnectionAuthentication(HttpGet method, URI uri) throws HopException {
    if (data.restConnection == null) {
      return;
    }
    Map<String, String> authHeaders = new LinkedHashMap<>();
    data.restConnection.applyAuthentication(authHeaders, uri.toString());
    for (Map.Entry<String, String> authHeader : authHeaders.entrySet()) {
      method.setHeader(authHeader.getKey(), authHeader.getValue());
    }
  }

  /** HttpClient 5 requires {@link BasicScheme#initPreemptive} for preemptive Basic auth. */
  private static @NotNull HttpClientContext buildHttpClientContext(
      HttpHost target, String httpLogin, String httpPassword) {
    HttpClientContext localContext = HttpClientContext.create();
    if (StringUtils.isBlank(httpLogin)) {
      return localContext;
    }
    AuthCache authCache = new BasicAuthCache();
    BasicScheme basicAuth = new BasicScheme();
    char[] passwordChars = httpPassword != null ? httpPassword.toCharArray() : new char[0];
    basicAuth.initPreemptive(new UsernamePasswordCredentials(httpLogin, passwordChars));
    authCache.put(target, basicAuth);
    localContext.setAuthCache(authCache);
    return localContext;
  }

  private void addHeadersToMethod(Object[] rowData, HttpGet method) throws HopValueException {
    if (data.useHeaderParameters) {
      for (int i = 0; i < data.headerParametersNrs.length; i++) {
        method.addHeader(
            data.headerParameters[i].getName(),
            data.inputRowMeta.getString(rowData, data.headerParametersNrs[i]));
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG,
                  "HTTPDialog.Log.HeaderValue",
                  data.headerParameters[i].getName(),
                  data.inputRowMeta.getString(rowData, data.headerParametersNrs[i])));
        }
      }
    }
  }

  /**
   * The client for one request.
   *
   * <p>Credentials are registered for the host they belong to rather than for any host at all: a
   * proxy asking for authentication of its own used to be answered with the web server's user name
   * and password, because a single wildcard {@link org.apache.hc.client5.http.auth.AuthScope}
   * matched both challenges (issue #3440).
   */
  private HttpClientManager.HttpClientBuilderFacade createClientBuilder(HttpHost target) {
    HttpClientManager.HttpClientBuilderFacade clientBuilder =
        HttpClientManager.getInstance().createBuilder();

    if (data.realConnectionTimeout > -1) {
      clientBuilder.setConnectionTimeout(data.realConnectionTimeout);
    }
    if (data.realSocketTimeout > -1) {
      clientBuilder.setSocketTimeout(data.realSocketTimeout);
    }
    if (StringUtils.isNotBlank(data.realHttpLogin)) {
      clientBuilder.setCredentials(
          data.realHttpLogin, data.realHttpPassword, new AuthScope(target));
    }
    if (StringUtils.isNotBlank(data.realProxyHost)) {
      HttpHost proxy = new HttpHost("http", data.realProxyHost, data.realProxyPort);
      clientBuilder.setProxy(proxy.getHostName(), proxy.getPort(), proxy.getSchemeName());
      clientBuilder.setNonProxyHosts(data.realNonProxyHosts);
      if (StringUtils.isNotBlank(data.realProxyUsername)) {
        clientBuilder.setCredentials(
            data.realProxyUsername, data.realProxyPassword, new AuthScope(proxy));
      }
    }
    if (meta.isIgnoreSsl()) {
      clientBuilder.ignoreSsl(true);
    }
    return clientBuilder;
  }

  private URIBuilder constructUrlBuilder(IRowMeta outputRowMeta, Object[] row) throws HopException {
    URIBuilder uriBuilder;
    try {
      String baseUrl = data.realUrl;
      if (meta.isUrlInField()) {
        // get dynamic url
        baseUrl = outputRowMeta.getString(row, data.indexOfUrlField);
        if (data.restConnection != null) {
          baseUrl =
              RestConnection.resolveAgainstBase(resolve(data.restConnection.getBaseUrl()), baseUrl);
        }
      }

      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "HTTP.Log.Connecting", baseUrl));
      }

      uriBuilder = new URIBuilder(baseUrl); // the base URL with variable substitution

      for (int i = 0; i < data.argNrs.length; i++) {
        HttpMeta.QueryParameter parameter = meta.getLookupParameters().getQueryParameters().get(i);
        String key = parameter.getParameter();
        String value = outputRowMeta.getString(row, data.argNrs[i]);
        if (!key.isEmpty()) {
          uriBuilder.addParameter(key, Const.NVL(value, ""));
        }
      }
    } catch (Exception e) {
      throw new HopException(BaseMessages.getString(PKG, "HTTP.Log.UnableCreateUrl"), e);
    }
    return uriBuilder;
  }

  protected int requestStatusCode(CloseableHttpResponse httpResponse) {
    return httpResponse.getCode();
  }

  protected Header[] searchForHeaders(CloseableHttpResponse response) {
    return response.getHeaders();
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow();

    boolean firstWithoutPreviousTransforms = first && data.withoutPreviousTransforms;
    if (r == null && !firstWithoutPreviousTransforms) {
      setOutputDone();
      return false;
    }

    if (first) {
      firstProcessRow(r);
    }

    try {
      Object[] outputRowData = execHttp(data.inputRowMeta, r); // add new values to the row
      putRow(data.outputRowMeta, outputRowData); // copy row to output rowset(s)

      if (checkFeedback(getLinesRead()) && isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "HTTP.LineNumber") + getLinesRead());
      }
    } catch (HopException e) {
      String errorMessage;

      if (getTransformMeta().isDoingErrorHandling()) {
        errorMessage = e.toString();
      } else {
        logError(BaseMessages.getString(PKG, "HTTP.ErrorInTransformRunning") + e.getMessage());
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      // Simply add this row to the error row
      putError(data.inputRowMeta, r, 1, errorMessage, null, "HTTP001");
    }

    return true;
  }

  private void firstProcessRow(Object[] r) throws HopException {
    data.inputRowMeta = data.withoutPreviousTransforms ? new RowMeta() : getInputRowMeta();
    data.outputRowMeta = data.inputRowMeta.clone();
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

    if (meta.isUrlInField()) {
      if (Utils.isEmpty(meta.getUrlField())) {
        logError(BaseMessages.getString(PKG, "HTTP.Log.NoField"));
        throw new HopException(BaseMessages.getString(PKG, "HTTP.Log.NoField"));
      }

      // cache the position of the field
      if (data.indexOfUrlField < 0) {
        String realUrlFieldName = resolve(meta.getUrlField());
        data.indexOfUrlField = data.inputRowMeta.indexOfValue(realUrlFieldName);
        if (data.indexOfUrlField < 0) {
          // The field is unreachable !
          logError(BaseMessages.getString(PKG, "HTTP.Log.ErrorFindingField", realUrlFieldName));
          throw new HopException(
              BaseMessages.getString(
                  PKG, CONST_HTTP_EXCEPTION_ERROR_FINDING_FIELD, realUrlFieldName));
        }
      }
    }

    // check for headers
    int nrHeaders = meta.getLookupParameters().getHeaders().size();
    if (nrHeaders > 0) {
      data.useHeaderParameters = true;
    }

    data.headerParametersNrs = new int[nrHeaders];
    data.headerParameters = new NameValuePair[nrHeaders];

    // get the headers
    for (int i = 0; i < nrHeaders; i++) {
      HttpMeta.HeaderParameter headerParameter = meta.getLookupParameters().getHeaders().get(i);
      int fieldIndex = data.inputRowMeta.indexOfValue(headerParameter.getField());
      if (fieldIndex < 0) {
        logError(
            BaseMessages.getString(PKG, CONST_HTTP_EXCEPTION_ERROR_FINDING_FIELD)
                + headerParameter.getField()
                + "]");
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, CONST_HTTP_EXCEPTION_ERROR_FINDING_FIELD, headerParameter.getField()));
      }

      data.headerParametersNrs[i] = fieldIndex;
      data.headerParameters[i] =
          new BasicNameValuePair(
              resolve(headerParameter.getParameter()),
              data.outputRowMeta.getString(r, data.headerParametersNrs[i]));
    }
  }

  @Override
  public boolean init() {

    if (super.init()) {
      // get authentication settings once
      data.realProxyHost = resolve(meta.getProxyHost());
      data.realProxyPort = Const.toInt(resolve(meta.getProxyPort()), 8080);
      data.realProxyUsername = resolve(meta.getProxyUsername());
      data.realProxyPassword = Utils.resolvePassword(variables, meta.getProxyPassword());
      data.realNonProxyHosts = resolve(meta.getNonProxyHosts());
      data.realHttpLogin = resolve(meta.getHttpLogin());
      data.realHttpPassword = Utils.resolvePassword(variables, meta.getHttpPassword());

      if (!loadRestConnection()) {
        return false;
      }

      // Resolved here rather than on the first row: the base URL it may hang off belongs to the
      // connection just loaded, and every request needs it, including the first.
      data.realUrl =
          data.restConnection == null
              ? resolve(meta.getUrl())
              : RestConnection.resolveAgainstBase(
                  resolve(data.restConnection.getBaseUrl()), resolve(meta.getUrl()));

      data.realSocketTimeout = Const.toInt(resolve(meta.getSocketTimeout()), -1);
      data.realConnectionTimeout = Const.toInt(resolve(meta.getConnectionTimeout()), -1);

      data.withoutPreviousTransforms =
          getPipelineMeta().getPrevTransforms(getTransformMeta()).length == 0;

      return true;
    }
    return false;
  }

  /**
   * Loads the selected REST connection and the client that goes with it. Returns false when a
   * connection is named but cannot be loaded, which has to stop the transform: falling back to the
   * transform's own fields would send the request somewhere else, unauthenticated.
   */
  private boolean loadRestConnection() {
    String realConnectionName = resolve(meta.getConnectionName());
    if (Utils.isEmpty(realConnectionName)) {
      return true;
    }
    try {
      data.restConnection =
          metadataProvider.getSerializer(RestConnection.class).load(realConnectionName);
      if (data.restConnection == null) {
        logError(BaseMessages.getString(PKG, "HTTP.Error.ConnectionNotFound", realConnectionName));
        return false;
      }
      data.restConnection.setVariables(this);
      data.restConnectionClient =
          RestClientFactory.createClient(data.restConnection.createClientSettings());
      return true;
    } catch (Exception e) {
      // Keep the cause: a class loader split between the metadata plugin and this transform
      // surfaces here as a ClassCastException, which is not a missing connection at all.
      logError(
          BaseMessages.getString(PKG, "HTTP.Error.ConnectionNotLoaded", realConnectionName), e);
      return false;
    }
  }

  @Override
  public void dispose() {
    if (data.restConnectionClient != null) {
      try {
        data.restConnectionClient.close();
      } catch (IOException e) {
        logError(BaseMessages.getString(PKG, "HTTP.Error.ClosingClient"), e);
      }
      data.restConnectionClient = null;
    }
    super.dispose();
  }
}
