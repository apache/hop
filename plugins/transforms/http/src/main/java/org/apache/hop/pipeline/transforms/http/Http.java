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
import java.util.List;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.AuthCache;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
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
    CloseableHttpClient httpClient = createClientBuilder().build();

    // Prepare Http get
    URI uri = null;
    try {
      URIBuilder uriBuilder = constructUrlBuilder(rowMeta, rowData);

      uri = uriBuilder.build();
      HttpGet method = new HttpGet(uri);

      // Add Custom Http headers
      addHeadersToMethod(rowData, method);

      Object[] newRow = null;
      if (rowData != null) {
        newRow = rowData.clone();
      }
      // Execute request
      CloseableHttpResponse httpResponse = null;
      try {
        // used for calculating the responseTime
        long startTime = System.currentTimeMillis();

        // Preemptive authentication
        HttpHost target = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
        if (StringUtils.isNotBlank(data.realProxyHost)) {
          target = new HttpHost(data.realProxyHost, data.realProxyPort, "http");
        }

        httpResponse = httpClient.execute(target, method, buildtHttpClientContext(target));

        // calculate the responseTime
        long responseTime = System.currentTimeMillis() - startTime;
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "HTTP.Log.ResponseTime", responseTime, uri));
        }
        int statusCode = requestStatusCode(httpResponse);
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
        // Release current connection to the connection pool once you are done
        method.releaseConnection();
      }
      return newRow;
    } catch (UnknownHostException uhe) {
      throw new HopException(
          BaseMessages.getString(PKG, "HTTP.Error.UnknownHostException", uhe.getMessage()));
    } catch (Exception e) {
      throw new HopException(BaseMessages.getString(PKG, "HTTP.Log.UnableGetResult", uri), e);
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
    dataVolumeIn = (dataVolumeIn != null ? dataVolumeIn : 0L) + bodyBytes.length;

    ByteArrayEntity countedEntity = new ByteArrayEntity(bodyBytes);
    if (entity.getContentType() != null) {
      countedEntity.setContentType(entity.getContentType());
    }
    if (entity.getContentEncoding() != null) {
      countedEntity.setContentEncoding(entity.getContentEncoding());
    }

    return StringUtils.isEmpty(meta.getEncoding())
        ? EntityUtils.toString(countedEntity)
        : EntityUtils.toString(countedEntity, meta.getEncoding());
  }

  private static @NotNull HttpClientContext buildtHttpClientContext(HttpHost target) {
    // Create AuthCache instance
    AuthCache authCache = new BasicAuthCache();
    // Generate BASIC scheme object and add it to the local
    // auth cache
    BasicScheme basicAuth = new BasicScheme();
    authCache.put(target, basicAuth);
    // Add AuthCache to the execution context
    HttpClientContext localContext = HttpClientContext.create();
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

  private HttpClientManager.HttpClientBuilderFacade createClientBuilder() {
    HttpClientManager.HttpClientBuilderFacade clientBuilder =
        HttpClientManager.getInstance().createBuilder();

    if (data.realConnectionTimeout > -1) {
      clientBuilder.setConnectionTimeout(data.realConnectionTimeout);
    }
    if (data.realSocketTimeout > -1) {
      clientBuilder.setSocketTimeout(data.realSocketTimeout);
    }
    if (StringUtils.isNotBlank(data.realHttpLogin)) {
      clientBuilder.setCredentials(data.realHttpLogin, data.realHttpPassword);
    }
    if (StringUtils.isNotBlank(data.realProxyHost)) {
      clientBuilder.setProxy(data.realProxyHost, data.realProxyPort);
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

  protected int requestStatusCode(HttpResponse httpResponse) {
    return httpResponse.getStatusLine().getStatusCode();
  }

  protected Header[] searchForHeaders(CloseableHttpResponse response) {
    return response.getAllHeaders();
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
    } else {
      data.realUrl = resolve(meta.getUrl());
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
      data.realHttpLogin = resolve(meta.getHttpLogin());
      data.realHttpPassword = Utils.resolvePassword(variables, meta.getHttpPassword());

      data.realSocketTimeout = Const.toInt(resolve(meta.getSocketTimeout()), -1);
      data.realConnectionTimeout = Const.toInt(resolve(meta.getConnectionTimeout()), -1);

      data.withoutPreviousTransforms =
          getPipelineMeta().getPrevTransforms(getTransformMeta()).length == 0;

      return true;
    }
    return false;
  }
}
