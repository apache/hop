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

package org.apache.hop.pipeline.transforms.salesforcebulk;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPatch;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.core.util.Utils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/** Bulk API 2.0 over {@link HttpClientManager}. */
public class SalesforceBulkHttpClient implements SalesforceBulkClient {
  private static final ContentType CSV = ContentType.create("text/csv", StandardCharsets.UTF_8);

  private final SalesforceBulkSession session;
  private final CloseableHttpClient httpClient;
  private final String jobsRoot;

  public SalesforceBulkHttpClient(SalesforceBulkSession session) {
    this(session, HttpClientManager.getInstance().createBuilder().useSystemProxy(true).build());
  }

  SalesforceBulkHttpClient(SalesforceBulkSession session, CloseableHttpClient httpClient) {
    this.session = session;
    this.httpClient = httpClient;
    this.jobsRoot =
        session.getInstanceUrl() + "/services/data/v" + session.getApiVersion() + "/jobs";
  }

  @Override
  public SalesforceBulkJob createIngestJob(SalesforceBulkIngestRequest request)
      throws HopException {
    JSONObject body = new JSONObject();
    body.put("object", request.getSalesforceObject());
    body.put("operation", request.getOperation().getCode());
    body.put("contentType", "CSV");
    body.put("lineEnding", request.getLineEnding().getCode());
    body.put("columnDelimiter", request.getDelimiter().getCode());
    body.put("concurrencyMode", request.getConcurrency().getCode());
    if (request.getOperation() == SalesforceBulkOperation.UPSERT
        && !Utils.isEmpty(request.getExternalIdField())) {
      body.put("externalIdFieldName", request.getExternalIdField());
    }
    HttpPost post = new HttpPost(jobsRoot + "/ingest");
    post.setEntity(new StringEntity(body.toJSONString(), ContentType.APPLICATION_JSON));
    return readJob(exchange(post, false));
  }

  @Override
  public void uploadIngestData(String jobId, byte[] csv) throws HopException {
    HttpPut put = new HttpPut(jobsRoot + "/ingest/" + jobId + "/batches");
    put.setEntity(new ByteArrayEntity(csv, CSV));
    exchange(put, false);
  }

  @Override
  public SalesforceBulkJob completeIngestUpload(String jobId) throws HopException {
    return patchJob(jobsRoot + "/ingest/" + jobId, "UploadComplete");
  }

  @Override
  public SalesforceBulkJob getIngestJob(String jobId) throws HopException {
    return readJob(exchange(new HttpGet(jobsRoot + "/ingest/" + jobId), false));
  }

  @Override
  public String ingestResults(String jobId, boolean failed) throws HopException {
    String suffix = failed ? "/failedResults" : "/successfulResults";
    HttpGet get = new HttpGet(jobsRoot + "/ingest/" + jobId + suffix);
    return exchange(get, true).body;
  }

  @Override
  public void abortIngestJob(String jobId) throws HopException {
    patchJob(jobsRoot + "/ingest/" + jobId, "Aborted");
  }

  @Override
  public SalesforceBulkJob createQueryJob(String operation, String soql) throws HopException {
    JSONObject body = new JSONObject();
    body.put("operation", operation);
    body.put("query", soql);
    HttpPost post = new HttpPost(jobsRoot + "/query");
    post.setEntity(new StringEntity(body.toJSONString(), ContentType.APPLICATION_JSON));
    return readJob(exchange(post, false));
  }

  @Override
  public SalesforceBulkJob getQueryJob(String jobId) throws HopException {
    return readJob(exchange(new HttpGet(jobsRoot + "/query/" + jobId), false));
  }

  @Override
  public SalesforceBulkQueryPage queryResults(String jobId, String locator) throws HopException {
    HttpGet get = new HttpGet(jobsRoot + "/query/" + jobId + "/results");
    if (locator != null && !locator.isBlank() && !"null".equalsIgnoreCase(locator)) {
      get.setHeader("Sforce-Locator", locator);
    }
    HttpResult result = exchange(get, true);
    return new SalesforceBulkQueryPage(result.body, result.locator);
  }

  @Override
  public void abortQueryJob(String jobId) throws HopException {
    patchJob(jobsRoot + "/query/" + jobId, "Aborted");
  }

  @Override
  public void close() {
    try {
      httpClient.close();
    } catch (IOException ignored) {
      // The shared connection pool stays open.
    }
  }

  private SalesforceBulkJob patchJob(String url, String state) throws HopException {
    JSONObject body = new JSONObject();
    body.put("state", state);
    HttpPatch patch = new HttpPatch(url);
    patch.setEntity(new StringEntity(body.toJSONString(), ContentType.APPLICATION_JSON));
    return readJob(exchange(patch, false));
  }

  private HttpResult exchange(HttpUriRequestBase request, boolean allowMissing)
      throws HopException {
    request.setHeader("Authorization", "Bearer " + session.getAccessToken());
    request.setHeader("Accept", "application/json, text/csv");
    try {
      return httpClient.execute(
          request,
          response -> {
            String body =
                response.getEntity() == null
                    ? ""
                    : EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            Header locator = response.getFirstHeader("Sforce-Locator");
            int code = response.getCode();
            if (allowMissing && code == 404) {
              return new HttpResult(code, "", null);
            }
            if (code >= 300) {
              throw new java.io.IOException(
                  "Salesforce Bulk API " + request.getMethod() + " returned " + code + ": " + body);
            }
            return new HttpResult(code, body, locator == null ? null : locator.getValue());
          });
    } catch (Exception e) {
      String message = e.getMessage() == null ? e.toString() : e.getMessage();
      throw new HopException(message, e);
    }
  }

  private static SalesforceBulkJob readJob(HttpResult result) throws HopException {
    try {
      Object parsed = new JSONParser().parse(result.body);
      if (!(parsed instanceof JSONObject object)) {
        throw new HopException("Salesforce Bulk API returned no job: " + result.body);
      }
      return new SalesforceBulkJob(
          text(object, "id"),
          text(object, "state"),
          text(object, "errorMessage"),
          integer(object, "numberRecordsProcessed"),
          integer(object, "numberRecordsFailed"));
    } catch (ParseException e) {
      throw new HopException("Salesforce Bulk API returned invalid JSON: " + result.body, e);
    }
  }

  private static String text(JSONObject object, String key) {
    Object value = object.get(key);
    return value == null ? null : String.valueOf(value);
  }

  private static int integer(JSONObject object, String key) {
    Object value = object.get(key);
    if (value instanceof Number number) {
      return number.intValue();
    }
    if (value == null || String.valueOf(value).isBlank()) {
      return 0;
    }
    return Integer.parseInt(String.valueOf(value));
  }

  private static final class HttpResult {
    private final int code;
    private final String body;
    private final String locator;

    private HttpResult(int code, String body, String locator) {
      this.code = code;
      this.body = body == null ? "" : body;
      this.locator = locator;
    }
  }
}
