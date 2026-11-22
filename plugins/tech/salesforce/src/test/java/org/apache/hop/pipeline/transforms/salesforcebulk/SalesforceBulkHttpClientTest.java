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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.patch;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

@WireMockTest
class SalesforceBulkHttpClientTest {

  @Test
  void runsAnIngestJobAndAPagedQuery(WireMockRuntimeInfo wireMock) throws Exception {
    stubFor(
        post(urlEqualTo("/services/data/v64.0/jobs/ingest"))
            .withHeader("Authorization", equalTo("Bearer test-token"))
            .willReturn(json("{\"id\":\"7501\",\"state\":\"Open\"}")));
    stubFor(
        put(urlEqualTo("/services/data/v64.0/jobs/ingest/7501/batches"))
            .willReturn(aResponse().withStatus(201)));
    stubFor(
        patch(urlEqualTo("/services/data/v64.0/jobs/ingest/7501"))
            .willReturn(
                json(
                    "{\"id\":\"7501\",\"state\":\"JobComplete\",\"numberRecordsProcessed\":1,\"numberRecordsFailed\":0}")));
    stubFor(
        get(urlEqualTo("/services/data/v64.0/jobs/ingest/7501/successfulResults"))
            .willReturn(aResponse().withStatus(200).withBody("Name,sf__Id\nAda,001\n")));
    stubFor(
        get(urlEqualTo("/services/data/v64.0/jobs/ingest/7501/failedResults"))
            .willReturn(aResponse().withStatus(404)));
    stubFor(
        post(urlEqualTo("/services/data/v64.0/jobs/query"))
            .willReturn(json("{\"id\":\"750q\",\"state\":\"UploadComplete\"}")));
    stubFor(
        get(urlEqualTo("/services/data/v64.0/jobs/query/750q"))
            .willReturn(json("{\"id\":\"750q\",\"state\":\"JobComplete\"}")));
    stubFor(
        get(urlEqualTo("/services/data/v64.0/jobs/query/750q/results"))
            .withHeader("Sforce-Locator", absent())
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Sforce-Locator", "page2")
                    .withBody("Id,Name\n1,Acme\n")));
    stubFor(
        get(urlEqualTo("/services/data/v64.0/jobs/query/750q/results"))
            .withHeader("Sforce-Locator", equalTo("page2"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Sforce-Locator", "null")
                    .withBody("Id,Name\n2,Beta\n")));

    SalesforceBulkSession session =
        new SalesforceBulkSession(wireMock.getHttpBaseUrl(), "test-token", "64.0");
    try (SalesforceBulkHttpClient client = new SalesforceBulkHttpClient(session)) {
      SalesforceBulkJob created =
          client.createIngestJob(
              new SalesforceBulkIngestRequest(
                  "Account",
                  SalesforceBulkOperation.INSERT,
                  null,
                  SalesforceBulkLineEnding.LF,
                  SalesforceBulkDelimiter.COMMA,
                  SalesforceBulkConcurrency.PARALLEL));
      assertEquals("7501", created.getId());
      client.uploadIngestData("7501", "Name\nAda\n".getBytes());
      SalesforceBulkJob done = client.completeIngestUpload("7501");
      assertEquals("JobComplete", done.getState());
      assertEquals(1, done.getRecordsProcessed());
      assertTrue(client.ingestResults("7501", false).contains("Ada"));
      assertEquals("", client.ingestResults("7501", true));

      SalesforceBulkJob query = client.createQueryJob("query", "SELECT Id, Name FROM Account");
      SalesforceBulkJob finished = client.getQueryJob(query.getId());
      assertTrue(finished.isFinished());
      SalesforceBulkQueryPage first = client.queryResults(query.getId(), null);
      assertTrue(first.hasMore());
      assertTrue(first.getCsv().contains("Acme"));
      SalesforceBulkQueryPage second = client.queryResults(query.getId(), first.getLocator());
      assertFalse(second.hasMore());
      assertTrue(second.getCsv().contains("Beta"));
    }
  }

  @Test
  void reportsAnUnauthorizedCall(WireMockRuntimeInfo wireMock) {
    stubFor(
        post(urlEqualTo("/services/data/v64.0/jobs/ingest"))
            .willReturn(
                aResponse().withStatus(401).withBody("[{\"errorCode\":\"INVALID_SESSION_ID\"}]")));
    SalesforceBulkHttpClient client =
        new SalesforceBulkHttpClient(
            new SalesforceBulkSession(wireMock.getHttpBaseUrl(), "bad", "64.0"));
    HopException error =
        assertThrows(
            HopException.class,
            () ->
                client.createIngestJob(
                    new SalesforceBulkIngestRequest(
                        "Account",
                        SalesforceBulkOperation.INSERT,
                        null,
                        SalesforceBulkLineEnding.LF,
                        SalesforceBulkDelimiter.COMMA,
                        SalesforceBulkConcurrency.PARALLEL)));
    assertTrue(error.getMessage().contains("401"));
    client.close();
  }

  private static ResponseDefinitionBuilder json(String body) {
    return aResponse()
        .withStatus(200)
        .withHeader("Content-Type", "application/json")
        .withBody(body);
  }
}
