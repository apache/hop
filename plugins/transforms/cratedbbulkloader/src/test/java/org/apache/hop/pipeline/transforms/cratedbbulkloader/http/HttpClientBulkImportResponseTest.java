/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.cratedbbulkloader.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.http.HttpResponse;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class HttpClientBulkImportResponseTest {

  private static final String MOCK_ENDPOINT = "http://localhost:4200/_sql";
  private static final String MOCK_USER = "alice";
  private static final String MOCK_PASSWORD = "password";

  private HttpResponse<String> mockResponse;
  private CrateDBHttpResponse crateDBHttpResponse;
  private BulkImportClient client;

  @BeforeEach
  public void setup() {
    mockResponse = Mockito.mock(HttpResponse.class);
    crateDBHttpResponse = new CrateDBHttpResponse();
    client = new BulkImportClient(MOCK_ENDPOINT, MOCK_USER, MOCK_PASSWORD);
  }

  @Test
  public void shouldCalculateOutputRows() throws HopException, IOException {
    String responseBody =
        "{\"cols\":[],\"duration\":20.493383,\"results\":[{\"rowcount\": 1},{\"rowcount\": 1},{\"rowcount\": 1},{\"rowcount\":  1},{\"rowcount\": 1}]}";
    when(mockResponse.body()).thenReturn(responseBody);
    when(mockResponse.statusCode()).thenReturn(200);

    HttpBulkImportResponse httpBulkImportResponse =
        HttpBulkImportResponse.fromHttpResponse(mockResponse);

    assertEquals(200, httpBulkImportResponse.statusCode());
    assertEquals(5, httpBulkImportResponse.outputRows());
  }

  @Test
  public void shouldCountRejectedRows() throws HopException, IOException {
    String responseBody =
        "{\"cols\":[],\"duration\":20.493383,\"results\":[{\"rowcount\": 1},{\"rowcount\": 1},{\"rowcount\": 1},{\"rowcount\": 1},{\"rowcount\":-2}]}";
    // as per documentation: rowcount == 1 means imported row, while rowcount == -2 means rejected
    // row
    when(mockResponse.body()).thenReturn(responseBody);
    when(mockResponse.statusCode()).thenReturn(200);

    HttpBulkImportResponse httpBulkImportResponse =
        HttpBulkImportResponse.fromHttpResponse(mockResponse);

    assertEquals(200, httpBulkImportResponse.statusCode());
    assertEquals(4, httpBulkImportResponse.outputRows());
    assertEquals(1, httpBulkImportResponse.rejectedRows());
  }

  @Test
  public void shouldReturnZeroOutputRowsWhenEmpty() throws HopException, IOException {
    String emptyResultsResponseBody = "{\"cols\":[],\"duration\":20.493383,\"results\":[]}";
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(emptyResultsResponseBody);

    HttpBulkImportResponse httpBulkImportResponse =
        HttpBulkImportResponse.fromHttpResponse(mockResponse);

    assertEquals(0, httpBulkImportResponse.outputRows());
  }

  @Test
  public void shouldThrowRuntimeExceptionWhenInvalidJson() {
    when(mockResponse.body()).thenReturn("invalid json");

    assertThrows(HopException.class, () -> HttpBulkImportResponse.fromHttpResponse(mockResponse));
  }
}
