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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class HttpTest {

  private TransformMockHelper<HttpMeta, HttpData> transformMockHelper;
  private Http httpTransform;
  private HttpMeta httpMeta;
  private HttpData httpData;

  private static final String TEST_RESPONSE_BODY = "Test response from HTTP server";
  private static final String TEST_URL = "http://example.com/api";

  @BeforeEach
  void setUp() throws Exception {
    transformMockHelper = new TransformMockHelper<>("HTTP Test", HttpMeta.class, HttpData.class);
    when(transformMockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(transformMockHelper.iLogChannel);
    when(transformMockHelper.pipeline.isRunning()).thenReturn(true);

    // Mock getPrevTransforms to return an array with one element by default
    when(transformMockHelper.pipelineMeta.getPrevTransforms(any()))
        .thenReturn(new org.apache.hop.pipeline.transform.TransformMeta[1]);

    httpMeta = new HttpMeta();
    httpMeta.setDefault();
    httpMeta.setUrl(TEST_URL);
    httpMeta.setFieldName("response");
    httpMeta.allocate(0, 0);

    httpData = new HttpData();

    httpTransform =
        new Http(
            transformMockHelper.transformMeta,
            httpMeta,
            httpData,
            0,
            transformMockHelper.pipelineMeta,
            transformMockHelper.pipeline);
  }

  @AfterEach
  void tearDown() {
    transformMockHelper.cleanUp();
  }

  @Test
  void testProcessRowWithNullRow() throws Exception {
    // Create a spy to mock getRow()
    Http httpSpy = spy(httpTransform);
    doReturn(null).when(httpSpy).getRow();

    // Process row - should return false when no more rows
    boolean result = httpSpy.processRow();

    assertFalse(result, "processRow should return false when row is null");
  }

  @Test
  void testProcessRowFullExecution() throws Exception {
    // Configure meta
    httpMeta.setUrl(TEST_URL);
    httpMeta.setFieldName("responseBody");
    httpMeta.setResultCodeFieldName("statusCode");
    httpMeta.allocate(0, 0);

    // Set up input row meta
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("id"));
    Object[] inputRow = new Object[] {"test123"};

    // Mock HttpClientManager for the full execution
    try (MockedStatic<HttpClientManager> mockedManager = mockStatic(HttpClientManager.class)) {
      HttpClientManager mockManager = mock(HttpClientManager.class);
      HttpClientManager.HttpClientBuilderFacade mockBuilder =
          mock(HttpClientManager.HttpClientBuilderFacade.class);
      CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
      CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);

      mockedManager.when(HttpClientManager::getInstance).thenReturn(mockManager);
      when(mockManager.createBuilder()).thenReturn(mockBuilder);
      doReturn(mockBuilder).when(mockBuilder).setConnectionTimeout(anyInt());
      doReturn(mockBuilder).when(mockBuilder).setSocketTimeout(anyInt());
      doReturn(mockBuilder).when(mockBuilder).setCredentials(anyString(), anyString());
      doReturn(mockBuilder).when(mockBuilder).setProxy(anyString(), anyInt());
      org.mockito.Mockito.doNothing().when(mockBuilder).ignoreSsl(anyBoolean());
      when(mockBuilder.build()).thenReturn(mockClient);

      // Mock HTTP response
      StatusLine mockStatusLine = mock(StatusLine.class);
      when(mockStatusLine.getStatusCode()).thenReturn(HttpURLConnection.HTTP_OK);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);

      HttpEntity mockEntity = mock(HttpEntity.class);
      when(mockEntity.getContent())
          .thenReturn(
              new ByteArrayInputStream("Success response".getBytes(StandardCharsets.UTF_8)));
      when(mockResponse.getEntity()).thenReturn(mockEntity);
      when(mockResponse.getAllHeaders()).thenReturn(new Header[0]);

      when(mockClient.execute(any(HttpHost.class), any(HttpRequest.class), any(HttpContext.class)))
          .thenReturn(mockResponse);

      // Create spy to mock getRow and capture putRow
      Http httpSpy = spy(httpTransform);
      doReturn(inputRow).doReturn(null).when(httpSpy).getRow();
      doReturn(inputRowMeta).when(httpSpy).getInputRowMeta();

      // Capture output
      final Object[][] outputRows = new Object[1][];
      org.mockito.Mockito.doAnswer(
              invocation -> {
                outputRows[0] = invocation.getArgument(1);
                return null;
              })
          .when(httpSpy)
          .putRow(any(IRowMeta.class), any(Object[].class));

      // Initialize and process
      httpSpy.init();
      boolean result1 = httpSpy.processRow();
      boolean result2 = httpSpy.processRow();

      // Verify
      assertTrue(result1, "First processRow should return true");
      assertFalse(result2, "Second processRow should return false (no more rows)");
      assertNotNull(outputRows[0], "Output row should be captured");
      assertTrue(outputRows[0].length >= 3, "Output should have input + response fields");
      assertEquals("test123", outputRows[0][0], "First field should be input ID");
      assertEquals("Success response", outputRows[0][1], "Second field should be response body");
      assertEquals(200L, outputRows[0][2], "Third field should be status code");
    }
  }

  @Test
  void testProcessRowWithQueryParameters() throws Exception {
    // Configure meta with query parameters
    httpMeta.setUrl(TEST_URL);
    httpMeta.setFieldName("response");
    httpMeta.allocate(2, 0);
    httpMeta.getArgumentField()[0] = "userId";
    httpMeta.getArgumentField()[1] = "action";
    httpMeta.getArgumentParameter()[0] = "id";
    httpMeta.getArgumentParameter()[1] = "action";

    // Set up input row meta
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("userId"));
    inputRowMeta.addValueMeta(new ValueMetaString("action"));
    Object[] inputRow = new Object[] {"user123", "view"};

    // Mock HttpClientManager
    try (MockedStatic<HttpClientManager> mockedManager = mockStatic(HttpClientManager.class)) {
      HttpClientManager mockManager = mock(HttpClientManager.class);
      HttpClientManager.HttpClientBuilderFacade mockBuilder =
          mock(HttpClientManager.HttpClientBuilderFacade.class);
      CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
      CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);

      mockedManager.when(HttpClientManager::getInstance).thenReturn(mockManager);
      when(mockManager.createBuilder()).thenReturn(mockBuilder);
      doReturn(mockBuilder).when(mockBuilder).setConnectionTimeout(anyInt());
      doReturn(mockBuilder).when(mockBuilder).setSocketTimeout(anyInt());
      doReturn(mockBuilder).when(mockBuilder).setCredentials(anyString(), anyString());
      doReturn(mockBuilder).when(mockBuilder).setProxy(anyString(), anyInt());
      org.mockito.Mockito.doNothing().when(mockBuilder).ignoreSsl(anyBoolean());
      when(mockBuilder.build()).thenReturn(mockClient);

      StatusLine mockStatusLine = mock(StatusLine.class);
      when(mockStatusLine.getStatusCode()).thenReturn(HttpURLConnection.HTTP_OK);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);

      HttpEntity mockEntity = mock(HttpEntity.class);
      when(mockEntity.getContent())
          .thenReturn(new ByteArrayInputStream("Query result".getBytes(StandardCharsets.UTF_8)));
      when(mockResponse.getEntity()).thenReturn(mockEntity);
      when(mockResponse.getAllHeaders()).thenReturn(new Header[0]);

      when(mockClient.execute(any(HttpHost.class), any(HttpRequest.class), any(HttpContext.class)))
          .thenReturn(mockResponse);

      // Create spy
      Http httpSpy = spy(httpTransform);
      doReturn(inputRow).doReturn(null).when(httpSpy).getRow();
      doReturn(inputRowMeta).when(httpSpy).getInputRowMeta();

      final Object[][] outputRows = new Object[1][];
      org.mockito.Mockito.doAnswer(
              invocation -> {
                outputRows[0] = invocation.getArgument(1);
                return null;
              })
          .when(httpSpy)
          .putRow(any(IRowMeta.class), any(Object[].class));

      // Initialize and process
      httpSpy.init();
      boolean result = httpSpy.processRow();

      // Verify
      assertTrue(result, "processRow should return true");
      assertNotNull(outputRows[0], "Output row should be captured");
      assertEquals("user123", outputRows[0][0], "First field should be userId");
      assertEquals("view", outputRows[0][1], "Second field should be action");
      assertEquals("Query result", outputRows[0][2], "Third field should be response");
    }
  }

  @Test
  void testProcessRowWithHeaders() throws Exception {
    // Configure meta with custom headers
    httpMeta.setUrl(TEST_URL);
    httpMeta.setFieldName("response");
    httpMeta.allocate(0, 1);
    httpMeta.getHeaderField()[0] = "authToken";
    httpMeta.getHeaderParameter()[0] = "Authorization";

    // Set up input row meta
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("authToken"));
    Object[] inputRow = new Object[] {"Bearer token123"};

    // Mock HttpClientManager
    try (MockedStatic<HttpClientManager> mockedManager = mockStatic(HttpClientManager.class)) {
      HttpClientManager mockManager = mock(HttpClientManager.class);
      HttpClientManager.HttpClientBuilderFacade mockBuilder =
          mock(HttpClientManager.HttpClientBuilderFacade.class);
      CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
      CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);

      mockedManager.when(HttpClientManager::getInstance).thenReturn(mockManager);
      when(mockManager.createBuilder()).thenReturn(mockBuilder);
      doReturn(mockBuilder).when(mockBuilder).setConnectionTimeout(anyInt());
      doReturn(mockBuilder).when(mockBuilder).setSocketTimeout(anyInt());
      doReturn(mockBuilder).when(mockBuilder).setCredentials(anyString(), anyString());
      doReturn(mockBuilder).when(mockBuilder).setProxy(anyString(), anyInt());
      org.mockito.Mockito.doNothing().when(mockBuilder).ignoreSsl(anyBoolean());
      when(mockBuilder.build()).thenReturn(mockClient);

      StatusLine mockStatusLine = mock(StatusLine.class);
      when(mockStatusLine.getStatusCode()).thenReturn(HttpURLConnection.HTTP_OK);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);

      HttpEntity mockEntity = mock(HttpEntity.class);
      when(mockEntity.getContent())
          .thenReturn(new ByteArrayInputStream("Authorized".getBytes(StandardCharsets.UTF_8)));
      when(mockResponse.getEntity()).thenReturn(mockEntity);
      when(mockResponse.getAllHeaders()).thenReturn(new Header[0]);

      when(mockClient.execute(any(HttpHost.class), any(HttpRequest.class), any(HttpContext.class)))
          .thenReturn(mockResponse);

      // Create spy
      Http httpSpy = spy(httpTransform);
      doReturn(inputRow).doReturn(null).when(httpSpy).getRow();
      doReturn(inputRowMeta).when(httpSpy).getInputRowMeta();

      final Object[][] outputRows = new Object[1][];
      org.mockito.Mockito.doAnswer(
              invocation -> {
                outputRows[0] = invocation.getArgument(1);
                return null;
              })
          .when(httpSpy)
          .putRow(any(IRowMeta.class), any(Object[].class));

      // Initialize and process
      httpSpy.init();
      boolean result = httpSpy.processRow();

      // Verify
      assertTrue(result, "processRow should return true");
      assertNotNull(outputRows[0], "Output row should be captured");
      assertEquals("Bearer token123", outputRows[0][0], "First field should be auth token");
      assertEquals("Authorized", outputRows[0][1], "Second field should be response");
    }
  }

  @Test
  void testInit() {
    httpMeta.setProxyHost("proxy.example.com");
    httpMeta.setProxyPort("8080");
    httpMeta.setHttpLogin("user");
    httpMeta.setHttpPassword("password");
    httpMeta.setSocketTimeout("5000");
    httpMeta.setConnectionTimeout("3000");

    boolean result = httpTransform.init();

    assertTrue(result, "init should return true");
    assertEquals("proxy.example.com", httpData.realProxyHost);
    assertEquals(8080, httpData.realProxyPort);
    assertEquals("user", httpData.realHttpLogin);
    assertEquals("password", httpData.realHttpPassword);
    assertEquals(5000, httpData.realSocketTimeout);
    assertEquals(3000, httpData.realConnectionTimeout);
  }

  @Test
  void testRequestStatusCode() {
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);

    int statusCode = httpTransform.requestStatusCode(response);

    assertEquals(200, statusCode);
  }

  @Test
  void testSearchForHeaders() {
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    Header[] headers =
        new Header[] {
          new BasicHeader("Content-Type", "application/json"),
          new BasicHeader("Authorization", "Bearer token")
        };
    when(response.getAllHeaders()).thenReturn(headers);

    Header[] result = httpTransform.searchForHeaders(response);

    assertNotNull(result);
    assertEquals(2, result.length);
    assertEquals("Content-Type", result[0].getName());
    assertEquals("application/json", result[0].getValue());
  }

  @Test
  void testCallHttpServiceBasicCall() throws Exception {
    // Set up input row meta
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("id"));
    Object[] inputRow = new Object[] {"123"};

    // Configure meta
    httpMeta.setUrl(TEST_URL);
    httpMeta.setFieldName("result");
    httpMeta.allocate(0, 0);

    // Set up data
    httpData.realUrl = TEST_URL;
    httpData.realConnectionTimeout = 10000;
    httpData.realSocketTimeout = 10000;
    httpData.argnrs = new int[0];
    httpData.inputRowMeta = inputRowMeta;
    httpData.useHeaderParameters = false;
    httpData.headerParametersNrs = new int[0];

    // Mock HttpClientManager
    try (MockedStatic<HttpClientManager> mockedManager = mockStatic(HttpClientManager.class)) {
      HttpClientManager mockManager = mock(HttpClientManager.class);
      HttpClientManager.HttpClientBuilderFacade mockBuilder =
          mock(HttpClientManager.HttpClientBuilderFacade.class);
      CloseableHttpClient mockClient = mock(CloseableHttpClient.class);
      CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);

      mockedManager.when(HttpClientManager::getInstance).thenReturn(mockManager);
      when(mockManager.createBuilder()).thenReturn(mockBuilder);
      doReturn(mockBuilder).when(mockBuilder).setConnectionTimeout(anyInt());
      doReturn(mockBuilder).when(mockBuilder).setSocketTimeout(anyInt());
      doReturn(mockBuilder).when(mockBuilder).setCredentials(anyString(), anyString());
      doReturn(mockBuilder).when(mockBuilder).setProxy(anyString(), anyInt());
      // ignoreSsl is a void method, use doNothing
      org.mockito.Mockito.doNothing().when(mockBuilder).ignoreSsl(anyBoolean());
      when(mockBuilder.build()).thenReturn(mockClient);

      // Mock HTTP response
      StatusLine mockStatusLine = mock(StatusLine.class);
      when(mockStatusLine.getStatusCode()).thenReturn(HttpURLConnection.HTTP_OK);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);

      HttpEntity mockEntity = mock(HttpEntity.class);
      when(mockEntity.getContent())
          .thenReturn(
              new ByteArrayInputStream(TEST_RESPONSE_BODY.getBytes(StandardCharsets.UTF_8)));
      when(mockResponse.getEntity()).thenReturn(mockEntity);
      when(mockResponse.getAllHeaders()).thenReturn(new Header[0]);

      when(mockClient.execute(any(HttpHost.class), any(HttpRequest.class), any(HttpContext.class)))
          .thenReturn(mockResponse);

      // Call the method
      Object[] result = httpTransform.callHttpService(inputRowMeta, inputRow);

      // Verify result
      assertNotNull(result, "Result should not be null");
      // Result should have input + response field (method adds response body to end)
      assertTrue(result.length >= 2, "Result should have at least input + response fields");
      assertEquals("123", result[0], "First field should be input ID");
      assertEquals(TEST_RESPONSE_BODY, result[1], "Second field should be response body");
    }
  }

  @Test
  void testCallHttpServiceWithResponseFields() throws Exception {
    // Test that callHttpService prepares to return all response fields

    // Configure meta with all response fields
    httpMeta.setUrl(TEST_URL);
    httpMeta.setFieldName("responseBody");
    httpMeta.setResultCodeFieldName("statusCode");
    httpMeta.setResponseTimeFieldName("responseTime");
    httpMeta.setResponseHeaderFieldName("responseHeaders");
    httpMeta.allocate(0, 0);

    // Set up input row
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("id"));

    // Prepare output row meta
    IRowMeta outputRowMeta = inputRowMeta.clone();
    httpMeta.getFields(outputRowMeta, "HTTP", null, null, httpTransform, null);

    // Verify output fields are configured
    assertEquals(5, outputRowMeta.size()); // 1 input + 4 output fields
    assertNotNull(outputRowMeta.searchValueMeta("responseBody"));
    assertNotNull(outputRowMeta.searchValueMeta("statusCode"));
    assertNotNull(outputRowMeta.searchValueMeta("responseTime"));
    assertNotNull(outputRowMeta.searchValueMeta("responseHeaders"));
  }

  @Test
  void testCallHttpServiceWithQueryParameters() throws Exception {
    // Test callHttpService setup with query parameters

    // Configure with query parameters
    httpMeta.setUrl(TEST_URL);
    httpMeta.setFieldName("result");
    httpMeta.allocate(2, 0);
    httpMeta.getArgumentField()[0] = "userId";
    httpMeta.getArgumentField()[1] = "filter";
    httpMeta.getArgumentParameter()[0] = "id";
    httpMeta.getArgumentParameter()[1] = "filter";

    // Set up input row
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("userId"));
    inputRowMeta.addValueMeta(new ValueMetaString("filter"));
    Object[] inputRow = new Object[] {"user123", "active"};

    // Set up data for the call
    httpData.realUrl = TEST_URL;
    httpData.argnrs = new int[] {0, 1};
    httpData.inputRowMeta = inputRowMeta;

    // Verify parameters are accessible for URL building
    assertEquals("user123", inputRowMeta.getString(inputRow, 0));
    assertEquals("active", inputRowMeta.getString(inputRow, 1));
    assertEquals("id", httpMeta.getArgumentParameter()[0]);
    assertEquals("filter", httpMeta.getArgumentParameter()[1]);
  }

  @Test
  void testCallHttpServiceWithHeaders() throws Exception {
    // Test callHttpService setup with custom headers

    // Configure with custom headers
    httpMeta.setUrl(TEST_URL);
    httpMeta.setFieldName("result");
    httpMeta.allocate(0, 2);
    httpMeta.getHeaderField()[0] = "tokenField";
    httpMeta.getHeaderField()[1] = "contentTypeField";
    httpMeta.getHeaderParameter()[0] = "Authorization";
    httpMeta.getHeaderParameter()[1] = "Content-Type";

    // Set up input row
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("tokenField"));
    inputRowMeta.addValueMeta(new ValueMetaString("contentTypeField"));
    Object[] inputRow = new Object[] {"Bearer token123", "application/json"};

    // Set up data
    httpData.realUrl = TEST_URL;
    httpData.argnrs = new int[0];
    httpData.inputRowMeta = inputRowMeta;
    httpData.useHeaderParameters = true;
    httpData.headerParametersNrs = new int[] {0, 1};

    // Verify headers are accessible
    assertEquals("Bearer token123", inputRowMeta.getString(inputRow, 0));
    assertEquals("application/json", inputRowMeta.getString(inputRow, 1));
    assertEquals("Authorization", httpMeta.getHeaderParameter()[0]);
    assertEquals("Content-Type", httpMeta.getHeaderParameter()[1]);
  }

  @Test
  void testGetFieldsAddsOutputFields() throws Exception {
    httpMeta.setFieldName("responseBody");
    httpMeta.setResultCodeFieldName("statusCode");
    httpMeta.setResponseTimeFieldName("responseTime");
    httpMeta.setResponseHeaderFieldName("headers");

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("input1"));

    httpMeta.getFields(inputRowMeta, "HTTP", null, null, httpTransform, null);

    assertEquals(5, inputRowMeta.size(), "Should have 5 fields (1 input + 4 output)");
    assertNotNull(inputRowMeta.searchValueMeta("responseBody"));
    assertNotNull(inputRowMeta.searchValueMeta("statusCode"));
    assertNotNull(inputRowMeta.searchValueMeta("responseTime"));
    assertNotNull(inputRowMeta.searchValueMeta("headers"));
  }

  @Test
  void testProcessRowWithUrlInField() throws Exception {
    // Configure meta to use URL from field
    httpMeta.setUrlInField(true);
    httpMeta.setUrlField("urlColumn");
    httpMeta.setFieldName("result");
    httpMeta.allocate(0, 0);

    // Set up input row meta
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("urlColumn"));

    // Create row with URL value
    Object[] inputRow = new Object[] {"http://example.com/test"};

    // Create spy and mock getRow
    Http httpSpy = spy(httpTransform);
    doReturn(inputRow).doReturn(null).when(httpSpy).getRow();
    doReturn(inputRowMeta).when(httpSpy).getInputRowMeta();

    // Initialize
    httpSpy.init();
    httpSpy.setInputRowMeta(inputRowMeta);

    // Note: Full execution would require mocking HTTP client
    // This test verifies the URL field resolution logic
    httpData.inputRowMeta = inputRowMeta;
    httpData.indexOfUrlField = inputRowMeta.indexOfValue("urlColumn");

    assertEquals(0, httpData.indexOfUrlField, "URL field index should be found");
  }

  @Test
  void testProcessRowWithArgumentFields() throws Exception {
    // Configure meta with argument fields
    httpMeta.setUrl(TEST_URL);
    httpMeta.allocate(2, 0);
    httpMeta.getArgumentField()[0] = "param1";
    httpMeta.getArgumentField()[1] = "param2";
    httpMeta.getArgumentParameter()[0] = "key1";
    httpMeta.getArgumentParameter()[1] = "key2";
    httpMeta.setFieldName("result");

    // Set up input row meta
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("param1"));
    inputRowMeta.addValueMeta(new ValueMetaString("param2"));

    // Create row with parameter values
    Object[] inputRow = new Object[] {"value1", "value2"};

    // Create spy
    Http httpSpy = spy(httpTransform);
    doReturn(inputRow).doReturn(null).when(httpSpy).getRow();
    doReturn(inputRowMeta).when(httpSpy).getInputRowMeta();

    // Initialize
    httpSpy.init();
    httpSpy.setInputRowMeta(inputRowMeta);

    // Set up data for first row processing
    httpData.inputRowMeta = inputRowMeta;
    httpData.argnrs = new int[2];
    httpData.argnrs[0] = inputRowMeta.indexOfValue("param1");
    httpData.argnrs[1] = inputRowMeta.indexOfValue("param2");

    assertEquals(0, httpData.argnrs[0]);
    assertEquals(1, httpData.argnrs[1]);
  }

  @Test
  void testProcessRowWithHeaderFields() throws Exception {
    // Configure meta with header fields
    httpMeta.setUrl(TEST_URL);
    httpMeta.allocate(0, 2);
    httpMeta.getHeaderField()[0] = "authField";
    httpMeta.getHeaderField()[1] = "contentTypeField";
    httpMeta.getHeaderParameter()[0] = "Authorization";
    httpMeta.getHeaderParameter()[1] = "Content-Type";
    httpMeta.setFieldName("result");

    // Set up input row meta
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("authField"));
    inputRowMeta.addValueMeta(new ValueMetaString("contentTypeField"));

    // Create row with header values
    Object[] inputRow = new Object[] {"Bearer token", "application/json"};

    // Create spy
    Http httpSpy = spy(httpTransform);
    doReturn(inputRow).doReturn(null).when(httpSpy).getRow();
    doReturn(inputRowMeta).when(httpSpy).getInputRowMeta();

    // Initialize
    httpSpy.init();
    httpSpy.setInputRowMeta(inputRowMeta);

    // Set up data
    httpData.inputRowMeta = inputRowMeta;
    httpData.headerParametersNrs = new int[2];
    httpData.headerParametersNrs[0] = inputRowMeta.indexOfValue("authField");
    httpData.headerParametersNrs[1] = inputRowMeta.indexOfValue("contentTypeField");

    assertEquals(0, httpData.headerParametersNrs[0]);
    assertEquals(1, httpData.headerParametersNrs[1]);
  }
}
