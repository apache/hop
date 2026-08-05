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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.fields.HeaderField;
import org.apache.hop.pipeline.transforms.rest.fields.MatrixParameterField;
import org.apache.hop.pipeline.transforms.rest.fields.ParameterField;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Covers {@link Rest#callRest(Object[])} one HTTP verb and one input-field kind at a time. The
 * transform is given a stubbed client through {@link RestData#client}, so each test asserts on the
 * request that reached the wire — see {@link FakeHttpClient#captured()}.
 */
class RestCallRestTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeAll
  static void beforeClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @BeforeEach
  void setUp() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  @Test
  void testCallRestWithGetMethod() throws HopException {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");
    meta.getResultField().setCode("statusCode");
    meta.getResultField().setResponseTime("responseTime");
    meta.getResultField().setResponseHeader("headers");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = "http://example.com";
    data.resultFieldName = "result";
    data.resultCodeFieldName = "statusCode";
    data.resultResponseFieldName = "responseTime";
    data.resultHeaderFieldName = "headers";
    data.inputRowMeta = rowMeta("field1");

    Rest rest = transform(meta, data, json(200, "{\"result\":\"success\"}"));

    Object[] outputRow = rest.callRest(new Object[] {"value1"});

    assertNotNull(outputRow);
    assertTrue(outputRow.length >= 1);
    // The result fields are appended to the input row, in declaration order.
    assertEquals("value1", outputRow[0]);
    assertEquals("{\"result\":\"success\"}", outputRow[1]);
    assertEquals(200L, outputRow[2]);
    assertNotNull(outputRow[3]); // response time
    assertNotNull(outputRow[4]); // headers

    assertEquals("GET", FakeHttpClient.captured().getMethod());
    // A rootless URL picks up the empty path the request line needs.
    assertEquals("http://example.com/", uri());
  }

  /**
   * The URL is sent as configured. Only an empty path becomes {@code /}, because the request line
   * has no way to express "no path" — {@code /api} and {@code /api/} are different resources on
   * many servers, so neither may be rewritten into the other.
   */
  @ParameterizedTest
  @CsvSource({
    "http://example.com, http://example.com/",
    "http://example.com/, http://example.com/",
    "http://example.com/api, http://example.com/api",
    "http://example.com/api/, http://example.com/api/",
    "http://example.com/api?x=1, http://example.com/api?x=1"
  })
  void testTheConfiguredUrlIsSentUnaltered(String configured, String expected) throws HopException {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl(configured);
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = configured;
    data.resultFieldName = "result";
    data.inputRowMeta = rowMeta();

    Rest rest = transform(meta, data, json(200, "{}"));
    rest.callRest(new Object[] {});

    assertEquals(expected, uri());
  }

  @Test
  void testCallRestWithPostMethod() throws HopException {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_POST);
    meta.setUrl("http://example.com/api");
    meta.setBodyField("body");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_POST;
    data.realUrl = "http://example.com/api";
    data.resultFieldName = "result";
    data.useBody = true;
    data.indexOfBodyField = 1;
    data.inputRowMeta = rowMeta("field1", "body");

    Rest rest = transform(meta, data, json(201, "{\"id\":123}"));

    Object[] outputRow = rest.callRest(new Object[] {"value1", "{\"name\":\"test\"}"});

    assertNotNull(outputRow);
    assertEquals("{\"id\":123}", outputRow[2]);
    assertEquals("POST", FakeHttpClient.captured().getMethod());
    assertEquals("{\"name\":\"test\"}", requestBody());
  }

  @Test
  void testCallRestPostWithoutBodySendsEmptyEntity() throws HopException {
    // Issue #7621: a body-less POST must still produce a Content-Length header. The transform
    // normalizes a null body to an empty string, so an empty entity is sent rather than none at
    // all — without one the request would go out with no Content-Length.
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_POST);
    meta.setUrl("http://example.com/api");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_POST;
    data.realUrl = "http://example.com/api";
    data.resultFieldName = "result";
    data.useBody = false; // no body configured -> entity is null
    data.inputRowMeta = rowMeta("field1");

    Rest rest = transform(meta, data, json(200, "{}"));

    Object[] outputRow = rest.callRest(new Object[] {"value1"});

    assertNotNull(outputRow);
    assertNotNull(FakeHttpClient.captured().getEntity());
    assertEquals(0, FakeHttpClient.captured().getEntity().getContentLength());
    assertEquals("", requestBody());
  }

  @Test
  void testCallRestWithPutMethod() throws HopException {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_PUT);
    meta.setUrl("http://example.com/api/1");
    meta.setBodyField("body");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_PUT;
    data.realUrl = "http://example.com/api/1";
    data.resultFieldName = "result";
    data.useBody = true;
    data.indexOfBodyField = 0;
    data.inputRowMeta = rowMeta("body");

    Rest rest = transform(meta, data, json(200, "{\"updated\":true}"));

    Object[] outputRow = rest.callRest(new Object[] {"{\"field\":\"value\"}"});

    assertNotNull(outputRow);
    assertEquals("{\"updated\":true}", outputRow[1]);
    assertEquals("PUT", FakeHttpClient.captured().getMethod());
    assertEquals("{\"field\":\"value\"}", requestBody());
  }

  @Test
  void testCallRestWithDeleteMethod() throws HopException {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_DELETE);
    meta.setUrl("http://example.com/api/123");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");
    meta.getResultField().setCode("statusCode");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_DELETE;
    data.realUrl = "http://example.com/api/123";
    data.resultFieldName = "result";
    data.resultCodeFieldName = "statusCode";
    data.inputRowMeta = rowMeta("id");

    Rest rest = transform(meta, data, json(204, ""));

    Object[] outputRow = rest.callRest(new Object[] {"123"});

    assertNotNull(outputRow);
    assertEquals(204L, outputRow[2]);
    assertEquals("DELETE", FakeHttpClient.captured().getMethod());
  }

  @Test
  void testCallRestWithHeadMethod() throws HopException {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_HEAD);
    meta.setUrl("http://example.com/api");
    meta.setResultField(new ResultField());
    meta.getResultField().setCode("statusCode");
    meta.getResultField().setResponseHeader("headers");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_HEAD;
    data.realUrl = "http://example.com/api";
    data.resultCodeFieldName = "statusCode";
    data.resultHeaderFieldName = "headers";
    data.inputRowMeta = rowMeta();

    Rest rest = transform(meta, data, json(200, ""));

    Object[] outputRow = rest.callRest(new Object[] {});

    assertNotNull(outputRow);
    assertEquals(200L, outputRow[0]);
    assertEquals("HEAD", FakeHttpClient.captured().getMethod());
  }

  @Test
  void testCallRestWithOptionsMethod() throws HopException {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_OPTIONS);
    meta.setUrl("http://example.com/api");
    meta.setResultField(new ResultField());
    meta.getResultField().setResponseHeader("allowedMethods");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_OPTIONS;
    data.realUrl = "http://example.com/api";
    data.resultHeaderFieldName = "allowedMethods";
    data.inputRowMeta = rowMeta();

    Rest rest =
        transform(meta, data, FakeHttpClient.returning(200, "", Map.of("Allow", "GET,PUT")));

    Object[] outputRow = rest.callRest(new Object[] {});

    assertNotNull(outputRow);
    assertEquals("OPTIONS", FakeHttpClient.captured().getMethod());
    assertTrue(String.valueOf(outputRow[0]).contains("Allow"));
  }

  @Test
  void testCallRestWithPatchMethod() throws HopException {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_PATCH);
    meta.setUrl("http://example.com/api");
    meta.setBodyField("body");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_PATCH;
    data.realUrl = "http://example.com/api";
    data.resultFieldName = "result";
    data.useBody = true;
    data.indexOfBodyField = 0;
    data.inputRowMeta = rowMeta("body");

    Rest rest = transform(meta, data, json(200, "{\"patched\":true}"));

    Object[] outputRow = rest.callRest(new Object[] {"{\"field\":\"value\"}"});

    assertNotNull(outputRow);
    assertEquals("{\"patched\":true}", outputRow[1]);
    assertEquals(RestMeta.HTTP_METHOD_PATCH, FakeHttpClient.captured().getMethod());
    assertEquals("{\"field\":\"value\"}", requestBody());
  }

  @Test
  void testCallRestWithQueryParameters() throws HopException {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com/api");
    List<ParameterField> params = new ArrayList<>();
    params.add(new ParameterField("searchField", "search"));
    params.add(new ParameterField("limitField", "limit"));
    meta.setParameterFields(params);
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = "http://example.com/api";
    data.resultFieldName = "result";
    data.useParams = true;
    data.nrParams = 2;
    data.paramNames = new String[] {"search", "limit"};
    data.indexOfParamFields = new int[] {0, 1};
    data.inputRowMeta = rowMeta("searchField", "limitField");

    Rest rest = transform(meta, data, json(200, "[{\"id\":1},{\"id\":2}]"));

    Object[] outputRow = rest.callRest(new Object[] {"test", "10"});

    assertNotNull(outputRow);
    assertEquals("[{\"id\":1},{\"id\":2}]", outputRow[2]);
    assertEquals("http://example.com/api?search=test&limit=10", uri());
  }

  @Test
  void testCallRestWithHeaders() throws HopException {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com/api");
    List<HeaderField> headerFields = new ArrayList<>();
    headerFields.add(new HeaderField("authField", "Authorization"));
    headerFields.add(new HeaderField("typeField", "Content-Type"));
    meta.setHeaderFields(headerFields);
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = "http://example.com/api";
    data.resultFieldName = "result";
    data.useHeaders = true;
    data.nrheader = 2;
    data.headerNames = new String[] {"Authorization", "Content-Type"};
    data.indexOfHeaderFields = new int[] {0, 1};
    data.inputRowMeta = rowMeta("authField", "typeField");

    Rest rest = transform(meta, data, json(200, "{\"authenticated\":true}"));

    Object[] outputRow = rest.callRest(new Object[] {"Bearer token123", "application/json"});

    assertNotNull(outputRow);
    assertEquals("{\"authenticated\":true}", outputRow[2]);
    ClassicHttpRequest request = FakeHttpClient.captured();
    assertEquals("Bearer token123", request.getFirstHeader("Authorization").getValue());
    assertEquals("application/json", request.getFirstHeader("Content-Type").getValue());
  }

  @Test
  void testCallRestWithMatrixParameters() throws HopException {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com/api");
    List<MatrixParameterField> matrixParams = new ArrayList<>();
    matrixParams.add(new MatrixParameterField("authorField", "author"));
    matrixParams.add(new MatrixParameterField("yearField", "year"));
    meta.setMatrixParameterFields(matrixParams);
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = "http://example.com/api";
    data.resultFieldName = "result";
    data.useMatrixParams = true;
    data.nrMatrixParams = 2;
    data.matrixParamNames = new String[] {"author", "year"};
    data.indexOfMatrixParamFields = new int[] {0, 1};
    data.inputRowMeta = rowMeta("authorField", "yearField");

    Rest rest = transform(meta, data, json(200, "[{\"book\":\"title\"}]"));

    Object[] outputRow = rest.callRest(new Object[] {"John Doe", "2023"});

    assertNotNull(outputRow);
    assertEquals("[{\"book\":\"title\"}]", outputRow[2]);
    // Matrix parameters attach to the last path segment. The space is percent-encoded rather than
    // form-encoded: a '+' in a path is a literal plus, not a space.
    assertEquals("http://example.com/api;author=John%20Doe;year=2023", uri());
  }

  @Test
  void testCallRestWithMatrixParametersBeforeQueryString() throws HopException {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com/api?page=1");
    List<MatrixParameterField> matrixParams = new ArrayList<>();
    matrixParams.add(new MatrixParameterField("authorField", "author"));
    meta.setMatrixParameterFields(matrixParams);
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = "http://example.com/api?page=1";
    data.resultFieldName = "result";
    data.useMatrixParams = true;
    data.nrMatrixParams = 1;
    data.matrixParamNames = new String[] {"author"};
    data.indexOfMatrixParamFields = new int[] {0};
    data.inputRowMeta = rowMeta("authorField");

    Rest rest = transform(meta, data, json(200, "[]"));

    rest.callRest(new Object[] {"orwell"});

    assertEquals("http://example.com/api;author=orwell?page=1", uri());
  }

  @Test
  void testCallRestWithDynamicUrl() throws HopException {
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrlInField(true);
    meta.setUrlField("urlField");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.resultFieldName = "result";
    data.indexOfUrlField = 0;
    data.inputRowMeta = rowMeta("urlField");

    Rest rest = transform(meta, data, json(200, "{\"dynamic\":true}"));

    Object[] outputRow = rest.callRest(new Object[] {"http://dynamic-url.com/api/resource"});

    assertNotNull(outputRow);
    assertEquals("{\"dynamic\":true}", outputRow[1]);
    assertEquals("http://dynamic-url.com/api/resource", uri());
  }

  @Test
  void testCallRestWithDynamicMethod() throws HopException {
    RestMeta meta = new RestMeta();
    meta.setDynamicMethod(true);
    meta.setMethodFieldName("methodField");
    meta.setUrl("http://example.com/api");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.realUrl = "http://example.com/api";
    data.resultFieldName = "result";
    data.indexOfMethod = 0;
    data.inputRowMeta = rowMeta("methodField");

    Rest rest = transform(meta, data, json(200, "{\"created\":true}"));

    Object[] outputRow = rest.callRest(new Object[] {"POST"});

    assertNotNull(outputRow);
    assertEquals("{\"created\":true}", outputRow[1]);
    assertEquals("POST", FakeHttpClient.captured().getMethod());
  }

  @Test
  void testCallRestWithNonStandardMethodIsSentVerbatim() throws HopException {
    // Issue #4770: the verb is a token the server defines, not one of a fixed list, so whatever is
    // configured goes on the request line unchanged.
    RestMeta meta = new RestMeta();
    meta.setMethod("PURGE");
    meta.setUrl("http://example.com/api");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = "PURGE";
    data.realUrl = "http://example.com/api";
    data.resultFieldName = "result";
    data.inputRowMeta = rowMeta();

    Rest rest = transform(meta, data, json(200, "{}"));

    assertNotNull(rest.callRest(new Object[] {}));
    assertEquals("PURGE", FakeHttpClient.captured().getMethod());
  }

  /** Builds the transform under test, wired to a stubbed client instead of a real one. */
  private static Rest transform(RestMeta meta, RestData data, CloseableHttpClient client) {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    data.client = client;

    Rest rest = new Rest(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine());
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));
    return rest;
  }

  private static CloseableHttpClient json(int status, String body) {
    return FakeHttpClient.returning(status, body, Map.of("Content-Type", "application/json"));
  }

  private static IRowMeta rowMeta(String... fieldNames) {
    IRowMeta rowMeta = new RowMeta();
    for (String fieldName : fieldNames) {
      rowMeta.addValueMeta(new ValueMetaString(fieldName));
    }
    return rowMeta;
  }

  /** The URL the captured request was actually sent to. */
  private static String uri() {
    try {
      return FakeHttpClient.captured().getUri().toString();
    } catch (Exception e) {
      throw new IllegalStateException("The captured request has no usable URI", e);
    }
  }

  private static String requestBody() {
    try {
      return EntityUtils.toString(FakeHttpClient.captured().getEntity(), StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new IllegalStateException("Unable to read the captured request body", e);
    }
  }
}
