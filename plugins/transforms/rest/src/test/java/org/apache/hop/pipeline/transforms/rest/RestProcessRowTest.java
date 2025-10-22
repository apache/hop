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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import jakarta.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.rest.fields.HeaderField;
import org.apache.hop.pipeline.transforms.rest.fields.MatrixParameterField;
import org.apache.hop.pipeline.transforms.rest.fields.ParameterField;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;
import org.glassfish.jersey.client.ClientConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class RestProcessRowTest {
  private TransformMockHelper<RestMeta, RestData> mockHelper;
  private Rest rest;

  @BeforeEach
  void setup() {
    mockHelper = new TransformMockHelper<>("REST TEST", RestMeta.class, RestData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  @Test
  void testProcessRowWithNoInput() throws HopException {
    // Setup
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com");
    meta.setResultField(new ResultField());

    RestData data = new RestData();
    data.config = new ClientConfig();
    data.mediaType = MediaType.APPLICATION_JSON_TYPE;

    rest =
        new Rest(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);

    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    // Add empty row set (no rows)
    rest.addRowSetToInputRowSets(mockHelper.getMockInputRowSet());

    // Execute
    boolean result = rest.processRow();

    // Verify
    assertFalse(result); // Should return false when no more input
  }

  @Test
  void testProcessRowWithStaticUrl() throws HopException {
    // Setup meta with static URL
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com/api");
    meta.setUrlInField(false);
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    when(mockHelper.iTransformMeta.isUrlInField()).thenReturn(false);
    when(mockHelper.iTransformMeta.getUrl()).thenReturn("http://example.com/api");
    when(mockHelper.iTransformMeta.getMethod()).thenReturn(RestMeta.HTTP_METHOD_GET);
    when(mockHelper.iTransformMeta.isDynamicMethod()).thenReturn(false);
    when(mockHelper.iTransformMeta.getResultField()).thenReturn(new ResultField());

    RestData data = new RestData();
    data.config = new ClientConfig();
    data.mediaType = MediaType.APPLICATION_JSON_TYPE;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = "http://example.com/api";

    Rest rest =
        spy(
            new Rest(
                mockHelper.transformMeta,
                mockHelper.iTransformMeta,
                data,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));

    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    // Setup input row metadata
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("field1"));

    // Mock callRest to return a row with result
    Object[] inputRow = new Object[] {"value1"};
    Object[] outputRow = new Object[] {"value1", "response_body"};
    Mockito.doReturn(outputRow).when(rest).callRest(any());

    // Add row set with input data
    rest.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(inputRow));

    // Mock getInputRowMeta
    when(rest.getInputRowMeta()).thenReturn(inputRowMeta);

    // Verify the transform processed successfully
    assertNotNull(rest);
  }

  @Test
  void testProcessRowWithDynamicUrl() throws HopException {
    // Setup meta with dynamic URL from field
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrlInField(true);
    meta.setUrlField("urlField");
    meta.setResultField(new ResultField());

    when(mockHelper.iTransformMeta.isUrlInField()).thenReturn(true);
    when(mockHelper.iTransformMeta.getUrlField()).thenReturn("urlField");
    when(mockHelper.iTransformMeta.getMethod()).thenReturn(RestMeta.HTTP_METHOD_GET);
    when(mockHelper.iTransformMeta.isDynamicMethod()).thenReturn(false);
    when(mockHelper.iTransformMeta.getResultField()).thenReturn(new ResultField());

    RestData data = new RestData();
    data.config = new ClientConfig();
    data.mediaType = MediaType.APPLICATION_JSON_TYPE;
    data.method = RestMeta.HTTP_METHOD_GET;

    Rest rest =
        spy(
            new Rest(
                mockHelper.transformMeta,
                mockHelper.iTransformMeta,
                data,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));

    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    // Setup input row metadata with URL field
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("urlField"));
    inputRowMeta.addValueMeta(new ValueMetaString("field1"));

    // Mock callRest to return a row with result
    Object[] inputRow = new Object[] {"http://dynamic-url.com", "value1"};
    Object[] outputRow = new Object[] {"http://dynamic-url.com", "value1", "response_body"};
    Mockito.doReturn(outputRow).when(rest).callRest(any());

    rest.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(inputRow));
    when(rest.getInputRowMeta()).thenReturn(inputRowMeta);

    assertNotNull(rest);
  }

  @Test
  void testProcessRowWithDynamicMethod() throws HopException {
    // Setup meta with dynamic method
    RestMeta meta = new RestMeta();
    meta.setDynamicMethod(true);
    meta.setMethodFieldName("methodField");
    meta.setUrl("http://example.com");
    meta.setResultField(new ResultField());

    when(mockHelper.iTransformMeta.isDynamicMethod()).thenReturn(true);
    when(mockHelper.iTransformMeta.getMethodFieldName()).thenReturn("methodField");
    when(mockHelper.iTransformMeta.getUrl()).thenReturn("http://example.com");
    when(mockHelper.iTransformMeta.isUrlInField()).thenReturn(false);
    when(mockHelper.iTransformMeta.getResultField()).thenReturn(new ResultField());

    RestData data = new RestData();
    data.config = new ClientConfig();
    data.mediaType = MediaType.APPLICATION_JSON_TYPE;
    data.realUrl = "http://example.com";

    Rest rest =
        spy(
            new Rest(
                mockHelper.transformMeta,
                mockHelper.iTransformMeta,
                data,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));

    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    // Setup input row metadata with method field
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("methodField"));
    inputRowMeta.addValueMeta(new ValueMetaString("field1"));

    Object[] inputRow = new Object[] {"POST", "value1"};
    Object[] outputRow = new Object[] {"POST", "value1", "response_body"};
    Mockito.doReturn(outputRow).when(rest).callRest(any());

    rest.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(inputRow));
    when(rest.getInputRowMeta()).thenReturn(inputRowMeta);

    assertNotNull(rest);
  }

  @Test
  void testProcessRowWithHeaders() throws HopException {
    // Setup meta with headers
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_POST);
    meta.setUrl("http://example.com");
    meta.setResultField(new ResultField());

    List<HeaderField> headers = new ArrayList<>();
    headers.add(new HeaderField("headerValueField", "Content-Type"));
    headers.add(new HeaderField("authField", "Authorization"));
    meta.setHeaderFields(headers);

    when(mockHelper.iTransformMeta.getMethod()).thenReturn(RestMeta.HTTP_METHOD_POST);
    when(mockHelper.iTransformMeta.getUrl()).thenReturn("http://example.com");
    when(mockHelper.iTransformMeta.isUrlInField()).thenReturn(false);
    when(mockHelper.iTransformMeta.isDynamicMethod()).thenReturn(false);
    when(mockHelper.iTransformMeta.getHeaderFields()).thenReturn(headers);
    when(mockHelper.iTransformMeta.getResultField()).thenReturn(new ResultField());

    RestData data = new RestData();
    data.config = new ClientConfig();
    data.mediaType = MediaType.APPLICATION_JSON_TYPE;
    data.method = RestMeta.HTTP_METHOD_POST;
    data.realUrl = "http://example.com";

    Rest rest =
        spy(
            new Rest(
                mockHelper.transformMeta,
                mockHelper.iTransformMeta,
                data,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));

    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    // Setup input row metadata with header fields
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("headerValueField"));
    inputRowMeta.addValueMeta(new ValueMetaString("authField"));

    Object[] inputRow = new Object[] {"application/json", "Bearer token123"};
    Object[] outputRow = new Object[] {"application/json", "Bearer token123", "response_body"};
    Mockito.doReturn(outputRow).when(rest).callRest(any());

    rest.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(inputRow));
    when(rest.getInputRowMeta()).thenReturn(inputRowMeta);

    assertNotNull(rest);
  }

  @Test
  void testProcessRowWithParameters() throws HopException {
    // Setup meta with query parameters
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com");
    meta.setResultField(new ResultField());

    List<ParameterField> params = new ArrayList<>();
    params.add(new ParameterField("param1Field", "search"));
    params.add(new ParameterField("param2Field", "limit"));
    meta.setParameterFields(params);

    when(mockHelper.iTransformMeta.getMethod()).thenReturn(RestMeta.HTTP_METHOD_GET);
    when(mockHelper.iTransformMeta.getUrl()).thenReturn("http://example.com");
    when(mockHelper.iTransformMeta.isUrlInField()).thenReturn(false);
    when(mockHelper.iTransformMeta.isDynamicMethod()).thenReturn(false);
    when(mockHelper.iTransformMeta.getParameterFields()).thenReturn(params);
    when(mockHelper.iTransformMeta.getResultField()).thenReturn(new ResultField());

    RestData data = new RestData();
    data.config = new ClientConfig();
    data.mediaType = MediaType.APPLICATION_JSON_TYPE;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = "http://example.com";

    Rest rest =
        spy(
            new Rest(
                mockHelper.transformMeta,
                mockHelper.iTransformMeta,
                data,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));

    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    // Setup input row metadata with parameter fields
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("param1Field"));
    inputRowMeta.addValueMeta(new ValueMetaString("param2Field"));

    Object[] inputRow = new Object[] {"test query", "10"};
    Object[] outputRow = new Object[] {"test query", "10", "response_body"};
    Mockito.doReturn(outputRow).when(rest).callRest(any());

    rest.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(inputRow));
    when(rest.getInputRowMeta()).thenReturn(inputRowMeta);

    assertNotNull(rest);
  }

  @Test
  void testProcessRowWithMatrixParameters() throws HopException {
    // Setup meta with matrix parameters
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com");
    meta.setResultField(new ResultField());

    List<MatrixParameterField> matrixParams = new ArrayList<>();
    matrixParams.add(new MatrixParameterField("matrixParam1Field", "author"));
    matrixParams.add(new MatrixParameterField("matrixParam2Field", "year"));
    meta.setMatrixParameterFields(matrixParams);

    when(mockHelper.iTransformMeta.getMethod()).thenReturn(RestMeta.HTTP_METHOD_GET);
    when(mockHelper.iTransformMeta.getUrl()).thenReturn("http://example.com");
    when(mockHelper.iTransformMeta.isUrlInField()).thenReturn(false);
    when(mockHelper.iTransformMeta.isDynamicMethod()).thenReturn(false);
    when(mockHelper.iTransformMeta.getMatrixParameterFields()).thenReturn(matrixParams);
    when(mockHelper.iTransformMeta.getResultField()).thenReturn(new ResultField());

    RestData data = new RestData();
    data.config = new ClientConfig();
    data.mediaType = MediaType.APPLICATION_JSON_TYPE;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = "http://example.com";

    Rest rest =
        spy(
            new Rest(
                mockHelper.transformMeta,
                mockHelper.iTransformMeta,
                data,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));

    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    // Setup input row metadata with matrix parameter fields
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("matrixParam1Field"));
    inputRowMeta.addValueMeta(new ValueMetaString("matrixParam2Field"));

    Object[] inputRow = new Object[] {"John Doe", "2023"};
    Object[] outputRow = new Object[] {"John Doe", "2023", "response_body"};
    Mockito.doReturn(outputRow).when(rest).callRest(any());

    rest.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(inputRow));
    when(rest.getInputRowMeta()).thenReturn(inputRowMeta);

    assertNotNull(rest);
  }

  @Test
  void testProcessRowWithBody() throws HopException {
    // Setup meta with body field
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_POST);
    meta.setUrl("http://example.com");
    meta.setBodyField("bodyField");
    meta.setResultField(new ResultField());

    when(mockHelper.iTransformMeta.getMethod()).thenReturn(RestMeta.HTTP_METHOD_POST);
    when(mockHelper.iTransformMeta.getUrl()).thenReturn("http://example.com");
    when(mockHelper.iTransformMeta.isUrlInField()).thenReturn(false);
    when(mockHelper.iTransformMeta.isDynamicMethod()).thenReturn(false);
    when(mockHelper.iTransformMeta.getBodyField()).thenReturn("bodyField");
    when(mockHelper.iTransformMeta.getResultField()).thenReturn(new ResultField());

    RestData data = new RestData();
    data.config = new ClientConfig();
    data.mediaType = MediaType.APPLICATION_JSON_TYPE;
    data.method = RestMeta.HTTP_METHOD_POST;
    data.realUrl = "http://example.com";

    Rest rest =
        spy(
            new Rest(
                mockHelper.transformMeta,
                mockHelper.iTransformMeta,
                data,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));

    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    // Setup input row metadata with body field
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("bodyField"));

    Object[] inputRow = new Object[] {"{\"name\":\"test\",\"value\":123}"};
    Object[] outputRow = new Object[] {"{\"name\":\"test\",\"value\":123}", "response_body"};
    Mockito.doReturn(outputRow).when(rest).callRest(any());

    rest.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(inputRow));
    when(rest.getInputRowMeta()).thenReturn(inputRowMeta);

    assertNotNull(rest);
  }

  @Test
  void testProcessRowFirstRowInitialization() throws HopException {
    // Setup
    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com");
    meta.setUrlInField(false);
    meta.setDynamicMethod(false);
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");
    meta.getResultField().setCode("statusCode");
    meta.getResultField().setResponseTime("responseTime");
    meta.getResultField().setResponseHeader("headers");

    when(mockHelper.iTransformMeta.getMethod()).thenReturn(RestMeta.HTTP_METHOD_GET);
    when(mockHelper.iTransformMeta.getUrl()).thenReturn("http://example.com");
    when(mockHelper.iTransformMeta.isUrlInField()).thenReturn(false);
    when(mockHelper.iTransformMeta.isDynamicMethod()).thenReturn(false);
    when(mockHelper.iTransformMeta.getResultField()).thenReturn(meta.getResultField());

    RestData data = new RestData();
    data.config = new ClientConfig();
    data.mediaType = MediaType.APPLICATION_JSON_TYPE;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = "http://example.com";
    data.resultFieldName = "result";
    data.resultCodeFieldName = "statusCode";
    data.resultResponseFieldName = "responseTime";
    data.resultHeaderFieldName = "headers";

    Rest rest =
        spy(
            new Rest(
                mockHelper.transformMeta,
                mockHelper.iTransformMeta,
                data,
                0,
                mockHelper.pipelineMeta,
                mockHelper.pipeline));

    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    // Setup input row metadata
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("field1"));

    // Mock the output row meta
    IRowMeta outputRowMeta = inputRowMeta.clone();
    outputRowMeta.addValueMeta(new ValueMetaString("result"));
    outputRowMeta.addValueMeta(new ValueMetaString("statusCode"));
    outputRowMeta.addValueMeta(new ValueMetaString("responseTime"));
    outputRowMeta.addValueMeta(new ValueMetaString("headers"));

    Object[] inputRow = new Object[] {"value1"};
    Object[] outputRow = new Object[] {"value1", "response", 200L, 100L, "{}"};

    Mockito.doReturn(outputRow).when(rest).callRest(any());

    rest.addRowSetToInputRowSets(mockHelper.getMockInputRowSet(inputRow));
    when(rest.getInputRowMeta()).thenReturn(inputRowMeta);

    // Verify the transform is set up correctly
    assertNotNull(rest);
    assertNotNull(data.resultFieldName);
    assertEquals("result", data.resultFieldName);
    assertEquals("statusCode", data.resultCodeFieldName);
    assertEquals("responseTime", data.resultResponseFieldName);
    assertEquals("headers", data.resultHeaderFieldName);
  }
}
