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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
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
import org.glassfish.jersey.client.ClientConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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
    // Setup mocks
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(200);
    when(response.readEntity(String.class)).thenReturn("{\"result\":\"success\"}");
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    headers.add("Content-Type", "application/json");
    when(response.getHeaders()).thenReturn(headers);

    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(builder.get(Response.class)).thenReturn(response);
    when(builder.header(anyString(), any())).thenReturn(builder);
    when(builder.accept((MediaType[]) any())).thenReturn(builder);

    WebTarget webTarget = mock(WebTarget.class);
    when(webTarget.request()).thenReturn(builder);
    when(webTarget.getUri()).thenReturn(URI.create("http://example.com"));

    Client client = mock(Client.class);
    when(client.target(anyString())).thenReturn(webTarget);

    ClientBuilder clientBuilder = mock(ClientBuilder.class);
    when(clientBuilder.withConfig(any(ClientConfig.class))).thenReturn(clientBuilder);
    when(clientBuilder.property(anyString(), any())).thenReturn(clientBuilder);
    when(clientBuilder.hostnameVerifier(any())).thenReturn(clientBuilder);
    when(clientBuilder.sslContext(any())).thenReturn(clientBuilder);
    when(clientBuilder.build()).thenReturn(client);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      // Setup transform
      TransformMeta transformMeta = new TransformMeta();
      transformMeta.setName("TestRest");
      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName("TestRest");
      pipelineMeta.addTransform(transformMeta);

      RestMeta meta = new RestMeta();
      meta.setMethod(RestMeta.HTTP_METHOD_GET);
      meta.setUrl("http://example.com");
      meta.setResultField(new ResultField());
      meta.getResultField().setFieldName("result");
      meta.getResultField().setCode("statusCode");
      meta.getResultField().setResponseTime("responseTime");
      meta.getResultField().setResponseHeader("headers");

      RestData data = new RestData();
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_JSON_TYPE;
      data.method = RestMeta.HTTP_METHOD_GET;
      data.realUrl = "http://example.com";
      data.resultFieldName = "result";
      data.resultCodeFieldName = "statusCode";
      data.resultResponseFieldName = "responseTime";
      data.resultHeaderFieldName = "headers";

      // Setup input row
      IRowMeta inputRowMeta = new RowMeta();
      inputRowMeta.addValueMeta(new ValueMetaString("field1"));
      data.inputRowMeta = inputRowMeta;

      Rest rest =
          spy(new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
      when(rest.createClientBuilder()).thenReturn(clientBuilder);
      rest.setMetadataProvider(mock(IHopMetadataProvider.class));

      // Execute
      Object[] inputRow = new Object[] {"value1"};
      Object[] outputRow = rest.callRest(inputRow);

      // Verify
      assertNotNull(outputRow);
      // Output row should have at least the input fields plus result fields
      assertTrue(outputRow.length >= 1);
      assertEquals("value1", outputRow[0]);
      // The result fields are added at the end of the input row
      // Result field is at input size + 0
      assertEquals("{\"result\":\"success\"}", outputRow[1]); // result field
      // Status code is at input size + 1
      assertEquals(200L, outputRow[2]); // status code
      // Response time is at input size + 2
      assertNotNull(outputRow[3]); // response time
      // Headers are at input size + 3
      assertNotNull(outputRow[4]); // headers

      verify(builder, times(1)).get(Response.class);
    }
  }

  @Test
  void testCallRestWithPostMethod() throws HopException {
    // Setup mocks
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(201);
    when(response.readEntity(String.class)).thenReturn("{\"id\":123}");
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    headers.add("Content-Type", "application/json");
    when(response.getHeaders()).thenReturn(headers);

    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(builder.post(any(Entity.class))).thenReturn(response);
    when(builder.header(anyString(), any())).thenReturn(builder);
    when(builder.accept((MediaType[]) any())).thenReturn(builder);

    WebTarget webTarget = mock(WebTarget.class);
    when(webTarget.request()).thenReturn(builder);
    when(webTarget.getUri()).thenReturn(URI.create("http://example.com/api"));

    Client client = mock(Client.class);
    when(client.target(anyString())).thenReturn(webTarget);

    ClientBuilder clientBuilder = mock(ClientBuilder.class);
    when(clientBuilder.withConfig(any(ClientConfig.class))).thenReturn(clientBuilder);
    when(clientBuilder.property(anyString(), any())).thenReturn(clientBuilder);
    when(clientBuilder.hostnameVerifier(any())).thenReturn(clientBuilder);
    when(clientBuilder.sslContext(any())).thenReturn(clientBuilder);
    when(clientBuilder.build()).thenReturn(client);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      // Setup transform
      TransformMeta transformMeta = new TransformMeta();
      transformMeta.setName("TestRest");
      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName("TestRest");
      pipelineMeta.addTransform(transformMeta);

      RestMeta meta = new RestMeta();
      meta.setMethod(RestMeta.HTTP_METHOD_POST);
      meta.setUrl("http://example.com/api");
      meta.setBodyField("body");
      meta.setResultField(new ResultField());
      meta.getResultField().setFieldName("result");

      RestData data = new RestData();
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_JSON_TYPE;
      data.method = RestMeta.HTTP_METHOD_POST;
      data.realUrl = "http://example.com/api";
      data.resultFieldName = "result";
      data.useBody = true;
      data.indexOfBodyField = 1;

      // Setup input row
      IRowMeta inputRowMeta = new RowMeta();
      inputRowMeta.addValueMeta(new ValueMetaString("field1"));
      inputRowMeta.addValueMeta(new ValueMetaString("body"));
      data.inputRowMeta = inputRowMeta;

      Rest rest =
          spy(new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
      when(rest.createClientBuilder()).thenReturn(clientBuilder);
      rest.setMetadataProvider(mock(IHopMetadataProvider.class));

      // Execute
      Object[] inputRow = new Object[] {"value1", "{\"name\":\"test\"}"};
      Object[] outputRow = rest.callRest(inputRow);

      // Verify
      assertNotNull(outputRow);
      assertEquals("{\"id\":123}", outputRow[2]); // result field
      verify(builder, times(1)).post(any(Entity.class));
    }
  }

  @Test
  void testCallRestWithPutMethod() throws HopException {
    // Setup mocks
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(200);
    when(response.readEntity(String.class)).thenReturn("{\"updated\":true}");
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    when(response.getHeaders()).thenReturn(headers);

    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(builder.put(any(Entity.class))).thenReturn(response);
    when(builder.header(anyString(), any())).thenReturn(builder);
    when(builder.accept((MediaType[]) any())).thenReturn(builder);

    WebTarget webTarget = mock(WebTarget.class);
    when(webTarget.request()).thenReturn(builder);
    when(webTarget.getUri()).thenReturn(URI.create("http://example.com/api"));

    Client client = mock(Client.class);
    when(client.target(anyString())).thenReturn(webTarget);

    ClientBuilder clientBuilder = mock(ClientBuilder.class);
    when(clientBuilder.withConfig(any(ClientConfig.class))).thenReturn(clientBuilder);
    when(clientBuilder.property(anyString(), any())).thenReturn(clientBuilder);
    when(clientBuilder.hostnameVerifier(any())).thenReturn(clientBuilder);
    when(clientBuilder.sslContext(any())).thenReturn(clientBuilder);
    when(clientBuilder.build()).thenReturn(client);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      // Setup transform
      TransformMeta transformMeta = new TransformMeta();
      transformMeta.setName("TestRest");
      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName("TestRest");
      pipelineMeta.addTransform(transformMeta);

      RestMeta meta = new RestMeta();
      meta.setMethod(RestMeta.HTTP_METHOD_PUT);
      meta.setUrl("http://example.com/api");
      meta.setBodyField("body");
      meta.setResultField(new ResultField());
      meta.getResultField().setFieldName("result");

      RestData data = new RestData();
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_JSON_TYPE;
      data.method = RestMeta.HTTP_METHOD_PUT;
      data.realUrl = "http://example.com/api";
      data.resultFieldName = "result";
      data.useBody = true;
      data.indexOfBodyField = 0;

      IRowMeta inputRowMeta = new RowMeta();
      inputRowMeta.addValueMeta(new ValueMetaString("body"));
      data.inputRowMeta = inputRowMeta;

      Rest rest =
          spy(new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
      when(rest.createClientBuilder()).thenReturn(clientBuilder);
      rest.setMetadataProvider(mock(IHopMetadataProvider.class));

      // Execute
      Object[] inputRow = new Object[] {"{\"name\":\"updated\"}"};
      Object[] outputRow = rest.callRest(inputRow);

      // Verify
      assertNotNull(outputRow);
      assertEquals("{\"updated\":true}", outputRow[1]);
      verify(builder, times(1)).put(any(Entity.class));
    }
  }

  @Test
  void testCallRestWithDeleteMethod() throws HopException {
    // Setup mocks
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(204);
    when(response.readEntity(String.class)).thenReturn("");
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    when(response.getHeaders()).thenReturn(headers);

    Invocation invocation = mock(Invocation.class);
    when(invocation.invoke()).thenReturn(response);

    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(builder.build(eq("DELETE"), any(Entity.class))).thenReturn(invocation);
    when(builder.header(anyString(), any())).thenReturn(builder);
    when(builder.accept((MediaType[]) any())).thenReturn(builder);

    WebTarget webTarget = mock(WebTarget.class);
    when(webTarget.request()).thenReturn(builder);
    when(webTarget.getUri()).thenReturn(URI.create("http://example.com/api/123"));

    Client client = mock(Client.class);
    when(client.target(anyString())).thenReturn(webTarget);

    ClientBuilder clientBuilder = mock(ClientBuilder.class);
    when(clientBuilder.withConfig(any(ClientConfig.class))).thenReturn(clientBuilder);
    when(clientBuilder.property(anyString(), any())).thenReturn(clientBuilder);
    when(clientBuilder.hostnameVerifier(any())).thenReturn(clientBuilder);
    when(clientBuilder.sslContext(any())).thenReturn(clientBuilder);
    when(clientBuilder.build()).thenReturn(client);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      // Setup transform
      TransformMeta transformMeta = new TransformMeta();
      transformMeta.setName("TestRest");
      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName("TestRest");
      pipelineMeta.addTransform(transformMeta);

      RestMeta meta = new RestMeta();
      meta.setMethod(RestMeta.HTTP_METHOD_DELETE);
      meta.setUrl("http://example.com/api/123");
      meta.setResultField(new ResultField());
      meta.getResultField().setFieldName("result");
      meta.getResultField().setCode("statusCode");

      RestData data = new RestData();
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_JSON_TYPE;
      data.method = RestMeta.HTTP_METHOD_DELETE;
      data.realUrl = "http://example.com/api/123";
      data.resultFieldName = "result";
      data.resultCodeFieldName = "statusCode";

      IRowMeta inputRowMeta = new RowMeta();
      inputRowMeta.addValueMeta(new ValueMetaString("id"));
      data.inputRowMeta = inputRowMeta;

      Rest rest =
          spy(new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
      when(rest.createClientBuilder()).thenReturn(clientBuilder);
      rest.setMetadataProvider(mock(IHopMetadataProvider.class));

      // Execute
      Object[] inputRow = new Object[] {"123"};
      Object[] outputRow = rest.callRest(inputRow);

      // Verify
      assertNotNull(outputRow);
      assertEquals(204L, outputRow[2]); // status code
      verify(builder, times(1)).build(eq("DELETE"), any(Entity.class));
    }
  }

  @Test
  void testCallRestWithHeadMethod() throws HopException {
    // Setup mocks
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(200);
    when(response.readEntity(String.class)).thenReturn("");
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    headers.add("Content-Length", "1234");
    when(response.getHeaders()).thenReturn(headers);

    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(builder.head()).thenReturn(response);
    when(builder.header(anyString(), any())).thenReturn(builder);
    when(builder.accept((MediaType[]) any())).thenReturn(builder);

    WebTarget webTarget = mock(WebTarget.class);
    when(webTarget.request()).thenReturn(builder);
    when(webTarget.getUri()).thenReturn(URI.create("http://example.com/api"));

    Client client = mock(Client.class);
    when(client.target(anyString())).thenReturn(webTarget);

    ClientBuilder clientBuilder = mock(ClientBuilder.class);
    when(clientBuilder.withConfig(any(ClientConfig.class))).thenReturn(clientBuilder);
    when(clientBuilder.property(anyString(), any())).thenReturn(clientBuilder);
    when(clientBuilder.hostnameVerifier(any())).thenReturn(clientBuilder);
    when(clientBuilder.sslContext(any())).thenReturn(clientBuilder);
    when(clientBuilder.build()).thenReturn(client);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      // Setup transform
      TransformMeta transformMeta = new TransformMeta();
      transformMeta.setName("TestRest");
      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName("TestRest");
      pipelineMeta.addTransform(transformMeta);

      RestMeta meta = new RestMeta();
      meta.setMethod(RestMeta.HTTP_METHOD_HEAD);
      meta.setUrl("http://example.com/api");
      meta.setResultField(new ResultField());
      meta.getResultField().setCode("statusCode");
      meta.getResultField().setResponseHeader("headers");

      RestData data = new RestData();
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_JSON_TYPE;
      data.method = RestMeta.HTTP_METHOD_HEAD;
      data.realUrl = "http://example.com/api";
      data.resultCodeFieldName = "statusCode";
      data.resultHeaderFieldName = "headers";

      IRowMeta inputRowMeta = new RowMeta();
      data.inputRowMeta = inputRowMeta;

      Rest rest =
          spy(new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
      when(rest.createClientBuilder()).thenReturn(clientBuilder);
      rest.setMetadataProvider(mock(IHopMetadataProvider.class));

      // Execute
      Object[] inputRow = new Object[] {};
      Object[] outputRow = rest.callRest(inputRow);

      // Verify
      assertNotNull(outputRow);
      assertEquals(200L, outputRow[0]); // status code
      verify(builder, times(1)).head();
    }
  }

  @Test
  void testCallRestWithOptionsMethod() throws HopException {
    // Setup mocks
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(200);
    when(response.readEntity(String.class)).thenReturn("");
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    headers.add("Allow", "GET, POST, PUT, DELETE");
    when(response.getHeaders()).thenReturn(headers);

    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(builder.options()).thenReturn(response);
    when(builder.header(anyString(), any())).thenReturn(builder);
    when(builder.accept((MediaType[]) any())).thenReturn(builder);

    WebTarget webTarget = mock(WebTarget.class);
    when(webTarget.request()).thenReturn(builder);
    when(webTarget.getUri()).thenReturn(URI.create("http://example.com/api"));

    Client client = mock(Client.class);
    when(client.target(anyString())).thenReturn(webTarget);

    ClientBuilder clientBuilder = mock(ClientBuilder.class);
    when(clientBuilder.withConfig(any(ClientConfig.class))).thenReturn(clientBuilder);
    when(clientBuilder.property(anyString(), any())).thenReturn(clientBuilder);
    when(clientBuilder.hostnameVerifier(any())).thenReturn(clientBuilder);
    when(clientBuilder.sslContext(any())).thenReturn(clientBuilder);
    when(clientBuilder.build()).thenReturn(client);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      // Setup transform
      TransformMeta transformMeta = new TransformMeta();
      transformMeta.setName("TestRest");
      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName("TestRest");
      pipelineMeta.addTransform(transformMeta);

      RestMeta meta = new RestMeta();
      meta.setMethod(RestMeta.HTTP_METHOD_OPTIONS);
      meta.setUrl("http://example.com/api");
      meta.setResultField(new ResultField());
      meta.getResultField().setResponseHeader("allowedMethods");

      RestData data = new RestData();
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_JSON_TYPE;
      data.method = RestMeta.HTTP_METHOD_OPTIONS;
      data.realUrl = "http://example.com/api";
      data.resultHeaderFieldName = "allowedMethods";

      IRowMeta inputRowMeta = new RowMeta();
      data.inputRowMeta = inputRowMeta;

      Rest rest =
          spy(new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
      when(rest.createClientBuilder()).thenReturn(clientBuilder);
      rest.setMetadataProvider(mock(IHopMetadataProvider.class));

      // Execute
      Object[] inputRow = new Object[] {};
      Object[] outputRow = rest.callRest(inputRow);

      // Verify
      assertNotNull(outputRow);
      verify(builder, times(1)).options();
    }
  }

  @Test
  void testCallRestWithPatchMethod() throws HopException {
    // Setup mocks
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(200);
    when(response.readEntity(String.class)).thenReturn("{\"patched\":true}");
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    when(response.getHeaders()).thenReturn(headers);

    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(builder.method(eq(RestMeta.HTTP_METHOD_PATCH), any(Entity.class))).thenReturn(response);
    when(builder.header(anyString(), any())).thenReturn(builder);
    when(builder.accept((MediaType[]) any())).thenReturn(builder);

    WebTarget webTarget = mock(WebTarget.class);
    when(webTarget.request()).thenReturn(builder);
    when(webTarget.getUri()).thenReturn(URI.create("http://example.com/api"));

    Client client = mock(Client.class);
    when(client.target(anyString())).thenReturn(webTarget);

    ClientBuilder clientBuilder = mock(ClientBuilder.class);
    when(clientBuilder.withConfig(any(ClientConfig.class))).thenReturn(clientBuilder);
    when(clientBuilder.property(anyString(), any())).thenReturn(clientBuilder);
    when(clientBuilder.hostnameVerifier(any())).thenReturn(clientBuilder);
    when(clientBuilder.sslContext(any())).thenReturn(clientBuilder);
    when(clientBuilder.build()).thenReturn(client);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      // Setup transform
      TransformMeta transformMeta = new TransformMeta();
      transformMeta.setName("TestRest");
      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName("TestRest");
      pipelineMeta.addTransform(transformMeta);

      RestMeta meta = new RestMeta();
      meta.setMethod(RestMeta.HTTP_METHOD_PATCH);
      meta.setUrl("http://example.com/api");
      meta.setBodyField("body");
      meta.setResultField(new ResultField());
      meta.getResultField().setFieldName("result");

      RestData data = new RestData();
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_JSON_TYPE;
      data.method = RestMeta.HTTP_METHOD_PATCH;
      data.realUrl = "http://example.com/api";
      data.resultFieldName = "result";
      data.useBody = true;
      data.indexOfBodyField = 0;

      IRowMeta inputRowMeta = new RowMeta();
      inputRowMeta.addValueMeta(new ValueMetaString("body"));
      data.inputRowMeta = inputRowMeta;

      Rest rest =
          spy(new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
      when(rest.createClientBuilder()).thenReturn(clientBuilder);
      rest.setMetadataProvider(mock(IHopMetadataProvider.class));

      // Execute
      Object[] inputRow = new Object[] {"{\"field\":\"value\"}"};
      Object[] outputRow = rest.callRest(inputRow);

      // Verify
      assertNotNull(outputRow);
      assertEquals("{\"patched\":true}", outputRow[1]);
      verify(builder, times(1)).method(eq(RestMeta.HTTP_METHOD_PATCH), any(Entity.class));
    }
  }

  @Test
  void testCallRestWithQueryParameters() throws HopException {
    // Setup mocks
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(200);
    when(response.readEntity(String.class)).thenReturn("[{\"id\":1},{\"id\":2}]");
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    when(response.getHeaders()).thenReturn(headers);

    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(builder.get(Response.class)).thenReturn(response);
    when(builder.header(anyString(), any())).thenReturn(builder);
    when(builder.accept((MediaType[]) any())).thenReturn(builder);

    WebTarget webTarget = mock(WebTarget.class);
    when(webTarget.request()).thenReturn(builder);
    when(webTarget.queryParam(anyString(), any())).thenReturn(webTarget);
    when(webTarget.getUri()).thenReturn(URI.create("http://example.com/api?search=test&limit=10"));

    Client client = mock(Client.class);
    when(client.target(anyString())).thenReturn(webTarget);

    ClientBuilder clientBuilder = mock(ClientBuilder.class);
    when(clientBuilder.withConfig(any(ClientConfig.class))).thenReturn(clientBuilder);
    when(clientBuilder.property(anyString(), any())).thenReturn(clientBuilder);
    when(clientBuilder.hostnameVerifier(any())).thenReturn(clientBuilder);
    when(clientBuilder.sslContext(any())).thenReturn(clientBuilder);
    when(clientBuilder.build()).thenReturn(client);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      // Setup transform
      TransformMeta transformMeta = new TransformMeta();
      transformMeta.setName("TestRest");
      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName("TestRest");
      pipelineMeta.addTransform(transformMeta);

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
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_JSON_TYPE;
      data.method = RestMeta.HTTP_METHOD_GET;
      data.realUrl = "http://example.com/api";
      data.resultFieldName = "result";
      data.useParams = true;
      data.nrParams = 2;
      data.paramNames = new String[] {"search", "limit"};
      data.indexOfParamFields = new int[] {0, 1};

      IRowMeta inputRowMeta = new RowMeta();
      inputRowMeta.addValueMeta(new ValueMetaString("searchField"));
      inputRowMeta.addValueMeta(new ValueMetaString("limitField"));
      data.inputRowMeta = inputRowMeta;

      Rest rest =
          spy(new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
      when(rest.createClientBuilder()).thenReturn(clientBuilder);
      rest.setMetadataProvider(mock(IHopMetadataProvider.class));

      // Execute
      Object[] inputRow = new Object[] {"test", "10"};
      Object[] outputRow = rest.callRest(inputRow);

      // Verify
      assertNotNull(outputRow);
      assertEquals("[{\"id\":1},{\"id\":2}]", outputRow[2]);
      verify(webTarget, times(2)).queryParam(anyString(), any());
    }
  }

  @Test
  void testCallRestWithHeaders() throws HopException {
    // Setup mocks
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(200);
    when(response.readEntity(String.class)).thenReturn("{\"authenticated\":true}");
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    when(response.getHeaders()).thenReturn(headers);

    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(builder.get(Response.class)).thenReturn(response);
    when(builder.header(anyString(), any())).thenReturn(builder);
    when(builder.accept((MediaType[]) any())).thenReturn(builder);

    WebTarget webTarget = mock(WebTarget.class);
    when(webTarget.request()).thenReturn(builder);
    when(webTarget.getUri()).thenReturn(URI.create("http://example.com/api"));

    Client client = mock(Client.class);
    when(client.target(anyString())).thenReturn(webTarget);

    ClientBuilder clientBuilder = mock(ClientBuilder.class);
    when(clientBuilder.withConfig(any(ClientConfig.class))).thenReturn(clientBuilder);
    when(clientBuilder.property(anyString(), any())).thenReturn(clientBuilder);
    when(clientBuilder.hostnameVerifier(any())).thenReturn(clientBuilder);
    when(clientBuilder.sslContext(any())).thenReturn(clientBuilder);
    when(clientBuilder.build()).thenReturn(client);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      // Setup transform
      TransformMeta transformMeta = new TransformMeta();
      transformMeta.setName("TestRest");
      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName("TestRest");
      pipelineMeta.addTransform(transformMeta);

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
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_JSON_TYPE;
      data.method = RestMeta.HTTP_METHOD_GET;
      data.realUrl = "http://example.com/api";
      data.resultFieldName = "result";
      data.useHeaders = true;
      data.nrheader = 2;
      data.headerNames = new String[] {"Authorization", "Content-Type"};
      data.indexOfHeaderFields = new int[] {0, 1};

      IRowMeta inputRowMeta = new RowMeta();
      inputRowMeta.addValueMeta(new ValueMetaString("authField"));
      inputRowMeta.addValueMeta(new ValueMetaString("typeField"));
      data.inputRowMeta = inputRowMeta;

      Rest rest =
          spy(new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
      when(rest.createClientBuilder()).thenReturn(clientBuilder);
      rest.setMetadataProvider(mock(IHopMetadataProvider.class));

      // Execute
      Object[] inputRow = new Object[] {"Bearer token123", "application/json"};
      Object[] outputRow = rest.callRest(inputRow);

      // Verify
      assertNotNull(outputRow);
      assertEquals("{\"authenticated\":true}", outputRow[2]);
      verify(builder, times(2)).header(anyString(), any());
    }
  }

  @Test
  void testCallRestWithMatrixParameters() throws HopException {
    // Setup mocks
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(200);
    when(response.readEntity(String.class)).thenReturn("[{\"book\":\"title\"}]");
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    when(response.getHeaders()).thenReturn(headers);

    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(builder.get(Response.class)).thenReturn(response);
    when(builder.header(anyString(), any())).thenReturn(builder);
    when(builder.accept((MediaType[]) any())).thenReturn(builder);

    WebTarget webTarget = mock(WebTarget.class);
    when(webTarget.request()).thenReturn(builder);
    when(webTarget.getUri()).thenReturn(URI.create("http://example.com/api"));
    when(webTarget.getUriBuilder()).thenReturn(UriBuilder.fromUri("http://example.com/api"));

    Client client = mock(Client.class);
    when(client.target(anyString())).thenReturn(webTarget);
    when(client.target(any(URI.class))).thenReturn(webTarget);

    ClientBuilder clientBuilder = mock(ClientBuilder.class);
    when(clientBuilder.withConfig(any(ClientConfig.class))).thenReturn(clientBuilder);
    when(clientBuilder.property(anyString(), any())).thenReturn(clientBuilder);
    when(clientBuilder.hostnameVerifier(any())).thenReturn(clientBuilder);
    when(clientBuilder.sslContext(any())).thenReturn(clientBuilder);
    when(clientBuilder.build()).thenReturn(client);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      // Setup transform
      TransformMeta transformMeta = new TransformMeta();
      transformMeta.setName("TestRest");
      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName("TestRest");
      pipelineMeta.addTransform(transformMeta);

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
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_JSON_TYPE;
      data.method = RestMeta.HTTP_METHOD_GET;
      data.realUrl = "http://example.com/api";
      data.resultFieldName = "result";
      data.useMatrixParams = true;
      data.nrMatrixParams = 2;
      data.matrixParamNames = new String[] {"author", "year"};
      data.indexOfMatrixParamFields = new int[] {0, 1};

      IRowMeta inputRowMeta = new RowMeta();
      inputRowMeta.addValueMeta(new ValueMetaString("authorField"));
      inputRowMeta.addValueMeta(new ValueMetaString("yearField"));
      data.inputRowMeta = inputRowMeta;

      Rest rest =
          spy(new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
      when(rest.createClientBuilder()).thenReturn(clientBuilder);
      rest.setMetadataProvider(mock(IHopMetadataProvider.class));

      // Execute
      Object[] inputRow = new Object[] {"John Doe", "2023"};
      Object[] outputRow = rest.callRest(inputRow);

      // Verify
      assertNotNull(outputRow);
      assertEquals("[{\"book\":\"title\"}]", outputRow[2]);
    }
  }

  @Test
  void testCallRestWithDynamicUrl() throws HopException {
    // Setup mocks
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(200);
    when(response.readEntity(String.class)).thenReturn("{\"dynamic\":true}");
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    when(response.getHeaders()).thenReturn(headers);

    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(builder.get(Response.class)).thenReturn(response);
    when(builder.header(anyString(), any())).thenReturn(builder);
    when(builder.accept((MediaType[]) any())).thenReturn(builder);

    WebTarget webTarget = mock(WebTarget.class);
    when(webTarget.request()).thenReturn(builder);
    when(webTarget.getUri()).thenReturn(URI.create("http://dynamic-url.com/api/resource"));

    Client client = mock(Client.class);
    when(client.target(anyString())).thenReturn(webTarget);

    ClientBuilder clientBuilder = mock(ClientBuilder.class);
    when(clientBuilder.withConfig(any(ClientConfig.class))).thenReturn(clientBuilder);
    when(clientBuilder.property(anyString(), any())).thenReturn(clientBuilder);
    when(clientBuilder.hostnameVerifier(any())).thenReturn(clientBuilder);
    when(clientBuilder.sslContext(any())).thenReturn(clientBuilder);
    when(clientBuilder.build()).thenReturn(client);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      // Setup transform
      TransformMeta transformMeta = new TransformMeta();
      transformMeta.setName("TestRest");
      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName("TestRest");
      pipelineMeta.addTransform(transformMeta);

      RestMeta meta = new RestMeta();
      meta.setMethod(RestMeta.HTTP_METHOD_GET);
      meta.setUrlInField(true);
      meta.setUrlField("urlField");
      meta.setResultField(new ResultField());
      meta.getResultField().setFieldName("result");

      RestData data = new RestData();
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_JSON_TYPE;
      data.method = RestMeta.HTTP_METHOD_GET;
      data.resultFieldName = "result";
      data.indexOfUrlField = 0;

      IRowMeta inputRowMeta = new RowMeta();
      inputRowMeta.addValueMeta(new ValueMetaString("urlField"));
      data.inputRowMeta = inputRowMeta;

      Rest rest =
          spy(new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
      when(rest.createClientBuilder()).thenReturn(clientBuilder);
      rest.setMetadataProvider(mock(IHopMetadataProvider.class));

      // Execute
      Object[] inputRow = new Object[] {"http://dynamic-url.com/api/resource"};
      Object[] outputRow = rest.callRest(inputRow);

      // Verify
      assertNotNull(outputRow);
      assertEquals("{\"dynamic\":true}", outputRow[1]);
    }
  }

  @Test
  void testCallRestWithDynamicMethod() throws HopException {
    // Setup mocks
    Response response = mock(Response.class);
    when(response.getStatus()).thenReturn(201);
    when(response.readEntity(String.class)).thenReturn("{\"created\":true}");
    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    when(response.getHeaders()).thenReturn(headers);

    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(builder.post(any(Entity.class))).thenReturn(response);
    when(builder.header(anyString(), any())).thenReturn(builder);
    when(builder.accept((MediaType[]) any())).thenReturn(builder);

    WebTarget webTarget = mock(WebTarget.class);
    when(webTarget.request()).thenReturn(builder);
    when(webTarget.getUri()).thenReturn(URI.create("http://example.com/api"));

    Client client = mock(Client.class);
    when(client.target(anyString())).thenReturn(webTarget);

    ClientBuilder clientBuilder = mock(ClientBuilder.class);
    when(clientBuilder.withConfig(any(ClientConfig.class))).thenReturn(clientBuilder);
    when(clientBuilder.property(anyString(), any())).thenReturn(clientBuilder);
    when(clientBuilder.hostnameVerifier(any())).thenReturn(clientBuilder);
    when(clientBuilder.sslContext(any())).thenReturn(clientBuilder);
    when(clientBuilder.build()).thenReturn(client);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      // Setup transform
      TransformMeta transformMeta = new TransformMeta();
      transformMeta.setName("TestRest");
      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName("TestRest");
      pipelineMeta.addTransform(transformMeta);

      RestMeta meta = new RestMeta();
      meta.setDynamicMethod(true);
      meta.setMethodFieldName("methodField");
      meta.setUrl("http://example.com/api");
      meta.setResultField(new ResultField());
      meta.getResultField().setFieldName("result");

      RestData data = new RestData();
      data.config = new ClientConfig();
      data.mediaType = MediaType.APPLICATION_JSON_TYPE;
      data.realUrl = "http://example.com/api";
      data.resultFieldName = "result";
      data.indexOfMethod = 0;

      IRowMeta inputRowMeta = new RowMeta();
      inputRowMeta.addValueMeta(new ValueMetaString("methodField"));
      data.inputRowMeta = inputRowMeta;

      Rest rest =
          new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine()));
      rest.setMetadataProvider(mock(IHopMetadataProvider.class));

      // Execute
      Object[] inputRow = new Object[] {"POST"};
      Object[] outputRow = rest.callRest(inputRow);

      // Verify
      assertNotNull(outputRow);
      assertEquals("{\"created\":true}", outputRow[1]);
    }
  }

  @Test
  void testCallRestWithUnknownMethod() throws Exception {
    // Setup transform
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setUrl("http://example.com/api");
    meta.setResultField(new ResultField());

    RestData data = new RestData();
    data.config = new ClientConfig();
    data.mediaType = MediaType.APPLICATION_JSON_TYPE;
    data.method = "INVALID_METHOD";
    data.realUrl = "http://example.com/api";

    IRowMeta inputRowMeta = new RowMeta();
    data.inputRowMeta = inputRowMeta;

    // ensure the clientBuilder variable is defined before stubbing in this test
    ClientBuilder clientBuilder = mock(ClientBuilder.class);
    Rest rest =
        spy(new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
    when(rest.createClientBuilder()).thenReturn(clientBuilder);
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    // This should throw an exception for unknown method
    // We'll need to mock the client builder even for the error case
    when(clientBuilder.withConfig(any(ClientConfig.class))).thenReturn(clientBuilder);
    when(clientBuilder.property(anyString(), any())).thenReturn(clientBuilder);
    when(clientBuilder.hostnameVerifier(any())).thenReturn(clientBuilder);
    when(clientBuilder.sslContext(any())).thenReturn(clientBuilder);

    Client client = mock(Client.class);
    WebTarget webTarget = mock(WebTarget.class);
    when(client.target(anyString())).thenReturn(webTarget);
    when(webTarget.getUri()).thenReturn(URI.create("http://example.com/api"));
    Invocation.Builder builder = mock(Invocation.Builder.class);
    when(webTarget.request()).thenReturn(builder);
    when(builder.header(anyString(), any())).thenReturn(builder);
    when(clientBuilder.build()).thenReturn(client);

    try (MockedStatic<ClientBuilder> mockedStatic = Mockito.mockStatic(ClientBuilder.class)) {
      mockedStatic.when(ClientBuilder::newBuilder).thenReturn(clientBuilder);

      // Execute and expect exception
      Object[] inputRow = new Object[] {};
      assertThrows(HopException.class, () -> rest.callRest(inputRow));
    }
  }
}
