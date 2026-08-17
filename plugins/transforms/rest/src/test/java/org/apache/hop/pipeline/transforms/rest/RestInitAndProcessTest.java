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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.apache.hc.core5.http.ContentType;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.rest.client.RestAuthType;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class RestInitAndProcessTest {

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
    // Ensure HopLogStore is initialized before each test
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  @Test
  void testInitWithStaticMethod() {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com");
    meta.setApplicationType(RestMeta.APPLICATION_TYPE_JSON);
    meta.setConnectionTimeout("5000");
    meta.setReadTimeout("10000");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");
    meta.getResultField().setCode("statusCode");
    meta.getResultField().setResponseTime("responseTime");
    meta.getResultField().setResponseHeader("headers");

    RestData data = new RestData();

    Rest rest =
        new Rest(transformMeta, meta, data, 1, pipelineMeta, spy(new LocalPipelineEngine()));
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    boolean result = rest.init();

    assertTrue(result);
    assertEquals("result", data.resultFieldName);
    assertEquals("statusCode", data.resultCodeFieldName);
    assertEquals("responseTime", data.resultResponseFieldName);
    assertEquals("headers", data.resultHeaderFieldName);
    assertEquals(5000, data.realConnectionTimeout);
    assertEquals(10000, data.realReadTimeout);
    assertEquals(RestMeta.HTTP_METHOD_GET, data.method);
    assertEquals(ContentType.APPLICATION_JSON, data.mediaType);
  }

  @Test
  void testInitWithDynamicMethod() {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setDynamicMethod(true);
    meta.setMethodFieldName("methodField");
    meta.setUrl("http://example.com");
    meta.setApplicationType(RestMeta.APPLICATION_TYPE_XML);
    meta.setConnectionTimeout("3000");
    meta.setReadTimeout("6000");
    meta.setResultField(new ResultField());

    RestData data = new RestData();

    Rest rest =
        new Rest(transformMeta, meta, data, 1, pipelineMeta, spy(new LocalPipelineEngine()));
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    boolean result = rest.init();

    assertTrue(result);
    assertEquals(3000, data.realConnectionTimeout);
    assertEquals(6000, data.realReadTimeout);
    assertEquals(ContentType.APPLICATION_XML, data.mediaType);
  }

  @Test
  void testInitWithMissingStaticMethod() {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setDynamicMethod(false);
    // Don't set method
    meta.setUrl("http://example.com");
    meta.setResultField(new ResultField());

    RestData data = new RestData();

    Rest rest =
        new Rest(transformMeta, meta, data, 1, pipelineMeta, spy(new LocalPipelineEngine()));
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    boolean result = rest.init();

    assertFalse(result);
  }

  @Test
  void testInitWithProxySettings() throws HopException {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_POST);
    meta.setUrl("http://example.com");
    meta.setProxyHost("proxy.example.com");
    meta.setProxyPort("8080");
    meta.setHttpLogin("user");
    meta.setHttpPassword("password");
    meta.setResultField(new ResultField());

    RestData data = new RestData();

    Rest rest =
        new Rest(transformMeta, meta, data, 1, pipelineMeta, spy(new LocalPipelineEngine()));
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    boolean result = rest.init();

    assertTrue(result);
    assertEquals("proxy.example.com", data.realProxyHost);
    assertEquals(8080, data.realProxyPort);
    assertEquals("user", data.realHttpLogin);
    assertNotNull(data.realHttpPassword);
    // The credentials reach the client configuration as Basic auth. They used to be turned into a
    // Jersey HttpAuthenticationFeature on the data object; they are now a request header written
    // by RestAuthenticator, so there is nothing on `data` left to assert.
    assertEquals(RestAuthType.BASIC, rest.createClientSettings().getAuthType());
    assertEquals("user", rest.createClientSettings().getBasicUsername());
  }

  @Test
  void testInitWithAllApplicationTypes() {
    for (String appType : RestMeta.APPLICATION_TYPES) {
      TransformMeta transformMeta = new TransformMeta();
      transformMeta.setName("TestRest");
      PipelineMeta pipelineMeta = new PipelineMeta();
      pipelineMeta.setName("TestRest");
      pipelineMeta.addTransform(transformMeta);

      RestMeta meta = new RestMeta();
      meta.setMethod(RestMeta.HTTP_METHOD_GET);
      meta.setUrl("http://example.com");
      meta.setApplicationType(appType);
      meta.setResultField(new ResultField());

      RestData data = new RestData();

      Rest rest =
          new Rest(transformMeta, meta, data, 1, pipelineMeta, spy(new LocalPipelineEngine()));
      rest.setMetadataProvider(mock(IHopMetadataProvider.class));

      boolean result = rest.init();

      assertTrue(result);
      assertNotNull(data.mediaType);

      // Verify correct media type is set based on application type
      switch (appType) {
        case RestMeta.APPLICATION_TYPE_XML:
          assertEquals(ContentType.APPLICATION_XML, data.mediaType);
          break;
        case RestMeta.APPLICATION_TYPE_JSON:
          assertEquals(ContentType.APPLICATION_JSON, data.mediaType);
          break;
        case RestMeta.APPLICATION_TYPE_OCTET_STREAM:
          assertEquals(ContentType.APPLICATION_OCTET_STREAM, data.mediaType);
          break;
        case RestMeta.APPLICATION_TYPE_XHTML:
          assertEquals(ContentType.APPLICATION_XHTML_XML, data.mediaType);
          break;
        case RestMeta.APPLICATION_TYPE_FORM_URLENCODED:
          assertEquals(ContentType.APPLICATION_FORM_URLENCODED, data.mediaType);
          break;
        case RestMeta.APPLICATION_TYPE_ATOM_XML:
          assertEquals(ContentType.APPLICATION_ATOM_XML, data.mediaType);
          break;
        case RestMeta.APPLICATION_TYPE_SVG_XML:
          assertEquals(ContentType.APPLICATION_SVG_XML, data.mediaType);
          break;
        case RestMeta.APPLICATION_TYPE_TEXT_XML:
          assertEquals(ContentType.TEXT_XML, data.mediaType);
          break;
        case RestMeta.APPLICATION_TYPE_TEXT_PLAIN:
        default:
          assertEquals(ContentType.TEXT_PLAIN, data.mediaType);
          break;
      }
    }
  }

  @Test
  void testInitWithIgnoreSsl() throws HopException {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("https://example.com");
    meta.setIgnoreSsl(true);
    meta.setResultField(new ResultField());

    RestData data = new RestData();

    Rest rest =
        new Rest(transformMeta, meta, data, 1, pipelineMeta, spy(new LocalPipelineEngine()));
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    boolean result = rest.init();

    assertTrue(result);
    // A trust-all context, plus the permissive host name verifier that has to accompany it.
    assertNotNull(rest.createClientSettings().getSslContext());
    assertTrue(rest.createClientSettings().isPermissiveHostnameVerifier());
  }

  @Test
  void testInitWithDefaultTimeouts() {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com");
    // Don't set timeouts, should use defaults
    meta.setResultField(new ResultField());

    RestData data = new RestData();

    Rest rest =
        new Rest(transformMeta, meta, data, 1, pipelineMeta, spy(new LocalPipelineEngine()));
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    boolean result = rest.init();

    assertTrue(result);
    assertEquals(-1, data.realConnectionTimeout);
    assertEquals(-1, data.realReadTimeout);
  }

  @Test
  void testInitWithVariables() {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("${BASE_URL}/api");
    meta.setConnectionTimeout("${CONN_TIMEOUT}");
    meta.setReadTimeout("${READ_TIMEOUT}");
    meta.setResultField(new ResultField());

    RestData data = new RestData();

    Rest rest =
        new Rest(transformMeta, meta, data, 1, pipelineMeta, spy(new LocalPipelineEngine()));
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));
    rest.setVariable("BASE_URL", "http://example.com");
    rest.setVariable("CONN_TIMEOUT", "3000");
    rest.setVariable("READ_TIMEOUT", "7000");

    boolean result = rest.init();

    assertTrue(result);
    assertEquals(3000, data.realConnectionTimeout);
    assertEquals(7000, data.realReadTimeout);
  }
}
