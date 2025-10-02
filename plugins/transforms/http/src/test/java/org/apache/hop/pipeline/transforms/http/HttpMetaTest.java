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
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class HttpMetaTest {

  private HttpMeta meta;

  @BeforeEach
  void setUp() {
    meta = new HttpMeta();
  }

  @Test
  void testSetDefault() {
    meta.setDefault();

    assertEquals(String.valueOf(HttpMeta.DEFAULT_SOCKET_TIMEOUT), meta.getSocketTimeout());
    assertEquals(String.valueOf(HttpMeta.DEFAULT_CONNECTION_TIMEOUT), meta.getConnectionTimeout());
    assertEquals(
        String.valueOf(HttpMeta.DEFAULT_CLOSE_CONNECTIONS_TIME),
        meta.getCloseIdleConnectionsTime());
    assertEquals("result", meta.getFieldName());
    assertEquals("UTF-8", meta.getEncoding());
    assertEquals("", meta.getResultCodeFieldName());
    assertEquals("", meta.getResponseTimeFieldName());
    assertEquals("", meta.getResponseHeaderFieldName());
  }

  @Test
  void testUrlConfiguration() {
    // Test URL properties
    meta.setUrl("http://example.com");
    assertEquals("http://example.com", meta.getUrl());

    meta.setUrlInField(true);
    assertTrue(meta.isUrlInField());

    meta.setUrlField("urlColumn");
    assertEquals("urlColumn", meta.getUrlField());

    meta.setUrlInField(false);
    assertFalse(meta.isUrlInField());
  }

  @Test
  void testOutputFieldConfiguration() {
    // Test all output field properties
    meta.setFieldName("resultField");
    assertEquals("resultField", meta.getFieldName());

    meta.setResultCodeFieldName("responseCode");
    assertEquals("responseCode", meta.getResultCodeFieldName());

    meta.setResponseTimeFieldName("responseTime");
    assertEquals("responseTime", meta.getResponseTimeFieldName());

    meta.setResponseHeaderFieldName("responseHeader");
    assertEquals("responseHeader", meta.getResponseHeaderFieldName());
  }

  @Test
  void testProxyAndAuthConfiguration() {
    // Test proxy and authentication properties
    meta.setProxyHost("proxy.example.com");
    assertEquals("proxy.example.com", meta.getProxyHost());

    meta.setProxyPort("8080");
    assertEquals("8080", meta.getProxyPort());

    meta.setHttpLogin("username");
    assertEquals("username", meta.getHttpLogin());

    meta.setHttpPassword("password123");
    assertEquals("password123", meta.getHttpPassword());
  }

  @Test
  void testTimeoutAndConnectionConfiguration() {
    // Test timeout and connection properties
    meta.setSocketTimeout("5000");
    assertEquals("5000", meta.getSocketTimeout());

    meta.setConnectionTimeout("3000");
    assertEquals("3000", meta.getConnectionTimeout());

    meta.setCloseIdleConnectionsTime("10000");
    assertEquals("10000", meta.getCloseIdleConnectionsTime());

    meta.setEncoding("ISO-8859-1");
    assertEquals("ISO-8859-1", meta.getEncoding());

    meta.setIgnoreSsl(true);
    assertTrue(meta.isIgnoreSsl());

    meta.setIgnoreSsl(false);
    assertFalse(meta.isIgnoreSsl());
  }

  @Test
  void testArgumentsAndHeadersConfiguration() {
    // Test allocate
    meta.allocate(3, 2);
    assertNotNull(meta.getArgumentField());
    assertNotNull(meta.getArgumentParameter());
    assertNotNull(meta.getHeaderField());
    assertNotNull(meta.getHeaderParameter());
    assertEquals(3, meta.getArgumentField().length);
    assertEquals(3, meta.getArgumentParameter().length);
    assertEquals(2, meta.getHeaderField().length);
    assertEquals(2, meta.getHeaderParameter().length);

    // Test argument fields and parameters
    String[] argFields = {"field1", "field2", "field3"};
    meta.setArgumentField(argFields);
    assertEquals(3, meta.getArgumentField().length);
    assertEquals("field1", meta.getArgumentField()[0]);
    assertEquals("field2", meta.getArgumentField()[1]);

    String[] argParams = {"param1", "param2"};
    meta.setArgumentParameter(argParams);
    assertEquals("param1", meta.getArgumentParameter()[0]);
    assertEquals("param2", meta.getArgumentParameter()[1]);

    // Test header fields and parameters
    String[] headerFields = {"Authorization", "Content-Type"};
    meta.setHeaderField(headerFields);
    assertEquals(2, meta.getHeaderField().length);
    assertEquals("Authorization", meta.getHeaderField()[0]);

    String[] headerParams = {"Bearer token", "application/json"};
    meta.setHeaderParameter(headerParams);
    assertEquals(2, meta.getHeaderParameter().length);
    assertEquals("Bearer token", meta.getHeaderParameter()[0]);
  }

  @Test
  void testClone() {
    meta.allocate(2, 1);
    meta.setUrl("http://test.com");
    meta.setFieldName("output");
    meta.getArgumentField()[0] = "arg1";
    meta.getArgumentField()[1] = "arg2";
    meta.getArgumentParameter()[0] = "param1";
    meta.getArgumentParameter()[1] = "param2";
    meta.getHeaderField()[0] = "header1";
    meta.getHeaderParameter()[0] = "value1";

    HttpMeta cloned = (HttpMeta) meta.clone();

    assertNotNull(cloned);
    assertEquals(meta.getUrl(), cloned.getUrl());
    assertEquals(meta.getFieldName(), cloned.getFieldName());
    assertEquals(2, cloned.getArgumentField().length);
    assertEquals("arg1", cloned.getArgumentField()[0]);
    assertEquals("arg2", cloned.getArgumentField()[1]);
  }

  @Test
  void testGetFields() throws Exception {
    Variables variables = new Variables();

    // Test with all fields configured
    meta.setFieldName("resultField");
    meta.setResultCodeFieldName("statusCode");
    meta.setResponseTimeFieldName("responseTime");
    meta.setResponseHeaderFieldName("headers");

    IRowMeta rowMetaAll = new RowMeta();
    meta.getFields(rowMetaAll, "HttpTransform", null, null, variables, null);
    assertEquals(4, rowMetaAll.size());
    assertNotNull(rowMetaAll.searchValueMeta("resultField"));
    assertNotNull(rowMetaAll.searchValueMeta("statusCode"));
    assertNotNull(rowMetaAll.searchValueMeta("responseTime"));
    assertNotNull(rowMetaAll.searchValueMeta("headers"));
    assertEquals("HttpTransform", rowMetaAll.searchValueMeta("resultField").getOrigin());

    // Test with empty field names
    meta.setFieldName("");
    meta.setResultCodeFieldName("");
    meta.setResponseTimeFieldName("");
    meta.setResponseHeaderFieldName("");
    IRowMeta rowMetaEmpty = new RowMeta();
    meta.getFields(rowMetaEmpty, "HttpTransform", null, null, variables, null);
    assertEquals(0, rowMetaEmpty.size());

    // Test with only result field
    meta.setFieldName("result");
    IRowMeta rowMetaSingle = new RowMeta();
    meta.getFields(rowMetaSingle, "HttpTransform", null, null, variables, null);
    assertEquals(1, rowMetaSingle.size());
    assertNotNull(rowMetaSingle.searchValueMeta("result"));
  }

  @Test
  void testGetXml() {
    meta.allocate(1, 1);
    meta.setUrl("http://example.com");
    meta.setUrlInField(true);
    meta.setUrlField("urlField");
    meta.setEncoding("UTF-8");
    meta.setHttpLogin("user");
    meta.setHttpPassword("pass");
    meta.setProxyHost("proxy");
    meta.setProxyPort("8080");
    meta.getArgumentField()[0] = "arg1";
    meta.getArgumentParameter()[0] = "param1";
    meta.getHeaderField()[0] = "header1";
    meta.getHeaderParameter()[0] = "value1";
    meta.setFieldName("result");
    meta.setResultCodeFieldName("code");

    String xml = meta.getXml();

    assertNotNull(xml);
    assertTrue(xml.contains("<url>http://example.com</url>"));
    assertTrue(xml.contains("<urlInField>Y</urlInField>"));
    assertTrue(xml.contains("<urlField>urlField</urlField>"));
    assertTrue(xml.contains("<encoding>UTF-8</encoding>"));
    assertTrue(xml.contains("<httpLogin>user</httpLogin>"));
    assertTrue(xml.contains("<proxyHost>proxy</proxyHost>"));
    assertTrue(xml.contains("<proxyPort>8080</proxyPort>"));
    assertTrue(xml.contains("<name>arg1</name>"));
    assertTrue(xml.contains("<parameter>param1</parameter>"));
    assertTrue(xml.contains("<name>result</name>"));
    assertTrue(xml.contains("<code>code</code>"));
  }

  @Test
  void testCheckValidation() {
    TransformMeta transformMeta = mock(TransformMeta.class);
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    IRowMeta prev = mock(IRowMeta.class);
    List<ICheckResult> remarks;

    // Test with input transform
    remarks = new ArrayList<>();
    meta.check(
        remarks, pipelineMeta, transformMeta, prev, new String[] {"prev"}, null, null, null, null);
    assertTrue(remarks.size() > 0);
    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_OK));

    // Test without input transform
    remarks = new ArrayList<>();
    meta.check(remarks, pipelineMeta, transformMeta, prev, new String[] {}, null, null, null, null);
    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));

    // Test URL in field - missing field name
    remarks = new ArrayList<>();
    meta.setUrlInField(true);
    meta.setUrlField("");
    meta.check(
        remarks, pipelineMeta, transformMeta, prev, new String[] {"prev"}, null, null, null, null);
    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));

    // Test URL in field - with field name
    remarks = new ArrayList<>();
    meta.setUrlField("urlColumn");
    meta.check(
        remarks, pipelineMeta, transformMeta, prev, new String[] {"prev"}, null, null, null, null);
    assertFalse(remarks.isEmpty());

    // Test URL not in field - missing URL
    remarks = new ArrayList<>();
    meta.setUrlInField(false);
    meta.setUrl("");
    meta.check(
        remarks, pipelineMeta, transformMeta, prev, new String[] {"prev"}, null, null, null, null);
    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));

    // Test URL not in field - with URL
    remarks = new ArrayList<>();
    meta.setUrl("http://example.com");
    meta.check(
        remarks, pipelineMeta, transformMeta, prev, new String[] {"prev"}, null, null, null, null);
    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_OK));
  }

  @Test
  void testMiscellaneous() {
    // Test error handling support
    assertTrue(meta.supportsErrorHandling());

    // Test constants
    assertEquals(10000, HttpMeta.DEFAULT_SOCKET_TIMEOUT);
    assertEquals(10000, HttpMeta.DEFAULT_CONNECTION_TIMEOUT);
    assertEquals(HttpMeta.DEFAULT_CLOSE_CONNECTIONS_TIME, -1);
    assertEquals("header", HttpMeta.CONST_HEADER);
    assertEquals("result", HttpMeta.CONST_RESULT);
    assertEquals("        ", HttpMeta.CONST_SPACES_LONG);
    assertEquals("      ", HttpMeta.CONST_SPACES);
    assertEquals("parameter", HttpMeta.CONST_PARAMETER);

    // Test allocate with zero
    meta.allocate(0, 0);
    assertEquals(0, meta.getArgumentField().length);
    assertEquals(0, meta.getArgumentParameter().length);
    assertEquals(0, meta.getHeaderField().length);
    assertEquals(0, meta.getHeaderParameter().length);

    // Test clone without allocation
    HttpMeta cloned = (HttpMeta) meta.clone();
    assertNotNull(cloned);
    assertEquals(0, cloned.getArgumentField().length);
  }
}
