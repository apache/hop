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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class HttpMetaTest {
  @BeforeAll
  static void before() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testLoadSave() throws Exception {
    Path path = Paths.get(Objects.requireNonNull(getClass().getResource("/transform.xml")).toURI());
    HttpMeta meta = loadHttpMetaFromXml(Files.readString(path));

    validateHttpMeta(meta);

    // Do an XML round trip
    //
    String xmlCopy =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);
    HttpMeta metaCopy = loadHttpMetaFromXml(xmlCopy);
    validateHttpMeta(metaCopy);
  }

  private static @NotNull HttpMeta loadHttpMetaFromXml(String xml) throws HopXmlException {
    HttpMeta meta = new HttpMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        HttpMeta.class,
        meta,
        new MemoryMetadataProvider());
    return meta;
  }

  private static void validateHttpMeta(HttpMeta meta) {
    assertNotNull(meta.getResultFields());
    assertEquals("result", meta.getResultFields().getFieldName());
    assertEquals("httpStatus", meta.getResultFields().getResultCodeFieldName());
    assertEquals("responseHeaders", meta.getResultFields().getResponseHeaderFieldName());
    assertEquals("responseTime", meta.getResultFields().getResponseTimeFieldName());

    assertEquals(Const.UTF_8, meta.getEncoding());
    assertEquals("http-user", meta.getHttpLogin());
    assertEquals("http-password", meta.getHttpPassword());
    assertEquals("url", meta.getUrl());
    assertEquals("urlField", meta.getUrlField());
    assertEquals("10000", meta.getConnectionTimeout());
    assertEquals("98765", meta.getCloseIdleConnectionsTime());
    assertEquals("proxyHost", meta.getProxyHost());
    assertEquals("proxyPort", meta.getProxyPort());
    assertTrue(meta.isUrlInField());
    assertTrue(meta.isIgnoreSsl());

    assertNotNull(meta.getLookupParameters());
    assertEquals(2, meta.getLookupParameters().getQueryParameters().size());
    HttpMeta.QueryParameter p0 = meta.getLookupParameters().getQueryParameters().get(0);
    assertEquals("queryField1", p0.getField());
    assertEquals("queryParameter1", p0.getParameter());
    HttpMeta.QueryParameter p1 = meta.getLookupParameters().getQueryParameters().get(1);
    assertEquals("queryField2", p1.getField());
    assertEquals("queryParameter2", p1.getParameter());

    assertEquals(3, meta.getLookupParameters().getHeaders().size());
    HttpMeta.HeaderParameter h0 = meta.getLookupParameters().getHeaders().get(0);
    assertEquals("headerField1", h0.getField());
    assertEquals("headerParameter1", h0.getParameter());
    HttpMeta.HeaderParameter h1 = meta.getLookupParameters().getHeaders().get(1);
    assertEquals("headerField2", h1.getField());
    assertEquals("headerParameter2", h1.getParameter());
    HttpMeta.HeaderParameter h2 = meta.getLookupParameters().getHeaders().get(2);
    assertEquals("headerField3", h2.getField());
    assertEquals("headerParameter3", h2.getParameter());
  }
}
