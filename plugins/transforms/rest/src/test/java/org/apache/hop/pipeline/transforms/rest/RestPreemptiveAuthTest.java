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

package org.apache.hop.pipeline.transforms.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hc.core5.http.ContentType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

/**
 * Issue #4196: the transform's "use preemptive authentication" checkbox must actually decide
 * whether Basic credentials go out on the first request or only after a 401 challenge. It used to
 * be serialized and drawn but never read, so every request was preemptive whatever the box said.
 *
 * <p>Run against a server in this JVM rather than a mock, because the difference is only visible in
 * the request sequence: preemptive is one request carrying Authorization, challenge-response is an
 * unauthenticated request, a 401, then a second request that carries it.
 */
class RestPreemptiveAuthTest {

  private HttpServer server;

  /** Every request the server saw, as "METHOD path <credentials or ->". */
  private final List<String> requests = new CopyOnWriteArrayList<>();

  @BeforeEach
  void startServer() throws IOException {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
    requests.clear();

    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/secret", this::handle);
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  @Test
  void preemptiveSendsCredentialsOnTheFirstRequest() throws Exception {
    Object[] row = run(true);

    assertNotNull(row);
    assertEquals(1, requests.size(), "preemptive auth must not need a challenge round trip");
    assertEquals("GET /secret hop:secret", requests.get(0));
  }

  @Test
  void nonPreemptiveWaitsForTheChallenge() throws Exception {
    Object[] row = run(false);

    assertNotNull(row);
    assertEquals(2, requests.size(), "challenge-response auth takes an extra round trip");
    assertEquals("GET /secret -", requests.get(0), "the first request must be unauthenticated");
    assertEquals("GET /secret hop:secret", requests.get(1));
  }

  @Test
  void aPipelineSavedBeforeTheOptionExistedStaysPreemptive() {
    // The old "preemptive" key was written by every version of this transform and read by none,
    // so it carries no intent. What matters is that a file without the new key keeps the
    // behaviour it has always had.
    RestMeta meta = new RestMeta();
    meta.setDefault();

    assertTrue(meta.isPreemptive());
    assertFalse(meta.isNonPreemptiveBasicAuth());
  }

  @Test
  void aStoredOldPreemptiveKeyLoadsWithoutErrorAndIsIgnored() throws Exception {
    // Every pipeline in the wild still carries this element. Dropping the property must not make
    // those files fail to load, and must not change what they do — whichever value they carry,
    // because neither value was ever a choice anyone made.
    for (String stored : new String[] {"N", "Y"}) {
      RestMeta meta = fromXml("<transform><preemptive>" + stored + "</preemptive></transform>");

      assertTrue(
          meta.isPreemptive(),
          "a pipeline carrying <preemptive>" + stored + "> must stay preemptive");
      assertFalse(meta.isNonPreemptiveBasicAuth());
    }
  }

  @Test
  void theNewKeyRoundTrips() throws Exception {
    assertFalse(
        fromXml("<transform><non_preemptive_basic_auth>Y</non_preemptive_basic_auth></transform>")
            .isPreemptive());
    assertTrue(
        fromXml("<transform><non_preemptive_basic_auth>N</non_preemptive_basic_auth></transform>")
            .isPreemptive());
  }

  private static RestMeta fromXml(String xml) throws Exception {
    Node node = XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), TransformMeta.XML_TAG);
    return XmlMetadataUtil.deSerializeFromXml(node, RestMeta.class, new MemoryMetadataProvider());
  }

  @Test
  void theOldPreemptiveKeyIsGone() {
    // Removed rather than repurposed: reading <preemptive>N</preemptive> back would have flipped
    // every existing pipeline to challenge-response.
    assertNull(fieldNamed("preemptive"));
    assertNotNull(fieldNamed("nonPreemptiveBasicAuth"));
  }

  private static Field fieldNamed(String name) {
    for (Field field : RestMeta.class.getDeclaredFields()) {
      if (field.getName().equals(name)) {
        return field;
      }
    }
    return null;
  }

  /** Runs one row through the transform against the local server. */
  private Object[] run(boolean preemptive) throws HopException {
    String url = "http://localhost:" + server.getAddress().getPort() + "/secret";

    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setDefault();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl(url);
    meta.setHttpLogin("hop");
    meta.setHttpPassword("secret");
    meta.setPreemptive(preemptive);
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");
    meta.getResultField().setCode("status");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = url;
    data.resultFieldName = "result";
    data.resultCodeFieldName = "status";
    data.realHttpLogin = "hop";
    data.realHttpPassword = "secret";

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("field1"));
    data.inputRowMeta = inputRowMeta;

    Rest rest = new Rest(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine());
    rest.setMetadataProvider(new MemoryMetadataProvider());
    return rest.callRest(new Object[] {"value1"});
  }

  private void handle(HttpExchange exchange) throws IOException {
    String authorization = exchange.getRequestHeaders().getFirst("Authorization");
    requests.add(
        exchange.getRequestMethod()
            + " "
            + exchange.getRequestURI().getPath()
            + " "
            + (authorization == null ? "-" : decodeBasic(authorization)));

    if (authorization == null) {
      exchange.getResponseHeaders().add("WWW-Authenticate", "Basic realm=\"hop\"");
      respond(exchange, 401, "unauthorized");
      return;
    }
    respond(exchange, 200, "{\"ok\":true}");
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  private static String decodeBasic(String header) {
    if (!header.startsWith("Basic ")) {
      return header;
    }
    return new String(
        Base64.getDecoder().decode(header.substring("Basic ".length())), StandardCharsets.UTF_8);
  }
}
