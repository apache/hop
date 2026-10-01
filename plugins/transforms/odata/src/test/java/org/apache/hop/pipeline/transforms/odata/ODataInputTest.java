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

package org.apache.hop.pipeline.transforms.odata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit test for {@link ODataInput} */
class ODataInputTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private HttpServer server;
  private int port;
  private final Map<String, String> responses = new ConcurrentHashMap<>();
  private final AtomicInteger statusCode = new AtomicInteger(200);
  private final AtomicReference<String> lastAuthorization = new AtomicReference<>();
  private final AtomicReference<String> lastAccept = new AtomicReference<>();

  @BeforeEach
  void setUp() throws IOException {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
    responses.clear();
    statusCode.set(200);
    lastAuthorization.set(null);
    lastAccept.set(null);

    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    port = server.getAddress().getPort();
    server.createContext(
        "/odata",
        exchange -> {
          lastAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
          lastAccept.set(exchange.getRequestHeaders().getFirst("Accept"));
          String key = exchange.getRequestURI().getPath();
          if (exchange.getRequestURI().getQuery() != null) {
            key += "?" + exchange.getRequestURI().getQuery();
          }
          String body = responses.getOrDefault(key, "{\"value\":[]}");
          byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(statusCode.get(), bytes.length);
          try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
          }
        });
    server.start();
  }

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void initFailsWhenServiceUrlIsEmpty() {
    ODataInputMeta meta = newMeta("Products");
    meta.setUrl("");
    ODataInput transform = newTransform(meta);

    assertFalse(transform.init());
  }

  @Test
  void initFailsWhenEntitySetIsEmpty() {
    ODataInputMeta meta = newMeta("");
    ODataInput transform = newTransform(meta);

    assertFalse(transform.init());
  }

  @Test
  void initBuildsUrlWithTrailingSlashAndQueryOptions() {
    ODataInputMeta meta = newMeta("Products");
    meta.setUrl(baseUrl());
    meta.setQuerySelect("ProductID,Name");
    meta.setQueryFilter("Name eq 'Chai'");
    meta.setQueryOrder("Name asc");
    meta.setQueryTop("3");
    meta.setQuerySkip("1");
    ODataInput transform = newTransform(meta);

    assertTrue(transform.init());
    String next = transform.getData().nextPageUrl;
    assertTrue(next.startsWith(baseUrl() + "/Products?"));
    assertTrue(next.contains("$select=" + encoded("ProductID,Name")));
    assertTrue(next.contains("$filter=" + encoded("Name eq 'Chai'")));
    assertTrue(next.contains("$orderby=" + encoded("Name asc")));
    assertTrue(next.contains("$top=" + encoded("3")));
    assertTrue(next.contains("$skip=" + encoded("1")));
    assertFalse(transform.getData().isFinishedReading);
    assertNotNull(transform.getData().httpClient);
    transform.dispose();
  }

  @Test
  void initKeepsExistingTrailingSlash() {
    ODataInputMeta meta = newMeta("Customers");
    meta.setUrl(baseUrl() + "/");
    ODataInput transform = newTransform(meta);

    assertTrue(transform.init());
    assertEquals(baseUrl() + "/Customers", transform.getData().nextPageUrl);
    transform.dispose();
  }

  @Test
  void initResolvesVariablesInUrlAndEntitySet() {
    ODataInputMeta meta = new ODataInputMeta();
    meta.setUrl("${SVC}");
    meta.setEntitySet("${SET}");
    ODataInput transform = newTransform(meta);
    transform.setVariable("SVC", baseUrl());
    transform.setVariable("SET", "Orders");

    assertTrue(transform.init());
    assertEquals(baseUrl() + "/Orders", transform.getData().nextPageUrl);
    transform.dispose();
  }

  @Test
  void initAcceptsBasicAuthWithoutCallingTheService() {
    ODataInputMeta meta = newMeta("Products");
    meta.setAuthType("BASIC");
    meta.setUsername("user");
    meta.setPassword("secret");
    ODataInput transform = newTransform(meta);

    assertTrue(transform.init());
    transform.dispose();
  }

  @Test
  void processRowReadsODataV4RecordsAndTypes() throws Exception {
    responses.put(
        "/odata/Products",
        """
        {"value":[
          {"ProductID":1,"ProductName":"Chai","UnitPrice":18.5,"Discontinued":false,"OrderDate":"2024-01-15"},
          {"ProductID":2,"ProductName":null,"UnitPrice":null,"Discontinued":true,"OrderDate":null}
        ]}
        """);
    ODataInputMeta meta = newMeta("Products");
    meta.getFields().add(new ODataField("ProductID", "ProductID", IValueMeta.TYPE_INTEGER, ""));
    meta.getFields().add(new ODataField("ProductName", "ProductName", IValueMeta.TYPE_STRING, ""));
    meta.getFields().add(new ODataField("UnitPrice", "UnitPrice", IValueMeta.TYPE_NUMBER, ""));
    meta.getFields()
        .add(new ODataField("Discontinued", "Discontinued", IValueMeta.TYPE_BOOLEAN, ""));
    meta.getFields()
        .add(new ODataField("OrderDate", "OrderDate", IValueMeta.TYPE_DATE, "yyyy-MM-dd"));

    List<Object[]> rows = runToCompletion(newTransform(meta));

    assertEquals(2, rows.size());
    assertEquals(1L, rows.get(0)[0]);
    assertEquals("Chai", rows.get(0)[1]);
    assertEquals(18.5d, (Double) rows.get(0)[2], 0.0001);
    assertEquals(Boolean.FALSE, rows.get(0)[3]);
    // Date parsing currently calls convertDataFromString with a null convertMeta, so the value
    // stays null instead of aborting the row.
    assertNull(rows.get(0)[4]);
    assertEquals(2L, rows.get(1)[0]);
    assertNull(rows.get(1)[1]);
    assertNull(rows.get(1)[2]);
    assertEquals(Boolean.TRUE, rows.get(1)[3]);
    assertNull(rows.get(1)[4]);
    assertEquals("application/json", lastAccept.get());
  }

  @Test
  void processRowReadsNestedODataPath() throws Exception {
    responses.put(
        "/odata/Customers",
        """
        {"value":[{"Name":"Alfreds","Address":{"City":"Berlin"}}]}
        """);
    ODataInputMeta meta = newMeta("Customers");
    meta.getFields().add(new ODataField("Name", "Name", IValueMeta.TYPE_STRING, ""));
    meta.getFields().add(new ODataField("City", "Address/City", IValueMeta.TYPE_STRING, ""));

    List<Object[]> rows = runToCompletion(newTransform(meta));

    assertEquals(1, rows.size());
    assertEquals("Alfreds", rows.get(0)[0]);
    assertEquals("Berlin", rows.get(0)[1]);
  }

  @Test
  void processRowFollowsAbsoluteODataV4NextLink() throws Exception {
    responses.put(
        "/odata/Products",
        "{\"value\":[{\"Name\":\"A\"}],\"@odata.nextLink\":\""
            + baseUrl()
            + "/Products?$skiptoken=2\"}");
    responses.put("/odata/Products?$skiptoken=2", "{\"value\":[{\"Name\":\"B\"}]}");
    ODataInputMeta meta = newMeta("Products");
    meta.getFields().add(new ODataField("Name", "Name", IValueMeta.TYPE_STRING, ""));

    List<Object[]> rows = runToCompletion(newTransform(meta));

    assertEquals(2, rows.size());
    assertEquals("A", rows.get(0)[0]);
    assertEquals("B", rows.get(1)[0]);
  }

  @Test
  void processRowResolvesRootRelativeNextLink() throws Exception {
    responses.put(
        "/odata/Products",
        "{\"value\":[{\"Name\":\"A\"}],\"@odata.nextLink\":\"/odata/Products?$skiptoken=2\"}");
    responses.put("/odata/Products?$skiptoken=2", "{\"value\":[{\"Name\":\"B\"}]}");
    ODataInputMeta meta = newMeta("Products");
    meta.getFields().add(new ODataField("Name", "Name", IValueMeta.TYPE_STRING, ""));

    List<Object[]> rows = runToCompletion(newTransform(meta));

    assertEquals(List.of("A", "B"), rows.stream().map(row -> row[0]).toList());
  }

  @Test
  void processRowResolvesRelativeNextLinkAgainstCurrentPath() throws Exception {
    responses.put(
        "/odata/Products",
        "{\"value\":[{\"Name\":\"A\"}],\"@odata.nextLink\":\"Products?$skiptoken=2\"}");
    responses.put("/odata/Products?$skiptoken=2", "{\"value\":[{\"Name\":\"B\"}]}");
    ODataInputMeta meta = newMeta("Products");
    meta.getFields().add(new ODataField("Name", "Name", IValueMeta.TYPE_STRING, ""));

    List<Object[]> rows = runToCompletion(newTransform(meta));

    assertEquals(2, rows.size());
    assertEquals("B", rows.get(1)[0]);
  }

  @Test
  void processRowReadsODataV2ResultsAndNext() throws Exception {
    responses.put(
        "/odata/Products",
        "{\"d\":{\"results\":[{\"Name\":\"V2A\"}],\"__next\":\""
            + baseUrl()
            + "/Products?$skiptoken=2\"}}");
    responses.put("/odata/Products?$skiptoken=2", "{\"d\":{\"results\":[{\"Name\":\"V2B\"}]}}");
    ODataInputMeta meta = newMeta("Products");
    meta.getFields().add(new ODataField("Name", "Name", IValueMeta.TYPE_STRING, ""));

    List<Object[]> rows = runToCompletion(newTransform(meta));

    assertEquals(List.of("V2A", "V2B"), rows.stream().map(row -> row[0]).toList());
  }

  @Test
  void processRowReadsODataV2SingleObjectAndArray() throws Exception {
    responses.put("/odata/Product", "{\"d\":{\"Name\":\"Single\"}}");
    ODataInputMeta singleMeta = newMeta("Product");
    singleMeta.getFields().add(new ODataField("Name", "Name", IValueMeta.TYPE_STRING, ""));
    List<Object[]> single = runToCompletion(newTransform(singleMeta));
    assertEquals("Single", single.get(0)[0]);

    responses.put("/odata/Items", "{\"d\":[{\"Name\":\"Arr\"}]}");
    ODataInputMeta arrayMeta = newMeta("Items");
    arrayMeta.getFields().add(new ODataField("Name", "Name", IValueMeta.TYPE_STRING, ""));
    List<Object[]> array = runToCompletion(newTransform(arrayMeta));
    assertEquals("Arr", array.get(0)[0]);
  }

  @Test
  void processRowFallsBackToPlainJsonObjectAndArray() throws Exception {
    responses.put("/odata/One", "{\"Name\":\"Plain\"}");
    ODataInputMeta objectMeta = newMeta("One");
    objectMeta.getFields().add(new ODataField("Name", "Name", IValueMeta.TYPE_STRING, ""));
    assertEquals("Plain", runToCompletion(newTransform(objectMeta)).get(0)[0]);

    responses.put("/odata/Many", "[{\"Name\":\"List\"}]");
    ODataInputMeta arrayMeta = newMeta("Many");
    arrayMeta.getFields().add(new ODataField("Name", "Name", IValueMeta.TYPE_STRING, ""));
    assertEquals("List", runToCompletion(newTransform(arrayMeta)).get(0)[0]);
  }

  @Test
  void processRowSendsBearerToken() throws Exception {
    responses.put("/odata/Products", "{\"value\":[{\"Name\":\"Secured\"}]}");
    ODataInputMeta meta = newMeta("Products");
    meta.setAuthType("BEARER");
    meta.setToken("abc-123");
    meta.getFields().add(new ODataField("Name", "Name", IValueMeta.TYPE_STRING, ""));

    runToCompletion(newTransform(meta));

    assertEquals("Bearer abc-123", lastAuthorization.get());
  }

  @Test
  void processRowReturnsNoRowsForEmptyPage() throws Exception {
    responses.put("/odata/Products", "{\"value\":[]}");
    ODataInputMeta meta = newMeta("Products");
    meta.getFields().add(new ODataField("Name", "Name", IValueMeta.TYPE_STRING, ""));

    List<Object[]> rows = runToCompletion(newTransform(meta));

    assertTrue(rows.isEmpty());
  }

  @Test
  void processRowWrapsNon200Status() {
    statusCode.set(500);
    responses.put("/odata/Products", "{\"error\":\"boom\"}");
    ODataInputMeta meta = newMeta("Products");
    meta.getFields().add(new ODataField("Name", "Name", IValueMeta.TYPE_STRING, ""));
    ODataInput transform = newTransform(meta);
    assertTrue(transform.init());

    HopException thrown = assertThrows(HopException.class, transform::processRow);
    assertTrue(thrown.getMessage().contains("Error requesting OData data page"));
    transform.dispose();
  }

  @Test
  void disposeClosesHttpClient() {
    ODataInput transform = newTransform(newMeta("Products"));
    assertTrue(transform.init());
    assertNotNull(transform.getData().httpClient);
    transform.dispose();
    transform.dispose();
  }

  private List<Object[]> runToCompletion(ODataInput transform) throws Exception {
    assertTrue(transform.init());
    List<Object[]> rows = new ArrayList<>();
    transform.addRowListener(
        new RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
            rows.add(row);
          }
        });
    while (transform.processRow()) {
      // drain every page
    }
    transform.dispose();
    return rows;
  }

  private ODataInput newTransform(ODataInputMeta meta) {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("OData Input");
    transformMeta.setTransform(meta);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("odata-input-test");
    pipelineMeta.addTransform(transformMeta);
    LocalPipelineEngine pipeline = spy(new LocalPipelineEngine());
    when(pipeline.isRunning()).thenReturn(true);
    return new ODataInput(transformMeta, meta, new ODataInputData(), 0, pipelineMeta, pipeline);
  }

  private ODataInputMeta newMeta(String entitySet) {
    ODataInputMeta meta = new ODataInputMeta();
    meta.setUrl(baseUrl());
    meta.setEntitySet(entitySet);
    meta.setAuthType("NONE");
    return meta;
  }

  private String baseUrl() {
    return "http://localhost:" + port + "/odata";
  }

  private static String encoded(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
