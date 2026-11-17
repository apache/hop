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

package org.apache.hop.lineage.datahub;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.datacatalog.CatalogAnswer;
import org.apache.hop.datacatalog.CatalogAnswer.Reason;
import org.apache.hop.datacatalog.CatalogEntityId;
import org.apache.hop.datacatalog.DatasetRef;
import org.apache.hop.datacatalog.IAssertionCapability.AssertionResult;
import org.apache.hop.datacatalog.IAssertionCapability.Outcome;
import org.apache.hop.datacatalog.IDeprecationCapability.Deprecation;
import org.apache.hop.datacatalog.ISchemaCapability.CatalogField;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DataHubDataCatalogTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ORDERS =
      "urn:li:dataset:(urn:li:dataPlatform:postgres,sales.public.orders,DEV)";

  /** The answer to a GraphQL operation, by operation name; a missing one answers {@code {}}. */
  private final Map<String, Answer> answers = new ConcurrentHashMap<>();

  private final List<JsonNode> requests = new CopyOnWriteArrayList<>();
  private final List<String> authorizations = new CopyOnWriteArrayList<>();
  private HttpServer server;
  private DataHubDataCatalog catalog;

  private record Answer(int status, String body, long delayMs) {}

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/api/graphql", this::handle);
    server.start();
  }

  @AfterEach
  void stopServer() {
    if (catalog != null) {
      catalog.close();
    }
    server.stop(0);
  }

  private void handle(HttpExchange exchange) throws IOException {
    JsonNode request = MAPPER.readTree(exchange.getRequestBody());
    requests.add(request);
    authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
    String query = request.path("query").asText();
    Answer answer = new Answer(200, "{\"data\":{}}", 0);
    for (Map.Entry<String, Answer> entry : answers.entrySet()) {
      if (query.contains(entry.getKey())) {
        answer = entry.getValue();
      }
    }
    if (answer.delayMs() > 0) {
      try {
        Thread.sleep(answer.delayMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    byte[] body = answer.body().getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(answer.status(), body.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(body);
    }
  }

  private void answer(String operation, String data) {
    answers.put(operation, new Answer(200, "{\"data\":" + data + "}", 0));
  }

  private String url() {
    return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
  }

  private DataHubDataCatalog connect(String url) throws HopException {
    DataHubDataCatalog configured = new DataHubDataCatalog();
    configured.setUrl("${DATAHUB_URL}");
    configured.setToken(Encr.encryptPasswordIfNotUsingVariables("my-token"));
    configured.setEnvironment("dev");
    configured.setReadTimeoutMs("500");
    IVariables variables = new Variables();
    variables.setVariable("DATAHUB_URL", url);
    catalog = configured.clone();
    catalog.initialize(variables, new MemoryMetadataProvider());
    return catalog;
  }

  @Test
  void aPingSaysTheVersionAndTheUserAndSendsTheToken() throws Exception {
    answer(
        "appConfig",
        "{\"me\":{\"corpUser\":{\"username\":\"hop\"}},\"appConfig\":{\"appVersion\":\"v1.7.0\"}}");

    CatalogAnswer<String> answer = connect(url()).ping();

    assertEquals(CatalogAnswer.known("DataHub v1.7.0, reading as hop"), answer);
    assertEquals("Bearer my-token", authorizations.get(0));
  }

  @Test
  void aTableResolvesToTheUrnTheLineageSinkWrites() throws Exception {
    answer("hopDatasetExists", "{\"dataset\":{\"urn\":\"" + ORDERS + "\",\"exists\":true}}");

    CatalogAnswer<CatalogEntityId> answer =
        connect(url()).resolve(DatasetRef.of("postgres://db:5432", "sales.public.orders"));

    assertEquals(CatalogAnswer.known(new CatalogEntityId(ORDERS)), answer);
    assertEquals(ORDERS, requests.get(0).path("variables").path("urn").asText());
  }

  @Test
  void aDatasetDataHubDoesNotHaveIsNotFound() throws Exception {
    connect(url());
    answer("hopDatasetExists", "{\"dataset\":null}");
    assertInstanceOf(CatalogAnswer.NotFound.class, catalog.resolve(DatasetRef.ofCatalogId(ORDERS)));

    answer("hopDatasetExists", "{\"dataset\":{\"urn\":\"" + ORDERS + "\",\"exists\":false}}");
    assertInstanceOf(CatalogAnswer.NotFound.class, catalog.resolve(DatasetRef.ofCatalogId(ORDERS)));
  }

  @Test
  void anIncompleteDatasetIsNotAskedFor() throws Exception {
    CatalogAnswer<CatalogEntityId> answer =
        connect(url()).resolve(DatasetRef.of(null, "sales.public.orders"));

    CatalogAnswer.Unknown<CatalogEntityId> unknown =
        assertInstanceOf(CatalogAnswer.Unknown.class, answer);
    assertEquals(Reason.NOT_RESOLVABLE, unknown.reason());
    assertTrue(requests.isEmpty());
  }

  @Test
  void theSchemaHasTheTagsAndTermsOfTheIngestedSchemaAndOfTheUi() throws Exception {
    answer(
        "hopDatasetSchema",
        "{\"dataset\":{\"schemaMetadata\":{\"fields\":["
            + "{\"fieldPath\":\"id\",\"nativeDataType\":\"INTEGER\",\"type\":\"NUMBER\","
            + "\"nullable\":false,\"globalTags\":null,\"glossaryTerms\":null},"
            + "{\"fieldPath\":\"email\",\"nativeDataType\":\"VARCHAR(255)\",\"type\":\"STRING\","
            + "\"nullable\":true,\"globalTags\":{\"tags\":[{\"tag\":{\"urn\":\"urn:li:tag:PII\","
            + "\"name\":\"PII\",\"properties\":null}}]},\"glossaryTerms\":null},"
            + "{\"fieldPath\":\"[version=2.0].[type=struct].address.[type=string].city\","
            + "\"nativeDataType\":\"string\",\"type\":\"STRING\"}]},"
            + "\"editableSchemaMetadata\":{\"editableSchemaFieldInfo\":["
            + "{\"fieldPath\":\"email\",\"globalTags\":{\"tags\":[{\"tag\":"
            + "{\"urn\":\"urn:li:tag:Legacy\",\"name\":\"Legacy\",\"properties\":null}}]},"
            + "\"glossaryTerms\":null},"
            + "{\"fieldPath\":\"[version=2.0].[type=string].email\",\"globalTags\":{\"tags\":[{\"tag\":"
            + "{\"urn\":\"urn:li:tag:Contact\",\"name\":\"Contact\",\"properties\":{\"name\":\"Contact\"}}}]},"
            + "\"glossaryTerms\":{\"terms\":[{\"term\":{\"urn\":\"urn:li:glossaryTerm:7d4e\","
            + "\"name\":\"7d4e\",\"hierarchicalName\":\"7d4e\","
            + "\"properties\":{\"name\":\"Personal\"},\"parentNodes\":{\"nodes\":["
            + "{\"properties\":{\"name\":\"Classification\"}},"
            + "{\"properties\":{\"name\":\"Governance\"}}]}}}]}}]}}}");

    CatalogAnswer<List<CatalogField>> answer = connect(url()).schema(new CatalogEntityId(ORDERS));

    List<CatalogField> fields =
        assertInstanceOf(CatalogAnswer.Known.class, answer).value() instanceof List<?> list
            ? list.stream().map(CatalogField.class::cast).toList()
            : List.of();
    assertEquals(
        List.of("id", "email", "address.city"), fields.stream().map(CatalogField::name).toList());
    CatalogField id = fields.get(0);
    assertEquals("INTEGER", id.nativeType());
    assertEquals("NUMBER", id.type());
    assertEquals(Boolean.FALSE, id.nullable());
    assertTrue(id.tags().isEmpty());
    CatalogField email = fields.get(1);
    assertEquals(
        Set.of(
            "urn:li:tag:PII",
            "PII",
            "urn:li:tag:Legacy",
            "Legacy",
            "urn:li:tag:Contact",
            "Contact"),
        email.tags());
    assertTrue(email.isLabelledWith("pii"));
    assertTrue(email.isLabelledWith("Governance.Classification.Personal"));
    assertTrue(email.isLabelledWith("urn:li:glossaryTerm:7d4e"));
    assertFalse(email.isLabelledWith("Classification.Personal"));
    assertTrue(email.isLabelledWith("Personal"));
    assertFalse(fields.get(2).isLabelledWith("PII"));
    assertNull(fields.get(2).nullable());
  }

  @Test
  void aDatasetWithoutASchemaHasNone() throws Exception {
    answer("hopDatasetSchema", "{\"dataset\":{\"schemaMetadata\":null}}");

    assertInstanceOf(
        CatalogAnswer.NotFound.class, connect(url()).schema(new CatalogEntityId(ORDERS)));
  }

  @Test
  void theDeprecationIsRead() throws Exception {
    answer(
        "hopDatasetDeprecation",
        "{\"dataset\":{\"deprecation\":{\"deprecated\":true,\"note\":\"Use orders_v2\","
            + "\"decommissionTime\":1767225600000}}}");

    CatalogAnswer<Deprecation> answer = connect(url()).deprecation(new CatalogEntityId(ORDERS));

    assertEquals(
        CatalogAnswer.known(
            new Deprecation(true, "Use orders_v2", Instant.ofEpochMilli(1767225600000L))),
        answer);
  }

  @Test
  void noDeprecationIsNotFound() throws Exception {
    answer("hopDatasetDeprecation", "{\"dataset\":{\"deprecation\":null}}");

    assertInstanceOf(
        CatalogAnswer.NotFound.class, connect(url()).deprecation(new CatalogEntityId(ORDERS)));
  }

  @Test
  void theLastUpdateIsTheNewestOperation() throws Exception {
    answer(
        "hopDatasetOperations",
        "{\"dataset\":{\"operations\":[{\"lastUpdatedTimestamp\":1000},"
            + "{\"lastUpdatedTimestamp\":3000},{\"lastUpdatedTimestamp\":2000}]}}");

    CatalogAnswer<Instant> answer = connect(url()).lastUpdated(new CatalogEntityId(ORDERS));

    assertEquals(CatalogAnswer.known(Instant.ofEpochMilli(3000)), answer);
  }

  @Test
  void noOperationIsNotFound() throws Exception {
    answer("hopDatasetOperations", "{\"dataset\":{\"operations\":[]}}");

    assertInstanceOf(
        CatalogAnswer.NotFound.class, connect(url()).lastUpdated(new CatalogEntityId(ORDERS)));
  }

  @Test
  void theLatestRunOfEveryAssertionIsRead() throws Exception {
    answer(
        "hopDatasetAssertions",
        "{\"dataset\":{\"assertions\":{\"total\":3,\"assertions\":["
            + "{\"urn\":\"urn:li:assertion:a\",\"info\":{\"description\":\"No null ids\"},"
            + "\"runEvents\":{\"runEvents\":[{\"timestampMillis\":5000,"
            + "\"result\":{\"type\":\"SUCCESS\"}}]}},"
            + "{\"urn\":\"urn:li:assertion:b\",\"info\":null,"
            + "\"runEvents\":{\"runEvents\":[{\"timestampMillis\":6000,"
            + "\"result\":{\"type\":\"FAILURE\"}}]}},"
            + "{\"urn\":\"urn:li:assertion:c\",\"info\":{\"description\":\"Fresh\"},"
            + "\"runEvents\":{\"runEvents\":[]}}]}}}");

    CatalogAnswer<List<AssertionResult>> answer =
        connect(url()).latestAssertionResults(new CatalogEntityId(ORDERS));

    assertEquals(
        CatalogAnswer.known(
            List.of(
                new AssertionResult(
                    "urn:li:assertion:a",
                    "No null ids",
                    Outcome.PASSED,
                    Instant.ofEpochMilli(5000)),
                new AssertionResult(
                    "urn:li:assertion:b", null, Outcome.FAILED, Instant.ofEpochMilli(6000)),
                new AssertionResult("urn:li:assertion:c", "Fresh", Outcome.UNKNOWN, null))),
        answer);
    assertEquals(
        DataHubDataCatalog.MAX_ASSERTIONS, requests.get(0).path("variables").path("count").asInt());
  }

  @Test
  void aDatasetWithoutAssertionsIsNotFound() throws Exception {
    answer(
        "hopDatasetAssertions", "{\"dataset\":{\"assertions\":{\"total\":0,\"assertions\":[]}}}");

    assertInstanceOf(
        CatalogAnswer.NotFound.class,
        connect(url()).latestAssertionResults(new CatalogEntityId(ORDERS)));
  }

  @Test
  void aRefusedTokenIsUnauthorized() throws Exception {
    answers.put("appConfig", new Answer(401, "Unauthorized", 0));
    assertEquals(Reason.UNAUTHORIZED, reasonOf(connect(url()).ping()));

    // DataHub answers a token without the privilege with a GraphQL error carrying the status
    answers.put(
        "appConfig",
        new Answer(
            200, "{\"errors\":[{\"message\":\"Unauthorized\",\"extensions\":{\"code\":403}}]}", 0));
    assertEquals(Reason.UNAUTHORIZED, reasonOf(catalog.ping()));
  }

  @Test
  void anUnreachableDataHubIsUnreachable() throws Exception {
    int freePort;
    try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
      freePort = socket.getLocalPort();
    }

    assertEquals(Reason.UNREACHABLE, reasonOf(connect("http://127.0.0.1:" + freePort).ping()));
  }

  @Test
  void aSlowDataHubTimesOut() throws Exception {
    answers.put("appConfig", new Answer(200, "{\"data\":{}}", 3000));

    assertEquals(Reason.TIMEOUT, reasonOf(connect(url()).ping()));
  }

  @Test
  void aQueryDataHubCannotRunIsAnError() throws Exception {
    answers.put(
        "hopDatasetOperations",
        new Answer(200, "{\"errors\":[{\"message\":\"Validation error\"}]}", 0));

    assertEquals(Reason.ERROR, reasonOf(connect(url()).lastUpdated(new CatalogEntityId(ORDERS))));
  }

  @Test
  void aCatalogWithoutAUrlCannotBeInitialized() {
    DataHubDataCatalog configured = new DataHubDataCatalog();

    assertThrows(
        HopException.class,
        () -> configured.initialize(new Variables(), new MemoryMetadataProvider()));
  }

  @Test
  void aTimeoutMustBeAPositiveNumber() {
    DataHubDataCatalog configured = new DataHubDataCatalog();
    configured.setUrl("http://localhost:8080");
    configured.setReadTimeoutMs("soon");

    HopException e =
        assertThrows(
            HopException.class,
            () -> configured.initialize(new Variables(), new MemoryMetadataProvider()));
    assertTrue(e.getMessage().contains("soon"));
  }

  @Test
  void anUninitializedCatalogDoesNotAsk() {
    CatalogAnswer<String> answer = new DataHubDataCatalog().ping();

    assertEquals(Reason.ERROR, reasonOf(answer));
    assertTrue(requests.isEmpty());
  }

  private static Reason reasonOf(CatalogAnswer<?> answer) {
    return assertInstanceOf(CatalogAnswer.Unknown.class, answer).reason();
  }
}
