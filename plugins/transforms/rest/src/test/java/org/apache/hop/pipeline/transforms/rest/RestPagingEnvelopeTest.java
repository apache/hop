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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.rest.RestConnection;
import org.apache.hop.metadata.rest.RestPaginationType;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Page-number paging against an API that counts pages from zero, wants a page size, and wraps its
 * items in an envelope that is never empty (issues #9548 and #9549). The fake API below behaves
 * like the Ironclad public API: {@code GET ?page=N&pageSize=M} returns {@code
 * {"page":N,"pageSize":M,"count":6,"list":[...]}}, with {@code "list":[]} past the last page.
 */
class RestPagingEnvelopeTest {

  private static final int TOTAL_ITEMS = 6;

  @BeforeEach
  void setUp() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  // ------------------------------------------------------------------ #9548

  @Test
  void zeroBasedPagesWithAPageSizeReadEveryItemOnce() throws Exception {
    RestConnection connection = pageNumberConnection();
    connection.setFirstPageNumber("0");
    connection.setPageSizeParamName("pageSize");
    connection.setDefaultLimit(2);
    PagingRest rest = build(connection, "$.list[*]", 10, RestPagingEnvelopeTest::envelopeJson);

    rest.runPaginationLoop(new Object[] {"row"});

    assertEquals(
        List.of("page=0&pageSize=2", "page=1&pageSize=2", "page=2&pageSize=2", "page=3&pageSize=2"),
        rest.queries);
    assertEquals(TOTAL_ITEMS, rest.emitted.size());
  }

  @Test
  void anOldConnectionStillStartsAtPageOneWithoutAPageSize() throws Exception {
    // Saved before #9548: no first page number and no page size parameter. The editor keeps the
    // fields of other paging modes, so a limit parameter and a default limit left over from an
    // earlier OFFSET_LIMIT setup must not make it send a page size it never sent before.
    RestConnection connection = pageNumberConnection();
    connection.setLimitParamName("pageSize");
    connection.setDefaultLimit(2);
    PagingRest rest = build(connection, null, 1, uri -> "{\"list\":[1]}");

    rest.runPaginationLoop(new Object[] {"row"});

    assertEquals(List.of("page=1"), rest.queries);
  }

  @Test
  void aDefaultLimitWithoutAParameterNameSendsNoPageSize() throws Exception {
    RestConnection connection = pageNumberConnection();
    connection.setDefaultLimit(50);
    PagingRest rest = build(connection, null, 1, uri -> "{\"list\":[1]}");

    rest.runPaginationLoop(new Object[] {"row"});

    assertEquals(List.of("page=1"), rest.queries);
  }

  @Test
  void theFirstPageNumberCanBeAVariable() throws Exception {
    RestConnection connection = pageNumberConnection();
    connection.setFirstPageNumber("${FIRST_PAGE}");
    PagingRest rest = build(connection, null, 1, uri -> "{\"list\":[1]}");
    rest.setVariable("FIRST_PAGE", "0");

    rest.runPaginationLoop(new Object[] {"row"});

    assertEquals(List.of("page=0"), rest.queries);
  }

  @Test
  void aFirstPageNumberThatIsNotANumberIsAnError() throws Exception {
    RestConnection connection = pageNumberConnection();
    connection.setFirstPageNumber("first");
    PagingRest rest = build(connection, null, 1, uri -> "{\"list\":[1]}");

    assertThrows(HopException.class, () -> rest.runPaginationLoop(new Object[] {"row"}));
    assertTrue(rest.queries.isEmpty());
  }

  // ------------------------------------------------------------------ #9549

  @Test
  void withoutAnItemsPathAnEnvelopeOnlyStopsAtTheSafeguard() throws Exception {
    // Today's behaviour, kept: the body is never empty, so only maxPagesLoops ends it.
    PagingRest rest = build(zeroBasedEnvelope(), null, 7, RestPagingEnvelopeTest::envelopeJson);

    rest.runPaginationLoop(new Object[] {"row"});

    assertEquals(7, rest.queries.size());
  }

  @Test
  void anItemsPathEndsPagingOnTheFirstEmptyPage() throws Exception {
    RestConnection connection = zeroBasedEnvelope();
    connection.setItemsJsonPath("$.list");
    PagingRest rest = build(connection, null, 50, RestPagingEnvelopeTest::envelopeJson);

    rest.runPaginationLoop(new Object[] {"row"});

    // One row per page, the empty last page included.
    assertEquals(4, rest.queries.size());
    assertEquals(4, rest.emitted.size());
  }

  @Test
  void aTotalCountStopsAfterTheLastFullPage() throws Exception {
    RestConnection connection = zeroBasedEnvelope();
    connection.setItemsJsonPath("$.list");
    connection.setTotalCountJsonPath("$.count");
    PagingRest rest = build(connection, null, 50, RestPagingEnvelopeTest::envelopeJson);

    rest.runPaginationLoop(new Object[] {"row"});

    assertEquals(3, rest.queries.size(), "the empty page after the last item is not requested");
    assertEquals(3, rest.emitted.size());
  }

  @Test
  void aTotalCountCountsWithTheSplitPathWhenThereIsNoItemsPath() throws Exception {
    RestConnection connection = zeroBasedEnvelope();
    connection.setTotalCountJsonPath("$.count");
    PagingRest rest = build(connection, "$.list[*]", 50, RestPagingEnvelopeTest::envelopeJson);

    rest.runPaginationLoop(new Object[] {"row"});

    assertEquals(3, rest.queries.size());
    assertEquals(TOTAL_ITEMS, rest.emitted.size());
  }

  @Test
  void theItemsPathDecidesTheStopAndTheSplitPathTheRows() throws Exception {
    // The split path picks one field per item and so always finds something; the items path
    // still sees the empty page.
    RestConnection connection = zeroBasedEnvelope();
    connection.setItemsJsonPath("$.list");
    PagingRest rest = build(connection, "$.page", 50, RestPagingEnvelopeTest::envelopeJson);

    rest.runPaginationLoop(new Object[] {"row"});

    assertEquals(4, rest.queries.size());
    assertEquals(4, rest.emitted.size());
  }

  @Test
  void anItemsPathAlsoEndsOffsetPaging() throws Exception {
    RestConnection connection = new RestConnection();
    connection.setPaginationType(RestPaginationType.OFFSET_LIMIT);
    connection.setDefaultLimit(2);
    connection.setItemsJsonPath("$.list");
    PagingRest rest =
        build(
            connection,
            null,
            50,
            uri -> {
              Map<String, String> q = query(uri);
              int offset = Integer.parseInt(q.get("offset"));
              return envelope(offset / 2, 2);
            });

    rest.runPaginationLoop(new Object[] {"row"});

    assertEquals(
        List.of("offset=0&limit=2", "offset=2&limit=2", "offset=4&limit=2", "offset=6&limit=2"),
        rest.queries);
  }

  @Test
  void xmlItemsAndTotalCountUseTheXPaths() throws Exception {
    RestConnection connection = zeroBasedEnvelope();
    connection.setItemsXPath("/page/list/item");
    connection.setTotalCountXPath("/page/count");
    PagingRest rest =
        build(
            connection,
            null,
            50,
            uri -> {
              Map<String, String> q = query(uri);
              int page = Integer.parseInt(q.get("page"));
              StringBuilder xml = new StringBuilder("<page><count>6</count><list>");
              for (int id = page * 2 + 1; id <= Math.min(page * 2 + 2, TOTAL_ITEMS); id++) {
                xml.append("<item>").append(id).append("</item>");
              }
              return xml.append("</list></page>").toString();
            });
    rest.getMeta().setApplicationType(RestMeta.APPLICATION_TYPE_XML);
    rest.data().mediaType = ContentType.APPLICATION_XML;

    rest.runPaginationLoop(new Object[] {"row"});

    assertEquals(3, rest.queries.size());
  }

  // ------------------------------------------------------------------ helpers

  private static RestConnection pageNumberConnection() {
    RestConnection connection = new RestConnection();
    connection.setName("paging");
    connection.setPaginationType(RestPaginationType.PAGE_NUMBER);
    connection.setPageParamName("page");
    return connection;
  }

  private static RestConnection zeroBasedEnvelope() {
    RestConnection connection = pageNumberConnection();
    connection.setFirstPageNumber("0");
    connection.setPageSizeParamName("pageSize");
    connection.setDefaultLimit(2);
    return connection;
  }

  /** The Ironclad-style envelope for a zero-based page of the given size. */
  private static String envelopeJson(URI uri) {
    Map<String, String> q = query(uri);
    return envelope(Integer.parseInt(q.get("page")), Integer.parseInt(q.get("pageSize")));
  }

  private static String envelope(int page, int pageSize) {
    StringBuilder list = new StringBuilder();
    for (int id = page * pageSize + 1; id <= Math.min((page + 1) * pageSize, TOTAL_ITEMS); id++) {
      if (!list.isEmpty()) {
        list.append(',');
      }
      list.append("{\"id\":").append(id).append('}');
    }
    return "{\"page\":"
        + page
        + ",\"pageSize\":"
        + pageSize
        + ",\"count\":"
        + TOTAL_ITEMS
        + ",\"list\":["
        + list
        + "]}";
  }

  private static Map<String, String> query(URI uri) {
    Map<String, String> q = new LinkedHashMap<>();
    if (uri.getRawQuery() != null) {
      for (String pair : uri.getRawQuery().split("&")) {
        int eq = pair.indexOf('=');
        q.put(pair.substring(0, eq), pair.substring(eq + 1));
      }
    }
    return q;
  }

  private PagingRest build(
      RestConnection connection, String splitPath, int maxPages, Function<URI, String> api)
      throws Exception {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setDefault();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com/items");
    meta.setPaginationEnabled(true);
    meta.setMaxPagesLoops(maxPages);
    meta.setResultSplitPath(splitPath);
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");
    meta.getResultField().setCode("status");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = "http://example.com/items";
    data.resultFieldName = "result";
    data.resultCodeFieldName = "status";

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("field1"));
    data.inputRowMeta = rowMeta;
    data.outputRowMeta = rowMeta.clone();

    PagingRest rest =
        new PagingRest(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine());
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));
    data.client = fakeApi(api, rest.queries);

    // The connection is normally loaded from metadata in init().
    Field field = Rest.class.getDeclaredField("connection");
    field.setAccessible(true);
    field.set(rest, connection);
    return rest;
  }

  /** A client that answers each request with whatever the API function returns for its URI. */
  private static CloseableHttpClient fakeApi(Function<URI, String> api, List<String> queries)
      throws Exception {
    CloseableHttpClient client = mock(CloseableHttpClient.class);
    when(client.execute(any(ClassicHttpRequest.class), any(HttpClientResponseHandler.class)))
        .thenAnswer(
            invocation -> {
              ClassicHttpRequest request = invocation.getArgument(0);
              URI uri = request.getUri();
              queries.add(uri.getRawQuery());
              BasicClassicHttpResponse response = new BasicClassicHttpResponse(200);
              response.setEntity(
                  new ByteArrayEntity(
                      api.apply(uri).getBytes(StandardCharsets.UTF_8),
                      ContentType.APPLICATION_JSON));
              HttpClientResponseHandler<?> handler = invocation.getArgument(1);
              return handler.handleResponse(response);
            });
    return client;
  }

  /** Collects the emitted rows. */
  static class PagingRest extends Rest {
    final List<Object[]> emitted = new ArrayList<>();
    final List<String> queries = new ArrayList<>();

    PagingRest(
        TransformMeta transformMeta,
        RestMeta meta,
        RestData data,
        int copyNr,
        PipelineMeta pipelineMeta,
        Pipeline pipeline) {
      super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
    }

    RestData data() {
      return (RestData) getData();
    }

    @Override
    public void putRow(IRowMeta rowMeta, Object[] row) {
      emitted.add(row);
    }
  }
}
