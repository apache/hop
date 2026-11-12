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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ContentType;
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
 * Issue #9273: stopping a pipeline has to end REST pagination, instead of fetching (and logging)
 * every remaining page up to the page limit.
 */
class RestPagingStopTest {

  @BeforeEach
  void setUp() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  @Test
  void stoppingEndsPaginationAfterTheCurrentPage() throws Exception {
    // PAGE_NUMBER keeps going while pages have content, and this server never runs dry: only
    // the stop (or maxPagesLoops) can end it.
    PagingRest rest = build("{\"id\":1}", null);
    rest.stopAfter = 2;

    rest.runPaginationLoop(new Object[] {"row"});

    assertEquals(2, requestCount(rest), "no page may be requested after the stop");
    assertEquals(2, rest.emitted.size());
  }

  @Test
  void stoppingEndsTheSplitOfTheCurrentPage() throws Exception {
    PagingRest rest = build("[1,2,3,4,5]", "$[*]");
    rest.stopAfter = 2;

    rest.runPaginationLoop(new Object[] {"row"});

    assertEquals(1, requestCount(rest));
    assertEquals(2, rest.emitted.size(), "the rest of the page is not turned into rows");
  }

  @Test
  void stoppingBeforeTheFirstPageIsNotAnError() throws Exception {
    // With a split path, zero rows normally fails the transform. After a stop it must not.
    PagingRest rest = build("[1,2,3]", "$[*]");
    rest.stopAfter = 0;

    rest.runPaginationLoop(new Object[] {"row"});

    assertEquals(0, requestCount(rest));
    assertEquals(0, rest.emitted.size());
  }

  @Test
  void withoutAStopPaginationRunsToMaxPagesLoops() throws Exception {
    PagingRest rest = build("{\"id\":1}", null);

    rest.runPaginationLoop(new Object[] {"row"});

    assertEquals(5, requestCount(rest));
  }

  private static long requestCount(PagingRest rest) {
    return mockingDetails(rest.data().client).getInvocations().stream()
        .filter(i -> i.getMethod().getName().equals("execute"))
        .count();
  }

  private PagingRest build(String body, String splitPath) throws Exception {
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
    meta.setMaxPagesLoops(5);
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
    CloseableHttpClient client =
        FakeHttpClient.returning(200, body, Map.of("Content-Type", "application/json"));
    data.client = client;

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("field1"));
    data.inputRowMeta = rowMeta;
    data.outputRowMeta = rowMeta.clone();

    PagingRest rest =
        new PagingRest(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine());
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    // The connection is normally loaded from metadata in init().
    RestConnection connection = new RestConnection();
    connection.setPaginationType(RestPaginationType.PAGE_NUMBER);
    Field field = Rest.class.getDeclaredField("connection");
    field.setAccessible(true);
    field.set(rest, connection);
    return rest;
  }

  /** Collects the emitted rows, and pretends the pipeline was stopped after a number of them. */
  static class PagingRest extends Rest {
    final List<Object[]> emitted = new ArrayList<>();
    int stopAfter = -1;

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

    @Override
    public boolean isStopped() {
      return stopAfter >= 0 && emitted.size() >= stopAfter;
    }
  }
}
