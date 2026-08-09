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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.pipeline.transforms.rest.fields.HeaderField;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A REST client with no incoming hop is a starting point: a GET or a POST against a fixed endpoint
 * is meaningful on its own, so it makes its request once instead of never.
 *
 * <p>The distinction that matters is <em>no hop</em> versus <em>a hop that carried no rows</em>.
 * The second means something upstream decided there was nothing to do, and firing a request then —
 * a POST, say — would be actively wrong. So the decision comes from the pipeline layout, not from
 * what happens to arrive at runtime.
 */
class RestNoInputRowsTest {

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
  void aTransformWithoutAnIncomingHopStillMakesItsRequest() throws Exception {
    RecordingRest rest = build(false, meta -> {});

    assertTrue(rest.init());
    // The first call does the work and reports "no more rows"; there is only ever one.
    assertFalse(rest.processRow(), "a single request, then done");

    assertEquals(1, RestNoInputRowsTest.requestCount(rest));
    assertEquals("GET", FakeHttpClient.captured().getMethod());
    assertEquals(1, rest.emittedRows.size(), "one output row for the one response");
  }

  @Test
  void theOutputRowCarriesTheResultFields() throws Exception {
    RecordingRest rest = build(false, meta -> {});

    rest.init();
    rest.processRow();

    Object[] row = rest.emittedRows.get(0);
    assertNotNull(row);
    // No input fields to carry over, so the result fields start at index 0.
    assertEquals("{\"ok\":true}", row[0]);
    assertEquals(200L, row[1]);
  }

  @Test
  void aHopCarryingNoRowsStillMakesNoRequest() throws Exception {
    // The upstream transform exists but produces nothing. This must stay a no-op.
    RecordingRest rest = build(true, meta -> {});

    assertTrue(rest.init());
    assertFalse(rest.processRow());

    assertEquals(0, RestNoInputRowsTest.requestCount(rest));
    assertTrue(rest.emittedRows.isEmpty());
  }

  @Test
  void anOptionThatNeedsAFieldIsRejectedWithoutAnIncomingHop() throws Exception {
    RecordingRest rest =
        build(
            false,
            meta -> {
              meta.setUrlInField(true);
              meta.setUrlField("urlField");
            });
    rest.init();

    HopException e = assertThrows(HopException.class, rest::processRow);

    assertTrue(e.getMessage().contains("Accept URL from field"), e.getMessage());
    assertEquals(0, RestNoInputRowsTest.requestCount(rest));
  }

  @Test
  void headerFieldsAreRejectedWithoutAnIncomingHopToo() throws Exception {
    RecordingRest rest =
        build(
            false,
            meta -> meta.setHeaderFields(List.of(new HeaderField("authField", "Authorization"))));
    rest.init();

    HopException e = assertThrows(HopException.class, rest::processRow);

    assertTrue(e.getMessage().contains("Headers"), e.getMessage());
  }

  private static int requestCount(RecordingRest rest) {
    return rest.requests.size();
  }

  /**
   * @param withIncomingHop whether the pipeline gives the transform a predecessor
   */
  private RecordingRest build(boolean withIncomingHop, java.util.function.Consumer<RestMeta> tweak)
      throws HopException {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");

    TransformMeta restTransformMeta = new TransformMeta("REST", new RestMeta());
    pipelineMeta.addTransform(restTransformMeta);

    RestMeta meta = (RestMeta) restTransformMeta.getTransform();
    meta.setDefault();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com/status");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");
    meta.getResultField().setCode("status");
    tweak.accept(meta);

    RestData data = new RestData();
    data.client = FakeHttpClient.returning(200, "{\"ok\":true}", Map.of());

    RecordingRest rest =
        new RecordingRest(
            restTransformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine());
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));

    if (withIncomingHop) {
      // Added after construction on purpose: BaseTransform's constructor dispatches against the
      // running pipeline's row sets, which do not exist outside a real execution. init() reads the
      // layout, and that is what decides whether this transform waits for rows.
      TransformMeta upstream = new TransformMeta("upstream", new DummyMeta());
      pipelineMeta.addTransform(upstream);
      pipelineMeta.addPipelineHop(new PipelineHopMeta(upstream, restTransformMeta));
    }
    return rest;
  }

  /** Captures what the transform emitted, and how many requests it made. */
  static class RecordingRest extends Rest {
    final List<Object[]> emittedRows = new CopyOnWriteArrayList<>();
    final List<String> requests = new CopyOnWriteArrayList<>();

    RecordingRest(
        TransformMeta transformMeta,
        RestMeta meta,
        RestData data,
        int copyNr,
        PipelineMeta pipelineMeta,
        org.apache.hop.pipeline.Pipeline pipeline) {
      super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
    }

    @Override
    protected Object[] callRest(Object[] rowData) throws HopException {
      requests.add("call");
      return super.callRest(rowData);
    }

    @Override
    public void putRow(IRowMeta rowMeta, Object[] row) {
      emittedRows.add(row);
    }

    @Override
    public Object[] getRow() {
      // Stands in for an upstream transform that finished without producing anything.
      return null;
    }
  }
}
