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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hc.core5.http.ContentType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Issue #2746: a streaming response produces one row per record as it arrives, rather than one row
 * holding the whole body.
 */
class RestStreamingTest {

  @BeforeEach
  void setUp() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  @Test
  void ndjsonEmitsOneRowPerLine() throws Exception {
    StreamingRest rest =
        build(RestStreamingFormat.NDJSON, "{\"id\":1}\n{\"id\":2}\n\n{\"id\":3}\n", meta -> {});

    Object[] summary = rest.callRest(new Object[] {"row"});

    assertEquals(3, rest.emitted.size(), "a row per non-blank line");
    assertEquals("{\"id\":1}", rest.emitted.get(0)[1]);
    assertEquals("{\"id\":3}", rest.emitted.get(2)[1]);
    // The rows have already gone downstream, so there is no summary row to add.
    assertEquals(null, summary);
  }

  @Test
  void ndjsonCarriesTheStatusOntoEveryRow() throws Exception {
    StreamingRest rest = build(RestStreamingFormat.NDJSON, "{\"a\":1}\n{\"a\":2}\n", meta -> {});

    rest.callRest(new Object[] {"row"});

    assertEquals(200L, rest.emitted.get(0)[2]);
    assertEquals(200L, rest.emitted.get(1)[2]);
  }

  @Test
  void sseEmitsOneRowPerEvent() throws Exception {
    String stream =
        ": a comment that is not a record\n"
            + "event: message\n"
            + "data: {\"id\":1}\n"
            + "\n"
            + "data: {\"id\":2}\n"
            + "\n";

    StreamingRest rest = build(RestStreamingFormat.SSE, stream, meta -> {});

    rest.callRest(new Object[] {"row"});

    assertEquals(2, rest.emitted.size(), "comments and framing fields are not records");
    // The payload reaches the row, not the "data:" framing around it.
    assertEquals("{\"id\":1}", rest.emitted.get(0)[1]);
    assertEquals("{\"id\":2}", rest.emitted.get(1)[1]);
  }

  @Test
  void sseJoinsMultipleDataLinesIntoOneRecord() throws Exception {
    StreamingRest rest =
        build(RestStreamingFormat.SSE, "data: line one\ndata: line two\n\n", meta -> {});

    rest.callRest(new Object[] {"row"});

    assertEquals(1, rest.emitted.size());
    assertEquals("line one\nline two", rest.emitted.get(0)[1]);
  }

  @Test
  void sseEmitsATrailingEventWithNoBlankLineAfterIt() throws Exception {
    StreamingRest rest = build(RestStreamingFormat.SSE, "data: last one", meta -> {});

    rest.callRest(new Object[] {"row"});

    assertEquals(1, rest.emitted.size());
    assertEquals("last one", rest.emitted.get(0)[1]);
  }

  @Test
  void stoppingTheTransformEndsTheStream() throws Exception {
    StreamingRest rest =
        build(RestStreamingFormat.NDJSON, "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n", meta -> {});
    // Without the stop check, an endless feed would ignore a stop request and hang the pipeline.
    rest.stopAfter = 1;

    rest.callRest(new Object[] {"row"});

    assertEquals(1, rest.emitted.size());
  }

  @Test
  void stoppingAnEndlessStreamActuallyEndsIt() throws Exception {
    // The reported bug: the read loop returned on stop, but the client then tried to drain what
    // was left of the entity so it could reuse the connection. On a feed that never ends, that
    // never returns and the transform sits in "Halting". The request has to be aborted.
    StreamingRest rest = build(RestStreamingFormat.NDJSON, "", meta -> {});
    rest.data().client = FakeHttpClient.endlessNdjson();
    rest.stopAfter = 5;

    // A plain assertion would hang rather than fail if this regressed, so bound it.
    assertTimeoutPreemptively(
        java.time.Duration.ofSeconds(10),
        () -> rest.callRest(new Object[] {"row"}),
        "stopping must end the stream instead of blocking on an endless entity");

    assertEquals(5, rest.emitted.size());
  }

  @Test
  void streamingRefusesToCombineWithASplitPath() {
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                buildAndInitFirstRow(
                    RestStreamingFormat.NDJSON, meta -> meta.setResultSplitPath("$[*]")));

    assertTrue(e.getMessage().contains("result split path"), e.getMessage());
  }

  @Test
  void streamingRefusesToCombineWithABinaryResult() {
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                buildAndInitFirstRow(
                    RestStreamingFormat.NDJSON, meta -> meta.getResultField().setBinary(true)));

    assertTrue(e.getMessage().contains("binary result"), e.getMessage());
  }

  @Test
  void sseEventTypeAndIdReachTheirOwnColumns() throws Exception {
    // Mapped fields rather than a JSON envelope: the record stays the record, so a payload that is
    // already JSON goes straight into a JSON Input transform without being unwrapped first.
    StreamingRest rest =
        build(
            RestStreamingFormat.SSE,
            "id: 1\nevent: created\ndata: {\"a\":1}\n\nid: 2\nevent: updated\ndata: {\"a\":2}\n\n",
            meta -> {
              meta.setStreamingEventNameField("event_name");
              meta.setStreamingEventIdField("event_id");
            });
    rest.data().streamingEventNameField = "event_name";
    rest.data().streamingEventIdField = "event_id";

    rest.callRest(new Object[] {"row"});

    assertEquals(2, rest.emitted.size());
    // input field, result, status, then the two mapped columns.
    assertEquals("{\"a\":1}", rest.emitted.get(0)[1]);
    assertEquals("created", rest.emitted.get(0)[3]);
    assertEquals("1", rest.emitted.get(0)[4]);
    assertEquals("updated", rest.emitted.get(1)[3]);
    assertEquals("2", rest.emitted.get(1)[4]);
  }

  @Test
  void anEventIdPersistsUntilTheServerSendsANewOne() throws Exception {
    // The spec calls it the "last event ID" - the point a consumer resumes from - so it carries
    // forward. The event type does not.
    StreamingRest rest =
        build(
            RestStreamingFormat.SSE,
            "id: 7\nevent: created\ndata: one\n\ndata: two\n\n",
            meta -> {});
    rest.data().streamingEventNameField = "event_name";
    rest.data().streamingEventIdField = "event_id";

    rest.callRest(new Object[] {"row"});

    assertEquals("created", rest.emitted.get(0)[3]);
    assertEquals("7", rest.emitted.get(0)[4]);
    assertEquals(null, rest.emitted.get(1)[3], "the event type resets");
    assertEquals("7", rest.emitted.get(1)[4], "the id persists");
  }

  @Test
  void unmappedEventFieldsAddNoColumns() throws Exception {
    StreamingRest rest =
        build(RestStreamingFormat.SSE, "id: 9\nevent: created\ndata: payload\n\n", meta -> {});
    // Nothing mapped, so nothing beyond the usual result fields.
    rest.callRest(new Object[] {"row"});

    Object[] row = rest.emitted.get(0);
    assertEquals("payload", row[1]);
    assertEquals(3, countNonNullTail(row), "no extra columns when the fields are not named");
  }

  private static int countNonNullTail(Object[] row) {
    int n = 0;
    for (Object o : row) {
      if (o != null) {
        n++;
      }
    }
    return n;
  }

  @Test
  void aFailedStreamingRequestIsAnErrorRatherThanZeroRows() {
    // It used to finish green with no rows on any non-2xx, so a 401 or a 404 looked exactly like
    // an empty feed and there was nothing to diagnose from.
    StreamingRest rest = build(RestStreamingFormat.NDJSON, "", meta -> {});
    rest.data().client = FakeHttpClient.returning(404, "{\"error\":\"no such stream\"}", Map.of());

    HopException e = assertThrows(HopException.class, () -> rest.callRest(new Object[] {"row"}));

    String message = e.toString() + (e.getCause() == null ? "" : e.getCause().toString());
    assertTrue(message.contains("404"), message);
    assertTrue(
        message.contains("no such stream"), "the server's explanation must survive: " + message);
    assertTrue(rest.emitted.isEmpty());
  }

  @Test
  void sseAsksForAnEventStream() throws Exception {
    // Asking an event-stream endpoint for application/json is a good way to get something that is
    // not a stream, or a 406.
    StreamingRest rest = build(RestStreamingFormat.SSE, "data: one\n\n", meta -> {});

    rest.callRest(new Object[] {"row"});

    assertEquals(
        "text/event-stream", FakeHttpClient.captured().getFirstHeader("Accept").getValue());
  }

  @Test
  void ndjsonKeepsTheConfiguredAcceptHeader() throws Exception {
    StreamingRest rest = build(RestStreamingFormat.NDJSON, "{}\n", meta -> {});

    rest.callRest(new Object[] {"row"});

    assertEquals("application/json", FakeHttpClient.captured().getFirstHeader("Accept").getValue());
  }

  /** Drives just the first-row setup, which is where the combinations are checked. */
  private void buildAndInitFirstRow(
      RestStreamingFormat format, java.util.function.Consumer<RestMeta> tweak) throws Exception {
    StreamingRest rest = build(format, "{}\n", tweak);
    rest.data().binaryResult = rest.meta().getResultField().isBinary();
    rest.checkCombinations();
  }

  @Test
  void theStreamingSettingsSurviveASaveAndLoad() throws Exception {
    // The dialog's own load/save is SWT and cannot run here, but the metadata round trip can — and
    // a key that does not serialise would break persistence just as surely.
    RestMeta meta = new RestMeta();
    meta.setDefault();
    meta.setStreamingEnabled(true);
    meta.setStreamingFormat(RestStreamingFormat.SSE);
    meta.setStreamingEventNameField("event_name");
    meta.setStreamingEventIdField("event_id");

    RestMeta loaded =
        XmlMetadataUtil.deSerializeFromXml(
            XmlHandler.getSubNode(
                XmlHandler.loadXmlString(
                    XmlHandler.openTag(TransformMeta.XML_TAG)
                        + meta.getXml()
                        + XmlHandler.closeTag(TransformMeta.XML_TAG)),
                TransformMeta.XML_TAG),
            RestMeta.class,
            new MemoryMetadataProvider());

    assertTrue(loaded.isStreamingEnabled());
    assertEquals(RestStreamingFormat.SSE, loaded.getStreamingFormat());
    assertEquals("event_name", loaded.getStreamingEventNameField());
    assertEquals("event_id", loaded.getStreamingEventIdField());
  }

  private StreamingRest build(
      RestStreamingFormat format, String body, java.util.function.Consumer<RestMeta> tweak) {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setDefault();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com/stream");
    meta.setStreamingEnabled(true);
    meta.setStreamingFormat(format);
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");
    meta.getResultField().setCode("status");
    tweak.accept(meta);

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = "http://example.com/stream";
    data.resultFieldName = "result";
    data.resultCodeFieldName = "status";
    data.streaming = true;
    data.streamingFormat = format;
    data.client =
        FakeHttpClient.returning(200, body, Map.of("Content-Type", "application/x-ndjson"));

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("field1"));
    data.inputRowMeta = rowMeta;
    data.outputRowMeta = rowMeta.clone();

    StreamingRest rest =
        new StreamingRest(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine());
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));
    return rest;
  }

  /** Collects the emitted rows, and can pretend the pipeline was stopped part-way. */
  static class StreamingRest extends Rest {
    final List<Object[]> emitted = new ArrayList<>();
    int stopAfter = -1;

    StreamingRest(
        TransformMeta transformMeta,
        RestMeta meta,
        RestData data,
        int copyNr,
        PipelineMeta pipelineMeta,
        org.apache.hop.pipeline.Pipeline pipeline) {
      super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
    }

    RestData data() {
      return (RestData) getData();
    }

    RestMeta meta() {
      return (RestMeta) getMeta();
    }

    /** Exposes the first-row validation for the combination tests. */
    void checkCombinations() throws Exception {
      java.lang.reflect.Method m =
          Rest.class.getDeclaredMethod("rejectUnsupportedStreamingCombinations");
      m.setAccessible(true);
      try {
        m.invoke(this);
      } catch (java.lang.reflect.InvocationTargetException e) {
        throw (Exception) e.getCause();
      }
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
