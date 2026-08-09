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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hc.core5.http.ContentType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Issue #4094: at detailed level the transform logs the request as one literal block — request
 * line, headers, body — instead of only the scattered per-header and per-parameter lines, which
 * never showed the headers Hop adds for you.
 */
class RestRequestLoggingTest {

  @BeforeEach
  void setUp() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
  }

  @Test
  void theRequestIsLoggedAsOneLiteralBlock() throws Exception {
    CapturingRest rest = build(RestMeta.HTTP_METHOD_POST, "{\"name\":\"hop\"}");

    rest.callRest(new Object[] {"row"});

    String log = rest.detailedLog();
    assertTrue(log.contains("POST"), log);
    assertTrue(
        log.contains("/api/things?q=hop"), "the request line carries the query string:\n" + log);
    assertTrue(log.contains("X-Trace: abc123"), "a configured header:\n" + log);
    // Neither of these is a header on the request object: the client derives Host from the route
    // and Content-Type from the entity. Without them the block would misrepresent the request.
    assertTrue(log.contains("Host: example.com"), "the host:\n" + log);
    assertTrue(log.contains("Content-Type: application/json"), "the content type:\n" + log);
    assertTrue(log.contains("{\"name\":\"hop\"}"), "the body:\n" + log);
  }

  @Test
  void headersHopAddsItselfAreIncluded() throws Exception {
    // The point of building the block from the real request: Accept and Content-Type are set by
    // the transform, not by the user, and appeared nowhere in the log before.
    CapturingRest rest = build(RestMeta.HTTP_METHOD_POST, "{}");

    rest.callRest(new Object[] {"row"});

    assertTrue(rest.detailedLog().contains("Accept"), rest.detailedLog());
  }

  @Test
  void credentialsAreMasked() throws Exception {
    // Debug logs end up in tickets and CI output. A verbatim Authorization header would be a
    // credential leak, so this is the assertion that matters most here.
    CapturingRest rest = build(RestMeta.HTTP_METHOD_GET, null);
    rest.getRestData().headerNames = new String[] {"Authorization"};

    rest.callRest(new Object[] {"row"});

    String log = rest.detailedLog();
    assertTrue(log.contains("Authorization"), "the header is still listed:\n" + log);
    assertTrue(log.contains("********"), "masked:\n" + log);
    assertFalse(log.contains("super-secret-token"), "the token must never reach the log:\n" + log);
  }

  @Test
  void aBinaryBodyIsSummarisedRatherThanDecoded() throws Exception {
    CapturingRest rest = build(RestMeta.HTTP_METHOD_POST, "placeholder");
    rest.getRestData().binaryBody = true;
    // A binary body comes from a Binary field, not a String one.
    rest.getRestData().inputRowMeta.setValueMeta(3, new ValueMetaBinary("bodyField"));
    rest.rowValues =
        new Object[] {"row", "abc123", "hop", new byte[] {(byte) 0x89, 'P', 'N', 'G', (byte) 0xFF}};

    rest.callRest(new Object[] {"row"});

    String log = rest.detailedLog();
    assertTrue(log.contains("binary body"), log);
    assertTrue(log.contains("5 bytes"), log);
  }

  @Test
  void nothingIsLoggedBelowDetailedLevel() throws Exception {
    CapturingRest rest = build(RestMeta.HTTP_METHOD_GET, null);
    rest.detailedEnabled = false;

    rest.callRest(new Object[] {"row"});

    assertTrue(
        rest.detailedLog().isEmpty(), "the block is a detailed-level aid, not always-on noise");
  }

  private CapturingRest build(String method, String body) {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setMethod(method);
    meta.setUrl("http://example.com/api/things");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    RestData data = new RestData();
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = method;
    data.realUrl = "http://example.com/api/things";
    data.resultFieldName = "result";
    data.client = FakeHttpClient.returning(200, "{}", Map.of());

    // One configured header and one query parameter, so the block has something of each to show.
    data.useHeaders = true;
    data.nrheader = 1;
    data.headerNames = new String[] {"X-Trace"};
    data.indexOfHeaderFields = new int[] {1};
    data.useParams = true;
    data.nrParams = 1;
    data.paramNames = new String[] {"q"};
    data.indexOfParamFields = new int[] {2};

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("field1"));
    rowMeta.addValueMeta(new ValueMetaString("traceField"));
    rowMeta.addValueMeta(new ValueMetaString("qField"));
    if (body != null) {
      rowMeta.addValueMeta(new ValueMetaString("bodyField"));
      data.useBody = true;
      data.indexOfBodyField = 3;
    }
    data.inputRowMeta = rowMeta;

    CapturingRest rest =
        new CapturingRest(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine());
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));
    rest.setRestData(data);
    rest.rowValues =
        body == null
            ? new Object[] {"row", "abc123", "hop"}
            : new Object[] {"row", "abc123", "hop", body};
    return rest;
  }

  /** Collects what would have gone to the log, and lets a test choose the log level. */
  static class CapturingRest extends Rest {
    private final List<String> detailed = new CopyOnWriteArrayList<>();
    boolean detailedEnabled = true;
    Object[] rowValues;
    private RestData restData;

    RestData getRestData() {
      return restData;
    }

    void setRestData(RestData restData) {
      this.restData = restData;
    }

    CapturingRest(
        TransformMeta transformMeta,
        RestMeta meta,
        RestData data,
        int copyNr,
        PipelineMeta pipelineMeta,
        org.apache.hop.pipeline.Pipeline pipeline) {
      super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
    }

    @Override
    public boolean isDetailed() {
      return detailedEnabled;
    }

    @Override
    public void logDetailed(String message) {
      detailed.add(message);
    }

    @Override
    protected Object[] callRest(Object[] rowData) throws HopException {
      return super.callRest(rowValues);
    }

    String detailedLog() {
      return String.join("\n", detailed);
    }
  }
}
