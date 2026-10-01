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

package org.apache.hop.pipeline.transforms.rest;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Map;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ContentType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.rest.fields.ResultField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * An HTTP client owns a connection pool, so it belongs to the transform copy and not to the
 * request: building one per row means a fresh TCP and TLS handshake for every row, and on the REST
 * connection path the abandoned clients were never closed at all. These tests pin the lifecycle
 * down — one client for every row, released on dispose.
 */
class RestClientLifecycleTest {

  private CloseableHttpClient client;

  @BeforeEach
  void setUp() {
    if (!HopLogStore.isInitialized()) {
      HopLogStore.init();
    }
    client = FakeHttpClient.returning(200, "{}", Map.of("Content-Type", "application/json"));
  }

  private Rest newRest(RestData data) throws HopException {
    TransformMeta transformMeta = new TransformMeta();
    transformMeta.setName("TestRest");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("TestRest");
    pipelineMeta.addTransform(transformMeta);

    RestMeta meta = new RestMeta();
    meta.setMethod(RestMeta.HTTP_METHOD_GET);
    meta.setUrl("http://example.com");
    meta.setResultField(new ResultField());
    meta.getResultField().setFieldName("result");

    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("field1"));
    data.inputRowMeta = inputRowMeta;
    data.mediaType = ContentType.APPLICATION_JSON;
    data.method = RestMeta.HTTP_METHOD_GET;
    data.realUrl = "http://example.com";
    data.resultFieldName = "result";

    Rest rest =
        spy(new Rest(transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
    doReturn(client).when(rest).createClient();
    rest.setMetadataProvider(mock(IHopMetadataProvider.class));
    return rest;
  }

  @Test
  void clientIsBuiltOnceAndReusedAcrossRows() throws HopException {
    RestData data = new RestData();
    Rest rest = newRest(data);

    rest.callRest(new Object[] {"row1"});
    rest.callRest(new Object[] {"row2"});
    rest.callRest(new Object[] {"row3"});

    // One client for three rows, kept on the data object so the connection pool survives.
    verify(rest, times(1)).createClient();
    assertSame(client, data.client);
  }

  @Test
  void clientIsNotClosedBetweenRows() throws Exception {
    RestData data = new RestData();
    Rest rest = newRest(data);

    rest.callRest(new Object[] {"row1"});
    rest.callRest(new Object[] {"row2"});

    verify(client, never()).close();
  }

  @Test
  void disposeClosesAndReleasesTheClient() throws Exception {
    RestData data = new RestData();
    Rest rest = newRest(data);

    rest.callRest(new Object[] {"row1"});
    rest.dispose();

    verify(client, times(1)).close();
    assertNull(data.client);
  }

  @Test
  void disposeWithoutAnyRequestDoesNotFail() throws HopException {
    RestData data = new RestData();
    Rest rest = newRest(data);

    rest.dispose();

    assertNull(data.client);
  }
}
