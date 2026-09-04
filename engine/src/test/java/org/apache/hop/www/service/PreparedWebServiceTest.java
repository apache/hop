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

package org.apache.hop.www.service;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.junit.jupiter.api.Test;

/**
 * Drives the row listener that turns pipeline rows into the response body. Both {@code
 * /hop/webService} and the JSON API stream through this, so the binary/text split and the status
 * code field behave the same on either.
 */
class PreparedWebServiceTest {

  private final ByteArrayOutputStream sink = new ByteArrayOutputStream();
  private RowAdapter listener;

  private PreparedWebService prepare(String fieldName, String statusCodeField) throws Exception {
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    IEngineComponent component = mock(IEngineComponent.class);
    when(pipeline.findComponent("out", 0)).thenReturn(component);
    doAnswer(
            invocation -> {
              listener = invocation.getArgument(0);
              return null;
            })
        .when(component)
        .addRowListener(any());

    return new PreparedWebService(
        pipeline, "obj-1", "application/json", "UTF-8", "out", fieldName, statusCodeField);
  }

  private IWebServiceOutput output() {
    return new IWebServiceOutput() {
      @Override
      public void setContentType(String contentType, String encoding) {
        // recorded by the caller when it matters
      }

      @Override
      public void setStatus(int statusCode) {
        // recorded by the caller when it matters
      }

      @Override
      public OutputStream getOutputStream() {
        return sink;
      }
    };
  }

  @Test
  void aTextFieldIsWrittenAsUtf8() throws Exception {
    PreparedWebService prepared = prepare("msg", "");
    prepared.execute(output());

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("msg"));
    listener.rowWrittenEvent(rowMeta, new Object[] {"héllo"});

    assertEquals("héllo", sink.toString(StandardCharsets.UTF_8));
  }

  @Test
  void everyRowIsAppendedInOrder() throws Exception {
    PreparedWebService prepared = prepare("msg", "");
    prepared.execute(output());

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("msg"));
    listener.rowWrittenEvent(rowMeta, new Object[] {"a"});
    listener.rowWrittenEvent(rowMeta, new Object[] {"b"});
    listener.rowWrittenEvent(rowMeta, new Object[] {"c"});

    assertEquals("abc", sink.toString(StandardCharsets.UTF_8));
  }

  @Test
  void aBinaryFieldIsWrittenRawWithoutEncodingConversion() throws Exception {
    PreparedWebService prepared = prepare("blob", "");
    prepared.execute(output());

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBinary("blob"));
    byte[] raw = {0x00, (byte) 0xFF, 0x10, (byte) 0x80};
    listener.rowWrittenEvent(rowMeta, new Object[] {raw});

    assertArrayEquals(raw, sink.toByteArray());
  }

  @Test
  void aNullBinaryFieldWritesNothingRatherThanFailing() throws Exception {
    PreparedWebService prepared = prepare("blob", "");
    prepared.execute(output());

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBinary("blob"));
    listener.rowWrittenEvent(rowMeta, new Object[] {null});

    assertEquals(0, sink.size());
  }

  @Test
  void aMissingOutputFieldIsReportedClearly() throws Exception {
    PreparedWebService prepared = prepare("nosuchfield", "");
    prepared.execute(output());

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("msg"));

    HopTransformException thrown =
        assertThrows(
            HopTransformException.class,
            () -> listener.rowWrittenEvent(rowMeta, new Object[] {"value"}));

    assertTrue(thrown.getMessage().contains("nosuchfield"));
  }

  @Test
  void theStatusCodeFieldDrivesTheOutputStatus() throws Exception {
    int[] seen = {0};
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    IEngineComponent component = mock(IEngineComponent.class);
    when(pipeline.findComponent("out", 0)).thenReturn(component);
    doAnswer(
            invocation -> {
              listener = invocation.getArgument(0);
              return null;
            })
        .when(component)
        .addRowListener(any());

    PreparedWebService prepared =
        new PreparedWebService(pipeline, "obj-1", "text/plain", "UTF-8", "out", "msg", "code");
    prepared.execute(
        new IWebServiceOutput() {
          @Override
          public void setContentType(String contentType, String encoding) {
            // not under test here
          }

          @Override
          public void setStatus(int statusCode) {
            seen[0] = statusCode;
          }

          @Override
          public OutputStream getOutputStream() {
            return sink;
          }
        });

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("msg"));
    rowMeta.addValueMeta(new ValueMetaInteger("code"));
    listener.rowWrittenEvent(rowMeta, new Object[] {"not found", 404L});

    assertEquals(404, seen[0]);
  }

  @Test
  void withoutAStatusCodeFieldTheStatusStaysAt200() throws Exception {
    int[] seen = {0};
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    IEngineComponent component = mock(IEngineComponent.class);
    when(pipeline.findComponent("out", 0)).thenReturn(component);
    doAnswer(
            invocation -> {
              listener = invocation.getArgument(0);
              return null;
            })
        .when(component)
        .addRowListener(any());

    new PreparedWebService(pipeline, "obj-1", "text/plain", "UTF-8", "out", "msg", "")
        .execute(
            new IWebServiceOutput() {
              @Override
              public void setContentType(String contentType, String encoding) {
                // not under test here
              }

              @Override
              public void setStatus(int statusCode) {
                seen[0] = statusCode;
              }

              @Override
              public OutputStream getOutputStream() {
                return sink;
              }
            });

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("msg"));
    listener.rowWrittenEvent(rowMeta, new Object[] {"ok"});

    assertEquals(200, seen[0]);
  }

  @Test
  void theContentTypeIsAnnouncedBeforeTheStreamIsOpened() throws Exception {
    String[] announced = new String[2];
    PreparedWebService prepared = prepare("msg", "");

    prepared.execute(
        new IWebServiceOutput() {
          @Override
          public void setContentType(String contentType, String encoding) {
            announced[0] = contentType;
            announced[1] = encoding;
          }

          @Override
          public void setStatus(int statusCode) {
            // not under test here
          }

          @Override
          public OutputStream getOutputStream() {
            assertEquals("application/json", announced[0], "content type must be set first");
            return sink;
          }
        });

    assertEquals("application/json", announced[0]);
    assertEquals("UTF-8", announced[1]);
  }

  @Test
  void theEngineIsStartedAndWaitedFor() throws Exception {
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    IEngineComponent component = mock(IEngineComponent.class);
    when(pipeline.findComponent("out", 0)).thenReturn(component);

    new PreparedWebService(pipeline, "obj-1", "text/plain", "UTF-8", "out", "msg", "")
        .execute(output());

    verify(pipeline).startThreads();
    verify(pipeline).waitUntilFinished();
  }

  @Test
  void theResolvedDetailsAreExposedToTheTransport() throws Exception {
    PreparedWebService prepared = prepare("msg", "");

    assertEquals("application/json", prepared.getContentType());
    assertEquals("UTF-8", prepared.getEncoding());
    assertEquals("obj-1", prepared.getServerObjectId());
  }
}
