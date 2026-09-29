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

package org.apache.hop.pipeline.transforms.textfileoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.Const;
import org.apache.hop.core.compress.CompressionPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

class TextFileOutputFlushTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<TextFileOutputMeta, TextFileOutputData> helper;

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    PluginRegistry.addPluginType(CompressionPluginType.getInstance());
    PluginRegistry.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "Text file output flush", TextFileOutputMeta.class, TextFileOutputData.class);
    Mockito.when(helper.logChannelFactory.create(Mockito.any(), Mockito.any()))
        .thenReturn(helper.iLogChannel);
    Mockito.when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void flushIntervalDefaultsToAFewSeconds() throws Exception {
    FlushProbe transform = newProbe();
    transform.setVariable(Const.HOP_FILE_OUTPUT_MAX_STREAM_LIFE, null);
    assertEquals(TextFileOutput.DEFAULT_FILE_FLUSH_INTERVAL_MS, transform.getFlushInterval());

    transform.setVariable(Const.HOP_FILE_OUTPUT_MAX_STREAM_LIFE, "0");
    assertEquals(TextFileOutput.DEFAULT_FILE_FLUSH_INTERVAL_MS, transform.getFlushInterval());

    transform.setVariable(Const.HOP_FILE_OUTPUT_MAX_STREAM_LIFE, "nope");
    assertEquals(TextFileOutput.DEFAULT_FILE_FLUSH_INTERVAL_MS, transform.getFlushInterval());

    transform.setVariable(Const.HOP_FILE_OUTPUT_MAX_STREAM_LIFE, "2500");
    assertEquals(2500, transform.getFlushInterval());
  }

  @Test
  void slowRowsAreFlushedOnTheIntervalAndAgainAfterThat() throws Exception {
    FlushProbe transform = newProbe();
    transform.setVariable(Const.HOP_FILE_OUTPUT_MAX_STREAM_LIFE, "1000");
    assertTrue(transform.init());

    transform.now = 1_000_000L;
    transform.row = new Object[] {"a"};
    assertTrue(transform.processRow());
    assertEquals("", transform.written(), "first row stays buffered until the interval elapses");

    transform.now = 1_000_500L;
    transform.row = new Object[] {"b"};
    assertTrue(transform.processRow());
    assertEquals("", transform.written(), "a row inside the interval does not flush");

    transform.now = 1_001_001L;
    transform.row = new Object[] {"c"};
    assertTrue(transform.processRow());
    assertEquals("a\nb\nc\n", transform.written());

    transform.now = 1_002_002L;
    transform.row = new Object[] {"d"};
    assertTrue(transform.processRow());
    assertEquals("a\nb\nc\nd\n", transform.written());
  }

  private FlushProbe newProbe() {
    TextFileOutputMeta meta = new TextFileOutputMeta();
    meta.setDefault();
    meta.setHeaderEnabled(false);
    meta.setFooterEnabled(false);
    meta.setSeparator("");
    meta.setEnclosure("");
    meta.setFileFormat("UNIX");
    meta.setEncoding(Const.UTF_8);
    meta.setCreateParentFolder(false);
    meta.setEndedLine(null);
    meta.getFileSettings().setFileName("out.txt");
    meta.getFileSettings().setExtension("");
    meta.getFileSettings().setDoNotOpenNewFileInit(true);
    meta.getFileSettings().setAddToResultFiles(false);
    meta.getFileSettings().setFastDump(true);

    FlushProbe transform =
        new FlushProbe(
            helper.transformMeta,
            meta,
            new TextFileOutputData(),
            helper.pipelineMeta,
            helper.pipeline);
    RowMeta rowMeta = new RowMeta();
    ValueMetaString column = new ValueMetaString("col");
    column.setLength(-1);
    rowMeta.addValueMeta(column);
    transform.setInputRowMeta(rowMeta);
    return transform;
  }

  private static final class FlushProbe extends TextFileOutput {
    private long now;
    private Object[] row;
    private final ByteArrayOutputStream written = new ByteArrayOutputStream();

    private FlushProbe(
        org.apache.hop.pipeline.transform.TransformMeta transformMeta,
        TextFileOutputMeta meta,
        TextFileOutputData data,
        org.apache.hop.pipeline.PipelineMeta pipelineMeta,
        org.apache.hop.pipeline.Pipeline pipeline) {
      super(transformMeta, meta, data, 0, pipelineMeta, pipeline);
    }

    private String written() {
      return written.toString(StandardCharsets.UTF_8);
    }

    @Override
    protected long currentFlushTimeMillis() {
      return now;
    }

    @Override
    public Object[] getRow() {
      return row;
    }

    @Override
    public void putRow(IRowMeta rowMeta, Object[] row) {
      // The flush test does not chain rows to a downstream transform.
    }

    @Override
    protected OutputStream getOutputStream(
        String vfsFilename, IVariables variables, boolean append) {
      return new OutputStream() {
        @Override
        public void write(int b) {
          written.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
          written.write(b, off, len);
        }
      };
    }
  }
}
