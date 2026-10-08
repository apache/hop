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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import org.apache.hop.core.Const;
import org.apache.hop.core.compress.CompressionOutputStream;
import org.apache.hop.core.compress.CompressionPluginType;
import org.apache.hop.core.compress.gzip.GzipCompressionProvider;
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

    transform.setVariable(Const.HOP_FILE_OUTPUT_MAX_STREAM_LIFE, "-1");
    assertEquals(-1, transform.getFlushInterval());
  }

  @Test
  void negativeIntervalDoesNotFlushOnTheClock() throws Exception {
    FlushProbe transform = newProbe();
    transform.setVariable(Const.HOP_FILE_OUTPUT_MAX_STREAM_LIFE, "-1");
    assertTrue(transform.init());

    transform.now = 1_000_000L;
    transform.row = new Object[] {"a"};
    assertTrue(transform.processRow());

    transform.now = 1_060_000L;
    transform.row = new Object[] {"b"};
    assertTrue(transform.processRow());
    assertEquals("", transform.written(), "a negative interval does not flush on the clock");
  }

  @Test
  void intervalFlushAfterLastRowStillClosesGzipFile() throws Exception {
    FlushProbe transform = newProbe("GZip");
    transform.setVariable(Const.HOP_FILE_OUTPUT_MAX_STREAM_LIFE, "1000");
    assertTrue(transform.init());

    transform.now = 1_000_000L;
    transform.row = new Object[] {"a"};
    assertTrue(transform.processRow());

    // Last data row. The interval elapses while writing it, so the flush clears the dirty flag.
    transform.now = 1_002_000L;
    transform.row = new Object[] {"b"};
    assertTrue(transform.processRow());
    assertFalse(transform.currentStreamDirty(), "interval flush cleared the dirty flag");
    assertTrue(transform.currentStreamOpen(), "interval flush must not close the file");
    assertFalse(transform.isOutputClosed());

    transform.row = null;
    assertFalse(transform.processRow());
    assertFalse(transform.currentStreamOpen());
    assertTrue(transform.isOutputClosed());
    assertEquals("a\nb\n", gunzip(transform.gzipBytes()));
  }

  @Test
  void closeAfterFlushClosesStreamThatIntervalFlushAlreadyCleaned() throws Exception {
    TextFileOutputData data = new TextFileOutputData();
    assertCleanOpenStreamIsClosed(data.new FileStreamsList());
    assertCleanOpenStreamIsClosed(data.new FileStreamsMap());
  }

  private static void assertCleanOpenStreamIsClosed(TextFileOutputData.IFileStreamsCollection coll)
      throws Exception {
    String collection = coll.getClass().getSimpleName();
    ByteArrayOutputStream raw = new ByteArrayOutputStream();
    CompressionOutputStream compression = new GzipCompressionProvider().createOutputStream(raw);
    BufferedOutputStream buffered = new BufferedOutputStream(compression, 5000);
    TextFileOutputData.FileStream stream =
        new TextFileOutputData().new FileStream(raw, compression, buffered);
    buffered.write("hello\n".getBytes(StandardCharsets.UTF_8));
    stream.setDirty(true);
    coll.add("out.txt", stream);

    coll.flushOpenFiles(false);
    assertFalse(stream.isDirty(), collection);
    assertTrue(stream.isOpen(), collection);
    assertEquals(1, coll.getNumOpenFiles(), collection);

    coll.flushOpenFiles(true);
    assertFalse(stream.isOpen(), collection);
    assertEquals(0, coll.getNumOpenFiles(), collection);
    assertEquals("hello\n", gunzip(raw.toByteArray()), collection);
  }

  private static String gunzip(byte[] gzipBytes) throws IOException {
    try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(gzipBytes))) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
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
    return newProbe("None");
  }

  private FlushProbe newProbe(String compression) {
    TextFileOutputMeta meta = new TextFileOutputMeta();
    meta.setDefault();
    meta.setFileCompression(compression);
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
    private boolean outputClosed;
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

    private byte[] gzipBytes() {
      return written.toByteArray();
    }

    private boolean isOutputClosed() {
      return outputClosed;
    }

    private boolean currentStreamOpen() {
      TextFileOutputData.FileStream last = data.getFileStreamsCollection().getLastStream();
      return last != null && last.isOpen();
    }

    private boolean currentStreamDirty() {
      TextFileOutputData.FileStream last = data.getFileStreamsCollection().getLastStream();
      return last != null && last.isDirty();
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

        @Override
        public void close() {
          outputClosed = true;
        }
      };
    }
  }
}
