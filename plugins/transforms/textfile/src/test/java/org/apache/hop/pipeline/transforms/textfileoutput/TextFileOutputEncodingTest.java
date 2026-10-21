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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.hop.core.compress.CompressionPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/** Unit test for {@link TextFileOutput} encoding */
class TextFileOutputEncodingTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final String TEXT = "V001;Maria\nV002;Jose\nV003;Ana\n";
  private static final String CJK = "V001;日本\n";

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
            "Text file output encoding", TextFileOutputMeta.class, TextFileOutputData.class);
    Mockito.when(helper.logChannelFactory.create(Mockito.any(), Mockito.any()))
        .thenReturn(helper.iLogChannel);
    Mockito.when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  static Stream<String> encodings() {
    Stream<String> available =
        Charset.availableCharsets().values().stream()
            .map(Charset::name)
            .filter(name -> canEncode(name, TEXT));
    Stream<String> aliases =
        Stream.of("UnicodeBig", "unicode", "UnicodeLittle", "UTF-32BE-BOM", "UTF-32LE-BOM");
    return Stream.concat(available, aliases).distinct();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("encodings")
  void charsetRoundTripsWithoutRepeatingByteOrderMark(String encoding) throws Exception {
    byte[] bytes =
        write(
            encoding,
            stringRowMeta(),
            List.of(
                new Object[] {"V001", "Maria"},
                new Object[] {"V002", "Jose"},
                new Object[] {"V003", "Ana"}),
            options -> {});
    assertRoundTrip(encoding, bytes, TEXT);
  }

  @Test
  void cjkFieldsRoundTripForEncodingsThatCanRepresentThem() throws Exception {
    List<String> encodings = new ArrayList<>();
    for (Charset charset : Charset.availableCharsets().values()) {
      if (canEncode(charset.name(), CJK)) {
        encodings.add(charset.name());
      }
    }
    assertTrue(encodings.contains("UTF-8"));
    assertTrue(encodings.contains("UTF-16"));
    assertTrue(encodings.contains("ISO-2022-JP"));
    assertTrue(encodings.contains("ISO-2022-KR"));
    assertTrue(encodings.contains("x-ISO-2022-CN-GB"));
    assertTrue(encodings.contains("x-ISO-2022-CN-CNS"));
    List<Object[]> rows = List.<Object[]>of(new Object[] {"V001", "日本"});
    for (String encoding : encodings) {
      byte[] bytes = write(encoding, stringRowMeta(), rows, options -> {});
      assertRoundTrip(encoding, bytes, CJK);
    }
  }

  @Test
  void appendDoesNotWriteASecondByteOrderMark() throws Exception {
    ByteArrayOutputStream storage = new ByteArrayOutputStream();
    List<Object[]> firstRows =
        List.<Object[]>of(new Object[] {"V001", "Maria"}, new Object[] {"V002", "Jose"});
    write("UTF-16", stringRowMeta(), firstRows, options -> {}, storage, false);
    write(
        "UTF-16",
        stringRowMeta(),
        List.<Object[]>of(new Object[] {"V003", "Ana"}),
        options -> options.getFileSettings().setFileAppended(true),
        storage,
        true);
    assertRoundTrip("UTF-16", storage.toByteArray(), TEXT);
  }

  @Test
  void headerIsEncodedAfterTheSingleByteOrderMark() throws Exception {
    byte[] bytes =
        write(
            "x-UTF-16LE-BOM",
            stringRowMeta(),
            List.<Object[]>of(new Object[] {"V001", "Maria"}),
            options -> options.setHeaderEnabled(true));
    assertRoundTrip("x-UTF-16LE-BOM", bytes, "id;name\nV001;Maria\n");
  }

  @Test
  void dosLineEndingIsEncodedOnce() throws Exception {
    byte[] bytes =
        write(
            "UTF-16",
            stringRowMeta(),
            List.<Object[]>of(new Object[] {"V001", "Maria"}),
            options -> options.setFileFormat("DOS"));
    assertRoundTrip("UTF-16", bytes, "V001;Maria\r\n");
  }

  @Test
  void integerFieldDoesNotRepeatByteOrderMark() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    byte[] bytes =
        write(
            "X-UTF-32LE-BOM",
            rowMeta,
            List.<Object[]>of(new Object[] {1L, "Maria"}, new Object[] {2L, "Jose"}),
            options -> {});
    assertRoundTrip("X-UTF-32LE-BOM", bytes, "1;Maria\n2;Jose\n");
  }

  @Test
  void characterUfeffIsDataAndNotASecondByteOrderMark() throws Exception {
    byte[] bytes =
        write(
            "UTF-16",
            stringRowMeta(),
            List.<Object[]>of(new Object[] {"\uFEFFV001", "Maria"}),
            options -> {});
    assertEquals("\uFEFFV001;Maria\n", new String(bytes, StandardCharsets.UTF_16));
    assertEquals(2, count(bytes, new byte[] {(byte) 0xFE, (byte) 0xFF}));
  }

  @Test
  void rightPadDoesNotRepeatByteOrderMarkInTheFiller() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("id"));
    byte[] bytes =
        write(
            "UTF-16",
            rowMeta,
            List.<Object[]>of(new Object[] {"A"}),
            options -> {
              options.setSeparator("");
              options.getOutputFields().getFirst().setLength(4);
            });
    assertRoundTrip("UTF-16", bytes, "A   \n");
  }

  private byte[] write(
      String encoding,
      IRowMeta rowMeta,
      List<Object[]> rows,
      Consumer<TextFileOutputMeta> customize)
      throws Exception {
    ByteArrayOutputStream storage = new ByteArrayOutputStream();
    return write(encoding, rowMeta, rows, customize, storage, false);
  }

  private byte[] write(
      String encoding,
      IRowMeta rowMeta,
      List<Object[]> rows,
      Consumer<TextFileOutputMeta> customize,
      ByteArrayOutputStream storage,
      boolean append)
      throws Exception {
    TextFileOutputMeta meta = new TextFileOutputMeta();
    meta.setDefault();
    meta.setFileCompression("None");
    meta.setHeaderEnabled(false);
    meta.setFooterEnabled(false);
    meta.setSeparator(";");
    meta.setEnclosure("");
    meta.setFileFormat("UNIX");
    meta.setEncoding(encoding);
    meta.setCreateParentFolder(false);
    meta.setEndedLine(null);
    meta.getFileSettings().setFileName("out.txt");
    meta.getFileSettings().setExtension("");
    meta.getFileSettings().setDoNotOpenNewFileInit(false);
    meta.getFileSettings().setAddToResultFiles(false);
    meta.getFileSettings().setFastDump(false);
    meta.getFileSettings().setFileAppended(append);
    meta.getOutputFields().clear();
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      TextFileField field = new TextFileField();
      field.setName(valueMeta.getName());
      field.setType(valueMeta.getType());
      field.setLength(-1);
      field.setPrecision(-1);
      meta.getOutputFields().add(field);
    }
    customize.accept(meta);

    TextFileOutputData data = new TextFileOutputData();
    // The bytes are captured in memory. Skip the VFS lineage lookup that runs when a file closes.
    data.setBeamContext(true);
    EncodingProbe probe =
        new EncodingProbe(
            helper.transformMeta, meta, data, helper.pipelineMeta, helper.pipeline, storage);
    probe.setInputRowMeta(rowMeta);
    assertTrue(probe.init(), encoding);
    for (Object[] row : rows) {
      probe.row = row;
      assertTrue(probe.processRow(), encoding);
    }
    probe.row = null;
    assertFalse(probe.processRow(), encoding);
    assertEquals(0, probe.getErrors(), encoding);
    probe.dispose();
    return storage.toByteArray();
  }

  private static void assertRoundTrip(String encoding, byte[] bytes, String expected) {
    byte[] mark = TextFileCharset.byteOrderMark(encoding);
    if (mark.length > 0 && !expected.contains("\uFEFF")) {
      assertTrue(bytes.length >= mark.length, encoding);
      assertArrayEquals(mark, Arrays.copyOf(bytes, mark.length), encoding);
      assertEquals(1, count(bytes, mark), encoding);
    }
    String decoded = new String(bytes, Charset.forName(encoding));
    if (mark.length > 0 && decoded.startsWith("\uFEFF")) {
      decoded = decoded.substring(1);
    }
    assertEquals(expected, decoded, encoding);
  }

  private static boolean canEncode(String encoding, String text) {
    try {
      return Charset.forName(encoding).newEncoder().canEncode(text);
    } catch (RuntimeException e) {
      return false;
    }
  }

  private static int count(byte[] data, byte[] pattern) {
    int found = 0;
    for (int i = 0; i + pattern.length <= data.length; i++) {
      boolean match = true;
      for (int j = 0; j < pattern.length; j++) {
        if (data[i + j] != pattern[j]) {
          match = false;
          break;
        }
      }
      if (match) {
        found++;
        i += pattern.length - 1;
      }
    }
    return found;
  }

  private static RowMeta stringRowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  private static final class EncodingProbe extends TextFileOutput {
    private Object[] row;
    private final ByteArrayOutputStream written;

    private EncodingProbe(
        org.apache.hop.pipeline.transform.TransformMeta transformMeta,
        TextFileOutputMeta meta,
        TextFileOutputData data,
        org.apache.hop.pipeline.PipelineMeta pipelineMeta,
        org.apache.hop.pipeline.Pipeline pipeline,
        ByteArrayOutputStream written) {
      super(transformMeta, meta, data, 0, pipelineMeta, pipeline);
      this.written = written;
    }

    @Override
    public Object[] getRow() {
      return row;
    }

    @Override
    public void putRow(IRowMeta rowMeta, Object[] row) {
      // Rows are not chained to a downstream transform.
    }

    @Override
    public boolean isFileExists(String filename) {
      return written.size() > 0;
    }

    @Override
    public boolean isFileEmpty(String filename) {
      return written.size() == 0;
    }

    @Override
    protected OutputStream getOutputStream(
        String vfsFilename, IVariables variables, boolean append) {
      if (!append) {
        written.reset();
      }
      return new OutputStream() {
        @Override
        public void write(int b) {
          written.write(b);
        }

        @Override
        public void write(byte @NotNull [] b, int off, int len) {
          written.write(b, off, len);
        }
      };
    }
  }
}
