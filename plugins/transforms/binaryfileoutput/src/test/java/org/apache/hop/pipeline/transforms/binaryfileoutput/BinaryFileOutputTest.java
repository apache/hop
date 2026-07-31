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

package org.apache.hop.pipeline.transforms.binaryfileoutput;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineTestingUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class BinaryFileOutputTest {

  private static final String BINARY_FIELD = "content";
  private static final String FILENAME_FIELD = "filename";

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<BinaryFileOutputMeta, BinaryFileOutputData> helper;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "BinaryFileOutputTest", BinaryFileOutputMeta.class, BinaryFileOutputData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.logChannelFactory.create(any())).thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  private BinaryFileOutput createTransform(BinaryFileOutputMeta meta, BinaryFileOutputData data)
      throws Exception {
    Constructor<BinaryFileOutput> kons =
        BinaryFileOutput.class.getConstructor(
            TransformMeta.class,
            BinaryFileOutputMeta.class,
            BinaryFileOutputData.class,
            int.class,
            PipelineMeta.class,
            Pipeline.class);
    return kons.newInstance(
        helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
  }

  private RowMeta rowMeta() {
    RowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaBinary(BINARY_FIELD));
    rm.addValueMeta(new ValueMetaString(FILENAME_FIELD));
    return rm;
  }

  private static String vfsPath(Path path) {
    return path.toUri().toString();
  }

  private void wireInput(BinaryFileOutput transform, RowMeta rm, Object[]... rows) {
    BlockingRowSet input = new BlockingRowSet(Math.max(16, rows.length + 1));
    for (Object[] row : rows) {
      input.putRow(rm, row);
    }
    input.setDone();
    transform.setInputRowSets(new ArrayList<>(Collections.singletonList(input)));
  }

  private BinaryFileOutputMeta baseMeta() {
    BinaryFileOutputMeta meta = new BinaryFileOutputMeta();
    meta.setDefault();
    meta.setBinaryField(BINARY_FIELD);
    meta.setFilenameField(FILENAME_FIELD);
    meta.setCreateParentFolder(false);
    meta.setOverwriteFile(true);
    meta.setAddResultFilenames(false);
    return meta;
  }

  @Test
  void writesRawBytesUnchanged(@TempDir Path tempDir) throws Exception {
    // Include non-UTF8 / control bytes that text writers often corrupt
    byte[] payload = new byte[] {0x00, 0x01, (byte) 0x98, (byte) 0xFF, 0x0A, 0x0D, 0x1A};
    Path target = tempDir.resolve("out.bin");

    BinaryFileOutputMeta meta = baseMeta();
    BinaryFileOutput transform = createTransform(meta, new BinaryFileOutputData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {payload, vfsPath(target)});

    List<Object[]> out = PipelineTestingUtil.execute(transform, 1, false);
    assertArrayEquals(payload, (byte[]) out.getFirst()[0]);
    assertEquals(vfsPath(target), out.getFirst()[1]);

    assertTrue(Files.isRegularFile(target));
    assertArrayEquals(payload, Files.readAllBytes(target));
  }

  @Test
  void writesEmptyBinaryFile(@TempDir Path tempDir) throws Exception {
    byte[] payload = new byte[0];
    Path target = tempDir.resolve("empty.bin");

    BinaryFileOutputMeta meta = baseMeta();
    BinaryFileOutput transform = createTransform(meta, new BinaryFileOutputData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {payload, vfsPath(target)});

    PipelineTestingUtil.execute(transform, 1, false);

    assertTrue(Files.isRegularFile(target));
    assertEquals(0, Files.size(target));
  }

  @Test
  void createsParentFolderWhenConfigured(@TempDir Path tempDir) throws Exception {
    byte[] payload = new byte[] {0x01, 0x02, 0x03};
    Path target = tempDir.resolve("nested/dir/out.bin");

    BinaryFileOutputMeta meta = baseMeta();
    meta.setCreateParentFolder(true);
    BinaryFileOutput transform = createTransform(meta, new BinaryFileOutputData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {payload, vfsPath(target)});

    PipelineTestingUtil.execute(transform, 1, false);

    assertTrue(Files.isRegularFile(target));
    assertArrayEquals(payload, Files.readAllBytes(target));
  }

  @Test
  void failsWhenParentMissingAndCreateDisabled(@TempDir Path tempDir) throws Exception {
    byte[] payload = new byte[] {0x01};
    Path target = tempDir.resolve("missing/parent/out.bin");

    BinaryFileOutputMeta meta = baseMeta();
    meta.setCreateParentFolder(false);
    BinaryFileOutput transform = createTransform(meta, new BinaryFileOutputData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {payload, vfsPath(target)});

    assertFalse(transform.processRow());
    assertTrue(transform.getErrors() > 0);
    assertFalse(Files.exists(target));
  }

  @Test
  void overwritesWhenEnabled(@TempDir Path tempDir) throws Exception {
    Path target = tempDir.resolve("out.bin");
    Files.write(target, new byte[] {0x11, 0x22});
    byte[] payload = new byte[] {(byte) 0xAA, (byte) 0xBB, (byte) 0xCC};

    BinaryFileOutputMeta meta = baseMeta();
    meta.setOverwriteFile(true);
    BinaryFileOutput transform = createTransform(meta, new BinaryFileOutputData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {payload, vfsPath(target)});

    PipelineTestingUtil.execute(transform, 1, false);

    assertArrayEquals(payload, Files.readAllBytes(target));
  }

  @Test
  void failsWhenExistsAndOverwriteDisabled(@TempDir Path tempDir) throws Exception {
    Path target = tempDir.resolve("out.bin");
    byte[] original = new byte[] {0x11, 0x22};
    Files.write(target, original);
    byte[] payload = new byte[] {(byte) 0xAA};

    BinaryFileOutputMeta meta = baseMeta();
    meta.setOverwriteFile(false);
    BinaryFileOutput transform = createTransform(meta, new BinaryFileOutputData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {payload, vfsPath(target)});

    assertFalse(transform.processRow());
    assertTrue(transform.getErrors() > 0);
    assertArrayEquals(original, Files.readAllBytes(target));
  }

  @Test
  void failsOnNullBinary(@TempDir Path tempDir) throws Exception {
    Path target = tempDir.resolve("out.bin");

    BinaryFileOutputMeta meta = baseMeta();
    BinaryFileOutput transform = createTransform(meta, new BinaryFileOutputData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {null, vfsPath(target)});

    assertFalse(transform.processRow());
    assertTrue(transform.getErrors() > 0);
    assertFalse(Files.exists(target));
  }

  @Test
  void failsOnEmptyFilename(@TempDir Path tempDir) throws Exception {
    BinaryFileOutputMeta meta = baseMeta();
    BinaryFileOutput transform = createTransform(meta, new BinaryFileOutputData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {new byte[] {0x01}, ""});

    assertFalse(transform.processRow());
    assertTrue(transform.getErrors() > 0);
  }

  @Test
  void throwsWhenBinaryFieldNotConfigured(@TempDir Path tempDir) throws Exception {
    Path target = tempDir.resolve("out.bin");
    BinaryFileOutputMeta meta = baseMeta();
    meta.setBinaryField(null);
    BinaryFileOutput transform = createTransform(meta, new BinaryFileOutputData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {new byte[] {0x01}, vfsPath(target)});

    assertThrows(HopException.class, () -> PipelineTestingUtil.execute(transform, 1, false));
  }

  @Test
  void writesEachRowToItsOwnFile(@TempDir Path tempDir) throws Exception {
    Path t1 = tempDir.resolve("a.bin");
    Path t2 = tempDir.resolve("b.bin");
    byte[] p1 = new byte[] {0x01, 0x02};
    byte[] p2 = new byte[] {(byte) 0xFE, (byte) 0xFD, (byte) 0xFC};

    BinaryFileOutputMeta meta = baseMeta();
    BinaryFileOutput transform = createTransform(meta, new BinaryFileOutputData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {p1, vfsPath(t1)}, new Object[] {p2, vfsPath(t2)});

    List<Object[]> out = PipelineTestingUtil.execute(transform, 2, false);
    assertEquals(2, out.size());
    assertArrayEquals(p1, Files.readAllBytes(t1));
    assertArrayEquals(p2, Files.readAllBytes(t2));
  }

  @Test
  void addResultFilenames_registersTarget(@TempDir Path tempDir) throws Exception {
    Path target = tempDir.resolve("result.bin");
    byte[] payload = new byte[] {0x0A};

    BinaryFileOutputMeta meta = baseMeta();
    meta.setAddResultFilenames(true);
    BinaryFileOutput transform = createTransform(meta, new BinaryFileOutputData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {payload, vfsPath(target)});

    PipelineTestingUtil.execute(transform, 1, false);

    assertEquals(1, transform.getResultFiles().size());
    assertTrue(transform.getResultFiles().containsKey(target.toUri().toString()));
  }
}
