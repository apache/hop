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

package org.apache.hop.pipeline.transforms.processfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
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

class ProcessFilesTest {

  private static final String SOURCE_FIELD = "source";
  private static final String TARGET_FIELD = "target";

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<ProcessFilesMeta, ProcessFilesData> helper;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "ProcessFilesTest", ProcessFilesMeta.class, ProcessFilesData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.logChannelFactory.create(any())).thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  private ProcessFiles createTransform(ProcessFilesMeta meta, ProcessFilesData data)
      throws Exception {
    Constructor<ProcessFiles> kons =
        ProcessFiles.class.getConstructor(
            TransformMeta.class,
            ProcessFilesMeta.class,
            ProcessFilesData.class,
            int.class,
            PipelineMeta.class,
            Pipeline.class);
    return kons.newInstance(
        helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
  }

  private RowMeta rowMeta() {
    RowMeta rm = new RowMeta();
    rm.addValueMeta(new ValueMetaString(SOURCE_FIELD));
    rm.addValueMeta(new ValueMetaString(TARGET_FIELD));
    return rm;
  }

  private static String vfsPath(Path path) {
    return path.toUri().toString();
  }

  private void wireInput(ProcessFiles transform, RowMeta rm, Object[]... rows) {
    BlockingRowSet input = new BlockingRowSet(Math.max(16, rows.length + 1));
    for (Object[] row : rows) {
      input.putRow(rm, row);
    }
    input.setDone();
    transform.setInputRowSets(new ArrayList<>(Collections.singletonList(input)));
  }

  private ProcessFilesMeta baseMeta(int operationType) {
    ProcessFilesMeta meta = new ProcessFilesMeta();
    meta.setDefault();
    meta.setSimulate(false);
    meta.setOperationType(operationType);
    meta.setSourceFilenameField(SOURCE_FIELD);
    meta.setTargetFilenameField(TARGET_FIELD);
    meta.setCreateParentFolder(false);
    meta.setOverwriteTargetFile(false);
    meta.setAddResultFilenames(false);
    return meta;
  }

  @Test
  void copy_writesTargetAndPassesRowThrough(@TempDir Path tempDir) throws Exception {
    Path source = tempDir.resolve("in.txt");
    Path target = tempDir.resolve("out.txt");
    Files.writeString(source, "hello-processfiles", StandardCharsets.UTF_8);

    ProcessFilesMeta meta = baseMeta(ProcessFilesMeta.OPERATION_TYPE_COPY);
    ProcessFiles transform = createTransform(meta, new ProcessFilesData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {vfsPath(source), vfsPath(target)});

    List<Object[]> out = PipelineTestingUtil.execute(transform, 1, false);
    PipelineTestingUtil.assertResult(
        new Object[] {vfsPath(source), vfsPath(target)}, out.getFirst());

    assertTrue(Files.exists(target));
    assertEquals("hello-processfiles", Files.readString(target, StandardCharsets.UTF_8));
    assertTrue(Files.exists(source));
  }

  @Test
  void copy_createsParentFolderWhenConfigured(@TempDir Path tempDir) throws Exception {
    Path source = tempDir.resolve("in.txt");
    Path target = tempDir.resolve("nested/dir/out.txt");
    Files.writeString(source, "x", StandardCharsets.UTF_8);

    ProcessFilesMeta meta = baseMeta(ProcessFilesMeta.OPERATION_TYPE_COPY);
    meta.setCreateParentFolder(true);
    ProcessFiles transform = createTransform(meta, new ProcessFilesData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {vfsPath(source), vfsPath(target)});

    PipelineTestingUtil.execute(transform, 1, false);

    assertTrue(Files.isRegularFile(target));
    assertEquals("x", Files.readString(target, StandardCharsets.UTF_8));
  }

  @Test
  void copy_simulate_doesNotCreateTarget(@TempDir Path tempDir) throws Exception {
    Path source = tempDir.resolve("in.txt");
    Path target = tempDir.resolve("out.txt");
    Files.writeString(source, "only-source", StandardCharsets.UTF_8);

    ProcessFilesMeta meta = baseMeta(ProcessFilesMeta.OPERATION_TYPE_COPY);
    meta.setSimulate(true);
    ProcessFiles transform = createTransform(meta, new ProcessFilesData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {vfsPath(source), vfsPath(target)});

    PipelineTestingUtil.execute(transform, 1, false);

    assertFalse(Files.exists(target));
  }

  @Test
  void copy_skipsWhenTargetExistsAndOverwriteDisabled(@TempDir Path tempDir) throws Exception {
    Path source = tempDir.resolve("in.txt");
    Path target = tempDir.resolve("out.txt");
    Files.writeString(source, "new-content", StandardCharsets.UTF_8);
    Files.writeString(target, "old-content", StandardCharsets.UTF_8);

    ProcessFilesMeta meta = baseMeta(ProcessFilesMeta.OPERATION_TYPE_COPY);
    meta.setOverwriteTargetFile(false);
    ProcessFiles transform = createTransform(meta, new ProcessFilesData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {vfsPath(source), vfsPath(target)});

    PipelineTestingUtil.execute(transform, 1, false);

    assertEquals("old-content", Files.readString(target, StandardCharsets.UTF_8));
  }

  @Test
  void copy_overwritesWhenEnabled(@TempDir Path tempDir) throws Exception {
    Path source = tempDir.resolve("in.txt");
    Path target = tempDir.resolve("out.txt");
    Files.writeString(source, "replacement", StandardCharsets.UTF_8);
    Files.writeString(target, "stale", StandardCharsets.UTF_8);

    ProcessFilesMeta meta = baseMeta(ProcessFilesMeta.OPERATION_TYPE_COPY);
    meta.setOverwriteTargetFile(true);
    ProcessFiles transform = createTransform(meta, new ProcessFilesData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {vfsPath(source), vfsPath(target)});

    PipelineTestingUtil.execute(transform, 1, false);

    assertEquals("replacement", Files.readString(target, StandardCharsets.UTF_8));
  }

  @Test
  void move_removesSourceAndWritesTarget(@TempDir Path tempDir) throws Exception {
    Path source = tempDir.resolve("move-src.txt");
    Path target = tempDir.resolve("move-dst.txt");
    Files.writeString(source, "moved-bytes", StandardCharsets.UTF_8);

    ProcessFilesMeta meta = baseMeta(ProcessFilesMeta.OPERATION_TYPE_MOVE);
    ProcessFiles transform = createTransform(meta, new ProcessFilesData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {vfsPath(source), vfsPath(target)});

    PipelineTestingUtil.execute(transform, 1, false);

    assertFalse(Files.exists(source));
    assertEquals("moved-bytes", Files.readString(target, StandardCharsets.UTF_8));
  }

  @Test
  void delete_removesSource(@TempDir Path tempDir) throws Exception {
    Path source = tempDir.resolve("del.txt");
    Files.writeString(source, "bye", StandardCharsets.UTF_8);
    Path dummyTarget = tempDir.resolve("ignored.txt");

    ProcessFilesMeta meta = baseMeta(ProcessFilesMeta.OPERATION_TYPE_DELETE);
    ProcessFiles transform = createTransform(meta, new ProcessFilesData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {vfsPath(source), vfsPath(dummyTarget)});

    PipelineTestingUtil.execute(transform, 1, false);

    assertFalse(Files.exists(source));
  }

  @Test
  void delete_simulate_leavesSource(@TempDir Path tempDir) throws Exception {
    Path source = tempDir.resolve("keep.txt");
    Files.writeString(source, "still-here", StandardCharsets.UTF_8);

    ProcessFilesMeta meta = baseMeta(ProcessFilesMeta.OPERATION_TYPE_DELETE);
    meta.setSimulate(true);
    ProcessFiles transform = createTransform(meta, new ProcessFilesData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {vfsPath(source), vfsPath(tempDir.resolve("dummy.txt"))});

    PipelineTestingUtil.execute(transform, 1, false);

    assertTrue(Files.exists(source));
  }

  @Test
  void addResultFilenames_registersTargetAfterCopy(@TempDir Path tempDir) throws Exception {
    Path source = tempDir.resolve("in.txt");
    Path target = tempDir.resolve("result.txt");
    Files.writeString(source, "r", StandardCharsets.UTF_8);

    ProcessFilesMeta meta = baseMeta(ProcessFilesMeta.OPERATION_TYPE_COPY);
    meta.setAddResultFilenames(true);
    ProcessFiles transform = createTransform(meta, new ProcessFilesData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {vfsPath(source), vfsPath(target)});

    PipelineTestingUtil.execute(transform, 1, false);

    assertEquals(1, transform.getResultFiles().size());
    assertTrue(transform.getResultFiles().containsKey(target.toUri().toString()));
  }

  @Test
  void copy_withDataVolumeVariable_tracksVolume(@TempDir Path tempDir) throws Exception {
    Path source = tempDir.resolve("in.txt");
    Path target = tempDir.resolve("out.txt");
    String payload = "metric-payload";
    Files.writeString(source, payload, StandardCharsets.UTF_8);

    helper.pipeline.setVariable(Const.HOP_METRIC_DATA_VOLUME, "Y");

    ProcessFilesMeta meta = baseMeta(ProcessFilesMeta.OPERATION_TYPE_COPY);
    ProcessFiles transform = createTransform(meta, new ProcessFilesData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {vfsPath(source), vfsPath(target)});

    PipelineTestingUtil.execute(transform, 1, false);

    assertEquals(payload.length(), (int) (long) transform.getDataVolumeIn());
    assertEquals(payload.length(), (int) (long) transform.getDataVolumeOut());
  }

  @Test
  void throwsWhenSourceFilenameFieldNotSet(@TempDir Path tempDir) throws Exception {
    Path source = tempDir.resolve("in.txt");
    Path target = tempDir.resolve("out.txt");
    Files.writeString(source, "x", StandardCharsets.UTF_8);

    ProcessFilesMeta meta = baseMeta(ProcessFilesMeta.OPERATION_TYPE_COPY);
    meta.setSourceFilenameField(null);
    ProcessFiles transform = createTransform(meta, new ProcessFilesData());
    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {vfsPath(source), vfsPath(target)});

    assertThrows(HopException.class, () -> PipelineTestingUtil.execute(transform, 1, false));
  }

  @Test
  void missingParentWithoutCreateFolder_sendsRowToErrorWhenConfigured(@TempDir Path tempDir)
      throws Exception {
    Path source = tempDir.resolve("in.txt");
    Path target = tempDir.resolve("no_such_parent/out.txt");
    Files.writeString(source, "x", StandardCharsets.UTF_8);

    when(helper.transformMeta.isDoingErrorHandling()).thenReturn(true);

    ProcessFilesMeta meta = baseMeta(ProcessFilesMeta.OPERATION_TYPE_COPY);
    meta.setCreateParentFolder(false);
    ProcessFiles transform = spy(createTransform(meta, new ProcessFilesData()));
    doNothing()
        .when(transform)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            anyLong(),
            anyString(),
            nullable(String.class),
            anyString());

    RowMeta rm = rowMeta();
    wireInput(transform, rm, new Object[] {vfsPath(source), vfsPath(target)});

    assertTrue(transform.processRow());
    verify(transform)
        .putError(
            any(IRowMeta.class),
            any(Object[].class),
            anyLong(),
            anyString(),
            nullable(String.class),
            anyString());
    assertFalse(Files.exists(target));
  }

  @Test
  void processesMultipleRows(@TempDir Path tempDir) throws Exception {
    Path s1 = tempDir.resolve("a.txt");
    Path t1 = tempDir.resolve("a.out");
    Path s2 = tempDir.resolve("b.txt");
    Path t2 = tempDir.resolve("b.out");
    Files.writeString(s1, "1", StandardCharsets.UTF_8);
    Files.writeString(s2, "2", StandardCharsets.UTF_8);

    ProcessFilesMeta meta = baseMeta(ProcessFilesMeta.OPERATION_TYPE_COPY);
    ProcessFiles transform = createTransform(meta, new ProcessFilesData());
    RowMeta rm = rowMeta();
    wireInput(
        transform,
        rm,
        new Object[] {vfsPath(s1), vfsPath(t1)},
        new Object[] {vfsPath(s2), vfsPath(t2)});

    List<Object[]> out = PipelineTestingUtil.execute(transform, 2, false);
    PipelineTestingUtil.assertResult(new Object[] {vfsPath(s1), vfsPath(t1)}, out.get(0));
    PipelineTestingUtil.assertResult(new Object[] {vfsPath(s2), vfsPath(t2)}, out.get(1));
    assertEquals("1", Files.readString(t1, StandardCharsets.UTF_8));
    assertEquals("2", Files.readString(t2, StandardCharsets.UTF_8));
  }
}
