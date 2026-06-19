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

package org.apache.hop.pipeline.transforms.execprocess;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.RegisterExtension;

class ExecProcessTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  TransformMockHelper<ExecProcessMeta, ExecProcessData> smh;

  @BeforeEach
  void setUp() {
    smh =
        new TransformMockHelper<>(
            "Execute a process", ExecProcessMeta.class, ExecProcessData.class);
    when(smh.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(smh.iLogChannel);
    when(smh.pipeline.isRunning()).thenReturn(true);
  }

  @AfterEach
  void cleanUp() {
    smh.cleanUp();
  }

  /**
   * Executable + two args for {@link ExecProcessMeta#setArgumentsInFields(boolean)} mode ({@code
   * /bin/sh}, {@code -c}, script). Uses POSIX {@code /bin/sh} so the same commands run on Linux and
   * macOS; subprocess tests use {@link EnabledOnOs} for {@link OS#LINUX} and {@link OS#MAC} only
   * (not Windows).
   */
  private static String[] echoStdoutAndStderrCommand() {
    return new String[] {"/bin/sh", "-c", "echo hop-out; echo hop-err 1>&2"};
  }

  /** Writes enough lines to stdout to exceed a typical OS pipe buffer (regression for deadlock). */
  private static String[] largeStdoutCommand() {
    return new String[] {
      "/bin/sh",
      "-c",
      "i=0; while [ $i -lt 2500 ]; do echo AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA; i=$((i+1)); done",
    };
  }

  private static String[] failingCommand() {
    return new String[] {"/bin/sh", "-c", "exit 7"};
  }

  private static String singleTokenEchoCommand() {
    return "/bin/echo hop-single";
  }

  private RowMeta inputRowMetaForArguments(String processField, String... argFieldNames) {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString(processField));
    for (String name : argFieldNames) {
      rowMeta.addValueMeta(new ValueMetaString(name));
    }
    return rowMeta;
  }

  private ExecProcessMeta metaForArgumentsMode(String processField, String... argFieldNames) {
    ExecProcessMeta meta = new ExecProcessMeta();
    meta.setDefault();
    meta.setProcessField(processField);
    meta.setResultFieldName("result_out");
    meta.setErrorFieldName("result_err");
    meta.setExitValueFieldName("result_exit");
    meta.setFailWhenNotSuccess(false);
    meta.setArgumentsInFields(true);
    for (String name : argFieldNames) {
      ExecProcessMeta.EPField f = new ExecProcessMeta.EPField();
      f.setName(name);
      meta.getArgumentFields().add(f);
    }
    return meta;
  }

  private IRowSet inputRows(IRowMeta rowMeta, Object[]... rows) {
    IRowSet rowSet = smh.getMockInputRowSet(rows);
    when(rowSet.getRowMeta()).thenReturn(rowMeta);
    return rowSet;
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void processRow_argumentArray_capturesStdoutStderrAndExitCode() throws HopException {
    HopEnvironment.init();

    String[] cmd = echoStdoutAndStderrCommand();
    ExecProcessMeta meta = metaForArgumentsMode("cmd", "a1", "a2");
    Object[] row = new Object[] {cmd[0], cmd[1], cmd[2]};
    RowMeta rowMeta = inputRowMetaForArguments("cmd", "a1", "a2");

    ExecProcess transform = newTransform(meta, rowMeta, row);

    assertTrue(transform.processRow());
    assertFalse(transform.processRow());

    Object[] out = readSingleOutputRow(transform);
    int base = rowMeta.size();
    assertEquals("hop-out", out[base].toString().trim());
    assertEquals("hop-err", out[base + 1].toString().trim());
    assertEquals(0L, out[base + 2]);
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void processRow_largeStdout_completesWithoutDeadlock() throws HopException {
    HopEnvironment.init();

    String[] cmd = largeStdoutCommand();
    ExecProcessMeta meta = metaForArgumentsMode("cmd", "a1", "a2");
    Object[] row = new Object[] {cmd[0], cmd[1], cmd[2]};
    RowMeta rowMeta = inputRowMetaForArguments("cmd", "a1", "a2");

    ExecProcess transform = newTransform(meta, rowMeta, row);

    assertTrue(transform.processRow());
    assertFalse(transform.processRow());

    Object[] out = readSingleOutputRow(transform);
    int base = rowMeta.size();
    String stdout = (String) out[base];
    assertNotNull(stdout);
    assertTrue(stdout.length() > 50_000, "expected large stdout");
    assertEquals(0L, out[base + 2]);
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void processRow_singleCommandString_capturesOutput() throws HopException {
    HopEnvironment.init();

    ExecProcessMeta meta = new ExecProcessMeta();
    meta.setDefault();
    meta.setProcessField("cmdline");
    meta.setResultFieldName("result_out");
    meta.setErrorFieldName("result_err");
    meta.setExitValueFieldName("result_exit");
    meta.setArgumentsInFields(false);

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("cmdline"));

    Object[] row = new Object[] {singleTokenEchoCommand()};
    ExecProcess transform = newTransform(meta, rowMeta, row);

    assertTrue(transform.processRow());
    assertFalse(transform.processRow());

    Object[] out = readSingleOutputRow(transform);
    assertEquals("hop-single", out[1].toString().trim());
    assertEquals(0L, out[3]);
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void processRow_failWhenNotSuccess_setsErrors() throws HopException {
    HopEnvironment.init();

    String[] cmd = failingCommand();
    ExecProcessMeta meta = metaForArgumentsMode("cmd", "a1", "a2");
    Object[] row = new Object[] {cmd[0], cmd[1], cmd[2]};
    RowMeta rowMeta = inputRowMetaForArguments("cmd", "a1", "a2");
    meta.setFailWhenNotSuccess(true);

    when(smh.transformMeta.isDoingErrorHandling()).thenReturn(false);

    ExecProcess transform = newTransform(meta, rowMeta, row);

    assertFalse(transform.processRow());
    assertEquals(1, transform.getErrors());
  }

  @Test
  void processRow_emptyProcessField_throws() throws HopException {
    HopEnvironment.init();

    ExecProcessMeta meta = new ExecProcessMeta();
    meta.setDefault();
    meta.setProcessField("cmdline");
    meta.setResultFieldName("result_out");
    meta.setErrorFieldName("result_err");
    meta.setExitValueFieldName("result_exit");

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("cmdline"));

    ExecProcess transform = newTransform(meta, rowMeta, new Object[] {""});

    assertThrows(HopException.class, transform::processRow);
  }

  @Test
  void init_requiresResultFieldName() throws HopException {
    HopEnvironment.init();

    ExecProcessMeta meta = new ExecProcessMeta();
    meta.setDefault();
    meta.setResultFieldName("");

    ExecProcess transform =
        new ExecProcess(
            smh.transformMeta, meta, new ExecProcessData(), 0, smh.pipelineMeta, smh.pipeline);

    assertFalse(transform.init());
  }

  private ExecProcess newTransform(ExecProcessMeta meta, IRowMeta rowMeta, Object[] row)
      throws HopException {
    TransformMeta tm = smh.transformMeta;
    when(tm.isDoingErrorHandling()).thenReturn(false);

    ExecProcess transform =
        new ExecProcess(tm, meta, new ExecProcessData(), 0, smh.pipelineMeta, smh.pipeline);
    transform.init();
    transform.setInputRowMeta(rowMeta);
    transform.addRowSetToInputRowSets(inputRows(rowMeta, row));
    transform.addRowSetToOutputRowSets(new QueueRowSet());
    return transform;
  }

  private static Object[] readSingleOutputRow(ExecProcess transform) throws HopException {
    IRowSet outputRowSet = transform.getOutputRowSets().get(0);
    Object[] out = outputRowSet.getRow();
    assertNotNull(out);
    return out;
  }
}
