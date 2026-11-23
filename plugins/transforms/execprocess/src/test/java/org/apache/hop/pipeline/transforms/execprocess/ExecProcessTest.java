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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
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
import org.junit.jupiter.api.Timeout;
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

  private RowMeta inputRowMetaForArguments(String... argFieldNames) {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("cmd"));
    for (String name : argFieldNames) {
      rowMeta.addValueMeta(new ValueMetaString(name));
    }
    return rowMeta;
  }

  private ExecProcessMeta metaForArgumentsMode(String... argFieldNames) {
    ExecProcessMeta meta = new ExecProcessMeta();
    meta.setDefault();
    meta.setProcessField("cmd");
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
    ExecProcessMeta meta = metaForArgumentsMode("a1", "a2");
    Object[] row = {cmd[0], cmd[1], cmd[2]};
    RowMeta rowMeta = inputRowMetaForArguments("a1", "a2");

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
    ExecProcessMeta meta = metaForArgumentsMode("a1", "a2");
    Object[] row = {cmd[0], cmd[1], cmd[2]};
    RowMeta rowMeta = inputRowMetaForArguments("a1", "a2");

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

    Object[] row = {singleTokenEchoCommand()};
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
    ExecProcessMeta meta = metaForArgumentsMode("a1", "a2");
    Object[] row = {cmd[0], cmd[1], cmd[2]};
    RowMeta rowMeta = inputRowMetaForArguments("a1", "a2");
    meta.setFailWhenNotSuccess(true);

    when(smh.transformMeta.isDoingErrorHandling()).thenReturn(false);

    ExecProcess transform = newTransform(meta, rowMeta, row);

    assertFalse(transform.processRow());
    assertEquals(1, transform.getErrors());
  }

  @Test
  void tokenizeCommandLine_splitsOnWhitespace() throws HopException {
    assertEquals(
        Arrays.asList("/bin/echo", "hop-single"),
        Arrays.asList(ExecProcess.tokenizeCommandLine("/bin/echo hop-single")));
    assertEquals(List.of("cmd"), Arrays.asList(ExecProcess.tokenizeCommandLine("cmd")));
    assertEquals(
        Arrays.asList("ls", "-la", "/work/hop/config"),
        Arrays.asList(ExecProcess.tokenizeCommandLine("  ls\t-la   /work/hop/config ")));
  }

  /** Regression for issue #5988 : quoted arguments must survive as a single, unquoted token. */
  @Test
  void tokenizeCommandLine_keepsQuotedSectionsTogetherAndStripsQuotes() throws HopException {
    List<String> expected = Arrays.asList("ls", "-la", "/work/hop/config");

    assertEquals(
        expected, Arrays.asList(ExecProcess.tokenizeCommandLine("ls -la /work/hop/config")));
    assertEquals(
        expected, Arrays.asList(ExecProcess.tokenizeCommandLine("ls -la \"/work/hop/config\"")));
    assertEquals(
        expected, Arrays.asList(ExecProcess.tokenizeCommandLine("ls -la '/work/hop/config'")));
  }

  @Test
  void tokenizeCommandLine_quotedWhitespaceStaysInOneArgument() throws HopException {
    assertEquals(
        Arrays.asList("ls", "-la", "/work/my folder"),
        Arrays.asList(ExecProcess.tokenizeCommandLine("ls -la \"/work/my folder\"")));
    assertEquals(
        Arrays.asList("ls", "-la", "/work/my folder"),
        Arrays.asList(ExecProcess.tokenizeCommandLine("ls -la '/work/my folder'")));
  }

  @Test
  void tokenizeCommandLine_handlesPartialQuotingAndNestedQuoteCharacters() throws HopException {
    // Quoting is positional : quoted and unquoted parts that touch stay in the same token
    assertEquals(
        Arrays.asList("cmd", "--path=/my folder/x"),
        Arrays.asList(ExecProcess.tokenizeCommandLine("cmd --path=\"/my folder\"/x")));
    // The other quote character is literal inside quotes
    assertEquals(
        Arrays.asList("echo", "it's here"),
        Arrays.asList(ExecProcess.tokenizeCommandLine("echo \"it's here\"")));
    assertEquals(
        Arrays.asList("echo", "say \"hi\""),
        Arrays.asList(ExecProcess.tokenizeCommandLine("echo 'say \"hi\"'")));
    // An empty quoted section is a real (empty) argument
    assertEquals(
        Arrays.asList("cmd", "", "x"),
        Arrays.asList(ExecProcess.tokenizeCommandLine("cmd \"\" x")));
  }

  /**
   * An unbalanced quote falls back to the pre-2.19 behaviour - split on whitespace, keep the quotes
   * - so a stray apostrophe in an argument keeps working instead of failing the row.
   */
  @Test
  void tokenizeCommandLine_unbalancedQuoteFallsBackToWhitespaceSplitting() throws HopException {
    assertEquals(
        Arrays.asList("echo", "--message=don't", "x"),
        Arrays.asList(ExecProcess.tokenizeCommandLine("echo --message=don't x")));
    assertEquals(
        Arrays.asList("ls", "-la", "\"/work/my", "folder"),
        Arrays.asList(ExecProcess.tokenizeCommandLine("ls -la \"/work/my folder")));
  }

  @Test
  void hasBalancedQuotes_detectsUnclosedQuotes() {
    assertTrue(ExecProcess.hasBalancedQuotes("ls -la \"/work/hop/config\""));
    assertTrue(ExecProcess.hasBalancedQuotes("echo 'it\"s'"));
    assertTrue(ExecProcess.hasBalancedQuotes("ls -la /work/hop/config"));
    assertFalse(ExecProcess.hasBalancedQuotes("ls -la \"/work/hop"));
    assertFalse(ExecProcess.hasBalancedQuotes("echo don't"));
  }

  @Test
  void tokenizeCommandLine_blankCommandThrows() {
    assertThrows(HopException.class, () -> ExecProcess.tokenizeCommandLine("   "));
  }

  @Test
  @EnabledOnOs({OS.LINUX, OS.MAC})
  void processRow_singleCommandString_quotedArgumentWithSpaces() throws HopException {
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

    Object[] row = {"/bin/echo \"hop single\" 'and more'"};
    ExecProcess transform = newTransform(meta, rowMeta, row);

    assertTrue(transform.processRow());
    assertFalse(transform.processRow());

    Object[] out = readSingleOutputRow(transform);
    assertEquals("hop single and more", out[1].toString().trim());
    assertEquals(0L, out[3]);
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

  @Test
  @Timeout(10)
  void stopWhileDrainingOutputDoesNotWaitForEndOfStream() throws Exception {
    HopEnvironment.init();
    final var transform =
        newTransform(metaForArgumentsMode(), inputRowMetaForArguments(), new Object[] {"test"});
    final var runtime = mock(Runtime.class);
    final var process = mock(Process.class);
    final var stdout = new BlockingInputStream();
    when(process.getInputStream()).thenReturn(stdout);
    when(process.getErrorStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
    when(process.getOutputStream()).thenReturn(new ByteArrayOutputStream());
    when(process.waitFor(1, TimeUnit.SECONDS)).thenReturn(true);
    when(process.descendants()).thenAnswer(invocation -> Stream.empty());
    when(runtime.exec(any(String[].class))).thenReturn(process);
    transform.getData().runtime = runtime;
    final var executor = Executors.newSingleThreadExecutor();
    try {
      final var running = executor.submit(transform::processRow);
      assertTrue(stdout.reading.await(5, TimeUnit.SECONDS));
      transform.stopRunning();
      transform.stopRunning();
      assertFalse(running.get(5, TimeUnit.SECONDS));
      assertTrue(stdout.closed.await(5, TimeUnit.SECONDS));
      assertEquals(0, transform.getErrors());
      assertEquals(0, transform.getOutputRowSets().getFirst().size());
    } finally {
      stdout.close();
      executor.shutdownNow();
    }
  }

  @Test
  @Timeout(10)
  void stopDuringLaunchCancelsTheNewProcessWithoutBlockingStopper() throws Exception {
    HopEnvironment.init();
    final var transform =
        newTransform(metaForArgumentsMode(), inputRowMetaForArguments(), new Object[] {"test"});
    final var runtime = mock(Runtime.class);
    final var process = mock(Process.class);
    final var launching = new CountDownLatch(1);
    final var releaseLaunch = new CountDownLatch(1);
    final var destroyed = new CountDownLatch(1);
    when(runtime.exec(any(String[].class)))
        .thenAnswer(
            invocation -> {
              launching.countDown();
              assertTrue(releaseLaunch.await(5, TimeUnit.SECONDS));
              return process;
            });
    when(process.getInputStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
    when(process.getErrorStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
    when(process.getOutputStream()).thenReturn(new ByteArrayOutputStream());
    when(process.isAlive()).thenReturn(true);
    when(process.descendants()).thenAnswer(invocation -> Stream.empty());
    when(process.destroyForcibly())
        .thenAnswer(
            invocation -> {
              destroyed.countDown();
              return process;
            });
    transform.getData().runtime = runtime;
    final var executor = Executors.newSingleThreadExecutor();
    try {
      final var running = executor.submit(transform::processRow);
      assertTrue(launching.await(5, TimeUnit.SECONDS));
      transform.stopRunning();
      releaseLaunch.countDown();
      assertFalse(running.get(5, TimeUnit.SECONDS));
      assertTrue(destroyed.await(5, TimeUnit.SECONDS));
      assertEquals(0, transform.getErrors());
    } finally {
      releaseLaunch.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  @Timeout(10)
  void cancellationRetainsChildrenObservedBeforeParentExit() throws Exception {
    HopEnvironment.init();
    final var transform =
        newTransform(metaForArgumentsMode(), inputRowMetaForArguments(), new Object[] {"test"});
    final var runtime = mock(Runtime.class);
    final var process = mock(Process.class);
    final var descendant = mock(ProcessHandle.class);
    final var destroyed = new CountDownLatch(1);
    final var stdout = new BlockingInputStream();
    when(descendant.destroyForcibly())
        .thenAnswer(
            invocation -> {
              destroyed.countDown();
              return true;
            });
    when(process.descendants()).thenReturn(Stream.of(descendant), Stream.empty());
    when(process.getInputStream()).thenReturn(stdout);
    when(process.getErrorStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
    when(process.getOutputStream()).thenReturn(new ByteArrayOutputStream());
    when(process.waitFor(1, TimeUnit.SECONDS)).thenReturn(true);
    when(runtime.exec(any(String[].class))).thenReturn(process);
    transform.getData().runtime = runtime;
    final var executor = Executors.newSingleThreadExecutor();
    try {
      final var running = executor.submit(transform::processRow);
      assertTrue(stdout.reading.await(5, TimeUnit.SECONDS));
      transform.stopRunning();
      assertFalse(running.get(5, TimeUnit.SECONDS));
      assertTrue(destroyed.await(5, TimeUnit.SECONDS));
    } finally {
      stdout.close();
      executor.shutdownNow();
    }
  }

  @Test
  void stopBeforeLaunchDoesNotStartAProcess() throws Exception {
    HopEnvironment.init();
    final var transform =
        newTransform(metaForArgumentsMode(), inputRowMetaForArguments(), new Object[] {"test"});
    final var runtime = mock(Runtime.class);
    transform.getData().runtime = runtime;
    transform.stopRunning();
    assertFalse(transform.processRow());
    verify(runtime, never()).exec(any(String[].class));
  }

  @Test
  @Timeout(10)
  void stoppedProcessThatExitsWithinGracePeriodIsDestroyedNotForceKilled() throws Exception {
    HopEnvironment.init();
    final var transform =
        newTransform(metaForArgumentsMode(), inputRowMetaForArguments(), new Object[] {"test"});
    final var runtime = mock(Runtime.class);
    final var process = mock(Process.class);
    final var stdout = new BlockingInputStream();
    when(process.getInputStream()).thenReturn(stdout);
    when(process.getErrorStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
    when(process.getOutputStream()).thenReturn(new ByteArrayOutputStream());
    when(process.isAlive()).thenReturn(true);
    when(process.descendants()).thenAnswer(invocation -> Stream.empty());
    // The process has not exited on its own while it is polled ...
    when(process.waitFor(1, TimeUnit.SECONDS)).thenReturn(false);
    // ... but a graceful destroy makes it exit within the grace period.
    when(process.waitFor(30, TimeUnit.SECONDS)).thenReturn(true);
    when(runtime.exec(any(String[].class))).thenReturn(process);
    transform.getData().runtime = runtime;
    final var executor = Executors.newSingleThreadExecutor();
    try {
      final var running = executor.submit(transform::processRow);
      assertTrue(stdout.reading.await(5, TimeUnit.SECONDS));
      transform.stopRunning();
      assertFalse(running.get(5, TimeUnit.SECONDS));
      // A graceful destroy must be tried first, and a forced kill never becomes necessary when the
      // process cooperates within the grace period.
      assertTrue(stdout.closed.await(5, TimeUnit.SECONDS));
      verify(process).destroy();
      verify(process, never()).destroyForcibly();
    } finally {
      stdout.close();
      executor.shutdownNow();
    }
  }

  @Test
  @Timeout(10)
  void forceKillRunsInlineOnTransformThreadBeforeProcessRowReturns() throws Exception {
    // Regression: the forced kill used to run on a daemon cleanup thread that hop-run's
    // System.exit() could kill before destroyForcibly() ever ran, leaving a SIGTERM-ignoring
    // command running. The graceful-then-forced kill must now run inline on the transform thread so
    // processRow() does not return until the process has been forcibly killed.
    HopEnvironment.init();
    final var transform =
        newTransform(metaForArgumentsMode(), inputRowMetaForArguments(), new Object[] {"test"});
    final var runtime = mock(Runtime.class);
    final var process = mock(Process.class);
    final var stdout = new BlockingInputStream();
    final var forceKillEntered = new CountDownLatch(1);
    final var releaseForceKill = new CountDownLatch(1);
    when(process.getInputStream()).thenReturn(stdout);
    when(process.getErrorStream()).thenReturn(new ByteArrayInputStream(new byte[0]));
    when(process.getOutputStream()).thenReturn(new ByteArrayOutputStream());
    when(process.isAlive()).thenReturn(true);
    when(process.descendants()).thenAnswer(invocation -> Stream.empty());
    // The process never exits on its own, neither in the poll loop nor within the grace period.
    when(process.waitFor(1, TimeUnit.SECONDS)).thenReturn(false);
    when(process.waitFor(30, TimeUnit.SECONDS)).thenReturn(false);
    when(process.destroyForcibly())
        .thenAnswer(
            invocation -> {
              forceKillEntered.countDown();
              assertTrue(releaseForceKill.await(5, TimeUnit.SECONDS));
              return process;
            });
    when(runtime.exec(any(String[].class))).thenReturn(process);
    transform.getData().runtime = runtime;
    final var executor = Executors.newSingleThreadExecutor();
    try {
      final var running = executor.submit(transform::processRow);
      assertTrue(stdout.reading.await(5, TimeUnit.SECONDS));
      transform.stopRunning();
      assertTrue(forceKillEntered.await(5, TimeUnit.SECONDS));
      // processRow is still running because the forced kill holds the transform thread; it must not
      // have returned and left the kill to a background thread that System.exit() could kill.
      assertThrows(TimeoutException.class, () -> running.get(2, TimeUnit.SECONDS));
      releaseForceKill.countDown();
      assertFalse(running.get(5, TimeUnit.SECONDS));
      verify(process).destroy();
      verify(process).destroyForcibly();
      assertEquals(0, transform.getErrors());
    } finally {
      releaseForceKill.countDown();
      stdout.close();
      executor.shutdownNow();
    }
  }

  private static final class BlockingInputStream extends InputStream {
    private final CountDownLatch reading = new CountDownLatch(1);
    private final CountDownLatch closed = new CountDownLatch(1);

    @Override
    public int read() {
      reading.countDown();
      boolean interrupted = false;
      while (true) {
        try {
          closed.await();
          break;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      return -1;
    }

    @Override
    public void close() {
      closed.countDown();
    }
  }

  private ExecProcess newTransform(ExecProcessMeta meta, IRowMeta rowMeta, Object[] row) {
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

  private static Object[] readSingleOutputRow(ExecProcess transform) {
    IRowSet outputRowSet = transform.getOutputRowSets().getFirst();
    Object[] out = outputRowSet.getRow();
    assertNotNull(out);
    return out;
  }
}
