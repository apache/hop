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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Execute a process * */
public class ExecProcess extends BaseTransform<ExecProcessMeta, ExecProcessData> {

  private static final Class<?> PKG = ExecProcessMeta.class;
  public static final String CONST_EXEC_PROCESS_EXCEPTION_COULD_NOT_FIND_FIELD =
      "ExecProcess.Exception.CouldNotFindField";

  /**
   * How long a stopped process is given to exit on a graceful destroy before it is force-killed.
   */
  private static final long GRACE_PERIOD_SECONDS = 30;

  private final AtomicReference<ProcessExecution> activeExecution = new AtomicReference<>();

  public ExecProcess(
      TransformMeta transformMeta,
      ExecProcessMeta meta,
      ExecProcessData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    // Get row from input rowset & set row busy!
    Object[] r = getRow();
    // no more input to be expected...
    if (r == null) {
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      initializeAndLookup();
    }

    Object[] outputRow = RowDataUtil.createResizedCopy(r, data.outputRowMeta.size());

    // get process to execute
    String processString = getInputRowMeta().getString(r, data.indexOfProcess);
    if (StringUtils.isEmpty(processString)) {
      throw new HopException(BaseMessages.getString(PKG, "ExecProcess.ProcessEmpty"));
    }

    ProcessResult processResult = new ProcessResult();

    try {
      // execute and return result
      if (meta.isArgumentsInFields()) {
        List<String> cmdArray = new ArrayList<>();
        cmdArray.add(processString);

        for (int argumentIndex : data.argumentIndexes) {
          // Runtime.exec will fail on null array elements
          // Convert to an empty string if value is null
          String argString = getInputRowMeta().getString(r, argumentIndex);
          cmdArray.add(Const.NVL(argString, ""));
        }

        if (!execProcess(cmdArray.toArray(new String[0]), processResult)) {
          setOutputDone();
          return false;
        }
      } else {
        if (isDetailed() && !hasBalancedQuotes(processString)) {
          logDetailed(
              BaseMessages.getString(PKG, "ExecProcess.Log.UnbalancedQuotes", processString));
        }
        if (!execProcess(tokenizeCommandLine(processString), processResult)) {
          setOutputDone();
          return false;
        }
      }

      if (meta.isFailWhenNotSuccess() && processResult.getExitValue() != 0) {
        String errorString = processResult.getErrorStream();
        if (StringUtils.isEmpty(errorString)) {
          errorString = processResult.getOutputStream();
        }
        throw new HopException(errorString);
      }

      // Add result field to input stream
      int rowIndex = getInputRowMeta().size();
      outputRow[rowIndex++] = processResult.getOutputStream();

      // Add result field to input stream
      outputRow[rowIndex++] = processResult.getErrorStream();

      // Add result field to input stream
      outputRow[rowIndex] = processResult.getExitValue();

      // add new values to the row. copy row to output rowset(s)
      putRow(data.outputRowMeta, outputRow);

      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(
                PKG,
                "ExecProcess.LineNumber",
                getLinesRead() + " : " + data.outputRowMeta.getString(outputRow)));
      }
    } catch (HopException e) {
      if (getTransformMeta().isDoingErrorHandling()) {
        putError(
            getInputRowMeta(), r, 1, e.toString(), meta.getResultFieldName(), "ExecProcess001");
      } else {
        logError(
            BaseMessages.getString(PKG, "ExecProcess.ErrorInTransformRunning") + e.getMessage());
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
    }

    return true;
  }

  private void initializeAndLookup() throws HopException {
    // Calculate the output row metadata.
    //
    data.outputRowMeta = getInputRowMeta().clone();
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

    // Check if a process field name is provided
    //
    if (StringUtils.isEmpty(meta.getProcessField())) {
      logError(BaseMessages.getString(PKG, "ExecProcess.Error.ProcessFieldMissing"));
      throw new HopException(BaseMessages.getString(PKG, "ExecProcess.Error.ProcessFieldMissing"));
    }

    // cache the position of the field
    if (data.indexOfProcess < 0) {
      data.indexOfProcess = getInputRowMeta().indexOfValue(meta.getProcessField());
      if (data.indexOfProcess < 0) {
        // The field is unreachable !
        logError(
            BaseMessages.getString(PKG, CONST_EXEC_PROCESS_EXCEPTION_COULD_NOT_FIND_FIELD)
                + "["
                + meta.getProcessField()
                + "]");
        throw new HopException(
            BaseMessages.getString(
                PKG, CONST_EXEC_PROCESS_EXCEPTION_COULD_NOT_FIND_FIELD, meta.getProcessField()));
      }
    }
    if (meta.isArgumentsInFields() && data.argumentIndexes == null) {
      data.argumentIndexes = new ArrayList<>();
      for (ExecProcessMeta.EPField field : meta.getArgumentFields()) {
        String fieldName = field.getName();
        int argumentIndex = getInputRowMeta().indexOfValue(fieldName);
        if (argumentIndex < 0) {
          logError(
              BaseMessages.getString(PKG, CONST_EXEC_PROCESS_EXCEPTION_COULD_NOT_FIND_FIELD)
                  + "["
                  + fieldName
                  + "]");
          throw new HopException(
              BaseMessages.getString(
                  PKG, CONST_EXEC_PROCESS_EXCEPTION_COULD_NOT_FIND_FIELD, fieldName));
        }
        data.argumentIndexes.add(argumentIndex);
      }
    }
  }

  @Override
  public void stopRunning() throws HopException {
    // A pipeline stop must not wait for a child process or a stream reader. In particular, a
    // descendant can keep a pipe open after the direct child exits.
    setStopped(true);
    final var execution = activeExecution.get();
    if (execution != null) {
      execution.cancelled.set(true);
    }
  }

  private boolean execProcess(final String[] command, final ProcessResult result)
      throws HopException {
    final var execution = new ProcessExecution();
    activeExecution.set(execution);
    Process child = null;
    boolean completed = false;
    try {
      if (isStopped()) {
        return false;
      }
      try {
        child = data.runtime.exec(command);
      } catch (Exception e) {
        // An exec failure may be an IOException, SecurityException or IllegalArgumentException; all
        // must reach the error hop rather than escape processRow as a non-HopException.
        if (isStopped() || execution.cancelled.get()) {
          return false;
        }
        result.setErrorStream(e.getMessage());
        return true;
      }
      if (isCancelled(execution)) {
        return false;
      }
      final Process process = child;
      // Owned daemon readers keep blocked pipes out of the shared common pool. They close their
      // streams on normal completion; cancellation also arranges asynchronous stream cleanup.
      final var stdout = execution.readers.submit(() -> readOutput(process.getInputStream()));
      final var stderr = execution.readers.submit(() -> readOutput(process.getErrorStream()));
      while (true) {
        // Remember children while the parent is alive: after it exits, inherited pipes may
        // still be held by descendants that are no longer discoverable through that parent.
        try (final var descendants = process.descendants()) {
          descendants.forEach(execution.descendants::add);
        }
        if (isCancelled(execution)) {
          return false;
        }
        // Poll at a second's granularity so we do not rescan /proc more often than needed.
        if (process.waitFor(1, TimeUnit.SECONDS)) {
          break;
        }
      }
      final var output = awaitOutput(stdout, execution);
      if (isCancelled(execution)) {
        return false;
      }
      final var error = awaitOutput(stderr, execution);
      if (isCancelled(execution)) {
        return false;
      }
      result.setOutputStream(output);
      result.setErrorStream(error);
      result.setExitValue(process.exitValue());
      completed = true;
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HopException("Interrupted while running process " + Arrays.toString(command), e);
    } catch (ExecutionException e) {
      throw new HopException("Unable to read process output", e.getCause());
    } finally {
      activeExecution.compareAndSet(execution, null);
      execution.readers.shutdownNow();
      if (child != null) {
        if (completed) {
          // Normal completion: the owned readers already closed the streams. Closing them again is
          // a defensive no-op that needs no dedicated thread.
          closeProcessStream(child.getOutputStream());
          closeProcessStream(child.getInputStream());
          closeProcessStream(child.getErrorStream());
        } else {
          // Aborted: destroy the process (graceful then forced) inline on the transform thread so a
          // hop-run shutdown that exits the JVM cannot orphan a still-running command, then close
          // its streams on a background thread.
          cleanupProcess(child, execution);
        }
      }
    }
  }

  private boolean isCancelled(final ProcessExecution execution) {
    return isStopped() || execution.cancelled.get();
  }

  private String awaitOutput(final Future<String> output, final ProcessExecution execution)
      throws InterruptedException, ExecutionException {
    while (!isCancelled(execution)) {
      try {
        return output.get(100, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        // Observe stop requests even after the process exits but a stream drainer is still active.
      }
    }
    return null;
  }

  private String readOutput(final InputStream stream) throws IOException {
    try (final var reader = new BufferedReader(new InputStreamReader(stream))) {
      return getOutputString(reader);
    }
  }

  private void cleanupProcess(final Process process, final ProcessExecution execution) {
    // Aborted: stop the process right away on the transform thread so a hop-run shutdown that calls
    // System.exit() cannot orphan a still-running command. Only the stream close, that can block in
    // native I/O or on a reader's monitor, is deferred to a background thread.
    killProcess(process, execution);
    closeStreamsAsync(process);
  }

  /**
   * Stop the process gracefully (SIGTERM) and, after a grace period, force it (SIGKILL). This runs
   * on the transform thread before processRow returns, so a pipeline shutdown that exits the JVM
   * cannot orphan a still-running command. The daemon cleanup that used to own this work could be
   * killed by System.exit() before the forced kill ever ran.
   */
  private void killProcess(final Process process, final ProcessExecution execution) {
    try {
      try (final var descendants = process.descendants()) {
        descendants.forEach(execution.descendants::add);
      }
      // Ask gracefully first (SIGTERM) so a script trapping it can clean up and roll back, then
      // fall back to a forced kill (SIGKILL) only if it does not exit.
      execution.descendants.forEach(ProcessHandle::destroy);
      if (process.isAlive()) {
        process.destroy();
      }
      if (!process.waitFor(GRACE_PERIOD_SECONDS, TimeUnit.SECONDS)) {
        execution.descendants.forEach(ProcessHandle::destroyForcibly);
        if (process.isAlive()) {
          process.destroyForcibly();
        }
      }
    } catch (InterruptedException e) {
      // A forced kill still makes sure the process goes away without waiting again.
      Thread.currentThread().interrupt();
      execution.descendants.forEach(ProcessHandle::destroyForcibly);
      if (process.isAlive()) {
        process.destroyForcibly();
      }
    }
  }

  /**
   * Close the process streams on a daemon thread. stream.close() may block in native I/O or on a
   * reader's monitor, so it must not run inline on the transform or pipeline thread.
   */
  private void closeStreamsAsync(final Process process) {
    final var closer =
        new Thread(
            () -> {
              closeProcessStream(process.getOutputStream());
              closeProcessStream(process.getInputStream());
              closeProcessStream(process.getErrorStream());
            },
            "hop-execprocess-cleanup");
    closer.setDaemon(true);
    closer.start();
  }

  private void closeProcessStream(final Closeable stream) {
    try {
      stream.close();
    } catch (IOException e) {
      logDebug("Unable to close process stream", e);
    }
  }

  private static final class ProcessExecution {
    private final AtomicBoolean cancelled = new AtomicBoolean();
    // Owned by the executing (transform) thread, which also performs the graceful-then-forced kill.
    private final Set<ProcessHandle> descendants = new HashSet<>();
    private final ExecutorService readers =
        Executors.newFixedThreadPool(
            2,
            task -> {
              final var thread = new Thread(task, "hop-execprocess-output");
              thread.setDaemon(true);
              return thread;
            });
  }

  /**
   * Split a command line into the executable and its arguments. Tokens are separated by whitespace,
   * but a section wrapped in single or double quotes is kept together as one token even when it
   * contains whitespace, and the quotes themselves are removed - the way a shell would do it.
   *
   * <p>Quoting is positional, so quoted and unquoted parts that touch each other end up in the same
   * token: {@code --path="/my folder"} yields the single argument {@code --path=/my folder}. There
   * is no backslash escaping: on Windows a backslash is a path separator, so {@code C:\my dir}
   * keeps its backslashes and needs quotes to survive the space.
   *
   * <p>Quotes only take effect when they are balanced, see {@link #hasBalancedQuotes(String)}.
   */
  static String[] tokenizeCommandLine(String command) throws HopException {
    List<String> tokens = tokenize(command, hasBalancedQuotes(command));
    if (tokens.isEmpty()) {
      throw new HopException(BaseMessages.getString(PKG, "ExecProcess.ProcessEmpty"));
    }
    return tokens.toArray(new String[0]);
  }

  /**
   * Whether every single and double quote in the command line is closed again.
   *
   * <p>When they are not, we can't tell a mis-typed quote from a quote character that is simply
   * part of an argument, so the command line is split on whitespace only and the quotes are left in
   * place - what every Hop version up to 2.18 did with any command line. That keeps a stray
   * apostrophe ({@code --message=don't}) working instead of turning it into a failing row.
   */
  static boolean hasBalancedQuotes(String command) {
    char quote = 0;
    for (int i = 0; i < command.length(); i++) {
      char c = command.charAt(i);
      if (quote != 0) {
        if (c == quote) {
          quote = 0;
        }
      } else if (c == '\'' || c == '"') {
        quote = c;
      }
    }
    return quote == 0;
  }

  private static List<String> tokenize(String command, boolean respectQuotes) {
    List<String> tokens = new ArrayList<>();
    StringBuilder token = new StringBuilder();
    boolean inToken = false;
    char quote = 0;

    for (int i = 0; i < command.length(); i++) {
      char c = command.charAt(i);
      if (quote != 0) {
        // Inside quotes everything is literal until the matching quote shows up
        if (c == quote) {
          quote = 0;
        } else {
          token.append(c);
        }
      } else if (respectQuotes && (c == '\'' || c == '"')) {
        quote = c;
        inToken = true;
      } else if (Character.isWhitespace(c)) {
        if (inToken) {
          tokens.add(token.toString());
          token.setLength(0);
          inToken = false;
        }
      } else {
        token.append(c);
        inToken = true;
      }
    }

    if (inToken) {
      tokens.add(token.toString());
    }
    return tokens;
  }

  private String getOutputString(BufferedReader b) throws IOException {
    StringBuilder returnValueBuffer = new StringBuilder();
    String line;
    String delim = meta.getOutputLineDelimiter();
    if (delim == null) {
      delim = "";
    } else {
      delim = resolve(delim);
    }

    while ((line = b.readLine()) != null) {
      if (!returnValueBuffer.isEmpty()) {
        returnValueBuffer.append(delim);
      }
      returnValueBuffer.append(line);
    }
    return returnValueBuffer.toString();
  }

  @Override
  public boolean init() {

    if (super.init()) {
      if (StringUtils.isEmpty(meta.getResultFieldName())) {
        logError(BaseMessages.getString(PKG, "ExecProcess.Error.ResultFieldMissing"));
        return false;
      }
      data.runtime = Runtime.getRuntime();
      return true;
    }
    return false;
  }
}
