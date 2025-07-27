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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang.StringUtils;
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
  private boolean killing;
  private CountDownLatch waitForLatch;

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
    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if (r == null) { // no more input to be expected...
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

        execProcess(cmdArray.toArray(new String[0]), processResult);
      } else {
        execProcess(processString, processResult);
      }

      if (meta.isFailWhenNotSuccess() && processResult.getExistStatus() != 0) {
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
      outputRow[rowIndex] = processResult.getExistStatus();

      // add new values to the row.
      putRow(data.outputRowMeta, outputRow); // copy row to output rowset(s)

      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(
                PKG,
                "ExecProcess.LineNumber",
                getLinesRead() + " : " + getInputRowMeta().getString(r)));
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
    if (waitForLatch != null) {
      killing = true;
      try {
        waitForLatch.await();
      } catch (InterruptedException e) {
        throw new HopException("Interrupted exception while kill the process", e);
      }
    }
  }

  private void execProcess(String process, ProcessResult processresult) throws HopException {
    execProcess(new String[] {process}, processresult);
  }

  private void execProcess(String[] process, ProcessResult processresult) throws HopException {
    Process p = null;
    waitForLatch = new CountDownLatch(1);
    try {
      String errorMsg = null;
      // execute process
      try {
        if (!meta.isArgumentsInFields()) {
          p = data.runtime.exec(process[0]);
        } else {
          p = data.runtime.exec(process);
        }
      } catch (Exception e) {
        errorMsg = e.getMessage();
      }
      if (p == null) {
        processresult.setErrorStream(errorMsg);
      } else {
        CompletableFuture<IOException> future =
            p.onExit()
                .thenApply(
                    processRef -> {
                      try {
                        // get output stream
                        processresult.setOutputStream(
                            getOutputString(
                                new BufferedReader(
                                    new InputStreamReader(processRef.getInputStream()))));

                        // get error message
                        processresult.setErrorStream(
                            getOutputString(
                                new BufferedReader(
                                    new InputStreamReader(processRef.getErrorStream()))));
                      } catch (IOException e) {
                        return e;
                      }
                      return null;
                    });

        // Wait until end
        IOException exception;
        while (true) {
          try {
            exception = future.get(1, TimeUnit.SECONDS);
            break;
          } catch (TimeoutException ignore) {
            if (killing) {
              p.children().forEach(ProcessHandle::destroy);
              if (p.isAlive()) {
                p.destroy();
              }
              exception = future.get();
              logMinimal(BaseMessages.getString(PKG, "ExecProcess.AbortProcess"));
              break;
            }
          }
        }
        if (exception != null) {
          throw exception;
        }

        // get exit status
        processresult.setExistStatus(p.exitValue());
      }
    } catch (IOException ioe) {
      throw new HopException(
          "IO exception while running the process " + Arrays.toString(process) + "!", ioe);
    } catch (InterruptedException ie) {
      throw new HopException(
          "Interrupted exception while running the process " + Arrays.toString(process) + "!", ie);
    } catch (Exception e) {
      throw new HopException(e);
    } finally {
      if (killing || waitForLatch.getCount() > 0) {
        waitForLatch.countDown();
        killing = false;
      }
      if (p != null) {
        p.destroy();
      }
    }
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
