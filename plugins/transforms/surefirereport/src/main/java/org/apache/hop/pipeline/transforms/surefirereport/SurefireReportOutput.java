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

package org.apache.hop.pipeline.transforms.surefirereport;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.surefirereport.SurefireTestCase.Status;

/**
 * Accumulates test-result rows and writes a Maven Surefire-compatible XML report at end of stream.
 */
public class SurefireReportOutput
    extends BaseTransform<SurefireReportOutputMeta, SurefireReportOutputData> {

  private static final Class<?> PKG = SurefireReportOutputMeta.class;

  public SurefireReportOutput(
      TransformMeta transformMeta,
      SurefireReportOutputMeta meta,
      SurefireReportOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      writeReport();
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      data.suiteStartMs = System.currentTimeMillis();
      resolveFieldIndexes(getInputRowMeta());
    }

    SurefireTestCase testCase = buildTestCase(getInputRowMeta(), row);
    synchronized (data.lock) {
      data.testCases.add(testCase);
    }

    putRow(data.outputRowMeta, row);
    return true;
  }

  private void resolveFieldIndexes(IRowMeta rowMeta) throws HopTransformException {
    data.indexTestName = requireField(rowMeta, meta.getTestNameField());
    data.indexDuration = optionalField(rowMeta, meta.getDurationField());
    data.indexResult = optionalField(rowMeta, meta.getResultField());
    data.indexSystemOut = optionalField(rowMeta, meta.getSystemOutField());
    data.indexSystemErr = optionalField(rowMeta, meta.getSystemErrField());
    data.indexFailureMessage = optionalField(rowMeta, meta.getFailureMessageField());
    data.indexFailureType = optionalField(rowMeta, meta.getFailureTypeField());
  }

  private int requireField(IRowMeta rowMeta, String fieldName) throws HopTransformException {
    if (Utils.isEmpty(fieldName)) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "SurefireReportOutput.Error.TestNameFieldMissing"));
    }
    int index = rowMeta.indexOfValue(fieldName);
    if (index < 0) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "SurefireReportOutput.Error.CouldNotFindField", fieldName));
    }
    return index;
  }

  private int optionalField(IRowMeta rowMeta, String fieldName) throws HopTransformException {
    if (Utils.isEmpty(fieldName)) {
      return -1;
    }
    int index = rowMeta.indexOfValue(fieldName);
    if (index < 0) {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "SurefireReportOutput.Error.CouldNotFindField", fieldName));
    }
    return index;
  }

  private SurefireTestCase buildTestCase(IRowMeta rowMeta, Object[] row) throws HopException {
    String name = normalizeTestName(ConstString(rowMeta.getString(row, data.indexTestName)));
    if (Utils.isEmpty(name)) {
      name = "unknown";
    }

    double durationSeconds = 0.0;
    if (data.indexDuration >= 0) {
      IValueMeta vm = rowMeta.getValueMeta(data.indexDuration);
      Object value = row[data.indexDuration];
      if (value != null) {
        double raw = vm.getNumber(value);
        durationSeconds = meta.isDurationInMilliseconds() ? raw / 1000.0 : raw;
        if (durationSeconds < 0) {
          durationSeconds = 0;
        }
      }
    }

    Status status = Status.PASS;
    if (data.indexResult >= 0) {
      status = parseStatus(rowMeta, row, data.indexResult);
    }

    String systemOut =
        data.indexSystemOut >= 0 ? ConstString(rowMeta.getString(row, data.indexSystemOut)) : null;
    String systemErr =
        data.indexSystemErr >= 0 ? ConstString(rowMeta.getString(row, data.indexSystemErr)) : null;
    String failureMessage =
        data.indexFailureMessage >= 0
            ? ConstString(rowMeta.getString(row, data.indexFailureMessage))
            : null;
    String failureType =
        data.indexFailureType >= 0
            ? ConstString(rowMeta.getString(row, data.indexFailureType))
            : null;

    if ((status == Status.FAIL || status == Status.ERROR)
        && Utils.isEmpty(failureType)
        && !Utils.isEmpty(name)) {
      failureType = name;
    }

    return new SurefireTestCase(
        name, durationSeconds, status, systemOut, systemErr, failureMessage, failureType);
  }

  static Status parseStatus(IRowMeta rowMeta, Object[] row, int index) throws HopException {
    IValueMeta vm = rowMeta.getValueMeta(index);
    Object value = row[index];
    if (value == null) {
      return Status.FAIL;
    }
    if (vm.isBoolean()) {
      return vm.getBoolean(value) ? Status.PASS : Status.FAIL;
    }
    if (vm.isNumeric()) {
      // Treat as exit-style: 0 = success, non-zero = failure
      double n = vm.getNumber(value);
      return n == 0.0 ? Status.PASS : Status.FAIL;
    }
    String text = vm.getString(value);
    if (text == null) {
      return Status.FAIL;
    }
    String normalized = text.trim().toUpperCase();
    return switch (normalized) {
      case "Y", "YES", "TRUE", "PASS", "PASSED", "SUCCESS", "OK" -> Status.PASS;
      case "SKIP", "SKIPPED" -> Status.SKIP;
      case "ERROR" -> Status.ERROR;
      case "N", "NO", "FALSE", "FAIL", "FAILED", "FAILURE" -> Status.FAIL;
      default -> Status.FAIL;
    };
  }

  private static String ConstString(String value) {
    return value == null ? "" : value;
  }

  /**
   * Normalize workflow filenames to the short test names used by the classic IT runner: strip path,
   * optional {@code main-}/{@code main_} prefix, and {@code .hwf} extension.
   */
  static String normalizeTestName(String raw) {
    if (raw == null || raw.isEmpty()) {
      return raw;
    }
    String name = raw;
    int slash = Math.max(name.lastIndexOf('/'), name.lastIndexOf('\\'));
    if (slash >= 0 && slash < name.length() - 1) {
      name = name.substring(slash + 1);
    }
    if (name.startsWith("main-") || name.startsWith("main_")) {
      name = name.substring(5);
    }
    if (name.endsWith(".hwf") || name.endsWith(".HWF")) {
      name = name.substring(0, name.length() - 4);
    }
    return name;
  }

  private void writeReport() throws HopException {
    String filename = resolve(meta.getFilename());
    if (Utils.isEmpty(filename)) {
      throw new HopException(
          BaseMessages.getString(PKG, "SurefireReportOutput.Error.FilenameMissing"));
    }

    String suiteName = resolve(meta.getSuiteName());
    if (Utils.isEmpty(suiteName)) {
      suiteName = "tests";
    }

    Path reportPath = Paths.get(filename);
    if (meta.isCreateParentFolder() && reportPath.getParent() != null) {
      try {
        java.nio.file.Files.createDirectories(reportPath.getParent());
      } catch (Exception e) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "SurefireReportOutput.Error.WriteFailed", filename, e.getMessage()),
            e);
      }
    }

    int failedOrErrored;
    try {
      synchronized (data.lock) {
        SurefireReportWriter.write(reportPath, suiteName, data.testCases);
        failedOrErrored = 0;
        for (SurefireTestCase testCase : data.testCases) {
          if (testCase.getStatus() == Status.FAIL || testCase.getStatus() == Status.ERROR) {
            failedOrErrored++;
          }
        }
        if (isBasic()) {
          logBasic(
              BaseMessages.getString(
                  PKG,
                  "SurefireReportOutput.Log.ReportWritten",
                  Integer.toString(data.testCases.size()),
                  filename));
        }
      }
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "SurefireReportOutput.Error.WriteFailed", filename, e.getMessage()),
          e);
    }

    if (meta.isFailOnTestFailure() && failedOrErrored > 0) {
      logError(
          BaseMessages.getString(
              PKG, "SurefireReportOutput.Error.TestsFailed", Integer.toString(failedOrErrored)));
      setErrors(failedOrErrored);
      stopAll();
    }
  }

  @Override
  public boolean init() {
    if (super.init()) {
      if (Utils.isEmpty(meta.getFilename())) {
        logError(BaseMessages.getString(PKG, "SurefireReportOutput.Error.FilenameMissing"));
        return false;
      }
      if (Utils.isEmpty(meta.getTestNameField())) {
        logError(BaseMessages.getString(PKG, "SurefireReportOutput.Error.TestNameFieldMissing"));
        return false;
      }
      return true;
    }
    return false;
  }
}
