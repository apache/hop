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

package org.apache.hop.pipeline.transforms.systemdata;

import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.AVAILABLE_PROCESSORS;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.COMMITTED_VIRTUAL_MEMORY_SIZE;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.COPYNR;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.CURRENT_PID;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.FILENAME;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.FREE_PHYSICAL_MEMORY_SIZE;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.FREE_SWAP_SPACE_SIZE;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.HOSTNAME;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.HOSTNAME_REAL;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.IP_ADDRESS;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.JVM_AVAILABLE_MEMORY;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.JVM_CPU_TIME;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.JVM_FREE_MEMORY;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.JVM_MAX_MEMORY;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.JVM_TOTAL_MEMORY;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.MODIFIED_DATE;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.MODIFIED_USER;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.NEXT_DAY_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.NEXT_DAY_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.NEXT_MONTH_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.NEXT_MONTH_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.NEXT_QUARTER_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.NEXT_QUARTER_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.NEXT_WEEK_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.NEXT_WEEK_END_US;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.NEXT_WEEK_OPEN_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.NEXT_WEEK_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.NEXT_WEEK_START_US;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.NEXT_YEAR_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.NEXT_YEAR_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PIPELINE_DATE_FROM;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PIPELINE_DATE_TO;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PIPELINE_NAME;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_ENTRY_NR;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_EXIT_STATUS;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_IS_STOPPED;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_LOG_TEXT;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_NR_ERRORS;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_NR_FILES;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_NR_FILES_RETRIEVED;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_NR_LINES_DELETED;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_NR_LINES_INPUT;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_NR_LINES_OUTPUT;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_NR_LINES_READ;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_NR_LINES_REJECTED;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_NR_LINES_UPDATED;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_NR_LINES_WRITTEN;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_NR_ROWS;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREVIOUS_RESULT_RESULT;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREV_DAY_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREV_DAY_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREV_MONTH_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREV_MONTH_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREV_QUARTER_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREV_QUARTER_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREV_WEEK_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREV_WEEK_END_US;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREV_WEEK_OPEN_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREV_WEEK_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREV_WEEK_START_US;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREV_YEAR_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.PREV_YEAR_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.SYSTEM_DATE;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.SYSTEM_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.THIS_DAY_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.THIS_DAY_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.THIS_MONTH_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.THIS_MONTH_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.THIS_QUARTER_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.THIS_QUARTER_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.THIS_WEEK_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.THIS_WEEK_END_US;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.THIS_WEEK_OPEN_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.THIS_WEEK_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.THIS_WEEK_START_US;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.THIS_YEAR_END;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.THIS_YEAR_START;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.TOTAL_PHYSICAL_MEMORY_SIZE;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.TOTAL_SWAP_SPACE_SIZE;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.WORKFLOW_DATE_FROM;
import static org.apache.hop.pipeline.transforms.systemdata.SystemDataType.WORKFLOW_DATE_TO;

import java.util.Calendar;
import java.util.Date;
import java.util.EnumMap;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Get information from the System or the supervising pipeline.
 *
 * <p>The transform supports a wide range of system data types (see {@link SystemDataType}),
 * including:
 *
 * <ul>
 *   <li>Pipeline and workflow execution timestamps
 *   <li>Date/time boundaries (day, week, month, quarter, year)
 *   <li>JVM and system metrics (memory, CPU, processors)
 *   <li>Environment information (hostname, IP, PID)
 *   <li>Previous execution result statistics
 * </ul>
 */
public class SystemData extends BaseTransform<SystemDataMeta, SystemDataData> {
  private final Map<SystemDataType, ThrowingSupplier<Object>> resolvers =
      new EnumMap<>(SystemDataType.class);

  public SystemData(
      TransformMeta transformMeta,
      SystemDataMeta meta,
      SystemDataData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  /** Initializes the transform and prepares all field resolvers. */
  @Override
  public boolean init() {
    if (super.init()) {
      List<TransformMeta> previous = getPipelineMeta().findPreviousTransforms(getTransformMeta());
      if (!Utils.isEmpty(previous)) {
        data.readsRows = true;
      }

      initResolvers();
      return true;
    }
    return false;
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row;
    if (data.readsRows) {
      row = getRow();
      if (row == null) {
        setOutputDone();
        return false;
      }

      if (first) {
        first = false;
        data.outputRowMeta = getInputRowMeta().clone();
        meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      }

    } else {
      // empty row
      row = new Object[] {};
      incrementLinesRead();

      if (first) {
        first = false;
        data.outputRowMeta = new RowMeta();
        meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
      }
    }

    IRowMeta imeta = getInputRowMeta();
    if (imeta == null) {
      imeta = new RowMeta();
      this.setInputRowMeta(imeta);
    }

    row = getSystemData(imeta, row);

    if (isRowLevel()) {
      logRowlevel("System info returned: " + data.outputRowMeta.getString(row));
    }

    putRow(data.outputRowMeta, row);

    if (!data.readsRows) {
      // Just one row and then stop!
      setOutputDone();
      return false;
    }

    return true;
  }

  /**
   * Populates system data fields into the output row.
   *
   * <p>This method iterates over configured fields and resolves each value using the corresponding
   * resolver.
   *
   * @param inputRowMeta metadata of the input row
   * @param inputRowData input row data
   * @return a new row containing original data plus system fields
   * @throws HopException if value resolution fails
   */
  private Object[] getSystemData(IRowMeta inputRowMeta, Object[] inputRowData) throws HopException {
    Object[] row = RowDataUtil.createResizedCopy(inputRowData, data.outputRowMeta.size());

    for (int i = 0, index = inputRowMeta.size(); i < meta.getFields().size(); i++, index++) {
      SystemDataMeta.SystemInfoField field = meta.getFields().get(i);
      row[index] = resolveFieldValue(field.getFieldType());
    }
    return row;
  }

  /**
   * Initializes all resolver mappings for supported {@link SystemDataType}.
   *
   * <p>This method registers suppliers for:
   *
   * <ul>
   *   <li>Core system fields (dates, pipeline info)
   *   <li>Date boundary calculations
   *   <li>JVM metrics
   *   <li>Previous execution results
   * </ul>
   *
   * <p>Initialization is idempotent and executed only once.
   */
  private void initResolvers() {
    if (!resolvers.isEmpty()) {
      return;
    }
    resolvers.put(SYSTEM_START, () -> getPipeline().getExecutionStartDate());
    resolvers.put(PIPELINE_DATE_TO, () -> getPipeline().getExecutionStartDate());
    resolvers.put(SYSTEM_DATE, Date::new);
    resolvers.put(PIPELINE_DATE_FROM, this::pipelineDateFrom);
    resolvers.put(WORKFLOW_DATE_FROM, this::workflowDateFrom);
    resolvers.put(WORKFLOW_DATE_TO, this::workflowDateTo);

    resolvers.put(PREV_DAY_START, () -> dayBoundary(-1, true));
    resolvers.put(PREV_DAY_END, () -> dayBoundary(-1, false));
    resolvers.put(THIS_DAY_START, () -> dayBoundary(0, true));
    resolvers.put(THIS_DAY_END, () -> dayBoundary(0, false));
    resolvers.put(NEXT_DAY_START, () -> dayBoundary(1, true));
    resolvers.put(NEXT_DAY_END, () -> dayBoundary(1, false));

    resolvers.put(PREV_MONTH_START, () -> monthBoundary(-1, true));
    resolvers.put(PREV_MONTH_END, () -> monthBoundary(-1, false));
    resolvers.put(THIS_MONTH_START, () -> monthBoundary(0, true));
    resolvers.put(THIS_MONTH_END, () -> monthBoundary(0, false));
    resolvers.put(NEXT_MONTH_START, () -> monthBoundary(1, true));
    resolvers.put(NEXT_MONTH_END, () -> monthBoundary(1, false));

    resolvers.put(PREV_WEEK_START, () -> weekBoundary(-1, Locale.getDefault(), true, false));
    resolvers.put(PREV_WEEK_END, () -> weekBoundary(-1, Locale.getDefault(), false, false));
    resolvers.put(PREV_WEEK_OPEN_END, () -> weekBoundary(-1, Locale.ROOT, false, true));
    resolvers.put(PREV_WEEK_START_US, () -> weekBoundary(-1, Locale.US, true, false));
    resolvers.put(PREV_WEEK_END_US, () -> weekBoundary(-1, Locale.US, false, false));
    resolvers.put(THIS_WEEK_START, () -> weekBoundary(0, Locale.getDefault(), true, false));
    resolvers.put(THIS_WEEK_END, () -> weekBoundary(0, Locale.getDefault(), false, false));
    resolvers.put(THIS_WEEK_OPEN_END, () -> weekBoundary(0, Locale.ROOT, false, true));
    resolvers.put(THIS_WEEK_START_US, () -> weekBoundary(0, Locale.US, true, false));
    resolvers.put(THIS_WEEK_END_US, () -> weekBoundary(0, Locale.US, false, false));
    resolvers.put(NEXT_WEEK_START, () -> weekBoundary(1, Locale.getDefault(), true, false));
    resolvers.put(NEXT_WEEK_END, () -> weekBoundary(1, Locale.getDefault(), false, false));
    resolvers.put(NEXT_WEEK_OPEN_END, () -> weekBoundary(1, Locale.ROOT, false, true));
    resolvers.put(NEXT_WEEK_START_US, () -> weekBoundary(1, Locale.US, true, false));
    resolvers.put(NEXT_WEEK_END_US, () -> weekBoundary(1, Locale.US, false, false));

    resolvers.put(PREV_QUARTER_START, () -> quarterBoundary(-1, true));
    resolvers.put(PREV_QUARTER_END, () -> quarterBoundary(-1, false));
    resolvers.put(THIS_QUARTER_START, () -> quarterBoundary(0, true));
    resolvers.put(THIS_QUARTER_END, () -> quarterBoundary(0, false));
    resolvers.put(NEXT_QUARTER_START, () -> quarterBoundary(1, true));
    resolvers.put(NEXT_QUARTER_END, () -> quarterBoundary(1, false));

    resolvers.put(PREV_YEAR_START, () -> yearBoundary(-1, true));
    resolvers.put(PREV_YEAR_END, () -> yearBoundary(-1, false));
    resolvers.put(THIS_YEAR_START, () -> yearBoundary(0, true));
    resolvers.put(THIS_YEAR_END, () -> yearBoundary(0, false));
    resolvers.put(NEXT_YEAR_START, () -> yearBoundary(1, true));
    resolvers.put(NEXT_YEAR_END, () -> yearBoundary(1, false));

    resolvers.put(COPYNR, () -> (long) getCopy());
    resolvers.put(PIPELINE_NAME, () -> getPipelineMeta().getName());
    resolvers.put(FILENAME, () -> getPipelineMeta().getFilename());
    resolvers.put(MODIFIED_USER, () -> getPipelineMeta().getModifiedUser());
    resolvers.put(MODIFIED_DATE, () -> getPipelineMeta().getModifiedDate());
    resolvers.put(HOSTNAME_REAL, Const::getHostnameReal);
    resolvers.put(HOSTNAME, Const::getHostname);
    resolvers.put(IP_ADDRESS, this::safeIpAddress);
    resolvers.put(CURRENT_PID, Management::getPID);

    // init jvm
    initResolversJvm();
    // init resolver result
    initResolversResult();
  }

  private void initResolversJvm() {
    resolvers.put(JVM_TOTAL_MEMORY, () -> Runtime.getRuntime().totalMemory());
    resolvers.put(JVM_FREE_MEMORY, () -> Runtime.getRuntime().freeMemory());
    resolvers.put(JVM_MAX_MEMORY, () -> Runtime.getRuntime().maxMemory());
    resolvers.put(JVM_AVAILABLE_MEMORY, this::jvmAvailableMemory);
    resolvers.put(AVAILABLE_PROCESSORS, () -> (long) Runtime.getRuntime().availableProcessors());
    resolvers.put(JVM_CPU_TIME, () -> Management.getJVMCpuTime() / 1000000);
    resolvers.put(TOTAL_PHYSICAL_MEMORY_SIZE, Management::getTotalPhysicalMemorySize);
    resolvers.put(TOTAL_SWAP_SPACE_SIZE, Management::getTotalSwapSpaceSize);
    resolvers.put(COMMITTED_VIRTUAL_MEMORY_SIZE, Management::getCommittedVirtualMemorySize);
    resolvers.put(FREE_PHYSICAL_MEMORY_SIZE, Management::getFreePhysicalMemorySize);
    resolvers.put(FREE_SWAP_SPACE_SIZE, Management::getFreeSwapSpaceSize);
  }

  private void initResolversResult() {
    resolvers.put(
        PREVIOUS_RESULT_RESULT, () -> previousResult() != null && previousResult().isResult());
    resolvers.put(PREVIOUS_RESULT_EXIT_STATUS, fromPreResult(Result::getExitStatus, 0L));
    resolvers.put(PREVIOUS_RESULT_ENTRY_NR, fromPreResult(Result::getEntryNr, 0L));
    resolvers.put(PREVIOUS_RESULT_NR_FILES, fromPreResult(r -> r.getResultFiles().size(), 0L));
    resolvers.put(
        PREVIOUS_RESULT_NR_FILES_RETRIEVED, fromPreResult(Result::getNrFilesRetrieved, 0L));
    resolvers.put(PREVIOUS_RESULT_NR_LINES_DELETED, fromPreResult(Result::getNrLinesDeleted, 0L));
    resolvers.put(PREVIOUS_RESULT_NR_LINES_INPUT, fromPreResult(Result::getNrLinesInput, 0L));
    resolvers.put(PREVIOUS_RESULT_NR_LINES_OUTPUT, fromPreResult(Result::getNrLinesOutput, 0L));
    resolvers.put(PREVIOUS_RESULT_NR_LINES_READ, fromPreResult(Result::getNrLinesRead, 0L));
    resolvers.put(PREVIOUS_RESULT_NR_LINES_REJECTED, fromPreResult(Result::getNrLinesRejected, 0L));
    resolvers.put(PREVIOUS_RESULT_NR_LINES_UPDATED, fromPreResult(Result::getNrLinesUpdated, 0L));
    resolvers.put(PREVIOUS_RESULT_NR_LINES_WRITTEN, fromPreResult(Result::getNrLinesWritten, 0L));
    resolvers.put(PREVIOUS_RESULT_NR_ROWS, fromPreResult(r -> r.getRows().size(), 0L));
    resolvers.put(
        PREVIOUS_RESULT_IS_STOPPED, () -> previousResult() != null && previousResult().isStopped());
    resolvers.put(PREVIOUS_RESULT_NR_ERRORS, fromPreResult(Result::getNrErrors, 0L));
    resolvers.put(PREVIOUS_RESULT_LOG_TEXT, fromPreResult(Result::getLogText, null));
  }

  /**
   * Creates a resolver that extracts a value from the previous execution result.
   *
   * <p>If the previous result is {@code null}, the provided default value is returned.
   *
   * @param func function to extract value from {@link Result}
   * @param defaultValue fallback value when result is not available
   * @param <T> return type
   * @return a supplier that safely resolves the value
   */
  private <T> ThrowingSupplier<T> fromPreResult(Function<Result, T> func, T defaultValue) {
    return () -> {
      Result r = previousResult();
      return r == null ? defaultValue : func.apply(r);
    };
  }

  /**
   * Resolves the value for a given system data type.
   *
   * @param type the system data type
   * @return the resolved value, or {@code null} if no resolver exists
   * @throws HopException if resolution fails
   */
  private Object resolveFieldValue(SystemDataType type) throws HopException {
    ThrowingSupplier<Object> supplier = resolvers.get(type);
    return supplier == null ? null : supplier.get();
  }

  private Date pipelineDateFrom() throws HopException {
    return calculateStartRange(
        getPipeline().getPipelineRunConfiguration().getExecutionInfoLocationName(),
        ExecutionType.Pipeline,
        getPipeline().getPipelineMeta().getName());
  }

  private Date workflowDateFrom() throws HopException {
    if (getPipeline().getParentWorkflow() == null) {
      return null;
    }
    return calculateStartRange(
        getPipeline()
            .getParentWorkflow()
            .getWorkflowRunConfiguration()
            .getExecutionInfoLocationName(),
        ExecutionType.Workflow,
        getPipeline().getParentWorkflow().getWorkflowMeta().getName());
  }

  private Date workflowDateTo() {
    return getPipeline().getParentWorkflow() == null
        ? null
        : getPipeline().getParentWorkflow().getExecutionStartDate();
  }

  /**
   * Computes the start or end of a day relative to the current date.
   *
   * @param dayOffset offset from current day (e.g., -1 = previous day)
   * @param start {@code true} for start of day, {@code false} for end of day
   * @return calculated date
   */
  private Date dayBoundary(int dayOffset, boolean start) {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DAY_OF_MONTH, dayOffset);
    setDayTime(cal, start);
    return cal.getTime();
  }

  private Date monthBoundary(int monthOffset, boolean start) {
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MONTH, monthOffset);
    cal.set(Calendar.DAY_OF_MONTH, start ? 1 : cal.getActualMaximum(Calendar.DAY_OF_MONTH));
    setDayTime(cal, start);
    return cal.getTime();
  }

  private Date weekBoundary(int weekOffset, Locale locale, boolean start, boolean openEnd) {
    Calendar cal =
        locale == null || Locale.getDefault().equals(locale)
            ? Calendar.getInstance()
            : Calendar.getInstance(locale);
    if (start) {
      cal.add(Calendar.WEEK_OF_YEAR, weekOffset);
      cal.set(Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek());
      setDayTime(cal, true);
    } else {
      cal.add(Calendar.WEEK_OF_YEAR, weekOffset + 1);
      cal.set(Calendar.DAY_OF_WEEK, cal.getFirstDayOfWeek());
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MILLISECOND, -1);
      if (openEnd) {
        cal.add(Calendar.DAY_OF_WEEK, -1);
      }
    }
    return cal.getTime();
  }

  private Date quarterBoundary(int quarterOffset, boolean start) {
    Calendar cal = Calendar.getInstance();
    int monthShift =
        start
            ? (quarterOffset * 3) - (cal.get(Calendar.MONTH) % 3)
            : (quarterOffset * 3 + 2) - (cal.get(Calendar.MONTH) % 3);
    cal.add(Calendar.MONTH, monthShift);
    cal.set(Calendar.DAY_OF_MONTH, start ? 1 : cal.getActualMaximum(Calendar.DATE));
    setDayTime(cal, start);
    return cal.getTime();
  }

  private Date yearBoundary(int yearOffset, boolean start) {
    Calendar cal = Calendar.getInstance();
    if (start) {
      cal.add(Calendar.YEAR, yearOffset);
      cal.set(Calendar.DAY_OF_YEAR, cal.getActualMinimum(Calendar.DATE));
      setDayTime(cal, true);
    } else {
      cal.add(Calendar.YEAR, yearOffset + 1);
      cal.set(Calendar.DAY_OF_YEAR, cal.getActualMinimum(Calendar.DATE));
      cal.add(Calendar.DAY_OF_YEAR, -1);
      setDayTime(cal, false);
    }
    return cal.getTime();
  }

  private void setDayTime(Calendar cal, boolean start) {
    cal.set(Calendar.HOUR_OF_DAY, start ? 0 : 23);
    cal.set(Calendar.MINUTE, start ? 0 : 59);
    cal.set(Calendar.SECOND, start ? 0 : 59);
    cal.set(Calendar.MILLISECOND, start ? 0 : 999);
  }

  private Object safeIpAddress() throws HopException {
    try {
      return Const.getIPAddress();
    } catch (Exception e) {
      throw new HopException(e);
    }
  }

  private long jvmAvailableMemory() {
    Runtime rt = Runtime.getRuntime();
    return rt.freeMemory() + (rt.maxMemory() - rt.totalMemory());
  }

  private Result previousResult() {
    return getPipeline().getPreviousResult();
  }

  /** Calculate the start of the data range for a pipeline. */
  private Date calculateStartRange(String locationName, ExecutionType executionType, String name)
      throws HopException {
    ExecutionInfoLocation location = loadLocation(metadataProvider, locationName);
    if (location == null) {
      // Nothing to look up!
      //
      return null;
    }
    IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();

    try {
      iLocation.initialize(this, metadataProvider);

      // Look up the previous successful execution of a pipeline with the given name
      //
      Execution execution = iLocation.findPreviousSuccessfulExecution(executionType, name);
      if (execution == null) {
        // We can go back millions of years but that would probably confuse a lot of 3rd party
        // systems.
        //
        return new GregorianCalendar(1900, Calendar.JANUARY, 1).getTime();
      } else {
        return execution.getExecutionStartDate();
      }
    } finally {
      iLocation.close();
    }
  }

  private ExecutionInfoLocation loadLocation(
      IHopMetadataProvider metadataProvider, String locationName) throws HopException {
    return metadataProvider.getSerializer(ExecutionInfoLocation.class).load(resolve(locationName));
  }

  /**
   * Functional interface similar to {@link java.util.function.Supplier}, but allows throwing {@link
   * HopException}.
   *
   * @param <T> supplied value type
   */
  @FunctionalInterface
  private interface ThrowingSupplier<T> {
    T get() throws HopException;
  }
}
