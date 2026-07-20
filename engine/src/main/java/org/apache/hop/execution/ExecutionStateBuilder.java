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
 *
 */

package org.apache.hop.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineMetrics;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IEngineMetric;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

public final class ExecutionStateBuilder {

  /** Metric map key: component duration in milliseconds. */
  public static final String METRIC_HEADER_DURATION = "Duration";

  /** Metric map key: component throughput in rows per second. */
  public static final String METRIC_HEADER_SPEED = "Speed";

  private ExecutionType executionType;
  private Date updateTime;
  private String statusDescription;
  private String parentId;
  private String id;
  private String name;
  private String copyNr;
  private String loggingText;
  private Integer lastLogLineNr;
  private List<ExecutionStateComponentMetrics> metrics;
  private List<String> childIds;
  private boolean failed;
  private Map<String, String> details;
  private String containerId;
  private Date executionEndDate;

  private ExecutionStateBuilder() {
    this.updateTime = new Date();
    this.metrics = new ArrayList<>();
    this.childIds = new ArrayList<>();
    this.details = new HashMap<>();
  }

  public static ExecutionStateBuilder of() {
    return new ExecutionStateBuilder();
  }

  private static String getLoggingText(String logChannelId, Integer lastLogLineNr) {
    StringBuffer loggingTextBuffer;
    if (lastLogLineNr != null) {
      loggingTextBuffer = HopLogStore.getAppender().getBuffer(logChannelId, false, lastLogLineNr);
    } else {
      loggingTextBuffer = HopLogStore.getAppender().getBuffer(logChannelId, false);
    }
    return loggingTextBuffer.toString();
  }

  public static ExecutionStateBuilder fromExecutor(
      IPipelineEngine<PipelineMeta> pipeline, Integer lastLogLineNr) {
    String parentLogChannelId =
        pipeline.getParent() == null ? null : pipeline.getParent().getLogChannelId();

    // The last log line nr for this pipeline?
    // Look it up before querying the store to avoid missing lines
    //
    int lastNrInLogStore = HopLogStore.getLastBufferLineNr();

    ExecutionStateBuilder builder =
        of().withExecutionType(ExecutionType.Pipeline)
            .withParentId(parentLogChannelId)
            .withId(pipeline.getLogChannelId())
            .withName(pipeline.getPipelineMeta().getName())
            .withLoggingText(getLoggingText(pipeline.getLogChannelId(), lastLogLineNr))
            .withLastLogLineNr(lastNrInLogStore)
            .withFailed(pipeline.getErrors() > 0)
            .withStatusDescription(pipeline.getStatusDescription())
            .withChildIds(
                LoggingRegistry.getInstance().getChildrenMap().get(pipeline.getLogChannelId()))
            .withContainerId(pipeline.getContainerId())
            .withExecutionEndDate(pipeline.getExecutionEndDate());

    EngineMetrics engineMetrics = pipeline.getEngineMetrics();

    if (engineMetrics != null) {
      // See if we have any component metrics...
      //
      for (IEngineComponent component : pipeline.getComponents()) {
        ExecutionStateComponentMetrics componentMetrics =
            new ExecutionStateComponentMetrics(
                component.getName(), Integer.toString(component.getCopyNr()));

        addMetric(componentMetrics, engineMetrics, component, Pipeline.METRIC_INIT);
        addMetric(componentMetrics, engineMetrics, component, Pipeline.METRIC_INPUT);
        addMetric(componentMetrics, engineMetrics, component, Pipeline.METRIC_OUTPUT);
        addMetric(componentMetrics, engineMetrics, component, Pipeline.METRIC_READ);
        addMetric(componentMetrics, engineMetrics, component, Pipeline.METRIC_WRITTEN);
        addMetric(componentMetrics, engineMetrics, component, Pipeline.METRIC_ERROR);
        addMetric(componentMetrics, engineMetrics, component, Pipeline.METRIC_REJECTED);
        addMetric(componentMetrics, engineMetrics, component, Pipeline.METRIC_UPDATED);
        addMetric(componentMetrics, engineMetrics, component, Pipeline.METRIC_BUFFER_IN);
        addMetric(componentMetrics, engineMetrics, component, Pipeline.METRIC_BUFFER_OUT);
        addMetric(componentMetrics, engineMetrics, component, Pipeline.METRIC_DATA_VOLUME);
        addMetric(componentMetrics, engineMetrics, component, Pipeline.METRIC_DATA_VOLUME_IN);
        addMetric(componentMetrics, engineMetrics, component, Pipeline.METRIC_DATA_VOLUME_OUT);

        // Duration (ms) and speed (rows/s) for the Metrics tab in the execution perspective.
        //
        long durationMs = resolveDurationMs(component);
        if (durationMs > 0) {
          componentMetrics.getMetrics().put(METRIC_HEADER_DURATION, durationMs);
        }
        Long speed = resolveSpeedRowsPerSecond(component, engineMetrics, durationMs);
        if (speed != null) {
          componentMetrics.getMetrics().put(METRIC_HEADER_SPEED, speed);
        }

        builder.addMetrics(componentMetrics);
      }
    }

    return builder;
  }

  /**
   * Resolves component duration in milliseconds. Prefers {@link
   * IEngineComponent#getExecutionDuration()}; falls back to first/last row timestamps when duration
   * is zero (same idea as the live pipeline metrics grid).
   */
  static long resolveDurationMs(IEngineComponent component) {
    long durationMs = component.getExecutionDuration();
    if (durationMs > 0) {
      return durationMs;
    }
    Date first = component.getFirstRowReadDate();
    if (first == null) {
      return 0;
    }
    Date last = component.getLastRowWrittenDate();
    if (last != null) {
      return Math.max(0, last.getTime() - first.getTime());
    }
    return Math.max(0, System.currentTimeMillis() - first.getTime());
  }

  /**
   * Resolves throughput in rows per second. Prefers the engine speed map when a numeric value can
   * be parsed; otherwise computes from row counts and duration (same formula as {@code
   * TransformStatus}).
   *
   * @return rows/s as a long, or {@code null} when speed is not meaningful
   */
  static Long resolveSpeedRowsPerSecond(
      IEngineComponent component, EngineMetrics engineMetrics, long durationMs) {
    if (engineMetrics != null) {
      String speedText = engineMetrics.getComponentSpeedMap().get(component);
      Long parsed = parseSpeedRowsPerSecond(speedText);
      if (parsed != null) {
        return parsed;
      }
    }
    if (durationMs <= 0) {
      return null;
    }
    long inProc = Math.max(component.getLinesInput(), component.getLinesRead());
    long outProc =
        Math.max(
            component.getLinesOutput() + component.getLinesUpdated(),
            component.getLinesWritten() + component.getLinesRejected());
    double lapsedSec = durationMs / 1000.0;
    if (lapsedSec <= 0) {
      return null;
    }
    double inSpeed = inProc / lapsedSec;
    double outSpeed = outProc / lapsedSec;
    return Math.round(Math.max(inSpeed, outSpeed));
  }

  /**
   * Parses a speed string as produced by the local pipeline engine / {@code TransformStatus} (e.g.
   * {@code " 1,234"}). Returns {@code null} for empty, dash, or non-numeric values.
   */
  static Long parseSpeedRowsPerSecond(String speedText) {
    if (speedText == null) {
      return null;
    }
    String cleaned = speedText.trim().replace(",", "").replace(" ", "");
    if (cleaned.isEmpty() || "-".equals(cleaned)) {
      return null;
    }
    try {
      // TransformStatus may use a decimal; store whole rows/s.
      return Math.round(Double.parseDouble(cleaned));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public static ExecutionStateBuilder fromTransform(
      IPipelineEngine<PipelineMeta> pipeline, IEngineComponent component) {
    return of().withExecutionType(ExecutionType.Transform)
        .withId(component.getLogChannelId())
        .withStatusDescription(component.getStatusDescription())
        .withFailed(component.getErrors() > 0)
        .withName(component.getName())
        .withCopyNr(Integer.toString(component.getCopyNr()))
        .withParentId(pipeline.getLogChannelId())
        .withContainerId(pipeline.getContainerId())
        .withExecutionEndDate(component.getExecutionEndDate());
  }

  private static void addMetric(
      ExecutionStateComponentMetrics componentMetrics,
      EngineMetrics engineMetrics,
      IEngineComponent component,
      IEngineMetric metric) {
    Long value = engineMetrics.getComponentMetric(component, metric);
    if (value != null) {
      componentMetrics.getMetrics().put(metric.getHeader(), value);
    }
  }

  public static ExecutionStateBuilder fromExecutor(
      IWorkflowEngine<WorkflowMeta> workflow, Integer lastLogLineNr) {
    String parentLogChannelId =
        workflow.getParent() == null ? null : workflow.getParent().getLogChannelId();

    // The last log line nr for this workflow?
    // Look it up before querying the store to avoid missing lines
    //
    int lastNrInLogStore = HopLogStore.getLastBufferLineNr();
    Result result = workflow.getResult();

    return of().withExecutionType(ExecutionType.Workflow)
        .withParentId(parentLogChannelId)
        .withId(workflow.getLogChannelId())
        .withName(workflow.getWorkflowMeta().getName())
        .withLoggingText(getLoggingText(workflow.getLogChannelId(), lastLogLineNr))
        .withLastLogLineNr(lastNrInLogStore)
        .withFailed(result != null && !result.isResult())
        .withStatusDescription(workflow.getStatusDescription())
        .withChildIds(
            LoggingRegistry.getInstance().getChildrenMap().get(workflow.getLogChannelId()))
        .withContainerId(workflow.getContainerId())
        .withExecutionEndDate(workflow.getExecutionEndDate());
  }

  public ExecutionStateBuilder withExecutionType(ExecutionType executionType) {
    assert executionType != null : "Execution type can not be null";
    this.executionType = executionType;
    return this;
  }

  public ExecutionStateBuilder withParentId(String parentId) {
    this.parentId = parentId;
    return this;
  }

  public ExecutionStateBuilder withId(String id) {
    this.id = id;
    return this;
  }

  public ExecutionStateBuilder withStatusDescription(String statusDescription) {
    this.statusDescription = statusDescription;
    return this;
  }

  public ExecutionStateBuilder withName(String name) {
    this.name = name;
    return this;
  }

  public ExecutionStateBuilder withCopyNr(String copyNr) {
    this.copyNr = copyNr;
    return this;
  }

  public ExecutionStateBuilder withLoggingText(String loggingText) {
    this.loggingText = loggingText;
    return this;
  }

  public ExecutionStateBuilder withLastLogLineNr(Integer lastLogLineNr) {
    this.lastLogLineNr = lastLogLineNr;
    return this;
  }

  public ExecutionStateBuilder withMetrics(List<ExecutionStateComponentMetrics> metrics) {
    this.metrics = metrics;
    return this;
  }

  public ExecutionStateBuilder addMetrics(ExecutionStateComponentMetrics... metrics) {
    this.metrics.addAll(Arrays.asList(metrics));
    return this;
  }

  public ExecutionStateBuilder withUpdateTime(Date updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  public ExecutionStateBuilder withChildIds(List<String> childIds) {
    this.childIds = childIds;
    return this;
  }

  public ExecutionStateBuilder withFailed(boolean failed) {
    this.failed = failed;
    return this;
  }

  public ExecutionStateBuilder withDetails(Map<String, String> details) {
    this.details = details;
    return this;
  }

  public ExecutionStateBuilder withContainerId(String containerId) {
    this.containerId = containerId;
    return this;
  }

  public ExecutionStateBuilder withExecutionEndDate(Date executionEndDate) {
    this.executionEndDate = executionEndDate;
    return this;
  }

  public ExecutionState build() {
    ExecutionState state = new ExecutionState();
    state.setExecutionType(executionType);
    state.setParentId(parentId);
    state.setId(id);
    state.setName(name);
    state.setCopyNr(copyNr);
    state.setLoggingText(loggingText);
    state.setLastLogLineNr(lastLogLineNr);
    state.setMetrics(metrics);
    state.setUpdateTime(updateTime);
    state.setStatusDescription(statusDescription);
    state.setChildIds(childIds);
    state.setFailed(failed);
    state.setDetails(details);
    state.setContainerId(containerId);
    state.setExecutionEndDate(executionEndDate);
    return state;
  }
}
