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

import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hop.execution.ExecutionMetricsType.*;

public final class ExecutionStateBuilder {
  private ExecutionType executionType;
  private Date updateTime;
  private String statusDescription;
  private String parentId;
  private String id;
  private String name;
  private Integer copyNr;
  private String loggingText;
  private Integer lastLogLineNr;
  private Map<String, Long> metricsMap;

  private ExecutionStateBuilder() {
    this.updateTime = new Date();
    this.metricsMap = new HashMap<>();
  }

  public static ExecutionStateBuilder anExecutionUpdate() {
    return new ExecutionStateBuilder();
  }

  public static ExecutionStateBuilder fromExecutor(
      IPipelineEngine<PipelineMeta> pipeline, IEngineComponent component, Integer lastLogLineNr) {

    String loggingTextBuffer = getLoggingText(component.getLogChannelId(), lastLogLineNr);

    ExecutionStateBuilder builder =
        anExecutionUpdate()
            .withExecutionType(ExecutionType.Transform)
            .withParentId(pipeline.getLogChannelId())
            .withId(component.getLogChannelId())
            .withCopyNr(component.getCopyNr())
            .withName(component.getName())
            .withLastLogLineNr(lastLogLineNr)
            .withLoggingText(loggingTextBuffer)
            .withStatusDescription(pipeline.getStatusDescription());

    builder.metricsMap.put(Errors.name(), component.getErrors());
    builder.metricsMap.put(Input.name(), component.getLinesInput());
    builder.metricsMap.put(Output.name(), component.getLinesOutput());
    builder.metricsMap.put(Read.name(), component.getLinesRead());
    builder.metricsMap.put(Written.name(), component.getLinesWritten());
    builder.metricsMap.put(Updated.name(), component.getLinesUpdated());
    builder.metricsMap.put(Rejected.name(), component.getLinesRejected());
    builder.metricsMap.put(BufferSizeInput.name(), component.getInputBufferSize());
    builder.metricsMap.put(BufferSizeOutput.name(), component.getOutputBufferSize());

    return builder;
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
        anExecutionUpdate()
            .withExecutionType(ExecutionType.Pipeline)
            .withParentId(parentLogChannelId)
            .withId(pipeline.getLogChannelId())
            .withName(pipeline.getPipelineMeta().getName())
            .withLoggingText(getLoggingText(pipeline.getLogChannelId(), lastLogLineNr))
            .withLastLogLineNr(lastNrInLogStore)
            .withStatusDescription(pipeline.getStatusDescription());
    return builder;
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

  public ExecutionStateBuilder withCopyNr(Integer copyNr) {
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

  public ExecutionStateBuilder withMetricsMap(Map<String, Long> metricsMap) {
    this.metricsMap = metricsMap;
    return this;
  }

  public ExecutionStateBuilder withUpdateTime(Date updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  public ExecutionState build() {
    ExecutionState executionUpdate = new ExecutionState();
    executionUpdate.setExecutionType(executionType);
    executionUpdate.setParentId(parentId);
    executionUpdate.setId(id);
    executionUpdate.setName(name);
    executionUpdate.setCopyNr(copyNr);
    executionUpdate.setLoggingText(loggingText);
    executionUpdate.setLastLogLineNr(lastLogLineNr);
    executionUpdate.setMetricsMap(metricsMap);
    executionUpdate.setUpdateTime(updateTime);
    executionUpdate.setStatusDescription(statusDescription);
    return executionUpdate;
  }
}
