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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Execution states are possible for pipelines and workflows but also for actions and transforms.
 */
public class ExecutionState {

  /** The type of execution to update */
  private ExecutionType executionType;
  /**
   * The unique ID of the parent executor. If this is an action or transform update, this is the ID
   * of that executor.
   */
  private String parentId;

  /** The unique ID of the executor. If this is an action or transform update, this can be null. */
  private String id;

  /** The name of the executor */
  private String name;

  /** The copy number of the transform update or null */
  private String copyNr;

  /** The log lines for this particular executor */
  private String loggingText;

  /** The last log line number to allow for incremental logging updates */
  private Integer lastLogLineNr;

  /** The metrics of components */
  private List<ExecutionStateComponentMetrics> metrics;

  /** The status description */
  private String statusDescription;

  /** The time of update */
  private Date updateTime;

  /** The list of immediate child IDs for this execution */
  private List<String> childIds;

  /** Extra details that a plugin might want to save in the state */
  private Map<String, String> details;

  /** Indicates whether or not the execution failed */
  private boolean failed;

  /**
   * The ID of the container where we are executing in. Usually this is the Object ID found on the
   * Hop server.
   */
  private String containerId;

  public ExecutionState() {
    this.metrics = new ArrayList<>();
    this.updateTime = new Date();
    this.childIds = new ArrayList<>();
    this.details = new HashMap<>();
  }

  /**
   * Gets executionType
   *
   * @return value of executionType
   */
  public ExecutionType getExecutionType() {
    return executionType;
  }

  /**
   * Sets executionType
   *
   * @param executionType value of executionType
   */
  public void setExecutionType(ExecutionType executionType) {
    this.executionType = executionType;
  }

  /**
   * Gets parentId
   *
   * @return value of parentId
   */
  public String getParentId() {
    return parentId;
  }

  /**
   * Sets parentId
   *
   * @param parentId value of parentId
   */
  public void setParentId(String parentId) {
    this.parentId = parentId;
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /**
   * Sets id
   *
   * @param id value of id
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * Sets name
   *
   * @param name value of name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets copyNr
   *
   * @return value of copyNr
   */
  public String getCopyNr() {
    return copyNr;
  }

  /**
   * Sets copyNr
   *
   * @param copyNr value of copyNr
   */
  public void setCopyNr(String copyNr) {
    this.copyNr = copyNr;
  }

  /**
   * Gets loggingText
   *
   * @return value of loggingText
   */
  public String getLoggingText() {
    return loggingText;
  }

  /**
   * Sets loggingText
   *
   * @param loggingText value of loggingText
   */
  public void setLoggingText(String loggingText) {
    this.loggingText = loggingText;
  }

  /**
   * Gets lastLogLineNr
   *
   * @return value of lastLogLineNr
   */
  public Integer getLastLogLineNr() {
    return lastLogLineNr;
  }

  /**
   * Sets lastLogLineNr
   *
   * @param lastLogLineNr value of lastLogLineNr
   */
  public void setLastLogLineNr(Integer lastLogLineNr) {
    this.lastLogLineNr = lastLogLineNr;
  }

  /**
   * Gets metrics
   *
   * @return value of metrics
   */
  public List<ExecutionStateComponentMetrics> getMetrics() {
    return metrics;
  }

  /**
   * Sets metrics
   *
   * @param metrics value of metrics
   */
  public void setMetrics(List<ExecutionStateComponentMetrics> metrics) {
    this.metrics = metrics;
  }

  /**
   * Gets statusDescription
   *
   * @return value of statusDescription
   */
  public String getStatusDescription() {
    return statusDescription;
  }

  /**
   * Sets statusDescription
   *
   * @param statusDescription value of statusDescription
   */
  public void setStatusDescription(String statusDescription) {
    this.statusDescription = statusDescription;
  }

  /**
   * Gets updateTime
   *
   * @return value of updateTime
   */
  public Date getUpdateTime() {
    return updateTime;
  }

  /**
   * Sets updateTime
   *
   * @param updateTime value of updateTime
   */
  public void setUpdateTime(Date updateTime) {
    this.updateTime = updateTime;
  }

  /**
   * Gets childIds
   *
   * @return value of childIds
   */
  public List<String> getChildIds() {
    return childIds;
  }

  /**
   * Sets childIds
   *
   * @param childIds value of childIds
   */
  public void setChildIds(List<String> childIds) {
    this.childIds = childIds;
  }

  /**
   * Gets failed
   *
   * @return value of failed
   */
  public boolean isFailed() {
    return failed;
  }

  /**
   * Sets failed
   *
   * @param failed value of failed
   */
  public void setFailed(boolean failed) {
    this.failed = failed;
  }

  /**
   * Gets details
   *
   * @return value of details
   */
  public Map<String, String> getDetails() {
    return details;
  }

  /**
   * Sets details
   *
   * @param details value of details
   */
  public void setDetails(Map<String, String> details) {
    this.details = details;
  }

  /**
   * Gets containerId
   *
   * @return value of containerId
   */
  public String getContainerId() {
    return containerId;
  }

  /**
   * Sets containerId
   *
   * @param containerId value of containerId
   */
  public void setContainerId(String containerId) {
    this.containerId = containerId;
  }
}
