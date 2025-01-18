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
import lombok.Getter;
import lombok.Setter;

/**
 * Execution states are possible for pipelines and workflows but also for actions and transforms.
 */
@Getter
@Setter
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

  /** Indicates whether the execution failed */
  private boolean failed;

  /** If the execution finished, what was the end date? */
  private Date executionEndDate;

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
}
