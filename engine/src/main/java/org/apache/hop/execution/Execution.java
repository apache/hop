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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.logging.LogLevel;

/**
 * This is all the static information we have about an execution. It contains no status information
 * as such.
 */
@Setter
@Getter
public class Execution {

  /**
   * Set when a project is enabled. Empty means execution information is not filtered by project.
   * The projects plugin writes the same name ({@code HOP_PROJECT_ID}).
   */
  public static final String VARIABLE_HOP_PROJECT_ID = "HOP_PROJECT_ID";

  @SuppressWarnings("java:S115")
  public enum EnvironmentDetailType {
    ContainerId,
    MaxMemory,
    FreeMemory,
    TotalMemory,
    AvailableProcessors,
    JavaVersion,
    JavaUser,
    HostName,
    HostAddress,
  }

  /** The name of the pipeline or workflow execution */
  private String name;

  /**
   * Project that registered this execution. Omitted from JSON when empty so a 2.19 reader, which
   * rejects unknown properties, can still open the document.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private String projectId;

  /** The filename that is executing */
  private String filename;

  /** The unique ID of a pipeline or workflow that is about to get executed */
  private String id;

  /**
   * The pipeline or workflow execution that owns this one. When an action or transform starts it,
   * this is that workflow or pipeline, not the action or transform log channel id.
   */
  private String parentId;

  /**
   * Log channel id of the action or transform that started this pipeline or workflow. Empty when
   * this execution was not started by one. Omitted when empty so a reader that rejects unknown
   * properties can still open a document that has no launcher.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private String launchedById;

  /**
   * Pipeline and workflow execution ids started by this action or transform. Written when the child
   * is registered so a refresh does not have to search for them.
   */
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private List<String> childExecutionIds;

  /** The type of execution to register: Pipeline or Workflow only */
  private ExecutionType executionType;

  /** Serialized form of the executing pipeline or workflow (XML) */
  private String executorXml;

  /** Serialized version of the metadata at the time of execution (JSON) */
  private String metadataJson;

  /**
   * This map contains all the variables and their variables as they are set in the executor at
   * registration time.
   */
  @Deprecated @JsonIgnore private Map<String, String> variableValues;

  /** The name of the run configuration used as referenced in the serialized metadata. */
  private String runConfigurationName;

  /** The log level used to execute */
  private LogLevel logLevel;

  /** The parameters used to execute */
  @JsonIgnore private Map<String, String> parameterValues;

  /** Details about the environment */
  private Map<String, String> environmentDetails;

  /** The time of registration */
  private Date registrationDate;

  /** The start time of the execution */
  private Date executionStartDate;

  /** The copy number for an executor transform running in multiple copies */
  private String copyNr;

  public Execution() {
    this.variableValues = new HashMap<>();
    this.parameterValues = new HashMap<>();
    this.environmentDetails = new HashMap<>();
  }

  /**
   * @return true when {@code childId} was not already recorded
   */
  public boolean addChildExecutionId(String childId) {
    if (StringUtils.isEmpty(childId)) {
      return false;
    }
    if (childExecutionIds == null) {
      childExecutionIds = new ArrayList<>();
    }
    if (childExecutionIds.contains(childId)) {
      return false;
    }
    childExecutionIds.add(childId);
    return true;
  }

  /**
   * Keep every child id from {@code other}. Two location instances can persist the same action, and
   * the later write must not drop ids the other one recorded.
   */
  public void unionChildExecutionIds(Execution other) {
    if (other == null || other.childExecutionIds == null) {
      return;
    }
    for (String childId : other.childExecutionIds) {
      addChildExecutionId(childId);
    }
  }
}
