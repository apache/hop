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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.logging.LogLevel;

/**
 * This is all the static information we have about an execution. It contains no status information
 * as such.
 */
@Setter
@Getter
public class Execution {

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

  /** The filename that is executing */
  private String filename;

  /** The unique ID of a pipeline or workflow that is about to get executed */
  private String id;

  /** The unique ID of the parent executor (pipeline or workflow) */
  private String parentId;

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
  private Map<String, String> parameterValues;

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
}
