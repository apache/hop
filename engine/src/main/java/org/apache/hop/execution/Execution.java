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

import org.apache.hop.core.logging.LogLevel;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * This is all the static information we have about an execution. It contains no status information
 * as such.
 */
public class Execution {

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
  private Map<String, String> variableValues;

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
   * Gets filename
   *
   * @return value of filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * Sets filename
   *
   * @param filename value of filename
   */
  public void setFilename(String filename) {
    this.filename = filename;
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
   * Gets executorXml
   *
   * @return value of executorXml
   */
  public String getExecutorXml() {
    return executorXml;
  }

  /**
   * Sets executorXml
   *
   * @param executorXml value of executorXml
   */
  public void setExecutorXml(String executorXml) {
    this.executorXml = executorXml;
  }

  /**
   * Gets metadataJson
   *
   * @return value of metadataJson
   */
  public String getMetadataJson() {
    return metadataJson;
  }

  /**
   * Sets metadataJson
   *
   * @param metadataJson value of metadataJson
   */
  public void setMetadataJson(String metadataJson) {
    this.metadataJson = metadataJson;
  }

  /**
   * Gets variableValues
   *
   * @return value of variableValues
   */
  public Map<String, String> getVariableValues() {
    return variableValues;
  }

  /**
   * Sets variableValues
   *
   * @param variableValues value of variableValues
   */
  public void setVariableValues(Map<String, String> variableValues) {
    this.variableValues = variableValues;
  }

  /**
   * Gets runConfigurationName
   *
   * @return value of runConfigurationName
   */
  public String getRunConfigurationName() {
    return runConfigurationName;
  }

  /**
   * Sets runConfigurationName
   *
   * @param runConfigurationName value of runConfigurationName
   */
  public void setRunConfigurationName(String runConfigurationName) {
    this.runConfigurationName = runConfigurationName;
  }

  /**
   * Gets logLevel
   *
   * @return value of logLevel
   */
  public LogLevel getLogLevel() {
    return logLevel;
  }

  /**
   * Sets logLevel
   *
   * @param logLevel value of logLevel
   */
  public void setLogLevel(LogLevel logLevel) {
    this.logLevel = logLevel;
  }

  /**
   * Gets parameterValues
   *
   * @return value of parameterValues
   */
  public Map<String, String> getParameterValues() {
    return parameterValues;
  }

  /**
   * Sets parameterValues
   *
   * @param parameterValues value of parameterValues
   */
  public void setParameterValues(Map<String, String> parameterValues) {
    this.parameterValues = parameterValues;
  }

  /**
   * Gets environmentDetails
   *
   * @return value of environmentDetails
   */
  public Map<String, String> getEnvironmentDetails() {
    return environmentDetails;
  }

  /**
   * Sets environmentDetails
   *
   * @param environmentDetails value of environmentDetails
   */
  public void setEnvironmentDetails(Map<String, String> environmentDetails) {
    this.environmentDetails = environmentDetails;
  }

  /**
   * Gets registrationDate
   *
   * @return value of registrationDate
   */
  public Date getRegistrationDate() {
    return registrationDate;
  }

  /**
   * Sets registrationDate
   *
   * @param registrationDate value of registrationDate
   */
  public void setRegistrationDate(Date registrationDate) {
    this.registrationDate = registrationDate;
  }

  /**
   * Gets executionStartDate
   *
   * @return value of executionStartDate
   */
  public Date getExecutionStartDate() {
    return executionStartDate;
  }

  /**
   * Sets executionStartDate
   *
   * @param executionStartDate value of executionStartDate
   */
  public void setExecutionStartDate(Date executionStartDate) {
    this.executionStartDate = executionStartDate;
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
}
