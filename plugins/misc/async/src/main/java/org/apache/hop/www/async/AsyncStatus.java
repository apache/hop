/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.www.async;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.hop.www.HopServerPipelineStatus;

import java.util.*;

public class AsyncStatus {
  private String service;
  private String id;

  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  private Date logDate;

  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  private Date startDate;

  @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
  private Date endDate;

  private String statusDescription;
  private Map<String, String> statusVariables;
  private List<HopServerPipelineStatus> pipelineStatuses;

  public AsyncStatus() {
    logDate = new Date();
    statusVariables = new HashMap<>();
    pipelineStatuses = new ArrayList<>();
  }

  /**
   * Gets service
   *
   * @return value of service
   */
  public String getService() {
    return service;
  }

  /** @param service The service to set */
  public void setService(String service) {
    this.service = service;
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /** @param id The id to set */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Gets logDate
   *
   * @return value of logDate
   */
  public Date getLogDate() {
    return logDate;
  }

  /** @param logDate The logDate to set */
  public void setLogDate(Date logDate) {
    this.logDate = logDate;
  }

  /**
   * Gets startDate
   *
   * @return value of startDate
   */
  public Date getStartDate() {
    return startDate;
  }

  /** @param startDate The startDate to set */
  public void setStartDate(Date startDate) {
    this.startDate = startDate;
  }

  /**
   * Gets endDate
   *
   * @return value of endDate
   */
  public Date getEndDate() {
    return endDate;
  }

  /** @param endDate The endDate to set */
  public void setEndDate(Date endDate) {
    this.endDate = endDate;
  }

  /**
   * Gets statusDescription
   *
   * @return value of statusDescription
   */
  public String getStatusDescription() {
    return statusDescription;
  }

  /** @param statusDescription The statusDescription to set */
  public void setStatusDescription(String statusDescription) {
    this.statusDescription = statusDescription;
  }

  /**
   * Gets statusVariables
   *
   * @return value of statusVariables
   */
  public Map<String, String> getStatusVariables() {
    return statusVariables;
  }

  /** @param statusVariables The statusVariables to set */
  public void setStatusVariables(Map<String, String> statusVariables) {
    this.statusVariables = statusVariables;
  }

  /**
   * Gets pipelineStatuses
   *
   * @return value of pipelineStatuses
   */
  public List<HopServerPipelineStatus> getPipelineStatuses() {
    return pipelineStatuses;
  }

  /** @param pipelineStatuses The pipelineStatuses to set */
  public void setPipelineStatuses(List<HopServerPipelineStatus> pipelineStatuses) {
    this.pipelineStatuses = pipelineStatuses;
  }
}
