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

package org.apache.hop.rest.v1.resources.execute;

import java.util.HashMap;
import java.util.Map;

public class SyncRequest {
  private String service;
  private String runConfig;
  private Map<String, String> variables;
  private String bodyContent;

  public SyncRequest() {
    variables = new HashMap<>();
  }

  /**
   * Gets service
   *
   * @return value of service
   */
  public String getService() {
    return service;
  }

  /**
   * Sets service
   *
   * @param service value of service
   */
  public void setService(String service) {
    this.service = service;
  }

  /**
   * Gets runConfig
   *
   * @return value of runConfig
   */
  public String getRunConfig() {
    return runConfig;
  }

  /**
   * Sets runConfig
   *
   * @param runConfig value of runConfig
   */
  public void setRunConfig(String runConfig) {
    this.runConfig = runConfig;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public Map<String, String> getVariables() {
    return variables;
  }

  /**
   * Sets variables
   *
   * @param variables value of variables
   */
  public void setVariables(Map<String, String> variables) {
    this.variables = variables;
  }

  /**
   * Gets bodyContent
   *
   * @return value of bodyContent
   */
  public String getBodyContent() {
    return bodyContent;
  }

  /**
   * Sets bodyContent
   *
   * @param bodyContent value of bodyContent
   */
  public void setBodyContent(String bodyContent) {
    this.bodyContent = bodyContent;
  }
}
