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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ExecutionStateComponentMetrics {
  private String componentName;
  private String componentCopy;
  private Map<String, Long> metrics;

  public ExecutionStateComponentMetrics() {
    metrics = Collections.synchronizedMap(new HashMap<>());
  }

  public ExecutionStateComponentMetrics(String componentName, String componentCopy) {
    this();
    this.componentName = componentName;
    this.componentCopy = componentCopy;
  }

  public ExecutionStateComponentMetrics(
      String componentName, String componentCopy, Map<String, Long> metrics) {
    this.componentName = componentName;
    this.componentCopy = componentCopy;
    this.metrics = metrics;
  }

  /**
   * Gets componentName
   *
   * @return value of componentName
   */
  public String getComponentName() {
    return componentName;
  }

  /**
   * Sets componentName
   *
   * @param componentName value of componentName
   */
  public void setComponentName(String componentName) {
    this.componentName = componentName;
  }

  /**
   * Gets componentCopy
   *
   * @return value of componentCopy
   */
  public String getComponentCopy() {
    return componentCopy;
  }

  /**
   * Sets componentCopy
   *
   * @param componentCopy value of componentCopy
   */
  public void setComponentCopy(String componentCopy) {
    this.componentCopy = componentCopy;
  }

  /**
   * Gets metrics
   *
   * @return value of metrics
   */
  public Map<String, Long> getMetrics() {
    return metrics;
  }

  /**
   * Sets metrics
   *
   * @param metrics value of metrics
   */
  public void setMetrics(Map<String, Long> metrics) {
    this.metrics = metrics;
  }
}
