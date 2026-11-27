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
 */

package org.apache.hop.airflow.engine;

import org.apache.hop.airflow.launcher.HopAirflowBundle;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/**
 * Which tasks the DAG of a workflow has. The code is the value of {@link
 * HopAirflowBundle#ATTRIBUTE_TASKS} in the stub.
 */
public enum AirflowDagTasks implements IEnumHasCodeAndDescription {
  /**
   * One task per action, which Airflow shows, retries and clears on its own. A workflow whose hops
   * don't fit Airflow, such as a loop, still runs as one task.
   */
  PER_ACTION,
  /**
   * One task that runs the whole workflow: Hop starts once, and the actions run on one worker, so
   * they can pass local files and JVM-scope variables to each other.
   */
  WHOLE_WORKFLOW;

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(AirflowDagTasks.class, "AirflowDagTasks." + name());
  }

  /**
   * The tasks for a code or a description, ignoring case.
   *
   * @return the tasks, or null for a blank value
   * @throws IllegalArgumentException for a value that is neither
   */
  public static AirflowDagTasks lookup(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    for (AirflowDagTasks tasks : values()) {
      if (tasks.getCode().equalsIgnoreCase(value.trim())
          || tasks.getDescription().equalsIgnoreCase(value.trim())) {
        return tasks;
      }
    }
    throw new IllegalArgumentException(
        "Unknown DAG tasks '" + value + "': use " + PER_ACTION + " or " + WHOLE_WORKFLOW);
  }
}
