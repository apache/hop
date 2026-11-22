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

package org.apache.hop.airflow.client;

/**
 * A DAG run as the Airflow REST API reports it.
 *
 * @param dagId the DAG
 * @param dagRunId the run, unique within the DAG
 * @param state {@code queued}, {@code running}, {@code success} or {@code failed}
 */
public record AirflowDagRun(String dagId, String dagRunId, String state) {

  public static final String STATE_SUCCESS = "success";
  public static final String STATE_FAILED = "failed";

  public boolean isSuccess() {
    return STATE_SUCCESS.equalsIgnoreCase(state);
  }

  public boolean isFailed() {
    return STATE_FAILED.equalsIgnoreCase(state);
  }

  public boolean isFinished() {
    return isSuccess() || isFailed();
  }
}
