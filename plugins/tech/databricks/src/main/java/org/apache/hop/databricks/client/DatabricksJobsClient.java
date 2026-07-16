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

package org.apache.hop.databricks.client;

import java.util.Map;
import org.apache.hop.core.exception.HopException;

/**
 * Minimal Databricks Jobs API surface used by Hop. Implementations may use the official SDK or
 * REST.
 */
public interface DatabricksJobsClient extends AutoCloseable {

  /**
   * Verify credentials against the workspace. Returns a short identity string for UI (user name or
   * host).
   */
  String testConnection() throws HopException;

  /** Trigger a run of an existing job; returns the new run id. */
  long runNow(long jobId, Map<String, String> notebookOrJarParams) throws HopException;

  /** One-time submit (runs/submit). Body is raw Jobs API JSON. Returns run id. */
  long submitRun(String submitRunJsonBody) throws HopException;

  /** Create a job from raw Jobs API create JSON. Returns job id. */
  long createJob(String createJobJsonBody) throws HopException;

  /** Reset/update a job (jobs/reset) with raw JSON including job_id. */
  void resetJob(String resetJobJsonBody) throws HopException;

  DatabricksRunStatus getRun(long runId) throws HopException;

  void cancelRun(long runId) throws HopException;

  @Override
  void close();
}
