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

package org.apache.hop.run;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Result;
import org.apache.hop.core.failure.ExecutionFailure;
import org.apache.hop.core.failure.FailureCause;

/**
 * What hop-run writes to the file of {@code --result-json}: whether the run succeeded, its counters
 * and, when it failed, why. Experimental: the fields can still change.
 */
@Getter
@Setter
public class HopRunResult {
  public static final int SCHEMA_VERSION = 1;

  public static final String TYPE_PIPELINE = "PIPELINE";
  public static final String TYPE_WORKFLOW = "WORKFLOW";

  private int schemaVersion = SCHEMA_VERSION;

  /** {@link #TYPE_PIPELINE} or {@link #TYPE_WORKFLOW}. */
  private String type;

  /** The file that was run, as given and with variables resolved. */
  private String filename;

  private String name;

  /** The execution id, the same as at the execution information location. Null without a run. */
  private String executionId;

  private String runConfiguration;

  /** Whether the run succeeded: what decides the exit code. */
  private boolean success;

  /** Whether the run was stopped, for example by a termination signal. */
  private boolean stopped;

  private Date startDate;

  private Date endDate;

  /** Row and error counters of the run: errors, read, written, input, output, updated, ... */
  private Map<String, Long> counters = new LinkedHashMap<>();

  /** Why the run failed, null when it did not. */
  private ExecutionFailure failure;

  /** The failures of its transforms or (the last 100) actions, oldest first. */
  private List<ExecutionFailure> failures = new ArrayList<>();

  /**
   * Why hop-run itself failed, when there is no failure of the run to tell: the file could not be
   * loaded, the run configuration does not exist, and so on.
   */
  private FailureCause error;

  void setCountersFrom(Result result) {
    if (result == null) {
      return;
    }
    counters.put("errors", result.getNrErrors());
    counters.put("read", result.getNrLinesRead());
    counters.put("written", result.getNrLinesWritten());
    counters.put("input", result.getNrLinesInput());
    counters.put("output", result.getNrLinesOutput());
    counters.put("updated", result.getNrLinesUpdated());
    counters.put("rejected", result.getNrLinesRejected());
    counters.put("deleted", result.getNrLinesDeleted());
    counters.put("filesRetrieved", result.getNrFilesRetrieved());
    stopped = result.isStopped();
  }
}
