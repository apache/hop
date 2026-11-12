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

package org.apache.hop.core.failure;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;

/**
 * A failure of a transform, action, pipeline or workflow, as structured data.
 *
 * <p>A transform or action failure names the component and carries the cause. A pipeline or
 * workflow failure has no component; it points at the failure that brought it down through {@code
 * causedBy}, and so does an action whose child pipeline or workflow failed. {@link #getRootCause()}
 * follows that chain to the start. Every failure in a chain carries the category of its root cause.
 */
@Getter
@Setter
public class ExecutionFailure {
  public static final int SCHEMA_VERSION = 1;

  private int schemaVersion = SCHEMA_VERSION;

  private String failureId = UUID.randomUUID().toString();

  private Date occurredAt = new Date();

  /** The pipeline or workflow this failure happened in. */
  private FailureRun run;

  /** The parents of {@link #run}, the top-level execution first. */
  private List<FailureRun> parents = new ArrayList<>();

  /** The failing transform or action, null for the failure of a pipeline or workflow. */
  private FailureComponent component;

  private FailurePhase phase;

  private FailureCategory category = FailureCategory.UNKNOWN;

  /** Why it failed, null when nothing is known. */
  private FailureCause cause;

  /** The row layout the transform received, empty when unknown or for an action. */
  private List<FailureField> inputLayout = new ArrayList<>();

  /** Row and error counters: read, written, input, output, updated, rejected, errors. */
  private Map<String, Long> counters = new LinkedHashMap<>();

  /** The failure that caused this one, null when this is where it started. */
  private ExecutionFailure causedBy;

  /** The failure where the chain of {@code causedBy} starts: this one if it has none. */
  @JsonIgnore
  public ExecutionFailure getRootCause() {
    ExecutionFailure root = this;
    // Bounded, in case someone builds a loop by hand
    for (int depth = 0; root.causedBy != null && depth < 100; depth++) {
      root = root.causedBy;
    }
    return root;
  }
}
