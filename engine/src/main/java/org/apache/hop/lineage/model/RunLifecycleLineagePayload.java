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

package org.apache.hop.lineage.model;

import java.util.Objects;
import lombok.Getter;

/** Marks start or end of a logical run (pipeline, workflow, or other subject). */
@Getter
public final class RunLifecycleLineagePayload implements LineagePayload {

  private final RunLifecyclePhase phase;
  private final String detail;

  public RunLifecycleLineagePayload(RunLifecyclePhase phase, String detail) {
    this.phase = Objects.requireNonNull(phase, "phase");
    this.detail = detail;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RunLifecycleLineagePayload that = (RunLifecycleLineagePayload) o;
    return phase == that.phase && Objects.equals(detail, that.detail);
  }

  @Override
  public int hashCode() {
    return Objects.hash(phase, detail);
  }
}
