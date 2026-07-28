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

package org.apache.hop.projects.resources;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

/** Outcome of comparing a {@link SystemResourceRequirement} to a {@link SystemResourceSnapshot}. */
@Getter
public class SystemResourceCheckResult {

  private final List<String> violations = new ArrayList<>();

  public boolean hasViolations() {
    return !violations.isEmpty();
  }

  public void addViolation(String message) {
    if (message != null && !message.isBlank()) {
      violations.add(message);
    }
  }

  public String formatReport() {
    StringBuilder sb = new StringBuilder();
    for (String v : violations) {
      sb.append("  ").append(v).append('\n');
    }
    return sb.toString();
  }
}
