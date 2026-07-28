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
import java.util.Collections;
import java.util.List;
import lombok.Getter;

/** Configured system resource thresholds for a lifecycle environment. Null thresholds = skip. */
@Getter
public class SystemResourceRequirement {

  /** Minimum JVM max heap in mebibytes, or null if not checked. */
  private final Long minMaxMemoryMb;

  /** Minimum CPU cores, or null if not checked. */
  private final Integer minProcessors;

  private final List<DiskSpaceRequirement> diskChecks;

  public SystemResourceRequirement(
      Long minMaxMemoryMb, Integer minProcessors, List<DiskSpaceRequirement> diskChecks) {
    this.minMaxMemoryMb = minMaxMemoryMb;
    this.minProcessors = minProcessors;
    this.diskChecks =
        diskChecks == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(diskChecks));
  }

  /** True when at least one threshold is configured. */
  public boolean hasAnyRequirement() {
    return minMaxMemoryMb != null || minProcessors != null || !diskChecks.isEmpty();
  }
}
