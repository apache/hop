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

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/**
 * Measured host resources at a point in time. Constructed for tests with fixed values, or via
 * {@link #capture(List)} against the live JVM and local filesystem.
 */
@Getter
public class SystemResourceSnapshot {

  private final long maxMemoryBytes;
  private final long totalMemoryBytes;
  private final int availableProcessors;

  /** Resolved local path → usable free bytes ({@link File#getUsableSpace()}). */
  private final Map<String, Long> usableSpaceByPath;

  public SystemResourceSnapshot(
      long maxMemoryBytes,
      long totalMemoryBytes,
      int availableProcessors,
      Map<String, Long> usableSpaceByPath) {
    this.maxMemoryBytes = maxMemoryBytes;
    this.totalMemoryBytes = totalMemoryBytes;
    this.availableProcessors = availableProcessors;
    this.usableSpaceByPath =
        usableSpaceByPath == null
            ? Map.of()
            : Collections.unmodifiableMap(new HashMap<>(usableSpaceByPath));
  }

  /**
   * Capture live JVM/OS values. For each path, records {@link File#getUsableSpace()} (0 when the
   * path does not exist).
   */
  public static SystemResourceSnapshot capture(List<String> resolvedPaths) {
    Runtime runtime = Runtime.getRuntime();
    Map<String, Long> usable = new HashMap<>();
    if (resolvedPaths != null) {
      for (String path : resolvedPaths) {
        if (path == null || path.isBlank()) {
          continue;
        }
        usable.put(path, new File(path).getUsableSpace());
      }
    }
    return new SystemResourceSnapshot(
        runtime.maxMemory(), runtime.totalMemory(), runtime.availableProcessors(), usable);
  }
}
