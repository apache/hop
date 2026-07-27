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
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;

/**
 * Compares configured system resource requirements against a measured snapshot. Disk free-space
 * thresholds are MiB expressions resolved via {@link IVariables#resolve(String)} and {@link
 * Const#toLongExpanded(String, long)}.
 */
public final class SystemResourceChecker {

  private SystemResourceChecker() {}

  /**
   * Check all configured thresholds without variable resolution (literals only). Collects every
   * violation rather than stopping at the first.
   */
  public static SystemResourceCheckResult check(
      SystemResourceRequirement requirement, SystemResourceSnapshot actual) {
    return check(requirement, actual, null);
  }

  /**
   * Check all configured thresholds. Disk min-free expressions are resolved with {@code variables}
   * when non-null: {@code Const.toLongExpanded(variables.resolve(minFree), -1)}.
   */
  public static SystemResourceCheckResult check(
      SystemResourceRequirement requirement, SystemResourceSnapshot actual, IVariables variables) {
    SystemResourceCheckResult result = new SystemResourceCheckResult();
    if (requirement == null || actual == null || !requirement.hasAnyRequirement()) {
      return result;
    }

    if (requirement.getMinMaxMemoryMb() != null) {
      long requiredBytes = requirement.getMinMaxMemoryMb() * ResourceAttributes.BYTES_PER_MIB;
      long actualBytes = actual.getMaxMemoryBytes();
      if (actualBytes < requiredBytes) {
        result.addViolation(
            "JVM max memory (maxMemory/-Xmx) is "
                + formatMb(actualBytes)
                + " MiB but environment requires at least "
                + requirement.getMinMaxMemoryMb()
                + " MiB");
      }
    }

    if (requirement.getMinProcessors() != null) {
      int actualCores = actual.getAvailableProcessors();
      if (actualCores < requirement.getMinProcessors()) {
        result.addViolation(
            "Available processors is "
                + actualCores
                + " but environment requires at least "
                + requirement.getMinProcessors());
      }
    }

    for (DiskSpaceRequirement disk : requirement.getDiskChecks()) {
      if (disk == null || StringUtils.isBlank(disk.getPath())) {
        continue;
      }
      String path = disk.getPath();
      Long usable = actual.getUsableSpaceByPath().get(path);
      if (usable == null) {
        result.addViolation("Disk path '" + path + "' was not measured (missing from snapshot)");
        continue;
      }

      long minMb = resolveMinFreeMb(disk.getMinFreeBytes(), variables);
      if (minMb <= 0) {
        result.addViolation(
            "Disk path '"
                + path
                + "' has invalid min free space expression: '"
                + Const.NVL(disk.getMinFreeBytes(), "")
                + "'");
        continue;
      }
      long minBytes = minMb * ResourceAttributes.BYTES_PER_MIB;

      File file = new File(path);
      if (!file.exists()) {
        result.addViolation(
            "Disk path '" + path + "' does not exist (required free space: " + minMb + " MiB)");
        continue;
      }
      if (usable < minBytes) {
        result.addViolation(
            "Disk path '"
                + path
                + "' has "
                + formatMb(usable)
                + " MiB free but environment requires at least "
                + minMb
                + " MiB");
      }
    }

    return result;
  }

  /**
   * Resolve disk paths with variables, capture a live snapshot, and check. Min free expressions are
   * resolved during the check via {@link Const#toLongExpanded(String, long)}.
   */
  public static SystemResourceCheckResult checkLive(
      SystemResourceRequirement requirement, IVariables variables) {
    if (requirement == null || !requirement.hasAnyRequirement()) {
      return new SystemResourceCheckResult();
    }
    List<String> resolvedPaths = new ArrayList<>();
    List<DiskSpaceRequirement> resolvedDisks = new ArrayList<>();
    for (DiskSpaceRequirement disk : requirement.getDiskChecks()) {
      if (disk == null || StringUtils.isBlank(disk.getPath())) {
        continue;
      }
      String path = disk.getPath().trim();
      if (variables != null) {
        path = variables.resolve(path);
      }
      if (StringUtils.isBlank(path)) {
        continue;
      }
      resolvedPaths.add(path);
      // Keep min-free expression as configured; resolve + toLongExpanded happens in check()
      resolvedDisks.add(new DiskSpaceRequirement(path, disk.getMinFreeBytes()));
    }
    SystemResourceRequirement resolved =
        new SystemResourceRequirement(
            requirement.getMinMaxMemoryMb(), requirement.getMinProcessors(), resolvedDisks);
    SystemResourceSnapshot snapshot = SystemResourceSnapshot.capture(resolvedPaths);
    return check(resolved, snapshot, variables);
  }

  /**
   * Resolve a min-free MiB expression: {@code Const.toLongExpanded(variables.resolve(expr), -1)}.
   *
   * @return positive MiB value, or {@code <= 0} when missing/invalid
   */
  static long resolveMinFreeMb(String minFreeExpression, IVariables variables) {
    if (StringUtils.isBlank(minFreeExpression)) {
      return -1L;
    }
    String resolved =
        variables != null ? variables.resolve(minFreeExpression.trim()) : minFreeExpression.trim();
    return Const.toLongExpanded(resolved, -1L);
  }

  private static long formatMb(long bytes) {
    return bytes / ResourceAttributes.BYTES_PER_MIB;
  }
}
