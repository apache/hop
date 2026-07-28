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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SystemResourceCheckerTest {

  private static final long MIB = ResourceAttributes.BYTES_PER_MIB;

  @Test
  void emptyRequirementAlwaysPasses() {
    SystemResourceRequirement req = new SystemResourceRequirement(null, null, List.of());
    SystemResourceSnapshot snap = new SystemResourceSnapshot(0, 0, 0, Map.of());
    SystemResourceCheckResult result = SystemResourceChecker.check(req, snap);
    assertFalse(result.hasViolations());
  }

  @Test
  void memoryPassesWhenMaxHeapMeetsMinimum() {
    SystemResourceRequirement req = new SystemResourceRequirement(1024L, null, List.of());
    SystemResourceSnapshot snap = new SystemResourceSnapshot(2048 * MIB, 512 * MIB, 8, Map.of());
    assertFalse(SystemResourceChecker.check(req, snap).hasViolations());
  }

  @Test
  void memoryFailsWhenMaxHeapBelowMinimum() {
    SystemResourceRequirement req = new SystemResourceRequirement(4096L, null, List.of());
    SystemResourceSnapshot snap = new SystemResourceSnapshot(1024 * MIB, 256 * MIB, 8, Map.of());
    SystemResourceCheckResult result = SystemResourceChecker.check(req, snap);
    assertTrue(result.hasViolations());
    assertEquals(1, result.getViolations().size());
    assertTrue(result.getViolations().get(0).contains("max memory"));
    assertTrue(result.getViolations().get(0).contains("4096"));
  }

  @Test
  void processorsPassAndFail() {
    SystemResourceRequirement req = new SystemResourceRequirement(null, 4, List.of());
    SystemResourceSnapshot enough = new SystemResourceSnapshot(MIB, MIB, 8, Map.of());
    SystemResourceSnapshot shortfall = new SystemResourceSnapshot(MIB, MIB, 2, Map.of());
    assertFalse(SystemResourceChecker.check(req, enough).hasViolations());
    SystemResourceCheckResult fail = SystemResourceChecker.check(req, shortfall);
    assertTrue(fail.hasViolations());
    assertTrue(fail.getViolations().get(0).contains("processors"));
  }

  @Test
  void diskFailsWhenUsableSpaceBelowMinimum(@TempDir Path temp) {
    String path = temp.toString();
    // min free expression is MiB; require a huge amount
    SystemResourceRequirement req =
        new SystemResourceRequirement(
            null, null, List.of(new DiskSpaceRequirement(path, "10000000")));
    SystemResourceSnapshot snap = new SystemResourceSnapshot(MIB, MIB, 4, Map.of(path, 100L * MIB));
    SystemResourceCheckResult result = SystemResourceChecker.check(req, snap);
    assertTrue(result.hasViolations());
    assertTrue(result.getViolations().get(0).contains(path));
    assertTrue(result.getViolations().get(0).contains("free"));
  }

  @Test
  void diskPassesWhenUsableSpaceMeetsMinimum(@TempDir Path temp) {
    String path = temp.toString();
    SystemResourceRequirement req =
        new SystemResourceRequirement(null, null, List.of(new DiskSpaceRequirement(path, "10")));
    SystemResourceSnapshot snap = new SystemResourceSnapshot(MIB, MIB, 4, Map.of(path, 100 * MIB));
    assertFalse(SystemResourceChecker.check(req, snap).hasViolations());
  }

  @Test
  void diskFailsWhenPathMissingFromFilesystem() {
    String path = "/path/that/does/not/exist-for-resource-check";
    SystemResourceRequirement req =
        new SystemResourceRequirement(null, null, List.of(new DiskSpaceRequirement(path, "100")));
    SystemResourceSnapshot snap = new SystemResourceSnapshot(MIB, MIB, 4, Map.of(path, 0L));
    SystemResourceCheckResult result = SystemResourceChecker.check(req, snap);
    assertTrue(result.hasViolations());
    assertTrue(result.getViolations().get(0).contains("does not exist"));
  }

  @Test
  void diskResolvesMinFreeVariableWithToLongExpanded(@TempDir Path temp) {
    String path = temp.toString();
    Variables variables = new Variables();
    variables.setVariable("MIN_DISK_MB", "10");

    SystemResourceRequirement req =
        new SystemResourceRequirement(
            null, null, List.of(new DiskSpaceRequirement(path, "${MIN_DISK_MB}")));
    SystemResourceSnapshot enough =
        new SystemResourceSnapshot(MIB, MIB, 4, Map.of(path, 100 * MIB));
    assertFalse(SystemResourceChecker.check(req, enough, variables).hasViolations());

    // Require more free space than available via variable
    variables.setVariable("MIN_DISK_MB", "10000000");
    SystemResourceCheckResult fail = SystemResourceChecker.check(req, enough, variables);
    assertTrue(fail.hasViolations());
    assertTrue(fail.getViolations().get(0).contains("10000000"));
  }

  @Test
  void diskAcceptsExpandedNumberForms(@TempDir Path temp) {
    String path = temp.toString();
    // "1.5m" via toLongExpanded → 1_500_000 MiB — fail against small free space
    SystemResourceRequirement req =
        new SystemResourceRequirement(null, null, List.of(new DiskSpaceRequirement(path, "1.5m")));
    SystemResourceSnapshot snap = new SystemResourceSnapshot(MIB, MIB, 4, Map.of(path, 100 * MIB));
    SystemResourceCheckResult result = SystemResourceChecker.check(req, snap);
    assertTrue(result.hasViolations());
  }

  @Test
  void diskReportsInvalidMinFreeExpression(@TempDir Path temp) {
    String path = temp.toString();
    SystemResourceRequirement req =
        new SystemResourceRequirement(
            null, null, List.of(new DiskSpaceRequirement(path, "not-a-number")));
    SystemResourceSnapshot snap = new SystemResourceSnapshot(MIB, MIB, 4, Map.of(path, 100 * MIB));
    SystemResourceCheckResult result = SystemResourceChecker.check(req, snap);
    assertTrue(result.hasViolations());
    assertTrue(result.getViolations().get(0).contains("invalid min free"));
  }

  @Test
  void resolveMinFreeMbUsesVariablesAndToLongExpanded() {
    Variables variables = new Variables();
    variables.setVariable("N", "2m");
    assertEquals(2_000_000L, SystemResourceChecker.resolveMinFreeMb("${N}", variables));
    assertEquals(1024L, SystemResourceChecker.resolveMinFreeMb("1024", null));
    assertEquals(-1L, SystemResourceChecker.resolveMinFreeMb("bogus", null));
  }

  @Test
  void collectsAllViolations() {
    SystemResourceRequirement req =
        new SystemResourceRequirement(
            8192L, 16, List.of(new DiskSpaceRequirement("/missing-disk-path-xyz", "100")));
    SystemResourceSnapshot snap =
        new SystemResourceSnapshot(512 * MIB, 128 * MIB, 2, Map.of("/missing-disk-path-xyz", 0L));
    SystemResourceCheckResult result = SystemResourceChecker.check(req, snap);
    assertEquals(3, result.getViolations().size());
    String report = result.formatReport();
    assertTrue(report.contains("max memory"));
    assertTrue(report.contains("processors"));
    assertTrue(report.contains("missing-disk-path-xyz"));
  }

  @Test
  void captureReflectsLiveRuntimeAndTempDir(@TempDir Path temp) {
    SystemResourceSnapshot snap = SystemResourceSnapshot.capture(List.of(temp.toString()));
    assertTrue(snap.getMaxMemoryBytes() > 0);
    assertTrue(snap.getTotalMemoryBytes() > 0);
    assertTrue(snap.getMaxMemoryBytes() >= snap.getTotalMemoryBytes());
    assertTrue(snap.getAvailableProcessors() >= 1);
    assertTrue(snap.getUsableSpaceByPath().get(temp.toString()) > 0);
  }

  @Test
  void checkLivePassesAgainstLenientThresholds(@TempDir Path temp) {
    SystemResourceRequirement req =
        new SystemResourceRequirement(
            1L, 1, List.of(new DiskSpaceRequirement(temp.toString(), "1")));
    SystemResourceCheckResult result = SystemResourceChecker.checkLive(req, null);
    assertFalse(result.hasViolations(), () -> result.formatReport());
  }
}
