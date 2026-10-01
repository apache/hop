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
package org.apache.hop.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for version ordering. */
public class VersionCompareTest {

  @Test
  public void testNumericSegmentsCompareNumerically() {
    assertTrue(VersionCompare.compare("2.20.0", "2.19.0") > 0);
    assertTrue(VersionCompare.compare("2.9.0", "2.10.0") < 0);
    assertEquals(0, VersionCompare.compare("2.19.0", "2.19.0"));
  }

  @Test
  public void testMissingSegmentsCountAsZero() {
    assertEquals(0, VersionCompare.compare("2.19", "2.19.0"));
    assertTrue(VersionCompare.compare("2.19.1", "2.19") > 0);
  }

  @Test
  public void testSnapshotIsOlderThanItsRelease() {
    assertTrue(VersionCompare.compare("2.20.0-SNAPSHOT", "2.20.0") < 0);
    assertTrue(VersionCompare.compare("2.20.0-SNAPSHOT", "2.19.0") > 0);
  }

  @Test
  public void testQualifierIsOlderThanTheBareVersion() {
    assertTrue(VersionCompare.compare("2.19.0-rc1", "2.19.0") < 0);
    assertTrue(VersionCompare.compare("1.0-alpha", "1.0") < 0);
  }

  @Test
  public void testHopReleaseCandidatesOrderAgainstTheRunningVersion() {
    // Apache Hop tags every release "-rc1", so this is the comparison the notification floor
    // actually performs in the field.
    assertTrue(VersionCompare.compare("2.19.0-rc1", "2.20.0-SNAPSHOT") < 0);
    assertTrue(VersionCompare.compare("2.3.0-rc1", "2.20.0-SNAPSHOT") < 0);
    // A newer release candidate than the snapshot you are running is worth hearing about.
    assertTrue(VersionCompare.compare("2.20.0-rc1", "2.20.0-SNAPSHOT") > 0);
    assertTrue(VersionCompare.compare("2.21.0-rc1", "2.20.0-SNAPSHOT") > 0);
  }

  @Test
  public void testSnapshotDetection() {
    assertTrue(VersionCompare.isSnapshot("2.20.0-SNAPSHOT"));
    assertTrue(VersionCompare.isSnapshot("2.20.0-snapshot"));
    assertFalse(VersionCompare.isSnapshot("2.20.0"));
    assertFalse(VersionCompare.isSnapshot(null));
  }

  @Test
  public void testStripSnapshotQualifier() {
    assertEquals("2.20.0", VersionCompare.stripSnapshotQualifier("2.20.0-SNAPSHOT"));
    assertEquals("2.20.0", VersionCompare.stripSnapshotQualifier("2.20.0"));
  }

  @Test
  public void testVersionTagsWrittenWithALeadingV() {
    // Tagging a release "v3.0.0" is GitHub's own recommendation. Treated as a qualifier the tag
    // sorted below every plain version, which silently emptied the notification floor of every
    // repository following that convention.
    assertTrue(VersionCompare.compare("v3.0.0", "2.9.0") > 0);
    assertTrue(VersionCompare.compare("v2.9.1", "2.9.0") > 0);
    assertTrue(VersionCompare.compare("v2.8.0", "2.9.0") < 0);
    assertEquals(0, VersionCompare.compare("v2.9.0", "2.9.0"));
    // Both sides tagged that way order numerically rather than alphabetically: v10 after v9.
    assertTrue(VersionCompare.compare("v10.0.0", "v9.0.0") > 0);
    assertTrue(VersionCompare.compare("V3.0.0", "v2.0.0") > 0);
  }

  @Test
  public void testAVersionNamedAfterAWordIsNotStripped() {
    // Only a "v" in front of a digit is a version prefix. Anything else is part of the name.
    assertTrue(VersionCompare.compare("vault-1.0", "1.0") < 0);
    assertEquals(0, VersionCompare.compare("vault-1.0", "vault-1.0"));
  }

  @Test
  public void testLatestAndNewestFirst() {
    assertEquals("2.20.0", VersionCompare.latest(List.of("2.19.0", "2.20.0", "2.9.0")));
    assertNull(VersionCompare.latest(List.of()));
    List<String> sorted =
        List.of("2.9.0", "2.20.0", "2.19.0").stream().sorted(VersionCompare.newestFirst()).toList();
    assertEquals(List.of("2.20.0", "2.19.0", "2.9.0"), sorted);
  }
}
