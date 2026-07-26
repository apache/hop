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

package org.apache.hop.marketplace.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class VersionCompatTest {

  @Test
  void compareNumericAndSnapshot() {
    assertTrue(VersionCompat.compare("2.19.0", "2.18.1") > 0);
    assertTrue(VersionCompat.compare("2.18.1", "2.19.0-SNAPSHOT") < 0);
    assertTrue(VersionCompat.compare("0.4.0-SNAPSHOT", "0.3.0") > 0);
    assertEquals(0, VersionCompat.compare("2.19.0-SNAPSHOT", "2.19.0-SNAPSHOT"));
  }

  @Test
  void latestPicksHighest() {
    assertEquals(
        "0.4.0-SNAPSHOT", VersionCompat.latest(List.of("0.3.0", "0.4.0-SNAPSHOT", "0.2.0")));
  }

  @Test
  void minHopVersionFilter() {
    OptionalPluginInfo info = new OptionalPluginInfo();
    info.setArtifactId("hop-datavault");
    info.setMinHopVersion("2.18.1");
    assertTrue(VersionCompat.isCompatibleWithHop(info, "2.19.0-SNAPSHOT"));
    assertTrue(VersionCompat.isCompatibleWithHop(info, "2.18.1"));
    assertFalse(VersionCompat.isCompatibleWithHop(info, "2.17.0"));
  }

  @Test
  void maxHopVersionFilter() {
    OptionalPluginInfo info = new OptionalPluginInfo();
    info.setMaxHopVersion("2.19.0");
    assertTrue(VersionCompat.isCompatibleWithHop(info, "2.18.0"));
    assertFalse(VersionCompat.isCompatibleWithHop(info, "2.20.0"));
  }

  @Test
  void noBoundsAlwaysCompatible() {
    OptionalPluginInfo info = new OptionalPluginInfo();
    info.setArtifactId("auto");
    assertTrue(VersionCompat.isCompatibleWithHop(info, "2.19.0"));
    assertTrue(VersionCompat.isCompatibleWithHop(info, null));
  }

  @Test
  void nexusPreferableHigherVersion() {
    OptionalPluginInfo existing = new OptionalPluginInfo();
    existing.setVersion("0.3.0");
    existing.setLastUpdated("2026-01-01T00:00:00Z");
    assertTrue(
        NexusRepositoryBrowser.isPreferable("0.4.0-SNAPSHOT", "2026-07-21T14:00:00Z", existing));
    assertFalse(NexusRepositoryBrowser.isPreferable("0.2.0", "2026-07-21T14:00:00Z", existing));
  }

  @Test
  void nexusPreferableSameVersionNewerTimestamp() {
    OptionalPluginInfo existing = new OptionalPluginInfo();
    existing.setVersion("0.4.0-SNAPSHOT");
    existing.setLastUpdated("2026-07-21T10:56:17Z");
    assertTrue(
        NexusRepositoryBrowser.isPreferable("0.4.0-SNAPSHOT", "2026-07-21T14:42:59Z", existing));
  }
}
