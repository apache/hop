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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class PluginDiscoveryResolveInstallTest {

  @Test
  void pickExactArtifactId() throws Exception {
    OptionalPluginInfo a = plugin("hop-datavault", "0.4.0-SNAPSHOT", "community");
    OptionalPluginInfo b = plugin("hop-other", "1.0", "community");
    OptionalPluginInfo chosen = PluginDiscovery.pickInstallMatch("hop-datavault", List.of(a, b));
    assertEquals("hop-datavault", chosen.getArtifactId());
    assertEquals("0.4.0-SNAPSHOT", chosen.getVersion());
  }

  @Test
  void pickShortNameSubstringOnArtifact() throws Exception {
    OptionalPluginInfo a = plugin("hop-datavault", "0.4.0-SNAPSHOT", "data-hopper-community");
    OptionalPluginInfo chosen = PluginDiscovery.pickInstallMatch("datavault", List.of(a));
    assertEquals("hop-datavault", chosen.getArtifactId());
  }

  @Test
  void ambiguousShortNameThrows() {
    OptionalPluginInfo a = plugin("hop-datavault", "0.4.0-SNAPSHOT", "c1");
    OptionalPluginInfo b = plugin("hop-datavault-ui", "1.0", "c1");
    HopException ex =
        assertThrows(
            HopException.class, () -> PluginDiscovery.pickInstallMatch("datavault", List.of(a, b)));
    assertTrue(ex.getMessage().toLowerCase().contains("ambiguous"));
  }

  @Test
  void fullGavSkipsDiscovery() throws Exception {
    PluginDiscovery.InstallTarget target =
        PluginDiscovery.resolveInstall(
            "org.example:foo:9.9.9", "org.apache.hop", "2.19.0-SNAPSHOT", null, null);
    assertEquals("org.example:foo:9.9.9", target.coordinates().gav());
    assertNull(target.preferredRepoId());
  }

  @Test
  void unknownNameFallsBackToLiteralParse() throws Exception {
    PluginDiscovery.InstallTarget target =
        PluginDiscovery.resolveInstall(
            "totally-unknown-plugin", "org.apache.hop", "2.19.0-SNAPSHOT", null, null);
    assertEquals(
        "org.apache.hop:totally-unknown-plugin:2.19.0-SNAPSHOT", target.coordinates().gav());
  }

  private static OptionalPluginInfo plugin(String artifactId, String version, String source) {
    OptionalPluginInfo info = new OptionalPluginInfo();
    info.setGroupId("org.apache.hop");
    info.setArtifactId(artifactId);
    info.setVersion(version);
    info.setSource(source);
    return info;
  }
}
