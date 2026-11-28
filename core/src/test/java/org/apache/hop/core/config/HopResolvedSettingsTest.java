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

package org.apache.hop.core.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** Layered resolution of hop-config, the process environment, and {@code -D}. */
class HopResolvedSettingsTest {

  @Test
  void configIsUsedWhenNothingElseIsSet() {
    assertEquals("N", HopResolvedSettings.resolveLayered("N", null, null));
  }

  @Test
  void environmentOverridesHopConfig() {
    assertEquals("Y", HopResolvedSettings.resolveLayered("N", "Y", null));
  }

  @Test
  void seededSystemPropertyDoesNotHideTheEnvironment() {
    // HopEnvironment copies the hop-config value onto the JVM. That copy must not beat -e.
    assertEquals("Y", HopResolvedSettings.resolveLayered("N", "Y", "N"));
    assertEquals("Y", HopResolvedSettings.resolveLayered(null, "Y", ""));
  }

  @Test
  void explicitSystemPropertyWinsOverTheEnvironment() {
    assertEquals("Y", HopResolvedSettings.resolveLayered("N", "N", "Y"));
    assertEquals("N", HopResolvedSettings.resolveLayered("Y", "Y", "N"));
  }

  @Test
  void absentLayersStayAbsent() {
    assertNull(HopResolvedSettings.resolveLayered(null, null, null));
  }
}
