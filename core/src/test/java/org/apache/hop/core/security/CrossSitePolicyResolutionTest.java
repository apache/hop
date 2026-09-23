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

package org.apache.hop.core.security;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * How Hop Web decides its cross-site policy. The promise to operators is that {@code
 * HOP_SERVER_CROSS_SITE_POLICY} is the same knob here as on the standalone hop-server, so it is
 * worth pinning down.
 */
class CrossSitePolicyResolutionTest {

  @AfterEach
  void clearOverride() {
    System.clearProperty(CrossSitePolicy.CONFIG_KEY);
  }

  @Test
  void defaultsToSameSiteWhenNothingIsConfigured() {
    assertEquals(CrossSitePolicy.SAME_SITE, new HopSecurityConfig().resolveCrossSitePolicy());
  }

  @Test
  void configFileValueIsUsed() {
    HopSecurityConfig config = new HopSecurityConfig();
    config.setCrossSitePolicy(CrossSitePolicy.SAME_ORIGIN.getCode());

    assertEquals(CrossSitePolicy.SAME_ORIGIN, config.resolveCrossSitePolicy());
  }

  /** The escape hatch: an operator must be able to turn the check off without editing the JSON. */
  @Test
  void overrideWinsOverTheConfigFile() {
    HopSecurityConfig config = new HopSecurityConfig();
    config.setCrossSitePolicy(CrossSitePolicy.SAME_ORIGIN.getCode());
    System.setProperty(CrossSitePolicy.CONFIG_KEY, CrossSitePolicy.OFF.getCode());

    assertEquals(CrossSitePolicy.OFF, config.resolveCrossSitePolicy());
  }

  @Test
  void blankOverrideLeavesTheConfigFileInCharge() {
    HopSecurityConfig config = new HopSecurityConfig();
    config.setCrossSitePolicy(CrossSitePolicy.OFF.getCode());
    System.setProperty(CrossSitePolicy.CONFIG_KEY, "   ");

    assertEquals(CrossSitePolicy.OFF, config.resolveCrossSitePolicy());
  }

  /** A typo must not silently leave the check off; fall back to the safe default instead. */
  /**
   * Regression for the Hop Web failure this was first shipped with: {@code HopEnvironment.init()}
   * copies every {@code @Variable} setting into a system property without consulting the
   * environment, so the property is present but blank. Honouring it shadowed the config file (and
   * the environment variable) and silently pinned every deployment to the default.
   */
  @Test
  void blankInjectedPropertyDoesNotShadowTheConfigFile() {
    HopSecurityConfig config = new HopSecurityConfig();
    config.setCrossSitePolicy(CrossSitePolicy.OFF.getCode());
    System.setProperty(CrossSitePolicy.CONFIG_KEY, "");

    assertEquals(CrossSitePolicy.OFF, config.resolveCrossSitePolicy());
  }

  /** An operator who really passes -D still wins; HopEnvironment leaves such a value alone. */
  @Test
  void explicitSystemPropertyStillWins() {
    HopSecurityConfig config = new HopSecurityConfig();
    config.setCrossSitePolicy(CrossSitePolicy.SAME_SITE.getCode());
    System.setProperty(CrossSitePolicy.CONFIG_KEY, CrossSitePolicy.OFF.getCode());

    assertEquals(CrossSitePolicy.OFF, config.resolveCrossSitePolicy());
  }

  @Test
  void unusableValueFallsBackToTheSafeDefault() {
    HopSecurityConfig config = new HopSecurityConfig();
    config.setCrossSitePolicy("disabled-please");

    assertEquals(CrossSitePolicy.SAME_SITE, config.resolveCrossSitePolicy());
  }

  @Test
  void unusableOverrideFallsBackToTheSafeDefault() {
    HopSecurityConfig config = new HopSecurityConfig();
    config.setCrossSitePolicy(CrossSitePolicy.OFF.getCode());
    System.setProperty(CrossSitePolicy.CONFIG_KEY, "nonsense");

    assertEquals(CrossSitePolicy.SAME_SITE, config.resolveCrossSitePolicy());
  }

  /** An older security-config.json has no such field; it must read as the default, not as null. */
  @Test
  void missingFieldReadsAsTheDefault() {
    HopSecurityConfig config = new HopSecurityConfig();
    config.setCrossSitePolicy(null);

    assertEquals(CrossSitePolicy.SAME_SITE, config.resolveCrossSitePolicy());
  }
}
