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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests the decision {@link CrossSitePolicy} makes about a {@code Sec-Fetch-Site} value. */
class CrossSitePolicyTest {

  /**
   * hop-run, the Hop GUI, curl and customer automation send no Sec-Fetch-* headers. Every policy
   * has to let them through or the change breaks every existing integration.
   */
  @ParameterizedTest
  @ValueSource(strings = {"same-site", "same-origin", "off"})
  void clientWithoutTheHeaderIsAlwaysAllowed(String code) throws Exception {
    CrossSitePolicy policy = CrossSitePolicy.parse(code);

    assertTrue(policy.allows(null));
    assertTrue(policy.allows(List.of()));
    // A header that arrived with no value at all reads the same as one that never arrived.
    assertTrue(policy.allows(List.of("")));
    assertTrue(policy.allows(List.of("   ")));
  }

  @Test
  void defaultPolicyRejectsOnlyCrossSite() {
    CrossSitePolicy policy = CrossSitePolicy.SAME_SITE;

    assertTrue(policy.allows(List.of("none")));
    assertTrue(policy.allows(List.of("same-origin")));
    assertTrue(policy.allows(List.of("same-site")));
    assertFalse(policy.allows(List.of("cross-site")));
  }

  @Test
  void sameOriginPolicyRejectsSameSiteAsWell() {
    CrossSitePolicy policy = CrossSitePolicy.SAME_ORIGIN;

    assertTrue(policy.allows(List.of("none")));
    assertTrue(policy.allows(List.of("same-origin")));
    assertFalse(policy.allows(List.of("same-site")));
    assertFalse(policy.allows(List.of("cross-site")));
  }

  @Test
  void offPolicyAllowsEverything() {
    CrossSitePolicy policy = CrossSitePolicy.OFF;

    assertTrue(policy.allows(List.of("cross-site")));
    assertTrue(policy.allows(List.of("same-site")));
    assertTrue(policy.allows(List.of("anything at all")));
    assertTrue(policy.allows(List.of("same-origin", "cross-site")));
  }

  @Test
  void headerValueIsReadCaseInsensitivelyAndTrimmed() {
    assertTrue(CrossSitePolicy.SAME_SITE.allows(List.of(" Same-Origin ")));
    assertFalse(CrossSitePolicy.SAME_SITE.allows(List.of(" Cross-Site ")));
  }

  /** A client that cannot send the header correctly can always send nothing at all. */
  @Test
  void unrecognisedHeaderValueIsRejected() {
    assertFalse(CrossSitePolicy.SAME_SITE.allows(List.of("nonsense")));
    assertFalse(CrossSitePolicy.SAME_SITE.allows(List.of("same-origin, cross-site")));
  }

  /** Contradictory headers, most likely added on the way in. There is no safe reading. */
  @Test
  void severalHeaderValuesAreRejected() {
    assertFalse(CrossSitePolicy.SAME_SITE.allows(List.of("same-origin", "cross-site")));
    assertFalse(CrossSitePolicy.SAME_SITE.allows(List.of("same-origin", "same-origin")));
    // Blank values do not count towards the ambiguity.
    assertTrue(CrossSitePolicy.SAME_SITE.allows(List.of("same-origin", "")));
  }

  @Test
  void policyIsParsedFromItsCode() throws Exception {
    assertEquals(CrossSitePolicy.SAME_SITE, CrossSitePolicy.parse("same-site"));
    assertEquals(CrossSitePolicy.SAME_ORIGIN, CrossSitePolicy.parse("same-origin"));
    assertEquals(CrossSitePolicy.OFF, CrossSitePolicy.parse("off"));
    assertEquals(CrossSitePolicy.SAME_ORIGIN, CrossSitePolicy.parse("  SAME-Origin "));
  }

  @Test
  void missingPolicyFallsBackToSameSite() throws Exception {
    assertEquals(CrossSitePolicy.SAME_SITE, CrossSitePolicy.parse(null));
    assertEquals(CrossSitePolicy.SAME_SITE, CrossSitePolicy.parse(""));
    assertEquals(CrossSitePolicy.SAME_SITE, CrossSitePolicy.parse("   "));
  }

  @Test
  void unknownPolicyIsRefused() {
    HopException e = assertThrows(HopException.class, () -> CrossSitePolicy.parse("strict"));
    assertTrue(e.getMessage().contains("same-origin"), e.getMessage());
  }
}
