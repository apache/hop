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

package org.apache.hop.pipeline.transforms.odata;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit test for {@link ODataAuthType} */
class ODataAuthTypeTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @Test
  void lookupCodeIsCaseInsensitiveAndFallsBackToNone() {
    assertEquals(ODataAuthType.NONE, ODataAuthType.lookupCode("NONE"));
    assertEquals(ODataAuthType.BASIC, ODataAuthType.lookupCode("basic"));
    assertEquals(ODataAuthType.BEARER, ODataAuthType.lookupCode("Bearer"));
    assertEquals(ODataAuthType.NONE, ODataAuthType.lookupCode("unknown"));
    assertEquals(ODataAuthType.NONE, ODataAuthType.lookupCode(null));
  }

  @Test
  void lookupDescriptionMatchesLocalizedLabels() {
    assertEquals(
        ODataAuthType.NONE, ODataAuthType.lookupDescription(ODataAuthType.NONE.getDescription()));
    assertEquals(
        ODataAuthType.BASIC, ODataAuthType.lookupDescription(ODataAuthType.BASIC.getDescription()));
    assertEquals(
        ODataAuthType.BEARER,
        ODataAuthType.lookupDescription(ODataAuthType.BEARER.getDescription()));
    assertEquals(ODataAuthType.NONE, ODataAuthType.lookupDescription("not-an-auth-type"));
    assertEquals(ODataAuthType.NONE, ODataAuthType.lookupDescription(null));
  }

  @Test
  void getDescriptionsReturnsEveryEnumValue() {
    String[] descriptions = ODataAuthType.getDescriptions();
    assertEquals(ODataAuthType.values().length, descriptions.length);
    assertArrayEquals(
        new String[] {
          ODataAuthType.NONE.getDescription(),
          ODataAuthType.BASIC.getDescription(),
          ODataAuthType.BEARER.getDescription()
        },
        descriptions);
  }

  @Test
  void codesAreStableWireValues() {
    assertEquals("NONE", ODataAuthType.NONE.getCode());
    assertEquals("BASIC", ODataAuthType.BASIC.getCode());
    assertEquals("BEARER", ODataAuthType.BEARER.getCode());
    assertNotEquals(ODataAuthType.NONE.getDescription(), ODataAuthType.NONE.getCode());
  }
}
