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

package org.apache.hop.naming.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class NamingSchemeSelectorTest {

  @Test
  void typeSpecificSchemesHideGeneral() {
    NamingScheme general = scheme("all-snake", NamingSchemeType.GENERAL);
    NamingScheme field = scheme("field-snake", NamingSchemeType.HOP_FIELD);
    List<NamingScheme> picked =
        NamingSchemeSelector.matching(List.of(general, field), NamingSchemeType.HOP_FIELD);
    assertEquals(1, picked.size());
    assertEquals("field-snake", picked.get(0).getName());
  }

  @Test
  void generalIsUsedWhenNoSpecificSchemeExists() {
    NamingScheme general = scheme("all-snake", NamingSchemeType.GENERAL);
    NamingScheme table = scheme("table-upper", NamingSchemeType.DATABASE_TABLE);
    List<NamingScheme> picked =
        NamingSchemeSelector.matching(List.of(general, table), NamingSchemeType.HOP_TRANSFORM);
    assertEquals(1, picked.size());
    assertEquals("all-snake", picked.get(0).getName());
  }

  @Test
  void emptyWhenNeitherSpecificNorGeneralExists() {
    NamingScheme table = scheme("table-upper", NamingSchemeType.DATABASE_TABLE);
    assertTrue(NamingSchemeSelector.matching(List.of(table), NamingSchemeType.HOP_FIELD).isEmpty());
  }

  @Test
  void explicitNameWinsOverType() {
    NamingScheme general = scheme("all-snake", NamingSchemeType.GENERAL);
    NamingScheme field = scheme("field-snake", NamingSchemeType.HOP_FIELD);
    NamingScheme picked =
        NamingSchemeSelector.resolve(
            List.of(general, field), NamingSchemeType.HOP_FIELD, "all-snake");
    assertEquals("all-snake", picked.getName());
  }

  @Test
  void uniqueMatchIsResolvedWithoutExplicitName() {
    NamingScheme general = scheme("all-snake", NamingSchemeType.GENERAL);
    NamingScheme picked =
        NamingSchemeSelector.resolve(List.of(general), NamingSchemeType.HOP_FIELD, null);
    assertEquals("all-snake", picked.getName());
  }

  @Test
  void requestingGeneralReturnsOnlyGeneralSchemes() {
    NamingScheme general = scheme("all-snake", NamingSchemeType.GENERAL);
    NamingScheme field = scheme("field-snake", NamingSchemeType.HOP_FIELD);
    List<NamingScheme> picked =
        NamingSchemeSelector.matching(List.of(general, field), NamingSchemeType.GENERAL);
    assertEquals(1, picked.size());
    assertEquals("all-snake", picked.get(0).getName());
  }

  @Test
  void pluginKindDoesNotBecomeHopField() {
    NamingScheme field = scheme("field-snake", NamingSchemeType.HOP_FIELD);
    NamingScheme hub = new NamingScheme("hub-snake");
    hub.setType("dv-hub");
    List<NamingScheme> picked = NamingSchemeSelector.matching(List.of(field, hub), "dv-hub");
    assertEquals(1, picked.size());
    assertEquals("hub-snake", picked.get(0).getName());
  }

  @Test
  void displayFromCodeKeepsUnknownPluginCodes() {
    assertEquals("dv-hub", NamingSchemeType.displayFromCode("dv-hub"));
  }

  private static NamingScheme scheme(String name, NamingSchemeType type) {
    NamingScheme scheme = new NamingScheme(name);
    scheme.setType(type.getCode());
    return scheme;
  }
}
