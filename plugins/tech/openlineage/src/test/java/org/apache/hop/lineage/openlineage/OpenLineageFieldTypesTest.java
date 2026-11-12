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

package org.apache.hop.lineage.openlineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.row.IValueMeta;
import org.junit.jupiter.api.Test;

class OpenLineageFieldTypesTest {

  @Test
  void mapsEveryHopTypeDescriptionThatHasAnOpenLineageName() {
    assertEquals("string", OpenLineageFieldTypes.of("String"));
    assertEquals("int", OpenLineageFieldTypes.of("Integer"));
    assertEquals("double", OpenLineageFieldTypes.of("Number"));
    assertEquals("decimal", OpenLineageFieldTypes.of("BigNumber"));
    assertEquals("date", OpenLineageFieldTypes.of("Date"));
    assertEquals("timestamp", OpenLineageFieldTypes.of("Timestamp"));
    assertEquals("boolean", OpenLineageFieldTypes.of("Boolean"));
    assertEquals("binary", OpenLineageFieldTypes.of("Binary"));
    assertEquals("struct", OpenLineageFieldTypes.of("Json"));
    // An IPv4/IPv6 address travels as text.
    assertEquals("string", OpenLineageFieldTypes.of("Internet Address"));
  }

  @Test
  void leavesTypesWithoutAnHonestMappingOut() {
    assertNull(OpenLineageFieldTypes.of("Serializable"));
    assertNull(OpenLineageFieldTypes.of("something a plugin invented"));
    assertNull(OpenLineageFieldTypes.of(""));
    assertNull(OpenLineageFieldTypes.of(null));
  }

  @Test
  void coversEveryTypeDescriptionHopKnows() {
    for (String typeCode : IValueMeta.typeCodes) {
      if ("-".equals(typeCode) || "Serializable".equals(typeCode)) {
        continue;
      }
      assertNotNull(
          OpenLineageFieldTypes.of(typeCode),
          "no OpenLineage type for the Hop type '" + typeCode + "'");
    }
  }
}
