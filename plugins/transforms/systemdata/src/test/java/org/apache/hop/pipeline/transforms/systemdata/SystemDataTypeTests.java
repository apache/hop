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

package org.apache.hop.pipeline.transforms.systemdata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Unit test for {@link SystemDataType} */
class SystemDataTypeTests {

  @Test
  void testEnumBasicProperties() {
    SystemDataType type = SystemDataType.SYSTEM_DATE;

    assertEquals("system date (variable)", type.getCode());
    assertNotNull(type.getDescription());
    assertFalse(type.getDescription().isEmpty());
  }

  @Test
  void testNoneDefault() {
    SystemDataType none = SystemDataType.NONE;

    assertEquals("", none.getCode());
    assertNotNull(none.getDescription());
  }

  @Test
  void testLookupDescription_success() {
    String desc = SystemDataType.SYSTEM_DATE.getDescription();

    SystemDataType result = SystemDataType.lookupDescription(desc);
    assertEquals(SystemDataType.SYSTEM_DATE, result);
  }

  @Test
  void testLookupDescription_notFound() {
    SystemDataType result = SystemDataType.lookupDescription("not-exist");
    assertEquals(SystemDataType.NONE, result);
  }

  @Test
  void testGetDescriptions() {
    String[] descriptions = SystemDataType.getDescriptions();

    assertNotNull(descriptions);
    assertTrue(descriptions.length > 0);

    boolean found = false;
    for (String desc : descriptions) {
      if (desc.equals(SystemDataType.SYSTEM_DATE.getDescription())) {
        found = true;
        break;
      }
    }

    assertTrue(found);
  }

  @Test
  void testAllEnumHaveCodeAndDescription() {
    for (SystemDataType type : SystemDataType.values()) {
      assertNotNull(type.getCode(), type.name() + " code is null");
      assertNotNull(type.getDescription(), type.name() + " description is null");
    }
  }

  @Test
  void testDescriptionUnique() {
    Set<String> set = new HashSet<>();

    for (SystemDataType type : SystemDataType.values()) {
      String desc = type.getDescription();
      assertTrue(set.add(desc), "Duplicate description: " + desc);
    }
  }

  @Test
  void testDescriptionNotFallbackKey() {
    for (SystemDataType type : SystemDataType.values()) {
      if (type.equals(SystemDataType.NONE)) {
        continue;
      }

      String desc = type.getDescription();
      assertFalse(
          desc.contains("SystemDataMeta.TypeDesc."), "i18n not resolved for: " + type.name());
    }
  }
}
