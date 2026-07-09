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
package org.apache.hop.pipeline.transforms.randomvalue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Unit test for {@link RandomValueMeta.RandomType}. */
class RandomTypeTest {

  @Test
  void testEnumBasicProperties() {
    RandomValueMeta.RandomType type = RandomValueMeta.RandomType.NUMBER;

    assertEquals("random number", type.getCode());
    assertNotNull(type.getDescription());
    assertFalse(type.getDescription().isEmpty());
  }

  @Test
  void testNoneDefault() {
    RandomValueMeta.RandomType none = RandomValueMeta.RandomType.NONE;

    assertEquals("", none.getCode());
    assertEquals("", none.getDescription());
  }

  @Test
  void testLookupDescriptionSuccess() {
    String desc = RandomValueMeta.RandomType.INTEGER.getDescription();

    RandomValueMeta.RandomType result = RandomValueMeta.RandomType.lookupDescription(desc);
    assertEquals(RandomValueMeta.RandomType.INTEGER, result);
  }

  @Test
  void testLookupDescriptionNotFound() {
    RandomValueMeta.RandomType result = RandomValueMeta.RandomType.lookupDescription("not-exist");
    assertEquals(RandomValueMeta.RandomType.NONE, result);
  }

  @Test
  void testLookupCodeSuccess() {
    RandomValueMeta.RandomType result = RandomValueMeta.RandomType.lookupCode("random uuid4");
    assertEquals(RandomValueMeta.RandomType.UUID4, result);
  }

  @Test
  void testLookupCodeNotFound() {
    RandomValueMeta.RandomType result = RandomValueMeta.RandomType.lookupCode("not-exist");
    assertEquals(RandomValueMeta.RandomType.NONE, result);
  }

  @Test
  void testGetDescriptionsExcludesNone() {
    String[] descriptions = RandomValueMeta.RandomType.getDescriptions();

    assertNotNull(descriptions);
    assertEquals(RandomValueMeta.RandomType.values().length - 1, descriptions.length);
    for (String description : descriptions) {
      assertFalse(description.isEmpty());
    }
  }

  @Test
  void testGetDescriptionsContainsKnownType() {
    String[] descriptions = RandomValueMeta.RandomType.getDescriptions();

    boolean found = false;
    for (String desc : descriptions) {
      if (desc.equals(RandomValueMeta.RandomType.STRING.getDescription())) {
        found = true;
        break;
      }
    }

    assertTrue(found);
  }

  @Test
  void testAllEnumHaveCodeAndDescription() {
    for (RandomValueMeta.RandomType type : RandomValueMeta.RandomType.values()) {
      assertNotNull(type.getCode(), type.name() + " code is null");
      assertNotNull(type.getDescription(), type.name() + " description is null");
    }
  }

  @Test
  void testDescriptionUniqueAmongSelectableTypes() {
    Set<String> set = new HashSet<>();

    for (RandomValueMeta.RandomType type : RandomValueMeta.RandomType.values()) {
      if (type == RandomValueMeta.RandomType.NONE) {
        continue;
      }
      String desc = type.getDescription();
      assertTrue(set.add(desc), "Duplicate description: " + desc);
    }
  }

  @Test
  void testDescriptionNotFallbackKey() {
    for (RandomValueMeta.RandomType type : RandomValueMeta.RandomType.values()) {
      if (type == RandomValueMeta.RandomType.NONE) {
        continue;
      }

      String desc = type.getDescription();
      assertFalse(
          desc.contains("RandomValueMeta.TypeDesc."), "i18n not resolved for: " + type.name());
    }
  }
}
