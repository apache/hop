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

package org.apache.hop.mongo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.ReadPreference;
import java.util.Collection;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class NamedReadPreferenceTest {

  @Test
  void testGetName() {
    assertEquals("primary", NamedReadPreference.PRIMARY.getName());
    assertEquals("primaryPreferred", NamedReadPreference.PRIMARY_PREFERRED.getName());
    assertEquals("secondary", NamedReadPreference.SECONDARY.getName());
    assertEquals("secondaryPreferred", NamedReadPreference.SECONDARY_PREFERRED.getName());
    assertEquals("nearest", NamedReadPreference.NEAREST.getName());
  }

  @Test
  void testGetPreference() {
    assertEquals(ReadPreference.primary(), NamedReadPreference.PRIMARY.getPreference());
    assertEquals(
        ReadPreference.primaryPreferred(), NamedReadPreference.PRIMARY_PREFERRED.getPreference());
    assertEquals(ReadPreference.secondary(), NamedReadPreference.SECONDARY.getPreference());
    assertEquals(
        ReadPreference.secondaryPreferred(),
        NamedReadPreference.SECONDARY_PREFERRED.getPreference());
    assertEquals(ReadPreference.nearest(), NamedReadPreference.NEAREST.getPreference());
  }

  @Test
  void testGetPreferenceNames() {
    Collection<String> names = NamedReadPreference.getPreferenceNames();

    assertNotNull(names);
    assertEquals(5, names.size());
    assertTrue(names.contains("primary"));
    assertTrue(names.contains("primaryPreferred"));
    assertTrue(names.contains("secondary"));
    assertTrue(names.contains("secondaryPreferred"));
    assertTrue(names.contains("nearest"));
  }

  @Test
  void testByName() {
    assertEquals(NamedReadPreference.PRIMARY, NamedReadPreference.byName("primary"));
    assertEquals(
        NamedReadPreference.PRIMARY_PREFERRED, NamedReadPreference.byName("primaryPreferred"));
    assertEquals(NamedReadPreference.SECONDARY, NamedReadPreference.byName("secondary"));
    assertEquals(
        NamedReadPreference.SECONDARY_PREFERRED, NamedReadPreference.byName("secondaryPreferred"));
    assertEquals(NamedReadPreference.NEAREST, NamedReadPreference.byName("nearest"));
  }

  @Test
  void testByNameCaseInsensitive() {
    assertEquals(NamedReadPreference.PRIMARY, NamedReadPreference.byName("PRIMARY"));
    assertEquals(NamedReadPreference.PRIMARY, NamedReadPreference.byName("Primary"));
    assertEquals(NamedReadPreference.SECONDARY, NamedReadPreference.byName("SECONDARY"));
  }

  @Test
  void testByNameNotFound() {
    assertNull(NamedReadPreference.byName("nonexistent"));
    assertNull(NamedReadPreference.byName(""));
    assertNull(NamedReadPreference.byName(null));
  }

  @Test
  void testGetTaggableReadPreferencePrimaryPreferred() {
    Document tagSet = new Document("dc", "east");
    ReadPreference pref = NamedReadPreference.PRIMARY_PREFERRED.getTaggableReadPreference(tagSet);

    assertNotNull(pref);
    assertEquals("primaryPreferred", pref.getName());
  }

  @Test
  void testGetTaggableReadPreferenceSecondary() {
    Document tagSet = new Document("dc", "west");
    ReadPreference pref = NamedReadPreference.SECONDARY.getTaggableReadPreference(tagSet);

    assertNotNull(pref);
    assertEquals("secondary", pref.getName());
  }

  @Test
  void testGetTaggableReadPreferenceSecondaryPreferred() {
    Document tagSet = new Document("use", "production");
    ReadPreference pref = NamedReadPreference.SECONDARY_PREFERRED.getTaggableReadPreference(tagSet);

    assertNotNull(pref);
    assertEquals("secondaryPreferred", pref.getName());
  }

  @Test
  void testGetTaggableReadPreferenceNearest() {
    Document tagSet = new Document("rack", "r1");
    ReadPreference pref = NamedReadPreference.NEAREST.getTaggableReadPreference(tagSet);

    assertNotNull(pref);
    assertEquals("nearest", pref.getName());
  }

  @Test
  void testGetTaggableReadPreferencePrimaryReturnsNull() {
    Document tagSet = new Document("dc", "east");
    // PRIMARY doesn't support tag sets, should return null
    ReadPreference pref = NamedReadPreference.PRIMARY.getTaggableReadPreference(tagSet);
    assertNull(pref);
  }

  @Test
  void testGetTaggableReadPreferenceWithMultipleTags() {
    Document firstTagSet = new Document("dc", "east").append("rack", "r1");
    Document secondTagSet = new Document("dc", "west");

    ReadPreference pref =
        NamedReadPreference.NEAREST.getTaggableReadPreference(firstTagSet, secondTagSet);

    assertNotNull(pref);
    assertEquals("nearest", pref.getName());
  }

  @Test
  void testAllValuesHaveValidPreferences() {
    for (NamedReadPreference namedPref : NamedReadPreference.values()) {
      assertNotNull(namedPref.getPreference());
      assertNotNull(namedPref.getName());
    }
  }
}
