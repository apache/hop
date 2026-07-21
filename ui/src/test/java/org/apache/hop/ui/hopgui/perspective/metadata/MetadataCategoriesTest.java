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

package org.apache.hop.ui.hopgui.perspective.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.metadata.api.HopMetadataCategory;
import org.junit.jupiter.api.Test;

class MetadataCategoriesTest {

  @Test
  void normalize_nullAndEmptyBecomeOther() {
    assertEquals(MetadataCategories.OTHER, MetadataCategories.normalize(null));
    assertEquals(MetadataCategories.OTHER, MetadataCategories.normalize(""));
    assertEquals(
        HopMetadataCategory.CONNECTIONS,
        MetadataCategories.normalize(HopMetadataCategory.CONNECTIONS));
  }

  @Test
  void orderOf_knownCategoriesFollowDeclarationOrder() {
    assertEquals(0, MetadataCategories.orderOf(HopMetadataCategory.CONNECTIONS));
    assertEquals(1, MetadataCategories.orderOf(HopMetadataCategory.FILE_STORAGE));
    // Connections sorts before file storage, file storage before variables.
    assertTrue(
        MetadataCategories.orderOf(HopMetadataCategory.CONNECTIONS)
            < MetadataCategories.orderOf(HopMetadataCategory.FILE_STORAGE));
    assertTrue(
        MetadataCategories.orderOf(HopMetadataCategory.FILE_STORAGE)
            < MetadataCategories.orderOf(HopMetadataCategory.VARIABLES));
  }

  @Test
  void orderOf_unknownComesAfterKnownButBeforeOther() {
    int known = MetadataCategories.orderOf(HopMetadataCategory.VARIABLES);
    int unknown = MetadataCategories.orderOf("some-third-party-category");
    int other = MetadataCategories.orderOf(MetadataCategories.OTHER);

    assertTrue(known < unknown, "a known category must sort before an unknown one");
    assertTrue(unknown < other, "an unknown category must sort before the Other bucket");
    assertEquals(Integer.MAX_VALUE, other);
    // null normalizes to the Other bucket.
    assertEquals(other, MetadataCategories.orderOf(null));
  }

  @Test
  void imageFor_knownReturnsConfiguredIconOthersFallBack() {
    assertEquals(
        "ui/images/connection.svg", MetadataCategories.imageFor(HopMetadataCategory.CONNECTIONS));
    assertEquals(
        "ui/images/location.svg", MetadataCategories.imageFor(HopMetadataCategory.FILE_STORAGE));
    // Unknown and Other fall back to the generic metadata icon.
    assertEquals(
        "ui/images/metadata.svg", MetadataCategories.imageFor("some-third-party-category"));
    assertEquals("ui/images/metadata.svg", MetadataCategories.imageFor(MetadataCategories.OTHER));
  }

  @Test
  void labelFor_unknownReturnsRawIdKnownReturnsTranslatedLabel() {
    // An unknown id is shown verbatim (no i18n key exists for it).
    assertEquals(
        "some-third-party-category", MetadataCategories.labelFor("some-third-party-category"));
    // A known id resolves to a real label, not the raw id or a blank string.
    String connectionsLabel = MetadataCategories.labelFor(HopMetadataCategory.CONNECTIONS);
    assertFalse(connectionsLabel == null || connectionsLabel.isEmpty());
    assertFalse(HopMetadataCategory.CONNECTIONS.equals(connectionsLabel));
  }
}
