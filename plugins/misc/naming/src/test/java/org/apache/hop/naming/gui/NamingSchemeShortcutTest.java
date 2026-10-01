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

package org.apache.hop.naming.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.naming.metadata.NamingScheme;
import org.apache.hop.naming.metadata.NamingSchemeType;
import org.apache.hop.naming.metadata.NamingWordSeparator;
import org.apache.hop.ui.core.widget.NamingSchemeTypes;
import org.junit.jupiter.api.Test;

class NamingSchemeShortcutTest {

  @Test
  void skipEmptyNullMarkerAndVariables() {
    assertTrue(NamingSchemeShortcut.shouldSkip(null));
    assertTrue(NamingSchemeShortcut.shouldSkip(""));
    assertTrue(NamingSchemeShortcut.shouldSkip("<null>"));
    assertTrue(NamingSchemeShortcut.shouldSkip("${TABLE_NAME}"));
    assertTrue(NamingSchemeShortcut.shouldSkip("prefix_${VAR}_suffix"));
    assertFalse(NamingSchemeShortcut.shouldSkip("Order ID"));
  }

  @Test
  void rememberLastUsedIgnoresNulls() {
    NamingSchemeShortcut shortcut = NamingSchemeShortcut.INSTANCE;
    shortcut.rememberLastUsed(null, "scheme");
    shortcut.rememberLastUsed("file", null);
    shortcut.rememberLastUsed("", "scheme");
    shortcut.rememberLastUsed("file", "scheme");
  }

  @Test
  void newSchemeForTypeSetsActionDefaults() {
    NamingScheme scheme = NamingSchemeShortcut.newSchemeForType(NamingSchemeTypes.HOP_ACTION);
    assertEquals(NamingSchemeTypes.HOP_ACTION, scheme.getType());
    assertEquals(NamingWordSeparator.SPACE.getCode(), scheme.getWordSeparator());
    assertTrue(scheme.isCapitalizeFirstWord());
  }

  @Test
  void newSchemeForTypeKeepsFieldDefaults() {
    NamingScheme scheme = NamingSchemeShortcut.newSchemeForType(NamingSchemeTypes.HOP_FIELD);
    assertEquals(NamingSchemeTypes.HOP_FIELD, scheme.getType());
    assertEquals(NamingWordSeparator.UNDERSCORE.getCode(), scheme.getWordSeparator());
    assertFalse(scheme.isCapitalizeFirstWord());
  }

  @Test
  void uniqueSchemeNameAddsSuffixWhenTaken() {
    assertEquals(
        "Hop action names", NamingSchemeShortcut.uniqueSchemeName("Hop action names", List.of()));
    assertEquals(
        "Hop action names 2",
        NamingSchemeShortcut.uniqueSchemeName("Hop action names", List.of("Hop action names")));
    assertEquals(
        "Hop action names 3",
        NamingSchemeShortcut.uniqueSchemeName(
            "Hop action names", List.of("Hop action names", "Hop action names 2")));
  }

  @Test
  void typeCodesMatchUiConstants() {
    assertEquals(NamingSchemeTypes.GENERAL, NamingSchemeType.GENERAL.getCode());
    assertEquals(NamingSchemeTypes.HOP_FIELD, NamingSchemeType.HOP_FIELD.getCode());
    assertEquals(NamingSchemeTypes.HOP_TRANSFORM, NamingSchemeType.HOP_TRANSFORM.getCode());
    assertEquals(NamingSchemeTypes.HOP_ACTION, NamingSchemeType.HOP_ACTION.getCode());
    assertEquals(NamingSchemeTypes.HOP_PIPELINE, NamingSchemeType.HOP_PIPELINE.getCode());
    assertEquals(NamingSchemeTypes.HOP_WORKFLOW, NamingSchemeType.HOP_WORKFLOW.getCode());
    assertEquals(NamingSchemeTypes.HOP_METADATA, NamingSchemeType.HOP_METADATA.getCode());
    assertEquals(NamingSchemeTypes.HOP_VARIABLE, NamingSchemeType.HOP_VARIABLE.getCode());
    assertEquals(NamingSchemeTypes.DATABASE_TABLE, NamingSchemeType.DATABASE_TABLE.getCode());
    assertEquals(NamingSchemeTypes.DATABASE_COLUMN, NamingSchemeType.DATABASE_COLUMN.getCode());
    assertEquals(NamingSchemeTypes.FILE, NamingSchemeType.FILE.getCode());
    assertEquals(NamingSchemeTypes.FOLDER, NamingSchemeType.FOLDER.getCode());
  }
}
