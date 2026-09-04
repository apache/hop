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

package org.apache.hop.ui.core.dialog;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The rule that decides which entry typing a full name selects.
 *
 * <p>Relevance scoring alone put "If null" above "Null if" and "Spark lake table input" above
 * "Table input", so typing a transform's own name and pressing Enter created a different one.
 */
class ContextDialogTest {

  @Test
  @DisplayName("a name equal to the search text is an exact match")
  void matchesTheSameName() {
    assertTrue(ContextDialog.isExactName("Null if", "Null if"));
  }

  @Test
  @DisplayName("case and surrounding space do not matter")
  void ignoresCaseAndSpace() {
    assertTrue(ContextDialog.isExactName("Avro decode", "avro DECODE"));
    assertTrue(ContextDialog.isExactName("  Table input  ", "Table input"));
    assertTrue(ContextDialog.isExactName("Table input", "  Table input  "));
  }

  @Test
  @DisplayName("a name that merely contains the search text is not an exact match")
  void doesNotMatchASubstring() {
    assertFalse(ContextDialog.isExactName("Spark lake table input", "Table input"));
    assertFalse(ContextDialog.isExactName("If null", "Null if"));
  }

  @Test
  @DisplayName("an empty search matches nothing, so the ordinary ranking is left alone")
  void emptySearchMatchesNothing() {
    assertFalse(ContextDialog.isExactName("Table input", ""));
    assertFalse(ContextDialog.isExactName("Table input", "   "));
    assertFalse(ContextDialog.isExactName("Table input", null));
  }

  @Test
  @DisplayName("a missing name matches nothing")
  void nullNameMatchesNothing() {
    assertFalse(ContextDialog.isExactName(null, "Table input"));
  }
}
