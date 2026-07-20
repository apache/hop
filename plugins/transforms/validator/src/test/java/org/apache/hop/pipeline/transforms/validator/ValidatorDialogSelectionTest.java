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

package org.apache.hop.pipeline.transforms.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for non-UI logic in {@link ValidatorDialog}. Full SWT dialog coverage is out of scope
 * for headless unit tests.
 */
class ValidatorDialogSelectionTest {

  private ValidatorDialog dialog;

  @BeforeEach
  void setUp() {
    dialog = mock(ValidatorDialog.class);
    when(dialog.spacesValidation(anyString())).thenCallRealMethod();
    when(dialog.spacesValidation(null)).thenCallRealMethod();
    when(dialog.trailingSpacesValidation(anyString())).thenCallRealMethod();
    when(dialog.trailingSpacesValidation(null)).thenCallRealMethod();
  }

  @Test
  void indexAfterRemovalSelectsSameIndexWhenItemsRemainAfter() {
    // Remove index 0 from [A,B,C] -> remaining [B,C], select B (index 0)
    assertEquals(0, ValidatorDialog.indexAfterRemoval(0, 2));
  }

  @Test
  void indexAfterRemovalSelectsPreviousWhenLastItemRemoved() {
    // Remove index 2 from [A,B,C] -> remaining [A,B], select B (index 1)
    assertEquals(1, ValidatorDialog.indexAfterRemoval(2, 2));
  }

  @Test
  void indexAfterRemovalSelectsMiddleNeighbor() {
    // Remove index 1 from [A,B,C] -> remaining [A,C], select C (index 1)
    assertEquals(1, ValidatorDialog.indexAfterRemoval(1, 2));
  }

  @Test
  void indexAfterRemovalReturnsMinusOneWhenEmpty() {
    assertEquals(-1, ValidatorDialog.indexAfterRemoval(0, 0));
  }

  @Test
  void indexAfterRemovalKeepsSingleRemainingItem() {
    assertEquals(0, ValidatorDialog.indexAfterRemoval(0, 1));
    assertEquals(0, ValidatorDialog.indexAfterRemoval(5, 1));
  }

  @Test
  void spacesValidationDetectsOnlySpaces() {
    assertTrue(dialog.spacesValidation("   "));
    assertFalse(dialog.spacesValidation("a"));
    assertFalse(dialog.spacesValidation(" a "));
    assertFalse(dialog.spacesValidation(""));
    assertFalse(dialog.spacesValidation(null));
  }

  @Test
  void trailingSpacesValidationDetectsTrailingSpace() {
    assertTrue(dialog.trailingSpacesValidation("value "));
    assertTrue(dialog.trailingSpacesValidation(" "));
    assertFalse(dialog.trailingSpacesValidation("value"));
    assertFalse(dialog.trailingSpacesValidation(""));
    assertFalse(dialog.trailingSpacesValidation(null));
  }
}
