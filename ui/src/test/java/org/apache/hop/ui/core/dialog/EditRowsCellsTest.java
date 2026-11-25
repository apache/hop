/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.dialog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Locale;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.ui.core.widget.NullPlaceholder;
import org.junit.jupiter.api.Test;

/**
 * What {@code EditRowsDialog} does with a cell on OK, without a display. The integer field uses the
 * {@code #} format mask from the report.
 */
class EditRowsCellsTest {

  @Test
  void aValueTypedOverTheNullPlaceholderIsKept() throws Exception {
    assertEquals(42L, convert("42", true));
  }

  @Test
  void thePlaceholderItselfStaysNull() throws Exception {
    assertNull(convert(NullPlaceholder.TEXT, true));
  }

  @Test
  void thePlaceholderTextWithoutTheNullColorIsNotANull() {
    HopException e = assertThrows(HopException.class, () -> convert(NullPlaceholder.TEXT, false));
    assertTrue(e.getMessage().contains(NullPlaceholder.TEXT));
  }

  @Test
  void surroundingWhitespaceIsTrimmedBeforeTheMaskIsApplied() throws Exception {
    assertEquals(42L, convert("  42  ", false));
  }

  @Test
  void aPaddedStringIsKept() throws Exception {
    IValueMeta name = new ValueMetaString("name");
    IValueMeta parse = new ValueMetaString("name");

    assertEquals("  ABC  ", EditRowsCells.convertCell(name, parse, "  ABC  ", false, 1));
  }

  @Test
  void aValueThatDoesNotMatchTheMaskNamesTheRowFieldValueAndMask() {
    // '#' does not include a grouping separator. Pin the regional settings so ',' is that
    // separator and the whole string has to match the mask.
    Locale previous = Locale.getDefault();
    Locale.setDefault(Locale.US);
    try {
      HopException e = assertThrows(HopException.class, () -> convert("1,234", false));

      String message = EditRowsCells.errorDialogMessage(e);
      assertTrue(message.contains("3"), message);
      assertTrue(message.contains("id"), message);
      assertTrue(message.contains("1,234"), message);
      assertTrue(message.contains("#"), message);
      assertTrue(message.toLowerCase().contains("format mask"), message);
      assertFalse(message.contains("\n"), message);
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  void aCommitClearsTheNullColorUnlessTheTextIsStillThePlaceholder() {
    assertFalse(NullPlaceholder.clearNullColor(NullPlaceholder.TEXT));
    assertTrue(NullPlaceholder.clearNullColor("42"));
    assertTrue(NullPlaceholder.clearNullColor("  42  "));
    assertTrue(NullPlaceholder.clearNullColor(""));
    assertTrue(NullPlaceholder.clearNullColor(null));
    assertTrue(NullPlaceholder.isPlaceholder(NullPlaceholder.TEXT, true));
    assertFalse(NullPlaceholder.isPlaceholder("42", true));
    assertFalse(NullPlaceholder.isPlaceholder(NullPlaceholder.TEXT, false));
  }

  private static Object convert(String cellText, boolean foregroundIsNullColor)
      throws HopException {
    IValueMeta id = new ValueMetaInteger("id");
    id.setConversionMask("#");
    IValueMeta parse = new ValueMetaString("id");
    parse.setConversionMask(id.getConversionMask());
    parse.setDecimalSymbol(id.getDecimalSymbol());
    parse.setGroupingSymbol(id.getGroupingSymbol());
    return EditRowsCells.convertCell(id, parse, cellText, foregroundIsNullColor, 3);
  }
}
