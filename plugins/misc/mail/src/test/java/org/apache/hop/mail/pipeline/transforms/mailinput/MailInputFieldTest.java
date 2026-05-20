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
package org.apache.hop.mail.pipeline.transforms.mailinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.junit.jupiter.api.Test;

class MailInputFieldTest {

  @Test
  void setColumnIntStoresLocalizedDescriptionAndGetColumnIndexInverts() {
    MailInputField field = new MailInputField();
    field.setColumn(MailInputField.COLUMN_HEADER);

    // setColumn(int) stores the localized description, getColumn() returns the String.
    assertEquals(MailInputField.ColumnDesc[MailInputField.COLUMN_HEADER], field.getColumn());

    // getColumnIndex resolves back to the int constant.
    assertEquals(MailInputField.COLUMN_HEADER, field.getColumnIndex());
  }

  @Test
  void getColumnIndexIsIndependentOfFieldName() {
    // Regression guard: getColumnIndex used to read from `name`, which made it impossible
    // to use a HEADER field whose name is the actual header to look up.
    MailInputField field = new MailInputField();
    field.setColumn(MailInputField.COLUMN_HEADER);
    field.setName("X-Custom-Header");

    assertEquals(MailInputField.COLUMN_HEADER, field.getColumnIndex());
  }

  @Test
  void columnByCodeRoundTripsThroughGetColumnCode() {
    for (int i = 0; i < MailInputField.ColumnCode.length; i++) {
      String code = MailInputField.getColumnCode(i);
      assertEquals(i, MailInputField.getColumnByCode(code), "round-trip for column " + i);
    }
  }

  @Test
  void getColumnByCodeReturnsZeroForUnknown() {
    assertEquals(0, MailInputField.getColumnByCode(null));
    assertEquals(0, MailInputField.getColumnByCode("not-a-real-code"));
  }

  @Test
  void getColumnByDescReturnsZeroForUnknown() {
    assertEquals(0, MailInputField.getColumnByDesc(null));
    assertEquals(0, MailInputField.getColumnByDesc("not-a-real-description"));
  }

  @Test
  void getColumnCodeClampsOutOfRangeIndex() {
    // Returns the first entry as a fallback rather than throwing.
    assertEquals(MailInputField.ColumnCode[0], MailInputField.getColumnCode(-1));
    assertEquals(
        MailInputField.ColumnCode[0],
        MailInputField.getColumnCode(MailInputField.ColumnCode.length));
  }

  @Test
  void cloneProducesIndependentCopyWithSameValues() {
    MailInputField original = new MailInputField();
    original.setName("subject");
    original.setColumn(MailInputField.COLUMN_SUBJECT);

    MailInputField copy = (MailInputField) original.clone();

    assertNotSame(original, copy);
    assertEquals(original.getName(), copy.getName());
    assertEquals(original.getColumn(), copy.getColumn());
    assertEquals(original.getColumnIndex(), copy.getColumnIndex());
  }

  @Test
  void setColumnStringSetsBareValue() {
    MailInputField field = new MailInputField();
    field.setColumn("freeform");
    assertEquals("freeform", field.getColumn());
    // unknown desc resolves to 0 (MESSAGE_NR fallback).
    assertEquals(MailInputField.COLUMN_MESSAGE_NR, field.getColumnIndex());
  }
}
