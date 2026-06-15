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
package org.apache.hop.pipeline.transforms.vcard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.Test;

class VCardStreamFieldMatcherTest {

  @Test
  void guessesCommonFieldNames() {
    assertProperty(VCardPropertyType.FN, "fn", null, VCardStreamFieldMatcher.guessMapping("fn"));
    assertProperty(
        VCardPropertyType.EMAIL, "email", null, VCardStreamFieldMatcher.guessMapping("email"));
    assertProperty(
        VCardPropertyType.EMAIL_TYPE,
        "email_type",
        null,
        VCardStreamFieldMatcher.guessMapping("email_type"));
    assertProperty(
        VCardPropertyType.EMAIL,
        "email_internet",
        "INTERNET",
        VCardStreamFieldMatcher.guessMapping("email_internet"));
    assertProperty(
        VCardPropertyType.TEL,
        "tel_voice_work",
        "VOICE,WORK",
        VCardStreamFieldMatcher.guessMapping("tel_voice_work"));
  }

  @Test
  void suggestsMappingsFromStream() {
    RowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("fn"));
    row.addValueMeta(new ValueMetaString("email"));
    row.addValueMeta(new ValueMetaString("email_type"));
    row.addValueMeta(new ValueMetaString("unknown_field"));

    assertEquals(3, VCardStreamFieldMatcher.suggestFromStream(row).size());
  }

  @Test
  void ignoresUnknownFields() {
    assertNull(VCardStreamFieldMatcher.guessMapping("customer_reference"));
  }

  private static void assertProperty(
      VCardPropertyType property,
      String hopField,
      String parameterTypes,
      VCardFieldMapping mapping) {
    assertNotNull(mapping);
    assertEquals(property, mapping.getProperty());
    assertEquals(hopField, mapping.getHopField());
    assertEquals(parameterTypes, mapping.getParameterTypes());
  }
}
