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
import static org.junit.jupiter.api.Assertions.assertTrue;

import ezvcard.VCard;
import ezvcard.VCardVersion;
import java.util.List;
import org.apache.hop.core.row.RowMeta;
import org.junit.jupiter.api.Test;

class VCardMapperTest {

  private static final String SAMPLE =
      """
      BEGIN:VCARD
      VERSION:3.0
      FN:Hop Test
      N:Test;Hop;;;
      UID:hop-test-001
      EMAIL;TYPE=INTERNET:test@example.com
      TEL;TYPE=VOICE,WORK:+1-555-0100
      END:VCARD
      """;

  @Test
  void parseAndExtractEmailWithTypes() throws Exception {
    VCard card = VCardMapper.parseAll(SAMPLE).get(0);
    VCardFieldMapping emailMapping =
        new VCardFieldMapping(VCardPropertyType.EMAIL, "email", "INTERNET");
    assertEquals("test@example.com", VCardMapper.extractValue(card, emailMapping));
    VCardFieldMapping emailTypeMapping =
        new VCardFieldMapping(VCardPropertyType.EMAIL_TYPE, "email_type", "INTERNET");
    assertEquals("INTERNET", VCardMapper.extractValue(card, emailTypeMapping));
    VCardFieldMapping telMapping =
        new VCardFieldMapping(VCardPropertyType.TEL, "tel", "VOICE,WORK");
    assertEquals("+1-555-0100", VCardMapper.extractValue(card, telMapping));
    VCardFieldMapping telTypeMapping =
        new VCardFieldMapping(VCardPropertyType.TEL_TYPE, "tel_type", "VOICE,WORK");
    assertEquals("VOICE,WORK", VCardMapper.extractValue(card, telTypeMapping));
  }

  @Test
  void roundTripPreservesMappedFields() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("fn"));
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("uid"));
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("email"));
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("email_type"));

    VCard card = new VCard();
    Object[] row = new Object[] {"Hop Test", "hop-test-001", "test@example.com", "INTERNET,WORK"};

    List<VCardFieldMapping> mappings =
        List.of(
            new VCardFieldMapping(VCardPropertyType.FN, "fn"),
            new VCardFieldMapping(VCardPropertyType.UID, "uid"),
            new VCardFieldMapping(VCardPropertyType.EMAIL, "email", "INTERNET,WORK"),
            new VCardFieldMapping(VCardPropertyType.EMAIL_TYPE, "email_type", "INTERNET,WORK"));
    VCardMapper.applyMappings(card, row, rowMeta, mappings, false, false);

    String text = VCardMapper.write(card, VCardVersion.V3_0);
    VCard parsed = VCardMapper.parseAll(text).get(0);
    assertEquals("Hop Test", VCardMapper.extractValue(parsed, mappings.get(0)));
    assertEquals("hop-test-001", VCardMapper.extractValue(parsed, mappings.get(1)));
    assertEquals("test@example.com", VCardMapper.extractValue(parsed, mappings.get(2)));
    assertEquals("INTERNET,WORK", VCardMapper.extractValue(parsed, mappings.get(3)));
  }

  @Test
  void buildVCardFromRow() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("fn"));
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("uid"));
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("email"));
    rowMeta.addValueMeta(new org.apache.hop.core.row.value.ValueMetaString("email_type"));

    VCard card = new VCard();
    Object[] row = new Object[] {"Hop Test", "hop-test-001", "test@example.com", "INTERNET,WORK"};

    List<VCardFieldMapping> mappings =
        List.of(
            new VCardFieldMapping(VCardPropertyType.FN, "fn"),
            new VCardFieldMapping(VCardPropertyType.UID, "uid"),
            new VCardFieldMapping(VCardPropertyType.EMAIL, "email", "INTERNET,WORK"),
            new VCardFieldMapping(VCardPropertyType.EMAIL_TYPE, "email_type", "INTERNET,WORK"));
    VCardMapper.applyMappings(card, row, rowMeta, mappings, true, false);

    String text = VCardMapper.write(card, VCardVersion.V3_0);
    assertTrue(text.contains("FN:Hop Test"));
    assertTrue(text.contains("UID:hop-test-001"));
    assertTrue(text.toUpperCase().contains("EMAIL;TYPE=INTERNET,WORK:TEST@EXAMPLE.COM"));
  }
}
