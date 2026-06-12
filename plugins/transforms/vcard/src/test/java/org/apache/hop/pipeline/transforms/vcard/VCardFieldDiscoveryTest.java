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
import ezvcard.parameter.EmailType;
import ezvcard.parameter.TelephoneType;
import ezvcard.property.Email;
import ezvcard.property.FormattedName;
import ezvcard.property.StructuredName;
import ezvcard.property.Telephone;
import ezvcard.property.Uid;
import java.util.List;
import org.junit.jupiter.api.Test;

class VCardFieldDiscoveryTest {

  @Test
  void discoverFromSampleCard() throws Exception {
    VCard card = new VCard();
    card.setFormattedName(new FormattedName("Edwin Weber"));
    StructuredName name = new StructuredName();
    name.setFamily("Weber");
    name.setGiven("Edwin");
    card.setStructuredName(name);
    card.setUid(new Uid("partner-739"));
    Email email = new Email("test@example.com");
    email.getTypes().add(EmailType.INTERNET);
    card.addEmail(email);
    Telephone tel = new Telephone("+31-626766031");
    tel.getTypes().add(TelephoneType.VOICE);
    tel.getTypes().add(TelephoneType.WORK);
    card.addTelephoneNumber(tel);

    List<VCardFieldMapping> mappings = VCardFieldDiscovery.discoverFromCard(card);

    assertTrue(mappings.stream().anyMatch(m -> m.getProperty() == VCardPropertyType.FN));
    assertTrue(mappings.stream().anyMatch(m -> m.getProperty() == VCardPropertyType.UID));
    assertTrue(
        mappings.stream()
            .anyMatch(
                m ->
                    m.getProperty() == VCardPropertyType.EMAIL
                        && "INTERNET".equals(m.getParameterTypes())
                        && "email_internet".equals(m.getHopField())));
    assertTrue(
        mappings.stream()
            .anyMatch(
                m ->
                    m.getProperty() == VCardPropertyType.EMAIL_TYPE
                        && "INTERNET".equals(m.getParameterTypes())
                        && "email_internet_type".equals(m.getHopField())));
    assertTrue(
        mappings.stream()
            .anyMatch(
                m ->
                    m.getProperty() == VCardPropertyType.TEL
                        && "VOICE,WORK".equals(m.getParameterTypes())
                        && "tel_voice_work".equals(m.getHopField())));
    assertTrue(
        mappings.stream()
            .anyMatch(
                m ->
                    m.getProperty() == VCardPropertyType.TEL_TYPE
                        && "VOICE,WORK".equals(m.getParameterTypes())
                        && "tel_voice_work_type".equals(m.getHopField())));
    assertEquals(
        "email_internet",
        VCardFieldDiscovery.defaultHopFieldName(VCardPropertyType.EMAIL, "INTERNET"));
    assertEquals(
        "email_type", VCardFieldDiscovery.defaultHopFieldName(VCardPropertyType.EMAIL_TYPE, null));
    assertEquals(
        "tel_type", VCardFieldDiscovery.defaultHopFieldName(VCardPropertyType.TEL_TYPE, null));
  }

  @Test
  void discoverFromVcardText() throws Exception {
    String sample =
        """
        BEGIN:VCARD
        VERSION:3.0
        FN:Hop Test
        UID:hop-test-001
        EMAIL;TYPE=INTERNET:test@example.com
        END:VCARD
        """;
    List<VCardFieldMapping> mappings =
        VCardFieldDiscovery.discoverFromCards(VCardMapper.parseAll(sample));
    assertEquals(4, mappings.size());
  }
}
