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
package org.apache.hop.pipeline.transforms.standardizephonenumber;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.i18n.phonenumbers.PhoneNumberUtil.PhoneNumberFormat;
import org.junit.jupiter.api.Test;

/** Unit test for {@link StandardizePhoneField} */
class StandardizePhoneFieldTest {

  @Test
  void defaultsUseE164Format() {
    StandardizePhoneField field = new StandardizePhoneField();
    assertEquals(PhoneNumberFormat.E164.name(), field.getNumberFormat());
    assertNull(field.getInputField());
    assertNull(field.getOutputField());
    assertNull(field.getCountryField());
    assertNull(field.getNumberTypeField());
    assertNull(field.getIsValidNumberField());
  }

  @Test
  void settersStripBlankValuesToNull() {
    StandardizePhoneField field = new StandardizePhoneField();
    field.setInputField("  ");
    field.setOutputField("");
    field.setCountryField("\t");
    field.setNumberTypeField("   ");
    field.setIsValidNumberField(" ");

    assertNull(field.getInputField());
    assertNull(field.getOutputField());
    assertNull(field.getCountryField());
    assertNull(field.getNumberTypeField());
    assertNull(field.getIsValidNumberField());
  }

  @Test
  void settersTrimSurroundingWhitespace() {
    StandardizePhoneField field = new StandardizePhoneField();
    field.setInputField(" PHONE ");
    field.setOutputField(" E164 ");
    field.setCountryField(" COUNTRY ");
    field.setNumberTypeField(" TYPE ");
    field.setIsValidNumberField(" VALID ");

    assertEquals("PHONE", field.getInputField());
    assertEquals("E164", field.getOutputField());
    assertEquals("COUNTRY", field.getCountryField());
    assertEquals("TYPE", field.getNumberTypeField());
    assertEquals("VALID", field.getIsValidNumberField());
  }

  @Test
  void cloneCopiesAllProperties() {
    StandardizePhoneField original = new StandardizePhoneField();
    original.setInputField("PHONE");
    original.setOutputField("PHONE_E164");
    original.setNumberFormat("NATIONAL");
    original.setCountryField("COUNTRY");
    original.setDefaultCountry("BE");
    original.setNumberTypeField("TYPE");
    original.setIsValidNumberField("VALID");

    StandardizePhoneField copy = (StandardizePhoneField) original.clone();

    assertNotSame(original, copy);
    assertEquals(original.getInputField(), copy.getInputField());
    assertEquals(original.getOutputField(), copy.getOutputField());
    assertEquals(original.getNumberFormat(), copy.getNumberFormat());
    assertEquals(original.getCountryField(), copy.getCountryField());
    assertEquals(original.getDefaultCountry(), copy.getDefaultCountry());
    assertEquals(original.getNumberTypeField(), copy.getNumberTypeField());
    assertEquals(original.getIsValidNumberField(), copy.getIsValidNumberField());
  }

  @Test
  void copyConstructorMatchesClone() {
    StandardizePhoneField original = new StandardizePhoneField();
    original.setInputField("PHONE");
    original.setOutputField("OUT");
    original.setDefaultCountry("FR");

    StandardizePhoneField copy = new StandardizePhoneField(original);
    assertEquals("PHONE", copy.getInputField());
    assertEquals("OUT", copy.getOutputField());
    assertEquals("FR", copy.getDefaultCountry());
  }
}
