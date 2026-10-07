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

package org.apache.hop.pipeline.transforms.creditcardvalidator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class CardSchemeTest {

  @Test
  void testVisaLengths() {
    assertTrue(CardScheme.isValidLength("411111", 13));
    assertTrue(CardScheme.isValidLength("411111", 16));
    assertTrue(CardScheme.isValidLength("411111", 19));
    assertFalse(CardScheme.isValidLength("411111", 15));
    assertFalse(CardScheme.isValidLength("411111", 14));
  }

  @Test
  void testMastercardLengths() {
    assertTrue(CardScheme.isValidLength("511111", 16));
    assertFalse(CardScheme.isValidLength("511111", 15));
    assertTrue(CardScheme.isValidLength("222100", 16));
  }

  @Test
  void testAmexLengths() {
    assertTrue(CardScheme.isValidLength("341111", 15));
    assertFalse(CardScheme.isValidLength("341111", 16));
  }

  @Test
  void testDiscoverLengths() {
    assertTrue(CardScheme.isValidLength("601100", 16));
    assertTrue(CardScheme.isValidLength("601100", 19));
    assertTrue(CardScheme.isValidLength("651111", 17));
    assertFalse(CardScheme.isValidLength("601100", 15));
  }

  @Test
  void testJcbLengths() {
    assertTrue(CardScheme.isValidLength("352811", 16));
    assertTrue(CardScheme.isValidLength("352811", 19));
    assertFalse(CardScheme.isValidLength("352811", 15));
  }

  @Test
  void testUnknownSchemeAcceptsAnyLength() {
    assertTrue(CardScheme.isValidLength("999999", 16));
    assertTrue(CardScheme.isValidLength("999999", 13));
  }
}
