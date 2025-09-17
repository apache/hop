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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class CreditCardVerifierTest {

  @Test
  void testStatics() {
    int totalCardNames = -1;
    int totalNotValidCardNames = -1;
    for (int i = 0; i < 50; i++) {
      String result = CreditCardVerifier.getCardName(i);
      if (result == null) {
        totalCardNames = i - 1;
        break;
      }
    }
    for (int i = 0; i < 50; i++) {
      String result = CreditCardVerifier.getNotValidCardNames(i);
      if (result == null) {
        totalNotValidCardNames = i - 1;
        break;
      }
    }
    assertNotSame(-1, totalCardNames);
    assertNotSame(-1, totalNotValidCardNames);
    assertEquals(totalCardNames, totalNotValidCardNames);
  }

  @Test
  void testIsNumber() {
    assertFalse(CreditCardVerifier.isNumber(""));
    assertFalse(CreditCardVerifier.isNumber("a"));
    assertTrue(CreditCardVerifier.isNumber("1"));
    assertTrue(CreditCardVerifier.isNumber("1.01"));
  }
}
