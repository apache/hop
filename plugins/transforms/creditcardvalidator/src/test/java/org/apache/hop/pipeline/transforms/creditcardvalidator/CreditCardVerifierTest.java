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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

  @Test
  void testBinDatabaseValidAndTypeSpecificError(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("bins.csv");
    try (OutputStream out = Files.newOutputStream(file)) {
      out.write(
          ("BIN,CARDTYPE,ISSUERS,COUNTRY\n" + "512003,MASTERCARD,BCA,INDONESIA\n")
              .getBytes(StandardCharsets.UTF_8));
    }

    BinDatabase db = new BinDatabase();
    db.load(
        new Variables(), file.toUri().toString(), "", new ArrayList<>(), ",", "\"", "UTF-8", true);

    // Valid Luhn number for the BIN 512003 length 16.
    ReturnIndicator valid = CreditCardVerifier.checkCC("5120031234567898", db);
    assertTrue(valid.CardValid);
    assertEquals("MASTERCARD", valid.CardType);

    // Same BIN but failing Luhn -> type-specific message.
    ReturnIndicator invalid = CreditCardVerifier.checkCC("5120031234567890", db);
    assertFalse(invalid.CardValid);
    assertNotNull(invalid.UnValidMsg);
    assertTrue(invalid.UnValidMsg.contains("MASTERCARD"));
  }
}
