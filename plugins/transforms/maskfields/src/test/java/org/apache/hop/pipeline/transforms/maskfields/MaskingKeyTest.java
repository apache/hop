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

package org.apache.hop.pipeline.transforms.maskfields;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.Test;

class MaskingKeyTest {

  @Test
  void aNumberKeyDoesNotDependOnTheConversionMask() throws Exception {
    MaskingKey key = MaskingKey.plain();
    ValueMetaNumber plain = new ValueMetaNumber("amount");
    ValueMetaNumber formatted = new ValueMetaNumber("amount");
    formatted.setConversionMask("#,##0.00");
    assertEquals("1234.5", key.normalize(plain, 1234.5));
    assertEquals("1234.5", key.normalize(formatted, 1234.5));

    ValueMetaInteger integer = new ValueMetaInteger("id");
    integer.setConversionMask("000000");
    assertEquals("42", key.normalize(integer, 42L));
    assertEquals("000042", MaskingKey.legacyKey(integer, 42L));

    assertEquals("10", key.normalize(new ValueMetaBigNumber("big"), new BigDecimal("10.000")));
  }

  @Test
  void trimAndIgnoreCaseAreOptional() throws Exception {
    ValueMetaString name = new ValueMetaString("name");
    assertEquals(" Matt ", MaskingKey.plain().normalize(name, " Matt "));
    assertEquals("matt", new MaskingKey(true, true, null).normalize(name, " Matt "));
    assertEquals("", new MaskingKey(true, false, null).normalize(name, "   "));
  }

  @Test
  void aHashSecretThatDoesNotResolveIsAnError() {
    Variables variables = new Variables();
    variables.setVariable("EMPTY_SECRET", "");
    for (String secret : new String[] {"${NOT_SET}", "${EMPTY_SECRET}", "%%NOT_SET%%"}) {
      MaskingPattern pattern = databasePattern(secret);
      HopException e =
          assertThrows(HopException.class, () -> MaskingKey.forPattern(pattern, variables));
      assertTrue(e.getMessage().contains("Names"), e.getMessage());
    }
  }

  @Test
  void aResolvedHashSecretHashesDatabaseKeysOnly() throws Exception {
    Variables variables = new Variables();
    variables.setVariable("SECRET", "s3cret");
    String expected = new MaskingKey(false, false, "s3cret").storeKey("Matt");
    assertEquals(
        expected, MaskingKey.forPattern(databasePattern("${SECRET}"), variables).storeKey("Matt"));

    MaskingPattern memory = databasePattern("${NOT_SET}");
    memory.setStorage(MaskingStorage.MEMORY);
    assertEquals("Matt", MaskingKey.forPattern(memory, variables).storeKey("Matt"));
    assertEquals("Matt", MaskingKey.forPattern(databasePattern(""), variables).storeKey("Matt"));
  }

  private static MaskingPattern databasePattern(String secret) {
    MaskingPattern pattern = new MaskingPattern();
    pattern.setName("Names");
    pattern.setStorage(MaskingStorage.DATABASE);
    pattern.setHashSecret(secret);
    return pattern;
  }

  @Test
  void aSecretHashesTheStoredKey() throws Exception {
    MaskingKey key = new MaskingKey(false, false, "s3cret");
    String hashed = key.storeKey("Matt");
    assertTrue(hashed.startsWith(MaskingKey.HMAC_PREFIX));
    assertEquals(MaskingKey.HMAC_PREFIX.length() + 64, hashed.length());
    assertEquals(hashed, new MaskingKey(false, false, "s3cret").storeKey("Matt"));
    assertNotEquals(hashed, new MaskingKey(false, false, "other").storeKey("Matt"));
    assertEquals("Matt", MaskingKey.plain().storeKey("Matt"));
  }
}
