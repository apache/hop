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

package org.apache.hop.pipeline.transforms.validator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link HopValidatorException}. */
class HopValidatorExceptionTest {

  private Validator validator;

  @BeforeEach
  void setUp() {
    validator = mock(Validator.class);
    when(validator.resolve(anyString())).thenAnswer(invocation -> invocation.getArgument(0));
  }

  @Test
  void exposesCodeFieldNameAndValidation() {
    Validation field = new Validation();
    field.setName("rule");
    HopValidatorException exception =
        new HopValidatorException(
            validator,
            field,
            HopValidatorException.ERROR_NULL_VALUE_NOT_ALLOWED,
            "null not allowed",
            "value");

    assertEquals(HopValidatorException.ERROR_NULL_VALUE_NOT_ALLOWED, exception.getCode());
    assertEquals("value", exception.getFieldName());
    assertSame(field, exception.getValidatorField());
    assertTrue(exception.getMessage().contains("null not allowed"));
  }

  @Test
  void getCodeDescUsesBuiltInCodeWhenNoCustomErrorCode() {
    Validation field = new Validation();
    HopValidatorException exception =
        new HopValidatorException(
            validator,
            field,
            HopValidatorException.ERROR_LONGER_THAN_MAXIMUM_LENGTH,
            "too long",
            "value");

    assertEquals("KVD002", exception.getCodeDesc());
  }

  @Test
  void getCodeDescUsesResolvedCustomErrorCode() {
    Validation field = new Validation();
    field.setErrorCode("${ERR_CODE}");
    when(validator.resolve("${ERR_CODE}")).thenReturn("CUSTOM_CODE");

    HopValidatorException exception =
        new HopValidatorException(
            validator,
            field,
            HopValidatorException.ERROR_VALUE_NOT_IN_LIST,
            "not in list",
            "value");

    assertEquals("CUSTOM_CODE", exception.getCodeDesc());
  }

  @Test
  void getMessageUsesResolvedCustomErrorDescription() {
    Validation field = new Validation();
    field.setErrorDescription("${ERR_DESC}");
    when(validator.resolve("${ERR_DESC}")).thenReturn("custom description");

    HopValidatorException exception =
        new HopValidatorException(
            validator,
            field,
            HopValidatorException.ERROR_SHORTER_THAN_MINIMUM_LENGTH,
            "built-in message",
            "value");

    assertTrue(exception.getMessage().contains("custom description"));
  }

  @Test
  void getMessageFallsBackToBuiltInMessage() {
    Validation field = new Validation();
    HopValidatorException exception =
        new HopValidatorException(
            validator,
            field,
            HopValidatorException.ERROR_ONLY_NULL_VALUE_ALLOWED,
            "built-in message",
            "value");

    assertTrue(exception.getMessage().contains("built-in message"));
  }

  @Test
  void allBuiltInErrorCodesAreMapped() {
    Validation field = new Validation();
    String[] expected = {
      "KVD000", "KVD001", "KVD002", "KVD003", "KVD004", "KVD005", "KVD006", "KVD007", "KVD008",
      "KVD009", "KVD010", "KVD011", "KVD012", "KVD013", "KVD014", "KVD015",
    };

    for (int code = 0; code < expected.length; code++) {
      HopValidatorException exception =
          new HopValidatorException(validator, field, code, "msg", "field");
      assertEquals(expected[code], exception.getCodeDesc(), "code " + code);
    }
  }
}
