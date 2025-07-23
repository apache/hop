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

package org.apache.hop.pipeline.transforms.stringcut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.stream.Stream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StringCutTest {

  private TransformMockHelper<StringCutMeta, StringCutData> smh;
  private StringCut stringCut;
  private Method cutStringMethod;

  @BeforeAll
  static void initEnvironment() throws HopException {
    HopEnvironment.init();
  }

  @AfterAll
  static void cleanupEnvironment() {
    HopEnvironment.reset();
  }

  @BeforeEach
  void setUp() throws Exception {
    // Set up the mock helper
    smh = new TransformMockHelper<>("StringCut", StringCutMeta.class, StringCutData.class);
    when(smh.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(smh.iLogChannel);

    // Create StringCut instance with proper mocks
    StringCutMeta meta = new StringCutMeta();
    StringCutData data = new StringCutData();
    stringCut = new StringCut(smh.transformMeta, meta, data, 0, smh.pipelineMeta, smh.pipeline);

    // Make the private cutString method accessible for testing
    cutStringMethod =
        StringCut.class.getDeclaredMethod("cutString", String.class, int.class, int.class);
    cutStringMethod.setAccessible(true);
  }

  @AfterEach
  void cleanUp() {
    smh.cleanUp();
  }

  private String cutString(String input, int cutFrom, int cutTo) throws Exception {
    try {
      return (String) cutStringMethod.invoke(stringCut, input, cutFrom, cutTo);
    } catch (InvocationTargetException e) {
      throw new Exception(e.getCause());
    }
  }

  @Nested
  @DisplayName("Null and Empty String Tests")
  class NullAndEmptyStringTests {

    @Test
    @DisplayName("Should return null when input is null")
    void testNullString() throws Exception {
      String result = cutString(null, 0, 5);
      assertNull(result);
    }

    @Test
    @DisplayName("Should return empty string when input is empty string")
    void testEmptyString() throws Exception {
      String result = cutString("", 0, 5);
      assertEquals("", result);
    }

    @ParameterizedTest
    @ValueSource(strings = {"   ", "\t", "\n", "\r\n"})
    @DisplayName("Should handle whitespace strings correctly")
    void testWhitespaceStrings(String input) throws Exception {
      String result = cutString(input, 0, 1);
      assertEquals(input.substring(0, 1), result);
    }
  }

  @Nested
  @DisplayName("Positive Index Tests")
  class PositiveIndexTests {

    @Test
    @DisplayName("Should cut substring with positive indices")
    void testBasicPositiveCut() throws Exception {
      String result = cutString("Hello World", 0, 5);
      assertEquals("Hello", result);
    }

    @Test
    @DisplayName("Should cut from middle of string")
    void testMiddleCut() throws Exception {
      String result = cutString("Hello World", 6, 11);
      assertEquals("World", result);
    }

    @Test
    @DisplayName("Should cut single character")
    void testSingleCharacter() throws Exception {
      String result = cutString("Hello", 1, 2);
      assertEquals("e", result);
    }

    @Test
    @DisplayName("Should handle zero-length cut")
    void testZeroLengthCut() throws Exception {
      String result = cutString("Hello", 2, 2);
      assertEquals("", result);
    }

    @Test
    @DisplayName("Should return null when cutFrom exceeds string length")
    void testCutFromExceedsLength() throws Exception {
      String result = cutString("Hello", 10, 15);
      assertNull(result);
    }

    @Test
    @DisplayName("Should adjust cutTo when it exceeds string length")
    void testCutToExceedsLength() throws Exception {
      String result = cutString("Hello", 2, 100);
      assertEquals("llo", result);
    }

    @Test
    @DisplayName("Should return null when cutTo is less than cutFrom")
    void testCutToLessThanCutFrom() throws Exception {
      String result = cutString("Hello", 4, 2);
      assertNull(result);
    }

    @Test
    @DisplayName("Should handle cutting entire string")
    void testCutEntireString() throws Exception {
      String input = "Hello World";
      String result = cutString(input, 0, input.length());
      assertEquals(input, result);
    }
  }

  @Test
  @DisplayName("Should cut from end with negative indices")
  void testBasicNegativeCut() throws Exception {
    String result = cutString("Hello World", -1, -5);
    assertEquals("Worl", result);
  }

  @Test
  @DisplayName("Should return null when negative cutFrom exceeds string length")
  void testNegativeCutFromExceedsLength() throws Exception {
    String result = cutString("Hello", -10, -1);
    assertNull(result);
  }

  @Test
  @DisplayName("Should return null when negative cutFrom is less than cutTo")
  void testNegativeCutFromLessThanCutTo() throws Exception {
    String result = cutString("Hello", -4, -2);
    assertNull(result);
  }

  @Test
  @DisplayName("Should adjust negative cutTo when it exceeds string length")
  void testNegativeCutToExceedsLength() throws Exception {
    String result = cutString("Hello", -3, -10);
    assertEquals("He", result);
  }

  @Test
  @DisplayName("Should handle cutting last character")
  void testCutLastCharacter() throws Exception {
    String result = cutString("Hello", 0, -1);
    assertEquals("o", result);
  }

  @Test
  @DisplayName("Should handle cutting from negative to end")
  void testCutFromNegativeToEnd() throws Exception {
    String result = cutString("Hello World", -0, -5);
    assertEquals("World", result);
  }

  @Test
  @DisplayName("Should handle equal negative indices")
  void testEqualNegativeIndices() throws Exception {
    String result = cutString("Hello", -2, -2);
    assertEquals("", result);
  }

  @Test
  @DisplayName("Should handle cutFrom=0 with negative cutTo")
  void testCutFromZeroWithNegativeCutTo() throws Exception {
    String result = cutString("Hello World", 0, -3);
    assertEquals("rld", result);
  }

  @Test
  @DisplayName("Should handle cutFrom=0 with negative cutTo exceeding length")
  void testCutFromZeroWithNegativeCutToExceedingLength() throws Exception {
    String result = cutString("Hello", 0, -10);
    assertEquals("Hello", result);
  }

  @Test
  @DisplayName("Should handle cutFrom=0 with cutTo equal to negative string length")
  void testCutFromZeroWithCutToNegativeLength() throws Exception {
    String result = cutString("Hello", 0, -5);
    assertEquals("Hello", result);
  }

  @Test
  @DisplayName("Should handle single character string")
  void testSingleCharacterString() throws Exception {
    String result = cutString("A", 0, 1);
    assertEquals("A", result);
  }

  @Test
  @DisplayName("Should handle single character string with negative indices")
  void testSingleCharacterStringNegative() throws Exception {
    String result = cutString("A", -1, -0);
    assertEquals("A", result);
  }

  @Test
  @DisplayName("Should handle very long string")
  void testVeryLongString() throws Exception {
    String longString = "A".repeat(1000);
    String result = cutString(longString, 500, 600);
    assertEquals("A".repeat(100), result);
  }

  @Test
  @DisplayName("Should handle string with special characters")
  void testSpecialCharacters() throws Exception {
    String input = "Hello@#$%^&*()World";
    String result = cutString(input, 5, 11);
    assertEquals("@#$%^&", result);
  }

  @Test
  @DisplayName("Should handle Unicode characters")
  void testUnicodeCharacters() throws Exception {
    String input = "Hello 世界 World";
    String result = cutString(input, 6, 8);
    assertEquals("世界", result);
  }

  @Test
  @DisplayName("Should handle cutFrom at string boundary")
  void testCutFromAtBoundary() throws Exception {
    String input = "Hello";
    String result = cutString(input, 5, 5);
    assertEquals("", result);
  }

  @Test
  @DisplayName("Should handle negative cutFrom at negative string boundary")
  void testNegativeCutFromAtBoundary() throws Exception {
    String input = "Hello";
    String result = cutString(input, -4, -5);
    assertEquals("H", result);
  }

  @ParameterizedTest
  @MethodSource("provideInvalidCutParameters")
  @DisplayName("Should return null for invalid cut parameters")
  void testInvalidCutParameters(String input, int cutFrom, int cutTo) throws Exception {
    String result = cutString(input, cutFrom, cutTo);
    assertNull(result);
  }

  private Stream<Arguments> provideInvalidCutParameters() {
    return Stream.of(
        // cutFrom exceeds string length
        Arguments.of("Hello", 6, 10),
        Arguments.of("Hello", 100, 200),
        // cutTo less than cutFrom (positive)
        Arguments.of("Hello", 3, 1),
        Arguments.of("Hello", 4, 2),
        // negative cutFrom exceeds string length
        Arguments.of("Hello", -10, -1),
        Arguments.of("Hello", -50, -5),
        // negative cutFrom less than cutTo
        Arguments.of("Hello", -3, -1),
        Arguments.of("Hello", -4, -2));
  }

  @Test
  @DisplayName("Should handle extreme negative values")
  void testExtremeNegativeValues() throws Exception {
    String result = cutString("Hello", -1000, -500);
    assertNull(result);
  }

  @Test
  @DisplayName("Should handle extreme positive values")
  void testExtremePositiveValues() throws Exception {
    String result = cutString("Hello", 1000, 2000);
    assertNull(result);
  }
}
