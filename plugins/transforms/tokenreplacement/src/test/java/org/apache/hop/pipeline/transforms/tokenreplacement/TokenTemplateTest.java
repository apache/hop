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

package org.apache.hop.pipeline.transforms.tokenreplacement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** The template must give exactly what {@link TokenReplacingReader} gives for the same input. */
class TokenTemplateTest {

  private static final String[] NAMES = {"a", "b", "name", ""};

  @Test
  void fillsInTokens() {
    assertEquals(
        "Dear Ann, you live in Ghent.",
        fill("Dear ${name}, you live in ${city}.", "${", "}", "name", "Ann", "city", "Ghent"));
  }

  @Test
  void keepsTokensWithoutAValueAsWritten() {
    assertEquals("Hi ${nobody} and ${}", fill("Hi ${nobody} and ${}", "${", "}", "a", "x"));
  }

  @Test
  void keepsAnUnfinishedTokenWithTheRestOfTheText() {
    assertEquals("Hi x ${a and more", fill("Hi ${a} ${a and more", "${", "}", "a", "x"));
  }

  @Test
  void emptyValueAtTheStartKeepsTheRestOfTheText() {
    // The old reader returned nothing at all for this row
    assertEquals(" lives here", fill("${name} lives here", "${", "}", "name", ""));
  }

  @Test
  void doesNotLookForTokensInValues() {
    assertEquals("<${b}>", fill("<${a}>", "${", "}", "a", "${b}", "b", "nested"));
  }

  @Test
  void refusesEmptyMarkers() {
    Map<String, Integer> positions = Map.of();
    assertThrows(
        IllegalArgumentException.class, () -> TokenTemplate.parse("x", "", "}", positions));
    assertThrows(
        IllegalArgumentException.class, () -> TokenTemplate.parse("x", "${", "", positions));
  }

  @ParameterizedTest
  @CsvSource({"${,}", "{{,}}", "[,]", "<%,%>", "$,$", "##,#"})
  void givesTheSameAsTheReaderForRandomTemplates(String start, String end) throws IOException {
    Random random = new Random(start.hashCode() * 31L + end.hashCode());
    String alphabet = "ab xyz" + start + end + "name";
    for (int run = 0; run < 5_000; run++) {
      String template = randomText(random, alphabet, random.nextInt(40));

      // Values may hold markers and token names too; some are missing (null). No empty values:
      // the old reader ends its output at an empty value at the start of a read, see
      // emptyValueAtTheStartKeepsTheRestOfTheText
      Map<String, Integer> positions = new HashMap<>();
      String[] values = new String[NAMES.length];
      TokenResolver resolver = new TokenResolver();
      for (int i = 0; i < NAMES.length; i++) {
        positions.put(NAMES[i], i);
        values[i] =
            random.nextInt(5) == 0 ? null : randomText(random, alphabet, 1 + random.nextInt(7));
        resolver.addToken(NAMES[i], values[i]);
      }

      StringBuilder actual = new StringBuilder();
      TokenTemplate.parse(template, start, end, positions).appendTo(values, actual);

      assertEquals(
          readThroughReader(template, start, end, resolver),
          actual.toString(),
          "template [" + template + "] with markers " + start + " and " + end);
    }
  }

  private static String fill(String template, String start, String end, String... nameValues) {
    Map<String, Integer> positions = new HashMap<>();
    String[] values = new String[nameValues.length / 2];
    for (int i = 0; i < values.length; i++) {
      positions.put(nameValues[i * 2], i);
      values[i] = nameValues[i * 2 + 1];
    }
    StringBuilder output = new StringBuilder();
    TokenTemplate.parse(template, start, end, positions).appendTo(values, output);
    return output.toString();
  }

  private static String readThroughReader(
      String template, String start, String end, TokenResolver resolver) throws IOException {
    StringBuilder output = new StringBuilder();
    try (TokenReplacingReader reader =
        new TokenReplacingReader(resolver, new StringReader(template), start, end)) {
      char[] buffer = new char[7];
      int length;
      while ((length = reader.read(buffer)) > 0) {
        output.append(buffer, 0, length);
      }
    }
    return output.toString();
  }

  private static String randomText(Random random, String alphabet, int length) {
    StringBuilder text = new StringBuilder();
    for (int i = 0; i < length; i++) {
      text.append(alphabet.charAt(random.nextInt(alphabet.length())));
    }
    return text.toString();
  }
}
