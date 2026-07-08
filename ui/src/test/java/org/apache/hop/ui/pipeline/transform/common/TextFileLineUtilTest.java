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

package org.apache.hop.ui.pipeline.transform.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.logging.ILogChannel;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Tests for {@link TextFileLineUtil}, see issue #7244. */
class TextFileLineUtilTest {

  private static InputStreamReader reader(String content) {
    return new InputStreamReader(
        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
  }

  private static String getLine(
      String content, String enclosure, String escapeCharacter, boolean allowBreaks)
      throws HopFileException {
    return TextFileLineUtil.getLine(
        Mockito.mock(ILogChannel.class),
        reader(content),
        TextFileLineUtil.FILE_FORMAT_UNIX,
        new StringBuilder(),
        enclosure,
        escapeCharacter,
        allowBreaks);
  }

  /**
   * Issue #7244: with "break in enclosure" enabled and an empty (null) Escape character, reading a
   * line threw a NullPointerException in {@code matchChar} ("pattern is null").
   */
  @Test
  void getLineWithBreaksAndNullEscapeDoesNotThrow() throws Exception {
    String line = getLine("\"a\";\"b\"\n", "\"", null, true);
    assertEquals("\"a\";\"b\"", line);
  }

  /** An empty-string Escape character must behave the same as a null one. */
  @Test
  void getLineWithBreaksAndEmptyEscapeDoesNotThrow() throws Exception {
    String line = getLine("\"a\";\"b\"\n", "\"", "", true);
    assertEquals("\"a\";\"b\"", line);
  }

  /** A null enclosure combined with break-in-enclosure must not throw either. */
  @Test
  void getLineWithBreaksAndNullEnclosureDoesNotThrow() throws Exception {
    String line = getLine("a;b\n", null, null, true);
    assertEquals("a;b", line);
  }

  /**
   * A real line break inside an enclosed field must be kept on the same logical line when break in
   * enclosure is enabled, even without an escape character.
   */
  @Test
  void getLineKeepsBreakInsideEnclosureWithNullEscape() throws Exception {
    // "2";"val02";"hello this is ""a""\ncarriage return"
    String content = "\"2\";\"val02\";\"hello this is \"\"a\"\"\ncarriage return\"\nnext line\n";
    String line = getLine(content, "\"", null, true);
    assertEquals("\"2\";\"val02\";\"hello this is \"\"a\"\"\ncarriage return\"", line);
  }

  /** Reading past the end of the stream returns null (no trailing garbage). */
  @Test
  void getLineReturnsNullAtEndOfStream() throws Exception {
    assertNull(getLine("", "\"", null, true));
  }
}
