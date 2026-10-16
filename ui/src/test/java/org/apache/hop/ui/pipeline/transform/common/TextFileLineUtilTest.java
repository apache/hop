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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.io.input.UnsynchronizedBufferedReader;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.file.EncodingType;
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

  // The lines are the same read from the InputStreamReader itself, one character at a time, from
  // a buffered reader on top of it, or with a TextFileLineScanner.

  private static final int DOS = TextFileLineUtil.FILE_FORMAT_DOS;
  private static final int UNIX = TextFileLineUtil.FILE_FORMAT_UNIX;
  private static final int MIXED = TextFileLineUtil.FILE_FORMAT_MIXED;

  /** The 64 KB buffer of the Text File Input reader. */
  private static final int BUFFER = 64 * 1024;

  private static final List<Function<InputStreamReader, Reader>> READERS =
      List.of(
          isr -> isr,
          isr -> new BufferedReader(isr, BUFFER),
          isr -> new UnsynchronizedBufferedReader(isr, BUFFER),
          isr -> new UnsynchronizedBufferedReader(isr, 3));

  private static List<String> readLines(
      Reader reader,
      EncodingType encodingType,
      int format,
      String enclosure,
      String escape,
      boolean allowBreaks)
      throws HopFileException {
    List<String> lines = new ArrayList<>();
    StringBuilder buffer = new StringBuilder();
    String line;
    while ((line =
            TextFileLineUtil.getLine(
                null, reader, encodingType, format, buffer, enclosure, escape, allowBreaks))
        != null) {
      lines.add(line);
    }
    return lines;
  }

  private static void assertLines(
      String content,
      Charset charset,
      int format,
      String enclosure,
      String escape,
      boolean allowBreaks,
      String... expected)
      throws HopFileException {
    for (Function<InputStreamReader, Reader> wrap : READERS) {
      InputStreamReader isr =
          new InputStreamReader(new ByteArrayInputStream(content.getBytes(charset)), charset);
      EncodingType encodingType = EncodingType.guessEncodingType(isr.getEncoding());
      Reader reader = wrap.apply(isr);
      assertEquals(
          List.of(expected),
          readLines(reader, encodingType, format, enclosure, escape, allowBreaks),
          reader.getClass().getSimpleName());
    }
    // The scanner, with the buffer of Text File Input and a tiny one
    for (int bufferSize : new int[] {TextFileLineScanner.DEFAULT_BUFFER_SIZE, 3}) {
      InputStreamReader isr =
          new InputStreamReader(new ByteArrayInputStream(content.getBytes(charset)), charset);
      EncodingType encodingType = EncodingType.guessEncodingType(isr.getEncoding());
      TextFileLineScanner scanner = new TextFileLineScanner(isr, bufferSize);
      List<String> lines = new ArrayList<>();
      StringBuilder buffer = new StringBuilder();
      String line;
      while ((line = scanner.getLine(encodingType, format, buffer, enclosure, escape, allowBreaks))
          != null) {
        lines.add(line);
      }
      assertEquals(List.of(expected), lines, "TextFileLineScanner, buffer " + bufferSize);
    }
  }

  private static void assertLines(String content, int format, String... expected)
      throws HopFileException {
    assertLines(content, StandardCharsets.UTF_8, format, "\"", null, false, expected);
  }

  @Test
  void dosLineEnds() throws Exception {
    assertLines("a;b\r\nc;d\r\n", DOS, "a;b", "c;d");
    assertLines("a;b\r\n\r\nc;d", DOS, "a;b", "", "c;d");
  }

  @Test
  void dosLineEndWithoutItsSecondCharacterFails() {
    for (Function<InputStreamReader, Reader> wrap : READERS) {
      Reader reader =
          wrap.apply(
              new InputStreamReader(
                  new ByteArrayInputStream("a\nb\r\n".getBytes(StandardCharsets.UTF_8)),
                  StandardCharsets.UTF_8));
      assertThrows(
          HopFileException.class,
          () ->
              TextFileLineUtil.getLine(
                  null, reader, EncodingType.SINGLE, DOS, new StringBuilder(), "\"", null, false));
    }
    TextFileLineScanner scanner =
        new TextFileLineScanner(
            new InputStreamReader(
                new ByteArrayInputStream("a\nb\r\n".getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8));
    assertThrows(
        HopFileException.class,
        () -> scanner.getLine(EncodingType.SINGLE, DOS, new StringBuilder(), "\"", null, false));
  }

  @Test
  void unixLineEnds() throws Exception {
    assertLines("a;b\nc;d\n", UNIX, "a;b", "c;d");
    // A carriage return ends a line too
    assertLines("a;b\r\nc;d\r\n", UNIX, "a;b", "", "c;d", "");
    assertLines("\n\na\n", UNIX, "", "", "a");
  }

  @Test
  void mixedLineEnds() throws Exception {
    assertLines("a;b\r\nc;d\ne;f\r\n", MIXED, "a;b", "c;d", "e;f");
    // Carriage returns are left out wherever they are
    assertLines("a\rb\n\r\nc", MIXED, "ab", "", "c");
  }

  @Test
  void lastLineWithoutLineEnd() throws Exception {
    assertLines("a\r\nb", DOS, "a", "b");
    assertLines("a\nb", UNIX, "a", "b");
    assertLines("a\nb\r", MIXED, "a", "b");
    assertLines("x", UNIX, "x");
  }

  @Test
  void breaksInEnclosures() throws Exception {
    String unix = "1;\"x\ny\";2\n3;4\n";
    assertLines(unix, StandardCharsets.UTF_8, UNIX, "\"", null, true, "1;\"x\ny\";2", "3;4");
    assertLines(unix, StandardCharsets.UTF_8, UNIX, "\"", null, false, "1;\"x", "y\";2", "3;4");

    String dos = "1;\"x\r\ny\";2\r\n3\r\n";
    assertLines(dos, StandardCharsets.UTF_8, DOS, "\"", null, true, "1;\"x\r\ny\";2", "3");
    // Mixed leaves the carriage return out, inside the enclosure too
    assertLines(dos, StandardCharsets.UTF_8, MIXED, "\"", null, true, "1;\"x\ny\";2", "3");

    // A doubled enclosure inside an enclosed field
    assertLines(
        "\"a\"\"\nb\"\nc\n", StandardCharsets.UTF_8, UNIX, "\"", null, true, "\"a\"\"\nb\"", "c");

    // An enclosure of more than one character
    assertLines(
        "<<a\nb<<;c\nd\n", StandardCharsets.UTF_8, UNIX, "<<", null, true, "<<a\nb<<;c", "d");
  }

  @Test
  void escapedEnclosure() throws Exception {
    // "a\"<LF>b": the escaped enclosure keeps the field open over the line break
    assertLines(
        "\"a\\\"\nb\"\nc\n", StandardCharsets.UTF_8, UNIX, "\"", "\\", true, "\"a\\\"\nb\"", "c");
    // Without the escape character the enclosure closes the field: the line ends at the break
    assertLines(
        "\"a\\\"\nb\"\nc\n", StandardCharsets.UTF_8, UNIX, "\"", null, true, "\"a\\\"", "b\"\nc\n");
  }

  @Test
  void lineLongerThanTheBuffer() throws Exception {
    String longLine = "x".repeat(BUFFER * 2 + 17);
    assertLines(longLine + "\n" + "y\n", UNIX, longLine, "y");
    assertLines(longLine + "\r\n" + "y\r\n", DOS, longLine, "y");
  }

  @Test
  void lineEndStraddlingTheBuffer() throws Exception {
    // The carriage return is the last character of the first 64 KB, the line feed the first of
    // the next
    String first = "x".repeat(BUFFER - 1);
    assertLines(first + "\r\n" + "y\r\n", DOS, first, "y");
    assertLines(first + "\r\n" + "y\r\n", MIXED, first, "y");
    assertLines(first + "\r\n" + "y\r\n", UNIX, first, "", "y", "");
    // An enclosed line break straddling the buffer
    String enclosed = "\"" + "x".repeat(BUFFER - 2) + "\r\n" + "z\"";
    assertLines(
        enclosed + "\r\n" + "y\r\n", StandardCharsets.UTF_8, DOS, "\"", null, true, enclosed, "y");
  }

  @Test
  void utf8MultiByteCharacters() throws Exception {
    assertLines("é;ü;€;\uD83D\uDE00\nñ\n", UNIX, "é;ü;€;\uD83D\uDE00", "ñ");
    // Three bytes a character: the decoder fills its buffers in the middle of characters
    String euros = "€".repeat(BUFFER + 5);
    assertLines(euros + "\r\n" + "ñ\r\n", DOS, euros, "ñ");
  }

  @Test
  void utf16() throws Exception {
    String content = "a;\"b\r\nc\";€\r\nd;e\r\n";
    assertLines(content, StandardCharsets.UTF_16LE, DOS, "\"", null, true, "a;\"b\r\nc\";€", "d;e");
    assertLines(content, StandardCharsets.UTF_16BE, DOS, "\"", null, true, "a;\"b\r\nc\";€", "d;e");
    assertLines(
        content, StandardCharsets.UTF_16LE, MIXED, "\"", null, false, "a;\"b", "c\";€", "d;e");
  }

  @Test
  void utf16EncodingTypes() {
    assertEquals(
        EncodingType.DOUBLE_LITTLE_ENDIAN,
        EncodingType.guessEncodingType(
            new InputStreamReader(new ByteArrayInputStream(new byte[0]), StandardCharsets.UTF_16LE)
                .getEncoding()));
    assertEquals(
        EncodingType.DOUBLE_BIG_ENDIAN,
        EncodingType.guessEncodingType(
            new InputStreamReader(new ByteArrayInputStream(new byte[0]), StandardCharsets.UTF_16BE)
                .getEncoding()));
  }
}
