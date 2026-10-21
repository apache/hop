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

package org.apache.hop.ui.pipeline.transform.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.file.EncodingType;
import org.junit.jupiter.api.Test;

/**
 * The scanner reads the same lines as {@link TextFileLineUtil#getLine}, one character at a time,
 * whatever the buffer size: the same cases read both ways must give the same results.
 */
class TextFileLineScannerTest {

  private static final int[] FORMATS = {
    TextFileLineUtil.FILE_FORMAT_DOS,
    TextFileLineUtil.FILE_FORMAT_UNIX,
    TextFileLineUtil.FILE_FORMAT_MIXED,
    7 // not a format: no lines
  };

  private static final String[] ENCLOSURES = {null, "", "\"", "<<", "\"\""};

  private static final String[] ESCAPES = {null, "", "\\", "\"", "\\\\"};

  /** Buffers small enough for the short texts to straddle them everywhere. */
  private static final int[] BUFFER_SIZES = {1, 2, 3, 5, 8, 64};

  /**
   * The characters of the generated texts: line breaks, enclosures, escapes, and the characters a
   * UTF-16 little endian encoding type also takes as a carriage return and line feed.
   */
  private static final char[] ALPHABET = {
    'a', 'b', ';', '"', '"', '\\', '<', '\r', '\n', '\r', '\n', 'é', '€', 'ഀ', '਀'
  };

  /** One way of reading lines. */
  private interface LineSource {
    String getLine(
        EncodingType encodingType,
        int format,
        StringBuilder line,
        String enclosure,
        String escape,
        boolean allowBreaks)
        throws HopFileException;
  }

  private static LineSource perCharacter(Reader reader) {
    return (encodingType, format, line, enclosure, escape, allowBreaks) ->
        TextFileLineUtil.getLine(
            null, reader, encodingType, format, line, enclosure, escape, allowBreaks);
  }

  private static LineSource scanner(Reader reader, int bufferSize) {
    TextFileLineScanner scanner = new TextFileLineScanner(reader, bufferSize);
    return scanner::getLine;
  }

  /**
   * Every result of reading lines until the end of the input: the line returned and what the line
   * buffer holds after it, or the error.
   */
  private static List<String> outcomes(
      LineSource source,
      EncodingType encodingType,
      int format,
      String enclosure,
      String escape,
      boolean allowBreaks,
      int maxCalls) {
    List<String> outcomes = new ArrayList<>();
    StringBuilder line = new StringBuilder();
    for (int call = 0; call < maxCalls; call++) {
      try {
        String result = source.getLine(encodingType, format, line, enclosure, escape, allowBreaks);
        outcomes.add(result == null ? "<null>" : "[" + result + "]");
        outcomes.add("buffer[" + line + "]");
        if (result == null) {
          break;
        }
      } catch (HopFileException e) {
        outcomes.add("error " + e.getMessage().trim() + " buffer[" + line + "]");
      }
    }
    return outcomes;
  }

  private static void assertSameOutcomes(
      String text,
      EncodingType encodingType,
      int format,
      String enclosure,
      String escape,
      boolean allowBreaks,
      int... bufferSizes) {
    int maxCalls = text.length() + 3;
    List<String> expected =
        outcomes(
            perCharacter(new StringReader(text)),
            encodingType,
            format,
            enclosure,
            escape,
            allowBreaks,
            maxCalls);
    for (int bufferSize : bufferSizes) {
      List<String> actual =
          outcomes(
              scanner(new StringReader(text), bufferSize),
              encodingType,
              format,
              enclosure,
              escape,
              allowBreaks,
              maxCalls);
      if (!expected.equals(actual)) {
        assertEquals(
            expected,
            actual,
            String.format(
                "text %s, %s, format %d, enclosure %s, escape %s, breaks %s, buffer %d",
                Arrays.toString(text.toCharArray()),
                encodingType,
                format,
                enclosure,
                escape,
                allowBreaks,
                bufferSize));
      }
    }
  }

  private static void assertSameOutcomesForAllSettings(String text) {
    for (EncodingType encodingType : EncodingType.values()) {
      for (int format : FORMATS) {
        for (String enclosure : ENCLOSURES) {
          for (String escape : ESCAPES) {
            assertSameOutcomes(text, encodingType, format, enclosure, escape, true, BUFFER_SIZES);
            assertSameOutcomes(text, encodingType, format, enclosure, escape, false, BUFFER_SIZES);
          }
        }
      }
    }
  }

  @Test
  void sameLinesAsReadingOneCharacterAtATime() {
    Random random = new Random(20261014L);
    for (int i = 0; i < 200; i++) {
      char[] text = new char[random.nextInt(40)];
      for (int j = 0; j < text.length; j++) {
        text[j] = ALPHABET[random.nextInt(ALPHABET.length)];
      }
      assertSameOutcomesForAllSettings(new String(text));
    }
  }

  @Test
  void sameLinesForTheUsualCases() {
    String[] texts = {
      "",
      "a",
      "a;b\r\nc;d\r\n",
      "a;b\nc;d\n",
      "a;b\r\nc;d\ne;f\r\n",
      "a;b\r\n\r\nc;d",
      "a\nb\r",
      "\r\n",
      "\n\r\n\r",
      "1;\"x\r\ny\";2\r\n3\r\n",
      "1;\"x\ny\";2\n3;4\n",
      "\"a\"\"\nb\"\nc\n",
      "\"a\\\"\nb\"\nc\n",
      "<<a\nb<<;c\nd\n",
      "é;ü;€;😀\r\nñ\r\n",
      "\"unclosed\r\nenclosure",
    };
    for (String text : texts) {
      assertSameOutcomesForAllSettings(text);
    }
  }

  @Test
  void sameLinesOverTheBufferBoundary() {
    int size = TextFileLineScanner.DEFAULT_BUFFER_SIZE;
    String x = "x".repeat(size - 1);
    String[] texts = {
      x + "\r\ny\r\n",
      x + "\ny\n",
      x + "x\r\ny\r\n",
      "\"" + x.substring(1) + "\r\nz\";\"\r\ny\r\n",
      "y".repeat(size * 2 + 3) + "\r\nz",
    };
    for (String text : texts) {
      for (int format : FORMATS) {
        for (boolean allowBreaks : new boolean[] {true, false}) {
          assertSameOutcomes(
              text,
              EncodingType.SINGLE,
              format,
              "\"",
              null,
              allowBreaks,
              1,
              7,
              TextFileLineScanner.DEFAULT_BUFFER_SIZE);
        }
      }
    }
  }

  /** A read error is an error, as it is when reading one character at a time. */
  @Test
  void sameLinesOnAReadError() {
    String[] texts = {"ab\ncd", "ab\r\ncd\r", "ab\r", "\"a\nb", ""};
    for (String text : texts) {
      for (int format : FORMATS) {
        for (boolean allowBreaks : new boolean[] {true, false}) {
          List<String> expected =
              outcomes(
                  perCharacter(new FailingReader(text)),
                  EncodingType.SINGLE,
                  format,
                  "\"",
                  null,
                  allowBreaks,
                  6);
          for (int bufferSize : BUFFER_SIZES) {
            assertEquals(
                expected,
                outcomes(
                    scanner(new FailingReader(text), bufferSize),
                    EncodingType.SINGLE,
                    format,
                    "\"",
                    null,
                    allowBreaks,
                    6),
                text + " format " + format + " buffer " + bufferSize);
          }
        }
      }
    }
  }

  @Test
  void readsTheLinesInOrder() throws Exception {
    TextFileLineScanner scanner = new TextFileLineScanner(new StringReader("a;b\r\nc;d\r\ne"), 2);
    StringBuilder line = new StringBuilder();
    List<String> lines = new ArrayList<>();
    String read;
    while ((read =
            scanner.getLine(
                EncodingType.SINGLE, TextFileLineUtil.FILE_FORMAT_DOS, line, "\"", null, false))
        != null) {
      lines.add(read);
    }
    assertEquals(List.of("a;b", "c;d", "e"), lines);
  }

  @Test
  void closesTheReader() throws Exception {
    boolean[] closed = {false};
    Reader reader =
        new StringReader("a") {
          @Override
          public void close() {
            closed[0] = true;
            super.close();
          }
        };
    new TextFileLineScanner(reader).close();
    assertTrue(closed[0]);
  }

  @Test
  void bufferSizeMustBePositive() {
    StringReader reader = new StringReader("");
    assertThrows(IllegalArgumentException.class, () -> new TextFileLineScanner(reader, 0));
  }

  /**
   * A line cut short by a read error is never returned: it would become a truncated row. The lines
   * read completely before the error are.
   */
  @Test
  void neverReturnsALineCutShortByAReadError() throws Exception {
    int[] formats = {
      TextFileLineUtil.FILE_FORMAT_DOS,
      TextFileLineUtil.FILE_FORMAT_UNIX,
      TextFileLineUtil.FILE_FORMAT_MIXED
    };
    for (int format : formats) {
      for (boolean allowBreaks : new boolean[] {true, false}) {
        for (int bufferSize : BUFFER_SIZES) {
          TextFileLineScanner scanner =
              new TextFileLineScanner(new FailingReader("ab\ncd"), bufferSize);
          StringBuilder line = new StringBuilder();
          if (format != TextFileLineUtil.FILE_FORMAT_DOS) {
            // A DOS file needs CR and LF: there the first line is an error already
            assertEquals(
                "ab", scanner.getLine(EncodingType.SINGLE, format, line, "\"", null, allowBreaks));
          }
          assertThrows(
              HopFileException.class,
              () -> {
                for (int i = 0; i < 3; i++) {
                  scanner.getLine(EncodingType.SINGLE, format, line, "\"", null, allowBreaks);
                }
              },
              "format " + format + " buffer " + bufferSize);
          assertThrows(
              HopFileException.class,
              () ->
                  TextFileLineUtil.getLine(
                      null,
                      new FailingReader("cd"),
                      EncodingType.SINGLE,
                      format,
                      new StringBuilder(),
                      "\"",
                      null,
                      allowBreaks));
        }
      }
    }
  }

  /** Gives the text, then fails every read. */
  private static final class FailingReader extends Reader {
    private final String text;
    private int position;

    private FailingReader(String text) {
      this.text = text;
    }

    @Override
    public int read(char[] buffer, int offset, int length) throws IOException {
      if (position >= text.length()) {
        throw new IOException("read error");
      }
      int count = Math.min(length, text.length() - position);
      text.getChars(position, position + count, buffer, offset);
      position += count;
      return count;
    }

    @Override
    public void close() {
      // Nothing to close
    }
  }
}
