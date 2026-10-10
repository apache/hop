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

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.file.EncodingType;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.pipeline.transform.common.TextFileLineUtil.EnclosureData;

/**
 * Reads the lines of a text file the way {@link TextFileLineUtil#getLine} does, from a buffer of
 * characters: the reader is read a buffer at a time and the characters of a line are copied in
 * runs, not one {@link Reader#read()} per character.
 *
 * <p>The scanner reads ahead of the line it returns. Read all the lines of a reader through the
 * same scanner, and don't read from the reader itself in between.
 */
public class TextFileLineScanner implements Closeable {
  private static final Class<?> PKG = TextFileLineUtil.class;

  public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

  private final Reader reader;

  private final char[] buffer;

  /** The next character to scan. */
  private int position;

  /** The end of the characters in the buffer. */
  private int limit;

  /** The first character of the enclosure of the line being read. */
  private int enclosureStart;

  /** The first character of the escape character of the line being read, -1 without one. */
  private int escapeStart;

  /**
   * @param reader the reader of the text, a bare {@link java.io.InputStreamReader} is fine
   */
  public TextFileLineScanner(Reader reader) {
    this(reader, DEFAULT_BUFFER_SIZE);
  }

  /**
   * @param reader the reader of the text, a bare {@link java.io.InputStreamReader} is fine
   * @param bufferSize the number of characters read from the reader at a time
   */
  public TextFileLineScanner(Reader reader, int bufferSize) {
    if (bufferSize <= 0) {
      throw new IllegalArgumentException("The buffer size must be positive: " + bufferSize);
    }
    this.reader = reader;
    this.buffer = new char[bufferSize];
  }

  /**
   * Reads the next line, as {@link TextFileLineUtil#getLine} does.
   *
   * @param encodingType the encoding type, for the line feed and carriage return characters
   * @param formatNr {@link TextFileLineUtil#FILE_FORMAT_DOS}, {@link
   *     TextFileLineUtil#FILE_FORMAT_UNIX} or {@link TextFileLineUtil#FILE_FORMAT_MIXED}
   * @param line the buffer the line is built in
   * @param enclosure the enclosure, for line breaks in enclosed fields
   * @param escapeCharacter the escape character, for line breaks in enclosed fields
   * @param allowBreaks keep line breaks inside enclosures in the line
   * @return the line, or null at the end of the input
   * @throws HopFileException when the line can't be read
   */
  public String getLine(
      EncodingType encodingType,
      int formatNr,
      StringBuilder line,
      String enclosure,
      String escapeCharacter,
      boolean allowBreaks)
      throws HopFileException {
    // Without an enclosure no line break is ever enclosed: skip following the enclosures
    EnclosureData ed = null;
    if (allowBreaks && enclosure != null && !enclosure.isEmpty()) {
      ed = new EnclosureData(0, 0, enclosure, escapeCharacter);
      enclosureStart = enclosure.charAt(0);
      escapeStart =
          escapeCharacter == null || escapeCharacter.isEmpty() ? -1 : escapeCharacter.charAt(0);
    }

    line.setLength(0);
    try {
      return switch (formatNr) {
        case TextFileLineUtil.FILE_FORMAT_DOS -> readDos(encodingType, line, ed);
        case TextFileLineUtil.FILE_FORMAT_UNIX -> readUnix(encodingType, line, ed);
        case TextFileLineUtil.FILE_FORMAT_MIXED -> readMixed(encodingType, line, ed);
        default -> null;
      };
    } catch (HopFileException e) {
      throw e;
    } catch (Exception e) {
      // Never hand out the part of a line read before the error: it would become a truncated row
      throw new HopFileException(
          BaseMessages.getString(
              PKG, "TextFileLineUtil.Log.Error.ExceptionReadingLine", e.toString()),
          e);
    }
  }

  /** A line ends with two line break characters, carriage return or line feed, in any order. */
  private String readDos(EncodingType encodingType, StringBuilder line, EnclosureData ed)
      throws IOException, HopFileException {
    while (position < limit || fill()) {
      char[] chars = buffer;
      int end = limit;
      int start = position;
      for (int i = start; i < end; i++) {
        char c = chars[i];
        if (ed != null) {
          followEnclosures(ed, c);
        }
        if ((ed == null || !ed.inEnclosure)
            && (encodingType.isReturn(c) || encodingType.isLinefeed(c))) {
          line.append(chars, start, i - start);
          position = i + 1;
          int next = read(); // skip \n and \r
          if (!encodingType.isReturn(next) && !encodingType.isLinefeed(next)) {
            // Make sure it's really a linefeed or carriage return.
            // Raise an error if this is not a DOS formatted file.
            throw new HopFileException(
                BaseMessages.getString(PKG, "TextFileLineUtil.Log.SingleLineFound"));
          }
          return line.toString();
        }
      }
      line.append(chars, start, end - start);
      position = end;
    }
    return endOfInput(line);
  }

  /** A line ends with a line feed or a carriage return. */
  private String readUnix(EncodingType encodingType, StringBuilder line, EnclosureData ed)
      throws IOException {
    while (position < limit || fill()) {
      char[] chars = buffer;
      int end = limit;
      int start = position;
      for (int i = start; i < end; i++) {
        char c = chars[i];
        if (ed != null) {
          followEnclosures(ed, c);
        }
        if ((ed == null || !ed.inEnclosure)
            && (encodingType.isLinefeed(c) || encodingType.isReturn(c))) {
          line.append(chars, start, i - start);
          position = i + 1;
          return line.toString();
        }
      }
      line.append(chars, start, end - start);
      position = end;
    }
    return endOfInput(line);
  }

  /**
   * A line ends with a line feed. Carriage returns are left out, also in enclosures: not for Mac OS
   * 9 but works for Mac OS X. Mac OS 9 can use the UNIX format.
   */
  private String readMixed(EncodingType encodingType, StringBuilder line, EnclosureData ed)
      throws IOException {
    while (position < limit || fill()) {
      char[] chars = buffer;
      int end = limit;
      int start = position;
      for (int i = start; i < end; i++) {
        char c = chars[i];
        if (ed != null) {
          followEnclosures(ed, c);
        }
        if ((ed == null || !ed.inEnclosure) && encodingType.isLinefeed(c)) {
          line.append(chars, start, i - start);
          position = i + 1;
          return line.toString();
        } else if (encodingType.isReturn(c)) {
          line.append(chars, start, i - start);
          start = i + 1;
        }
      }
      line.append(chars, start, end - start);
      position = end;
    }
    return endOfInput(line);
  }

  /**
   * {@link TextFileLineUtil#handleBreaksInEnclosure} for the next character. Outside a match, with
   * no escape before it, a character that starts neither the enclosure nor the escape character
   * leaves everything as it is: skip those.
   */
  private void followEnclosures(EnclosureData ed, char c) {
    if (ed.enclosureIndex != 0
        || ed.escapeIndex != 0
        || ed.isPrevEscape
        || c == enclosureStart
        || c == escapeStart) {
      TextFileLineUtil.handleBreaksInEnclosure(ed, c);
    }
  }

  private static String endOfInput(StringBuilder line) {
    return line.isEmpty() ? null : line.toString();
  }

  /** The next character, or -1 at the end of the input. */
  private int read() throws IOException {
    if (position < limit || fill()) {
      return buffer[position++];
    }
    return -1;
  }

  /** Reads the next characters into the buffer, false at the end of the input. */
  private boolean fill() throws IOException {
    int count;
    do {
      count = reader.read(buffer, 0, buffer.length);
    } while (count == 0);
    if (count < 0) {
      return false;
    }
    position = 0;
    limit = count;
    return true;
  }

  /** Closes the reader. */
  @Override
  public void close() throws IOException {
    reader.close();
  }
}
