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

package org.apache.hop.pipeline.transforms.fileinput.text;

import org.apache.hop.ui.pipeline.transform.common.TextFileLineUtil;

/**
 * Finds where records end the way {@link TextFileLineUtil#getLine} does with breaks in enclosed
 * fields allowed: line ends inside an enclosed field don't end the record. It works on units, the
 * bytes of an encoding where ASCII bytes are never part of another character, or the 16-bit code
 * units of UTF-16, so it can run on the bytes of a file and tell where in the file a record ends.
 *
 * <p>Right after a line feed only three states are possible, which makes it possible to scan a
 * chunk without knowing what came before: outside an enclosure at the start of a record, inside an
 * enclosure, or (DOS format only) about to swallow the second character of a line end.
 */
final class TextFileRecordScanner {

  /** The state right after a line feed. */
  enum Start {
    /** A record starts here. */
    RECORD,
    /** Inside an enclosed field. */
    ENCLOSED,
    /** DOS format: the next unit is the second character of a line end. */
    SWALLOW
  }

  private static final int CR = 0x0D;
  private static final int LF = 0x0A;

  private final int[] enclosure;
  private final int[] escape;
  private final int format;

  private int enclosureIndex;
  private int escapeIndex;
  private boolean previousEscape;
  private boolean inEnclosure;
  private boolean swallowNext;
  private boolean recordHasCharacters;
  private boolean recordEnded;

  /**
   * @param enclosure the enclosure, as units; empty for none
   * @param escape the escape character(s), as units; empty for none
   * @param format {@link TextFileLineUtil#FILE_FORMAT_DOS}, {@code _UNIX} or {@code _MIXED}
   */
  TextFileRecordScanner(int[] enclosure, int[] escape, int format) {
    this.enclosure = enclosure;
    this.escape = escape;
    this.format = format;
  }

  /** Starts scanning in the given state. */
  void start(Start state) {
    recordEnded = false;
    enclosureIndex = 0;
    escapeIndex = 0;
    previousEscape = false;
    inEnclosure = state == Start.ENCLOSED;
    swallowNext = state == Start.SWALLOW;
    recordHasCharacters = state == Start.ENCLOSED;
  }

  /** The state after the last unit, which must have been a line feed. */
  Start state() {
    if (swallowNext) {
      return Start.SWALLOW;
    }
    return inEnclosure ? Start.ENCLOSED : Start.RECORD;
  }

  /**
   * Whether the record that {@link #feed} or {@link #finish} just ended had any characters, as
   * getLine returns them. Lines without characters are skipped when empty lines are left out.
   */
  boolean recordHadCharacters() {
    return recordHasCharacters;
  }

  /**
   * Feeds one unit.
   *
   * @return true when a record ends with this unit: the next unit starts a new record
   */
  boolean feed(int unit) {
    if (recordEnded) {
      // The previous unit ended a record: this one starts the next
      recordEnded = false;
      recordHasCharacters = false;
    }
    if (swallowNext) {
      // DOS: getLine reads the second character of a line end without looking at it
      swallowNext = false;
      return endRecord();
    }
    // As TextFileLineUtil.handleBreaksInEnclosure
    enclosureIndex = previousEscape ? 0 : match(enclosure, unit, enclosureIndex);
    if (enclosure.length > 0 && enclosureIndex == enclosure.length) {
      enclosureIndex = 0;
      inEnclosure = !inEnclosure;
    }
    previousEscape = false;
    escapeIndex = match(escape, unit, escapeIndex);
    if (escape.length > 0 && escapeIndex == escape.length) {
      previousEscape = true;
      escapeIndex = 0;
    }

    boolean lineEnd =
        switch (format) {
          case TextFileLineUtil.FILE_FORMAT_MIXED -> unit == LF;
          default -> unit == LF || unit == CR;
        };
    if (!inEnclosure && lineEnd) {
      if (format == TextFileLineUtil.FILE_FORMAT_DOS) {
        swallowNext = true;
        return false;
      }
      return endRecord();
    }
    if (format != TextFileLineUtil.FILE_FORMAT_MIXED || unit != CR) {
      // Mixed format drops carriage returns from the line
      recordHasCharacters = true;
    }
    return false;
  }

  /**
   * The file ended. getLine returns a last record without line end only when it has characters.
   *
   * @return true when a last record ends here
   */
  boolean finish() {
    swallowNext = false;
    return !recordEnded && recordHasCharacters;
  }

  private boolean endRecord() {
    // getLine starts every record with a fresh state; keep whether this one had characters until
    // the next unit
    boolean hadCharacters = recordHasCharacters;
    start(Start.RECORD);
    recordHasCharacters = hadCharacters;
    recordEnded = true;
    return true;
  }

  private static int match(int[] pattern, int unit, int index) {
    return pattern.length > 0 && unit == pattern[index] ? index + 1 : 0;
  }
}
