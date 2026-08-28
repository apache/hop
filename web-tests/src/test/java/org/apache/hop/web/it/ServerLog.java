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

package org.apache.hop.web.it;

import java.util.ArrayList;
import java.util.List;

/**
 * Finds crashes in what Hop Web logged.
 *
 * <p>Not everything that goes wrong reaches the browser. A dialog can open looking perfectly
 * healthy while the thread it started to fill its widgets has died - CheckSumDialog does exactly
 * that, calling the session scoped {@code PropsUi} from a plain {@code new Thread}, which RAP
 * rejects with "Invalid thread access". Nothing about the page says so; only the server log does.
 * Watching it turns an entire class of Hop Web bug into a test failure without the tests needing to
 * know anything about the dialog in question.
 */
public final class ServerLog {

  /**
   * An exception nobody caught. Deliberately narrow: Hop Web routinely logs handled problems -
   * malformed i18n patterns, CSS properties RAP does not implement - and treating those as failures
   * would leave the check permanently red and therefore ignored.
   */
  private static final String UNCAUGHT = "Exception in thread ";

  /** Stack frames worth quoting back; the rest of a trace is noise in a failure message. */
  private static final String HOP_FRAME = "at org.apache.hop.";

  private ServerLog() {}

  /** One entry per uncaught exception, each naming the Hop code that started it. */
  public static List<String> crashes(String log) {
    List<String> crashes = new ArrayList<>();
    String[] lines = log.split("\n");
    for (int i = 0; i < lines.length; i++) {
      if (!lines[i].startsWith(UNCAUGHT)) {
        continue;
      }
      crashes.add(lines[i].trim() + culprit(lines, i));
    }
    return crashes;
  }

  /**
   * The deepest Hop frame in the trace, which is the code that started the thread rather than the
   * shared helper it happened to die in.
   *
   * <p>Reporting the topmost frame instead names {@code PropsUiImpl.getInstanceInternal} for every
   * one of these, which says nothing about which dialog to go and fix.
   */
  private static String culprit(String[] lines, int start) {
    String deepest = null;
    for (int i = start + 1; i < lines.length && i <= start + 60; i++) {
      String line = lines[i].trim();
      if (line.startsWith(HOP_FRAME)) {
        deepest = line;
      } else if (!line.isEmpty() && !line.startsWith("at ")) {
        break;
      }
    }
    return deepest == null ? "" : " (" + deepest + ")";
  }
}
