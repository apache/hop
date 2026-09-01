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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

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

  /**
   * Failures that are a Hop Web bug whether or not somebody caught them.
   *
   * <p>Watching only for uncaught exceptions misses most of this class of bug: Hop wraps nearly
   * every UI callback in a {@code try/catch} that logs and carries on, and RAP's own life cycle
   * catches what escapes that. The three signatures below have no benign reading in a running Hop
   * Web, so they can be treated as failures wherever they appear in the log:
   *
   * <ul>
   *   <li><b>Invalid thread access</b> - a background thread touched a widget or a session scoped
   *       singleton (issues #8195, #7896). The fat client tolerates far more of this than RAP does.
   *   <li><b>Widget is disposed</b> - a widget used after its shell, or its whole session, went
   *       away; in Hop Web that is usually state that outlived the session it belongs to.
   *   <li><b>Argument not valid</b> - SWT's complaint about, among other things, an image disposed
   *       by somebody else, which is what a {@code static} cache of session scoped resources
   *       produces once a session times out (issue #3508).
   * </ul>
   */
  private static final List<String> ALWAYS_A_BUG =
      List.of("Invalid thread access", "Widget is disposed", "Argument not valid");

  /** Stack frames worth quoting back; the rest of a trace is noise in a failure message. */
  private static final String HOP_FRAME = "at org.apache.hop.";

  private ServerLog() {}

  /**
   * One entry per distinct crash, each naming the Hop code that started it.
   *
   * <p>Distinct rather than one per occurrence: a broken repaint logs the same failure on every
   * paint, and a hundred copies of one line say no more than the line does.
   */
  public static List<String> crashes(String log) {
    Set<String> crashes = new LinkedHashSet<>();
    String[] lines = log.split("\n");
    for (int i = 0; i < lines.length; i++) {
      if (isCrash(lines[i])) {
        crashes.add(lines[i].trim() + culprit(lines, i));
      }
    }
    return new ArrayList<>(crashes);
  }

  private static boolean isCrash(String line) {
    if (line.startsWith(UNCAUGHT)) {
      return true;
    }
    // Not on continuation lines: the signature appears again in every "Caused by" and in the
    // message RAP re-throws, and each repeat would be reported as a crash of its own.
    String stripped = line.stripLeading();
    if (line.startsWith("\t") || stripped.startsWith("at ") || stripped.startsWith("Caused by")) {
      return false;
    }
    return ALWAYS_A_BUG.stream().anyMatch(line::contains);
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
