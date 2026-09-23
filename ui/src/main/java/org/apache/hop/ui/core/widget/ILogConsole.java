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

package org.apache.hop.ui.core.widget;

import java.util.List;

/**
 * A log view that takes its lines incrementally.
 *
 * <p>The desktop log view is a StyledText that is appended to in place. Hop Web has no StyledText
 * and used a plain Text, which RAP re-sends in full every time a line is added: a pipeline that
 * logs a lot shipped the whole log to the browser once a second (issue #8435). A console gets only
 * the new lines, keeps the last {@code maxLines} of them itself and holds the same text the desktop
 * view would, so the copy / filter / "show errors" features that read it keep working.
 */
public interface ILogConsole {

  /** One log line as it is shown: the formatted text and whether it is an error line. */
  record Line(String text, boolean error) {}

  /** Adds lines at the end, in order. */
  void appendLines(List<Line> lines);

  /**
   * How many lines one refresh may add. Row-level logging produces far more lines per second than a
   * browser can take, and everything beyond what the console keeps would be dropped on arrival
   * anyway; the log browser sends the newest lines up to this number and says how many it skipped.
   */
  int getMaxLinesPerRefresh();

  /** Empties the view. */
  void clear();

  /**
   * Marks occurrences of a term in the lines shown from now on. Lines already shown are not
   * re-marked; the log browser rebuilds the view when the filter changes.
   *
   * @param term the text to mark, null or empty to stop marking
   * @param caseSensitive whether the match is case-sensitive
   */
  void setHighlight(String term, boolean caseSensitive);
}
