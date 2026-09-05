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

package org.apache.hop.ui.hopgui;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import org.eclipse.swt.widgets.Display;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * In Hop Web, asking whether this thread has a display goes through the RAP session bound to it.
 * When that session has been destroyed - the browser closed, the user logged out, the session timed
 * out - RWT dereferences it unchecked and the question itself throws (issue #8248). The answer for
 * such a thread is "no display", not an exception thrown out of a guard clause.
 */
class SessionDisplayTest {

  /** What RWT throws out of {@code LifeCycleUtil.getSessionDisplay} for a destroyed session. */
  private static NullPointerException deadSession() {
    return new NullPointerException(
        "Cannot invoke \"org.eclipse.rap.rwt.service.UISession.getAttribute(String)\""
            + " because \"uiSession\" is null");
  }

  @Test
  @DisplayName("a thread whose session is gone has no display, rather than an exception")
  void currentAnswersNullWhenTheSessionIsGone() {
    try (MockedStatic<Display> display = mockStatic(Display.class)) {
      display.when(Display::getCurrent).thenThrow(deadSession());

      assertNull(SessionDisplay.current());
    }
  }

  @Test
  @DisplayName("the display of a live session is handed back unchanged")
  void currentAnswersTheDisplayOfTheThread() {
    Display sessionDisplay = mock(Display.class);
    try (MockedStatic<Display> display = mockStatic(Display.class)) {
      display.when(Display::getCurrent).thenReturn(sessionDisplay);

      assertSame(sessionDisplay, SessionDisplay.current());
    }
  }

  @Test
  @DisplayName("the fallback to the default display is guarded the same way")
  void currentOrDefaultAnswersNullWhenNeitherResolves() {
    try (MockedStatic<Display> display = mockStatic(Display.class)) {
      display.when(Display::getCurrent).thenThrow(deadSession());
      display.when(Display::getDefault).thenThrow(deadSession());

      assertNull(SessionDisplay.currentOrDefault());
    }
  }

  @Test
  @DisplayName("a thread without a display of its own falls back to the default one")
  void currentOrDefaultFallsBackToTheDefaultDisplay() {
    Display defaultDisplay = mock(Display.class);
    try (MockedStatic<Display> display = mockStatic(Display.class)) {
      display.when(Display::getCurrent).thenReturn(null);
      display.when(Display::getDefault).thenReturn(defaultDisplay);

      assertSame(defaultDisplay, SessionDisplay.currentOrDefault());
    }
  }
}
