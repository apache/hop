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

import org.eclipse.swt.widgets.Display;

/**
 * Answers the question "does this thread have a display to work with?" without ever throwing.
 *
 * <p>{@code Display.getCurrent()} is the usual way to ask, and on the desktop it is a plain
 * thread-to-display lookup that cannot fail. In Hop Web it is not: RWT resolves the display through
 * the RAP session bound to the calling thread, so asking the question reaches into that session.
 *
 * <p>{@link BackgroundThreadFacade} deliberately binds a session to the threads that run background
 * work, so those threads do have one to reach into. A session that has since been destroyed - the
 * browser closed, the user logged out, the session timed out - is still bound to the thread but no
 * longer resolves, and RWT dereferences it unchecked: the guard meant to keep background threads
 * away from the UI is where the {@code NullPointerException} comes from instead (issue #8248,
 * following #7896 / #7897).
 *
 * <p>A thread whose session died is a thread with no display, which is what these methods answer.
 */
public class SessionDisplay {

  private SessionDisplay() {
    // Utility class
  }

  /**
   * The display this thread is the user interface thread for.
   *
   * @return the display, or null when this thread has none - including when the session that would
   *     have provided it is gone
   */
  public static Display current() {
    try {
      return Display.getCurrent();
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * The display of this thread, falling back to the default display.
   *
   * <p>For callers that want a display to schedule work on rather than an answer about the calling
   * thread. The fallback resolves through the same session, so it is guarded too.
   *
   * @return a usable display, or null when there is none
   */
  public static Display currentOrDefault() {
    Display display = current();
    if (display != null) {
      return display;
    }
    try {
      return Display.getDefault();
    } catch (RuntimeException e) {
      return null;
    }
  }
}
