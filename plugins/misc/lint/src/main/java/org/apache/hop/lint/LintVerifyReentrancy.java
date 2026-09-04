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
package org.apache.hop.lint;

import org.apache.hop.core.exception.HopException;

/**
 * Stops the linter from reporting each policy finding twice.
 *
 * <p>The verify extension points hook {@code AfterCheckTransforms} so that lint findings appear in
 * Hop's own Problems tab. But the linter also runs those same native checks itself, to fold Hop's
 * built-in remarks into a lint run — which fires the extension, which adds the policy findings a
 * second time, on top of the ones the caller already collected. Deduplication does not catch them
 * because the extension rewords the message on the way through.
 *
 * <p>So when the linter drives the native checks, it marks the thread: the extension sees the mark
 * and stays out of the way, because whoever asked for the check is already collecting policy
 * results. A user pressing Verify in the GUI is unmarked, and still gets lint findings in the
 * Problems tab.
 */
public final class LintVerifyReentrancy {

  /**
   * The mark is a system property rather than a {@code ThreadLocal}, because the two sides of this
   * handshake are not always the same class. Running the CLI puts the lint jar on the JVM classpath
   * so its main class can start, while Hop's plugin registry loads the same jar again through its
   * own classloader to register the extension points. Each copy would get its own {@code
   * ThreadLocal}, the extension would never see the mark, and every finding would be reported
   * twice. System properties are JVM-global, so they cross that boundary.
   *
   * <p>Keyed by thread id so concurrent lint runs — the GUI lints several files at once — cannot
   * suppress each other.
   */
  private static final String MARK_PREFIX = "org.apache.hop.lint.verify.drivenByLinter.";

  private LintVerifyReentrancy() {}

  private static String markKey() {
    return MARK_PREFIX + Thread.currentThread().getId();
  }

  /** True when the current native check was started by the linter itself. */
  public static boolean isDrivenByLinter() {
    return System.getProperty(markKey()) != null;
  }

  /** Run native checks with the extension points suppressed for this thread. */
  public static void runDrivenByLinter(CheckTask task) throws HopException {
    boolean alreadyMarked = isDrivenByLinter();
    if (!alreadyMarked) {
      System.setProperty(markKey(), "true");
    }
    try {
      task.run();
    } finally {
      if (!alreadyMarked) {
        // Clear rather than set false: threads are pooled in the GUI, and a stale mark would
        // silently suppress lint findings on the next unrelated verify run.
        System.clearProperty(markKey());
      }
    }
  }

  /** A native check invocation that may fail the way Hop's check methods do. */
  @FunctionalInterface
  public interface CheckTask {
    void run() throws HopException;
  }
}
