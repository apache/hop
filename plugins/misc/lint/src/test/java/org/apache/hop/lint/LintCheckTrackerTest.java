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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.junit.jupiter.api.Test;

public class LintCheckTrackerTest {

  @Test
  public void needsLintingWhenNeverChecked() throws IOException {
    File temp = File.createTempFile("lint-tracker", ".hpl");
    temp.deleteOnExit();
    Files.writeString(temp.toPath(), "test");

    LintCheckTracker tracker = new LintCheckTracker();
    assertTrue(tracker.needsLinting(temp.getAbsolutePath()));
  }

  @Test
  public void skipsWhenNotModifiedSinceCheck() throws IOException, InterruptedException {
    File temp = File.createTempFile("lint-tracker", ".hpl");
    temp.deleteOnExit();
    Files.writeString(temp.toPath(), "test");

    LintCheckTracker tracker = new LintCheckTracker();
    tracker.markChecked(temp.getAbsolutePath());
    assertFalse(tracker.needsLinting(temp.getAbsolutePath()));
  }

  @Test
  public void needsLintingAfterFileChanges() throws IOException, InterruptedException {
    File temp = File.createTempFile("lint-tracker", ".hpl");
    temp.deleteOnExit();
    Files.writeString(temp.toPath(), "test");

    LintCheckTracker tracker = new LintCheckTracker();
    tracker.markChecked(temp.getAbsolutePath());

    Thread.sleep(1100);
    Files.writeString(temp.toPath(), "changed");

    assertTrue(tracker.needsLinting(temp.getAbsolutePath()));
  }

  @Test
  public void invalidateForcesRecheck() throws IOException {
    File temp = File.createTempFile("lint-tracker", ".hpl");
    temp.deleteOnExit();
    Files.writeString(temp.toPath(), "test");

    LintCheckTracker tracker = new LintCheckTracker();
    tracker.markChecked(temp.getAbsolutePath());
    tracker.invalidate(temp.getAbsolutePath());

    assertTrue(tracker.needsLinting(temp.getAbsolutePath()));
  }
}
