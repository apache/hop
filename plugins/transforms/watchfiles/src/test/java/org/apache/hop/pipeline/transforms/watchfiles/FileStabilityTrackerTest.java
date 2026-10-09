/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class FileStabilityTrackerTest {
  private FileState file(long size, long modified) {
    return new FileState("a", "a", "a", "/", "file", size, modified);
  }

  private FileChangeEvent created(FileState file) {
    return new FileChangeEvent(FileEventType.CREATED, file, null, 0);
  }

  @Test
  void growingFileResetsConsecutiveChecksAndKeepsCreated() {
    FileStabilityTracker tracker = new FileStabilityTracker(true, 0, 2, 100, 10);
    FileState initial = file(10, 0);
    tracker.observe(created(initial), 0);
    assertNull(tracker.check("a", initial, 100));
    FileState growing = file(100, 200);
    assertNull(tracker.check("a", growing, 200));
    assertNull(tracker.check("a", growing, 300));
    FileChangeEvent stable = tracker.check("a", growing, 400);
    assertEquals(FileEventType.CREATED, stable.getType());
    assertEquals(100, stable.file().getSize());
  }

  @Test
  void duplicateHintsDoNotRestartQuietInterval() {
    FileStabilityTracker tracker = new FileStabilityTracker(true, 0, 2, 100, 10);
    FileState file = file(10, 0);
    tracker.observe(created(file), 0);
    tracker.observe(created(file), 50);
    assertNull(tracker.check("a", file, 100));
    tracker.observe(created(file), 150);
    assertEquals(FileEventType.CREATED, tracker.check("a", file, 200).getType());
  }

  @Test
  void minimumAgeIsRequiredEvenWithStabilityDisabled() {
    FileStabilityTracker tracker = new FileStabilityTracker(false, 1000, 2, 100, 10);
    FileState file = file(10, 100);
    tracker.observe(created(file), 100);
    assertNull(tracker.check("a", file, 1099));
    assertFalse(tracker.due("a", 1099));
    assertNotNull(tracker.check("a", file, 1100));
  }

  @Test
  void observationsTooCloseDoNotCount() {
    FileStabilityTracker tracker = new FileStabilityTracker(true, 0, 2, 100, 10);
    FileState file = file(10, 0);
    tracker.observe(created(file), 0);
    assertNull(tracker.check("a", file, 99));
    assertNull(tracker.check("a", file, 100));
    assertNull(tracker.check("a", file, 199));
    assertNotNull(tracker.check("a", file, 200));
  }

  @Test
  void disappearingUnemittedFileIsForgotten() {
    FileStabilityTracker tracker = new FileStabilityTracker(true, 0, 2, 100, 10);
    tracker.observe(created(file(10, 0)), 0);
    assertNull(tracker.check("a", null, 100));
    assertEquals(0, tracker.size());
  }

  @Test
  void capacityIsExplicitAndRepeatedModifiesStayOneCandidate() {
    FileStabilityTracker tracker = new FileStabilityTracker(true, 0, 1, 100, 1);
    FileState old = file(10, 0);
    for (int i = 0; i < 100; i++) {
      tracker.observe(new FileChangeEvent(FileEventType.MODIFIED, file(20 + i, i), old, i), i);
    }
    assertEquals(1, tracker.size());
    FileState second = new FileState("b", "b", "b", "/", "file", 0, 0);
    assertThrows(IllegalStateException.class, () -> tracker.observe(created(second), 200));
    assertEquals(FileEventType.MODIFIED, tracker.check("a", file(119, 99), 200).getType());
  }
}
