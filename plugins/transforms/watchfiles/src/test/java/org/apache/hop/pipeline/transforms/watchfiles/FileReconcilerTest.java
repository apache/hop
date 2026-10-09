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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FileReconcilerTest {
  private FileState state(String uri, long size, long modified) {
    return new FileState(uri, uri, uri, "/", "file", size, modified);
  }

  @Test
  void recoversMissedCreateModifyDeleteAndIgnoresUnchanged() {
    FileState unchanged = state("a", 1, 1);
    FileState previous = state("b", 2, 2);
    FileState deleted = state("c", 3, 3);
    FileState created = state("d", 4, 4);
    FileState modified = state("b", 20, 2);
    List<FileChangeEvent> changes = new ArrayList<>();
    new FileReconciler()
        .compare(
            Map.of("a", unchanged, "b", previous, "c", deleted),
            Map.of("a", unchanged, "b", modified, "d", created),
            99,
            changes::add);
    assertEquals(3, changes.size());
    assertEquals(1, changes.stream().filter(e -> e.getType() == FileEventType.CREATED).count());
    FileChangeEvent modify =
        changes.stream()
            .filter(e -> e.getType() == FileEventType.MODIFIED)
            .findFirst()
            .orElseThrow();
    assertEquals(previous, modify.getPrevious());
    FileChangeEvent deletion =
        changes.stream()
            .filter(e -> e.getType() == FileEventType.DELETED)
            .findFirst()
            .orElseThrow();
    assertEquals(deleted, deletion.file());
    assertNull(deletion.getCurrent());
    assertEquals(99, deletion.getDetectedAt());
  }

  @Test
  void detectsTimestampChangeWithoutSizeChange() {
    List<FileChangeEvent> changes = new ArrayList<>();
    new FileReconciler()
        .compare(Map.of("a", state("a", 1, 1)), Map.of("a", state("a", 1, 2)), 99, changes::add);
    assertEquals(FileEventType.MODIFIED, changes.getFirst().getType());
  }
}
