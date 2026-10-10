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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

/** Opt-in test against a dedicated small tmpfs, never the ordinary source/state filesystem. */
@EnabledOnOs(OS.LINUX)
@EnabledIfEnvironmentVariable(named = "WATCHFILES_FULL_FILESYSTEM", matches = ".+")
class WatchFilesDiskFullTest {
  @Test
  void actualEnospcKeepsPreviousCheckpointAndRecoversAfterSpaceReturns() throws Exception {
    Path mount = Path.of(System.getenv("WATCHFILES_FULL_FILESYSTEM")).toRealPath();
    Path root = Files.createTempDirectory(mount, "watchfiles-");
    Path filler = root.resolve("filler");
    Map<String, FileState> files = Map.of("a", new FileState("a", "a", "a", "/", "file", 1, 100));
    try {
      try (JsonFileStateStore store = new JsonFileStateStore(root, "test", "root", "scope", 10)) {
        store.save(files);
        String original = Files.readString(root.resolve("test.json"));
        IOException full =
            assertThrows(
                IOException.class,
                () -> {
                  try (var output = Files.newOutputStream(filler)) {
                    for (int i = 0; i < 1024; i++) output.write(new byte[4096]);
                  }
                });
        assertEquals(0, Files.getFileStore(root).getUsableSpace(), full.toString());
        assertThrows(IOException.class, () -> store.save(Map.of()));
        assertEquals(original, Files.readString(root.resolve("test.json")));
        Files.delete(filler);
        store.save(Map.of());
      }
      try (JsonFileStateStore store = new JsonFileStateStore(root, "test", "root", "scope", 10)) {
        assertEquals(Map.of(), store.load());
      }
    } finally {
      // The UUID child is owned by this test; leave the caller's mount intact.
      for (String name : new String[] {"filler", "test.json", "test.json.tmp", "test.lock"}) {
        Files.deleteIfExists(root.resolve(name));
      }
      Files.delete(root);
    }
  }
}
