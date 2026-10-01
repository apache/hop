/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** How the Hop configuration file is written. */
class ConfigFileSerializerTest {

  @TempDir private Path folder;

  private final ConfigFileSerializer serializer = new ConfigFileSerializer();

  @Test
  @DisplayName("what was written is what is read back")
  void writesAndReadsTheConfiguration() throws Exception {
    String filename = folder.resolve("hop-config.json").toString();

    serializer.writeToFile(filename, Map.of("answer", "42"));

    assertEquals("42", serializer.readFromFile(filename).get("answer"));
  }

  @Test
  @DisplayName("writing again replaces the file, leaving nothing beside it")
  void leavesNothingBehind() throws Exception {
    String filename = folder.resolve("hop-config.json").toString();

    serializer.writeToFile(filename, Map.of("answer", "42"));
    serializer.writeToFile(filename, Map.of("answer", "43"));

    assertEquals("43", serializer.readFromFile(filename).get("answer"));
    assertEquals(List.of("hop-config.json"), filesInFolder());
  }

  /**
   * Two Hop Web sessions closing a dialog at the same moment both save the configuration. Writing
   * through one fixed temporary name had them deleting and moving each other's file, and whoever
   * lost the race was told the save had failed - the failure this test exists for.
   */
  @Test
  @DisplayName("several writers at once all succeed")
  void writesFromSeveralThreads() throws Exception {
    String filename = folder.resolve("hop-config.json").toString();
    int writers = 8;
    int writesEach = 25;

    CountDownLatch startTogether = new CountDownLatch(1);
    CountDownLatch finished = new CountDownLatch(writers);
    AtomicReference<Exception> failure = new AtomicReference<>();
    List<Thread> threads = new ArrayList<>();

    for (int writer = 0; writer < writers; writer++) {
      String value = "writer-" + writer;
      Thread thread =
          new Thread(
              () -> {
                try {
                  startTogether.await();
                  for (int i = 0; i < writesEach; i++) {
                    serializer.writeToFile(filename, Map.of("written-by", value));
                  }
                } catch (Exception e) {
                  failure.compareAndSet(null, e);
                } catch (Throwable e) {
                  failure.compareAndSet(null, new IllegalStateException(e));
                } finally {
                  finished.countDown();
                }
              });
      threads.add(thread);
      thread.start();
    }

    startTogether.countDown();
    assertTrue(finished.await(60, TimeUnit.SECONDS), "the writers did not finish");
    for (Thread thread : threads) {
      thread.join();
    }

    if (failure.get() != null) {
      throw new AssertionError("a writer failed to save the configuration", failure.get());
    }
    // Whichever writer went last, the file is one of theirs and it is complete.
    assertNotNull(serializer.readFromFile(filename).get("written-by"));
    assertEquals(List.of("hop-config.json"), filesInFolder());
  }

  /** A reader only ever sees a complete configuration, never a half written one. */
  @Test
  @DisplayName("a reader never catches the file half written")
  void isNeverReadHalfWritten() throws Exception {
    String filename = folder.resolve("hop-config.json").toString();
    Map<String, Object> big = new HashMap<>();
    for (int i = 0; i < 500; i++) {
      big.put("key-" + i, "value-" + i);
    }
    serializer.writeToFile(filename, big);

    AtomicReference<Exception> failure = new AtomicReference<>();
    CountDownLatch done = new CountDownLatch(1);
    Thread writer =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < 50; i++) {
                  serializer.writeToFile(filename, big);
                }
              } catch (Exception e) {
                failure.compareAndSet(null, e);
              } finally {
                done.countDown();
              }
            });
    writer.start();

    while (done.getCount() > 0) {
      // On Windows the file cannot always be opened while it is being replaced; that a read
      // failed is fine, that it succeeded and came back incomplete is not.
      try {
        Map<String, Object> read = serializer.readFromFile(filename);
        if (!read.isEmpty()) {
          assertEquals(big.size(), read.size(), "the configuration was read half written");
        }
      } catch (Exception acceptable) {
        // See above.
      }
    }
    writer.join();

    if (failure.get() != null) {
      throw new AssertionError("the writer failed to save the configuration", failure.get());
    }
  }

  private List<String> filesInFolder() throws IOException {
    try (Stream<Path> files = Files.list(folder)) {
      return files.map(path -> path.getFileName().toString()).sorted().toList();
    }
  }
}
