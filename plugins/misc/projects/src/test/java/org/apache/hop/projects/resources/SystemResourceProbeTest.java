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

package org.apache.hop.projects.resources;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 0 probes for issue #7583: document the JVM/OS APIs used by system resource validation.
 *
 * <p>Product checks use {@link Runtime#maxMemory()} (≈ {@code -Xmx}), {@link
 * File#getUsableSpace()}, and {@link Runtime#availableProcessors()}. {@link Runtime#totalMemory()}
 * is probed for sanity only (current heap allocation, not capacity).
 */
class SystemResourceProbeTest {

  @Test
  void jvmReportsPositiveTotalMemory() {
    long total = Runtime.getRuntime().totalMemory();
    assertTrue(total > 0, "totalMemory() should report allocated heap > 0, got " + total);
  }

  @Test
  void jvmMaxMemoryIsAtLeastTotalMemory() {
    Runtime runtime = Runtime.getRuntime();
    long max = runtime.maxMemory();
    long total = runtime.totalMemory();
    assertTrue(max > 0, "maxMemory() should be > 0, got " + max);
    assertTrue(
        max >= total, "maxMemory() (" + max + ") should be >= totalMemory() (" + total + ")");
  }

  @Test
  void availableProcessorsIsAtLeastOne() {
    int processors = Runtime.getRuntime().availableProcessors();
    assertTrue(processors >= 1, "availableProcessors() should be >= 1, got " + processors);
  }

  @Test
  void usableSpaceOnTempDirIsNonNegative(@TempDir Path temp) {
    long usable = temp.toFile().getUsableSpace();
    assertTrue(usable >= 0, "getUsableSpace() on temp dir should be >= 0, got " + usable);
    // On a real local filesystem we expect some free space
    assertTrue(usable > 0, "getUsableSpace() on temp dir should be > 0, got " + usable);
  }

  @Test
  void usableSpaceOnMissingPathIsNonNegative() {
    File missing = new File("/path/that/does/not/exist-" + UUID.randomUUID());
    // JDK returns 0L for non-existent paths rather than throwing
    long usable = missing.getUsableSpace();
    assertTrue(usable >= 0, "getUsableSpace() on missing path should be >= 0, got " + usable);
  }
}
