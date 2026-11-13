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

package org.apache.hop.core.fatjar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FatJarCacheTest {

  @TempDir Path tempDir;

  private Path cache;
  private IVariables variables;

  @BeforeAll
  static void initLogStore() {
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    cache = tempDir.resolve("cache");
    variables = new Variables();
    variables.setVariable(FatJarCache.VARIABLE_CACHE_FOLDER, cache.toString());
  }

  @Test
  void buildsOnceAndReusesTheCachedJar() throws Exception {
    List<String> jars = List.of(jar("a.jar", "a/A.txt"), jar("b.jar", "b/B.txt"));

    Path first = FatJarCache.getOrBuild(LogChannel.GENERAL, variables, "test", jars);
    assertTrue(Files.isRegularFile(first));
    assertTrue(first.startsWith(cache));
    assertTrue(first.getFileName().toString().matches("test-[0-9a-f]{16}\\.jar"));
    try (JarFile jarFile = new JarFile(first.toFile())) {
      assertNotNull(jarFile.getEntry("a/A.txt"));
      assertNotNull(jarFile.getEntry("b/B.txt"));
    }

    // The sidecar holds the content hash, so a deployment does not hash the jar again
    String sha = Files.readString(FatJarCache.sidecar(first), StandardCharsets.UTF_8).trim();
    assertEquals(FatJarCache.sha256Hex(first), sha);
    assertEquals(sha, FatJarCache.sha256Of(first));

    FileTime built = Files.getLastModifiedTime(first);
    Files.setLastModifiedTime(first, FileTime.from(Instant.now().minusSeconds(3600)));
    Path second = FatJarCache.getOrBuild(LogChannel.GENERAL, variables, "test", jars);
    assertEquals(first, second);
    // Reuse touches the jar for retention, it does not rebuild it
    assertTrue(Files.getLastModifiedTime(second).compareTo(built) >= 0);
    assertEquals(sha, FatJarCache.sha256Of(second));
  }

  @Test
  void aChangedInputJarGivesANewJar() throws Exception {
    String a = jar("a.jar", "a/A.txt");
    Path first = FatJarCache.getOrBuild(LogChannel.GENERAL, variables, "test", List.of(a));

    // A plugin upgrade: same file name, other size and time
    jar("a.jar", "a/A.txt", "a/A2.txt");
    new File(a).setLastModified(System.currentTimeMillis() + 5000);
    Path second = FatJarCache.getOrBuild(LogChannel.GENERAL, variables, "test", List.of(a));

    assertNotEquals(first, second);
    try (JarFile jarFile = new JarFile(second.toFile())) {
      assertNotNull(jarFile.getEntry("a/A2.txt"));
    }
  }

  @Test
  void flavorsDoNotShareAJar() throws Exception {
    List<String> jars = List.of(jar("a.jar", "a/A.txt"));
    assertNotEquals(
        FatJarCache.fingerprint(NativeSparkFatJar.FLAVOR_NATIVE, jars),
        FatJarCache.fingerprint(NativeSparkFatJar.FLAVOR_PROVIDED, jars));
  }

  @Test
  void installedJarsSkipsExcludedFolders() throws Exception {
    Path installation = tempDir.resolve("hop");
    touch(installation.resolve("lib/core/hop-core.jar"));
    touch(installation.resolve("lib/spark-client/spark-core_2.12-3.5.8.jar"));
    touch(installation.resolve("plugins/engines/spark/lib/spark-sql_2.13-4.1.3.jar"));
    touch(installation.resolve("plugins/engines/spark/hop-engines-spark.jar"));
    touch(installation.resolve("plugins/engines/spark/README.txt"));

    List<String> all = InstalledJars.find(installation.toFile(), file -> false);
    assertEquals(4, all.size());

    List<String> provided =
        InstalledJars.find(installation.toFile(), file -> NativeSparkFatJar.isExcluded(file, true));
    assertEquals(2, provided.size());
    assertTrue(provided.get(0).endsWith("hop-core.jar"));
    assertTrue(provided.get(1).endsWith("hop-engines-spark.jar"));
  }

  private String jar(String name, String... entries) throws IOException {
    Path jar = tempDir.resolve(name);
    try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(jar))) {
      for (String entry : entries) {
        out.putNextEntry(new ZipEntry(entry));
        out.write(new byte[] {(byte) 0xCA, (byte) 0xFE});
        out.closeEntry();
      }
    }
    return jar.toString();
  }

  private static void touch(Path path) throws IOException {
    Files.createDirectories(path.getParent());
    Files.write(path, new byte[0]);
  }
}
