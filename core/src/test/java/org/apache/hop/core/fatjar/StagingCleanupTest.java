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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StagingCleanupTest {

  private static final Duration RETENTION = Duration.ofDays(14);
  private static final String JAR = "hop-native-0123456789ab.jar";
  private static final String PACKAGE = "hop-spark-package-ba9876543210.zip";
  private static final String RUNTIME = "spark-runtime-aaaaaaaaaaaa.zip";

  @TempDir Path tempDir;

  private Path folder;
  private VfsStagingStore store;

  @BeforeAll
  static void init() throws Exception {
    HopLogStore.init();
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    folder = Files.createDirectories(tempDir.resolve("staging"));
    store = new VfsStagingStore(new Variables(), folder.toString(), null, "no folder");
  }

  @Test
  void stagingMarksEveryFileItUsesAlsoWhenAlreadyStaged() throws Exception {
    Path jar = tempDir.resolve("fat.jar");
    Files.write(jar, new byte[] {1, 2, 3});
    ContentStaging staging = new ContentStaging(LogChannel.GENERAL, store);
    String name = ContentStaging.fatJarName(jar);
    staging.stageFatJar(jar);
    Path marker = folder.resolve(name + StagingCleanup.MARKER_SUFFIX);
    assertTrue(Files.exists(marker));
    assertTrue(Files.readString(marker).startsWith(StagingCleanup.MARKER_FORMAT + "\n"));

    // A second run of another client: the jar is there, the marker is refreshed
    age(marker, 30);
    new ContentStaging(LogChannel.GENERAL, store).stageFatJar(jar);
    assertTrue(lastModified(marker).isAfter(Instant.now().minusSeconds(60)));
  }

  @Test
  void removesOnlyHopFilesNotUsedWithinTheRetention() throws Exception {
    stage(JAR, 40, 40);
    write(JAR + FatJarCache.SHA256_SIDECAR_SUFFIX, 40);
    stage(PACKAGE, 40, 2);
    write("customer-data.csv", 400);
    write("hop-native-notahash.jar", 400);

    List<StagingCleanup.Entry> entries = StagingCleanup.inspect(store, RETENTION, Instant.now());
    assertEquals(List.of(JAR, PACKAGE), entries.stream().map(StagingCleanup.Entry::name).toList());
    assertEquals(StagingCleanup.State.EXPIRED, entries.get(0).state());
    assertEquals(StagingCleanup.State.IN_USE, entries.get(1).state());
    assertEquals(3, entries.get(0).files().size());

    StagingCleanup.Result result =
        StagingCleanup.cleanUp(LogChannel.GENERAL, store, RETENTION, Instant.now());
    assertEquals(1, result.removed().size());
    assertFalse(Files.exists(folder.resolve(JAR)));
    assertFalse(Files.exists(folder.resolve(JAR + FatJarCache.SHA256_SIDECAR_SUFFIX)));
    assertFalse(Files.exists(folder.resolve(JAR + StagingCleanup.MARKER_SUFFIX)));
    assertTrue(Files.exists(folder.resolve(PACKAGE)));
    assertTrue(Files.exists(folder.resolve("customer-data.csv")));
    assertTrue(Files.exists(folder.resolve("hop-native-notahash.jar")));
  }

  @Test
  void theMarkerCountsNotTheUploadTime() throws Exception {
    // Uploaded long ago, used yesterday by a client that found it staged
    stage(RUNTIME, 300, 1);
    StagingCleanup.cleanUp(LogChannel.GENERAL, store, RETENTION, Instant.now());
    assertTrue(Files.exists(folder.resolve(RUNTIME)));
  }

  /**
   * A file without a marker was staged by a Hop version that wrote none, such as the fat jar of a
   * Databricks job deployed back then: the job may use it every day, so it is never removed.
   */
  @Test
  void aFileStagedWithoutAMarkerIsKept() throws Exception {
    write(JAR, 400);
    write(JAR + FatJarCache.SHA256_SIDECAR_SUFFIX, 400);
    List<StagingCleanup.Entry> entries = StagingCleanup.inspect(store, RETENTION, Instant.now());
    assertEquals(StagingCleanup.State.NOT_MARKED, entries.get(0).state());
    assertNotNull(entries.get(0).lastUsed(), "the upload time still shows");

    StagingCleanup.cleanUp(LogChannel.GENERAL, store, RETENTION, Instant.now());
    assertTrue(Files.exists(folder.resolve(JAR)));
    assertTrue(Files.exists(folder.resolve(JAR + FatJarCache.SHA256_SIDECAR_SUFFIX)));
  }

  @Test
  void aMarkerOfANewerHopKeepsItsFile() throws Exception {
    write(JAR, 40);
    Path marker = folder.resolve(JAR + StagingCleanup.MARKER_SUFFIX);
    Files.writeString(marker, "hop-staging-marker/2\nlease=forever\n", StandardCharsets.UTF_8);
    age(marker, 40);
    List<StagingCleanup.Entry> entries = StagingCleanup.inspect(store, RETENTION, Instant.now());
    assertEquals(StagingCleanup.State.UNKNOWN_MARKER, entries.get(0).state());
    StagingCleanup.cleanUp(LogChannel.GENERAL, store, RETENTION, Instant.now());
    assertTrue(Files.exists(folder.resolve(JAR)));
  }

  /**
   * A store that does not know when a file was written reports 0: nothing is removed on a time
   * nobody knows. The cleanup runs right after a run's own submit, and that run may be about to
   * download the files.
   */
  @Test
  void aFileWithoutAModificationTimeIsKept() throws Exception {
    write(JAR, 0);
    write(PACKAGE, 0);
    IStagingStore noTimes = withoutModificationTimes(store);

    List<StagingCleanup.Entry> entries = StagingCleanup.inspect(noTimes, RETENTION, Instant.now());
    assertEquals(2, entries.size());
    for (StagingCleanup.Entry entry : entries) {
      assertEquals(StagingCleanup.State.UNKNOWN_TIME, entry.state(), entry.name());
      assertNull(entry.lastUsed(), entry.name());
    }
    StagingCleanup.cleanUp(LogChannel.GENERAL, noTimes, RETENTION, Instant.now());
    assertTrue(Files.exists(folder.resolve(JAR)));
    assertTrue(Files.exists(folder.resolve(PACKAGE)));
  }

  /** Without a modification time, the marker's own text says when a run last used the file. */
  @Test
  void aMarkerWithoutAModificationTimeSaysWhenItWasWritten() throws Exception {
    write(JAR, 0);
    Files.writeString(
        folder.resolve(JAR + StagingCleanup.MARKER_SUFFIX),
        StagingCleanup.markerText(Instant.now().minus(Duration.ofDays(40))),
        StandardCharsets.UTF_8);
    write(PACKAGE, 0);
    Files.writeString(
        folder.resolve(PACKAGE + StagingCleanup.MARKER_SUFFIX),
        StagingCleanup.markerText(Instant.now().minus(Duration.ofDays(1))),
        StandardCharsets.UTF_8);
    IStagingStore noTimes = withoutModificationTimes(store);

    StagingCleanup.cleanUp(LogChannel.GENERAL, noTimes, RETENTION, Instant.now());
    assertFalse(Files.exists(folder.resolve(JAR)), "used 40 days ago");
    assertTrue(Files.exists(folder.resolve(PACKAGE)), "used yesterday");
  }

  /** A file with a keep sidecar, such as the fat jar of a Databricks job, is never removed. */
  @Test
  void aKeptFileIsNeverRemoved() throws Exception {
    Path jar = tempDir.resolve("fat.jar");
    Files.write(jar, new byte[] {1, 2, 3});
    ContentStaging staging = new ContentStaging(LogChannel.GENERAL, store);
    String name = ContentStaging.fatJarName(jar);
    staging.stageFatJar(jar);
    staging.keep(name, "A Databricks job uses this fat jar.");
    Path keep = folder.resolve(name + StagingCleanup.KEEP_SUFFIX);
    assertTrue(Files.readString(keep).startsWith("A Databricks job uses this fat jar."));
    age(folder.resolve(name), 400);
    age(folder.resolve(name + StagingCleanup.MARKER_SUFFIX), 400);
    age(keep, 400);

    List<StagingCleanup.Entry> entries = StagingCleanup.inspect(store, RETENTION, Instant.now());
    assertEquals(StagingCleanup.State.KEPT, entries.get(0).state());
    StagingCleanup.cleanUp(LogChannel.GENERAL, store, RETENTION, Instant.now());
    assertTrue(Files.exists(folder.resolve(name)));

    // Without the sidecar it ages like any other file
    Files.delete(keep);
    StagingCleanup.cleanUp(LogChannel.GENERAL, store, RETENTION, Instant.now());
    assertFalse(Files.exists(folder.resolve(name)));
  }

  /** An upload goes to a name of its own first: no run finds half a file under the final name. */
  @Test
  void anUploadLeavesOnlyTheCompleteFile() throws Exception {
    Path jar = tempDir.resolve("fat.jar");
    Files.write(jar, new byte[] {1, 2, 3});
    store.upload(jar, JAR);
    assertEquals(3, store.size(JAR));
    try (Stream<Path> files = Files.list(folder)) {
      assertEquals(List.of(JAR), files.map(f -> f.getFileName().toString()).toList());
    }
  }

  /** A run that staged the same content meanwhile keeps its file: its driver may be reading it. */
  @Test
  void anUploadKeepsTheSameContentStagedMeanwhile() throws Exception {
    Path jar = tempDir.resolve("fat.jar");
    Files.write(jar, new byte[] {1, 2, 3});
    Files.write(folder.resolve(JAR), new byte[] {1, 2, 3});
    age(folder.resolve(JAR), 2);
    Instant before = lastModified(folder.resolve(JAR));

    store.upload(jar, JAR);
    assertEquals(before, lastModified(folder.resolve(JAR)));
    try (Stream<Path> files = Files.list(folder)) {
      assertEquals(1, files.count(), "no partial upload left");
    }
  }

  /** A partial upload of a run that was killed goes with its staged file. */
  @Test
  void aPartialUploadGoesWithItsFile() throws Exception {
    stage(JAR, 40, 40);
    write(JAR + StagingCleanup.UPLOAD_INFIX + "0f1e", 40);
    List<StagingCleanup.Entry> entries = StagingCleanup.inspect(store, RETENTION, Instant.now());
    assertEquals(1, entries.size());
    assertEquals(3, entries.get(0).files().size());

    StagingCleanup.cleanUp(LogChannel.GENERAL, store, RETENTION, Instant.now());
    assertFalse(Files.exists(folder.resolve(JAR + StagingCleanup.UPLOAD_INFIX + "0f1e")));
  }

  @Test
  void leftOverSidecarsAreRemovedToo() throws Exception {
    // The package itself is gone: only its marker is left
    stage(PACKAGE, 40, 40);
    Files.delete(folder.resolve(PACKAGE));
    StagingCleanup.cleanUp(LogChannel.GENERAL, store, RETENTION, Instant.now());
    assertFalse(Files.exists(folder.resolve(PACKAGE + StagingCleanup.MARKER_SUFFIX)));
  }

  @Test
  void automaticCleanupRunsOnceADayAndCanBeTurnedOff() throws Exception {
    IVariables variables = new Variables();
    variables.setVariable(FatJarCache.VARIABLE_CACHE_FOLDER, tempDir.resolve("cache").toString());

    variables.setVariable(StagingCleanup.VARIABLE_RETENTION_DAYS, "0");
    stage(JAR, 40, 40);
    StagingCleanup.cleanUpIfDue(LogChannel.GENERAL, variables, store);
    assertTrue(Files.exists(folder.resolve(JAR)));

    variables.setVariable(StagingCleanup.VARIABLE_RETENTION_DAYS, "14");
    StagingCleanup.cleanUpIfDue(LogChannel.GENERAL, variables, store);
    assertFalse(Files.exists(folder.resolve(JAR)));

    // Same day: not again
    stage(JAR, 40, 40);
    StagingCleanup.cleanUpIfDue(LogChannel.GENERAL, variables, store);
    assertTrue(Files.exists(folder.resolve(JAR)));
  }

  @Test
  void aMissingFolderHasNothingStaged() throws Exception {
    VfsStagingStore missing =
        new VfsStagingStore(new Variables(), tempDir.resolve("nope").toString(), null, "x");
    assertTrue(StagingCleanup.inspect(missing, RETENTION, Instant.now()).isEmpty());
  }

  @Test
  void recognizesTheFilesHopStages() {
    assertEquals(JAR, StagingCleanup.stagedName(JAR + ".sha256"));
    assertEquals(PACKAGE, StagingCleanup.stagedName(PACKAGE + ".lastused"));
    assertEquals(RUNTIME, StagingCleanup.stagedName(RUNTIME));
    assertEquals(JAR, StagingCleanup.stagedName(JAR + StagingCleanup.UPLOAD_INFIX + "0f1e"));
    assertNull(StagingCleanup.stagedName("data.csv" + StagingCleanup.UPLOAD_INFIX + "0f1e"));
    assertNull(StagingCleanup.stagedName("hop-native.jar"));
    assertNull(StagingCleanup.stagedName("other-0123456789ab.jar"));
  }

  /** The store, except that it reports no modification times, as some object stores do. */
  private static IStagingStore withoutModificationTimes(VfsStagingStore store) {
    return new IStagingStore() {
      @Override
      public long size(String name) throws HopException {
        return store.size(name);
      }

      @Override
      public void upload(Path localFile, String name) throws HopException {
        store.upload(localFile, name);
      }

      @Override
      public String location(String name) {
        return store.location(name);
      }

      @Override
      public String clusterUrl(String name) {
        return store.clusterUrl(name);
      }

      @Override
      public boolean supportsCleanup() {
        return true;
      }

      @Override
      public List<StagedFile> list() throws HopException {
        return store.list().stream()
            .map(file -> new StagedFile(file.name(), file.size(), 0L))
            .toList();
      }

      @Override
      public void delete(String name) throws HopException {
        store.delete(name);
      }

      @Override
      public void writeText(String name, String text) throws HopException {
        store.writeText(name, text);
      }

      @Override
      public String readText(String name) throws HopException {
        return store.readText(name);
      }
    };
  }

  private void stage(String name, int uploadedDaysAgo, int usedDaysAgo) throws Exception {
    write(name, uploadedDaysAgo);
    Path marker = folder.resolve(name + StagingCleanup.MARKER_SUFFIX);
    Files.writeString(marker, StagingCleanup.markerText(Instant.now()), StandardCharsets.UTF_8);
    age(marker, usedDaysAgo);
  }

  private void write(String name, int daysAgo) throws Exception {
    Path file = folder.resolve(name);
    Files.write(file, new byte[] {4, 2});
    age(file, daysAgo);
  }

  private static void age(Path file, int daysAgo) throws Exception {
    Files.setLastModifiedTime(file, FileTime.from(Instant.now().minus(Duration.ofDays(daysAgo))));
  }

  private static Instant lastModified(Path file) throws Exception {
    return Files.getLastModifiedTime(file).toInstant();
  }
}
