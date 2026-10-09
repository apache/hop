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

package org.apache.hop.lakehouse.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.HopEnvironment;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PathTableOperationsTest {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @TempDir Path tempDir;

  private Path tableDir;
  private String location;
  private final AtomicInteger fileCounter = new AtomicInteger();

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void createTable() throws Exception {
    tableDir = tempDir.resolve("orders");
    location = tableDir.toUri().toString();
    IcebergTableTarget.atPath(location)
        .create(SCHEMA, PartitionSpec.unpartitioned(), Map.of())
        .commitTransaction();
  }

  private Path metadata(String name) {
    return tableDir.resolve("metadata").resolve(name);
  }

  private DataFile dataFile() {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(
            tableDir.resolve("data/file-" + fileCounter.incrementAndGet() + ".parquet").toString())
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private void append() {
    PathTableOperations ops = new PathTableOperations(location);
    new BaseTable(ops, location).newAppend().appendFile(dataFile()).commit();
  }

  private void setHint(String text) throws Exception {
    Files.writeString(metadata("version-hint.text"), text, StandardCharsets.UTF_8);
  }

  @Test
  void staleHintIsFollowedForward() throws Exception {
    append();
    append();
    assertTrue(Files.exists(metadata("v3.metadata.json")));
    // A crash between publishing v3 and updating the hint leaves the hint behind.
    setHint("1");

    assertTrue(
        IcebergTables.findCurrentMetadataFile(location).endsWith("/metadata/v3.metadata.json"));
    assertEquals(2, snapshotCount(IcebergTables.loadFromPath(location)));

    append();
    assertTrue(Files.exists(metadata("v4.metadata.json")), "the next commit writes v4");
    assertEquals("4", Files.readString(metadata("version-hint.text")).trim());
  }

  @Test
  void unreadableHintFallsBackToTheNewestFile() throws Exception {
    append();
    Files.delete(metadata("version-hint.text"));
    Files.createDirectory(metadata("version-hint.text"));

    append();

    assertTrue(Files.exists(metadata("v3.metadata.json")), "the commit succeeds without a hint");
    assertTrue(
        IcebergTables.findCurrentMetadataFile(location).endsWith("/metadata/v3.metadata.json"));
  }

  @Test
  void publishRefusesAVersionThatIsTaken() throws Exception {
    Path v1 = metadata("v1.metadata.json");
    String before = Files.readString(v1);
    Path temp = metadata("other-writer.metadata.json.tmp");
    Files.writeString(temp, "{}", StandardCharsets.UTF_8);

    PathTableOperations ops = new PathTableOperations(location);
    assertThrows(
        CommitFailedException.class,
        () -> ops.publish(temp.toUri().toString(), v1.toUri().toString(), 1));
    assertEquals(before, Files.readString(v1), "the committed version is left alone");
  }

  @Test
  void concurrentWritersInOneJvmLoseNoSnapshot() throws Exception {
    int writers = 8;
    int appendsEach = 5;
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<Integer>> results = new ArrayList<>();
    for (int w = 0; w < writers; w++) {
      Callable<Integer> writer =
          () -> {
            start.await();
            int failed = 0;
            for (int i = 0; i < appendsEach; i++) {
              try {
                append();
              } catch (CommitFailedException e) {
                failed++;
              }
            }
            return failed;
          };
      results.add(pool.submit(writer));
    }
    start.countDown();
    int failed = 0;
    for (Future<Integer> result : results) {
      failed += result.get();
    }
    pool.shutdown();

    Table table = IcebergTables.loadFromPath(location);
    int committed = writers * appendsEach - failed;
    assertEquals(
        committed, snapshotCount(table), "every commit that reported success is in the table");
    assertEquals(
        String.valueOf(committed),
        table.currentSnapshot().summary().get("total-data-files"),
        "and every appended file is still referenced");
  }

  private static int snapshotCount(Table table) {
    int count = 0;
    for (var ignored : table.snapshots()) {
      count++;
    }
    return count;
  }

  /**
   * On a file system without hard links, here Hop VFS's in-memory ram://, a version file that
   * already exists is never deleted or replaced: the commit fails instead.
   */
  @Test
  void nonLocalPublishNeverReplacesAVersion() throws Exception {
    String ram = "ram:///path-table-test-" + java.util.UUID.randomUUID();
    String target = ram + "/metadata/v2.metadata.json";
    String temp = ram + "/metadata/other.metadata.json.tmp";
    write(target, "committed by the first writer");
    write(temp, "second writer");

    PathTableOperations ops = new PathTableOperations(ram);
    assertThrows(CommitFailedException.class, () -> ops.publish(temp, target, 2));
    assertEquals("committed by the first writer", read(target));

    String free = ram + "/metadata/v3.metadata.json";
    ops.publish(temp, free, 3);
    assertEquals("second writer", read(free));
  }

  @Test
  void pathTableOnANonLocalFileSystem() throws Exception {
    String ram = "ram:///path-table-test-" + java.util.UUID.randomUUID();
    IcebergTableTarget.atPath(ram)
        .create(SCHEMA, PartitionSpec.unpartitioned(), Map.of())
        .commitTransaction();
    PathTableOperations ops = new PathTableOperations(ram);
    new BaseTable(ops, ram).newAppend().appendFile(dataFile()).commit();

    assertEquals(1, snapshotCount(IcebergTables.loadFromPath(ram)));
    assertTrue(IcebergTables.findCurrentMetadataFile(ram).endsWith("/metadata/v2.metadata.json"));
  }

  private static void write(String uri, String text) throws Exception {
    org.apache.commons.vfs2.FileObject file = org.apache.hop.core.vfs.HopVfs.getFileObject(uri);
    file.getParent().createFolder();
    try (java.io.OutputStream out = org.apache.hop.core.vfs.HopVfs.getOutputStream(file, false)) {
      out.write(text.getBytes(StandardCharsets.UTF_8));
    }
  }

  private static String read(String uri) throws Exception {
    try (java.io.InputStream in = org.apache.hop.core.vfs.HopVfs.getInputStream(uri)) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  /** On a local disk the version is published as a hard link of the temp file, atomically. */
  @Test
  void localPublishUsesAHardLink() throws Exception {
    org.junit.jupiter.api.Assumptions.assumeTrue(
        java.nio.file.FileSystems.getDefault().supportedFileAttributeViews().contains("unix"));
    Path temp = metadata("next.metadata.json.tmp");
    Path target = metadata("v7.metadata.json");
    Files.writeString(temp, "{}", StandardCharsets.UTF_8);

    new PathTableOperations(location)
        .publish(temp.toUri().toString(), target.toUri().toString(), 7);

    assertEquals(2, Files.getAttribute(target, "unix:nlink"), "target and temp are one file");
  }
}
