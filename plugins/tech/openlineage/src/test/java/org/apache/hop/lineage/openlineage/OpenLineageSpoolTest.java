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

package org.apache.hop.lineage.openlineage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OpenLineageSpoolTest {

  private static final OpenLineage OL = new OpenLineage(URI.create("https://example.com/producer"));
  private static final LogChannel LOG = new LogChannel("OpenLineageSpool");

  @TempDir Path folder;

  @BeforeAll
  static void initLogStore() {
    HopLogStore.init();
  }

  private OpenLineageSpool spool() {
    return spool(8L * 1024 * 1024);
  }

  private OpenLineageSpool spool(long maxBytes) {
    OpenLineageSpool spool = OpenLineageSpool.open(folder.toString(), maxBytes, LOG);
    assertNotNull(spool, "spool should open on an existing temp folder");
    return spool;
  }

  /** What a process started after {@code previous} shut down sends again, by job name. */
  private List<String> replayAfter(OpenLineageSpool previous) {
    previous.release();
    return replay(spool());
  }

  private static List<String> replay(OpenLineageSpool spool) {
    return spool.recover().stream().map(id -> spool.load(id).getJob().getName()).toList();
  }

  private static RunEvent event(String jobName) {
    return OL.newRunEventBuilder()
        .eventType(RunEvent.EventType.COMPLETE)
        .eventTime(Instant.ofEpochMilli(1_700_000_000_000L).atZone(ZoneOffset.UTC))
        .run(OL.newRun(UUID.randomUUID(), null))
        .job(OL.newJob("hop", jobName, null))
        .build();
  }

  /** The spool files anywhere under the configured folder. */
  private long filesOnDisk() throws IOException {
    try (Stream<Path> files = Files.walk(folder)) {
      return files
          .filter(path -> path.getFileName().toString().endsWith(OpenLineageSpool.FILE_SUFFIX))
          .count();
    }
  }

  @Test
  void storesAnEventAndDeletesItOnRequest() throws IOException {
    OpenLineageSpool spool = spool();
    String id = spool.store(event("pipeline-a"));

    assertNotNull(id);
    assertEquals(1, spool.getStoredCount());
    assertTrue(spool.getStoredBytes() > 0);
    assertEquals("pipeline-a", spool.load(id).getJob().getName());

    spool.delete(id);
    assertEquals(0, spool.getStoredCount());
    assertEquals(0, spool.getStoredBytes());
    assertEquals(0, filesOnDisk());
  }

  @Test
  void replaysEventsInTheOrderTheyWereStored() {
    OpenLineageSpool spool = spool();
    for (String name : List.of("first", "second", "third")) {
      spool.store(event(name));
    }

    assertEquals(List.of("first", "second", "third"), replayAfter(spool));
  }

  // A Hop Server and a hop-run configured with the same folder: neither may send what the other
  // has stored and not delivered yet.
  @Test
  void aLiveProcessKeepsItsEvents() {
    OpenLineageSpool server = spool();
    server.store(event("server"));

    OpenLineageSpool hopRun = spool();
    assertTrue(hopRun.recover().isEmpty(), "the server is alive; its events are its own");
    assertEquals(1, server.getStoredCount());
  }

  // A process that was killed leaves its heartbeat behind; once that is stale, the next process
  // takes its events over.
  @Test
  void takesOverTheEventsOfAKilledProcess() throws Exception {
    OpenLineageSpool killed = spool();
    killed.store(event("left-behind"));
    Thread.sleep(5);

    OpenLineageSpool next = OpenLineageSpool.open(folder.toString(), "", 1L << 20, 1, LOG);
    assertEquals(List.of("left-behind"), replay(next));
    assertEquals(1, next.getStoredCount());
    assertEquals(1, filesOnDisk(), "the file is moved, not copied");
  }

  // Taking over moves each file, so of two processes starting together only one sends it.
  @Test
  void aTakenOverEventIsSentByOneProcessOnly() {
    OpenLineageSpool previous = spool();
    previous.store(event("once"));
    previous.release();

    OpenLineageSpool first = spool();
    OpenLineageSpool second = spool();
    assertEquals(1, first.recover().size() + second.recover().size());
  }

  // A clean shutdown with nothing left to send leaves nothing behind.
  @Test
  void releasingAnEmptySpoolRemovesItsFolder() throws IOException {
    OpenLineageSpool spool = spool();
    spool.delete(spool.store(event("delivered")));
    spool.release();

    try (Stream<Path> children = Files.list(folder)) {
      assertEquals(0, children.count());
    }
  }

  @Test
  void roundTripsTheWholeEventIncludingFacets() {
    OpenLineage.RunEvent original =
        OL.newRunEventBuilder()
            .eventType(RunEvent.EventType.COMPLETE)
            .eventTime(Instant.ofEpochMilli(1_700_000_000_000L).atZone(ZoneOffset.UTC))
            .run(
                OL.newRunBuilder()
                    .runId(UUID.randomUUID())
                    .facets(
                        OL.newRunFacetsBuilder()
                            .parent(
                                OL.newParentRunFacet(
                                    OL.newParentRunFacetRun(UUID.randomUUID(), null),
                                    OL.newParentRunFacetJob("hop", "workflow", null),
                                    null))
                            .build())
                    .build())
            .job(
                OL.newJobBuilder()
                    .namespace("hop")
                    .name("pipeline")
                    .facets(
                        OL.newJobFacetsBuilder()
                            .documentation(
                                OL.newDocumentationJobFacetBuilder().description("docs").build())
                            .build())
                    .build())
            .inputs(
                List.of(
                    OL.newInputDatasetBuilder()
                        .namespace("file")
                        .name("in.csv")
                        .facets(OL.newDatasetFacetsBuilder().build())
                        .inputFacets(OL.newInputDatasetInputFacetsBuilder().build())
                        .build()))
            .outputs(
                List.of(
                    OL.newOutputDatasetBuilder()
                        .namespace("file")
                        .name("out.csv")
                        .facets(OL.newDatasetFacetsBuilder().build())
                        .outputFacets(OL.newOutputDatasetOutputFacetsBuilder().build())
                        .build()))
            .build();

    OpenLineageSpool spool = spool();
    String id = spool.store(original);

    RunEvent reloaded = spool.load(id);
    assertEquals(
        OpenLineageClientUtils.toJson(original),
        OpenLineageClientUtils.toJson(reloaded),
        "the spool must not lose facets");
  }

  @Test
  void refusesNewEventsWhenTheSpoolIsFull() {
    OpenLineageSpool spool = spool(1);
    assertNull(spool.store(event("too-big")), "a full spool must refuse the event");
    assertEquals(0, spool.getStoredCount());
    assertEquals(1, spool.getFullRefusals());
  }

  @Test
  void discardsFilesThatCannotBeRead() throws IOException {
    OpenLineageSpool previous = spool();
    String id = previous.store(event("broken"));
    previous.release();
    try (Stream<Path> files = Files.walk(folder)) {
      Files.writeString(
          files.filter(path -> path.getFileName().toString().equals(id)).findFirst().orElseThrow(),
          "{not json");
    }

    OpenLineageSpool spool = spool();
    List<String> ids = spool.recover();
    assertEquals(1, ids.size());
    assertNull(spool.load(ids.get(0)));
    assertEquals(0, spool.getStoredCount());
    assertEquals(1, spool.getUnreadableFiles());
    assertEquals(0, filesOnDisk(), "the unreadable file is removed so it cannot block replay");
  }

  // A spool pointed at a folder that holds other files must leave them alone: not count them, not
  // replay them and certainly not delete them as unreadable.
  @Test
  void leavesFilesItDidNotWriteAlone() throws IOException {
    Files.writeString(folder.resolve("settings.json"), "{\"mine\": true}");
    Path sub = Files.createDirectory(folder.resolve("archive"));
    Files.writeString(sub.resolve("0000000000001-00000000-" + UUID.randomUUID() + ".json"), "{}");
    OpenLineageSpool spool = spool();

    assertTrue(spool.recover().isEmpty());
    assertEquals(0, spool.getStoredCount());
    assertEquals(0, spool.getUnreadableFiles());
    assertTrue(Files.exists(folder.resolve("settings.json")));
    try (Stream<Path> files = Files.list(sub)) {
      assertEquals(1, files.count());
    }
  }

  // The OpenLineage and the DataHub sink map the same events differently. Configured with one
  // folder, neither may replay, count or delete what the other one spooled.
  @Test
  void twoSinksSharingAFolderKeepTheirOwnFiles() {
    OpenLineageSpool openLineage = spool();
    OpenLineageSpool dataHub = OpenLineageSpool.open(folder.toString(), "datahub-", 1L << 20, LOG);
    assertNotNull(dataHub);
    openLineage.store(event("for-openlineage"));
    dataHub.store(event("for-datahub"));
    openLineage.release();
    dataHub.release();

    assertEquals(List.of("for-openlineage"), replay(spool()));
    assertEquals(
        List.of("for-datahub"),
        replay(OpenLineageSpool.open(folder.toString(), "datahub-", 1L << 20, LOG)));
  }

  // Whatever removed a file, the spool must stop counting it, or it drifts towards "full".
  @Test
  void stopsCountingAFileThatIsGone() throws IOException {
    OpenLineageSpool spool = spool();
    String id = spool.store(event("shared"));
    try (Stream<Path> files = Files.walk(folder)) {
      Files.delete(
          files.filter(path -> path.getFileName().toString().equals(id)).findFirst().orElseThrow());
    }

    spool.delete(id);

    assertEquals(0, spool.getStoredCount());
    assertEquals(0, spool.getStoredBytes());
  }

  @Test
  void ignoresPartialFiles() throws IOException {
    OpenLineageSpool previous = spool();
    previous.store(event("whole"));
    previous.release();
    try (Stream<Path> folders = Files.list(folder)) {
      Path abandoned = folders.findFirst().orElseThrow();
      Files.writeString(
          abandoned.resolve("0000000000001-00000000-half" + OpenLineageSpool.TEMP_SUFFIX),
          "{\"eventType\":",
          StandardCharsets.UTF_8);
    }

    assertEquals(List.of("whole"), replay(spool()));
  }

  @Test
  void survivesTheSharedFileSystemManagerBeingReset() {
    OpenLineageSpool spool = spool();
    String id = spool.store(event("before-reset"));

    // hop-run replaces the shared VFS manager when it loads and closes a project; a sink lives
    // through both, so it must not hold on to FileObjects.
    HopVfs.reset();

    assertEquals("before-reset", spool.load(id).getJob().getName());
    assertNotNull(spool.store(event("after-reset")));
    spool.delete(id);
    spool.heartbeat();
    assertEquals(1, spool.getStoredCount());
  }

  @Test
  void isDisabledWithoutADirectory() {
    assertNull(OpenLineageSpool.open("", 1000, LOG));
    assertNull(OpenLineageSpool.open(null, 1000, LOG));
  }
}
