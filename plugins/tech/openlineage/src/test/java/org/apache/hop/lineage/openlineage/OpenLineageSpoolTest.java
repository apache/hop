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

  private static RunEvent event(String jobName) {
    return OL.newRunEventBuilder()
        .eventType(RunEvent.EventType.COMPLETE)
        .eventTime(Instant.ofEpochMilli(1_700_000_000_000L).atZone(ZoneOffset.UTC))
        .run(OL.newRun(UUID.randomUUID(), null))
        .job(OL.newJob("hop", jobName, null))
        .build();
  }

  @Test
  void storesAnEventAndDeletesItOnRequest() {
    OpenLineageSpool spool = spool();
    String id = spool.store(event("pipeline-a"));

    assertNotNull(id);
    assertEquals(1, spool.getStoredCount());
    assertTrue(spool.getStoredBytes() > 0);

    List<OpenLineageSpool.SpooledEvent> stored = spool.loadAll();
    assertEquals(1, stored.size());
    assertEquals(id, stored.get(0).id());

    spool.delete(id);
    assertEquals(0, spool.getStoredCount());
    assertEquals(0, spool.getStoredBytes());
    assertTrue(spool.loadAll().isEmpty());
  }

  @Test
  void replaysEventsInTheOrderTheyWereStored() {
    OpenLineageSpool spool = spool();
    for (String name : List.of("first", "second", "third")) {
      spool.store(event(name));
    }

    // A second instance is what a restarted process sees.
    List<OpenLineageSpool.SpooledEvent> replayed = spool().loadAll();
    assertEquals(
        List.of("first", "second", "third"),
        replayed.stream().map(e -> e.event().getJob().getName()).toList());
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
    spool.store(original);

    RunEvent reloaded = spool.loadAll().get(0).event();
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
    Files.writeString(
        folder.resolve("0000000000001-00000000-" + UUID.randomUUID() + ".json"), "{not json");
    OpenLineageSpool spool = spool();

    assertTrue(spool.loadAll().isEmpty());
    assertEquals(0, spool.getStoredCount());
    assertEquals(1, spool.getUnreadableFiles());
    try (var files = Files.list(folder)) {
      assertEquals(0, files.count(), "the unreadable file is removed so it cannot block replay");
    }
  }

  // A spool pointed at a folder that holds other files must leave them alone: not count them, not
  // replay them and certainly not delete them as unreadable.
  @Test
  void leavesFilesItDidNotWriteAlone() throws IOException {
    Files.writeString(folder.resolve("settings.json"), "{\"mine\": true}");
    Path sub = Files.createDirectory(folder.resolve("archive"));
    Files.writeString(sub.resolve("0000000000001-00000000-" + UUID.randomUUID() + ".json"), "{}");
    OpenLineageSpool spool = spool();

    assertEquals(0, spool.getStoredCount());
    assertTrue(spool.loadAll().isEmpty());
    assertEquals(0, spool.getUnreadableFiles());
    assertTrue(Files.exists(folder.resolve("settings.json")));
    try (var files = Files.list(sub)) {
      assertEquals(1, files.count());
    }
  }

  // Two processes sharing a folder can both replay a file; the one that loses the race to delete
  // it must still stop counting it, or the spool drifts towards "full".
  @Test
  void stopsCountingAFileAnotherProcessDelivered() throws IOException {
    OpenLineageSpool spool = spool();
    String id = spool.store(event("shared"));
    Files.delete(folder.resolve(id));

    spool.delete(id);

    assertEquals(0, spool.getStoredCount());
    assertEquals(0, spool.getStoredBytes());
  }

  @Test
  void ignoresPartialFiles() throws IOException {
    Files.writeString(
        folder.resolve("0000000000001-00000000-half" + OpenLineageSpool.TEMP_SUFFIX),
        "{\"eventType\":",
        StandardCharsets.UTF_8);
    OpenLineageSpool spool = spool();
    assertTrue(spool.loadAll().isEmpty());
    assertEquals(0, spool.getStoredCount());
  }

  @Test
  void survivesTheSharedFileSystemManagerBeingReset() {
    OpenLineageSpool spool = spool();
    String id = spool.store(event("before-reset"));

    // hop-run replaces the shared VFS manager when it loads and closes a project; a sink lives
    // through both, so it must not hold on to FileObjects.
    HopVfs.reset();

    assertEquals(1, spool.loadAll().size());
    assertNotNull(spool.store(event("after-reset")));
    spool.delete(id);
    assertEquals(1, spool.getStoredCount());
  }

  @Test
  void isDisabledWithoutADirectory() {
    assertNull(OpenLineageSpool.open("", 1000, LOG));
    assertNull(OpenLineageSpool.open(null, 1000, LOG));
  }
}
