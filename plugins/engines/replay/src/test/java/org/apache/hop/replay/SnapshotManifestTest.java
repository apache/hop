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

package org.apache.hop.replay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.replay.manifest.ActionSnapshotManifest;
import org.apache.hop.replay.manifest.SnapshotFieldMeta;
import org.apache.hop.replay.manifest.SnapshotManifest;
import org.junit.jupiter.api.Test;

class SnapshotManifestTest {

  @Test
  void testTransformSnapshotManifestJson() throws Exception {
    SnapshotManifest manifest = new SnapshotManifest();
    manifest.setSnapshotId("snap-12345");
    manifest.setPipelineName("pipeline-test");
    manifest.setTransformName("Table Input");
    manifest.setCopyNr(0);
    manifest.setCreatedTimestamp("2026-09-27T12:00:00Z");
    manifest.setRowCount(1500L);
    manifest.setCompression("Snappy");
    manifest.setDataFile("data.bin.snappy");
    manifest.setChecksum("abcdef1234567890");
    manifest.setStatus(SnapshotManifest.STATUS_SEALED);
    manifest.setFields(
        List.of(
            new SnapshotFieldMeta("id", "Integer", 9, 0),
            new SnapshotFieldMeta("name", "String", 50, -1)));

    String json = manifest.toJson();
    assertNotNull(json);

    SnapshotManifest parsed = SnapshotManifest.fromJson(json);
    assertEquals("snap-12345", parsed.getSnapshotId());
    assertEquals("pipeline-test", parsed.getPipelineName());
    assertEquals("Table Input", parsed.getTransformName());
    assertEquals(0, parsed.getCopyNr());
    assertEquals(1500L, parsed.getRowCount());
    assertEquals("Snappy", parsed.getCompression());
    assertEquals("data.bin.snappy", parsed.getDataFile());
    assertEquals("abcdef1234567890", parsed.getChecksum());
    assertEquals(SnapshotManifest.STATUS_SEALED, parsed.getStatus());
    assertEquals(2, parsed.getFields().size());
    assertEquals("id", parsed.getFields().get(0).getName());
    assertEquals("Integer", parsed.getFields().get(0).getType());
  }

  @Test
  void testActionSnapshotManifestJson() throws Exception {
    ActionSnapshotManifest manifest = new ActionSnapshotManifest();
    manifest.setManifestId("act-67890");
    manifest.setWorkflowName("main-wf");
    manifest.setActionName("stage-table.hpl");
    manifest.setActionType("PIPELINE");
    manifest.setExecutionDate("2026-09-27T12:05:00Z");
    manifest.setStatus(ActionSnapshotManifest.STATUS_SEALED);
    manifest.setElapsedTimeMillis(1234L);
    manifest.setResult(true);
    manifest.setNrErrors(0L);
    manifest.setNrLinesInput(100L);
    manifest.setNrLinesOutput(100L);
    manifest.getVariables().put("VAR_A", "VAL_A");

    String json = manifest.toJson();
    assertNotNull(json);

    ActionSnapshotManifest parsed = ActionSnapshotManifest.fromJson(json);
    assertEquals("act-67890", parsed.getManifestId());
    assertEquals("main-wf", parsed.getWorkflowName());
    assertEquals("stage-table.hpl", parsed.getActionName());
    assertEquals("PIPELINE", parsed.getActionType());
    assertEquals(ActionSnapshotManifest.STATUS_SEALED, parsed.getStatus());
    assertTrue(parsed.isResult());
    assertEquals(0L, parsed.getNrErrors());
    assertEquals(100L, parsed.getNrLinesInput());
    assertEquals("VAL_A", parsed.getVariables().get("VAR_A"));
  }
}
