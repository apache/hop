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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ReplayGateUtilTest {

  @Test
  void testStoreAndGetReplayGate() {
    Map<String, Map<String, String>> attributesMap = new HashMap<>();
    Map<String, String> replayGroup = ReplayGateUtil.getOrCreateReplayGroup(attributesMap);

    assertNotNull(replayGroup);
    assertFalse(ReplayGateUtil.hasReplayGate(replayGroup, "Stage Customers"));

    ReplayGate gate = new ReplayGate();
    gate.setEnabled(true);
    gate.setSpoolDirectory("s3://lakehouse/spool");
    gate.setCompression("ZSTD");
    gate.setRowLimit(5000);
    gate.setDescription("Stage gate for customer extracts");

    ReplayGateUtil.storeReplayGate(replayGroup, "Stage Customers", gate);

    assertTrue(ReplayGateUtil.hasReplayGate(replayGroup, "Stage Customers"));

    ReplayGate loaded = ReplayGateUtil.getReplayGate(replayGroup, "Stage Customers");
    assertNotNull(loaded);
    assertTrue(loaded.isEnabled());
    assertEquals("s3://lakehouse/spool", loaded.getSpoolDirectory());
    assertEquals("ZSTD", loaded.getCompression());
    assertEquals(5000, loaded.getRowLimit());
    assertEquals("Stage gate for customer extracts", loaded.getDescription());
  }

  @Test
  void testClearReplayGate() {
    Map<String, Map<String, String>> attributesMap = new HashMap<>();
    Map<String, String> replayGroup = ReplayGateUtil.getOrCreateReplayGroup(attributesMap);

    ReplayGate gate = new ReplayGate();
    gate.setEnabled(true);
    ReplayGateUtil.storeReplayGate(replayGroup, "TransformA", gate);

    assertTrue(ReplayGateUtil.hasReplayGate(replayGroup, "TransformA"));

    ReplayGateUtil.clearReplayGate(replayGroup, "TransformA");

    assertFalse(ReplayGateUtil.hasReplayGate(replayGroup, "TransformA"));
    assertNull(ReplayGateUtil.getReplayGate(replayGroup, "TransformA"));
  }

  @Test
  void testI18nLabelsResolvable() {
    assertEquals(
        "Enabled",
        org.apache.hop.i18n.BaseMessages.getString(ReplayGate.class, "ReplayGate.Enabled.Label"));
    assertEquals(
        "Enable this Replay Gate",
        org.apache.hop.i18n.BaseMessages.getString(ReplayGate.class, "ReplayGate.Enabled.Tooltip"));
    assertEquals(
        "Spool directory",
        org.apache.hop.i18n.BaseMessages.getString(
            ReplayGate.class, "ReplayGate.SpoolDirectory.Label"));
  }

  @Test
  void testStoreReplayGateWithEmptyFields() {
    Map<String, Map<String, String>> attributesMap = new HashMap<>();
    Map<String, String> replayGroup = ReplayGateUtil.getOrCreateReplayGroup(attributesMap);

    ReplayGate gate = new ReplayGate();
    gate.setEnabled(true);
    gate.setSpoolDirectory("");
    gate.setCompression("");
    gate.setDescription("");
    ReplayGateUtil.storeReplayGate(replayGroup, "TransformEmpty", gate);

    assertTrue(ReplayGateUtil.hasReplayGate(replayGroup, "TransformEmpty"));
    ReplayGate loaded = ReplayGateUtil.getReplayGate(replayGroup, "TransformEmpty");
    assertNotNull(loaded);
    assertTrue(loaded.isEnabled());
    assertEquals("", loaded.getSpoolDirectory());
    assertEquals("", loaded.getCompression());
    assertEquals(0, loaded.getRowLimit());
    assertEquals("", loaded.getDescription());
  }
}
