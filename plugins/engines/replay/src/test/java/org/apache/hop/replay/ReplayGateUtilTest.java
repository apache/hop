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

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.replay.engine.ReplayWorkflowRunConfiguration;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.junit.jupiter.api.Test;

class ReplayGateUtilTest {

  @Test
  void storeAndGetReplayGateOnTheAction() {
    ActionMeta actionMeta = new ActionMeta(new ActionDummy("Stage Customers"));
    assertNull(ReplayGateUtil.getReplayGate(actionMeta));

    ReplayGate gate = new ReplayGate();
    gate.setEnabled(true);
    gate.setSpoolDirectory("s3://lakehouse/spool");
    gate.setDescription("Stage gate for customer extracts");
    ReplayGateUtil.storeReplayGate(actionMeta, gate);

    ReplayGate loaded = ReplayGateUtil.getReplayGate(actionMeta);
    assertNotNull(loaded);
    assertTrue(loaded.isEnabled());
    assertEquals("s3://lakehouse/spool", loaded.getSpoolDirectory());
    assertEquals("Stage gate for customer extracts", loaded.getDescription());

    actionMeta.setName("Stage Customers renamed");
    ReplayGate renamed = ReplayGateUtil.getReplayGate(actionMeta);
    assertNotNull(renamed);
    assertEquals("s3://lakehouse/spool", renamed.getSpoolDirectory());
  }

  @Test
  void clearReplayGateRemovesTheAttributeGroup() {
    ActionMeta actionMeta = new ActionMeta(new ActionDummy("Load"));
    ReplayGate gate = new ReplayGate();
    gate.setEnabled(true);
    ReplayGateUtil.storeReplayGate(actionMeta, gate);

    ReplayGateUtil.clearReplayGate(actionMeta);

    assertNull(ReplayGateUtil.getReplayGate(actionMeta));
  }

  @Test
  void emptyOptionalFieldsAreNotStored() {
    ActionMeta actionMeta = new ActionMeta(new ActionDummy("Load"));
    ReplayGate gate = new ReplayGate();
    gate.setEnabled(true);
    gate.setSpoolDirectory("");
    gate.setDescription("");
    ReplayGateUtil.storeReplayGate(actionMeta, gate);

    ReplayGate loaded = ReplayGateUtil.getReplayGate(actionMeta);
    assertNotNull(loaded);
    assertTrue(loaded.isEnabled());
    assertEquals("", loaded.getSpoolDirectory());
    assertEquals("", loaded.getDescription());
    assertFalse(
        actionMeta
            .getAttributes(ReplayDefaults.REPLAY_GATE_GROUP)
            .containsKey(ReplayDefaults.GATE_ATTR_SPOOL_DIR));
  }

  @Test
  void labelsResolve() {
    assertEquals("Enabled", BaseMessages.getString(ReplayGate.class, "ReplayGate.Enabled.Label"));
    assertEquals("Replay gate", BaseMessages.getString(ReplayGate.class, "ReplayGate.Group"));
    assertEquals(
        "Source execution ID",
        BaseMessages.getString(
            ReplayWorkflowRunConfiguration.class,
            "ReplayWorkflowRunConfiguration.SourceExecutionId.Label"));
    assertEquals(
        "Start action name",
        BaseMessages.getString(
            ReplayWorkflowRunConfiguration.class,
            "ReplayWorkflowRunConfiguration.StartActionName.Label"));
  }

  @Test
  void spoolPathIncludesTheExecutionId() {
    ReplayGate gate = new ReplayGate();
    gate.setSpoolDirectory("/tmp/spool");
    assertEquals(
        "/tmp/spool/workflows/Nightly/exec-1/Load",
        ReplayGateUtil.getActionSpoolPath(null, gate, "Nightly", "exec-1", "Load"));
  }
}
