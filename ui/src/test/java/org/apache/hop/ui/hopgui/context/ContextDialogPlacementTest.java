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

package org.apache.hop.ui.hopgui.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.junit.jupiter.api.Test;

class ContextDialogPlacementTest {

  @Test
  void encodeDecodeCreateAction() {
    GuiAction action =
        new GuiAction(
            "pipeline-graph-create-transform-Dummy",
            GuiActionType.Create,
            "Dummy",
            "desc",
            null,
            (a, b, c) -> {});
    String payload = ContextDialogPlacement.encode(action);
    assertTrue(ContextDialogPlacement.isPlacementPayload(payload));
    assertFalse(ContextDialogPlacement.isChainPayload(payload));
    assertEquals(action.getId(), ContextDialogPlacement.decodeActionId(payload));
  }

  @Test
  void encodeDecodeChainPayload() {
    String payload = ContextDialogPlacement.encode("pipeline-graph-create-transform-Dummy", true);
    assertTrue(ContextDialogPlacement.isPlacementPayload(payload));
    assertTrue(ContextDialogPlacement.isChainPayload(payload));
    assertEquals(
        "pipeline-graph-create-transform-Dummy", ContextDialogPlacement.decodeActionId(payload));
  }

  @Test
  void ignoresUnrelatedText() {
    assertFalse(ContextDialogPlacement.isPlacementPayload("hello"));
    assertNull(ContextDialogPlacement.decodeActionId("hello"));
  }
}
