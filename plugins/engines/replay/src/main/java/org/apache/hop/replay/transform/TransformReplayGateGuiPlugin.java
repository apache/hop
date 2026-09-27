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

package org.apache.hop.replay.transform;

import java.util.Map;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.replay.ReplayDefaults;
import org.apache.hop.replay.ReplayGate;
import org.apache.hop.replay.ReplayGateDialog;
import org.apache.hop.replay.ReplayGateUtil;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineTransformContext;

@GuiPlugin
public class TransformReplayGateGuiPlugin {

  private static final Class<?> PKG = TransformReplayGateGuiPlugin.class;

  @GuiContextAction(
      id = "pipeline-graph-transform-11500-configure-replay-gate",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::ReplayTransform.ConfigureGate.Label",
      tooltip = "i18n::ReplayTransform.ConfigureGate.ToolTip",
      image = "replay-gate.svg",
      category = "Replay",
      categoryOrder = "8")
  public void configureReplayGate(HopGuiPipelineTransformContext context) {
    HopGui hopGui = HopGui.getInstance();
    try {
      PipelineMeta pipelineMeta = context.getPipelineMeta();
      TransformMeta transformMeta = context.getTransformMeta();
      IVariables variables = context.getPipelineGraph().getVariables();

      Map<String, String> replayGroup =
          ReplayGateUtil.getOrCreateReplayGroup(pipelineMeta.getAttributesMap());

      ReplayGate gate = ReplayGateUtil.getReplayGate(replayGroup, transformMeta.getName());
      if (gate == null) {
        gate = new ReplayGate();
      }

      String title =
          BaseMessages.getString(PKG, "ReplayTransform.Dialog.Title", transformMeta.getName());
      ReplayGateDialog dialog =
          new ReplayGateDialog(hopGui.getActiveShell(), variables, gate, title);
      if (dialog.open()) {
        ReplayGateUtil.storeReplayGate(replayGroup, transformMeta.getName(), gate);
        pipelineMeta.setChanged();
        context.getPipelineGraph().updateGui();
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getActiveShell(), "Error", "Error configuring Replay Gate for transform", e);
    }
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-11501-clear-replay-gate",
      parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
      type = GuiActionType.Delete,
      name = "i18n::ReplayTransform.ClearGate.Label",
      tooltip = "i18n::ReplayTransform.ClearGate.ToolTip",
      image = "replay-gate.svg",
      category = "Replay",
      categoryOrder = "8")
  public void clearReplayGate(HopGuiPipelineTransformContext context) {
    PipelineMeta pipelineMeta = context.getPipelineMeta();
    TransformMeta transformMeta = context.getTransformMeta();

    Map<String, String> replayGroup =
        pipelineMeta.getAttributesMap().get(ReplayDefaults.REPLAY_GATE_GROUP);
    if (replayGroup != null) {
      ReplayGateUtil.clearReplayGate(replayGroup, transformMeta.getName());
      pipelineMeta.setChanged();
      context.getPipelineGraph().updateGui();
    }
  }
}
