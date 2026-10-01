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

package org.apache.hop.replay.action;

import java.util.Map;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.replay.ReplayDefaults;
import org.apache.hop.replay.ReplayGate;
import org.apache.hop.replay.ReplayGatePainter;
import org.apache.hop.replay.ReplayGateUtil;
import org.apache.hop.workflow.WorkflowPainterExtension;

@ExtensionPoint(
    id = "DrawActionReplayGateExtensionPoint",
    description = "Draw a gate badge over a workflow action which is configured as a Replay Gate",
    extensionPointId = "WorkflowPainterAction")
public class DrawActionReplayGateExtensionPoint
    implements IExtensionPoint<WorkflowPainterExtension> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, WorkflowPainterExtension ext) {
    try {
      Map<String, String> replayGroup =
          ext.workflowMeta.getAttributesMap().get(ReplayDefaults.REPLAY_GATE_GROUP);
      if (replayGroup != null) {
        String actionName = ext.actionMeta.getName();
        ReplayGate gate = ReplayGateUtil.getReplayGate(replayGroup, actionName);
        if (gate != null && gate.isEnabled()) {
          Rectangle r =
              ReplayGatePainter.drawGate(
                  ext.gc, ext.x1, ext.y1, ext.iconSize, this.getClass().getClassLoader());
          ext.areaOwners.add(
              new AreaOwner(
                  AreaOwner.AreaType.CUSTOM,
                  r.x,
                  r.y,
                  r.width,
                  r.height,
                  ext.offset,
                  ext.actionMeta,
                  gate));
        }
      }
    } catch (Exception e) {
      // Ignore paint exceptions
    }
  }
}
