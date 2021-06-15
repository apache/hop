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

package org.apache.hop.workflow.actions.workflow;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowClipboardDelegate;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowClipboardExtension;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;

import java.util.List;

@ExtensionPoint(
    id = "HopGuiWorkflowHwfFilenamePastedExtensionPoint",
    extensionPointId = "HopGuiWorkflowClipboardFilePaste",
    description =
        "Handle pasting of a workflow filename on a workflow.  This code turns it into a workflow action.")
public class HopGuiWorkflowHwfFilenamePastedExtensionPoint
    implements IExtensionPoint<HopGuiWorkflowClipboardExtension> {
  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, HopGuiWorkflowClipboardExtension wce)
      throws HopException {

    HopGuiWorkflowClipboardDelegate clipboardDelegate = wce.workflowGraph.workflowClipboardDelegate;
    WorkflowMeta workflowMeta = wce.workflowMeta;
    HopGui hopGui = wce.workflowGraph.getHopGui();

    // Workflow?
    //
    HopWorkflowFileType workflowFileType = new HopWorkflowFileType<>();
    if (wce.filename.endsWith(workflowFileType.getDefaultFileExtension())) {

      // Add a new Workflow action...
      //
      String name = clipboardDelegate.getUniqueName(workflowMeta, wce.file.getName());

      ActionWorkflow actionWorkflow = new ActionWorkflow(name);
      actionWorkflow.setFileName(wce.filename);

      // Pick the first run configuration available...
      //
      List<String> names =
          hopGui
              .getMetadataProvider()
              .getSerializer(WorkflowRunConfiguration.class)
              .listObjectNames();
      if (!names.isEmpty()) {
        actionWorkflow.setRunConfiguration(names.get(0));
      }

      ActionMeta actionMeta = new ActionMeta(actionWorkflow);
      actionMeta.setLocation(new Point(wce.location));

      workflowMeta.addAction(actionMeta);

      hopGui.undoDelegate.addUndoNew(
          workflowMeta,
          new ActionMeta[] {actionMeta},
          new int[] {workflowMeta.indexOfAction(actionMeta)});

      // Shift the location for the next action
      //
      clipboardDelegate.shiftLocation(wce.location);
    }
  }
}
