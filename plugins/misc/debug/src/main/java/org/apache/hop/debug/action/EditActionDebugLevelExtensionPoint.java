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

package org.apache.hop.debug.action;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.debug.util.DebugLevelUtil;
import org.apache.hop.debug.util.Defaults;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.extension.HopGuiWorkflowGraphExtension;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

@ExtensionPoint(
  id = "EditActionDebugLevelExtensionPoint",
  extensionPointId = "WorkflowGraphMouseUp",
  description = "Edit the custom action debug level with a single click"
)
public class EditActionDebugLevelExtensionPoint implements IExtensionPoint<HopGuiWorkflowGraphExtension> {
  @Override public void callExtensionPoint( ILogChannel log, IVariables variables, HopGuiWorkflowGraphExtension ext ) throws HopException {
    try {
      if ( ext.getAreaOwner() == null || ext.getAreaOwner().getOwner() == null ) {
        return;
      }
      if ( !( ext.getAreaOwner().getOwner() instanceof ActionDebugLevel ) ) {
        return;
      }
      ActionDebugLevel debugLevel = (ActionDebugLevel) ext.getAreaOwner().getOwner();
      ActionDebugLevelDialog dialog = new ActionDebugLevelDialog( HopGui.getInstance().getShell(), debugLevel );
      if ( dialog.open() ) {
        WorkflowMeta workflowMeta = ext.getWorkflowGraph().getWorkflowMeta();
        ActionMeta actionCopy = (ActionMeta) ext.getAreaOwner().getParent();

        DebugLevelUtil.storeActionDebugLevel(
          workflowMeta.getAttributesMap().get( Defaults.DEBUG_GROUP ),
          actionCopy.getName(),
          debugLevel
        );
        ext.getWorkflowGraph().redraw();
      }
    } catch(Exception e) {
      new ErrorDialog( HopGui.getInstance().getShell(), "Error", "Error editing action debug level", e );
    }
  }
}
