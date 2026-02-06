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

package org.apache.hop.ui.hopgui.file.shared;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/**
 * Registers workflow executions for drill-down support when the GUI is active. Skips registration
 * when running headless (e.g. hop run, API) to avoid holding references and reduce memory use.
 */
@ExtensionPoint(
    id = "DrillDownWorkflowRegistrationExtensionPoint",
    extensionPointId = "WorkflowStart",
    description = "Registers workflow executions for drill-down support when GUI is active")
public class DrillDownWorkflowRegistrationExtensionPoint
    implements IExtensionPoint<IWorkflowEngine<WorkflowMeta>> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, IWorkflowEngine<WorkflowMeta> workflow)
      throws HopException {
    if (workflow == null || !"GUI".equalsIgnoreCase(Const.getHopPlatformRuntime())) {
      return;
    }
    DrillDownGuiPlugin.registerRunningWorkflow(workflow.getLogChannelId(), workflow);
  }
}
