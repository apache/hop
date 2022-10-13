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
 *
 */

package org.apache.hop.beam.gui;

import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;

@ExtensionPoint(
    id = "PipelineGraphUpdateGuiXP",
    extensionPointId = "HopGuiPipelineGraphUpdateGui",
    description = "Update the toolbar icons we add in the Beam GUI plugin")
public final class PipelineGraphUpdateGuiXP implements IExtensionPoint<HopGuiPipelineGraph> {
  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, HopGuiPipelineGraph graph)
      throws HopException {
    DataflowPipelineJob dataflowPipelineJob = HopBeamGuiPlugin.findDataflowPipelineJob();

    // Enable/Disable the toolbar icon.
    //
    graph
        .getToolBarWidgets()
        .enableToolbarItem(
            HopBeamGuiPlugin.TOOLBAR_ID_VISIT_GCP_DATAFLOW, dataflowPipelineJob != null);
  }
}
