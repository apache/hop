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

package org.apache.hop.neo4j.execution.path.workflow;

import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.neo4j.execution.NeoExecutionInfoLocation;
import org.apache.hop.neo4j.execution.path.base.NeoExecutionViewerErrorTab;
import org.apache.hop.ui.hopgui.perspective.execution.WorkflowExecutionViewer;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;

@GuiPlugin
public class NeoWorkflowExecutionViewerErrorTab extends NeoExecutionViewerErrorTab {
  private Tree wTree;
  private Color errorLineBackground;
  private Text wCypher;

  /** The constructor is called every time a new tab is created in the workflow execution viewer */
  public NeoWorkflowExecutionViewerErrorTab(WorkflowExecutionViewer viewer) {
    super(viewer);
  }

  @GuiTab(
      id = "90020-workflow-execution-viewer-neo4j-execution-error-tab",
      parentId = WorkflowExecutionViewer.WORKFLOW_EXECUTION_VIEWER_TABS,
      description = "Workflow to error path")
  public void addNeoExecutionPathTab(CTabFolder tabFolder) {
    super.addNeoErrorPathTab(tabFolder);
  }

  /**
   * This is called by the WorkflowExecutionViewer to see if the tab should be shown. We only want
   * the tab to be available for Neo4j locations.
   *
   * @param viewer
   * @return true if the error tab should be shown in the workflow viewer
   */
  public static boolean showTab(WorkflowExecutionViewer viewer) {
    ExecutionInfoLocation executionInfoLocation = getExecutionInfoLocation(viewer);
    if (!(executionInfoLocation.getExecutionInfoLocation() instanceof NeoExecutionInfoLocation)) {
      return false;
    }
    return viewer.getExecutionState() != null && viewer.getExecutionState().isFailed();
  }
}
