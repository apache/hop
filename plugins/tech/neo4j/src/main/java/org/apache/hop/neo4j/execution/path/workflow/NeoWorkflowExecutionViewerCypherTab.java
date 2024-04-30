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
import org.apache.hop.neo4j.execution.path.base.NeoExecutionViewerCypherTab;
import org.apache.hop.ui.hopgui.perspective.execution.WorkflowExecutionViewer;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.widgets.Text;

@GuiPlugin
public class NeoWorkflowExecutionViewerCypherTab extends NeoExecutionViewerCypherTab {
  private Text wCypher;

  /** The constructor is called every time a new tab is created in the workflow execution viewer */
  public NeoWorkflowExecutionViewerCypherTab(WorkflowExecutionViewer viewer) {
    super(viewer);
  }

  @GuiTab(
      id = "90010-workflow-execution-viewer-neo4j-cypher-tab",
      parentId = WorkflowExecutionViewer.WORKFLOW_EXECUTION_VIEWER_TABS,
      description = "Neo4j Cypher")
  @Override
  public void addNeo4jCypherTab(CTabFolder tabFolder) {
    super.addNeo4jCypherTab(tabFolder);
  }

  /**
   * This is called by the WorkflowExecutionViewer to see if the tab should be shown. We only want
   * the tab to be available for Neo4j locations.
   *
   * @param viewer
   * @return
   */
  public static boolean showTab(WorkflowExecutionViewer viewer) {
    ExecutionInfoLocation executionInfoLocation = getExecutionInfoLocation(viewer);
    return executionInfoLocation.getExecutionInfoLocation() instanceof NeoExecutionInfoLocation;
  }
}
