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

package org.apache.hop.ui.hopgui.file.workflow.delegates;

import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.workflow.WorkflowMeta;

/**
 * Workflow undo/redo is implemented with gzip XML snapshots on {@link HopGuiWorkflowGraph}. This
 * class remains as a hook for any leftover callers.
 */
public class HopGuiWorkflowUndoDelegate {

  private HopGuiWorkflowGraph workflowGraph;
  private HopGui hopGui;

  public HopGuiWorkflowUndoDelegate(HopGui hopGui, HopGuiWorkflowGraph workflowGraph) {
    this.hopGui = hopGui;
    this.workflowGraph = workflowGraph;
  }

  public void undoWorkflowAction(IHopFileTypeHandler handler, WorkflowMeta workflowMeta) {
    workflowGraph.undo();
  }

  public void redoWorkflowAction(IHopFileTypeHandler handler, WorkflowMeta workflowMeta) {
    workflowGraph.redo();
  }

  public HopGuiWorkflowGraph getWorkflowGraph() {
    return workflowGraph;
  }

  public void setWorkflowGraph(HopGuiWorkflowGraph workflowGraph) {
    this.workflowGraph = workflowGraph;
  }

  public HopGui getHopGui() {
    return hopGui;
  }

  public void setHopGui(HopGui hopGui) {
    this.hopGui = hopGui;
  }
}
