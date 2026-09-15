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

package org.apache.hop.ui.hopgui.file.pipeline.delegates;

import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;

/**
 * Pipeline undo/redo is implemented with gzip XML snapshots on {@link HopGuiPipelineGraph}. This
 * class remains as a hook for any leftover callers.
 */
public class HopGuiPipelineUndoDelegate {

  private HopGuiPipelineGraph pipelineGraph;
  private HopGui hopGui;

  public HopGuiPipelineUndoDelegate(HopGui hopGui, HopGuiPipelineGraph pipelineGraph) {
    this.hopGui = hopGui;
    this.pipelineGraph = pipelineGraph;
  }

  public void undoPipelineAction(IHopFileTypeHandler handler, PipelineMeta pipelineMeta) {
    pipelineGraph.undo();
  }

  public void redoPipelineAction(IHopFileTypeHandler handler, PipelineMeta pipelineMeta) {
    pipelineGraph.redo();
  }

  public HopGuiPipelineGraph getPipelineGraph() {
    return pipelineGraph;
  }

  public void setPipelineGraph(HopGuiPipelineGraph pipelineGraph) {
    this.pipelineGraph = pipelineGraph;
  }

  public HopGui getHopGui() {
    return hopGui;
  }

  public void setHopGui(HopGui hopGui) {
    this.hopGui = hopGui;
  }
}
