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

package org.apache.hop.ui.hopgui.file.pipeline.debug;

import java.util.Objects;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.debug.DebugController;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Composite;

/** Debugger tab on the pipeline graph execution results. */
@GuiPlugin(description = "Pipeline debugger")
public class HopGuiPipelineDebugDelegate {

  public static final String TAB_ID = "05000-pipeline-debug-tab";

  private static final Class<?> PKG = PipelineDebugPanel.class;

  private final HopGuiPipelineGraph pipelineGraph;
  private PipelineDebugPanel panel;

  public HopGuiPipelineDebugDelegate(HopGui hopGui, HopGuiPipelineGraph pipelineGraph) {
    Objects.requireNonNull(hopGui, "HopGui");
    this.pipelineGraph = pipelineGraph;
    pipelineGraph.setDebugDelegate(this);
  }

  @GuiTab(id = TAB_ID, parentId = HopGuiPipelineGraph.PIPELINE_GRAPH_TABS, description = "Debugger")
  public CTabItem addDebugTab(CTabFolder tabFolder) {
    CTabItem tab = new CTabItem(tabFolder, SWT.NONE);
    tab.setImage(GuiResource.getInstance().getImage("ui/images/debug.svg"));
    tab.setText(BaseMessages.getString(PKG, "PipelineDebugPanel.Tab.Text"));
    Composite composite = new Composite(tabFolder, SWT.NONE);
    panel =
        new PipelineDebugPanel(
            composite,
            pipelineGraph.getVariables(),
            this::controller,
            this::markCurrentTransform,
            pipelineGraph::exitDebugMode,
            pipelineGraph::resetDebugSession);
    tab.setControl(composite);
    panel.refresh();
    return tab;
  }

  public void refresh() {
    if (panel != null) {
      panel.refresh();
    }
  }

  public void selectHop(String from, String to) {
    if (panel != null) {
      panel.selectHop(from, to);
    }
  }

  public PipelineDebugPanel getPanel() {
    return panel;
  }

  public void processOne() {
    if (panel != null) {
      panel.processOne();
    }
  }

  public void continueRunning() {
    if (panel != null) {
      panel.continueRunning();
    }
  }

  public void pauseRunning() {
    if (panel != null) {
      panel.pauseRunning();
    }
  }

  private DebugController controller() {
    if (pipelineGraph.getPipeline() instanceof Pipeline pipeline) {
      return pipeline.getDebugController();
    }
    return null;
  }

  private void markCurrentTransform(String name) {
    if (name == null) {
      pipelineGraph.getStateMap().remove(DebugController.STATE_CURRENT_TRANSFORM);
    } else {
      pipelineGraph.getStateMap().put(DebugController.STATE_CURRENT_TRANSFORM, name);
    }
    pipelineGraph.redraw();
  }
}
