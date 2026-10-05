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

package org.apache.hop.git;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.tab.GuiTab;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;

@GuiPlugin(name = "Pipeline revisions", description = "Revisions of the pipeline")
public class PipelineRevisionDelegate extends BaseRevisionDelegate {

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "PipelineRevisions-Toolbar";
  public static final String TOOLBAR_ITEM_REFRESH = "PipelineRevisions-Toolbar-10100-Refresh";
  public static final String TOOLBAR_ITEM_SHOW_TEXT_DIFF =
      "PipelineRevisions-Toolbar-10200-ShowTextDiff";
  public static final String TOOLBAR_ITEM_SHOW_VISUAL_DIFF =
      "PipelineRevisions-Toolbar-10210-ShowVisualDiff";

  private final HopGuiPipelineGraph pipelineGraph;

  public PipelineRevisionDelegate(HopGui hopGui, HopGuiPipelineGraph pipelineGraph) {
    super(hopGui);
    this.pipelineGraph = pipelineGraph;
  }

  @Override
  protected String getToolbarParentId() {
    return GUI_PLUGIN_TOOLBAR_PARENT_ID;
  }

  @Override
  protected String getFilename() {
    return pipelineGraph.getPipelineMeta().getFilename();
  }

  @Override
  protected boolean hasChanges() {
    return pipelineGraph.hasChanged();
  }

  @Override
  protected List<String> getSelectionToolbarItemIds() {
    return List.of(TOOLBAR_ITEM_SHOW_TEXT_DIFF, TOOLBAR_ITEM_SHOW_VISUAL_DIFF);
  }

  @GuiTab(
      id = "90000-pipeline-revisions-tab",
      parentId = HopGuiPipelineGraph.PIPELINE_GRAPH_TABS,
      description = "Pipeline revisions")
  public CTabItem createRevisionsTab(CTabFolder tabFolder) {
    return super.createRevisionsTab(tabFolder);
  }

  /** Refreshes the pipeline revisions with the git history of the pipeline file. */
  @Override
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REFRESH,
      toolTip = "i18n::System.Button.Refresh",
      image = "ui/images/refresh.svg")
  @GuiKeyboardShortcut(key = SWT.F5)
  @GuiOsxKeyboardShortcut(key = SWT.F5)
  public void refresh() {
    super.refresh();
  }

  /** Compares the pipeline currently being edited with the selected revision as text. */
  @Override
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_SHOW_TEXT_DIFF,
      toolTip = "i18n::Revisions.Toolbar.ShowTextDiff.Tooltip",
      image = "diff-text.svg",
      separator = true)
  public void showTextDiff() {
    super.showTextDiff();
  }

  /** Compares the pipeline currently being edited with the selected revision graphically. */
  @Override
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_SHOW_VISUAL_DIFF,
      toolTip = "i18n::Revisions.Toolbar.ShowVisualDiff.Tooltip",
      image = "diff-graph.svg")
  public void showVisualDiff() {
    super.showVisualDiff();
  }

  /** {@inheritDoc} */
  @Override
  protected void showGraphDiff(String relativePath, String commitIdNew, String commitIdOld)
      throws HopException {
    GitGuiPlugin.getInstance().showPipelineFileDiff(relativePath, commitIdNew, commitIdOld);
  }
}
