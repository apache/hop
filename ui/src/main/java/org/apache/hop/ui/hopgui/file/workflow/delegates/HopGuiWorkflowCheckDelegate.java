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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ICheckResultSource;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

@GuiPlugin(description = "Workflow Graph Check Delegate")
public class HopGuiWorkflowCheckDelegate {
  private static final Class<?> PKG = HopGui.class;

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiWorkflowCheckDelegate-ToolBar";
  private static final String TOOLBAR_ITEM_COLLAPSE_ALL =
      "HopGuiWorkflowCheckDelegate-Toolbar-10010-CollapseAll";
  private static final String TOOLBAR_ITEM_EXPAND_ALL =
      "HopGuiWorkflowCheckDelegate-Toolbar-10020-ExpandAll";

  private final HopGui hopGui;
  private final HopGuiWorkflowGraph workflowGraph;
  @Getter private CTabItem workflowCheckTab;
  @Getter private GuiToolbarWidgets toolBarWidgets;
  private Tree wTree;

  /**
   * Check workflow and actions
   *
   * @param hopGui The Hop Gui instance
   * @param workflowGraph The workflow Graph
   */
  public HopGuiWorkflowCheckDelegate(HopGui hopGui, HopGuiWorkflowGraph workflowGraph) {
    this.hopGui = hopGui;
    this.workflowGraph = workflowGraph;
  }

  /**
   * When a toolbar is hit it knows the class so it will come here to ask for the instance.
   *
   * @return The active instance of this class
   */
  public static HopGuiWorkflowCheckDelegate getInstance() {
    IHopFileTypeHandler fileTypeHandler = HopGui.getInstance().getActiveFileTypeHandler();
    if (fileTypeHandler instanceof HopGuiWorkflowGraph hopGuiWorkflowGraph) {
      return hopGuiWorkflowGraph.workflowCheckDelegate;
    }
    return null;
  }

  public void addWorkflowCheck() {
    // First, see if we need to add the extra view...
    //
    if (workflowGraph.extraViewTabFolder == null || workflowGraph.extraViewTabFolder.isDisposed()) {
      workflowGraph.addExtraView();
    } else {
      if (workflowCheckTab != null && !workflowCheckTab.isDisposed()) {
        return;
      }
    }

    // Add a tab folder item to display the check result...
    //
    workflowCheckTab = new CTabItem(workflowGraph.extraViewTabFolder, SWT.NONE);
    workflowCheckTab.setFont(GuiResource.getInstance().getFontDefault());
    workflowCheckTab.setImage(GuiResource.getInstance().getImageCheck());
    workflowCheckTab.setText(BaseMessages.getString(PKG, "WorkflowGraph.Check.Tab.Name"));

    Composite checkComposite = new Composite(workflowGraph.extraViewTabFolder, SWT.NONE);
    checkComposite.setLayout(new FormLayout());

    // Add toolbar
    //
    ToolBar toolbar = new ToolBar(checkComposite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment(0, 0);
    fdToolBar.top = new FormAttachment(0, 0);
    fdToolBar.right = new FormAttachment(100, 0);
    toolbar.setLayoutData(fdToolBar);
    PropsUi.setLook(toolbar, Props.WIDGET_STYLE_TOOLBAR);

    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolbar, GUI_PLUGIN_TOOLBAR_PARENT_ID);
    toolbar.pack();

    FormData fd = new FormData();
    fd.top = new FormAttachment(0, 0);
    fd.left = new FormAttachment(0, 0); // First one in the left top corner
    fd.right = new FormAttachment(100, 0);
    toolbar.setLayoutData(fd);

    // Create the tree
    wTree = new Tree(checkComposite, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(wTree);

    FormData fdTree = new FormData();
    fdTree.top = new FormAttachment(toolbar, 0);
    fdTree.left = new FormAttachment(0, 0);
    fdTree.right = new FormAttachment(100, 0);
    fdTree.bottom = new FormAttachment(100, 0);
    wTree.setLayoutData(fdTree);
    wTree.addListener(SWT.DefaultSelection, this::edit);

    workflowCheckTab.setControl(checkComposite);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_COLLAPSE_ALL,
      toolTip = "i18n::System.Tooltip.CollapseALl",
      image = "ui/images/collapse-all.svg")
  public void collapseAll() {
    wTree.setRedraw(false);
    for (TreeItem item : wTree.getItems()) {
      item.setExpanded(false);
    }
    wTree.setRedraw(true);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_EXPAND_ALL,
      toolTip = "i18n::System.Tooltip.ExpandAll",
      image = "ui/images/expand-all.svg")
  public void expandAll() {
    wTree.setRedraw(false);
    for (TreeItem item : wTree.getItems()) {
      item.setExpanded(true);
    }
    wTree.setRedraw(true);
  }

  public void checkWorkflow() {
    try {
      final List<ICheckResult> remarks = new ArrayList<>();

      // Run the check in a progress dialog with a monitor...
      //
      ProgressMonitorDialog monitorDialog = new ProgressMonitorDialog(hopGui.getShell());
      monitorDialog.run(
          true,
          monitor -> {
            try {
              WorkflowMeta workflowMeta = workflowGraph.getWorkflowMeta();
              workflowMeta.checkActions(
                  remarks,
                  false,
                  monitor,
                  workflowGraph.getVariables(),
                  hopGui.getMetadataProvider());
            } catch (Throwable e) {
              throw new InvocationTargetException(
                  e,
                  BaseMessages.getString(
                      PKG, "WorkflowGraph.Check.ErrorCheckingWorkflow.Exception", e));
            }
          });

      // Update checks results
      //
      this.refresh(remarks);
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
          "WorkflowGraph.Check.ErrorCheckingWorkflow.Message",
          e);
    }
  }

  private void refresh(List<ICheckResult> remarks) {
    wTree.setRedraw(false);
    wTree.removeAll();

    Map<ICheckResultSource, TreeItem> mapSourceItems = new HashMap<>();
    for (ICheckResult cr : remarks) {
      // Ignore OK result
      if (cr.getType() == ICheckResult.TYPE_RESULT_OK) continue;

      ICheckResultSource source = cr.getSourceInfo();
      TreeItem item = mapSourceItems.get(source);
      if (source == null) {
        item = new TreeItem(wTree, SWT.NONE);
      } else if (item == null) {
        TreeItem parentItem = new TreeItem(wTree, SWT.NONE);
        parentItem.setText(source.getName());
        parentItem.setData(source);

        if (source instanceof IAction action) {
          Image image =
              GuiResource.getInstance()
                  .getSwtImageAction(action.getPluginId())
                  .getAsBitmapForSize(
                      hopGui.getDisplay(), ConstUi.MEDIUM_ICON_SIZE, ConstUi.MEDIUM_ICON_SIZE);
          if (image != null) {
            parentItem.setImage(image);
          }
          mapSourceItems.put(source, parentItem);
        }
        item = new TreeItem(parentItem, SWT.NONE);
        parentItem.setExpanded(true);
      } else {
        item = new TreeItem(item, SWT.NONE);
      }

      item.setText(cr.getText());
      item.setData(source);
      Image image = getImage(cr);
      if (image != null) {
        item.setImage(image);
      }
    }

    wTree.setRedraw(true);
  }

  private Image getImage(ICheckResult cr) {
    return switch (cr.getType()) {
      case ICheckResult.TYPE_RESULT_OK -> GuiResource.getInstance().getImageTrue();
      case ICheckResult.TYPE_RESULT_ERROR -> GuiResource.getInstance().getImageError();
      case ICheckResult.TYPE_RESULT_WARNING -> GuiResource.getInstance().getImageWarning();
      default -> null;
    };
  }

  /** Edit check result source */
  private void edit(Event event) {
    if (event.item instanceof TreeItem item) {
      if (item.getData() instanceof ICheckResultSource source) {
        ActionMeta actionMeta = workflowGraph.getWorkflowMeta().findAction(source.getName());
        workflowGraph.editAction(actionMeta);
      }
    }
  }
}
