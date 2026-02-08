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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.ICheckResultSource;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

@GuiPlugin(description = "Pipeline Graph Check Delegate")
public class HopGuiPipelineCheckDelegate {
  private static final Class<?> PKG = HopGui.class;

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiPipelineCheckDelegate-ToolBar";
  private static final String TOOLBAR_ITEM_COLLAPSE_ALL =
      "HopGuiPipelineCheckDelegate-Toolbar-10010-CollapseAll";
  private static final String TOOLBAR_ITEM_EXPAND_ALL =
      "HopGuiPipelineCheckDelegate-Toolbar-10020-ExpandAll";

  private final HopGui hopGui;
  private final HopGuiPipelineGraph pipelineGraph;
  @Getter private CTabItem pipelineCheckTab;
  @Getter private GuiToolbarWidgets toolBarWidgets;
  private Tree wTree;

  /**
   * Check pipeline and transforms
   *
   * @param hopGui The Hop Gui instance
   * @param pipelineGraph The pipeline graph
   */
  public HopGuiPipelineCheckDelegate(HopGui hopGui, HopGuiPipelineGraph pipelineGraph) {
    this.hopGui = hopGui;
    this.pipelineGraph = pipelineGraph;
  }

  /**
   * When a toolbar is hit it knows the class so it will come here to ask for the instance.
   *
   * @return The active instance of this class
   */
  public static HopGuiPipelineCheckDelegate getInstance() {
    IHopFileTypeHandler fileTypeHandler = HopGui.getInstance().getActiveFileTypeHandler();
    if (fileTypeHandler instanceof HopGuiPipelineGraph graph) {
      return graph.pipelineCheckDelegate;
    }
    return null;
  }

  public void addPipelineCheck() {
    // First, see if we need to add the extra view...
    //
    if (pipelineGraph.extraViewTabFolder == null || pipelineGraph.extraViewTabFolder.isDisposed()) {
      pipelineGraph.addExtraView();
    } else {
      if (pipelineCheckTab != null && !pipelineCheckTab.isDisposed()) {
        return;
      }
    }

    // Add a tab folder item to display the check result...
    //
    pipelineCheckTab = new CTabItem(pipelineGraph.extraViewTabFolder, SWT.NONE);
    pipelineCheckTab.setFont(GuiResource.getInstance().getFontDefault());
    pipelineCheckTab.setImage(GuiResource.getInstance().getImageCheck());
    pipelineCheckTab.setText(BaseMessages.getString(PKG, "PipelineGraph.Check.Tab.Name"));

    Composite checkComposite = new Composite(pipelineGraph.extraViewTabFolder, SWT.NONE);
    checkComposite.setLayout(new FormLayout());
    pipelineCheckTab.setControl(checkComposite);

    // Add toolbar
    //
    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(checkComposite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    Control toolbar = toolBarContainer.getControl();
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment(0, 0);
    fdToolBar.top = new FormAttachment(0, 0);
    fdToolBar.right = new FormAttachment(100, 0);
    toolbar.setLayoutData(fdToolBar);
    PropsUi.setLook(toolbar, Props.WIDGET_STYLE_TOOLBAR);

    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBarContainer, GUI_PLUGIN_TOOLBAR_PARENT_ID);
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

  public void checkPipeline() {
    try {
      final List<ICheckResult> remarks = new ArrayList<>();

      // Activate folder tab
      //
      pipelineGraph.extraViewTabFolder.setSelection(pipelineCheckTab);

      PipelineMeta pipelineMeta = pipelineGraph.getPipelineMeta();

      // Run the check in a progress dialog with a monitor...
      //
      ProgressMonitorDialog monitorDialog = new ProgressMonitorDialog(hopGui.getShell());

      // Run a watchdog in the background to cancel active database queries if check is canceled
      Runnable run =
          () -> {
            IProgressMonitor monitor = monitorDialog.getProgressMonitor();
            while (monitorDialog.getShell() == null
                || (!monitorDialog.getShell().isDisposed() && !monitor.isCanceled())) {
              try {
                Thread.sleep(250);
              } catch (InterruptedException e) {
                // Ignore sleep interruption exception
              }
            }

            // Disconnect and see what happens!
            if (monitor.isCanceled()) {
              try {
                pipelineMeta.cancelQueries();
              } catch (Exception e) {
                // Ignore cancel errors
              }
            }
          };
      new Thread(run).start();

      monitorDialog.run(
          true,
          monitor -> {
            try {
              pipelineMeta.checkTransforms(
                  remarks,
                  false,
                  monitor,
                  pipelineGraph.getVariables(),
                  hopGui.getMetadataProvider());
            } catch (Throwable e) {
              throw new InvocationTargetException(
                  e,
                  BaseMessages.getString(
                      PKG, "PipelineGraph.Check.ErrorCheckingPipeline.Exception", e));
            }
          });

      // Update checks results
      //
      this.refresh(remarks);

    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
          BaseMessages.getString(PKG, "PipelineGraph.Check.ErrorCheckingPipeline.Message"),
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

        if (source instanceof TransformMeta transform) {
          Image image =
              GuiResource.getInstance()
                  .getSwtImageTransform(transform.getPluginId())
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
    if (event.item instanceof TreeItem item
        && item.getData() instanceof ICheckResultSource source) {
      TransformMeta transformMeta = pipelineGraph.getPipelineMeta().findTransform(source.getName());
      pipelineGraph.editTransform(pipelineGraph.getPipelineMeta(), transformMeta);
    }
  }
}
