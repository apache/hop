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

package org.apache.hop.ui.hopgui.perspective.execution;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.hopgui.CanvasFacade;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.shared.SwtGc;
import org.apache.hop.ui.hopgui.shared.SwtScrollBar;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.WorkflowPainter;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.*;

@GuiPlugin
public class WorkflowExecutionViewer extends BaseExecutionViewer
    implements IExecutionViewer, PaintListener {
  private static final Class<?> PKG = WorkflowExecutionViewer.class; // For Translator

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "WorkflowExecutionViewer-Toolbar";

  public static final String TOOLBAR_ITEM_REFRESH = "WorkflowExecutionViewer-Toolbar-10100-Refresh";

  private final WorkflowMeta workflowMeta;
  private final String logChannelId;

  private ToolBar toolBar;
  private GuiToolbarWidgets toolBarWidgets;
  private SashForm sash;
  private Canvas canvas;
  protected float magnification = 1.0f;
  private CTabFolder tabFolder;
  private CTabItem infoTab;
  private CTabItem dataTab;

  public WorkflowExecutionViewer(
      Composite parent, HopGui hopGui, WorkflowMeta workflowMeta, String logChannelId) {
    super(parent, hopGui);
    this.workflowMeta = workflowMeta;
    this.logChannelId = logChannelId;

    addWidgets();
  }

  /** Add the widgets in the execution perspective parent tab folder */
  private void addWidgets() {
    // A toolbar at the top
    //
    toolBar = new ToolBar(this, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBar, GUI_PLUGIN_TOOLBAR_PARENT_ID);
    FormData layoutData = new FormData();
    layoutData.left = new FormAttachment(0, 0);
    layoutData.top = new FormAttachment(0, 0);
    layoutData.right = new FormAttachment(100, 0);
    toolBar.setLayoutData(layoutData);
    toolBar.pack();
    props.setLook(toolBar, Props.WIDGET_STYLE_TOOLBAR);

    // Below the toolbar we have a horizontal splitter: Canvas above and Execution tabs below
    //
    sash = new SashForm(this, SWT.VERTICAL);
    FormData fdSash = new FormData();
    fdSash.left = new FormAttachment(0, 0);
    fdSash.top = new FormAttachment(toolBar, 0);
    fdSash.right = new FormAttachment(100, 0);
    fdSash.bottom = new FormAttachment(100, 0);
    sash.setLayoutData(fdSash);

    // In this sash we have a canvas at the top and a tab folder with information at the bottom
    //
    // The canvas at the top
    canvas = new Canvas(sash, SWT.V_SCROLL | SWT.H_SCROLL | SWT.NO_BACKGROUND | SWT.BORDER);
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment(0, 0);
    fdCanvas.top = new FormAttachment(0, 0);
    fdCanvas.right = new FormAttachment(100, 0);
    fdCanvas.bottom = new FormAttachment(100, 0);
    canvas.setLayoutData(fdCanvas);
    canvas.addPaintListener(this);

    // The execution information tabs at the bottom
    //
    tabFolder = new CTabFolder(sash, SWT.MULTI);
    hopGui.getProps().setLook(tabFolder, Props.WIDGET_STYLE_TAB);

    infoTab = new CTabItem(tabFolder, SWT.NONE);
    infoTab.setImage(GuiResource.getInstance().getImageInfo());
    infoTab.setText(BaseMessages.getString(PKG, "WorkflowExecutionViewer.InfoTab.Title"));

    dataTab = new CTabItem(tabFolder, SWT.NONE);
    dataTab.setImage(GuiResource.getInstance().getImageData());
    dataTab.setText(BaseMessages.getString(PKG, "WorkflowExecutionViewer.DataTab.Title"));
  }

  @Override
  public Image getTitleImage() {
    return GuiResource.getInstance().getImageWorkflow();
  }

  @Override
  public String getTitleToolTip() {
    return workflowMeta.getDescription();
  }

  @Override
  public void paintControl(PaintEvent e) {
    Point area = getArea();
    if (area.x == 0 || area.y == 0) {
      return; // nothing to do!
    }

    // Do double buffering to prevent flickering on Windows
    //
    boolean needsDoubleBuffering =
        Const.isWindows() && "GUI".equalsIgnoreCase(Const.getHopPlatformRuntime());

    Image image = null;
    GC swtGc = e.gc;

    if (needsDoubleBuffering) {
      image = new Image(hopDisplay(), area.x, area.y);
      swtGc = new GC(image);
    }

    drawWorkflowImage(swtGc, area.x, area.y, magnification);

    if (needsDoubleBuffering) {
      // Draw the image onto the canvas and get rid of the resources
      //
      e.gc.drawImage(image, 0, 0);
      swtGc.dispose();
      image.dispose();
    }
  }

  protected Point getArea() {
    org.eclipse.swt.graphics.Rectangle rect = canvas.getClientArea();
    Point area = new Point(rect.width, rect.height);

    return area;
  }

  public void drawWorkflowImage(GC swtGc, int width, int height, float magnificationFactor) {

    IGc gc = new SwtGc(swtGc, width, height, iconSize);
    try {
      PropsUi propsUi = PropsUi.getInstance();

      int gridSize = propsUi.isShowCanvasGridEnabled() ? propsUi.getCanvasGridSize() : 1;

      ScrollBar horizontalScrollBar = canvas.getHorizontalBar();
      ScrollBar verticalScrollBar = canvas.getVerticalBar();

      WorkflowPainter workflowPainter =
          new WorkflowPainter(
              gc,
              hopGui.getVariables(),
                  workflowMeta,
              new Point(width, height),
              horizontalScrollBar == null ? null : new SwtScrollBar(horizontalScrollBar),
              verticalScrollBar == null ? null : new SwtScrollBar(verticalScrollBar),
              null,
              null,
              areaOwners,
              propsUi.getIconSize(),
              propsUi.getLineWidth(),
              gridSize,
              propsUi.getNoteFont().getName(),
              propsUi.getNoteFont().getHeight(),
              propsUi.getZoomFactor(),
              false);

      // correct the magnification with the overall zoom factor
      //
      float correctedMagnification = (float) (magnificationFactor * propsUi.getZoomFactor());

      workflowPainter.setMagnification(correctedMagnification);

      try {
        workflowPainter.drawWorkflow();
      } catch (Exception e) {
        new ErrorDialog(hopGui.getShell(), "Error", "Error drawing workflow image", e);
      }
    } finally {
      gc.dispose();
    }
    CanvasFacade.setData(canvas, magnification, workflowMeta, WorkflowExecutionViewer.class);
  }

  @Override
  public Control getControl() {
    return this;
  }

  /**
   * Refresh the information in the execution panes
   */
  @GuiToolbarElement(
          root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
          id = TOOLBAR_ITEM_REFRESH,
          toolTip = "i18n::WorkflowExecutionViewer.ToolbarElement.Refresh.Tooltip",
          image = "ui/images/refresh.svg")
  @GuiKeyboardShortcut(key = SWT.F5)
  @GuiOsxKeyboardShortcut(key = SWT.F5)
  public void refresh() {

  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return workflowMeta.getName();
  }

  /**
   * Gets logChannelId
   *
   * @return value of logChannelId
   */
  @Override
  public String getLogChannelId() {
    return logChannelId;
  }
}
