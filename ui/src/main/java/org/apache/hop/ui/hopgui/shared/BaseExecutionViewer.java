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

package org.apache.hop.ui.hopgui.shared;

import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.perspective.execution.DragViewZoomBase;
import org.apache.hop.ui.hopgui.perspective.execution.ExecutionPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseListener;
import org.eclipse.swt.events.MouseMoveListener;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.ToolBar;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseExecutionViewer extends DragViewZoomBase
    implements MouseListener, MouseMoveListener {

  public static final String STRING_STATE_STALE = "Stale";

  protected final HopGui hopGui;
  protected final ExecutionPerspective perspective;
  protected final PropsUi props;
  protected final int iconSize;
  protected final List<AreaOwner> areaOwners;

  protected final String locationName;
  protected final Execution execution;
  protected ExecutionState executionState;

  protected ToolBar toolBar;
  protected GuiToolbarWidgets toolBarWidgets;
  protected SashForm sash;
  protected CTabFolder tabFolder;

  protected Point lastClick;

  public BaseExecutionViewer(
      Composite parent,
      HopGui hopGui,
      ExecutionPerspective perspective,
      String locationName,
      Execution execution,
      ExecutionState executionState) {
    super(parent, SWT.NO_BACKGROUND);
    this.perspective = perspective;
    this.locationName = locationName;
    this.execution = execution;
    this.executionState = executionState;
    this.hopGui = hopGui;
    this.props = PropsUi.getInstance();
    this.iconSize = hopGui.getProps().getIconSize();
    this.areaOwners = new ArrayList<>();
    this.offset = new DPoint(0, 0);
  }

  protected Display hopDisplay() {
    return hopGui.getDisplay();
  }

  @Override
  public boolean setFocus() {
    if (canvas.isDisposed()) {
      return false;
    }
    return canvas.setFocus();
  }

  @Override
  public void redraw() {
    canvas.redraw();
    canvas.setFocus();
  }

  protected float calculateCorrectedMagnification() {
    return (float) (magnification * PropsUi.getInstance().getZoomFactor());
  }

  public synchronized AreaOwner getVisibleAreaOwner(int x, int y) {
    for (int i = areaOwners.size() - 1; i >= 0; i--) {
      AreaOwner areaOwner = areaOwners.get(i);
      if (areaOwner.contains(x, y)) {
        return areaOwner;
      }
    }
    return null;
  }

  protected String formatDate(Date date) {
    if (date == null) {
      return "";
    }
    return new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(date);
  }

  public abstract void drillDownOnLocation(Point location);

  @Override
  public void mouseDoubleClick(MouseEvent mouseEvent) {
    drillDownOnLocation(screen2real(mouseEvent.x, mouseEvent.y));
  }

  @Override
  public void mouseMove(MouseEvent e) {
    // Check to see if we're navigating with the view port
    //
    if (viewPortNavigation) {
      dragViewPort(new Point(e.x, e.y));
    }

    // Drag the view around with middle button on the background?
    //
    if (viewDrag && lastClick != null) {
      dragView(viewDragStart, new Point(e.x, e.y));
    }
  }

  @Override
  public void mouseUp(MouseEvent e) {
    if (viewPortNavigation || viewDrag) {
      viewDrag = false;
      viewPortNavigation = false;
      viewPortStart = null;
    }
  }

  public void mouseHover(MouseEvent e) {
    // don't do anything for now
  }

  /**
   * Gets toolBarWidgets
   *
   * @return value of toolBarWidgets
   */
  public GuiToolbarWidgets getToolBarWidgets() {
    return toolBarWidgets;
  }

  protected void viewMetadata(Execution execution) {
    try {
      // Also inflate the metadata
      //
      String metadataJson = execution.getMetadataJson();
      SerializableMetadataProvider metadataProvider =
          new SerializableMetadataProvider(metadataJson);

      // The variables set
      //
      IVariables variables = Variables.getADefaultVariableSpace();
      variables.setVariables(execution.getVariableValues());
      variables.setVariables(execution.getParameterValues());

      List<Class<IHopMetadata>> metadataClasses = metadataProvider.getMetadataClasses();
      Map<String, IHopMetadataSerializer<IHopMetadata>> serializerMap = new HashMap<>();
      for (Class<IHopMetadata> metadataClass : metadataClasses) {
        IHopMetadataSerializer<IHopMetadata> serializer =
            metadataProvider.getSerializer(metadataClass);
        String description = serializer.getDescription();
        serializerMap.put(description, serializer);
      }

      List<String> metadataTypes = new ArrayList<>(serializerMap.keySet());
      Collections.sort(metadataTypes);

      EnterSelectionDialog selectTypeDialog =
          new EnterSelectionDialog(
              hopGui.getShell(),
              metadataTypes.toArray(new String[0]),
              "View metadata",
              "Select the type of metadata to view");
      String description = selectTypeDialog.open();
      if (description == null) {
        return;
      }
      IHopMetadataSerializer<IHopMetadata> serializer = serializerMap.get(description);
      List<String> objectNames = serializer.listObjectNames();

      EnterSelectionDialog selectElementDialog =
          new EnterSelectionDialog(
              hopGui.getShell(),
              objectNames.toArray(new String[0]),
              "Select element",
              "Select the metadata element of type " + description);
      String name = selectElementDialog.open();
      if (name == null) {
        return;
      }

      MetadataManager<IHopMetadata> manager =
          new MetadataManager<>(variables, metadataProvider, serializer.getManagedClass(), hopGui.getShell());
      manager.editMetadata(name);
    } catch (Exception e) {
      new ErrorDialog(getShell(), "Error", "Error viewing the metadata", e);
    }
  }

  public abstract String getActiveId();

  /**
   * Gets perspective
   *
   * @return value of perspective
   */
  public ExecutionPerspective getPerspective() {
    return perspective;
  }

  /**
   * Gets areaOwners
   *
   * @return value of areaOwners
   */
  public List<AreaOwner> getAreaOwners() {
    return areaOwners;
  }

  /**
   * Gets locationName
   *
   * @return value of locationName
   */
  public String getLocationName() {
    return locationName;
  }

  /**
   * Gets execution
   *
   * @return value of execution
   */
  public Execution getExecution() {
    return execution;
  }

  /**
   * Gets toolBar
   *
   * @return value of toolBar
   */
  public ToolBar getToolBar() {
    return toolBar;
  }

  /**
   * Sets toolBar
   *
   * @param toolBar value of toolBar
   */
  public void setToolBar(ToolBar toolBar) {
    this.toolBar = toolBar;
  }

  /**
   * Sets toolBarWidgets
   *
   * @param toolBarWidgets value of toolBarWidgets
   */
  public void setToolBarWidgets(GuiToolbarWidgets toolBarWidgets) {
    this.toolBarWidgets = toolBarWidgets;
  }

  /**
   * Gets sash
   *
   * @return value of sash
   */
  public SashForm getSash() {
    return sash;
  }

  /**
   * Sets sash
   *
   * @param sash value of sash
   */
  public void setSash(SashForm sash) {
    this.sash = sash;
  }

  /**
   * Gets tabFolder
   *
   * @return value of tabFolder
   */
  public CTabFolder getTabFolder() {
    return tabFolder;
  }

  /**
   * Sets tabFolder
   *
   * @param tabFolder value of tabFolder
   */
  public void setTabFolder(CTabFolder tabFolder) {
    this.tabFolder = tabFolder;
  }

  /**
   * Gets lastClick
   *
   * @return value of lastClick
   */
  public Point getLastClick() {
    return lastClick;
  }

  /**
   * Sets lastClick
   *
   * @param lastClick value of lastClick
   */
  public void setLastClick(Point lastClick) {
    this.lastClick = lastClick;
  }

  /**
   * Gets executionState
   *
   * @return value of executionState
   */
  public ExecutionState getExecutionState() {
    return executionState;
  }

  /**
   * Sets executionState
   *
   * @param executionState value of executionState
   */
  public void setExecutionState(ExecutionState executionState) {
    this.executionState = executionState;
  }
}
