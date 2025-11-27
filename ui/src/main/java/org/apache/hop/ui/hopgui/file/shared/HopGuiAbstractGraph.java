/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.file.shared;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.gui.SnapAllignDistribute;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IGraphSnapAlignDistribute;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.perspective.execution.DragViewZoomBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolTip;

/**
 * The beginnings of a common graph object, used by {@code HopGuiWorkflowGraph} and {@code
 * HopGuiPipelineGraph} to share common behaviors.
 */
public abstract class HopGuiAbstractGraph extends DragViewZoomBase
    implements IGraphSnapAlignDistribute {

  public static final String STATE_MAGNIFICATION = "magnification";
  public static final String STATE_SCROLL_X_SELECTION = "offset-x";
  public static final String STATE_SCROLL_Y_SELECTION = "offset-y";

  protected HopGui hopGui;

  protected IVariables variables;

  protected Composite parentComposite;

  protected Point iconOffset;
  protected Point noteOffset;

  private boolean changedState;
  private final Font defaultFont;

  protected final String id;

  protected ToolTip toolTip;

  protected String mouseOverName;

  /**
   * This is a state map which can be used by plugins to render extra states on top of pipelines and
   * workflows or their components.
   */
  protected Map<String, Object> stateMap;

  public HopGuiAbstractGraph(HopGui hopGui, Composite parent, int style) {
    super(parent, style);
    this.parentComposite = parent;
    this.hopGui = hopGui;
    this.variables = new Variables();
    this.variables.copyFrom(hopGui.getVariables());
    this.defaultFont = GuiResource.getInstance().getFontDefault();
    this.changedState = false;
    this.id = UUID.randomUUID().toString();
    this.stateMap = new HashMap<>();
    this.offset = new DPoint(0.0, 0.0);
  }

  protected Shell hopShell() {
    return hopGui.getShell();
  }

  protected Display hopDisplay() {
    return hopGui.getDisplay();
  }

  public abstract boolean hasChanged();

  public abstract void setChanged();

  @Override
  public void redraw() {
    if (isDisposed() || canvas == null || canvas.isDisposed()) {
      return;
    }

    canvas.redraw();
  }

  @Override
  public boolean forceFocus() {
    return canvas.forceFocus();
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  public Map<String, Object> getStateProperties() {
    Map<String, Object> map = new HashMap<>();
    map.put(STATE_MAGNIFICATION, magnification);
    map.put(STATE_SCROLL_X_SELECTION, offset.x);
    map.put(STATE_SCROLL_Y_SELECTION, offset.y);
    return map;
  }

  public void applyStateProperties(Map<String, Object> stateProperties) {
    Double fMagnification = (Double) stateProperties.get(STATE_MAGNIFICATION);
    magnification = fMagnification == null ? 1.0f : fMagnification.floatValue();
    setZoomLabel();

    // Offsets used to be integers so don't automatically map to Double.
    //
    Object xOffset = stateProperties.get(STATE_SCROLL_X_SELECTION);
    if (xOffset != null) {
      offset.x = Double.parseDouble(xOffset.toString());
    }
    Object yOffset = stateProperties.get(STATE_SCROLL_Y_SELECTION);
    if (yOffset != null) {
      offset.y = Double.parseDouble(yOffset.toString());
    }
    redraw();
  }

  protected void showToolTip(org.eclipse.swt.graphics.Point location) {
    org.eclipse.swt.graphics.Point p = canvas.toDisplay(location);

    toolTip.setLocation(p.x + ConstUi.TOOLTIP_OFFSET, p.y + ConstUi.TOOLTIP_OFFSET);
    toolTip.setVisible(true);
  }

  public abstract SnapAllignDistribute createSnapAlignDistribute();

  @Override
  public void snapToGrid() {
    snapToGrid(ConstUi.GRID_SIZE);
  }

  private void snapToGrid(int size) {
    createSnapAlignDistribute().snapToGrid(size);
    setChanged();
  }

  public void alignLeft() {
    createSnapAlignDistribute().allignleft();
    setChanged();
  }

  public void alignRight() {
    createSnapAlignDistribute().allignright();
    setChanged();
  }

  public void alignTop() {
    createSnapAlignDistribute().alligntop();
    setChanged();
  }

  public void alignBottom() {
    createSnapAlignDistribute().allignbottom();
    setChanged();
  }

  @GuiKeyboardShortcut(alt = true, key = SWT.ARROW_RIGHT)
  @GuiOsxKeyboardShortcut(alt = true, key = SWT.ARROW_RIGHT)
  public void distributeHorizontal() {
    createSnapAlignDistribute().distributehorizontal();
    setChanged();
  }

  @GuiOsxKeyboardShortcut(alt = true, key = SWT.ARROW_UP)
  public void distributeVertical() {
    createSnapAlignDistribute().distributevertical();
    setChanged();
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /**
   * @param variables The variables to set
   */
  public void setVariables(IVariables variables) {
    this.variables = variables;
  }

  /**
   * Gets stateMap
   *
   * @return value of stateMap
   */
  public Map<String, Object> getStateMap() {
    return stateMap;
  }

  /**
   * @param stateMap The stateMap to set
   */
  public void setStateMap(Map<String, Object> stateMap) {
    this.stateMap = stateMap;
  }

  /**
   * Gets magnification
   *
   * @return value of magnification
   */
  public float getMagnification() {
    return magnification;
  }

  /**
   * Sets magnification
   *
   * @param magnification value of magnification
   */
  public void setMagnification(float magnification) {
    this.magnification = magnification;
  }

  /**
   * Gets viewPort
   *
   * @return value of viewPort
   */
  public Rectangle getViewPort() {
    return viewPort;
  }

  /**
   * Sets viewPort
   *
   * @param viewPort value of viewPort
   */
  public void setViewPort(Rectangle viewPort) {
    this.viewPort = viewPort;
  }

  /**
   * Gets graphPort
   *
   * @return value of graphPort
   */
  public Rectangle getGraphPort() {
    return graphPort;
  }

  /**
   * Sets graphPort
   *
   * @param graphPort value of graphPort
   */
  public void setGraphPort(Rectangle graphPort) {
    this.graphPort = graphPort;
  }

  protected void enableSnapAlignDistributeMenuItems(
      IHopFileType fileType, boolean selectedTransform) {
    GuiMenuWidgets menuWidgets = hopGui.getMainMenuWidgets();
    menuWidgets.enableMenuItem(
        fileType,
        HopGui.ID_MAIN_MENU_EDIT_SNAP_TO_GRID,
        IHopFileType.CAPABILITY_SNAP_TO_GRID,
        selectedTransform);
    menuWidgets.enableMenuItem(
        fileType,
        HopGui.ID_MAIN_MENU_EDIT_ALIGN_LEFT,
        IHopFileType.CAPABILITY_ALIGN_LEFT,
        selectedTransform);
    menuWidgets.enableMenuItem(
        fileType,
        HopGui.ID_MAIN_MENU_EDIT_ALIGN_RIGHT,
        IHopFileType.CAPABILITY_ALIGN_RIGHT,
        selectedTransform);
    menuWidgets.enableMenuItem(
        fileType,
        HopGui.ID_MAIN_MENU_EDIT_ALIGN_TOP,
        IHopFileType.CAPABILITY_ALIGN_TOP,
        selectedTransform);
    menuWidgets.enableMenuItem(
        fileType,
        HopGui.ID_MAIN_MENU_EDIT_ALIGN_BOTTOM,
        IHopFileType.CAPABILITY_ALIGN_BOTTOM,
        selectedTransform);
    menuWidgets.enableMenuItem(
        fileType,
        HopGui.ID_MAIN_MENU_EDIT_DISTRIBUTE_HORIZONTAL,
        IHopFileType.CAPABILITY_DISTRIBUTE_HORIZONTAL,
        selectedTransform);
    menuWidgets.enableMenuItem(
        fileType,
        HopGui.ID_MAIN_MENU_EDIT_DISTRIBUTE_VERTICAL,
        IHopFileType.CAPABILITY_DISTRIBUTE_VERTICAL,
        selectedTransform);
  }

  /**
   * Gets mouseOverName
   *
   * @return value of mouseOverName
   */
  public String getMouseOverName() {
    return mouseOverName;
  }

  /**
   * Sets mouseOverName
   *
   * @param mouseOverName value of mouseOverName
   */
  public void setMouseOverName(String mouseOverName) {
    this.mouseOverName = mouseOverName;
  }
}
