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

package org.apache.hop.ui.hopgui.perspective.dataorch;

import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolTip;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The beginnings of a common graph object, used by JobGraph and HopGuiPipelineGraph to share common
 * behaviors.
 */
public abstract class HopGuiAbstractGraph extends Composite {

  public static final String STATE_MAGNIFICATION = "magnification";
  public static final String STATE_SCROLL_X_THUMB = "scroll-x-thumb";
  public static final String STATE_SCROLL_X_SELECTION = "scroll-x-selection";
  public static final String STATE_SCROLL_X_MIN = "scroll-x-min";
  public static final String STATE_SCROLL_X_MAX = "scroll-x-max";
  public static final String STATE_SCROLL_Y_THUMB = "scroll-y-thumb";
  public static final String STATE_SCROLL_Y_SELECTION = "scroll-y-selection";
  public static final String STATE_SCROLL_Y_MIN = "scroll-y-min";
  public static final String STATE_SCROLL_Y_MAX = "scroll-y-max";

  protected HopGui hopGui;

  protected IVariables variables;

  protected Composite parentComposite;

  protected CTabItem parentTabItem;

  protected DPoint offset;
  protected Point iconOffset;
  protected Point noteOffset;

  protected Canvas canvas;

  protected float magnification = 1.0f;
  protected Rectangle viewPort;
  protected Rectangle graphPort;

  private boolean changedState;
  private final Font defaultFont;

  protected final String id;

  protected ToolTip toolTip;

  /**
   * This is a state map which can be used by plugins to render extra states on top of pipelines and
   * workflows or their components.
   */
  protected Map<String, Object> stateMap;

  protected boolean viewDrag;
  protected Point viewDragStart;
  protected DPoint viewDragBaseOffset;
  protected Point maximum;

  protected boolean viewPortNavigation;
  protected Point viewPortStart;

  public HopGuiAbstractGraph(HopGui hopGui, Composite parent, int style, CTabItem parentTabItem) {
    super(parent, style);
    this.parentComposite = parent;
    this.hopGui = hopGui;
    this.variables = new Variables();
    this.variables.copyFrom(hopGui.getVariables());
    this.parentTabItem = parentTabItem;
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

  protected float calculateCorrectedMagnification() {
    return (float) (magnification * PropsUi.getInstance().getZoomFactor());
  }

  protected Point magnifyPoint(Point p) {
    float cm = calculateCorrectedMagnification();
    return new Point(Math.round(p.x * cm), Math.round(p.y * cm));
  }

  protected Point getThumb(Point area, Point pipelineMax) {
    Point resizedMax = magnifyPoint(pipelineMax);

    Point thumb = new Point(0, 0);
    if (resizedMax.x <= area.x) {
      thumb.x = 100;
    } else {
      thumb.x = 100 * area.x / resizedMax.x;
    }

    if (resizedMax.y <= area.y) {
      thumb.y = 100;
    } else {
      thumb.y = 100 * area.y / resizedMax.y;
    }

    return thumb;
  }

  public int sign(int n) {
    return n < 0 ? -1 : (n > 0 ? 1 : 1);
  }

  protected Point getArea() {
    org.eclipse.swt.graphics.Rectangle rect = canvas.getClientArea();
    Point area = new Point(rect.width, rect.height);

    return area;
  }

  public abstract boolean hasChanged();

  @Override
  public void redraw() {
    if (isDisposed() || canvas == null || canvas.isDisposed() || parentTabItem.isDisposed()) {
      return;
    }

    if (hasChanged() != changedState) {
      changedState = hasChanged();
      if (hasChanged()) {
        parentTabItem.setFont(GuiResource.getInstance().getFontBold());
      } else {
        parentTabItem.setFont(defaultFont);
      }
    }
    canvas.redraw();
  }

  public abstract void setZoomLabel();

  @GuiKeyboardShortcut(control = true, key = '=')
  public void zoomInShortcut() {
    zoomIn();
  }

  @GuiKeyboardShortcut(control = true, key = '+')
  public void zoomIn() {
    magnification += 0.1f;
    // Minimum 1000%
    if (magnification > 10f) {
      magnification = 10f;
    }
    setZoomLabel();
    redraw();
  }

  @GuiKeyboardShortcut(control = true, key = '-')
  public void zoomOut() {
    magnification -= 0.1f;
    // Minimum 10%
    if (magnification < 0.1f) {
      magnification = 0.1f;
    }
    setZoomLabel();
    redraw();
  }

  @GuiKeyboardShortcut(control = true, key = '0')
  public void zoom100Percent() {
    magnification = 1.0f;
    setZoomLabel();
    redraw();
  }

  public Point screen2real(int x, int y) {
    float correctedMagnification = calculateCorrectedMagnification();
    DPoint real =
        new DPoint(
            ((double) x - offset.x) / correctedMagnification - offset.x,
            ((double) y - offset.y) / correctedMagnification - offset.y);
    return real.toPoint();
  }

  @Override
  public boolean forceFocus() {
    return canvas.forceFocus();
  }

  @Override
  public void dispose() {
    parentTabItem.dispose();
  }

  /**
   * Gets parentTabItem
   *
   * @return value of parentTabItem
   */
  public CTabItem getParentTabItem() {
    return parentTabItem;
  }

  /**
   * @param parentTabItem The parentTabItem to set
   */
  public void setParentTabItem(CTabItem parentTabItem) {
    this.parentTabItem = parentTabItem;
  }

  /**
   * Gets parentComposite
   *
   * @return value of parentComposite
   */
  public Composite getParentComposite() {
    return parentComposite;
  }

  /**
   * @param parentComposite The parentComposite to set
   */
  public void setParentComposite(Composite parentComposite) {
    this.parentComposite = parentComposite;
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

  /**
   * There are 2 ways to drag the view-port around. One way is to use the navigation rectangle at
   * the bottom. The other way is to click-drag the background.
   *
   * @param button
   * @param control
   * @param screenClick
   * @return
   */
  protected boolean setupDragView(int button, boolean control, Point screenClick) {
    // See if this is a click on the navigation view inner rectangle with the goal of dragging it
    // around a bit.
    //
    if (viewPort != null && viewPort.contains(screenClick)) {

      viewPortNavigation = true;
      viewPortStart = new Point(screenClick);
      viewDragBaseOffset = new DPoint(offset);
      return true;
    }

    // Middle button
    // CTRL + left button
    //
    viewDrag = button == 2 || (control && button == 1);
    if (viewDrag) {
      viewDragStart = screenClick;
      viewDragBaseOffset = new DPoint(offset);
      return true;
    }
    return false;
  }

  /**
   * Calculate the differences for the scrollbars. We take the system zoom factor and current
   * magnification into account
   */
  protected void dragView(Point lastClick, Point moved) {
    // The offset is in absolute numbers relative to the pipeline/workflow graph metadata.
    // The screen coordinates need to be corrected.  If the image is zoomed in we need to move less.
    //
    double zoomFactor = PropsUi.getNativeZoomFactor() * Math.max(0.1, magnification);
    double deltaX = (lastClick.x - moved.x) / zoomFactor;
    double deltaY = (lastClick.y - moved.y) / zoomFactor;

    offset.x = viewDragBaseOffset.x - deltaX;
    offset.y = viewDragBaseOffset.y - deltaY;

    validateOffset();

    redraw();
  }

  public void validateOffset() {
    double zoomFactor = PropsUi.getNativeZoomFactor() * Math.max(0.1, magnification);

    // What's the size of the graph when painted on screen?
    //
    double graphWidth = maximum.x;
    double graphHeight = maximum.y;

    // We need to know the size of the screen.
    //
    Point area = getArea();
    double viewWidth = area.x / zoomFactor;
    double viewHeight = area.y / zoomFactor;

    // As a percentage of the view area, how much can we go outside the graph area?
    //
    double overshootPct = 0.75;
    double overshootWidth = viewWidth*overshootPct;
    double overshootHeight = viewHeight*overshootPct;

    // Let's not move the graph off the screen to the top/left
    //
    double minX = -graphWidth-overshootWidth;
    if (offset.x < minX) {
      offset.x = minX;
    }
    double minY = -graphHeight-overshootHeight;
    if (offset.y < minY) {
      offset.y = minY;
    }

    // Are we moving the graph too far down/right?
    //
    double maxX = overshootWidth;
    if (offset.x > maxX) {
      offset.x = maxX;
    }
    double maxY = overshootHeight;
    if (offset.y > maxY) {
      offset.y = maxY;
    }
  }

  protected void dragViewPort(Point clickLocation) {
    // The delta is calculated
    //
    double deltaX = clickLocation.x - viewPortStart.x;
    double deltaY = clickLocation.y - viewPortStart.y;

    // What's the wiggle room for the little rectangle in the bigger one?
    //
    int wiggleX = viewPort.width;
    int wiggleY = viewPort.height;

    // What's that in percentages?  We draw the little rectangle at 25% size.
    //
    double deltaXPct = wiggleX == 0 ? 0 : deltaX / (wiggleX / 0.25);
    double deltaYPct = wiggleY == 0 ? 0 : deltaY / (wiggleY / 0.25);

    // The offset is then a matter of setting a percentage of the graph size
    //
    double deltaOffSetX = deltaXPct * maximum.x;
    double deltaOffSetY = deltaYPct * maximum.y;

    offset = new DPoint(viewDragBaseOffset.x - deltaOffSetX, viewDragBaseOffset.y - deltaOffSetY);

    // Make sure we don't catapult the view somewhere we can't find the graph anymore.
    //
    validateOffset();
    redraw();
  }

  @GuiKeyboardShortcut(key = SWT.HOME)
  @GuiOsxKeyboardShortcut(key = SWT.HOME)
  public void viewReset() {
    offset = new DPoint(0.0, 0.0);
    redraw();
  }

  @GuiKeyboardShortcut(key = SWT.ARROW_LEFT)
  @GuiOsxKeyboardShortcut(key = SWT.ARROW_LEFT)
  public void viewLeft() {
    offset.x += 15 * magnification * PropsUi.getNativeZoomFactor();
    validateOffset();
    redraw();
  }

  @GuiKeyboardShortcut(key = SWT.ARROW_RIGHT)
  @GuiOsxKeyboardShortcut(key = SWT.ARROW_RIGHT)
  public void viewRight() {
    offset.x -= 15 * magnification * PropsUi.getNativeZoomFactor();
    validateOffset();
    redraw();
  }

  @GuiKeyboardShortcut(key = SWT.ARROW_UP)
  @GuiOsxKeyboardShortcut(key = SWT.ARROW_UP)
  public void viewUp() {
    offset.y += 15 * magnification * PropsUi.getNativeZoomFactor();
    validateOffset();
    redraw();
  }

  @GuiKeyboardShortcut(key = SWT.ARROW_DOWN)
  @GuiOsxKeyboardShortcut(key = SWT.ARROW_DOWN)
  public void viewDown() {
    offset.y -= 15 * magnification * PropsUi.getNativeZoomFactor();
    validateOffset();
    redraw();
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
}
