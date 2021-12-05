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

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.*;

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

  protected Point offset, iconOffset, noteOffset;

  protected ScrolledComposite wsCanvas;
  protected Canvas canvas;

  protected float magnification = 1.0f;

  private boolean changedState;
  private Font defaultFont;

  protected final String id;

  protected ToolTip toolTip;

  /**
   * This is a state map which can be used by plugins to render extra states on top of pipelines and
   * workflows or their components.
   */
  protected Map<String, Object> stateMap;

  protected boolean avoidScrollAdjusting;

  protected boolean viewDrag;
  protected Point viewDragStart;
  protected int startHorizontalDragSelection;
  protected int startVerticalDragSelection;

  public HopGuiAbstractGraph(HopGui hopGui, Composite parent, int style, CTabItem parentTabItem) {
    super(parent, style);
    this.parentComposite = parent;
    this.hopGui = hopGui;
    this.variables = new Variables();
    this.variables.copyFrom(hopGui.getVariables());
    this.parentTabItem = parentTabItem;
    defaultFont = parentTabItem.getFont();
    changedState = false;
    this.id = UUID.randomUUID().toString();
    this.stateMap = new HashMap<>();
  }

  protected Shell hopShell() {
    return hopGui.getShell();
  }

  protected Display hopDisplay() {
    return hopGui.getDisplay();
  }

  protected abstract Point getOffset();

  protected Point getOffset(Point thumb, Point area) {
    Point p = new Point(0, 0);
    ScrollBar horizontalScrollBar = wsCanvas.getHorizontalBar();
    ScrollBar verticalScrollBar = wsCanvas.getVerticalBar();

    Point sel = new Point(horizontalScrollBar.getSelection(), verticalScrollBar.getSelection());

    if (thumb.x == 0 || thumb.y == 0) {
      return p;
    }
    float cm = calculateCorrectedMagnification();
    p.x = Math.round(-sel.x * area.x / thumb.x / cm);
    p.y = Math.round(-sel.y * area.y / thumb.y / cm);

    return p;
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

  @GuiKeyboardShortcut(control = true, key = '+')
  public void zoomInShortcut1() {
    zoomIn();
  }

  @GuiKeyboardShortcut(control = true, key = '=')
  public void zoomInShortcut2() {
    zoomIn();
  }

  public void zoomIn() {
    magnification += 0.1f;
    // Minimum 1000%
    if (magnification > 10f) {
      magnification = 10f;
    }
    adjustScrolling();
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
    adjustScrolling();
    setZoomLabel();
    redraw();
  }

  @GuiKeyboardShortcut(control = true, key = '0')
  public void zoom100Percent() {
    magnification = 1.0f;
    adjustScrolling();
    setZoomLabel();
    redraw();
  }

  public Point screen2real(int x, int y) {
    offset = getOffset();
    float correctedMagnification = calculateCorrectedMagnification();
    Point real;
    if (offset != null) {
      real =
          new Point(
              Math.round((x / correctedMagnification - offset.x)),
              Math.round((y / correctedMagnification - offset.y)));
    } else {
      real = new Point(x, y);
    }

    return real;
  }

  public Point real2screen(int x, int y) {
    offset = getOffset();
    Point screen = new Point(x + offset.x, y + offset.y);

    return screen;
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

  /** @param parentTabItem The parentTabItem to set */
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

  /** @param parentComposite The parentComposite to set */
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

    ScrollBar horizontalScrollBar = wsCanvas.getHorizontalBar();
    ScrollBar verticalScrollBar = wsCanvas.getVerticalBar();

    map.put(
        STATE_SCROLL_X_SELECTION,
        horizontalScrollBar != null ? horizontalScrollBar.getSelection() : 0);

    map.put(
        STATE_SCROLL_Y_SELECTION, verticalScrollBar != null ? verticalScrollBar.getSelection() : 0);
    return map;
  }

  public void applyStateProperties(Map<String, Object> stateProperties) {
    Double fMagnification = (Double) stateProperties.get(STATE_MAGNIFICATION);
    magnification = fMagnification == null ? 1.0f : fMagnification.floatValue();
    setZoomLabel();
    adjustScrolling();

    ScrollBar horizontalScrollBar = wsCanvas.getHorizontalBar();
    ScrollBar verticalScrollBar = wsCanvas.getVerticalBar();

    Integer scrollXSelection = (Integer) stateProperties.get(STATE_SCROLL_X_SELECTION);
    if (scrollXSelection != null && horizontalScrollBar != null) {
      horizontalScrollBar.setSelection(scrollXSelection);
    }

    Integer scrollYSelection = (Integer) stateProperties.get(STATE_SCROLL_Y_SELECTION);
    if (scrollYSelection != null && verticalScrollBar != null) {
      verticalScrollBar.setSelection(scrollYSelection);
    }

    redraw();
  }

  public abstract void adjustScrolling();

  protected void adjustScrolling(Point maximum) {
    int newWidth = (int) (calculateCorrectedMagnification() * maximum.x);
    int newHeight = (int) (calculateCorrectedMagnification() * maximum.y);
    int horizontalPct = 1;
    int verticalPct = 1;

    Rectangle canvasBounds = wsCanvas.getBounds();
    ScrollBar h = wsCanvas.getHorizontalBar();
    ScrollBar v = wsCanvas.getVerticalBar();

    if (canvasBounds.width == 0 || canvasBounds.height == 0) {
      h.setVisible(false);
      v.setVisible(false);
      return;
    }

    if (h != null) {
      horizontalPct = (int) Math.round(100.0 * h.getSelection() / 100.0);
    }
    if (v != null) {
      verticalPct = (int) Math.round(100.0 * v.getSelection() / 100.0);
    }

    canvas.setSize(canvasBounds.width, canvasBounds.height);
    if (h != null) {
      h.setVisible(newWidth >= canvasBounds.width);
    }
    if (v != null) {
      v.setVisible(newHeight >= canvasBounds.height);
    }

    int hThumb = (int) (100.0 * canvasBounds.width / newWidth);
    int vThumb = (int) (100.0 * canvasBounds.height / newHeight);

    if (h != null) {
      h.setMinimum(1);
      h.setMaximum(100);
      h.setThumb(Math.min(hThumb, 100));
      if (!EnvironmentUtils.getInstance().isWeb()) {
        h.setPageIncrement(5);
        h.setIncrement(1);
      }
      h.setSelection(horizontalPct);
    }
    if (v != null) {
      v.setMinimum(1);
      v.setMaximum(100);
      v.setThumb(Math.min(vThumb, 100));
      if (!EnvironmentUtils.getInstance().isWeb()) {
        v.setPageIncrement(5);
        v.setIncrement(1);
      }
      v.setSelection(verticalPct);
    }
    canvas.setFocus();
  }

  protected void showToolTip(org.eclipse.swt.graphics.Point location) {
    org.eclipse.swt.graphics.Point p = canvas.toDisplay(location);

    toolTip.setLocation(p.x + ConstUi.TOOLTIP_OFFSET, p.y + ConstUi.TOOLTIP_OFFSET);
    toolTip.setVisible(true);
  }

  protected void setupDragView(int button, Point screenClick) {
    viewDrag = button == 2; // Middle button
    if (viewDrag) {
      viewDragStart = screenClick;
      if (wsCanvas.getHorizontalBar() != null) {
        startHorizontalDragSelection = wsCanvas.getHorizontalBar().getSelection();
      } else {
        startHorizontalDragSelection = -1;
      }
      if (wsCanvas.getVerticalBar() != null) {
        startVerticalDragSelection = wsCanvas.getVerticalBar().getSelection();
      } else {
        startVerticalDragSelection = -1;
      }
    }
  }

  protected void dragView(Point lastClick, Point real) {

    /**
     * Calculate the differences for the scrollbars. We take the system zoom factor and current
     * magnification into account
     */
    int deltaX =
        (int)
            Math.round(
                (lastClick.x - real.x)
                    / (10.0 * PropsUi.getInstance().getZoomFactor())
                    / Math.max(1.0, magnification));
    int deltaY =
        (int)
            Math.round(
                (lastClick.y - real.y)
                    / (10.0 * PropsUi.getInstance().getZoomFactor())
                    / Math.max(1.0, magnification));

    ScrollBar h = wsCanvas.getHorizontalBar();
    if (h != null && startHorizontalDragSelection > 0) {
      int newSelection =
          Math.max(h.getMinimum(), Math.min(startHorizontalDragSelection + deltaX, h.getMaximum()));
      h.setSelection(newSelection);
    }
    ScrollBar v = wsCanvas.getVerticalBar();
    if (v != null && startVerticalDragSelection > 0) {
      int newSelection =
          Math.max(v.getMinimum(), Math.min(startVerticalDragSelection + deltaY, v.getMaximum()));
      v.setSelection(newSelection);
    }
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

  /** @param variables The variables to set */
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

  /** @param stateMap The stateMap to set */
  public void setStateMap(Map<String, Object> stateMap) {
    this.stateMap = stateMap;
  }

  /**
   * Gets avoidScrollAdjusting
   *
   * @return value of avoidScrollAdjusting
   */
  public boolean isAvoidScrollAdjusting() {
    return avoidScrollAdjusting;
  }

  /** @param avoidScrollAdjusting The avoidScrollAdjusting to set */
  public void setAvoidScrollAdjusting(boolean avoidScrollAdjusting) {
    this.avoidScrollAdjusting = avoidScrollAdjusting;
  }
}
