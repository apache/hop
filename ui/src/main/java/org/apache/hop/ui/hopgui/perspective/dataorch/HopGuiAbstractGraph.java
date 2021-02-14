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

package org.apache.hop.ui.hopgui.perspective.dataorch;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.ScrollBar;
import org.eclipse.swt.widgets.Shell;

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

  public boolean forceFocus() {
    return canvas.forceFocus();
  }

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
      horizontalScrollBar.setSelection(scrollXSelection.intValue());
    }

    Integer scrollYSelection = (Integer) stateProperties.get(STATE_SCROLL_Y_SELECTION);
    if (scrollYSelection != null && verticalScrollBar != null) {
      verticalScrollBar.setSelection(scrollYSelection.intValue());
    }

    redraw();
  }

  public abstract void adjustScrolling();

  protected void adjustScrolling( Point maximum ) {
    int newWidth = (int) (calculateCorrectedMagnification() * maximum.x);
    int newHeight = (int) (calculateCorrectedMagnification() * maximum.y);

    Rectangle canvasBounds = wsCanvas.getBounds();
    ScrollBar h = wsCanvas.getHorizontalBar();
    ScrollBar v = wsCanvas.getVerticalBar();

    if (canvasBounds.width==0 || canvasBounds.height==0) {
      h.setVisible( false );
      v.setVisible( false );
      return;
    }

    canvas.setSize(canvasBounds.width, canvasBounds.height);
    h.setVisible( newWidth >= canvasBounds.width );
    v.setVisible( newHeight >= canvasBounds.height );

    int hThumb = (int)(100.0*canvasBounds.width / newWidth );
    int vThumb = (int)(100.0*canvasBounds.height / newHeight );

    if (h != null) {
      h.setMinimum( 1 );
      h.setMaximum( 100 );
      h.setThumb(hThumb);
      if ( !EnvironmentUtils.getInstance().isWeb() ){
        h.setPageIncrement( 10 );
      }
    }
    if (v != null) {
      v.setMinimum( 1 );
      v.setMaximum( 100 );
      v.setThumb(vThumb);
      if ( !EnvironmentUtils.getInstance().isWeb() ) {
        v.setPageIncrement( 10 );
      }
    }
    canvas.setFocus();
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
}
