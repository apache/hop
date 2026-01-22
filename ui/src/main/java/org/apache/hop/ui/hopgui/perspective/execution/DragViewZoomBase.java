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

import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;

public abstract class DragViewZoomBase extends Composite {
  protected Canvas canvas;
  protected DPoint offset;
  protected Point maximum;
  protected float magnification = 1.0f;
  protected org.apache.hop.core.gui.Rectangle viewPort;
  protected org.apache.hop.core.gui.Rectangle graphPort;
  protected boolean viewDrag;
  protected Point viewDragStart;
  protected DPoint viewDragBaseOffset;
  protected boolean viewPortNavigation;
  protected Point viewPortStart;

  public DragViewZoomBase(Composite parent, int style) {
    super(parent, style);
  }

  @Override
  public abstract void redraw();

  public Point screen2real(int x, int y) {
    float correctedMagnification = calculateCorrectedMagnification();
    DPoint real =
        new DPoint(
            ((double) x - offset.x) / correctedMagnification - offset.x,
            ((double) y - offset.y) / correctedMagnification - offset.y);
    return real.toPoint();
  }

  protected float calculateCorrectedMagnification() {
    return (float) (magnification * PropsUi.getInstance().getZoomFactor());
  }

  public abstract void setZoomLabel();

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

  @GuiKeyboardShortcut(control = true, key = '+')
  public void zoomIn() {
    magnification += 0.1f;
    // Minimum 1000%
    if (magnification > 10f) {
      magnification = 10f;
    }
    validateOffset();
    setZoomLabel();
    redraw();
  }

  /**
   * Zoom method used when scrolling the mousewheel change canvas position to keep cursor location
   * in screen
   *
   * @param mouseEvent mouse position when scrolling
   */
  public void zoomIn(MouseEvent mouseEvent) {
    double oldZoomFactor = PropsUi.getNativeZoomFactor() * Math.max(0.1, magnification);
    // get Area
    Point area = getArea();
    double oldViewWidth = area.x / oldZoomFactor;
    double oldViewHeight = area.y / oldZoomFactor;

    magnification += 0.1f;
    // Maximum 1000%
    if (magnification > 10f) {
      magnification = 10f;
    }

    double zoomFactor = PropsUi.getNativeZoomFactor() * Math.max(0.1, magnification);
    double viewWidth = area.x / zoomFactor;
    double viewHeight = area.y / zoomFactor;
    offset.x =
        offset.x
            + Math.floor(((double) mouseEvent.x / area.x) * 100) / 100 * (viewWidth - oldViewWidth);
    offset.y =
        offset.y
            + Math.floor(((double) mouseEvent.y / area.y) * 100)
                / 100
                * (viewHeight - oldViewHeight);

    validateOffset();
    setZoomLabel();
    redraw();
  }

  // Double keyboard shortcut zoom in '+' or '='
  @GuiKeyboardShortcut(control = true, key = '=')
  public void zoomIn2() {
    zoomIn();
  }

  @GuiKeyboardShortcut(control = true, key = '-')
  public void zoomOut() {
    magnification -= 0.1f;
    // Minimum 10%
    if (magnification < 0.1f) {
      magnification = 0.1f;
    }
    validateOffset();
    setZoomLabel();
    redraw();
  }

  /**
   * Zoom method used when scrolling the mousewheel change canvas position to keep cursor location
   * in screen
   *
   * @param mouseEvent mouse position when scrolling
   */
  public void zoomOut(MouseEvent mouseEvent) {
    double oldZoomFactor = PropsUi.getNativeZoomFactor() * Math.max(0.1, magnification);
    // get Area
    Point area = getArea();
    double oldViewWidth = area.x / oldZoomFactor;
    double oldViewHeight = area.y / oldZoomFactor;

    magnification -= 0.1f;
    // Minimum 10%
    if (magnification < 0.1f) {
      magnification = 0.1f;
    }

    double zoomFactor = PropsUi.getNativeZoomFactor() * Math.max(0.1, magnification);
    double viewWidth = area.x / zoomFactor;
    double viewHeight = area.y / zoomFactor;

    offset.x = offset.x + ((double) mouseEvent.x / area.x) * (viewWidth - oldViewWidth);
    offset.y = offset.y + ((double) mouseEvent.y / area.y) * (viewHeight - oldViewHeight);
    offset.x = offset.x > 0 ? 0 : offset.x;
    offset.y = offset.y > 0 ? 0 : offset.y;

    validateOffset();
    setZoomLabel();
    redraw();
  }

  @GuiKeyboardShortcut(control = true, key = '0')
  public void zoom100Percent() {
    magnification = 1.0f;
    validateOffset();
    setZoomLabel();
    redraw();
  }

  // Double keyboard shortcut zoom 100% '0' or keypad 0
  @GuiKeyboardShortcut(control = true, key = SWT.KEYPAD_0)
  public void zoom100Percent2() {
    zoom100Percent();
  }

  @GuiKeyboardShortcut(control = true, key = '*')
  public void zoomFitToScreen() {
    if (maximum.x <= 0 || maximum.y <= 0) {
      return;
    }
    org.eclipse.swt.graphics.Rectangle canvasBounds = canvas.getBounds();

    // Correct the maximum pipeline/workflow size for magnification, zoom factor, ...
    //
    double zoomXPct = canvasBounds.width / (maximum.x * PropsUi.getNativeZoomFactor()) * .9;
    double zoomYPct = canvasBounds.height / (maximum.y * PropsUi.getNativeZoomFactor()) * .9;

    // We take the minimum value of both to make sure everything fits on screen
    //
    magnification = (float) Math.min(zoomXPct, zoomYPct);

    // Reset the offset as well, otherwise we might still be off-screen
    //
    offset = new DPoint(0.0, 0.0);

    // Set this on the zoom level widget
    //
    setZoomLabel();
    canvas.redraw();
  }

  // Double keyboard shortcut zoom fit to screen '*' or keypad *
  @GuiKeyboardShortcut(control = true, key = SWT.KEYPAD_MULTIPLY)
  public void zoomFitToScreen2() {
    zoomFitToScreen();
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
    // Middle button
    // CTRL + left button
    //
    viewDrag = button == 2 || (control && button == 1);
    if (viewDrag) {
      viewDragStart = screenClick;
      viewDragBaseOffset = new DPoint(offset);
      // Change cursor when dragging view
      setCursor(getDisplay().getSystemCursor(SWT.CURSOR_SIZEALL));

      // For web environment, enable pan mode for client-side visual feedback during drag
      if (EnvironmentUtils.getInstance().isWeb() && canvas != null) {
        canvas.setData("mode", "pan");
        canvas.setData("panStartOffset", new Point((int) offset.x, (int) offset.y));
        canvas.setData("panCurrentOffset", new Point((int) offset.x, (int) offset.y));

        // Pass boundary information to client for constraint validation
        double zoomFactor = PropsUi.getNativeZoomFactor() * Math.max(0.1, magnification);
        Point area = getArea();
        double viewWidth = area.x / zoomFactor;
        double viewHeight = area.y / zoomFactor;

        // Calculate min/max offset boundaries (same as validateOffset())
        double minX = -maximum.x + viewWidth;
        double minY = -maximum.y + viewHeight;
        double maxX = 0;
        double maxY = 0;

        canvas.setData(
            "panBoundaries",
            new org.apache.hop.core.gui.Rectangle((int) minX, (int) minY, (int) maxX, (int) maxY));

        // Force immediate redraw to sync pan data to client BEFORE mouse move events
        // This ensures the client has the pan data when the first MouseMove arrives
        redraw();
      }

      return true;
    }
    return false;
  }

  /**
   * See if this is a click on the navigation view inner rectangle with the goal of dragging it
   * around a bit.
   */
  protected boolean setupDragViewPort(Point screenClick) {
    if (viewPort != null && viewPort.contains(screenClick)) {
      viewPortNavigation = true;
      viewPortStart = new Point(screenClick);
      viewDragBaseOffset = new DPoint(offset);
      // Change cursor when dragging view port
      setCursor(getDisplay().getSystemCursor(SWT.CURSOR_SIZEALL));
      return true;
    }
    return false;
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

    // Let's not move the graph off the screen to the top/left
    //
    double minX = -graphWidth + viewWidth;
    if (offset.x < minX) {
      offset.x = minX;
    }
    double minY = -graphHeight + viewHeight;
    if (offset.y < minY) {
      offset.y = minY;
    }

    // Are we moving the graph too far down/right?
    //
    double maxX = 0;
    if (offset.x > maxX) {
      offset.x = maxX;
    }
    double maxY = 0;
    if (offset.y > maxY) {
      offset.y = maxY;
    }
  }

  protected Point getArea() {
    org.eclipse.swt.graphics.Rectangle rect = canvas.getClientArea();
    return new Point(rect.width, rect.height);
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

    // For web environment, update canvas data for client-side visual feedback during drag
    if (EnvironmentUtils.getInstance().isWeb() && canvas != null) {
      Point startOffset = (Point) canvas.getData("panStartOffset");
      if (startOffset != null) {
        int offsetDeltaX = (int) (offset.x - startOffset.x);
        int offsetDeltaY = (int) (offset.y - startOffset.y);
        canvas.setData("panOffsetDelta", new Point(offsetDeltaX, offsetDeltaY));
        canvas.setData("panCurrentOffset", new Point((int) offset.x, (int) offset.y));
      }
    }

    redraw();
  }

  protected void mouseScrolled(MouseEvent mouseEvent) {
    // Check if zoom scrolling is disabled
    if (PropsUi.getInstance().isZoomScrollingDisabled()) {
      return;
    }

    // Zoom in or out every time we get an event.
    //
    // In the future we do want to take the location of the mouse into account.
    // That way we can adjust the offset accordingly to keep the screen centered on the mouse while
    // zooming in or out.
    //
    if (mouseEvent.count > 0) {
      zoomIn(mouseEvent);
    } else {
      zoomOut(mouseEvent);
    }
  }
}
