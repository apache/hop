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

package org.apache.hop.ui.core.gui;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Monitor;
import org.eclipse.swt.widgets.Shell;

/** This class stores information about a screen, window, etc. */
public class WindowProperty {
  private String name;
  private boolean maximized;
  private int x;
  private int y;
  private int width;
  private int height;

  public WindowProperty() {
    name = null;
    maximized = false;
    x = -1;
    y = -1;
    width = -1;
    height = -1;
  }

  public WindowProperty(String name, boolean maximized, int x, int y, int width, int height) {
    this.name = name;
    this.maximized = maximized;
    this.x = x;
    this.y = y;
    this.width = width;
    this.height = height;
  }

  public WindowProperty(Shell shell) {
    name = shell.getText();

    maximized = shell.getMaximized();
    Rectangle rectangle = shell.getBounds();
    this.x = rectangle.x;
    this.y = rectangle.y;
    this.width = rectangle.width;
    this.height = rectangle.height;
  }

  public WindowProperty(String windowName, Map<String, Object> stateProperties) {
    this.name = windowName;
    setStateProperties(stateProperties);
  }

  public void setShell(Shell shell) {
    setShell(shell, false);
  }

  public void setShell(Shell shell, boolean onlyPosition) {
    setShell(shell, onlyPosition, -1, -1);
  }

  public void setShell(Shell shell, int minWidth, int minHeight) {
    setShell(shell, false, minWidth, minHeight);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WindowProperty that = (WindowProperty) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  public boolean hasSize() {
    return width > 0 && height > 0;
  }

  public boolean hasPosition() {
    return x > 0 && y > 0;
  }

  public Map<String, Object> getStateProperties() {
    Map<String, Object> map = new HashMap<>();
    map.put("max", maximized);
    map.put("x", x);
    map.put("y", y);
    map.put("width", width);
    map.put("height", height);
    return map;
  }

  public void setStateProperties(Map<String, Object> map) {
    Boolean bMaximized = (Boolean) map.get("max");
    maximized = bMaximized == null ? false : bMaximized;

    Integer iX = (Integer) map.get("x");
    x = iX == null ? -1 : iX;

    Integer iY = (Integer) map.get("y");
    y = iY == null ? -1 : iY;

    Integer iWidth = (Integer) map.get("width");
    width = iWidth == null ? -1 : iWidth;

    Integer iHeight = (Integer) map.get("height");
    height = iHeight == null ? -1 : iHeight;
  }

  /**
   * Performs calculations to size and position a dialog If the size passed in is too large for the
   * primary monitor client area, it is shrunk to fit. If the positioning leaves part of the dialog
   * outside the client area, it is centered instead.
   *
   * @param shell The dialog to position and size
   * @param onlyPosition Unused argument. If the window is outside the viewable client are, it must
   *     be resized to prevent inaccessibility.
   * @param minWidth
   * @param minHeight
   */
  public void setShell(Shell shell, boolean onlyPosition, int minWidth, int minHeight) {
    shell.setMaximized(maximized);
    shell.setBounds(new Rectangle(x, y, width, height));

    if (minWidth > 0 || minHeight > 0) {
      Rectangle bounds = shell.getBounds();
      if (bounds.width < minWidth) {
        bounds.width = minWidth;
      }
      if (bounds.height < minHeight) {
        bounds.height = minHeight;
      }
      shell.setSize(bounds.width, bounds.height);
    }

    // Use the saved size as-is, don't try to adjust it based on computed size
    Rectangle shellSize = shell.getBounds();

    Rectangle entireClientArea = shell.getDisplay().getClientArea();
    Rectangle resizedRect =
        new Rectangle(shellSize.x, shellSize.y, shellSize.width, shellSize.height);
    constrainRectangleToContainer(resizedRect, entireClientArea);

    boolean needsRepositioning = !resizedRect.equals(shellSize);
    boolean isClipped = isClippedByUnalignedMonitors(resizedRect, shell.getDisplay());

    // If the persisted size/location doesn't perfectly fit
    // into the entire client area, the persisted settings
    // likely were not meant for this configuration of monitors.
    // Try to find which monitor the saved position belongs to, then
    // relocate the shell to either that monitor, the parent monitor,
    // or the primary monitor as a last resort.
    //
    if (needsRepositioning || isClipped) {
      // First, try to find the monitor that contains the saved position
      Monitor monitor = getMonitorForPosition(shell.getDisplay(), x, y);

      // If no monitor contains the saved position, fall back to parent or primary
      if (monitor == null) {
        monitor = shell.getDisplay().getPrimaryMonitor();
        if (shell.getParent() != null) {
          monitor = shell.getParent().getMonitor();
        }
      }

      Rectangle monitorClientArea = monitor.getClientArea();
      constrainRectangleToContainer(resizedRect, monitorClientArea);

      resizedRect.x = monitorClientArea.x + (monitorClientArea.width - resizedRect.width) / 2;
      resizedRect.y = monitorClientArea.y + (monitorClientArea.height - resizedRect.height) / 2;

      shell.setBounds(resizedRect);
    }
  }

  /**
   * @param child
   * @param container
   */
  private void constrainRectangleToContainer(Rectangle child, Rectangle container) {
    Point originalSize = new Point(child.width, child.height);
    Point containerSize = new Point(container.width, container.height);
    Point oversize = new Point(originalSize.x - containerSize.x, originalSize.y - containerSize.y);
    if (oversize.x > 0) {
      child.width = originalSize.x - oversize.x;
    }
    if (oversize.y > 0) {
      child.height = originalSize.y - oversize.y;
    }
    // Detect if the dialog was positioned outside the container
    // If so, center the child in the container...
    //
    if (child.x < container.x
        || child.y < container.y
        || child.x + child.width > container.x + container.width
        || child.y + child.height > container.y + container.height) {
      child.x = (container.width - child.width) / 2;
      child.y = (container.height - child.height) / 2;
    }
  }

  /**
   * This method is needed in the case where the display has multiple monitors, but they do not form
   * a uniform rectangle. In this case, it is possible for Geometry.moveInside() to not detect that
   * the window is partially or completely clipped. We check to make sure at least the upper left
   * portion of the rectangle is visible to give the user the ability to reposition the dialog in
   * this rare case.
   *
   * @param constrainee
   * @param display
   * @return
   */
  private boolean isClippedByUnalignedMonitors(Rectangle constrainee, Display display) {
    boolean isClipped;
    Monitor[] monitors = display.getMonitors();
    if (monitors.length > 0) {
      // Loop searches for a monitor proving false
      isClipped = true;
      for (Monitor monitor : monitors) {
        if (monitor.getClientArea().contains(constrainee.x + 10, constrainee.y + 10)) {
          isClipped = false;
          break;
        }
      }
    } else {
      isClipped = false;
    }

    return isClipped;
  }

  /**
   * Finds the monitor that contains the specified position. This is used to restore windows on the
   * correct monitor in multi-monitor setups, especially on macOS where saved window positions
   * should be restored to the same monitor they were on when closed.
   *
   * @param display the display containing multiple monitors
   * @param x the x coordinate to check
   * @param y the y coordinate to check
   * @return the Monitor containing the position, or null if no monitor contains it
   */
  private Monitor getMonitorForPosition(Display display, int x, int y) {
    Monitor[] monitors = display.getMonitors();

    for (Monitor monitor : monitors) {
      Rectangle bounds = monitor.getBounds();

      // Check both the exact position and a point slightly inside (100px)
      // to handle positions saved at the very edge of a monitor
      if (bounds.contains(x, y) || bounds.contains(x + 100, y + 100)) {
        return monitor;
      }
    }

    return null;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets maximized
   *
   * @return value of maximized
   */
  public boolean isMaximized() {
    return maximized;
  }

  /**
   * @param maximized The maximized to set
   */
  public void setMaximized(boolean maximized) {
    this.maximized = maximized;
  }

  /**
   * Gets x
   *
   * @return value of x
   */
  public int getX() {
    return x;
  }

  /**
   * @param x The x to set
   */
  public void setX(int x) {
    this.x = x;
  }

  /**
   * Gets y
   *
   * @return value of y
   */
  public int getY() {
    return y;
  }

  /**
   * @param y The y to set
   */
  public void setY(int y) {
    this.y = y;
  }

  /**
   * Gets width
   *
   * @return value of width
   */
  public int getWidth() {
    return width;
  }

  /**
   * @param width The width to set
   */
  public void setWidth(int width) {
    this.width = width;
  }

  /**
   * Gets height
   *
   * @return value of height
   */
  public int getHeight() {
    return height;
  }

  /**
   * @param height The height to set
   */
  public void setHeight(int height) {
    this.height = height;
  }
}
