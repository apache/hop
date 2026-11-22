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

import java.lang.reflect.Method;
import org.apache.hop.ui.core.widget.OsHelper;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Monitor;

/**
 * Places a popup so it grows upward from the widget that was clicked.
 *
 * <p>SWT top-aligns a popup to the point passed to {@link Menu#setLocation(Point)}. A point just
 * under the status bar therefore drops the menu below the window whenever the desktop continues
 * there. When the window is maximized that point lands in the taskbar, outside every monitor work
 * area. {@code Menu.setLocation} then scales it with the primary monitor's zoom, so on a second
 * monitor with a different scale the menu jumps away from the click. The point returned here stays
 * inside the monitor that contains the anchor.
 */
final class PopupMenuPlacement {

  /** {@code SM_CYMENU}. Height of one menu row, in pixels, at the requested DPI. */
  private static final int SM_CYMENU = 0xf;

  /** {@code SM_CYMENUCHECK}. Height of the menu check bitmap. A row is at least this tall. */
  private static final int SM_CYMENUCHECK = 72;

  /**
   * Frame around the item stack, in points. SWT's own menu bounds add a few pixels past the items.
   */
  private static final int MENU_CHROME = 8;

  private PopupMenuPlacement() {}

  /**
   * Top-left of a top-aligned popup whose bottom edge sits on {@code anchor.y}. When that point
   * would leave {@code monitorArea}, it is pulled back inside. The bottom and right edges of an SWT
   * rectangle are exclusive, and a point on that edge is what makes the Windows placement fall back
   * to the primary monitor.
   */
  static Point above(Rectangle anchor, int menuHeight, Rectangle monitorArea) {
    int x = anchor.x;
    int y = anchor.y - Math.max(menuHeight, 0);
    if (monitorArea == null || monitorArea.width <= 0 || monitorArea.height <= 0) {
      return new Point(x, y);
    }
    int right = monitorArea.x + monitorArea.width;
    int bottom = monitorArea.y + monitorArea.height;
    if (x < monitorArea.x) {
      x = monitorArea.x;
    } else if (x >= right) {
      x = right - 1;
    }
    if (y < monitorArea.y) {
      y = monitorArea.y;
    } else if (y >= bottom) {
      y = bottom - 1;
    }
    return new Point(x, y);
  }

  static void showAbove(Menu menu, Rectangle anchor, Monitor monitor) {
    Rectangle area = monitor == null ? null : monitor.getClientArea();
    menu.setLocation(above(anchor, heightOf(menu, monitor), area));
    menu.setVisible(true);
  }

  /**
   * Height of {@code menu} in the same points {@link Menu#setLocation(Point)} expects. Windows
   * reports the row height in pixels at the monitor DPI; other platforms use the menu font. The
   * frame around the items is in points on every platform, because the font path is already in
   * points and must not be scaled a second time.
   */
  static int heightOf(Menu menu, Monitor monitor) {
    if (menu == null || menu.isDisposed()) {
      return 0;
    }
    MenuItem[] items = menu.getItems();
    if (items.length == 0) {
      return 0;
    }
    int rows = 0;
    int separators = 0;
    int tallestImage = 0;
    for (MenuItem item : items) {
      if (item.isDisposed()) {
        continue;
      }
      if ((item.getStyle() & SWT.SEPARATOR) != 0) {
        separators++;
        continue;
      }
      rows++;
      Image image = item.getImage();
      if (image != null && !image.isDisposed()) {
        tallestImage = Math.max(tallestImage, image.getBounds().height);
      }
    }
    if (rows == 0 && separators == 0) {
      return 0;
    }
    int rowPoints = rowHeight(menu, monitor, tallestImage);
    int separatorPoints = Math.max(6, rowPoints / 3);
    return rows * rowPoints + separators * separatorPoints + MENU_CHROME;
  }

  private static int rowHeight(Menu menu, Monitor monitor, int tallestImage) {
    int rowPoints = -1;
    if (OsHelper.isWindows() && monitor != null) {
      int zoom = monitor.getZoom();
      int rowPixels = windowsRowPixels(zoom);
      if (rowPixels > 0) {
        rowPoints = pixelsToPoints(rowPixels, zoom);
      }
    }
    if (rowPoints <= 0) {
      rowPoints = textHeight(menu) + 8;
    }
    if (tallestImage > 0) {
      rowPoints = Math.max(rowPoints, tallestImage + 2);
    }
    return rowPoints;
  }

  private static int textHeight(Menu menu) {
    GC gc = new GC(menu.getShell());
    try {
      gc.setFont(menu.getDisplay().getSystemFont());
      return gc.textExtent("Ag").y;
    } finally {
      gc.dispose();
    }
  }

  private static int windowsRowPixels(int zoom) {
    try {
      Class<?> os = Class.forName("org.eclipse.swt.internal.win32.OS");
      Method forDpi = os.getMethod("GetSystemMetricsForDpi", int.class, int.class);
      int dpi = (zoom <= 0 ? 100 : zoom) * 96 / 100;
      int cyMenu = (Integer) forDpi.invoke(null, SM_CYMENU, dpi);
      int cyCheck = (Integer) forDpi.invoke(null, SM_CYMENUCHECK, dpi);
      return Math.max(cyMenu, cyCheck);
    } catch (Exception | LinkageError e) {
      return -1;
    }
  }

  /** Same rounding as {@code DPIUtil.pixelToPoint}. */
  static int pixelsToPoints(int pixels, int zoom) {
    if (pixels <= 0) {
      return 0;
    }
    if (zoom == 100 || zoom <= 0) {
      return pixels;
    }
    return Math.round(pixels * 100f / zoom);
  }
}
