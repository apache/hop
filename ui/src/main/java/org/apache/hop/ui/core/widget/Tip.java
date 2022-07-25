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

package org.apache.hop.ui.core.widget;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.*;
import org.eclipse.swt.widgets.*;

public class Tip extends Composite {
  Shell parent;
  Shell tip;
  TrayItem item;
  int x;
  int y;
  int[] borderPolygon;
  boolean spikeAbove;
  boolean autohide;
  Listener listener;
  Listener parentListener;
  TextLayout layoutText;
  TextLayout layoutMessage;
  Region region;
  Font boldFont;
  Runnable runnable;
  static final int BORDER = 5;
  static final int PADDING = 5;
  static final int INSET = 4;
  static final int TIP_HEIGHT = 20;
  static final int IMAGE_SIZE = 16;
  static final int DELAY = 10000;

  public Tip(Shell parent, int style) {
    super(parent, style);
    this.parent = parent;
    this.autohide = true;
    this.x = this.y = -1;
    Display display = this.getDisplay();
    this.tip = new Shell(parent, 16392);
    this.tip.setBackground(parent.getBackground());
    this.listener =
        (event) -> {
          switch (event.type) {
            case 3:
              this.onMouseDown(event);
              break;
            case 9:
              this.onPaint(event);
              break;
            case 12:
              this.onDispose(event);
          }
        };
    this.addListener(12, this.listener);
    this.tip.addListener(9, this.listener);
    this.tip.addListener(3, this.listener);
    this.parentListener =
        (event) -> {
          this.dispose();
        };
    parent.addListener(12, this.parentListener);
  }

  public void addSelectionListener(SelectionListener listener) {
    this.checkWidget();
    if (listener == null) {
      SWT.error(4);
    }

    TypedListener typedListener = new TypedListener(listener);
    this.addListener(13, typedListener);
    this.addListener(14, typedListener);
  }

  void configure() {
    Display display = this.parent.getDisplay();
    int x = this.x;
    int y = this.y;
    if (x == -1 || y == -1) {
      Point point;
      point = display.getCursorLocation();

      x = point.x;
      y = point.y;
    }

    Monitor monitor = this.parent.getMonitor();
    Rectangle dest = monitor.getBounds();
    Point size = this.getSize(dest.width / 4);
    int w = size.x;
    int h = size.y;
    int t = (this.getStyle() & 4096) != 0 ? 20 : 0;
    int i = (this.getStyle() & 4096) != 0 ? 16 : 0;
    this.tip.setSize(w, h + t);
    this.spikeAbove = dest.height >= y + size.y + t;
    int[] polyline;
    if (dest.width >= x + size.x) {
      if (dest.height >= y + size.y + t) {
        polyline =
            new int[] {
              0, 5 + t, 1, 5 + t, 1, 3 + t, 3, 1 + t, 5, 1 + t, 5, t, 16, t, 16, 0, 35, t, w - 5, t,
              w - 5, 1 + t, w - 3, 1 + t, w - 1, 3 + t, w - 1, 5 + t, w, 5 + t, w, h - 5 + t, w - 1,
              h - 5 + t, w - 1, h - 3 + t, w - 2, h - 3 + t, w - 2, h - 2 + t, w - 3, h - 2 + t,
              w - 3, h - 1 + t, w - 5, h - 1 + t, w - 5, h + t, 5, h + t, 5, h - 1 + t, 3,
              h - 1 + t, 3, h - 2 + t, 2, h - 2 + t, 2, h - 3 + t, 1, h - 3 + t, 1, h - 5 + t, 0,
              h - 5 + t, 0, 5 + t
            };
        this.borderPolygon =
            new int[] {
              0, 5 + t, 1, 4 + t, 1, 3 + t, 3, 1 + t, 4, 1 + t, 5, t, 16, t, 16, 1, 35, t, w - 6,
              0 + t, w - 5, 1 + t, w - 4, 1 + t, w - 2, 3 + t, w - 2, 4 + t, w - 1, 5 + t, w - 1,
              h - 6 + t, w - 2, h - 5 + t, w - 2, h - 4 + t, w - 4, h - 2 + t, w - 5, h - 2 + t,
              w - 6, h - 1 + t, 5, h - 1 + t, 4, h - 2 + t, 3, h - 2 + t, 1, h - 4 + t, 1,
              h - 5 + t, 0, h - 6 + t, 0, 5 + t
            };
        this.tip.setLocation(Math.max(0, x - i), y);
      } else {
        polyline =
            new int[] {
              0, 5, 1, 5, 1, 3, 3, 1, 5, 1, 5, 0, w - 5, 0, w - 5, 1, w - 3, 1, w - 1, 3, w - 1, 5,
              w, 5, w, h - 5, w - 1, h - 5, w - 1, h - 3, w - 2, h - 3, w - 2, h - 2, w - 3, h - 2,
              w - 3, h - 1, w - 5, h - 1, w - 5, h, 35, h, 16, h + t, 16, h, 5, h, 5, h - 1, 3,
              h - 1, 3, h - 2, 2, h - 2, 2, h - 3, 1, h - 3, 1, h - 5, 0, h - 5, 0, 5
            };
        this.borderPolygon =
            new int[] {
              0, 5, 1, 4, 1, 3, 3, 1, 4, 1, 5, 0, w - 6, 0, w - 5, 1, w - 4, 1, w - 2, 3, w - 2, 4,
              w - 1, 5, w - 1, h - 6, w - 2, h - 5, w - 2, h - 4, w - 4, h - 2, w - 5, h - 2, w - 6,
              h - 1, 36, h - 1, 16, h + t - 1, 16, h - 1, 5, h - 1, 4, h - 2, 3, h - 2, 1, h - 4, 1,
              h - 5, 0, h - 6, 0, 5
            };
        this.tip.setLocation(Math.max(0, x - i), y - size.y - t);
      }
    } else if (dest.height >= y + size.y + t) {
      polyline =
          new int[] {
            0, 5 + t, 1, 5 + t, 1, 3 + t, 3, 1 + t, 5, 1 + t, 5, t, w - 35, t, w - 16, 0, w - 16, t,
            w - 5, t, w - 5, 1 + t, w - 3, 1 + t, w - 1, 3 + t, w - 1, 5 + t, w, 5 + t, w,
            h - 5 + t, w - 1, h - 5 + t, w - 1, h - 3 + t, w - 2, h - 3 + t, w - 2, h - 2 + t,
            w - 3, h - 2 + t, w - 3, h - 1 + t, w - 5, h - 1 + t, w - 5, h + t, 5, h + t, 5,
            h - 1 + t, 3, h - 1 + t, 3, h - 2 + t, 2, h - 2 + t, 2, h - 3 + t, 1, h - 3 + t, 1,
            h - 5 + t, 0, h - 5 + t, 0, 5 + t
          };
      this.borderPolygon =
          new int[] {
            0, 5 + t, 1, 4 + t, 1, 3 + t, 3, 1 + t, 4, 1 + t, 5, t, w - 35, t, w - 17, 2, w - 17, t,
            w - 6, t, w - 5, 1 + t, w - 4, 1 + t, w - 2, 3 + t, w - 2, 4 + t, w - 1, 5 + t, w - 1,
            h - 6 + t, w - 2, h - 5 + t, w - 2, h - 4 + t, w - 4, h - 2 + t, w - 5, h - 2 + t,
            w - 6, h - 1 + t, 5, h - 1 + t, 4, h - 2 + t, 3, h - 2 + t, 1, h - 4 + t, 1, h - 5 + t,
            0, h - 6 + t, 0, 5 + t
          };
      this.tip.setLocation(Math.min(dest.width - size.x, x - size.x + i), y);
    } else {
      polyline =
          new int[] {
            0, 5, 1, 5, 1, 3, 3, 1, 5, 1, 5, 0, w - 5, 0, w - 5, 1, w - 3, 1, w - 1, 3, w - 1, 5, w,
            5, w, h - 5, w - 1, h - 5, w - 1, h - 3, w - 2, h - 3, w - 2, h - 2, w - 3, h - 2,
            w - 3, h - 1, w - 5, h - 1, w - 5, h, w - 16, h, w - 16, h + t, w - 35, h, 5, h, 5,
            h - 1, 3, h - 1, 3, h - 2, 2, h - 2, 2, h - 3, 1, h - 3, 1, h - 5, 0, h - 5, 0, 5
          };
      this.borderPolygon =
          new int[] {
            0, 5, 1, 4, 1, 3, 3, 1, 4, 1, 5, 0, w - 6, 0, w - 5, 1, w - 4, 1, w - 2, 3, w - 2, 4,
            w - 1, 5, w - 1, h - 6, w - 2, h - 5, w - 2, h - 4, w - 4, h - 2, w - 5, h - 2, w - 6,
            h - 1, w - 17, h - 1, w - 17, h + t - 2, w - 36, h - 1, 5, h - 1, 4, h - 2, 3, h - 2, 1,
            h - 4, 1, h - 5, 0, h - 6, 0, 5
          };
      this.tip.setLocation(Math.min(dest.width - size.x, x - size.x + i), y - size.y - t);
    }

    if ((this.getStyle() & 4096) != 0) {
      if (this.region != null) {
        this.region.dispose();
      }

      this.region = new Region(display);
      this.region.add(polyline);
      this.tip.setRegion(this.region);
    }
  }

  public boolean getAutoHide() {
    this.checkWidget();
    return this.autohide;
  }

  Point getSize(int maxWidth) {
    int textWidth = 0;
    int messageWidth = 0;
    if (this.layoutText != null) {
      this.layoutText.setWidth(-1);
      textWidth = this.layoutText.getBounds().width;
    }

    if (this.layoutMessage != null) {
      this.layoutMessage.setWidth(-1);
      messageWidth = this.layoutMessage.getBounds().width;
    }

    int messageTrim = 28;
    boolean hasImage =
        this.layoutText != null && (this.getStyle() & 4096) != 0 && (this.getStyle() & 11) != 0;
    int textTrim = messageTrim + (hasImage ? 16 : 0);
    int width = Math.min(maxWidth, Math.max(textWidth + textTrim, messageWidth + messageTrim));
    int textHeight = 0;
    int messageHeight = 0;
    if (this.layoutText != null) {
      this.layoutText.setWidth(maxWidth - textTrim);
      textHeight = this.layoutText.getBounds().height;
    }

    if (this.layoutMessage != null) {
      this.layoutMessage.setWidth(maxWidth - messageTrim);
      messageHeight = this.layoutMessage.getBounds().height;
    }

    int height = 20 + messageHeight;
    if (this.layoutText != null) {
      height += Math.max(16, textHeight) + 10;
    }

    return new Point(width, height);
  }

  public String getMessage() {
    this.checkWidget();
    return this.layoutMessage != null ? this.layoutMessage.getText() : "";
  }

  public Shell getParent() {
    this.checkWidget();
    return this.parent;
  }

  public String getText() {
    this.checkWidget();
    return this.layoutText != null ? this.layoutText.getText() : "";
  }

  public boolean getVisible() {
    this.checkWidget();
    return this.tip.getVisible();
  }

  public boolean isVisible() {
    this.checkWidget();
    return this.getVisible();
  }

  void onDispose(Event event) {
    Control parent = this.getParent();
    parent.removeListener(12, this.parentListener);
    this.removeListener(12, this.listener);
    this.notifyListeners(12, event);
    event.type = 0;
    if (this.runnable != null) {
      Display display = this.getDisplay();
      display.timerExec(-1, this.runnable);
    }

    this.runnable = null;
    this.tip.dispose();
    this.tip = null;
    if (this.region != null) {
      this.region.dispose();
    }

    this.region = null;
    if (this.layoutText != null) {
      this.layoutText.dispose();
    }

    this.layoutText = null;
    if (this.layoutMessage != null) {
      this.layoutMessage.dispose();
    }

    this.layoutMessage = null;
    if (this.boldFont != null) {
      this.boldFont.dispose();
    }

    this.boldFont = null;
    this.borderPolygon = null;
  }

  void onMouseDown(Event event) {
    // this.sendSelectionEvent(13, (Event)null, true);
    this.setVisible(false);
  }

  void onPaint(Event event) {
    GC gc = event.gc;
    int x = 10;
    int y = 10;
    if ((this.getStyle() & 4096) != 0) {
      if (this.spikeAbove) {
        y += 20;
      }

      gc.drawPolygon(this.borderPolygon);
    } else {
      Rectangle rect = this.tip.getClientArea();
      gc.drawRectangle(rect.x, rect.y, rect.width - 1, rect.height - 1);
    }

    if (this.layoutText != null) {
      int id = this.getStyle() & 11;
      if ((this.getStyle() & 4096) != 0 && id != 0) {
        Display display = this.getDisplay();
        Image image = display.getSystemImage(id);
        Rectangle rect = image.getBounds();
        gc.drawImage(image, 0, 0, rect.width, rect.height, x, y, 16, 16);
        x += 16;
      }

      x += 4;
      this.layoutText.draw(gc, x, y);
      y += 10 + Math.max(16, this.layoutText.getBounds().height);
    }

    if (this.layoutMessage != null) {
      x = 14;
      this.layoutMessage.draw(gc, x, y);
    }
  }

  public void removeSelectionListener(SelectionListener listener) {
    this.checkWidget();
    if (listener == null) {
      SWT.error(4);
    }

    //        if (this.eventTable != null) {
    //            this.eventTable.unhook(13, listener);
    //            this.eventTable.unhook(14, listener);
    //        }
  }

  public void setAutoHide(boolean autoHide) {
    this.checkWidget();
    this.autohide = autoHide;
  }

  public void setLocation(int x, int y) {
    this.checkWidget();
    if (this.x != x || this.y != y) {
      this.x = x;
      this.y = y;
      if (this.tip.getVisible()) {
        this.configure();
      }
    }
  }

  public void setLocation(Point location) {
    this.checkWidget();
    if (location == null) {
      SWT.error(4);
    }

    this.setLocation(location.x, location.y);
  }

  public void setMessage(String string) {
    this.checkWidget();
    if (string == null) {
      SWT.error(4);
    }

    if (this.layoutMessage != null) {
      this.layoutMessage.dispose();
    }

    this.layoutMessage = null;
    if (string.length() != 0) {
      Display display = this.getDisplay();
      this.layoutMessage = new TextLayout(display);
      this.layoutMessage.setText(string);
    }

    if (this.tip.getVisible()) {
      this.configure();
    }
  }

  public void setText(String string) {
    this.checkWidget();
    if (string == null) {
      SWT.error(4);
    }

    if (this.layoutText != null) {
      this.layoutText.dispose();
    }

    this.layoutText = null;
    if (this.boldFont != null) {
      this.boldFont.dispose();
    }

    this.boldFont = null;
    if (string.length() != 0) {
      Display display = this.getDisplay();
      this.layoutText = new TextLayout(display);
      this.layoutText.setText(string);
      Font font = display.getSystemFont();
      FontData data = font.getFontData()[0];
      this.boldFont = new Font(display, data.getName(), data.getHeight(), 1);
      TextStyle style = new TextStyle(this.boldFont, (Color) null, (Color) null);
      this.layoutText.setStyle(style, 0, string.length());
    }

    if (this.tip.getVisible()) {
      this.configure();
    }
  }

  public void setVisible(boolean visible) {
    this.checkWidget();
    if (visible) {
      this.configure();
    }

    this.tip.setVisible(visible);
    Display display = this.getDisplay();
    if (this.runnable != null) {
      display.timerExec(-1, this.runnable);
    }

    this.runnable = null;
    if (this.autohide && visible) {
      this.runnable =
          () -> {
            if (!this.isDisposed()) {
              this.setVisible(false);
            }
          };
      display.timerExec(10000, this.runnable);
    }
  }
}
