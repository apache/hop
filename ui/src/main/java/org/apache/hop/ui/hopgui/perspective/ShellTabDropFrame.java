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

package org.apache.hop.ui.hopgui.perspective;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.graphics.Region;
import org.eclipse.swt.widgets.Shell;

/**
 * The desktop drop frame: a floating {@code SWT.NO_TRIM | SWT.ON_TOP} shell shaped into a hollow
 * rectangle with a {@link Region}. The cut-out centre is not part of the window, so the drag cursor
 * passes through it to the folder underneath (no enter/leave oscillation), and only the thin border
 * is painted (no compositing flicker).
 */
final class ShellTabDropFrame implements TabDropFrame {

  private Shell shell;

  /** The frame-shaped region applied to {@link #shell}; disposed with it. */
  private Region region;

  @Override
  public void show(CTabFolder folder, Rectangle bounds) {
    if (folder.isDisposed() || bounds.width <= 0 || bounds.height <= 0) {
      hide();
      return;
    }
    Point topLeft = folder.toDisplay(bounds.x, bounds.y);
    Rectangle onDisplay = new Rectangle(topLeft.x, topLeft.y, bounds.width, bounds.height);
    try {
      if (shell == null || shell.isDisposed()) {
        shell = new Shell(folder.getShell(), SWT.NO_TRIM | SWT.ON_TOP);
        shell.setBackground(folder.getDisplay().getSystemColor(SWT.COLOR_LIST_SELECTION));
        shell.addDisposeListener(e -> disposeRegion());
      }
      shell.setBounds(onDisplay);
      applyFrameRegion(onDisplay.width, onDisplay.height);
      if (!shell.getVisible()) {
        shell.setVisible(true);
      }
    } catch (Exception e) {
      hide();
    }
  }

  @Override
  public void hide() {
    if (shell != null && !shell.isDisposed() && shell.getVisible()) {
      shell.setVisible(false);
    }
  }

  @Override
  public void dispose() {
    if (shell != null && !shell.isDisposed()) {
      shell.dispose();
    }
    shell = null;
    disposeRegion();
  }

  /** Shape the shell as a hollow rectangle frame of the given size. */
  private void applyFrameRegion(int width, int height) {
    int border = Math.max(3, Math.min(8, Math.min(width, height) / 12));
    Region frame = new Region(shell.getDisplay());
    frame.add(0, 0, width, height);
    if (width > 2 * border && height > 2 * border) {
      frame.subtract(border, border, width - 2 * border, height - 2 * border);
    }
    shell.setRegion(frame);
    disposeRegion();
    region = frame;
  }

  private void disposeRegion() {
    if (region != null && !region.isDisposed()) {
      region.dispose();
    }
    region = null;
  }
}
