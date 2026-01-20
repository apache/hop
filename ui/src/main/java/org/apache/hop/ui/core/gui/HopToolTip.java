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

import lombok.Setter;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/**
 * A custom tooltip implementation using a Shell and Label that supports proper dark mode styling.
 * This replaces the SWT ToolTip widget which doesn't support background/foreground color
 * customization.
 */
public class HopToolTip {

  private final Shell tipShell;
  private final Label tipLabel;
  @Setter private boolean autoHide = true;

  /**
   * Creates a new custom tooltip
   *
   * @param parent The parent shell
   */
  public HopToolTip(Shell parent) {
    tipShell = new Shell(parent, SWT.ON_TOP | SWT.NO_FOCUS | SWT.TOOL);
    FillLayout layout = new FillLayout();
    layout.marginWidth = ConstUi.SMALL_MARGIN;
    layout.marginHeight = ConstUi.SMALL_MARGIN;
    tipShell.setLayout(layout);

    tipLabel = new Label(tipShell, SWT.NONE);

    // Apply dark mode styling
    GuiResource gui = GuiResource.getInstance();
    if (PropsUi.getInstance().isDarkMode()) {
      tipShell.setBackground(gui.getColor(32, 31, 27)); // Dark background
      tipLabel.setBackground(gui.getColor(32, 31, 27));
      tipLabel.setForeground(gui.getColor(224, 224, 224)); // Light text
    } else {
      tipShell.setBackground(gui.getColorWhite()); // Light background
      tipLabel.setBackground(gui.getColorWhite());
      tipLabel.setForeground(gui.getColorBlack()); // Dark text
    }

    // Auto-hide when user clicks anywhere
    Display.getCurrent()
        .addFilter(
            SWT.MouseDown,
            event -> {
              if (autoHide && !tipShell.isDisposed() && tipShell.isVisible()) {
                setVisible(false);
              }
            });
  }

  /**
   * Sets the tooltip text
   *
   * @param text The text to display
   */
  public void setText(String text) {
    if (tipLabel != null && !tipLabel.isDisposed()) {
      tipLabel.setText(text != null ? text : "");
      tipShell.pack();
    }
  }

  /**
   * Gets the tooltip text
   *
   * @return The current text
   */
  public String getText() {
    if (tipLabel != null && !tipLabel.isDisposed()) {
      return tipLabel.getText();
    }
    return "";
  }

  /**
   * Sets the tooltip location
   *
   * @param x The x coordinate
   * @param y The y coordinate
   */
  public void setLocation(int x, int y) {
    if (tipShell != null && !tipShell.isDisposed()) {
      tipShell.setLocation(x, y);
    }
  }

  /**
   * Sets the tooltip location
   *
   * @param location The location point
   */
  public void setLocation(Point location) {
    setLocation(location.x, location.y);
  }

  /**
   * Shows or hides the tooltip
   *
   * @param visible true to show, false to hide
   */
  public void setVisible(boolean visible) {
    if (tipShell != null && !tipShell.isDisposed()) {
      tipShell.setVisible(visible);
    }
  }

  /**
   * Checks if the tooltip is visible
   *
   * @return true if visible
   */
  public boolean isVisible() {
    return tipShell != null && !tipShell.isDisposed() && tipShell.isVisible();
  }

  /** Disposes the tooltip */
  public void dispose() {
    if (tipShell != null && !tipShell.isDisposed()) {
      tipShell.dispose();
    }
  }

  /**
   * Checks if the tooltip is disposed
   *
   * @return true if disposed
   */
  public boolean isDisposed() {
    return tipShell == null || tipShell.isDisposed();
  }
}
