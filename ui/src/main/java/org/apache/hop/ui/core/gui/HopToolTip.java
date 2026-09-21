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

package org.apache.hop.ui.core.gui;

import java.util.ArrayList;
import java.util.List;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/**
 * The canvas tooltip: a rounded "pill" in the inverted colours of the theme (dark on the light
 * theme, light on the dark theme), the way macOS and VS Code show theirs. It is a Shell of our own
 * because the SWT ToolTip widget cannot be coloured, which broke it in dark mode (issue #2665).
 *
 * <p>Desktop SWT gets the rounded corners from a shell region; Hop Web has no regions and gets them
 * from the {@code Shell.hopToolTip} rule in its theme CSS instead.
 */
public class HopToolTip {

  /** Custom variant of the tooltip shell and label, styled in the Hop Web theme CSS. */
  public static final String CUSTOM_VARIANT = "hopToolTip";

  private static final String RWT_CUSTOM_VARIANT = "org.eclipse.rap.rwt.customVariant";

  /** Corner radius before zoom. */
  private static final int CORNER_RADIUS = 6;

  private final Shell tipShell;
  private final Label tipLabel;
  private final boolean web;
  @Setter private boolean autoHide = true;

  /** Bumped on every setText/setVisible so that an older hide timer knows it is stale. */
  private int generation;

  /**
   * Creates a new custom tooltip
   *
   * @param parent The parent shell
   */
  public HopToolTip(Shell parent) {
    web = EnvironmentUtils.getInstance().isWeb();

    tipShell = new Shell(parent, SWT.NO_TRIM | SWT.ON_TOP | SWT.NO_FOCUS | SWT.TOOL);
    tipShell.setData(RWT_CUSTOM_VARIANT, CUSTOM_VARIANT);
    double zoom = PropsUi.getInstance().getZoomFactor();
    FillLayout layout = new FillLayout();
    layout.marginWidth = (int) Math.round(10 * zoom);
    layout.marginHeight = (int) Math.round(6 * zoom);
    tipShell.setLayout(layout);

    tipLabel = new Label(tipShell, SWT.NONE);
    tipLabel.setData(RWT_CUSTOM_VARIANT, CUSTOM_VARIANT);

    // Inverted colours: a dark pill on the light theme, a light pill on the dark theme.
    //
    GuiResource gui = GuiResource.getInstance();
    Color background;
    Color foreground;
    if (PropsUi.getInstance().isDarkMode()) {
      background = gui.getColor(243, 244, 246);
      foreground = gui.getColor(30, 30, 30);
    } else {
      background = gui.getColor(43, 45, 48);
      foreground = gui.getColor(242, 242, 242);
    }
    tipShell.setBackground(background);
    tipLabel.setBackground(background);
    tipLabel.setForeground(foreground);

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
   * Sets the tooltip text. Lines that only draw an ASCII rule ("-----") are dropped: the pill has
   * no room for decoration.
   *
   * @param text The text to display
   */
  public void setText(String text) {
    if (tipLabel != null && !tipLabel.isDisposed()) {
      generation++;
      tipLabel.setText(cleanText(text));
      tipShell.pack();
      if (!web) {
        Point size = tipShell.getSize();
        int radius = (int) Math.round(CORNER_RADIUS * PropsUi.getInstance().getZoomFactor());
        RoundedShellRegion.apply(tipShell, size.x, size.y, radius);
      }
    }
  }

  static String cleanText(String text) {
    if (text == null) {
      return "";
    }
    List<String> lines = new ArrayList<>();
    for (String line : text.split("\r?\n")) {
      String trimmed = line.strip();
      if (!trimmed.isEmpty() && trimmed.chars().allMatch(c -> c == '-' || c == '=')) {
        continue; // an ASCII rule
      }
      lines.add(line.stripTrailing());
    }
    // Drop the surrounding blank lines some tooltips use for spacing: the padding does that now.
    while (!lines.isEmpty() && lines.get(0).isBlank()) {
      lines.remove(0);
    }
    while (!lines.isEmpty() && lines.get(lines.size() - 1).isBlank()) {
      lines.remove(lines.size() - 1);
    }
    return String.join(Const.CR, lines).strip();
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
      generation++;
      tipShell.setVisible(visible);
    }
  }

  /**
   * Hide the tooltip after a delay, unless it was changed or hidden and shown again in the
   * meantime. For notices like "Selection cleared" that are not tied to something under the
   * pointer: on the desktop the next mouse move takes them down, in Hop Web nothing would.
   *
   * @param millis the delay in milliseconds
   */
  public void hideAfter(int millis) {
    if (tipShell == null || tipShell.isDisposed()) {
      return;
    }
    final int shown = generation;
    tipShell
        .getDisplay()
        .timerExec(
            millis,
            () -> {
              if (shown == generation && isVisible()) {
                setVisible(false);
              }
            });
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
