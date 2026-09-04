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
package org.apache.hop.lint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.hopgui.HopGui;

/** Groups lint results for canvas overlays and draws severity badges on pipeline/workflow icons. */
public final class LintCanvasOverlayHelper {

  public static final String AREA_LINT_OVERLAY = "hop-lint-overlay";

  /** AreaOwner marker for the clickable lint totals overlay drawn on the canvas. */
  public static final String AREA_LINT_TOTALS = "hop-lint-totals";

  // Layout constants for the totals overlay (logical pixels).
  private static final int TOTALS_PADDING = 7;
  public static final int TOTALS_HEIGHT = 26;
  private static final int TOTALS_ICON = 16;
  private static final int TOTALS_TEXT_GAP = 4;
  private static final int TOTALS_CHIP_GAP = 12;

  private static final String ERROR_SVG = "ui/images/error.svg";
  private static final String WARNING_SVG = "ui/images/warning.svg";
  private static final String INFO_SVG = "ui/images/info.svg";

  // Last drawn totals-overlay rectangle per file (logical widget coords), for click hit-testing.
  private static final Map<String, int[]> totalsScreenRects = new ConcurrentHashMap<>();

  private LintCanvasOverlayHelper() {}

  /**
   * Remember (or clear, when {@code width <= 0}) the on-screen totals overlay rectangle for a file.
   */
  public static void rememberTotalsRect(String filePath, int x, int y, int width, int height) {
    if (filePath == null) {
      return;
    }
    if (width <= 0) {
      totalsScreenRects.remove(filePath);
    } else {
      totalsScreenRects.put(filePath, new int[] {x, y, width, height});
    }
  }

  /** True when the given logical widget point falls inside the file's totals overlay. */
  public static boolean totalsRectContains(String filePath, int sx, int sy) {
    if (filePath == null) {
      return false;
    }
    int[] r = totalsScreenRects.get(filePath);
    return r != null && sx >= r[0] && sx <= r[0] + r[2] && sy >= r[1] && sy <= r[1] + r[3];
  }

  /** Count results per severity, returning {errors, warnings, infos}. */
  public static int[] countSeverities(List<LintResult> results) {
    int errors = 0;
    int warnings = 0;
    int infos = 0;
    if (results != null) {
      for (LintResult result : results) {
        if (result == null) {
          continue;
        }
        String severity = result.getSeverity();
        if ("ERROR".equalsIgnoreCase(severity)) {
          errors++;
        } else if ("WARNING".equalsIgnoreCase(severity)) {
          warnings++;
        } else {
          infos++;
        }
      }
    }
    return new int[] {errors, warnings, infos};
  }

  /**
   * Draw the lint totals overlay (error/warning/info icon + count for each severity) at the given
   * position. All three severities are always shown, including zero counts. Returns the total width
   * drawn. {@code iconMagnification} controls the SVG rasterization resolution (use the native zoom
   * factor for crisp icons on high-DPI displays).
   */
  public static int drawTotalsOverlay(
      IGc gc, int x, int y, int errors, int warnings, int infos, float iconMagnification) {
    if (gc == null) {
      return 0;
    }

    // Always show all three severities, in order, including zero counts.
    String[] svgs = {ERROR_SVG, WARNING_SVG, INFO_SVG};
    IGc.EColor[] fallbackColors = {IGc.EColor.RED, IGc.EColor.YELLOW, IGc.EColor.LIGHTBLUE};
    String[] texts = {String.valueOf(errors), String.valueOf(warnings), String.valueOf(infos)};

    gc.setFont(IGc.EFont.GRAPH);

    // Measure total width.
    int contentWidth = 0;
    for (int i = 0; i < texts.length; i++) {
      contentWidth += TOTALS_ICON + TOTALS_TEXT_GAP + gc.textExtent(texts[i]).x;
      if (i < texts.length - 1) {
        contentWidth += TOTALS_CHIP_GAP;
      }
    }
    int totalWidth = contentWidth + 2 * TOTALS_PADDING;

    // Background.
    gc.setBackground(IGc.EColor.BACKGROUND);
    gc.fillRoundRectangle(x, y, totalWidth, TOTALS_HEIGHT, 10, 10);
    gc.setLineWidth(1);
    gc.setForeground(IGc.EColor.GRAY);
    gc.drawRoundRectangle(x, y, totalWidth, TOTALS_HEIGHT, 10, 10);

    // Chips: severity icon + count.
    int cursorX = x + TOTALS_PADDING;
    int iconY = y + (TOTALS_HEIGHT - TOTALS_ICON) / 2;
    int textY = y + (TOTALS_HEIGHT - gc.textExtent("0").y) / 2;
    ClassLoader uiClassLoader = org.apache.hop.ui.hopgui.HopGui.class.getClassLoader();
    for (int i = 0; i < texts.length; i++) {
      try {
        gc.drawImage(
            new org.apache.hop.core.svg.SvgFile(svgs[i], uiClassLoader),
            cursorX,
            iconY,
            TOTALS_ICON,
            TOTALS_ICON,
            iconMagnification,
            0);
      } catch (Exception e) {
        // Best-effort: fall back to a colored swatch if the SVG cannot be rendered.
        gc.setBackground(fallbackColors[i]);
        gc.fillRoundRectangle(cursorX, iconY, TOTALS_ICON, TOTALS_ICON, 4, 4);
      }
      cursorX += TOTALS_ICON + TOTALS_TEXT_GAP;

      gc.setForeground(IGc.EColor.BLACK);
      gc.drawText(texts[i], cursorX, textY, true);
      cursorX += gc.textExtent(texts[i]).x + TOTALS_CHIP_GAP;
    }

    return totalWidth;
  }

  /**
   * The native zoom factor to draw the canvas overlays with, or {@code null} when there is no user
   * interface to draw them in. The painter extension points also run on Hop Server, which renders
   * pipeline and workflow SVG images without a UI: asking {@link PropsUi} for the zoom factor there
   * loads the SWT natives and fails hard, with an UnsatisfiedLinkError when GTK is not installed.
   */
  public static Float overlayZoomFactor() {
    try {
      // peekInstance, not getInstance: the latter builds a HopGui, and building one creates an SWT
      // Shell, which on a server fails outright instead of simply saying "no UI here".
      if (HopGui.peekInstance() == null) {
        return null;
      }
      return (float) PropsUi.getNativeZoomFactor();
    } catch (Exception | Error e) {
      // SWT throws SWTError, which extends Error directly rather than LinkageError, so catching
      // Exception alone let it through, all the way up into the servlet rendering the image.
      LogChannel.GENERAL.logDetailed("No user interface to draw the lint canvas overlay in: " + e);
      return null;
    }
  }

  public static boolean isEnabled() {
    try {
      LinterConfigPlugin config = LinterConfigPlugin.getInstance();
      return config.isLinterEnabled() && config.isShowProblemsBarEnabled();
    } catch (Exception e) {
      return true;
    }
  }

  public static Map<String, List<LintResult>> indexByElementName(
      List<LintResult> results, LintSourceRef.Kind kind) {
    Map<String, List<LintResult>> indexed = new HashMap<>();
    if (results == null || kind == null) {
      return indexed;
    }
    for (LintResult result : results) {
      if (result == null || result.getSource() == null) {
        continue;
      }
      if (result.getSource().getKind() != kind || !result.getSource().hasName()) {
        continue;
      }
      indexed.computeIfAbsent(result.getSource().getName(), key -> new ArrayList<>()).add(result);
    }
    return indexed;
  }

  public static String worstSeverity(Collection<LintResult> results) {
    if (results == null || results.isEmpty()) {
      return null;
    }
    boolean hasError = false;
    boolean hasWarning = false;
    for (LintResult result : results) {
      if (result == null || Utils.isEmpty(result.getSeverity())) {
        continue;
      }
      if ("ERROR".equalsIgnoreCase(result.getSeverity())) {
        hasError = true;
        break;
      }
      if ("WARNING".equalsIgnoreCase(result.getSeverity())) {
        hasWarning = true;
      }
    }
    if (hasError) {
      return "ERROR";
    }
    if (hasWarning) {
      return "WARNING";
    }
    return null;
  }

  public static void drawOverlay(
      IGc gc, int x, int y, int iconSize, boolean selected, String severity, double magnification) {
    if (gc == null || Utils.isEmpty(severity)) {
      return;
    }

    float mag = (float) magnification;

    int lineWidth = selected ? 3 : 2;
    gc.setLineWidth(lineWidth);

    if ("ERROR".equalsIgnoreCase(severity)) {
      gc.setForeground(IGc.EColor.RED);
      gc.drawRoundRectangle(x - 2, y - 2, iconSize + 3, iconSize + 3, 8, 8);
      int badgeX = x + iconSize - 8;
      int badgeY = y - 4;
      try {
        gc.drawImage(IGc.EImage.FAILURE, badgeX, badgeY, mag);
      } catch (Exception ignored) {
        // Icon drawing is best-effort on the canvas overlay
      }
    } else if ("WARNING".equalsIgnoreCase(severity)) {
      gc.setForeground(IGc.EColor.YELLOW);
      gc.drawRoundRectangle(x - 2, y - 2, iconSize + 3, iconSize + 3, 8, 8);
      int badgeX = x + iconSize - 8;
      int badgeY = y - 4;
      try {
        gc.drawImage(IGc.EImage.INFO, badgeX, badgeY, mag);
      } catch (Exception ignored) {
        // Icon drawing is best-effort on the canvas overlay
      }
    }
  }
}
