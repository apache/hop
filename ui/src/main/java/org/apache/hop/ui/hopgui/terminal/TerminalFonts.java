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

package org.apache.hop.ui.hopgui.terminal;

import java.awt.Font;
import java.awt.GraphicsEnvironment;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;

/** Font size and font fallback of the JediTerm terminal. */
final class TerminalFonts {

  /** The resolution AWT and SWT treat as a scale of 1. */
  static final float BASE_DPI = 96f;

  /** Java's logical fonts: the terminal font is one of them, so they make poor fallbacks. */
  private static final Set<String> LOGICAL_FAMILIES =
      Set.of("dialog", "dialoginput", "monospaced", "serif", "sansserif");

  /** The fonts that worked most recently, tried before any scan. */
  private static final int RECENT_FONTS = 8;

  /** Shared by every terminal tab. */
  static final TerminalFonts INSTANCE =
      new TerminalFonts(
          () -> GraphicsEnvironment.getLocalGraphicsEnvironment().getAvailableFontFamilyNames(),
          family -> new Font(family, Font.PLAIN, 1),
          Executors.newSingleThreadExecutor(
              runnable -> {
                Thread thread = new Thread(runnable, "hop-terminal-font-fallback");
                thread.setDaemon(true);
                return thread;
              }));

  private final Supplier<String[]> families;
  private final Function<String, Font> fontOfFamily;
  private final Executor scanner;

  /** The fallback per grapheme cluster; empty when no installed font can draw the cluster. */
  private final Map<String, Optional<Font>> byCluster = new ConcurrentHashMap<>();

  private final Set<String> scanning = ConcurrentHashMap.newKeySet();
  private final List<Font> recent = new CopyOnWriteArrayList<>();

  TerminalFonts(
      Supplier<String[]> families, Function<String, Font> fontOfFamily, Executor scanner) {
    this.families = families;
    this.fontOfFamily = fontOfFamily;
    this.scanner = scanner;
  }

  /**
   * The factor by which the AWT terminal font has to grow to match the rest of Hop on Linux. SWT
   * follows the desktop scale in two ways: the GDK scale factor (the monitor zoom) and the font
   * resolution of the session ({@code Xft.dpi}, the GNOME text scaling factor), which only shows in
   * the DPI. AWT follows neither, apart from its own scale. GTK4 reports a fixed 96 DPI, so the
   * zoom still counts.
   *
   * @param swtZoom the zoom of the monitor in percent
   * @param swtDpi the horizontal DPI of the SWT display
   * @param awtScale the scale of the AWT graphics configuration
   */
  static float hiDpiScale(int swtZoom, int swtDpi, double awtScale) {
    float swtScale = Math.max(swtZoom / 100f, swtDpi / BASE_DPI);
    if (swtScale <= 0f || Float.isNaN(swtScale)) {
      swtScale = 1f;
    }
    double scale = awtScale > 0 ? awtScale : 1.0;
    return (float) (swtScale / scale);
  }

  /**
   * An installed font that draws the grapheme cluster {@code text[start, end)}, which the terminal
   * font cannot draw. A font that worked for an earlier cluster is tried first: one font usually
   * covers a whole script. Otherwise the installed fonts are scanned in the background, never on
   * the paint thread, and {@code onFound} is called once a font is found, to repaint.
   *
   * @return the font, or empty while the scan runs or when no installed font draws the cluster
   */
  Optional<Font> fallbackFor(char[] text, int start, int end, Runnable onFound) {
    String cluster = new String(text, start, end - start);
    Optional<Font> known = byCluster.get(cluster);
    if (known != null) {
      return known;
    }
    for (Font font : recent) {
      if (font.canDisplayUpTo(cluster) == -1) {
        Optional<Font> found = Optional.of(font);
        byCluster.put(cluster, found);
        return found;
      }
    }
    if (scanning.add(cluster)) {
      scanner.execute(
          () -> {
            try {
              Optional<Font> found = scan(cluster);
              found.ifPresent(this::remember);
              byCluster.put(cluster, found);
              if (found.isPresent()) {
                onFound.run();
              }
            } finally {
              scanning.remove(cluster);
            }
          });
    }
    return Optional.empty();
  }

  /**
   * The first installed font that draws the cluster: a monospaced one if there is one, then any
   * other, and Java's logical fonts last.
   */
  private Optional<Font> scan(String cluster) {
    Font other = null;
    Font logical = null;
    for (String family : families.get()) {
      Font candidate = fontOfFamily.apply(family);
      if (candidate.canDisplayUpTo(cluster) != -1) {
        continue;
      }
      String name = family.toLowerCase(Locale.ROOT);
      if (LOGICAL_FAMILIES.contains(name)) {
        if (logical == null) {
          logical = candidate;
        }
      } else if (name.contains("mono")) {
        return Optional.of(candidate);
      } else if (other == null) {
        other = candidate;
      }
    }
    return Optional.ofNullable(other != null ? other : logical);
  }

  private void remember(Font font) {
    if (recent.contains(font)) {
      return;
    }
    recent.add(0, font);
    while (recent.size() > RECENT_FONTS) {
      recent.remove(recent.size() - 1);
    }
  }
}
