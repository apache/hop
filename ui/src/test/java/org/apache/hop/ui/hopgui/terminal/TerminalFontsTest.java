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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.awt.Font;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TerminalFontsTest {

  /** The installed fonts of the test, each with the characters it can draw. */
  private final Map<String, String> installed = new LinkedHashMap<>();

  private final AtomicInteger scans = new AtomicInteger();
  private final AtomicInteger repaints = new AtomicInteger();
  private final List<Runnable> pendingScans = new ArrayList<>();

  private final TerminalFonts fonts =
      new TerminalFonts(
          () -> {
            scans.incrementAndGet();
            return installed.keySet().toArray(new String[0]);
          },
          family -> font(family, installed.get(family)),
          pendingScans::add);

  @Test
  void theScaleFollowsTheZoomAndTheFontResolutionOfTheDesktop() {
    assertEquals(1.0f, TerminalFonts.hiDpiScale(100, 96, 1.0));
    // Xft.dpi 192 or a GNOME text scaling factor of 2, with a GDK scale factor of 1
    assertEquals(2.0f, TerminalFonts.hiDpiScale(100, 192, 1.0));
    // GDK_SCALE=2, or GTK4, which reports 96 DPI
    assertEquals(2.0f, TerminalFonts.hiDpiScale(200, 96, 1.0));
    // AWT scales by itself as well
    assertEquals(1.0f, TerminalFonts.hiDpiScale(200, 192, 2.0));
    // AWT scales more than SWT: the font has to shrink, not stay at its size
    assertEquals(0.5f, TerminalFonts.hiDpiScale(100, 96, 2.0));
    // nonsense from the platform does not break the font
    assertEquals(1.0f, TerminalFonts.hiDpiScale(0, 0, 0));
  }

  @Test
  void theFontsAreScannedOffThePaintThreadAndTheTerminalIsRepainted() {
    installed.put("CJK Mono", "中");

    assertEquals(Optional.empty(), fallbackFor("中"), "found on the paint thread");
    assertEquals(0, scans.get());

    runPendingScans();
    assertEquals(1, repaints.get());
    assertEquals("CJK Mono", fallbackFor("中").orElseThrow().getName());
  }

  @Test
  void aClusterIsScannedOnceWhileItsScanIsPending() {
    installed.put("CJK Mono", "中");
    fallbackFor("中");
    fallbackFor("中");
    assertEquals(1, pendingScans.size());
  }

  @Test
  void aClusterNoFontCanDrawDoesNotHideItsBaseCharacter() {
    installed.put("CJK Mono", "中");

    // an ideograph with a variation selector no installed font has
    fallbackFor("中️");
    runPendingScans();
    assertEquals(Optional.empty(), fallbackFor("中️"));

    fallbackFor("中");
    runPendingScans();
    assertEquals("CJK Mono", fallbackFor("中").orElseThrow().getName());
  }

  @Test
  void theFontThatWorkedIsTriedBeforeScanningAgain() {
    installed.put("Other", "x");
    installed.put("CJK Mono", "中文字");
    fallbackFor("中");
    runPendingScans();

    // the next ideograph of the same script is found at once, without a scan
    assertEquals("CJK Mono", fallbackFor("文").orElseThrow().getName());
    assertEquals(1, scans.get());
  }

  @Test
  void aClusterNoFontCanDrawIsScannedOnlyOnce() {
    installed.put("CJK Mono", "中");
    fallbackFor("☃");
    runPendingScans();
    fallbackFor("☃");
    fallbackFor("☃");
    assertTrue(pendingScans.isEmpty());
    assertEquals(1, scans.get());
    assertEquals(0, repaints.get());
  }

  @Test
  void aMonospacedFontIsPreferredOverLogicalAndProportionalFonts() {
    installed.put("Monospaced", "中");
    installed.put("Dialog", "中");
    installed.put("Noto Sans CJK", "中");
    installed.put("Noto Sans Mono CJK", "中");
    assertEquals("Noto Sans Mono CJK", scanFor("中"));

    installed.remove("Noto Sans Mono CJK");
    assertEquals("Noto Sans CJK", scanFor("文中"), "a real font beats a logical one");
  }

  @Test
  void aLogicalFontIsTheLastResort() {
    installed.put("Dialog", "中");
    assertEquals("Dialog", scanFor("中"));
  }

  private String scanFor(String cluster) {
    TerminalFonts fresh =
        new TerminalFonts(
            () -> installed.keySet().toArray(new String[0]),
            family -> font(family, installed.get(family) + cluster),
            Runnable::run);
    char[] text = cluster.toCharArray();
    fresh.fallbackFor(text, 0, text.length, () -> {});
    return fresh.fallbackFor(text, 0, text.length, () -> {}).orElseThrow().getName();
  }

  private Optional<Font> fallbackFor(String cluster) {
    // the cluster sits in the middle of a line, as when the terminal draws it
    char[] line = ("ab" + cluster + "cd").toCharArray();
    return fonts.fallbackFor(line, 2, 2 + cluster.length(), repaints::incrementAndGet);
  }

  private void runPendingScans() {
    List<Runnable> scansToRun = new ArrayList<>(pendingScans);
    pendingScans.clear();
    scansToRun.forEach(Runnable::run);
    assertFalse(scansToRun.isEmpty(), "no scan was started");
  }

  /** A font that draws exactly the given characters. */
  private static Font font(String family, String drawable) {
    return new Font(family, Font.PLAIN, 1) {
      @Override
      public int canDisplayUpTo(String text) {
        for (int i = 0; i < text.length(); i++) {
          if (drawable.indexOf(text.charAt(i)) < 0) {
            return i;
          }
        }
        return -1;
      }
    };
  }
}
