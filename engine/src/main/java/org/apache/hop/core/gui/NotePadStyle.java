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

package org.apache.hop.core.gui;

import org.apache.hop.core.NotePadType;
import org.apache.hop.core.gui.IGc.EImage;

/**
 * Fixed visual styles for Markdown {@link org.apache.hop.core.NotePadMeta} notes. Colors and icons
 * are system-owned so notes look consistent across platforms; user font/color fields are ignored
 * when Markdown mode is enabled.
 *
 * <p>Light/dark palettes follow the same rules as hop-data-vault {@code DvNoteStyle}: Important and
 * Information use distinct fills per mode; text uses high-contrast black/light so it stays readable
 * on those fills. Uses fixed RGB (no SWT dependency) so SVG export and unit tests work headlessly.
 */
public final class NotePadStyle {

  /** Dark-mode Important fill: dark orange (readable under light text). */
  private static final RgbColor IMPORTANT_BG_DARK = new RgbColor(160, 85, 20);

  /** Light-mode Important fill. */
  private static final RgbColor IMPORTANT_BG_LIGHT = new RgbColor(255, 220, 100);

  /** Dark-mode Information fill: dark gray (readable under light text). */
  private static final RgbColor INFORMATION_BG_DARK = new RgbColor(70, 72, 78);

  /** Light-mode Information fill. */
  private static final RgbColor INFORMATION_BG_LIGHT = new RgbColor(180, 210, 255);

  /** Dark-mode General fill. */
  private static final RgbColor GENERAL_BG_DARK = new RgbColor(55, 55, 58);

  /** Light-mode General fill (demo gray family). */
  private static final RgbColor GENERAL_BG_LIGHT = new RgbColor(230, 230, 230);

  /** Dark-mode Warning fill (PropsUi contrast of light red). */
  private static final RgbColor WARNING_BG_DARK = new RgbColor(120, 65, 65);

  /** Light-mode Warning fill. */
  private static final RgbColor WARNING_BG_LIGHT = new RgbColor(255, 200, 200);

  private NotePadStyle() {}

  public record RgbColor(int red, int green, int blue) {}

  /**
   * Optional process-wide dark-mode hint set by the UI so painters and SVG export can share one
   * source of truth. Defaults to false (light).
   */
  private static volatile boolean darkMode;

  public static void setDarkMode(boolean dark) {
    darkMode = dark;
  }

  public static boolean isDarkMode() {
    return darkMode;
  }

  public static RgbColor backgroundColor(NotePadType type) {
    return backgroundColor(type, darkMode);
  }

  public static RgbColor backgroundColor(NotePadType type, boolean dark) {
    return switch (typeOrGeneral(type)) {
      case GENERAL -> dark ? GENERAL_BG_DARK : GENERAL_BG_LIGHT;
      case IMPORTANT -> dark ? IMPORTANT_BG_DARK : IMPORTANT_BG_LIGHT;
      case WARNING -> dark ? WARNING_BG_DARK : WARNING_BG_LIGHT;
      case INFORMATION -> dark ? INFORMATION_BG_DARK : INFORMATION_BG_LIGHT;
    };
  }

  public static RgbColor borderColor(NotePadType type) {
    return borderColor(type, darkMode);
  }

  public static RgbColor borderColor(NotePadType type, boolean dark) {
    return switch (typeOrGeneral(type)) {
      case GENERAL -> dark ? new RgbColor(160, 160, 160) : new RgbColor(80, 80, 80);
      case IMPORTANT, INFORMATION ->
          dark ? new RgbColor(200, 200, 200) : new RgbColor(255, 255, 255);
      case WARNING -> dark ? new RgbColor(220, 100, 100) : new RgbColor(200, 0, 0);
    };
  }

  /**
   * Text foreground for note body. High-contrast black in light mode / light in dark mode so
   * contrast works on all type fills (same idea as DvNoteStyle#textColor).
   */
  public static RgbColor textColor(NotePadType type) {
    return textColor(type, darkMode);
  }

  public static RgbColor textColor(NotePadType type, boolean dark) {
    return dark ? new RgbColor(240, 240, 240) : new RgbColor(0, 0, 0);
  }

  /** Hyperlink foreground; same contrast rules as body text (DvNoteStyle). */
  public static RgbColor linkColor(NotePadType type) {
    return linkColor(type, darkMode);
  }

  public static RgbColor linkColor(NotePadType type, boolean dark) {
    // Same contrast rules as body text (DvNoteStyle): readable on all type fills
    return textColor(type, dark);
  }

  public static RgbColor codeBackground(NotePadType type) {
    return codeBackground(type, darkMode);
  }

  public static RgbColor codeBackground(NotePadType type, boolean dark) {
    return dark ? new RgbColor(40, 40, 44) : new RgbColor(235, 235, 235);
  }

  public static int borderWidth(NotePadType type, boolean selected) {
    int base =
        switch (typeOrGeneral(type)) {
          case WARNING, IMPORTANT -> 2;
          default -> 1;
        };
    return selected ? base + 1 : base;
  }

  /** Optional accent icon; {@code null} means no icon. */
  public static EImage icon(NotePadType type) {
    return switch (typeOrGeneral(type)) {
      case IMPORTANT -> EImage.INFO;
      case WARNING -> EImage.ERROR;
      case INFORMATION -> EImage.INFO_DISABLED;
      default -> null;
    };
  }

  private static NotePadType typeOrGeneral(NotePadType type) {
    return type == null ? NotePadType.GENERAL : type;
  }
}
