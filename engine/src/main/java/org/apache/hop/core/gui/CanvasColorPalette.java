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

import java.awt.Color;

/**
 * Canvas color palette aligned with {@code GuiResource}/{@code PropsUi#contrastColor} so Hop Web
 * SVG renders match the desktop canvas for light ({@code /ui}) and dark ({@code /ui-dark}) themes.
 */
public final class CanvasColorPalette {

  private final Color background;
  private final Color black;
  private final Color white;
  private final Color red;
  private final Color yellow;
  private final Color green;
  private final Color blue;
  private final Color magenta;
  private final Color purple;
  private final Color indigo;
  private final Color gray;
  private final Color lightGray;
  private final Color darkGray;
  private final Color lightBlue;
  private final Color crystal;
  private final Color hopDefault;
  private final Color hopTrue;
  private final Color hopFalse;
  private final Color deprecated;

  private CanvasColorPalette(
      Color background,
      Color black,
      Color white,
      Color red,
      Color yellow,
      Color green,
      Color blue,
      Color magenta,
      Color purple,
      Color indigo,
      Color gray,
      Color lightGray,
      Color darkGray,
      Color lightBlue,
      Color crystal,
      Color hopDefault,
      Color hopTrue,
      Color hopFalse,
      Color deprecated) {
    this.background = background;
    this.black = black;
    this.white = white;
    this.red = red;
    this.yellow = yellow;
    this.green = green;
    this.blue = blue;
    this.magenta = magenta;
    this.purple = purple;
    this.indigo = indigo;
    this.gray = gray;
    this.lightGray = lightGray;
    this.darkGray = darkGray;
    this.lightBlue = lightBlue;
    this.crystal = crystal;
    this.hopDefault = hopDefault;
    this.hopTrue = hopTrue;
    this.hopFalse = hopFalse;
    this.deprecated = deprecated;
  }

  public static CanvasColorPalette forDarkMode(boolean darkMode) {
    return darkMode ? dark() : light();
  }

  /** Light theme palette (matches GuiResource defaults for {@code /ui}). */
  public static CanvasColorPalette light() {
    return new CanvasColorPalette(
        rgb(235, 235, 235),
        rgb(0, 0, 0),
        rgb(254, 254, 254),
        rgb(255, 0, 0),
        rgb(255, 255, 0),
        rgb(0, 255, 0),
        rgb(0, 0, 255),
        rgb(255, 0, 255),
        rgb(128, 0, 128),
        rgb(75, 0, 130),
        rgb(215, 215, 215),
        rgb(225, 225, 225),
        rgb(100, 100, 100),
        rgb(135, 206, 250),
        rgb(61, 99, 128),
        rgb(61, 99, 128),
        rgb(12, 178, 15),
        rgb(255, 165, 0),
        rgb(246, 196, 56));
  }

  /** Dark theme palette (matches PropsUi contrasting colors for {@code /ui-dark}). */
  public static CanvasColorPalette dark() {
    return new CanvasColorPalette(
        rgb(50, 50, 50),
        rgb(255, 255, 255),
        rgb(35, 35, 35),
        rgb(255, 0, 0),
        rgb(255, 255, 0),
        rgb(0, 255, 0),
        rgb(0, 0, 255),
        rgb(255, 0, 255),
        rgb(128, 0, 128),
        rgb(75, 0, 130),
        rgb(100, 100, 100),
        rgb(70, 70, 70),
        rgb(215, 215, 215),
        rgb(135, 206, 250),
        rgb(61, 99, 128),
        rgb(61, 99, 128),
        rgb(12, 178, 15),
        rgb(255, 165, 0),
        rgb(246, 196, 56));
  }

  private static Color rgb(int r, int g, int b) {
    return new Color(r, g, b);
  }

  public Color getBackground() {
    return background;
  }

  public Color getBlack() {
    return black;
  }

  public Color getWhite() {
    return white;
  }

  public Color getRed() {
    return red;
  }

  public Color getYellow() {
    return yellow;
  }

  public Color getGreen() {
    return green;
  }

  public Color getBlue() {
    return blue;
  }

  public Color getMagenta() {
    return magenta;
  }

  public Color getPurple() {
    return purple;
  }

  public Color getIndigo() {
    return indigo;
  }

  public Color getGray() {
    return gray;
  }

  public Color getLightGray() {
    return lightGray;
  }

  public Color getDarkGray() {
    return darkGray;
  }

  public Color getLightBlue() {
    return lightBlue;
  }

  public Color getCrystal() {
    return crystal;
  }

  public Color getHopDefault() {
    return hopDefault;
  }

  public Color getHopTrue() {
    return hopTrue;
  }

  public Color getHopFalse() {
    return hopFalse;
  }

  public Color getDeprecated() {
    return deprecated;
  }
}
