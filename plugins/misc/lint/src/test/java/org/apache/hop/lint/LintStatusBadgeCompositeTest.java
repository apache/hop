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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.eclipse.swt.graphics.ImageData;
import org.eclipse.swt.graphics.PaletteData;
import org.eclipse.swt.graphics.RGB;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The Explorer badges a file's own icon with its lint status. RWT cannot draw onto an Image - a
 * {@code GC} built on one has neither a device nor a drawing delegate - so the badge is composited
 * from the {@link ImageData} of both icons, which needs no drawing surface and works the same on
 * the desktop and in Hop Web.
 */
class LintStatusBadgeCompositeTest {

  private static final RGB BASE_COLOR = new RGB(10, 20, 30);
  private static final RGB BADGE_COLOR = new RGB(200, 100, 50);

  /** A 16x16 icon of one colour, opaque, with an alpha channel - what an SVG icon loads as. */
  private static ImageData icon(int size, RGB color, int alpha) {
    ImageData data = new ImageData(size, size, 24, new PaletteData(0xFF0000, 0xFF00, 0xFF));
    data.alphaData = new byte[size * size];
    for (int y = 0; y < size; y++) {
      for (int x = 0; x < size; x++) {
        data.setPixel(x, y, data.palette.getPixel(color));
        data.setAlpha(x, y, alpha);
      }
    }
    return data;
  }

  private static RGB colorAt(ImageData data, int x, int y) {
    return data.palette.getRGB(data.getPixel(x, y));
  }

  @Test
  @DisplayName("the badge lands in the bottom right corner and leaves the rest of the icon alone")
  void badgeIsCompositedIntoTheCorner() {
    ImageData composite =
        LintStatusFilePainter.withBadge(icon(16, BASE_COLOR, 255), icon(8, BADGE_COLOR, 255), 8, 1);

    assertEquals(16, composite.width);
    assertEquals(16, composite.height);
    // One pixel of margin is kept, so the very last column and row stay the base icon.
    assertEquals(BADGE_COLOR, colorAt(composite, 14, 14));
    assertEquals(BASE_COLOR, colorAt(composite, 15, 15));
    assertEquals(BASE_COLOR, colorAt(composite, 0, 0));
    assertEquals(BASE_COLOR, colorAt(composite, 6, 6));
  }

  @Test
  @DisplayName("what the badge leaves transparent shows the icon underneath")
  void fullyTransparentBadgePixelsChangeNothing() {
    ImageData composite =
        LintStatusFilePainter.withBadge(icon(16, BASE_COLOR, 255), icon(8, BADGE_COLOR, 0), 8, 1);

    assertEquals(BASE_COLOR, colorAt(composite, 14, 14));
    assertEquals(255, composite.getAlpha(14, 14));
  }

  @Test
  @DisplayName("what the icon leaves transparent stays transparent")
  void theIconsOwnTransparencySurvives() {
    ImageData composite =
        LintStatusFilePainter.withBadge(icon(16, BASE_COLOR, 0), icon(8, BADGE_COLOR, 255), 8, 1);

    assertEquals(0, composite.getAlpha(0, 0));
    // Where the badge is opaque it wins outright, transparent base or not.
    assertEquals(255, composite.getAlpha(14, 14));
    assertEquals(BADGE_COLOR, colorAt(composite, 14, 14));
  }

  /** Palette icons say "transparent" with a pixel value rather than an alpha channel. */
  @Test
  @DisplayName("a palette icon's transparent pixel is read as transparent, not as its colour")
  void transparentPixelIsHonoured() {
    PaletteData palette = new PaletteData(BASE_COLOR, BADGE_COLOR);
    ImageData base = new ImageData(16, 16, 8, palette);
    base.transparentPixel = 1;
    for (int y = 0; y < 16; y++) {
      for (int x = 0; x < 16; x++) {
        base.setPixel(x, y, x == 0 && y == 0 ? 1 : 0);
      }
    }

    ImageData composite = LintStatusFilePainter.withBadge(base, icon(8, BADGE_COLOR, 255), 8, 1);

    assertEquals(0, composite.getAlpha(0, 0));
    assertEquals(255, composite.getAlpha(1, 1));
    assertEquals(BASE_COLOR, colorAt(composite, 1, 1));
  }

  /**
   * The platform asks for the composite again at each zoom it paints at, so the same badge is built
   * from bigger pixels rather than by scaling up the 100% one - that is what keeps a HiDPI icon
   * sharp. Everything scales together, the margin included, so the badge lands in the same place on
   * the icon at every zoom.
   */
  @Test
  @DisplayName("at 200% everything is twice the size and the badge sits in the same place")
  void geometryScalesWithTheZoom() {
    ImageData at100 =
        LintStatusFilePainter.withBadge(icon(16, BASE_COLOR, 255), icon(8, BADGE_COLOR, 255), 8, 1);
    ImageData at200 =
        LintStatusFilePainter.withBadge(
            icon(32, BASE_COLOR, 255), icon(16, BADGE_COLOR, 255), 16, 2);

    assertEquals(32, at200.width);
    // The badge corner of the 100% icon, at twice the scale, is still the badge corner.
    assertEquals(BADGE_COLOR, colorAt(at100, 7, 7));
    assertEquals(BADGE_COLOR, colorAt(at200, 14, 14));
    // And so is the last pixel before the margin.
    assertEquals(BADGE_COLOR, colorAt(at100, 14, 14));
    assertEquals(BADGE_COLOR, colorAt(at200, 29, 29));
    // The margin itself stays the base icon at both zooms.
    assertEquals(BASE_COLOR, colorAt(at100, 15, 15));
    assertEquals(BASE_COLOR, colorAt(at200, 30, 30));
    assertEquals(BASE_COLOR, colorAt(at200, 31, 31));
  }

  @Test
  @DisplayName("a half transparent badge blends with the icon rather than replacing it")
  void partialAlphaBlends() {
    ImageData composite =
        LintStatusFilePainter.withBadge(
            icon(16, new RGB(0, 0, 0), 255), icon(8, new RGB(255, 255, 255), 128), 8, 1);

    // Half of white over black, opaque either way.
    RGB blended = colorAt(composite, 14, 14);
    assertEquals(128, blended.red);
    assertEquals(128, blended.green);
    assertEquals(128, blended.blue);
    assertEquals(255, composite.getAlpha(14, 14));
  }
}
