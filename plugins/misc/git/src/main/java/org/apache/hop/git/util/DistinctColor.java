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

package org.apache.hop.git.util;

import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.widgets.Display;

public final class DistinctColor {

  private DistinctColor() {
    // Utility class
  }

  // No tint based on the gold ratio: good dispersion, no “rapid” repetition.
  private static final float GOLDEN_RATIO_CONJUGATE = 0.61803398875f;

  public static Color nextColor(Color color) {

    RGB rgb = color.getRGB();

    float[] hsv = rgbToHsv(rgb);

    float h = hsv[0];
    float s = 0.85f;
    float v = 0.90f;

    RGB candidate = null;

    // We try a few steps until we see a real, noticeable difference.
    for (int i = 0; i < 12; i++) {
      h = (h + GOLDEN_RATIO_CONJUGATE) % 1.0f;

      candidate = hsvToRgb(h, s, v);

      if (colorDistance(candidate, rgb) >= (80 * 80)) { // seuil ~ perceptible
        break;
      }
    }

    return new Color(Display.getCurrent(), candidate);
  }

  private static float[] rgbToHsv(RGB rgb) {
    float r = rgb.red / 255f;
    float g = rgb.green / 255f;
    float b = rgb.blue / 255f;

    float max = Math.max(r, Math.max(g, b));
    float min = Math.min(r, Math.min(g, b));
    float delta = max - min;

    float h;
    if (delta == 0f) {
      h = 0f;
    } else if (max == r) {
      h = ((g - b) / delta) % 6f;
    } else if (max == g) {
      h = ((b - r) / delta) + 2f;
    } else {
      h = ((r - g) / delta) + 4f;
    }
    h /= 6f;
    if (h < 0f) h += 1f;

    float s = (max == 0f) ? 0f : (delta / max);
    float v = max;

    return new float[] {h, s, v};
  }

  private static RGB hsvToRgb(float h, float s, float v) {
    h = (h % 1f + 1f) % 1f;
    s = range(s);
    v = range(v);

    float c = v * s;
    float hh = h * 6f;
    float x = c * (1f - Math.abs((hh % 2f) - 1f));
    float m = v - c;

    int sector = (int) hh;
    float r, g, b;
    switch (sector) {
      case 0:
        r = c;
        g = x;
        b = 0f;
        break;
      case 1:
        r = x;
        g = c;
        b = 0f;
        break;
      case 2:
        r = 0f;
        g = c;
        b = x;
        break;
      case 3:
        r = 0f;
        g = x;
        b = c;
        break;
      case 4:
        r = x;
        g = 0f;
        b = c;
        break;
      default: // sector 5 or edge case where hh >= 6
        r = c;
        g = 0f;
        b = x;
        break;
    }

    int red = toByte(m + r);
    int green = toByte(m + g);
    int blue = toByte(m + b);

    return new RGB(red, green, blue);
  }

  private static int toByte(float value) {
    value = range(value);
    return Math.round(value * 255f);
  }

  private static float range(float value) {
    return value < 0f ? 0f : Math.min(value, 1f);
  }

  /** Euclidean distance squared in RGB space (0..255) */
  private static int colorDistance(RGB a, RGB b) {
    int dr = a.red - b.red;
    int dg = a.green - b.green;
    int db = a.blue - b.blue;
    return dr * dr + dg * dg + db * db;
  }
}
