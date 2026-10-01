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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.Const;
import org.junit.jupiter.api.Test;

class HopToolTipTest {

  @Test
  void cleanTextDropsAsciiRulesAndSurroundingBlankLines() {
    String text =
        Const.CR
            + "This is a deprecated transform"
            + Const.CR
            + "-----------------------------------"
            + Const.CR
            + "Use Lookup instead.  "
            + Const.CR;
    assertEquals(
        "This is a deprecated transform" + Const.CR + "Use Lookup instead.",
        HopToolTip.cleanText(text));
  }

  @Test
  void cleanTextKeepsInnerBlankLinesAndDashesInProse() {
    String text = "Hop info" + Const.CR + Const.CR + "Read CSV -> Filter - stage 2";
    assertEquals(text, HopToolTip.cleanText(text));
    assertEquals("", HopToolTip.cleanText(null));
    assertEquals("Selection cleared", HopToolTip.cleanText("\n  Selection cleared \n"));
  }

  @Test
  void roundedRectangleWithoutRadiusIsThePlainRectangle() {
    assertArrayEquals(
        new int[] {0, 0, 40, 0, 40, 20, 0, 20}, RoundedShellRegion.roundedRectangle(40, 20, 0));
  }

  @Test
  void roundedRectangleStaysInsideTheBoundsAndTouchesEveryEdge() {
    int width = 120;
    int height = 30;
    int[] points = RoundedShellRegion.roundedRectangle(width, height, 6);
    int minX = Integer.MAX_VALUE;
    int maxX = Integer.MIN_VALUE;
    int minY = Integer.MAX_VALUE;
    int maxY = Integer.MIN_VALUE;
    for (int i = 0; i < points.length; i += 2) {
      minX = Math.min(minX, points[i]);
      maxX = Math.max(maxX, points[i]);
      minY = Math.min(minY, points[i + 1]);
      maxY = Math.max(maxY, points[i + 1]);
    }
    assertEquals(0, minX);
    assertEquals(width, maxX);
    assertEquals(0, minY);
    assertEquals(height, maxY);
    // the corners themselves are cut off
    assertEquals(false, containsPoint(points, 0, 0));
    assertEquals(false, containsPoint(points, width, height));
  }

  private static boolean containsPoint(int[] points, int x, int y) {
    for (int i = 0; i < points.length; i += 2) {
      if (points[i] == x && points[i + 1] == y) {
        return true;
      }
    }
    return false;
  }
}
