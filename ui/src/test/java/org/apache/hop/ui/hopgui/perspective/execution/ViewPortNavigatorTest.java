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

package org.apache.hop.ui.hopgui.perspective.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;
import org.junit.jupiter.api.Test;

class ViewPortNavigatorTest {

  @Test
  void hitMinimapDetectsGraphPortAndViewPort() {
    Rectangle graphPort = new Rectangle(800, 400, 200, 150);
    Rectangle viewPort = new Rectangle(820, 420, 40, 30);

    assertTrue(ViewPortNavigator.hitMinimap(graphPort, viewPort, new Point(810, 410)));
    assertTrue(ViewPortNavigator.hitMinimap(graphPort, viewPort, new Point(830, 425)));
    assertFalse(ViewPortNavigator.hitMinimap(graphPort, viewPort, new Point(10, 10)));
    assertFalse(ViewPortNavigator.hitMinimap(graphPort, viewPort, null));
    assertTrue(ViewPortNavigator.hitMinimap(null, viewPort, new Point(830, 425)));
    assertFalse(ViewPortNavigator.hitMinimap(null, null, new Point(830, 425)));
  }

  @Test
  void viewPortCenterIsMidpoint() {
    Rectangle viewPort = new Rectangle(100, 200, 40, 20);
    Point center = ViewPortNavigator.viewPortCenter(viewPort);
    assertEquals(120, center.x);
    assertEquals(210, center.y);
    assertNull(ViewPortNavigator.viewPortCenter(null));
  }

  @Test
  void dragOffsetMovesGraphOppositeTheOverlay() {
    // Overlay is 40px wide and represents 400 graph units (scale 0.1).
    Rectangle viewPort = new Rectangle(820, 420, 40, 20);
    DPoint base = new DPoint(-50.0, -80.0);

    // Drag the overlay 10px right and 5px down → graph offset decreases.
    DPoint moved =
        ViewPortNavigator.dragOffset(
            base, viewPort, new Point(830, 425), new Point(840, 430), 400.0, 200.0);

    assertEquals(-150.0, moved.x, 1e-9);
    assertEquals(-130.0, moved.y, 1e-9);
  }

  @Test
  void dragOffsetIgnoresInvalidInputs() {
    DPoint base = new DPoint(-10.0, -20.0);
    Rectangle viewPort = new Rectangle(0, 0, 10, 10);

    DPoint same =
        ViewPortNavigator.dragOffset(base, viewPort, new Point(0, 0), new Point(5, 5), 0.0, 10.0);
    assertEquals(-10.0, same.x, 1e-9);
    assertEquals(-20.0, same.y, 1e-9);
    assertNull(
        ViewPortNavigator.dragOffset(null, viewPort, new Point(0, 0), new Point(1, 1), 10, 10));
  }
}
