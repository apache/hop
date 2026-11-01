/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.junit.jupiter.api.Test;

class PopupMenuPlacementTest {

  @Test
  void menuBottomSitsOnTheAnchorTop() {
    Rectangle anchor = new Rectangle(400, 700, 80, 24);
    Rectangle monitor = new Rectangle(0, 0, 1920, 1080);

    Point where = PopupMenuPlacement.above(anchor, 200, monitor);

    assertEquals(400, where.x);
    assertEquals(500, where.y);
  }

  @Test
  void tallMenuStaysOnTheAnchorMonitor() {
    // A menu taller than the space above the button would otherwise start on the monitor
    // stacked above this one. That monitor can have a different scale.
    Rectangle anchor = new Rectangle(100, 30, 40, 20);
    Rectangle monitor = new Rectangle(0, 0, 800, 600);

    Point where = PopupMenuPlacement.above(anchor, 200, monitor);

    assertEquals(100, where.x);
    assertEquals(0, where.y);
    assertTrue(monitor.contains(where));
  }

  @Test
  void pointOnTheTaskbarIsPulledInsideTheWorkArea() {
    // The old anchor was the status bar plus a gap. On a maximized window that point is the
    // taskbar, and an SWT rectangle does not contain its bottom edge. Windows then scales the
    // menu with the primary monitor.
    Rectangle monitor = new Rectangle(1920, 0, 2560, 1440);
    Rectangle anchor = new Rectangle(2100, 1440, 120, 28);

    Point where = PopupMenuPlacement.above(anchor, 0, monitor);

    assertEquals(2100, where.x);
    assertEquals(1439, where.y);
    assertTrue(monitor.contains(where));
  }

  @Test
  void secondaryMonitorKeepsItsOwnOrigin() {
    Rectangle monitor = new Rectangle(-1920, 0, 1920, 1080);
    Rectangle anchor = new Rectangle(-400, 1000, 60, 24);

    Point where = PopupMenuPlacement.above(anchor, 80, monitor);

    assertEquals(-400, where.x);
    assertEquals(920, where.y);
    assertTrue(monitor.contains(where));
  }

  @Test
  void noMonitorLeavesThePointWhereTheMenuWouldOpen() {
    Point where = PopupMenuPlacement.above(new Rectangle(5, 8, 1, 1), 20, null);

    assertEquals(5, where.x);
    assertEquals(-12, where.y);
  }

  @Test
  void pixelsToPointsMatchesSwtRounding() {
    assertEquals(25, PopupMenuPlacement.pixelsToPoints(25, 100));
    assertEquals(100, PopupMenuPlacement.pixelsToPoints(150, 150));
    assertEquals(96, PopupMenuPlacement.pixelsToPoints(144, 150));
    assertEquals(0, PopupMenuPlacement.pixelsToPoints(0, 150));
  }
}
