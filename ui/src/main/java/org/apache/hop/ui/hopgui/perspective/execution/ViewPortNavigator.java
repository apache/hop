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

import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;

/**
 * Hit-testing and offset math for the canvas navigation minimap (viewport). Used by pipeline,
 * workflow, and execution viewers so dragging the blue overlay (or clicking the minimap) moves the
 * visible graph area.
 */
public final class ViewPortNavigator {

  private ViewPortNavigator() {}

  /**
   * True when {@code click} is on the minimap frame or the visible-area overlay.
   *
   * @param graphPort minimap rectangle in canvas pixels, may be null
   * @param viewPort visible-area overlay in canvas pixels, may be null
   * @param click canvas pixel location
   * @return true when the click is on the navigation view
   */
  public static boolean hitMinimap(Rectangle graphPort, Rectangle viewPort, Point click) {
    if (click == null) {
      return false;
    }
    if (graphPort != null && graphPort.contains(click)) {
      return true;
    }
    return viewPort != null && viewPort.contains(click);
  }

  /**
   * Center of the visible-area overlay, used as the drag origin when jumping the view to a minimap
   * click outside the overlay.
   *
   * @param viewPort visible-area overlay in canvas pixels
   * @return center point, or null when the overlay is missing
   */
  public static Point viewPortCenter(Rectangle viewPort) {
    if (viewPort == null) {
      return null;
    }
    return new Point(viewPort.x + viewPort.width / 2, viewPort.y + viewPort.height / 2);
  }

  /**
   * Convert a pixel drag of the visible-area overlay into a new graph offset.
   *
   * <p>The overlay size in pixels is {@code visibleSizeGraph * scale}. Moving the overlay right
   * reveals content further right, which decreases {@code offset.x}.
   *
   * @param baseOffset graph offset when the drag started
   * @param viewPort visible-area overlay in canvas pixels
   * @param start drag start in canvas pixels
   * @param current current pointer in canvas pixels
   * @param visibleWidthGraph visible canvas width in graph coordinates
   * @param visibleHeightGraph visible canvas height in graph coordinates
   * @return new graph offset, or a copy of {@code baseOffset} when the drag cannot be applied
   */
  public static DPoint dragOffset(
      DPoint baseOffset,
      Rectangle viewPort,
      Point start,
      Point current,
      double visibleWidthGraph,
      double visibleHeightGraph) {
    if (baseOffset == null) {
      return null;
    }
    if (viewPort == null
        || start == null
        || current == null
        || viewPort.width <= 0
        || viewPort.height <= 0
        || visibleWidthGraph <= 0
        || visibleHeightGraph <= 0) {
      return new DPoint(baseOffset);
    }
    double scaleX = viewPort.width / visibleWidthGraph;
    double scaleY = viewPort.height / visibleHeightGraph;
    if (scaleX == 0 || scaleY == 0) {
      return new DPoint(baseOffset);
    }
    double deltaGraphX = (current.x - start.x) / scaleX;
    double deltaGraphY = (current.y - start.y) / scaleY;
    return new DPoint(baseOffset.x - deltaGraphX, baseOffset.y - deltaGraphY);
  }
}
