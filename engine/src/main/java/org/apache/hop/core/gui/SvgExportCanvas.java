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

import lombok.Getter;

/**
 * Canvas size for an exported SVG diagram. The rectangle is the area that is actually painted
 * (icons, labels, and the size notes are drawn at), shifted so a margin of the same width sits on
 * every side. Stored locations are not enough: a pipeline placed away from the origin would keep
 * the empty band, and a note is often painted wider than the width stored on it.
 */
public final class SvgExportCanvas {

  /** Icon size used by the SVG painters. */
  public static final int ICON_SIZE = 32;

  /** Gap between an icon and the label under it. Matches the painters. */
  public static final int LABEL_GAP = 5;

  /** Even margin, in output pixels, around the painted diagram. */
  public static final int MARGIN = 16;

  private SvgExportCanvas() {}

  /** Inclusive minimum and exclusive maximum of painted graph coordinates. */
  @Getter
  public static final class Bounds {
    private int minX = Integer.MAX_VALUE;
    private int minY = Integer.MAX_VALUE;
    private int maxX = Integer.MIN_VALUE;
    private int maxY = Integer.MIN_VALUE;

    public void include(int x, int y, int width, int height) {
      if (width < 0) {
        width = 0;
      }
      if (height < 0) {
        height = 0;
      }
      minX = Math.min(minX, x);
      minY = Math.min(minY, y);
      maxX = Math.max(maxX, x + width);
      maxY = Math.max(maxY, y + height);
    }

    public boolean isEmpty() {
      return minX == Integer.MAX_VALUE;
    }
  }

  /**
   * Include an icon and the label centered under it. {@code infoIcon} is the description badge
   * drawn just outside the top-left corner.
   */
  public static void includeNode(
      Bounds bounds, IGc gc, Point location, String name, boolean infoIcon, int iconSize) {
    Point loc = location == null ? new Point(50, 50) : location;
    int padLeft = 1;
    int padTop = 1;
    if (infoIcon) {
      int overhang = iconSize / 4 + 1;
      padLeft = overhang;
      padTop = overhang;
    }
    bounds.include(loc.x - padLeft, loc.y - padTop, iconSize + padLeft + 1, iconSize + padTop + 1);

    gc.setFont(IGc.EFont.GRAPH);
    Point text = gc.textExtent(name == null ? "" : name);
    int nameX = loc.x + iconSize / 2 - text.x / 2;
    int nameY = loc.y + iconSize + LABEL_GAP;
    bounds.include(nameX, nameY, text.x, text.y);
  }

  /** Include a note at the size it is painted, plus the border stroke. */
  public static void includeNote(Bounds bounds, Point location, Point drawnSize) {
    if (location == null || drawnSize == null) {
      return;
    }
    int width = Math.max(0, drawnSize.x);
    int height = Math.max(0, drawnSize.y);
    bounds.include(location.x - 1, location.y - 1, width + 2, height + 2);
  }

  /** Painter offset that places the top-left of {@code bounds} at {@code margin}. */
  public static DPoint offsetFor(Bounds bounds, float magnification, int margin) {
    if (bounds.isEmpty()) {
      return new DPoint(0, 0);
    }
    float mag = magnification > 0f ? magnification : 1f;
    double pad = Math.max(0, margin);
    return new DPoint(pad / mag - bounds.minX, pad / mag - bounds.minY);
  }

  /** Output size of the SVG, in pixels. */
  public static Point canvasSize(Bounds bounds, float magnification, int margin) {
    int pad = Math.max(0, margin);
    if (bounds.isEmpty()) {
      int side = Math.max(1, 2 * pad);
      return new Point(side, side);
    }
    float mag = magnification > 0f ? magnification : 1f;
    int width = (int) Math.ceil((bounds.maxX - bounds.minX) * (double) mag) + 2 * pad;
    int height = (int) Math.ceil((bounds.maxY - bounds.minY) * (double) mag) + 2 * pad;
    return new Point(Math.max(1, width), Math.max(1, height));
  }
}
