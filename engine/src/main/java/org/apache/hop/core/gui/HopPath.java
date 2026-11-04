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
 * The geometry of a hop on the canvas: a straight line or a curve, stored as a polyline. The
 * painters use it to draw the hop and place the arrow and hop icons; the graphs use it to find the
 * hop under the mouse.
 *
 * <p>A curved hop leaves its source icon on the side that faces the target and enters the target on
 * the opposite side: left or right when the icons are further apart horizontally than vertically,
 * top or bottom otherwise.
 */
public final class HopPath {

  /** Number of straight segments used to approximate a curve. */
  private static final int CURVE_SEGMENTS = 32;

  /** Smallest distance between an end point and its Bézier control point. */
  private static final double MIN_CONTROL_DISTANCE = 20.0;

  /** Largest distance between an end point and its Bézier control point. */
  private static final double MAX_CONTROL_DISTANCE = 150.0;

  private final double[] xs;
  private final double[] ys;

  /** Cumulative length of the polyline at each point. */
  private final double[] lengths;

  @Getter private final boolean curved;

  /** For a curve: start, first control point, second control point, end. Null for a line. */
  private final double[] bezier;

  private HopPath(double[] xs, double[] ys, double[] bezier) {
    this.xs = xs;
    this.ys = ys;
    this.bezier = bezier;
    this.curved = bezier != null;
    this.lengths = new double[xs.length];
    for (int i = 1; i < xs.length; i++) {
      lengths[i] = lengths[i - 1] + Math.hypot(xs[i] - xs[i - 1], ys[i] - ys[i - 1]);
    }
  }

  /** A straight line from (x1,y1) to (x2,y2). */
  public static HopPath straight(int x1, int y1, int x2, int y2) {
    return new HopPath(new double[] {x1, x2}, new double[] {y1, y2}, null);
  }

  /**
   * Build the path of a hop between two points.
   *
   * @param shape the configured line shape
   * @param x1 x of the source: the icon center when {@code startIsIcon}, otherwise a free point
   * @param y1 y of the source
   * @param x2 x of the target: the icon center when {@code endIsIcon}, otherwise a free point
   * @param y2 y of the target
   * @param iconSize the icon size, used to start and end a curve on the icon border
   * @param startIsIcon true when (x1,y1) is the center of an icon
   * @param endIsIcon true when (x2,y2) is the center of an icon
   */
  public static HopPath create(
      HopLineShape shape,
      int x1,
      int y1,
      int x2,
      int y2,
      int iconSize,
      boolean startIsIcon,
      boolean endIsIcon) {
    if (shape != HopLineShape.CURVED) {
      return straight(x1, y1, x2, y2);
    }
    return curved(x1, y1, x2, y2, iconSize, startIsIcon, endIsIcon);
  }

  /** A curve between two icon centers (or free points), see {@link #create}. */
  public static HopPath curved(
      int x1, int y1, int x2, int y2, int iconSize, boolean startIsIcon, boolean endIsIcon) {
    double half = iconSize / 2.0;
    int dx = x2 - x1;
    int dy = y2 - y1;

    // Direction in which the hop leaves the source and enters the target.
    //
    double dirX;
    double dirY;
    if (Math.abs(dx) >= Math.abs(dy)) {
      dirX = dx >= 0 ? 1 : -1;
      dirY = 0;
    } else {
      dirX = 0;
      dirY = dy >= 0 ? 1 : -1;
    }

    double sx = startIsIcon ? x1 + dirX * half : x1;
    double sy = startIsIcon ? y1 + dirY * half : y1;
    double ex = endIsIcon ? x2 - dirX * half : x2;
    double ey = endIsIcon ? y2 - dirY * half : y2;

    double control =
        Math.clamp(Math.hypot(ex - sx, ey - sy) / 2.0, MIN_CONTROL_DISTANCE, MAX_CONTROL_DISTANCE);
    double c1x = sx + dirX * control;
    double c1y = sy + dirY * control;
    double c2x = ex - dirX * control;
    double c2y = ey - dirY * control;

    double[] xs = new double[CURVE_SEGMENTS + 1];
    double[] ys = new double[CURVE_SEGMENTS + 1];
    for (int i = 0; i <= CURVE_SEGMENTS; i++) {
      double t = (double) i / CURVE_SEGMENTS;
      double u = 1 - t;
      double a = u * u * u;
      double b = 3 * u * u * t;
      double c = 3 * u * t * t;
      double d = t * t * t;
      xs[i] = a * sx + b * c1x + c * c2x + d * ex;
      ys[i] = a * sy + b * c1y + c * c2y + d * ey;
    }
    return new HopPath(xs, ys, new double[] {sx, sy, c1x, c1y, c2x, c2y, ex, ey});
  }

  /** Total length of the path. */
  public double getLength() {
    return lengths[lengths.length - 1];
  }

  /** Draw the path with the current foreground color, line width and line style. */
  public void draw(IGc gc) {
    if (curved) {
      gc.drawCubicCurve(
          bezier[0], bezier[1], bezier[2], bezier[3], bezier[4], bezier[5], bezier[6], bezier[7]);
    } else {
      gc.drawLine(
          (int) Math.round(xs[0]),
          (int) Math.round(ys[0]),
          (int) Math.round(xs[1]),
          (int) Math.round(ys[1]));
    }
  }

  /**
   * The point at a fraction of the path length. For a straight line this is plain interpolation
   * between the two end points.
   *
   * @param fraction 0 is the start, 1 is the end
   * @return {x, y}
   */
  public double[] pointAt(double fraction) {
    if (!curved) {
      return new double[] {xs[0] + fraction * (xs[1] - xs[0]), ys[0] + fraction * (ys[1] - ys[0])};
    }
    double target = Math.clamp(fraction, 0.0, 1.0) * getLength();
    int i = segmentAt(target);
    double segmentLength = lengths[i + 1] - lengths[i];
    double f = segmentLength <= 0 ? 0 : (target - lengths[i]) / segmentLength;
    return new double[] {xs[i] + f * (xs[i + 1] - xs[i]), ys[i] + f * (ys[i + 1] - ys[i])};
  }

  /**
   * The direction of the path at a fraction of its length.
   *
   * @return the angle in radians, as returned by {@link Math#atan2}
   */
  public double angleAt(double fraction) {
    if (!curved) {
      return Math.atan2(ys[1] - ys[0], xs[1] - xs[0]);
    }
    int i = segmentAt(Math.clamp(fraction, 0.0, 1.0) * getLength());
    return Math.atan2(ys[i + 1] - ys[i], xs[i + 1] - xs[i]);
  }

  /** The shortest distance from (x,y) to the path. */
  public double distanceTo(double x, double y) {
    double best = Double.MAX_VALUE;
    for (int i = 0; i < xs.length - 1; i++) {
      best = Math.min(best, distanceToSegment(x, y, xs[i], ys[i], xs[i + 1], ys[i + 1]));
    }
    return best;
  }

  private int segmentAt(double length) {
    for (int i = 0; i < xs.length - 2; i++) {
      if (length <= lengths[i + 1]) {
        return i;
      }
    }
    return xs.length - 2;
  }

  private static double distanceToSegment(
      double px, double py, double ax, double ay, double bx, double by) {
    double vx = bx - ax;
    double vy = by - ay;
    double lengthSquared = vx * vx + vy * vy;
    double t = lengthSquared <= 0 ? 0 : ((px - ax) * vx + (py - ay) * vy) / lengthSquared;
    t = Math.clamp(t, 0.0, 1.0);
    return Math.hypot(px - (ax + t * vx), py - (ay + t * vy));
  }
}
