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

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Region;
import org.eclipse.swt.widgets.Shell;

/**
 * Clips a {@code SWT.NO_TRIM} shell to a rounded rectangle. Desktop SWT only: Hop Web (RAP) has no
 * {@link Region}, which is why this lives in a class of its own that is only loaded on the desktop.
 */
final class RoundedShellRegion {

  /** Points per corner arc: enough for a radius of a dozen pixels or so. */
  private static final int ARC_STEPS = 8;

  private static final String DATA_DISPOSE_HOOK = "RoundedShellRegion.disposeHook";

  private RoundedShellRegion() {}

  /**
   * Give the shell a rounded outline, disposing the previous region when there was one.
   *
   * @param shell the shell to clip
   * @param width the shell width
   * @param height the shell height
   * @param radius the corner radius in pixels
   */
  static void apply(Shell shell, int width, int height, int radius) {
    if (shell == null || shell.isDisposed() || width <= 0 || height <= 0) {
      return;
    }
    Region previous = shell.getRegion();
    Region region = new Region(shell.getDisplay());
    region.add(roundedRectangle(width, height, Math.min(radius, Math.min(width, height) / 2)));
    shell.setRegion(region);
    if (previous != null && !previous.isDisposed()) {
      previous.dispose();
    }
    // Dispose whatever region the shell holds when the shell goes: hooked once per shell.
    if (shell.getData(DATA_DISPOSE_HOOK) == null) {
      shell.setData(DATA_DISPOSE_HOOK, Boolean.TRUE);
      shell.addListener(
          SWT.Dispose,
          event -> {
            Region current = shell.getRegion();
            if (current != null && !current.isDisposed()) {
              current.dispose();
            }
          });
    }
  }

  /** The outline as a polygon: four quarter arcs, walked clockwise from the top-left corner. */
  static int[] roundedRectangle(int width, int height, int radius) {
    if (radius <= 0) {
      return new int[] {0, 0, width, 0, width, height, 0, height};
    }
    int[] points = new int[4 * (ARC_STEPS + 1) * 2];
    int i = 0;
    // centre of each corner arc and the start angle of that arc, clockwise from top-left
    int[][] corners = {
      {radius, radius, 180},
      {width - radius, radius, 270},
      {width - radius, height - radius, 0},
      {radius, height - radius, 90}
    };
    for (int[] corner : corners) {
      for (int step = 0; step <= ARC_STEPS; step++) {
        double angle = Math.toRadians(corner[2] + 90.0 * step / ARC_STEPS);
        points[i++] = (int) Math.round(corner[0] + radius * Math.cos(angle));
        points[i++] = (int) Math.round(corner[1] + radius * Math.sin(angle));
      }
    }
    return points;
  }
}
