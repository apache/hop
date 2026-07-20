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

package org.apache.hop.projects.xp;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.gui.BasePainter;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.util.Defaults;

/**
 * Shared helper that draws optional environment canvas text as a large watermark in the top-right
 * of the pipeline/workflow canvas (under graph content when used from *PainterStart).
 */
final class DrawEnvironmentCanvasTextExtension {

  private DrawEnvironmentCanvasTextExtension() {
    // utility
  }

  static void draw(BasePainter<?, ?> painter, IVariables variables) {
    if (painter == null || variables == null) {
      return;
    }

    String environmentName = variables.getVariable(Defaults.VARIABLE_HOP_ENVIRONMENT_NAME);
    if (StringUtils.isEmpty(environmentName)) {
      return;
    }

    LifecycleEnvironment environment =
        ProjectsConfigSingleton.getConfig().findEnvironment(environmentName);
    if (environment == null || StringUtils.isEmpty(environment.getCanvasText())) {
      return;
    }

    String text = variables.resolve(environment.getCanvasText());
    if (StringUtils.isEmpty(text)) {
      return;
    }

    IGc gc = painter.getGc();
    Point area = painter.getArea();
    if (gc == null || area == null || area.x <= 0 || area.y <= 0) {
      return;
    }

    // PainterStart runs under graph magnification. Draw in screen pixels so the label
    // stays fixed-size while zooming (same approach as the navigation view).
    float mag = gc.getMagnification();
    gc.setTransform(0.0f, 0.0f, 1.0f);

    try {
      gc.setFont("Sans", 48, true, false);
      Point extent = gc.textExtent(text);

      int margin = 16;
      int x = area.x - extent.x - margin;
      int y = margin;

      int oldAlpha = gc.getAlpha();
      gc.setAlpha(100);
      gc.setForeground(IGc.EColor.DARKGRAY);
      gc.drawText(text, x, y, true);
      gc.setAlpha(oldAlpha);
    } finally {
      // Restore graph magnification for the rest of the painter
      gc.setTransform(0.0f, 0.0f, mag);
    }
  }
}
