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

package org.apache.hop.spark.gui;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelinePainterExtension;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.spark.util.SparkRunMode;

/**
 * Draws a small badge at the bottom-left of a transform icon when a Spark run-mode override is set
 * (Force distributed / Force Driver Only).
 */
@ExtensionPoint(
    id = "DrawSparkRunModeOnTransformExtensionPoint",
    description =
        "Draw Spark run-mode override badge (bottom-left) on transforms with Force Distributed or Force Driver Only",
    extensionPointId = "PipelinePainterTransform")
public class DrawSparkRunModeOnTransformExtensionPoint
    implements IExtensionPoint<PipelinePainterExtension> {

  public static final String AREA_OWNER_PREFIX = "Spark run mode: ";

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, PipelinePainterExtension ext) {
    try {
      TransformMeta transformMeta = ext.transformMeta;
      if (transformMeta == null || !SparkRunMode.hasForcedOverride(transformMeta)) {
        return;
      }

      String override = SparkRunMode.getOverride(transformMeta);
      String svgName =
          SparkRunMode.OVERRIDE_FORCE_DRIVER_ONLY.equals(override)
              ? "spark-run-driver.svg"
              : "spark-run-distributed.svg";
      String label = SparkRunMode.displayLabelForOverride(override);

      Rectangle r =
          drawBadge(
              ext.gc, ext.x1, ext.y1, ext.iconSize, svgName, this.getClass().getClassLoader());
      ext.areaOwners.add(
          new AreaOwner(
              AreaOwner.AreaType.CUSTOM,
              r.x,
              r.y,
              r.width,
              r.height,
              ext.offset,
              transformMeta,
              AREA_OWNER_PREFIX + label));
    } catch (Exception e) {
      // Not critical for canvas painting
      log.logError("Error drawing Spark run-mode badge", e);
    }
  }

  /** Bottom-left corner of the transform icon (badge overlaps the lower-left of the glyph). */
  static Rectangle drawBadge(
      IGc gc, int x, int y, int iconSize, String svgResource, ClassLoader classLoader)
      throws Exception {
    int imageWidth = 16;
    int imageHeight = 16;
    int locationX = x - (2 * imageWidth) / 3;
    int locationY = y + iconSize - imageHeight / 3;

    gc.drawImage(
        new SvgFile(svgResource, classLoader),
        locationX,
        locationY,
        imageWidth,
        imageHeight,
        gc.getMagnification(),
        0);
    return new Rectangle(locationX, locationY, imageWidth, imageHeight);
  }
}
