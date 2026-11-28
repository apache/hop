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

package org.apache.hop.spark.gui;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelinePainterExtension;
import org.apache.hop.spark.pipeline.SparkTransformPlan;

/**
 * Designing for the native Spark engine: a badge at the top right of every transform icon saying
 * whether it runs as Spark code, as Hop's transform, in a chain of Hop transforms, or on one
 * stream.
 */
@ExtensionPoint(
    id = "DrawSparkPlanOnTransformExtensionPoint",
    description = "Draw how a transform runs on native Spark (top right of the icon)",
    extensionPointId = "PipelinePainterTransform")
public class DrawSparkPlanOnTransformExtensionPoint
    implements IExtensionPoint<PipelinePainterExtension> {

  private static final int SIZE = 16;

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, PipelinePainterExtension ext) {
    try {
      if (ext.transformMeta == null || !SparkPlanBadges.designingForSpark()) {
        return;
      }
      SparkTransformPlan plan = SparkPlanBadges.plan(ext.pipelineMeta, ext.transformMeta.getName());
      if (plan == null) {
        return;
      }
      int x = ext.x1 + ext.iconSize - (SIZE / 3);
      int y = ext.y1 - (2 * SIZE) / 3;
      ext.gc.drawImage(
          new SvgFile(svg(plan.kind()), getClass().getClassLoader()),
          x,
          y,
          SIZE,
          SIZE,
          ext.gc.getMagnification(),
          0);
      ext.areaOwners.add(
          new AreaOwner(
              AreaOwner.AreaType.CUSTOM,
              x,
              y,
              SIZE,
              SIZE,
              ext.offset,
              ext.transformMeta,
              new SparkPlanBadges.Badge(ext.transformMeta.getName(), plan)));
    } catch (Exception e) {
      // Not critical for canvas painting
      log.logDebug("Error drawing the native Spark badge: " + e.getMessage());
    }
  }

  static String svg(SparkTransformPlan.Kind kind) {
    return switch (kind) {
      case NATIVE -> "spark-plan-native.svg";
      case HOP -> "spark-plan-hop.svg";
      case HOP_FUSED -> "spark-plan-hop-fused.svg";
      case ONE_STREAM -> "spark-plan-one-stream.svg";
      case DRIVER -> "spark-plan-driver.svg";
    };
  }
}
