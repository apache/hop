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

import org.apache.hop.core.Const;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.spark.pipeline.SparkTransformPlan;
import org.apache.hop.ui.hopgui.file.shared.HopGuiTooltipExtension;

/**
 * The tooltip of a native Spark badge (how the transform runs, and why) and of a run mode badge.
 */
@ExtensionPoint(
    id = "SparkPlanToolTipExtensionPoint",
    description = "Explain the native Spark badge of a transform",
    extensionPointId = "HopGuiPipelineGraphAreaHover")
public class SparkPlanToolTipExtensionPoint implements IExtensionPoint<HopGuiTooltipExtension> {

  private static final Class<?> PKG = SparkPlanToolTipExtensionPoint.class;

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, HopGuiTooltipExtension ext) {
    if (ext.areaOwner == null) {
      return;
    }
    if (ext.areaOwner.getOwner() instanceof String text
        && text.startsWith(DrawSparkRunModeOnTransformExtensionPoint.AREA_OWNER_PREFIX)) {
      ext.tip.append(text);
      return;
    }
    if (!(ext.areaOwner.getOwner() instanceof SparkPlanBadges.Badge badge)) {
      return;
    }
    SparkTransformPlan plan = badge.plan();
    ext.tip.append(
        BaseMessages.getString(PKG, "SparkPlanBadge." + plan.kind().name() + ".ToolTip"));
    if (plan.reason() != null) {
      ext.tip.append(Const.CR).append(plan.reason());
    }
  }
}
