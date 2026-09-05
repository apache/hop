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
package org.apache.hop.lint;

import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.workflow.WorkflowPainterExtension;

@ExtensionPoint(
    id = "WorkflowLintActionPainterExtension",
    description = "Draw lint severity badges on workflow action icons",
    extensionPointId = "WorkflowPainterAction")
public class WorkflowLintActionPainterExtension
    implements IExtensionPoint<WorkflowPainterExtension> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, WorkflowPainterExtension ext) throws HopException {
    if (!LintCanvasOverlayHelper.isEnabled() || ext == null || ext.actionMeta == null) {
      return;
    }

    String filePath = LintPathUtils.normalizePath(ext.workflowMeta.getFilename());
    if (Utils.isEmpty(filePath)) {
      return;
    }

    Map<String, List<LintResult>> byAction =
        LintResultsManager.getInstance().getOverlayIndex(filePath, LintSourceRef.Kind.ACTION);

    String severity = LintCanvasOverlayHelper.worstSeverity(byAction.get(ext.actionMeta.getName()));
    if (severity == null) {
      // Nothing to report. If that is because somebody accepted the findings here, say so rather
      // than leaving the next reader to wonder whether this action was checked at all.
      if (LintCanvasOverlayHelper.isShowingIgnoredMarkers()
          && LintResultsManager.getInstance().isMarkedElement(filePath, ext.actionMeta.getName())) {
        LintCanvasOverlayHelper.drawIgnoredOverlay(
            ext.gc, ext.x1, ext.y1, ext.iconSize, ext.actionMeta.isSelected());
      }
      return;
    }

    LintCanvasOverlayHelper.drawOverlay(
        ext.gc,
        ext.x1,
        ext.y1,
        ext.iconSize,
        ext.actionMeta.isSelected(),
        severity,
        ext.gc.getMagnification());

    int badgeX = ext.x1 + ext.iconSize - 8;
    int badgeY = ext.y1 - 4;
    ext.areaOwners.add(
        new AreaOwner(
            AreaOwner.AreaType.CUSTOM,
            badgeX,
            badgeY,
            ConstUi.SMALL_ICON_SIZE,
            ConstUi.SMALL_ICON_SIZE,
            ext.offset,
            LintCanvasOverlayHelper.AREA_LINT_OVERLAY,
            ext.actionMeta.getName()));
  }
}
