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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.DPoint;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.WorkflowPainter;

/**
 * Draws the lint error/warning/info totals as a fixed overlay in the top-left of the workflow
 * canvas.
 */
@ExtensionPoint(
    id = "WorkflowLintTotalsPainterExtension",
    extensionPointId = "WorkflowPainterEnd",
    description = "Draws lint totals as a workflow canvas overlay")
public class WorkflowLintTotalsPainterExtension implements IExtensionPoint<WorkflowPainter> {

  private static final int SCREEN_X = 10;
  private static final int SCREEN_Y = 10;

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, WorkflowPainter painter)
      throws HopException {
    if (!LintCanvasOverlayHelper.isEnabled() || painter == null) {
      return;
    }
    try {
      WorkflowMeta workflowMeta = painter.getWorkflowMeta();
      if (workflowMeta == null) {
        return;
      }
      String filePath = LintPathUtils.normalizePath(workflowMeta.getFilename());
      if (filePath == null || filePath.isEmpty()) {
        return;
      }

      List<LintResult> results = LintResultsManager.getInstance().getResultsForFile(filePath);
      int[] counts = LintCanvasOverlayHelper.countSeverities(results);

      IGc gc = painter.getGc();
      DPoint offset = painter.getOffset();
      if (gc == null || offset == null) {
        return;
      }

      float nativeZoom = (float) PropsUi.getNativeZoomFactor();

      gc.setTransform(0f, 0f, nativeZoom);
      int width =
          LintCanvasOverlayHelper.drawTotalsOverlay(
              gc, SCREEN_X, SCREEN_Y, counts[0], counts[1], counts[2], nativeZoom);
      gc.setTransform((float) offset.x, (float) offset.y, painter.getMagnification());

      LintCanvasOverlayHelper.rememberTotalsRect(
          filePath, SCREEN_X, SCREEN_Y, width, LintCanvasOverlayHelper.TOTALS_HEIGHT);
    } catch (Exception e) {
      log.logError("Error drawing workflow lint totals overlay: " + e.getMessage(), e);
    }
  }
}
