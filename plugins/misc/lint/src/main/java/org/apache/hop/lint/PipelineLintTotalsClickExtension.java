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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.extension.HopGuiPipelineGraphExtension;

/** Opens the lint results when the lint totals canvas overlay is clicked on a pipeline. */
@ExtensionPoint(
    id = "PipelineLintTotalsClickExtension",
    extensionPointId = "PipelineGraphMouseDown",
    description = "Shows lint results when the pipeline lint totals overlay is clicked")
public class PipelineLintTotalsClickExtension
    implements IExtensionPoint<HopGuiPipelineGraphExtension> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, HopGuiPipelineGraphExtension ext) throws HopException {
    if (ext == null || ext.getEvent() == null) {
      return;
    }
    try {
      HopGuiPipelineGraph graph = ext.getPipelineGraph();
      if (graph == null || graph.getPipelineMeta() == null) {
        return;
      }
      PipelineMeta meta = graph.getPipelineMeta();
      String filePath = LintPathUtils.normalizePath(meta.getFilename());
      if (LintCanvasOverlayHelper.totalsRectContains(
          filePath, ext.getEvent().x, ext.getEvent().y)) {
        ext.setPreventingDefault(true);
        LintResultsUi.showResultsForFile(filePath);
      }
    } catch (Exception e) {
      log.logError("Error handling lint totals overlay click: " + e.getMessage(), e);
    }
  }
}
