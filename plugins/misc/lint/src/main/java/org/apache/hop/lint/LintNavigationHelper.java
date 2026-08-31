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

import org.apache.hop.core.util.Utils;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.eclipse.swt.widgets.Display;

/** Navigates from lint results to the relevant open editor element. */
public final class LintNavigationHelper {

  private LintNavigationHelper() {}

  public static void navigateTo(LintResult result) {
    if (result == null || Utils.isEmpty(result.getFileName())) {
      return;
    }

    HopGui hopGui = HopGui.getInstance();
    if (hopGui == null) {
      return;
    }

    Runnable navigate =
        () -> {
          try {
            String fileName = LintPathUtils.normalizePath(result.getFileName());
            IHopFileTypeHandler handler = hopGui.fileDelegate.fileOpen(fileName);
            if (handler == null) {
              return;
            }

            LintSourceRef source = result.getSource();
            if (source == null || !source.hasName()) {
              return;
            }

            if (handler instanceof HopGuiPipelineGraph
                && source.getKind() == LintSourceRef.Kind.TRANSFORM) {
              HopGuiPipelineGraph graph = (HopGuiPipelineGraph) handler;
              PipelineMeta pipelineMeta = graph.getPipelineMeta();
              if (pipelineMeta != null) {
                TransformMeta transform = pipelineMeta.findTransform(source.getName());
                if (transform != null) {
                  graph.setCurrentTransform(transform);
                  graph.redraw();
                }
              }
            } else if (handler instanceof HopGuiWorkflowGraph
                && source.getKind() == LintSourceRef.Kind.ACTION) {
              // Workflow editor has no setCurrentAction API; opening the file is enough for now.
            }
          } catch (Exception e) {
            org.apache.hop.core.logging.LogChannel.GENERAL.logError(
                "Failed to navigate to lint result: " + e.getMessage(), e);
          }
        };

    Display display = hopGui.getDisplay();
    if (display != null) {
      display.asyncExec(navigate);
    } else {
      navigate.run();
    }
  }
}
