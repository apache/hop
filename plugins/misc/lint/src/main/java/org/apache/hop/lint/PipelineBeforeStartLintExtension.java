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
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;

/**
 * When a pipeline starts, leave the lint Problems tab and show the log instead.
 *
 * <p>See {@link ExecutionTabFocusLintExtension} for why: Hop only chooses a tab when none is
 * selected, so an execution panel already sitting on Problems would stay there for the whole run.
 */
@ExtensionPoint(
    id = "PipelineBeforeStartLintExtension",
    extensionPointId = "HopGuiPipelineBeforeStart",
    description = "Moves the execution panel off the lint Problems tab when a pipeline starts")
public class PipelineBeforeStartLintExtension implements IExtensionPoint<Object> {

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Object object)
      throws HopException {
    try {
      if (HopGui.peekInstance() == null) {
        return;
      }
      HopGuiPipelineGraph graph = HopGui.getActivePipelineGraph();
      ExecutionTabFocusLintExtension.restoreLogTab(graph);
    } catch (Exception e) {
      // Choosing a tab is cosmetic: never let it interfere with starting a run.
      log.logDetailed("Could not restore the logging tab on start: " + e.getMessage());
    }
  }
}
