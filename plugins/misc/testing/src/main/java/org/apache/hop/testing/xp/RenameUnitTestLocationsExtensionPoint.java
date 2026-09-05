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

package org.apache.hop.testing.xp;

import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.TransformNameChange;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;

@ExtensionPoint(
    id = "RenameUnitTestLocationsExtensionPoint",
    extensionPointId = "PipelineTransformRenamed",
    description =
        "Keep unit test input/golden data set locations and tweaks in sync when a transform is renamed")
public class RenameUnitTestLocationsExtensionPoint implements IExtensionPoint<TransformNameChange> {

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, TransformNameChange change)
      throws HopException {
    if (change == null || change.getPipelineMeta() == null) {
      return;
    }

    HopGuiPipelineGraph pipelineGraph = TestingGuiPlugin.getPipelineGraph(change.getPipelineMeta());
    if (pipelineGraph == null) {
      return;
    }
    Map<String, Object> stateMap = pipelineGraph.getStateMap();
    if (stateMap == null) {
      return;
    }
    PipelineUnitTest unitTest =
        (PipelineUnitTest) stateMap.get(DataSetConst.STATE_KEY_ACTIVE_UNIT_TEST);
    if (unitTest == null) {
      return;
    }
    if (!unitTest.renameTransform(change.getOldName(), change.getNewName())) {
      return;
    }

    try {
      HopGui hopGui = HopGui.getInstance();
      if (hopGui == null || hopGui.getMetadataProvider() == null) {
        return;
      }
      IVariables graphVariables = pipelineGraph.getVariables();
      unitTest.setRelativeFilename(graphVariables, change.getPipelineMeta().getFilename());
      hopGui.getMetadataProvider().getSerializer(PipelineUnitTest.class).save(unitTest);
    } catch (Exception e) {
      log.logError(
          "Error saving unit test '"
              + unitTest.getName()
              + "' after renaming transform '"
              + change.getOldName()
              + "' to '"
              + change.getNewName()
              + "'",
          e);
    }
  }
}
