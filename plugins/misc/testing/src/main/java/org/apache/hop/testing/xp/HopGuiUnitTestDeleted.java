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

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.testing.util.UnitTestGraphVariables;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;

@ExtensionPoint(
    id = "HopGuiUnitTestDeleted",
    extensionPointId = "HopGuiMetadataObjectDeleted",
    description = "When HopGui deletes a pipeline unit test metadata object")
public class HopGuiUnitTestDeleted implements IExtensionPoint {

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Object object)
      throws HopException {
    if (!(object instanceof PipelineUnitTest unitTest)) {
      return;
    }

    TestingGuiPlugin.refreshUnitTestsList();

    // If the deleted unit test was active, clear its design-time variables from the graph.
    //
    if (Utils.isEmpty(unitTest.getName()) || HopGui.getInstance() == null) {
      return;
    }
    for (TabItemHandler item : HopGui.getExplorerPerspective().getItems()) {
      if (!(item.getTypeHandler() instanceof HopGuiPipelineGraph pipelineGraph)) {
        continue;
      }
      Object subject = pipelineGraph.getSubject();
      if (!(subject instanceof PipelineMeta pipelineMeta)) {
        continue;
      }
      PipelineUnitTest active = TestingGuiPlugin.getCurrentUnitTest(pipelineMeta);
      if (active != null && unitTest.getName().equals(active.getName())) {
        UnitTestGraphVariables.clear(pipelineGraph.getVariables(), pipelineGraph.getStateMap());
        pipelineGraph.getStateMap().remove(DataSetConst.STATE_KEY_ACTIVE_UNIT_TEST);
      }
    }
  }
}
