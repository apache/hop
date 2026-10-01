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
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.testing.util.UnitTestAutoOpening;
import org.apache.hop.testing.util.UnitTestGraphVariables;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;

/** Used for create/update/delete of unit test metadata objects */
public class HopGuiUnitTestChanged implements IExtensionPoint {

  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Object object)
      throws HopException {
    // We only respond to pipeline unit test changes
    //
    if (!(object instanceof PipelineUnitTest unitTest)) {
      return;
    }

    HopGui hopGui = HopGui.getInstance();

    // When auto-open is enabled on this test, clear it on other tests for the same pipeline
    // (create OK / metadata editor save).
    if (hopGui != null) {
      UnitTestAutoOpening.enforceExclusiveAutoOpening(
          log, variables, hopGui.getMetadataProvider(), unitTest);
    }

    TestingGuiPlugin.refreshUnitTestsList();

    // If this unit test is active on an open pipeline graph, re-apply its variables for
    // design-time.
    //
    if (Utils.isEmpty(unitTest.getName()) || hopGui == null) {
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
        UnitTestGraphVariables.apply(
            pipelineGraph.getVariables(), unitTest, pipelineGraph.getStateMap());
        // Keep state map pointing at the updated metadata object
        pipelineGraph.getStateMap().put(DataSetConst.STATE_KEY_ACTIVE_UNIT_TEST, unitTest);
      }
    }
  }
}
