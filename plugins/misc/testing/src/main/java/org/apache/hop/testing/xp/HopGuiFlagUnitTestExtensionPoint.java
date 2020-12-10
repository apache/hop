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
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;

@ExtensionPoint(
  extensionPointId = "HopGuiPipelineMetaExecutionStart",
  id = "HopGuiFlagUnitTestExtensionPoint",
  description = "Change the pipeline variables prior to execution but only in HopGui"
)
/**
 * Sets the __UnitTest_Run__ and __UnitTest_Name__ variables
 * in the variables of the Hop GUI pipeline graph.
 *
 * These can then be picked up later by the other XP plugins.
 */
public class HopGuiFlagUnitTestExtensionPoint implements IExtensionPoint<PipelineMeta> {

  @Override
  public void callExtensionPoint( ILogChannel log, IVariables variables, PipelineMeta pipelineMeta ) throws HopException {

    PipelineUnitTest unitTest = TestingGuiPlugin.getCurrentUnitTest( pipelineMeta );
    if ( unitTest == null ) {
      return;
    }

    // Look up the variables of the current active pipeline graph...
    //
    HopGuiPipelineGraph activePipelineGraph = HopGui.getActivePipelineGraph();
    if (activePipelineGraph==null) {
      return;
    }

    String unitTestName = unitTest.getName();

    if ( !StringUtil.isEmpty( unitTestName ) ) {
      // We're running in HopGui and there's a unit test selected : test it
      //
      variables.setVariable( DataSetConst.VAR_RUN_UNIT_TEST, "Y" );
      variables.setVariable( DataSetConst.VAR_UNIT_TEST_NAME, unitTestName );
    }
  }

}
