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
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;

@ExtensionPoint(
  extensionPointId = "HopGuiPipelineBeforeStart",
  id = "HopGuiFlagPipelineForUnitTestExtensionPoint",
  description = "Change the pipeline variables prior to execution but only in HopGui"
)
/**
 * Pick up the __UnitTest_Run__ and __UnitTest_Name__ variables
 * in the variables of the Hop GUI pipeline graph.  Set them in the pipeline
 *
 * These can then be picked up later by the other XP plugins.
 */
public class HopGuiFlagPipelineForUnitTestExtensionPoint implements IExtensionPoint<IPipelineEngine> {

  @Override
  public void callExtensionPoint( ILogChannel log, IVariables variables, IPipelineEngine pipeline ) throws HopException {

    PipelineUnitTest unitTest = TestingGuiPlugin.getCurrentUnitTest( pipeline.getPipelineMeta() );
    if ( unitTest == null ) {
      return;
    }

    String unitTestName = unitTest.getName();

    if ( !StringUtil.isEmpty( unitTestName ) ) {
      // We found the variables in the GUI and pass them to the pipeline right before (prepare) execution
      //
      pipeline.setVariable( DataSetConst.VAR_RUN_UNIT_TEST, variables.getVariable(DataSetConst.VAR_RUN_UNIT_TEST) );
      pipeline.setVariable( DataSetConst.VAR_UNIT_TEST_NAME, variables.getVariable( DataSetConst.VAR_UNIT_TEST_NAME ) );
    }
  }

}
