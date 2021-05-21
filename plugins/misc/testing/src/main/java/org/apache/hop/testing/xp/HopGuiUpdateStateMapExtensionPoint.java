/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.testing.xp;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.testing.UnitTestResult;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.ui.hopgui.file.pipeline.extension.HopGuiPipelineFinishedExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ExtensionPoint(
    id = "HopGuiUpdateStateMapExtensionPoint",
    description =
        "Update the state map with unit test results in the Hop GUI pipeline graph so that it can be rendered",
    extensionPointId = "HopGuiPipelineFinished")
public class HopGuiUpdateStateMapExtensionPoint
    implements IExtensionPoint<HopGuiPipelineFinishedExtension> {
  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, HopGuiPipelineFinishedExtension ext)
      throws HopException {

    final List<UnitTestResult> results =
        (List<UnitTestResult>)
            ext.pipeline.getExtensionDataMap().get(DataSetConst.UNIT_TEST_RESULTS);
    if (results != null && !results.isEmpty()) {
      Map<String, Object> stateMap = ext.pipelineGraph.getStateMap();
      if (stateMap == null) {
        stateMap = new HashMap<>();
        ext.pipelineGraph.setStateMap(stateMap);
      }
      Map<String, Boolean> resultsMap = new HashMap<>();
      stateMap.put(DataSetConst.STATE_KEY_GOLDEN_DATASET_RESULTS, resultsMap);

      for (UnitTestResult result : results) {
        if (StringUtils.isNotEmpty(result.getDataSetName())) {
          resultsMap.put(result.getDataSetName(), !result.isError());
        }
      }
    }
  }
}
