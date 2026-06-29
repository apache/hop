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
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.extension.HopGuiPipelineGraphExtension;

@ExtensionPoint(
    extensionPointId = "PipelineGraphMouseUp",
    id = "LocationMouseUpExtensionPoint",
    description = "Prevent showing context menu when clicking on a data set")
public class LocationMouseUpExtensionPoint
    implements IExtensionPoint<HopGuiPipelineGraphExtension> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, HopGuiPipelineGraphExtension pipelineGraphExtension)
      throws HopException {
    HopGuiPipelineGraph pipelineGraph = pipelineGraphExtension.getPipelineGraph();
    PipelineMeta pipelineMeta = pipelineGraph.getPipelineMeta();

    if (TestingGuiPlugin.getCurrentUnitTest(pipelineMeta) == null) {
      return;
    }

    Point point = pipelineGraphExtension.getPoint();
    AreaOwner areaOwner = pipelineGraph.getVisibleAreaOwner(point.x, point.y);
    if (areaOwner != null && areaOwner.getAreaType() != null) {
      if (DataSetConst.AREA_DRAWN_INPUT_DATA_SET.equals(areaOwner.getParent())
          || DataSetConst.AREA_DRAWN_GOLDEN_DATA_SET.equals(areaOwner.getParent())
          || DataSetConst.AREA_DRAWN_GOLDEN_DATA_RESULT.equals(areaOwner.getParent())) {
        pipelineGraphExtension.setPreventingDefault(true);
      }
    }
  }
}
