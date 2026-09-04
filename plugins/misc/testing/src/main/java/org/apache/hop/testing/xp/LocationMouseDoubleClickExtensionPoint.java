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

import java.util.List;
import java.util.function.Function;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.testing.DataSet;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.PipelineUnitTestSetLocation;
import org.apache.hop.testing.UnitTestResult;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.extension.HopGuiPipelineGraphExtension;
import org.apache.hop.ui.testing.PipelineUnitTestSetLocationDialog;
import org.eclipse.swt.events.MouseEvent;

@ExtensionPoint(
    extensionPointId = "PipelineGraphMouseDown",
    id = "LocationMouseDoubleClickExtensionPoint",
    description = "Open a data set when double clicked on it")
public class LocationMouseDoubleClickExtensionPoint
    implements IExtensionPoint<HopGuiPipelineGraphExtension> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, HopGuiPipelineGraphExtension pipelineGraphExtension)
      throws HopException {
    HopGuiPipelineGraph pipelineGraph = pipelineGraphExtension.getPipelineGraph();
    PipelineMeta pipelineMeta = pipelineGraph.getPipelineMeta();

    PipelineUnitTest unitTest = TestingGuiPlugin.getCurrentUnitTest(pipelineMeta);
    if (unitTest == null) {
      return;
    }

    // This is called for every mouse down on the canvas so figure out first whether one of the
    // unit test markers was clicked on.  Only then do any real work: loading the data sets and
    // determining transform fields is far too expensive to do on every click (issue #8203).
    //
    MouseEvent e = pipelineGraphExtension.getEvent();
    if (e.button != 1 && e.button != 2) {
      return;
    }
    Point point = pipelineGraphExtension.getPoint();
    AreaOwner areaOwner = pipelineGraph.getVisibleAreaOwner(point.x, point.y);
    if (areaOwner == null || areaOwner.getAreaType() == null) {
      return;
    }
    Object area = areaOwner.getParent();
    boolean inputDataSet = DataSetConst.AREA_DRAWN_INPUT_DATA_SET.equals(area);
    boolean goldenDataSet = DataSetConst.AREA_DRAWN_GOLDEN_DATA_SET.equals(area);
    boolean goldenDataResult = DataSetConst.AREA_DRAWN_GOLDEN_DATA_RESULT.equals(area);
    if (!inputDataSet && !goldenDataSet && !goldenDataResult) {
      return;
    }

    HopGui hopGui = HopGui.getInstance();
    try {
      String transformName = (String) areaOwner.getOwner();

      if (goldenDataResult) {
        pipelineGraphExtension.setPreventingDefault(true);
        showGoldenDataResult(pipelineGraph, hopGui, unitTest, transformName);
        return;
      }

      pipelineGraphExtension.setPreventingDefault(true);

      PipelineUnitTestSetLocation location =
          inputDataSet
              ? unitTest.findInputLocation(transformName)
              : unitTest.findGoldenLocation(transformName);
      if (location == null) {
        return;
      }

      List<DataSet> dataSets = hopGui.getMetadataProvider().getSerializer(DataSet.class).loadAll();

      PipelineUnitTestSetLocationDialog dialog =
          new PipelineUnitTestSetLocationDialog(
              hopGui.getActiveShell(),
              variables,
              hopGui.getMetadataProvider(),
              location,
              dataSets,
              pipelineMeta.getTransformNames(),
              transformFieldsResolver(pipelineGraph, pipelineMeta));
      if (dialog.open()) {
        hopGui.getMetadataProvider().getSerializer(PipelineUnitTest.class).save(unitTest);
        pipelineGraph.updateGui();
      }
    } catch (Exception e2) {
      new ErrorDialog(hopGui.getActiveShell(), "Error", "Error editing location", e2);
    }
  }

  /**
   * Resolve the output fields of a single transform for the dataset location dialog. The dialog
   * only asks for the transform it's mapping fields for, which keeps a slow transform (a Table
   * input needing a database connection for example) from blocking the whole dialog.
   */
  private Function<String, IRowMeta> transformFieldsResolver(
      HopGuiPipelineGraph pipelineGraph, PipelineMeta pipelineMeta) {
    return transformName -> {
      try {
        return pipelineMeta.getTransformFields(pipelineGraph.getVariables(), transformName);
      } catch (Exception e) {
        // Ignore GUI errors: the dialog reports unknown fields to the user.
        //
        return null;
      }
    };
  }

  private void showGoldenDataResult(
      HopGuiPipelineGraph pipelineGraph, HopGui hopGui, PipelineUnitTest unitTest, String name) {
    if (unitTest.findGoldenLocation(name) == null) {
      return;
    }

    // Find the errors list of the unit test...
    //
    IPipelineEngine<PipelineMeta> pipeline = pipelineGraph.getPipeline();
    if (pipeline == null) {
      return;
    }

    List<UnitTestResult> results =
        (List<UnitTestResult>) pipeline.getExtensionDataMap().get(DataSetConst.UNIT_TEST_RESULTS);
    if (Utils.isEmpty(results)) {
      return;
    }

    ValidatePipelineUnitTestExtensionPoint.showUnitTestErrors(pipeline, results, hopGui);
  }
}
