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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.LineageSinkRoutes;
import org.apache.hop.lineage.LineageUnitTestEmitter;
import org.apache.hop.lineage.model.UnitTestLineagePayload;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.PipelineUnitTestSetLocation;
import org.apache.hop.testing.UnitTestResult;
import org.apache.hop.testing.util.DataSetConst;
import org.apache.hop.ui.core.dialog.ShowRowsDialog;
import org.apache.hop.ui.hopgui.HopGui;

@ExtensionPoint(
    extensionPointId = "PipelineFinish",
    id = "ValidatePipelineUnitTestExtensionPoint",
    description = "Inject a bunch of rows into a transform during preview")
public class ValidatePipelineUnitTestExtensionPoint
    implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {

  public static final Class<?> PKG = ValidatePipelineUnitTestExtensionPoint.class; // i18n

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, IPipelineEngine<PipelineMeta> pipeline)
      throws HopException {

    final PipelineMeta pipelineMeta = pipeline.getPipelineMeta();
    boolean runUnitTest =
        "Y".equalsIgnoreCase(pipeline.getVariable(DataSetConst.VAR_RUN_UNIT_TEST));
    if (!runUnitTest) {
      return;
    }
    // The data set rows come from row listeners on the transforms of the local engine
    if (!(pipeline instanceof LocalPipelineEngine)) {
      return;
    }

    // We should always have a unit test name here...
    String unitTestName = pipeline.getVariable(DataSetConst.VAR_UNIT_TEST_NAME);
    if (StringUtil.isEmpty(unitTestName)) {
      return;
    }

    PipelineUnitTest unitTest = null;
    final List<UnitTestResult> results = new ArrayList<>();
    boolean validated = false;
    String notValidated = null;
    try {
      IHopMetadataProvider metadataProvider = pipelineMeta.getMetadataProvider();

      if (metadataProvider == null) {
        return; // Nothing to do here, we can't reference data sets.
      }

      // If the pipeline has a variable set with the unit test in it, we're dealing with a unit test
      // situation.
      //
      unitTest = metadataProvider.getSerializer(PipelineUnitTest.class).load(unitTestName);

      pipeline.getExtensionDataMap().put(DataSetConst.UNIT_TEST_RESULTS, results);

      // Validate execution results with what's in the data sets...
      //
      int errors =
          DataSetConst.validateTransformResultAgainstUnitTest(
              pipeline, unitTest, metadataProvider, variables, results);
      validated = true;
      if (errors == 0) {
        log.logBasic("Unit test '" + unitTest.getName() + "' passed successfully");
      } else {
        log.logBasic(
            "Unit test '"
                + unitTest.getName()
                + "' failed, "
                + errors
                + " errors detected, "
                + results.size()
                + " comments to report.");

        String dontShowResults =
            pipeline.getVariable(DataSetConst.VAR_DO_NOT_SHOW_UNIT_TEST_ERRORS, "N");
        // Only Hop GUI can show them: elsewhere touching HopGui needs SWT, and the log has them
        if (Const.isGuiRuntime() && "N".equalsIgnoreCase(dontShowResults)) {
          final HopGui hopGui = HopGui.getInstance();
          if (hopGui != null) {
            showUnitTestErrors(pipeline, results, hopGui);
          }
        }
      }
      log.logBasic("----------------------------------------------");
      for (UnitTestResult result : results) {
        if (result.getDataSetName() != null) {
          log.logBasic(
              result.getTransformName()
                  + " - "
                  + result.getDataSetName()
                  + " : "
                  + result.getComment());
        } else {
          log.logBasic(result.getComment());
        }
      }
      log.logBasic("----------------------------------------------");
    } catch (Throwable e) {
      log.logError("Unable to validate unit test/golden rows", e);
      if (!validated) {
        // Reported as an error, otherwise the previous result of the test would still show
        notValidated =
            "Unable to validate the unit test: " + Const.NVL(e.getMessage(), e.toString());
      }
    }
    if (unitTest != null) {
      reportToLineageQuietly(pipeline, unitTest, results, notValidated, log);
    }
  }

  /**
   * Reports the result to the lineage sinks of the run, after it is shown: a failure here is
   * logged, and never hides or changes the result of the test.
   */
  private static void reportToLineageQuietly(
      IPipelineEngine<PipelineMeta> pipeline,
      PipelineUnitTest unitTest,
      List<UnitTestResult> results,
      String why,
      ILogChannel log) {
    try {
      // Not isEnabled(): that is false for a unit test run, which reports only its results
      if (LineageSinkRoutes.isUnitTestReported(pipeline)) {
        reportToLineage(pipeline, unitTest, results, why);
      }
    } catch (Exception e) {
      log.logError("Unable to report the result of unit test '" + unitTest.getName() + "'", e);
    }
  }

  public static final void showUnitTestErrors(
      IPipelineEngine<PipelineMeta> pipeline, List<UnitTestResult> results, HopGui hopGui) {
    hopGui
        .getShell()
        .getDisplay()
        .asyncExec(
            () ->
                new ShowRowsDialog(
                        hopGui.getShell(),
                        pipeline,
                        BaseMessages.getString(PKG, "UnitTestsResult.Dialog.Header"),
                        BaseMessages.getString(PKG, "UnitTestsResult.Dialog.Message"),
                        UnitTestResult.getRowMeta(),
                        UnitTestResult.getRowData(results))
                    .open());
  }

  /**
   * One result per golden data set of the unit test, for the lineage sinks of the run: passed when
   * none of its rows is an error, failed with the comments of the errors otherwise. A golden data
   * set without results was not compared at all: an error.
   *
   * <p>The results that did not pass go first. A sink that gets the results but not the run's own
   * end, such as the one of a run configuration, ends the run with its first result: that is then
   * the run's worst.
   *
   * @param why why the validation stopped, such as a golden data set that could not be read: then
   *     every golden data set is an error, since a result may be from before its comparison
   *     finished. Null when the validation finished.
   */
  static void reportToLineage(
      IPipelineEngine<PipelineMeta> pipeline,
      PipelineUnitTest unitTest,
      List<UnitTestResult> results,
      String why) {
    Map<List<String>, List<UnitTestResult>> byLocation = new LinkedHashMap<>();
    List<PipelineUnitTestSetLocation> goldenDataSets =
        unitTest.getGoldenDataSets() == null ? List.of() : unitTest.getGoldenDataSets();
    for (PipelineUnitTestSetLocation golden : goldenDataSets) {
      if (!StringUtil.isEmpty(golden.getTransformName())
          && !StringUtil.isEmpty(golden.getDataSetName())
          && pipeline.getPipelineMeta().findTransform(golden.getTransformName()) != null) {
        byLocation.put(
            List.of(golden.getTransformName(), golden.getDataSetName()), new ArrayList<>());
      }
    }
    String notCompared = why;
    for (UnitTestResult result : results) {
      if (notCompared == null && StringUtil.isEmpty(result.getTransformName())) {
        notCompared = result.getComment();
      }
      if (!StringUtil.isEmpty(result.getTransformName())
          && !StringUtil.isEmpty(result.getDataSetName())) {
        byLocation
            .computeIfAbsent(
                List.of(result.getTransformName(), result.getDataSetName()), k -> new ArrayList<>())
            .add(result);
      }
    }
    PipelineMeta original =
        pipeline.getExtensionDataMap().get(DataSetConst.UNIT_TEST_ORIGINAL_PIPELINE)
                instanceof PipelineMeta meta
            ? meta
            : pipeline.getPipelineMeta();
    List<Runnable> passed = new ArrayList<>();
    for (Map.Entry<List<String>, List<UnitTestResult>> location : byLocation.entrySet()) {
      List<String> errors = new ArrayList<>();
      String comment = null;
      for (UnitTestResult result : location.getValue()) {
        if (result.isError()) {
          errors.add(result.getComment());
        } else if (comment == null) {
          comment = result.getComment();
        }
      }
      UnitTestLineagePayload.Result outcome;
      if (location.getValue().isEmpty() || why != null) {
        outcome = UnitTestLineagePayload.Result.ERROR;
        comment = Const.NVL(notCompared, "The rows were not compared");
      } else if (errors.isEmpty()) {
        outcome = UnitTestLineagePayload.Result.PASSED;
      } else {
        outcome = UnitTestLineagePayload.Result.FAILED;
        comment = String.join("; ", errors);
      }
      UnitTestLineagePayload.Result reported = outcome;
      String reportedComment = comment;
      Runnable report =
          () ->
              LineageUnitTestEmitter.emit(
                  pipeline,
                  original,
                  unitTest.getName(),
                  location.getKey().get(0),
                  location.getKey().get(1),
                  reported,
                  reportedComment);
      if (outcome == UnitTestLineagePayload.Result.PASSED) {
        passed.add(report);
      } else {
        report.run();
      }
    }
    passed.forEach(Runnable::run);
  }
}
