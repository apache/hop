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

package org.apache.hop.testing.actions.runtests;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.UnitTestResult;
import org.apache.hop.testing.util.UnitTestUtil;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

@Action(
    id = "RunPipelineTests",
    name = "i18n::RunPipelineTests.Name",
    description = "i18n::RunPipelineTests.Description",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.General",
    keywords = "i18n::RunPipelineTests.Keywords",
    image = "Test_tube_icon.svg",
    documentationUrl = "/workflow/actions/runpipelinetests.html")
@Getter
@Setter
public class RunPipelineTests extends ActionBase implements IAction, Cloneable {

  public static final String TEST_NAMES = "test_names";
  public static final String TEST_NAME = "test_name";

  @HopMetadataProperty(key = TEST_NAME, groupKey = TEST_NAMES)
  private List<RunPipelineTestsField> testNames;

  public RunPipelineTests(String name, String description) {
    super(name, description);
    testNames = new ArrayList<>();
  }

  public RunPipelineTests() {
    this("", "");
  }

  @Override
  public RunPipelineTests clone() {
    return (RunPipelineTests) super.clone();
  }

  @Override
  public Result execute(Result prevResult, int nr) throws HopException {

    IHopMetadataSerializer<PipelineUnitTest> testSerializer =
        getMetadataProvider().getSerializer(PipelineUnitTest.class);

    AtomicBoolean success = new AtomicBoolean(true);

    for (RunPipelineTestsField testName : testNames) {

      PipelineUnitTest test = testSerializer.load(testName.getTestName());

      UnitTestUtil.executeUnitTest(
          test,
          this,
          getLogLevel(),
          prevResult,
          getMetadataProvider(),
          this,
          // Something went wrong executing the pipeline itself.
          //
          (pipeline, result) -> {
            if (result.getNrErrors() > 0) {
              this.logError(
                  "There was an error running the pipeline for unit test '" + test.getName() + "'");
              success.set(false);
            }
          },
          // The pipeline ran fine: we can evaluate the test results
          (pipeline, testResults) -> {
            int errorCount = 0;
            for (UnitTestResult testResult : testResults) {
              if (testResult.isError()) {
                this.logError(
                    "Error in validating test data set '"
                        + testResult.getDataSetName()
                        + " : "
                        + testResult.getComment());
                errorCount++;
              }
            }
            if (errorCount > 0) {
              this.logError(
                  "There were test result evaluation errors in pipeline unit test '"
                      + test.getName());
              success.set(false);
            }
          },
          (test1, pipelineMeta, e) -> {
            if (test == null) {
              this.logError("Unable to load unit test for '" + testName, e);
            } else {
              this.logError(
                  "There was an exception executing pipeline unit test '" + test.getName(), e);
            }
            success.set(false);
          });
    }

    if (success.get()) {
      prevResult.setNrErrors(0);
      prevResult.setResult(true);
    } else {
      prevResult.setNrErrors(prevResult.getNrErrors() + 1);
      prevResult.setResult(false);
    }

    return prevResult;
  }

  @Override
  public String[] getReferencedObjectDescriptions() {
    String[] descriptions = new String[testNames.size()];
    for (int i = 0; i < descriptions.length; i++) {
      descriptions[i] = "Pipeline of unit test : " + testNames.get(i).getTestName();
    }
    return descriptions;
  }

  @Override
  public boolean[] isReferencedObjectEnabled() {
    boolean[] enabled = new boolean[testNames.size()];
    for (int i = 0; i < enabled.length; i++) {
      enabled[i] = true;
    }
    return enabled;
  }

  @Override
  public IHasFilename loadReferencedObject(
      int index, IHopMetadataProvider metadataProvider, IVariables variables) throws HopException {

    IHopMetadataSerializer<PipelineUnitTest> testSerializer =
        metadataProvider.getSerializer(PipelineUnitTest.class);
    RunPipelineTestsField testName = testNames.get(index);
    PipelineUnitTest test = testSerializer.load(testName.getTestName());
    if (test == null) {
      throw new HopException("Unit test '" + testName + "' could not be found");
    }
    return UnitTestUtil.loadTestPipeline(test, metadataProvider, variables);
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }
}
