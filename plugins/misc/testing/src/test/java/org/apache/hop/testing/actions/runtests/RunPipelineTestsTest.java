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

package org.apache.hop.testing.actions.runtests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.UnitTestResult;
import org.apache.hop.testing.util.IExceptionEvaluator;
import org.apache.hop.testing.util.IPipelineResultEvaluator;
import org.apache.hop.testing.util.ITestResultsEvaluator;
import org.apache.hop.testing.util.UnitTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Error text for Run pipeline unit tests names the unit test (issue #9443). */
class RunPipelineTestsTest {

  private MemoryMetadataProvider provider;

  @BeforeAll
  static void initHop() throws HopException {
    HopLogStore.init();
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    provider = new MemoryMetadataProvider();
  }

  @Test
  void missingUnitTestLogsTheTestName() throws Exception {
    CapturingRunPipelineTests action = action("no-such-test");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
    assertEquals(List.of("Unable to load unit test for 'no-such-test'"), action.errors);
    assertEquals(
        "Pipeline of unit test : no-such-test", action.getReferencedObjectDescriptions()[0]);
  }

  @Test
  void missingReferencedUnitTestNamesTheTest() {
    CapturingRunPipelineTests action = action("no-such-test");

    HopException exception =
        assertThrows(
            HopException.class, () -> action.loadReferencedObject(0, provider, new Variables()));

    assertTrue(exception.getMessage().contains("Unit test 'no-such-test' could not be found"));
    assertFalse(exception.getMessage().contains("RunPipelineTestsField"));
  }

  @Test
  void unitTestWithoutPipelineLogsTheTestName() throws Exception {
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.setName("orders-test");
    provider.getSerializer(PipelineUnitTest.class).save(unitTest);
    CapturingRunPipelineTests action = action("orders-test");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.isResult());
    assertEquals(1, result.getNrErrors());
    assertEquals(
        List.of("There was an exception executing pipeline unit test 'orders-test'"),
        action.errors);
  }

  @Test
  void evaluationErrorsCloseTheQuotedName() throws Exception {
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.setName("orders-test");
    provider.getSerializer(PipelineUnitTest.class).save(unitTest);
    CapturingRunPipelineTests action = action("orders-test");

    try (MockedStatic<UnitTestUtil> unitTests = mockStatic(UnitTestUtil.class)) {
      unitTests
          .when(
              () ->
                  UnitTestUtil.executeUnitTest(
                      any(), any(), any(), any(), any(), any(), any(), any(), any()))
          .thenAnswer(
              invocation -> {
                Result pipelineResult = new Result();
                pipelineResult.setNrErrors(1);
                IPipelineResultEvaluator pipelineEvaluator = invocation.getArgument(6);
                pipelineEvaluator.evaluatePipelineResults(null, pipelineResult);

                UnitTestResult dataSetError =
                    new UnitTestResult(
                        "orders",
                        "orders-test",
                        "orders-golden",
                        "Select values",
                        true,
                        "row 2 differs");
                ITestResultsEvaluator resultsEvaluator = invocation.getArgument(7);
                resultsEvaluator.evaluateTestResults(null, List.of(dataSetError));

                IExceptionEvaluator exceptionEvaluator = invocation.getArgument(8);
                exceptionEvaluator.evaluateTestException(
                    invocation.getArgument(0), null, new HopException("boom"));
                return null;
              });

      Result result = action.execute(new Result(), 0);

      assertFalse(result.isResult());
      assertEquals(1, result.getNrErrors());
      assertEquals(
          List.of(
              "There was an error running the pipeline for unit test 'orders-test'",
              "Error in validating test data set 'orders-golden' : row 2 differs",
              "There were test result evaluation errors in pipeline unit test 'orders-test'",
              "There was an exception executing pipeline unit test 'orders-test'"),
          action.errors);
      assertTrue(
          action.errors.stream().noneMatch(message -> message.contains("RunPipelineTestsField")));
    }
  }

  private CapturingRunPipelineTests action(String testName) {
    CapturingRunPipelineTests action = new CapturingRunPipelineTests();
    action.setMetadataProvider(provider);
    RunPipelineTestsField field = new RunPipelineTestsField();
    field.setTestName(testName);
    action.getTestNames().add(field);
    return action;
  }

  private static final class CapturingRunPipelineTests extends RunPipelineTests {
    private final List<String> errors = new ArrayList<>();

    @Override
    public void logError(String message) {
      errors.add(message);
    }

    @Override
    public void logError(String message, Throwable e) {
      errors.add(message);
    }
  }
}
