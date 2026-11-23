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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hop.lineage.LineageUnitTestEmitter;
import org.apache.hop.lineage.model.UnitTestLineagePayload.Result;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.PipelineUnitTestSetLocation;
import org.apache.hop.testing.UnitTestResult;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** What the lineage sinks of a unit test run hear about each golden data set. */
class ValidatePipelineUnitTestLineageTest {

  /** One golden data set as the sinks hear it: transform, data set, result, comment. */
  private record Reported(String transform, String dataSet, Result result, String comment) {}

  @SuppressWarnings("unchecked")
  private static IPipelineEngine<PipelineMeta> pipeline(String... transforms) {
    PipelineMeta meta = new PipelineMeta();
    for (String transform : transforms) {
      meta.addTransform(new TransformMeta(transform, null));
    }
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    when(pipeline.getPipelineMeta()).thenReturn(meta);
    when(pipeline.getExtensionDataMap()).thenReturn(new HashMap<>());
    return pipeline;
  }

  private static PipelineUnitTest unitTest(String... transformsAndDataSets) {
    PipelineUnitTest unitTest = new PipelineUnitTest();
    unitTest.setName("orders-test");
    List<PipelineUnitTestSetLocation> golden = new ArrayList<>();
    for (int i = 0; i < transformsAndDataSets.length; i += 2) {
      golden.add(
          new PipelineUnitTestSetLocation(
              transformsAndDataSets[i], transformsAndDataSets[i + 1], List.of(), List.of()));
    }
    unitTest.setGoldenDataSets(golden);
    return unitTest;
  }

  private static UnitTestResult result(
      String transform, String dataSet, boolean error, String comment) {
    return new UnitTestResult("orders", "orders-test", dataSet, transform, error, comment);
  }

  private static List<Reported> report(
      IPipelineEngine<PipelineMeta> pipeline,
      PipelineUnitTest unitTest,
      List<UnitTestResult> results,
      String why) {
    List<Reported> reported = new ArrayList<>();
    try (MockedStatic<LineageUnitTestEmitter> emitter = mockStatic(LineageUnitTestEmitter.class)) {
      emitter
          .when(
              () ->
                  LineageUnitTestEmitter.emit(
                      any(), any(), anyString(), anyString(), anyString(), any(), any()))
          .thenAnswer(
              call -> {
                reported.add(
                    new Reported(
                        call.getArgument(3),
                        call.getArgument(4),
                        call.getArgument(5),
                        call.getArgument(6)));
                return null;
              });
      ValidatePipelineUnitTestExtensionPoint.reportToLineage(pipeline, unitTest, results, why);
    }
    return reported;
  }

  @Test
  void eachGoldenDataSetPassesOrFailsWithTheCommentsOfItsErrors() {
    List<Reported> reported =
        report(
            pipeline("Write", "Load"),
            unitTest("Write", "golden-write", "Load", "golden-load"),
            List.of(
                result("Write", "golden-write", false, "3 rows compared"),
                result("Load", "golden-load", true, "row 1 differs"),
                result("Load", "golden-load", true, "row 2 differs")),
            null);

    assertEquals(
        List.of(
            new Reported("Write", "golden-write", Result.PASSED, "3 rows compared"),
            new Reported("Load", "golden-load", Result.FAILED, "row 1 differs; row 2 differs")),
        reported);
  }

  @Test
  void aUnitTestWithoutGoldenDataSetsReportsOnlyItsResults() {
    PipelineUnitTest unitTest = unitTest();
    unitTest.setGoldenDataSets(null);

    List<Reported> reported =
        report(
            pipeline("Write"),
            unitTest,
            List.of(result("Write", "golden-write", false, "3 rows compared")),
            null);

    assertEquals(
        List.of(new Reported("Write", "golden-write", Result.PASSED, "3 rows compared")), reported);
  }

  @Test
  void aValidationThatStoppedMakesEveryGoldenDataSetAnError() {
    String why = "Unable to validate the unit test: data set golden-load can't be read";
    List<Reported> reported =
        report(
            pipeline("Write", "Load"),
            unitTest("Write", "golden-write", "Load", "golden-load", "Gone", "golden-gone"),
            List.of(
                result("Write", "golden-write", false, "3 rows compared"),
                // Added before the golden data set is read, which then failed
                result("Load", "golden-load", false, "WARNING: no test results found")),
            why);

    assertEquals(
        List.of(
            new Reported("Write", "golden-write", Result.ERROR, why),
            new Reported("Load", "golden-load", Result.ERROR, why)),
        reported,
        "a golden data set of a transform the pipeline no longer has is left out, as the test"
            + " leaves it out");
  }

  @Test
  void withoutRowsToCompareEveryGoldenDataSetIsAnErrorWithTheReason() {
    List<Reported> reported =
        report(
            pipeline("Write"),
            unitTest("Write", "golden-write"),
            List.of(result(null, null, false, "No transform output result data found to validate")),
            null);

    assertEquals(
        List.of(
            new Reported(
                "Write",
                "golden-write",
                Result.ERROR,
                "No transform output result data found to validate")),
        reported);
  }
}
