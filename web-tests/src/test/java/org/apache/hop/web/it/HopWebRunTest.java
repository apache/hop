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

package org.apache.hop.web.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.hop.web.it.pages.ExecutionResultsPanel;
import org.apache.hop.web.it.pages.PipelineGraphPage;
import org.apache.hop.web.it.pages.PreviewDataDialog;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Running a pipeline, which is the one thing Hop Web is for and the one thing the suite never did.
 *
 * <p>It is also where the failures nobody sees live. Everything up to here happens on the request
 * thread RAP hands the UI; an execution does not. It starts threads of its own that report progress
 * back into widgets, and RAP - unlike the fat client's SWT - refuses any of that from a thread that
 * is not the UI thread of the session. Two of those (issues #8195 and #7896) shipped past a green
 * suite because nothing in it ever pressed Run.
 */
@DisplayName("Running a pipeline")
class HopWebRunTest extends HopWebTestBase {

  /** A sample that reads nothing from outside itself, so it runs the same anywhere. */
  private static final String PIPELINE = "/transforms/add-constants.hpl";

  private static final String GENERATOR = "generate 1 row";
  private static final String CONSTANTS = "add constants";

  private PipelineGraphPage openSample() {
    return hopGui.openPipeline(HopWebEnvironment.samplesFolder() + PIPELINE, GENERATOR);
  }

  @Test
  @DisplayName("a pipeline runs and every transform reports itself finished")
  void runsToCompletion() {
    PipelineGraphPage graph = openSample();

    ExecutionResultsPanel results = graph.run(hopGui, GENERATOR, CONSTANTS);

    // run() already waited for every transform to be Finished; this says which ones took part,
    // so a pipeline that "finished" without ever starting its transforms is not a pass.
    assertNotNull(results.metricsOf(GENERATOR), "no metrics for " + GENERATOR);
    assertNotNull(results.metricsOf(CONSTANTS), "no metrics for " + CONSTANTS);
  }

  @Test
  @DisplayName("the metrics report the rows that were really moved")
  void reportsRowCounts() {
    PipelineGraphPage graph = openSample();

    ExecutionResultsPanel results = graph.run(hopGui, GENERATOR, CONSTANTS);

    Map<String, String> generator = results.metricsOf(GENERATOR);
    Map<String, String> constants = results.metricsOf(CONSTANTS);
    // The sample generates one row and adds constants to it. Asserting the counts rather than the
    // status is what tells a pipeline that ran from one that only claimed to.
    assertEquals("1", generator.get("Written (rows)"), () -> GENERATOR + " wrote " + generator);
    assertEquals("1", constants.get("Read (rows)"), () -> CONSTANTS + " read " + constants);
    assertEquals("0", constants.get("Errors (rows)"), () -> CONSTANTS + " errored " + constants);
  }

  @Test
  @DisplayName("previewing a transform shows the rows it produced")
  void previewsRows() {
    PipelineGraphPage graph = openSample();

    PreviewDataDialog preview = graph.preview(hopGui, CONSTANTS);

    assertEquals(1, preview.rowCount(), "the sample produces exactly one row");
    List<List<String>> rows = preview.rows();
    // The constants the sample adds. Reading the data back is the only assertion in the suite that
    // says Hop Web moved the right values, rather than the right number of them.
    assertTrue(
        rows.stream().anyMatch(row -> row.contains("abcdefgh")),
        () -> "the previewed row does not carry the constants: " + rows);
    preview.close();
  }
}
