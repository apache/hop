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

import org.apache.hop.web.it.pages.ExecutionResultsPanel;
import org.apache.hop.web.it.pages.PipelineGraphPage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What is typed into a transform dialog is what the pipeline then does.
 *
 * <p>The dialog sweep opens all 275 transform dialogs and closes them again, which catches a dialog
 * that will not open and nothing else. Everything after that - typing into a field, the field
 * reaching the transform, the transform running with it - has never been tested in Hop Web, and it
 * is where the RAP specific bugs live: text fields that ignored the arrow keys (issue #7833), a
 * dialog whose grid did not work at all (issue #4475), fields that came back empty (issue #7301).
 *
 * <p>The assertion is deliberately made at the far end, on rows the engine actually produced,
 * rather than on the dialog showing the value back: a field that displays what was typed and drops
 * it on OK looks perfectly healthy from the browser.
 */
@DisplayName("Transform dialogs")
class HopWebDialogTest extends HopWebTestBase {

  private static final String TRANSFORM = "Generate rows";

  @Test
  @DisplayName("a value typed into a dialog reaches the running pipeline")
  void dialogValuesReachTheEngine() {
    PipelineGraphPage graph = hopGui.newPipeline();
    graph.addTransform(TRANSFORM);

    graph.actOnTransform(TRANSFORM, "Edit", 0, 0);
    hopGui.awaitDialog();
    hopGui.enterDialogField("Limit", "5");
    hopGui.clickButton("OK");
    hopGui.awaitNoDialog();

    // A pipeline can only be run once it has a name of its own.
    hopGui.saveFileAs(
        HopWebEnvironment.scratchFolder()
            + "/hop-web-it-dialog-"
            + System.currentTimeMillis()
            + ".hpl");

    ExecutionResultsPanel results = graph.run(hopGui, TRANSFORM);

    assertEquals(
        "5",
        results.metricsOf(TRANSFORM).get("Written (rows)"),
        () -> "the limit typed into the dialog did not reach the engine: " + results.metrics());
  }
}
