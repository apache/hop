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

package org.apache.hop.documentation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** The Markdown hop doc writes for a pipeline or workflow, for other plugins to publish. */
class DocMarkdownTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static PipelineMeta orders() throws Exception {
    PipelineMeta meta = new PipelineMeta();
    meta.setName("orders");
    meta.setDescription("Loads the orders");
    meta.addParameterDefinition("DAY", "today", "The day to load");
    meta.addNote(new NotePadMeta("Runs after the customers", 0, 0, 100, 50));
    meta.addNote(new NotePadMeta("Asks for a DAY", 0, 100, 100, 50));
    return meta;
  }

  @Test
  void aPipelineHasItsDetailsParametersAndNotesWithoutHeaderOrImage() throws Exception {
    String markdown =
        new DocMarkdown(new LogChannel("doc-test"), new Variables(), true, true)
            .pipeline(orders(), "flows/orders.hpl");

    assertTrue(markdown.startsWith("* Description : Loads the orders"), markdown);
    assertTrue(markdown.contains("* Filename : flows/orders.hpl"), markdown);
    assertTrue(markdown.contains("    - DAY : The day to load, default is [today]"), markdown);
    assertTrue(
        markdown.indexOf("Asks for a DAY") < markdown.indexOf("Runs after the customers"),
        "notes in alphabetical order: " + markdown);
    assertFalse(markdown.contains("title:"), markdown);
    assertFalse(markdown.contains("## Image"), markdown);
  }

  @Test
  void parametersAndNotesAreLeftOutAsHopDocDoesWithoutItsOptions() throws Exception {
    String markdown =
        new DocMarkdown(new LogChannel("doc-test"), new Variables(), false, false)
            .pipeline(orders(), "orders.hpl");

    assertFalse(markdown.contains("DAY"), markdown);
    assertFalse(markdown.contains("## Notes"), markdown);
  }

  @Test
  void aWorkflowHasItsDetails() throws Exception {
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("nightly");
    meta.setDescription("The nightly load");

    String markdown =
        new DocMarkdown(new LogChannel("doc-test"), new Variables(), true, true)
            .workflow(meta, "nightly.hwf");

    assertTrue(markdown.contains("* Description : The nightly load"), markdown);
    assertTrue(markdown.contains("* Filename : nightly.hwf"), markdown);
    assertEquals(-1, markdown.indexOf("## Notes"), markdown);
  }
}
