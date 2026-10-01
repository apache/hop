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

package org.apache.hop.ai.advisors.pipeline;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ai.advisor.AiAdvisorMetadataSelection;
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.ai.advisors.AiAdvisorInclusions;
import org.apache.hop.ai.engine.AiAdvisorMetadataContextTest.TestMetadataProvider;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.providers.OpenAiProvider;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.Test;

class PipelineAiContextBuilderTest {

  @Test
  void serializeStructureIncludesTransformsHopsAndFocus() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("demo-pipeline");
    TransformMeta input = new TransformMeta("TableInput", "Input", null);
    TransformMeta output = new TransformMeta("TableOutput", "Output", null);
    pipelineMeta.addTransform(input);
    pipelineMeta.addTransform(output);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(input, output));

    String json = PipelineAiContextBuilder.serializeStructure(pipelineMeta, "Input");
    assertTrue(json.contains("\"name\":\"Input\""));
    assertTrue(json.contains("\"name\":\"Output\""));
    assertTrue(json.contains("\"from\":\"Input\""));
    assertTrue(json.contains("\"to\":\"Output\""));
    assertTrue(json.contains("\"focusTransform\":\"Input\""));
  }

  @Test
  void userPromptIncludesFocusTransformXml() throws Exception {
    DummyMeta dummy = new DummyMeta();
    dummy.setDefault();
    TransformMeta input = new TransformMeta("Dummy", "Input", dummy);
    input.setLocation(new Point(10, 20));
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(input);
    AiAdvisorRequest request = new AiAdvisorRequest();
    request.setUserPrompt("Configure this transform");
    request.setArtifact(pipelineMeta);
    request.setVariables(new Variables());
    request.setFocusNodeName("Input");

    String prompt = PipelineAiContextBuilder.buildUserPrompt(pipelineMeta, request);
    assertTrue(prompt.contains("Focus transform:\nInput"));
    assertTrue(prompt.contains("Focus transform XML:"));
    assertTrue(prompt.contains("<transform>"));
    assertTrue(prompt.contains("Dummy"));
  }

  @Test
  void userPromptOmitsLogsWhenNotIncluded() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("demo");
    AiAdvisorRequest request = new AiAdvisorRequest();
    request.setUserPrompt("Why did this fail?");
    request.setArtifact(pipelineMeta);
    request.setVariables(new Variables());
    request.setLogExcerpt("ERROR: row rejected");
    request.getInclusions().put(AiAdvisorInclusions.LOGS, false);

    String prompt = PipelineAiContextBuilder.buildUserPrompt(pipelineMeta, request);
    assertTrue(prompt.contains("Why did this fail?"));
    assertFalse(prompt.contains("ERROR: row rejected"));
  }

  @Test
  void userPromptIncludesLogsWhenRequested() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("demo");
    AiAdvisorRequest request = new AiAdvisorRequest();
    request.setUserPrompt("Why did this fail?");
    request.setArtifact(pipelineMeta);
    request.setVariables(new Variables());
    request.setLogExcerpt("ERROR: row rejected");
    request.getInclusions().put(AiAdvisorInclusions.LOGS, true);

    String prompt = PipelineAiContextBuilder.buildUserPrompt(pipelineMeta, request);
    assertTrue(prompt.contains("ERROR: row rejected"));
  }

  @Test
  void firstTurnIncludesCatalogWhenRequestedAndMetadataTypeKeys() throws Exception {
    TestMetadataProvider provider = new TestMetadataProvider();
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("demo");
    AiAdvisorRequest request = new AiAdvisorRequest();
    request.setUserPrompt("Read a table into Excel");
    request.setArtifact(pipelineMeta);
    request.setVariables(new Variables());
    request.setMetadataProvider(provider);
    request.getInclusions().put(AiAdvisorInclusions.CATALOG, true);

    String prompt = PipelineAiContextBuilder.buildUserPrompt(pipelineMeta, request);
    assertTrue(prompt.contains("Available transform plugins JSON"));
    assertTrue(prompt.contains("Available metadata types JSON"));
    assertTrue(prompt.contains("Available database plugins JSON"));
    assertTrue(prompt.contains("ai-provider"));
  }

  @Test
  void followUpOmitsCatalogAndSummary() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("demo");
    AiAdvisorRequest request = new AiAdvisorRequest();
    request.setUserPrompt("What next?");
    request.setArtifact(pipelineMeta);
    request.setVariables(new Variables());
    request.setFollowUp(true);
    request.getInclusions().put(AiAdvisorInclusions.CATALOG, true);

    String prompt = PipelineAiContextBuilder.buildUserPrompt(pipelineMeta, request);
    assertTrue(prompt.contains("Pipeline structure JSON"));
    assertFalse(prompt.contains("Pipeline summary JSON"));
    assertFalse(prompt.contains("Available transform plugins JSON"));
    assertFalse(prompt.contains("Available metadata types JSON"));
    assertFalse(prompt.contains("Available database plugins JSON"));
  }

  @Test
  void xmlIsOmittedWhenConfigForbidsIt() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("demo");
    AiAdvisorRequest request = new AiAdvisorRequest();
    request.setUserPrompt("Show xml");
    request.setArtifact(pipelineMeta);
    request.setVariables(new Variables());
    request.getInclusions().put(AiAdvisorInclusions.XML, true);

    String prompt = PipelineAiContextBuilder.buildUserPrompt(pipelineMeta, request);
    assertFalse(prompt.contains("Pipeline topology XML"));
  }

  @Test
  void metadataIsOmittedUntilSelectedAndEnabled() throws Exception {
    TestMetadataProvider provider = new TestMetadataProvider();
    AiProvider object = new AiProvider();
    OpenAiProvider backend = new OpenAiProvider();
    backend.setPluginId("openai");
    object.setName("sales-db");
    object.setProvider(backend);
    object.setApiKey("sk-hidden");
    provider.getSerializer(AiProvider.class).save(object);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("demo");
    AiAdvisorRequest request = new AiAdvisorRequest();
    request.setUserPrompt("How is sales connected?");
    request.setArtifact(pipelineMeta);
    request.setVariables(new Variables());
    request.setMetadataProvider(provider);
    request.getMetadataSelections().add(new AiAdvisorMetadataSelection("ai-provider", "sales-db"));

    String omitted = PipelineAiContextBuilder.buildUserPrompt(pipelineMeta, request);
    assertFalse(omitted.contains("Selected metadata JSON"));

    request.getInclusions().put(AiAdvisorInclusions.METADATA, true);
    request.setFollowUp(true);
    String included = PipelineAiContextBuilder.buildUserPrompt(pipelineMeta, request);
    assertTrue(included.contains("Selected metadata JSON"));
    assertTrue(included.contains("sales-db"));
    assertFalse(included.contains("sk-hidden"));
  }
}
