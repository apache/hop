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

package org.apache.hop.lineage.datahub;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import io.openlineage.client.OpenLineage.RunEvent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.datacatalog.DataCatalog;
import org.apache.hop.lineage.datahub.DataHubTranslator.Mcp;
import org.apache.hop.lineage.metadata.LineageSink;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/** Publishing pipelines and workflows to DataHub without running them. */
class DataHubPublisherTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String INGEST = "/aspects?action=ingestProposalBatch";

  @RegisterExtension
  static WireMockExtension gms =
      WireMockExtension.newInstance()
          .options(WireMockConfiguration.wireMockConfig().dynamicPort())
          .build();

  @TempDir Path project;

  private MemoryMetadataProvider metadata;
  private IVariables variables;

  @BeforeAll
  static void init() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    gms.stubFor(post(urlEqualTo(INGEST)).willReturn(aResponse().withStatus(200).withBody("{}")));
    gms.stubFor(
        post(urlEqualTo("/api/graphql"))
            .willReturn(aResponse().withStatus(200).withBody("{\"data\":{\"entities\":[]}}")));
    metadata = new MemoryMetadataProvider();
    DataHubDataCatalog catalog = new DataHubDataCatalog();
    catalog.setPluginId("datahub");
    catalog.setPluginName("DataHub");
    catalog.setUrl(gms.baseUrl());
    catalog.setEnvironment("dev");
    metadata.getSerializer(DataCatalog.class).save(new DataCatalog("datahub", null, catalog));
    DataHubSinkConfiguration sink = new DataHubSinkConfiguration();
    sink.setPluginId("datahub");
    sink.setPluginName("DataHub");
    sink.setDataCatalog("datahub");
    sink.setPlatformInstance("emea");
    metadata.getSerializer(LineageSink.class).save(new LineageSink("datahub", null, sink));
    variables = new Variables();
    variables.setVariable("PROJECT_HOME", project.toString());
  }

  private void write(String relative, String content) throws Exception {
    Path file = project.resolve(relative);
    Files.createDirectories(file.getParent());
    Files.writeString(file, content);
  }

  private static List<JsonNode> proposalsSent() throws Exception {
    List<JsonNode> proposals = new ArrayList<>();
    for (var request : gms.findAll(postRequestedFor(urlEqualTo(INGEST)))) {
      MAPPER.readTree(request.getBodyAsString()).get("proposals").forEach(proposals::add);
    }
    return proposals;
  }

  @Test
  void aDefinitionHasNoRunsAndNoOperations() {
    DataHubTranslator translator =
        new DataHubTranslator("PROD", "prod", null, "urn:li:corpuser:datahub", false);
    translator.setDefinitionsOnly(true);
    List<Mcp> proposals = new ArrayList<>();
    for (RunEvent event :
        new HopEvents("orders").readThenWrite("file:///data/in.csv", "file:///out/x.csv")) {
      proposals.addAll(translator.translate(event).proposals());
    }

    assertFalse(proposals.isEmpty());
    assertTrue(proposals.stream().noneMatch(p -> p.entityType().equals("dataProcessInstance")));
    assertTrue(proposals.stream().anyMatch(p -> p.aspectName().equals("dataJobInputOutput")));
    assertTrue(proposals.stream().anyMatch(p -> p.aspectName().equals("dataFlowInfo")));
  }

  @Test
  void theProjectsPipelinesAndWorkflowsArePublished() throws Exception {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("orders");
    write("flows/orders.hpl", pipeline.getXml(variables));
    WorkflowMeta workflow = new WorkflowMeta();
    workflow.setName("nightly");
    write("nightly.hwf", workflow.getXml(variables));
    write("metadata/not-a-flow.hpl", pipeline.getXml(variables));
    write("broken.hpl", "<pipeline><info>");

    DataHubPublisher.Result result =
        new DataHubPublisher(variables, metadata, new LogChannel("publish-test"))
            .publish("datahub", "${PROJECT_HOME}");

    assertEquals(1, result.pipelines());
    assertEquals(1, result.workflows());
    assertEquals(1, result.unreadable().size());
    assertTrue(result.unreadable().get(0).endsWith("broken.hpl"), result.unreadable().toString());
    assertFalse(result.isSuccess(), "a file that can't be read makes the publication fail");
    assertEquals(0, result.health().waiting());

    List<String> flows = new ArrayList<>();
    for (JsonNode proposal : proposalsSent()) {
      assertFalse(
          proposal.get("entityType").asText().equals("dataProcessInstance"),
          "no runs: " + proposal);
      if (proposal.get("aspectName").asText().equals("dataFlowInfo")) {
        flows.add(proposal.get("entityUrn").asText());
      }
    }
    assertEquals(
        List.of(
            "urn:li:dataFlow:(hop,emea.flows/orders.hpl,dev)",
            "urn:li:dataFlow:(hop,emea.nightly.hwf,dev)"),
        flows.stream().distinct().sorted().toList());
  }

  @Test
  void namesResolveAsARunOfTheFileResolvesThem() throws Exception {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("orders");
    pipeline.addParameterDefinition("OUT_DIR", "/data/out", "");
    write("flows/orders.hpl", pipeline.getXml(variables));
    WorkflowMeta workflow = new WorkflowMeta();
    workflow.setName("nightly");
    write("jobs/nightly.hwf", workflow.getXml(variables));
    // What the publication runs with: an action in a workflow elsewhere, say
    variables.setVariable(Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER, "/the/calling/workflow");
    variables.setVariable("OUT_DIR", "/not/the/default");

    IVariables ofPipeline =
        DataHubPublisher.variablesOf(
            new PipelineMeta(project.resolve("flows/orders.hpl").toString(), metadata, variables),
            variables);
    IVariables ofWorkflow =
        DataHubPublisher.variablesOf(
            new WorkflowMeta(variables, project.resolve("jobs/nightly.hwf").toString(), metadata),
            variables);

    assertEquals(
        HopVfs.getFileObject(project.resolve("flows").toString()).getName().getURI(),
        ofPipeline.resolve("${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER + "}"));
    assertEquals("/data/out", ofPipeline.resolve("${OUT_DIR}"), "the default of the parameter");
    assertEquals(
        HopVfs.getFileObject(project.resolve("jobs").toString()).getName().getURI(),
        ofWorkflow.resolve("${" + Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER + "}"));
    assertEquals(project.toString(), ofWorkflow.resolve("${PROJECT_HOME}"));
    assertEquals(
        "/the/calling/workflow",
        variables.getVariable(Const.INTERNAL_VARIABLE_ENTRY_CURRENT_FOLDER),
        "the variables of the publication stay as they are");
  }

  @Test
  void theDocumentationOfEachPipelineAndWorkflowIsPublishedUnderItsProject() throws Exception {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("orders");
    pipeline.setDescription("Loads the orders");
    pipeline.addNote(new NotePadMeta("Runs after the customers", 0, 0, 100, 50));
    write("flows/orders.hpl", pipeline.getXml(variables));
    WorkflowMeta workflow = new WorkflowMeta();
    workflow.setName("nightly");
    workflow.addAction(new ActionMeta(new ActionStart("Start")));
    write("nightly.hwf", workflow.getXml(variables));
    variables.setVariable(DataHubPublisher.VAR_PROJECT_NAME, "sales");

    DataHubPublisher publisher =
        new DataHubPublisher(variables, metadata, new LogChannel("publish-test"));
    publisher.setIncludingDocumentation(true);
    DataHubPublisher.Result result = publisher.publish("datahub", "${PROJECT_HOME}");

    assertTrue(result.isSuccess(), DataHubPublishCommand.describe(result));
    Map<String, JsonNode> documents = new TreeMap<>();
    List<String> aspects = new ArrayList<>();
    int projectDocuments = 0;
    for (JsonNode proposal : proposalsSent()) {
      aspects.add(proposal.get("aspectName").asText());
      if (proposal.get("aspectName").asText().equals("documentInfo")) {
        JsonNode info = MAPPER.readTree(proposal.path("aspect").path("value").asText());
        documents.put(info.path("title").asText(), info);
        projectDocuments += info.path("title").asText().startsWith("Hop project") ? 1 : 0;
      }
    }
    assertEquals(
        List.of("Hop project sales", "Pipeline orders", "Workflow nightly"),
        List.copyOf(documents.keySet()));
    assertEquals(1, projectDocuments, "the project's document is sent once");
    assertTrue(
        aspects.indexOf("documentInfo") > aspects.lastIndexOf("dataFlowInfo"),
        "the documents follow the pipelines and workflows: " + aspects);
    String orders = documents.get("Pipeline orders").path("contents").path("text").asText();
    assertTrue(orders.contains("* Description : Loads the orders"), orders);
    assertTrue(orders.contains("* Filename : flows/orders.hpl"), orders);
    assertFalse(orders.contains("Runs after the customers"), "notes only when asked: " + orders);
    JsonNode nightly = documents.get("Workflow nightly");
    assertEquals(
        "urn:li:dataJob:(urn:li:dataFlow:(hop,emea.nightly.hwf,dev),Start)",
        nightly.path("relatedAssets").get(1).path("asset").asText());
    assertEquals(
        documents.get("Pipeline orders").path("parentDocument"),
        nightly.path("parentDocument"),
        "both under the project's document");
  }

  @Test
  void theNotesAndParametersAreDocumentedWhenAsked() throws Exception {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("orders");
    pipeline.addNote(new NotePadMeta("Runs after the customers", 0, 0, 100, 50));
    pipeline.addParameterDefinition("OUT_DIR", "/data/out", "Where the orders go");
    write("orders.hpl", pipeline.getXml(variables));

    DataHubPublisher publisher =
        new DataHubPublisher(variables, metadata, new LogChannel("publish-test"));
    publisher.setIncludingDocumentation(true);
    String withoutThem = documentText(publisher);
    gms.resetRequests();
    publisher.setIncludingNotes(true);
    publisher.setIncludingParameters(true);
    String withThem = documentText(publisher);

    assertFalse(withoutThem.contains("Runs after the customers"), withoutThem);
    assertFalse(withoutThem.contains("/data/out"), withoutThem);
    assertTrue(withThem.contains("Runs after the customers"), withThem);
    assertTrue(withThem.contains("/data/out"), withThem);
  }

  private String documentText(DataHubPublisher publisher) throws Exception {
    DataHubPublisher.Result result = publisher.publish("datahub", "${PROJECT_HOME}");
    assertTrue(result.isSuccess(), DataHubPublishCommand.describe(result));
    for (JsonNode proposal : proposalsSent()) {
      if (proposal.get("aspectName").asText().equals("documentInfo")) {
        JsonNode info = MAPPER.readTree(proposal.path("aspect").path("value").asText());
        if (info.path("title").asText().equals("Pipeline orders")) {
          return info.path("contents").path("text").asText();
        }
      }
    }
    throw new AssertionError("no document of the pipeline");
  }

  @Test
  void aDataHubWithoutDocumentsStillGetsEveryPipeline() throws Exception {
    // DataHub before 1.7 refuses documents
    gms.stubFor(
        post(urlEqualTo(INGEST))
            .atPriority(1)
            .withRequestBody(containing("\"entityType\":\"document\""))
            .willReturn(aResponse().withStatus(422).withBody("Unknown entity type document")));
    for (String name : List.of("customers", "orders", "invoices")) {
      PipelineMeta pipeline = new PipelineMeta();
      pipeline.setName(name);
      write(name + ".hpl", pipeline.getXml(variables));
    }
    variables.setVariable(DataHubPublisher.VAR_PROJECT_NAME, "sales");

    DataHubPublisher publisher =
        new DataHubPublisher(variables, metadata, new LogChannel("publish-test"));
    publisher.setIncludingDocumentation(true);
    DataHubPublisher.Result result = publisher.publish("datahub", "${PROJECT_HOME}");

    assertFalse(result.isSuccess());
    assertTrue(
        DataHubPublishCommand.describe(result).contains("needs DataHub 1.7 or later"),
        DataHubPublishCommand.describe(result));
    List<String> flows = new ArrayList<>();
    List<String> documentsTried = new ArrayList<>();
    for (var request : gms.findAll(postRequestedFor(urlEqualTo(INGEST)))) {
      for (JsonNode proposal : MAPPER.readTree(request.getBodyAsString()).get("proposals")) {
        String urn = proposal.get("entityUrn").asText();
        if (proposal.get("aspectName").asText().equals("dataFlowInfo") && !flows.contains(urn)) {
          flows.add(urn);
        }
        if (proposal.get("entityType").asText().equals("document")
            && !documentsTried.contains(urn)) {
          documentsTried.add(urn);
        }
      }
    }
    assertEquals(3, flows.size(), "every pipeline, delivered before the documents: " + flows);
    assertEquals(
        2, documentsTried.size(), "the project's and the first pipeline's: " + documentsTried);
  }

  @Test
  void withoutTheOptionNoDocumentsArePublished() throws Exception {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("orders");
    write("orders.hpl", pipeline.getXml(variables));

    new DataHubPublisher(variables, metadata, new LogChannel("publish-test"))
        .publish("datahub", "${PROJECT_HOME}");

    assertTrue(
        proposalsSent().stream().noneMatch(p -> p.get("entityType").asText().equals("document")));
  }

  @Test
  void onlyADataHubSinkCanBePublishedTo() {
    DataHubPublisher publisher =
        new DataHubPublisher(variables, metadata, new LogChannel("publish-test"));

    assertThrows(HopException.class, () -> publisher.publish("gone", "${PROJECT_HOME}"));
    assertThrows(HopException.class, () -> publisher.publish("datahub", "/no/such/folder"));
  }
}
