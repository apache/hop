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
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.datacatalog.DataCatalog;
import org.apache.hop.lineage.datahub.DataHubTranslator.Mcp;
import org.apache.hop.lineage.metadata.LineageSink;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.workflow.WorkflowMeta;
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
  void onlyADataHubSinkCanBePublishedTo() {
    DataHubPublisher publisher =
        new DataHubPublisher(variables, metadata, new LogChannel("publish-test"));

    assertThrows(HopException.class, () -> publisher.publish("gone", "${PROJECT_HOME}"));
    assertThrows(HopException.class, () -> publisher.publish("datahub", "/no/such/folder"));
  }
}
