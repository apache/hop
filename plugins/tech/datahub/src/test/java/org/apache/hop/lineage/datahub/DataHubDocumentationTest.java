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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import io.openlineage.client.OpenLineage.RunEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.datahub.DataHubTranslator.Mcp;
import org.apache.hop.lineage.model.DocumentationLineagePayload;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.openlineage.OpenLineageEventMapper;
import org.junit.jupiter.api.Test;

/** The documentation of pipelines as DataHub documents. */
class DataHubDocumentationTest {
  private static final String FLOW = "urn:li:dataFlow:(hop,emea.orders.hpl,prod)";

  /** A publication: each has a sink, and a translator, of its own. */
  private static DataHubTranslator publication() {
    return new DataHubTranslator("PROD", "prod", "emea", "urn:li:corpuser:datahub", false);
  }

  private List<Mcp> translate(String project, String markdown) {
    return translate(publication(), project, markdown);
  }

  private List<Mcp> translate(DataHubTranslator translator, String project, String markdown) {
    OpenLineageEventMapper mapper =
        OpenLineageEventMapper.builder("hop", "https://hop.apache.org")
            .transformJobs(true)
            .fileSpecNaming(true)
            .build();
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.PIPELINE)
            .pipelineName("orders")
            .hopFilename("/projects/sales/orders.hpl")
            .hopFilenamePortableKey("${PROJECT_HOME}/orders.hpl")
            .logChannelId(UUID.randomUUID().toString())
            .build();
    List<Mcp> proposals = new ArrayList<>();
    for (RunEvent event :
        mapper.toRunEvents(
            LineageEvent.of(
                LineageEventKind.DOCUMENTATION,
                ctx,
                new DocumentationLineagePayload(
                    "Pipeline orders", markdown, project, List.of("Read", "Write"))))) {
      proposals.addAll(translator.translate(event).proposals());
    }
    return proposals;
  }

  private static JsonNode document(List<Mcp> proposals, String title) {
    List<Mcp> found =
        proposals.stream()
            .filter(p -> p.aspectName().equals("documentInfo"))
            .filter(p -> p.value().path("title").asText().equals(title))
            .toList();
    assertEquals(1, found.size(), title + " in " + proposals);
    assertEquals("document", found.get(0).entityType());
    assertTrue(found.get(0).entityUrn().startsWith("urn:li:document:hop-"), found.toString());
    return found.get(0).value();
  }

  private static String urnOf(List<Mcp> proposals, String title) {
    return proposals.stream()
        .filter(p -> p.aspectName().equals("documentInfo"))
        .filter(p -> p.value().path("title").asText().equals(title))
        .findFirst()
        .orElseThrow()
        .entityUrn();
  }

  private static JsonNode aspect(List<Mcp> proposals, String urn, String aspect) {
    return proposals.stream()
        .filter(p -> p.entityUrn().equals(urn) && p.aspectName().equals(aspect))
        .findFirst()
        .orElseThrow(() -> new AssertionError(aspect + " of " + urn + " in " + proposals))
        .value();
  }

  @Test
  void thePipelinesDocumentIsRelatedToItsFlowAndTasksUnderItsProject() {
    List<Mcp> proposals = translate("sales", "* Description : Loads the orders");

    assertEquals(
        2,
        proposals.stream().filter(p -> p.aspectName().equals("documentInfo")).count(),
        "the document and its project's: " + proposals);
    JsonNode info = document(proposals, "Pipeline orders");
    String urn = urnOf(proposals, "Pipeline orders");
    assertEquals(
        "Pipeline",
        aspect(proposals, urn, "subTypes").path("typeNames").get(0).asText(),
        "a pipeline's document");
    JsonNode platform = aspect(proposals, urn, "dataPlatformInstance");
    assertEquals("urn:li:dataPlatform:hop", platform.path("platform").asText());
    assertEquals(
        "urn:li:dataPlatformInstance:(urn:li:dataPlatform:hop,emea)",
        platform.path("instance").asText());
    assertEquals("* Description : Loads the orders", info.path("contents").path("text").asText());
    assertEquals("PUBLISHED", info.path("status").path("state").asText());
    assertEquals("EXTERNAL", info.path("source").path("sourceType").asText());
    assertEquals("${PROJECT_HOME}/orders.hpl", info.path("source").path("externalId").asText());
    List<String> related = new ArrayList<>();
    info.path("relatedAssets").forEach(asset -> related.add(asset.path("asset").asText()));
    assertEquals(
        List.of(FLOW, "urn:li:dataJob:(" + FLOW + ",Read)", "urn:li:dataJob:(" + FLOW + ",Write)"),
        related);
    assertEquals("urn:li:corpuser:datahub", info.path("created").path("actor").asText());
    assertTrue(info.path("lastModified").path("time").asLong() > 0);

    Mcp project =
        proposals.stream()
            .filter(p -> p.value().path("title").asText().equals("Hop project sales"))
            .findFirst()
            .orElseThrow();
    assertEquals(project.entityUrn(), info.path("parentDocument").path("document").asText());
    assertEquals("sales", project.value().path("customProperties").path("hopProject").asText());
  }

  @Test
  void theProjectsDocumentIsSentWithTheFirstDocumentOfAPublication() {
    DataHubTranslator translator = publication();
    List<Mcp> first = translate(translator, "sales", "first");
    List<Mcp> second = translate(translator, "sales", "second");

    assertEquals(6, first.size(), "the project's document and the pipeline's: " + first);
    assertEquals(3, second.size(), "the pipeline's: " + second);
  }

  @Test
  void publishingAgainUpdatesTheSameDocuments() {
    List<Mcp> first = translate("sales", "first");
    List<Mcp> again = translate("sales", "second");

    assertEquals(
        first.stream().map(Mcp::entityUrn).toList(), again.stream().map(Mcp::entityUrn).toList());
    assertNotEquals(
        first.stream().map(Mcp::entityUrn).toList(),
        translate("other", "first").stream().map(Mcp::entityUrn).toList(),
        "another project has another project document");
  }

  @Test
  void withoutAProjectTheDocumentHasNoParent() {
    List<Mcp> proposals = translate(null, "text");

    assertEquals(
        1,
        proposals.stream().filter(p -> p.aspectName().equals("documentInfo")).count(),
        proposals.toString());
    assertFalse(document(proposals, "Pipeline orders").has("parentDocument"));
  }
}
