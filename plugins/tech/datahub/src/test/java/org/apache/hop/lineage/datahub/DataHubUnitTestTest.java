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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.OpenLineage.RunEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hop.lineage.context.LineageContext;
import org.apache.hop.lineage.context.LineageSubjectType;
import org.apache.hop.lineage.datahub.DataHubTranslator.Mcp;
import org.apache.hop.lineage.model.FileIoLineagePayload;
import org.apache.hop.lineage.model.FileIoOperation;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.LineageEventKind;
import org.apache.hop.lineage.model.LineagePayload;
import org.apache.hop.lineage.model.UnitTestLineagePayload;
import org.apache.hop.lineage.openlineage.OpenLineageEventMapper;
import org.junit.jupiter.api.Test;

/** Hop unit test results as DataHub assertions. */
class DataHubUnitTestTest {
  private static final String OUT =
      "urn:li:dataset:(urn:li:dataPlatform:file,/out/orders.csv,PROD)";

  private final DataHubTranslator translator =
      new DataHubTranslator("PROD", "prod", "emea", "urn:li:corpuser:datahub", false);

  private List<Mcp> translate(boolean passed, List<LineagePayload> tested) {
    OpenLineageEventMapper mapper =
        OpenLineageEventMapper.builder("hop", "https://hop.apache.org")
            .transformJobs(true)
            .fileSpecNaming(true)
            .build();
    LineageContext ctx =
        LineageContext.builder()
            .subjectType(LineageSubjectType.TRANSFORM)
            .pipelineName("orders")
            .transformName("Write")
            .hopFilename("/projects/sales/orders.hpl")
            .hopFilenamePortableKey("${PROJECT_HOME}/orders.hpl")
            .logChannelId(UUID.randomUUID().toString())
            .putAttribute("pipelineLogChannelId", UUID.randomUUID().toString())
            .build();
    List<Mcp> proposals = new ArrayList<>();
    for (RunEvent event :
        mapper.toRunEvents(
            LineageEvent.of(
                LineageEventKind.UNIT_TEST,
                ctx,
                new UnitTestLineagePayload(
                    "orders-test", "golden", passed, passed ? null : "row 3 differs", tested)))) {
      proposals.addAll(translator.translate(event).proposals());
    }
    return proposals;
  }

  private static Mcp only(List<Mcp> proposals, String aspect) {
    List<Mcp> found = proposals.stream().filter(p -> p.aspectName().equals(aspect)).toList();
    assertEquals(1, found.size(), aspect + " in " + proposals);
    return found.get(0);
  }

  private static List<LineagePayload> writesOrders() {
    return List.of(
        new FileIoLineagePayload(
            FileIoOperation.WRITE, null, "file:///out/orders.csv", null, true, null, null));
  }

  @Test
  void aFailedTestIsAFailedAssertionRunOnTheDatasetTheTransformWrites() {
    List<Mcp> proposals = translate(false, writesOrders());

    Mcp info = only(proposals, "assertionInfo");
    assertEquals("assertion", info.entityType());
    assertEquals("CUSTOM", info.value().path("type").asText());
    assertEquals(OUT, info.value().path("customAssertion").path("entity").asText());
    assertEquals("Hop unit test", info.value().path("customAssertion").path("type").asText());
    assertEquals("EXTERNAL", info.value().path("source").path("type").asText());
    assertEquals("orders-test", info.value().path("customProperties").path("hopUnitTest").asText());
    Mcp run = only(proposals, "assertionRunEvent");
    assertEquals(info.entityUrn(), run.entityUrn());
    assertEquals(OUT, run.value().path("asserteeUrn").asText());
    assertEquals("FAILURE", run.value().path("result").path("type").asText());
    assertEquals(
        "row 3 differs", run.value().path("result").path("nativeResults").path("comment").asText());
    assertEquals("FULL_TABLE", run.value().path("partitionSpec").path("type").asText());
    Mcp platform = only(proposals, "dataPlatformInstance");
    assertEquals("urn:li:dataPlatform:hop", platform.value().path("platform").asText());
    assertTrue(proposals.stream().noneMatch(p -> p.entityType().equals("dataProcessInstance")));
  }

  @Test
  void aRerunUpdatesTheSameAssertion() {
    String first = only(translate(false, writesOrders()), "assertionInfo").entityUrn();
    Mcp rerun = only(translate(true, writesOrders()), "assertionRunEvent");

    assertEquals(first, rerun.entityUrn());
    assertEquals("SUCCESS", rerun.value().path("result").path("type").asText());
    assertTrue(first.startsWith("urn:li:assertion:hop-"), first);
  }

  @Test
  void aTransformThatWritesNothingHasNoAssertion() {
    // DataHub takes assertions on datasets only
    assertTrue(translate(true, List.of()).isEmpty());
  }
}
