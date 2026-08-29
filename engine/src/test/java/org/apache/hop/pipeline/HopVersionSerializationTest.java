/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * The Hop version that created and last saved a pipeline or workflow is recorded in the file so a
 * project can be scanned for files built with an older release.
 *
 * <p>Files written before these elements existed must not silently claim to have been created by
 * the running version: the XML deserializer only assigns a field when its element is present, so
 * anything defaulted in the constructor would survive a load. Empty version strings are omitted
 * from XML (the same as a missing tag), so a subsequent save still does not invent a version.
 */
@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class HopVersionSerializationTest {

  private static final String CREATED_VERSION = "2.19.0";
  private static final String MODIFIED_VERSION = "2.20.0-SNAPSHOT";

  /** A pipeline as written before the created/modified Hop version elements existed. */
  private static final String PIPELINE_WITHOUT_VERSIONS =
      """
      <pipeline>
        <info>
          <name>no-hop-versions</name>
          <created_user>-</created_user>
          <created_date>2023/09/16 22:31:19.820</created_date>
          <modified_user>-</modified_user>
          <modified_date>2023/09/16 22:31:19.820</modified_date>
        </info>
      </pipeline>
      """;

  /** A workflow as written before the created/modified Hop version elements existed. */
  private static final String WORKFLOW_WITHOUT_VERSIONS =
      """
      <workflow>
        <name>no-hop-versions</name>
        <created_user>-</created_user>
        <created_date>2023/09/16 22:31:19.820</created_date>
        <modified_user>-</modified_user>
        <modified_date>2023/09/16 22:31:19.820</modified_date>
      </workflow>
      """;

  /** A pipeline where the created version was backfilled by hand, without a modified version. */
  private static final String PIPELINE_WITH_BACKFILLED_VERSION =
      """
      <pipeline>
        <info>
          <name>backfilled</name>
          <created_hop_version>1.2.0</created_hop_version>
        </info>
      </pipeline>
      """;

  /** A workflow where the created version was backfilled by hand, without a modified version. */
  private static final String WORKFLOW_WITH_BACKFILLED_VERSION =
      """
      <workflow>
        <name>backfilled</name>
        <created_hop_version>1.2.0</created_hop_version>
      </workflow>
      """;

  private final IVariables variables = new Variables();
  private final IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();

  private PipelineMeta loadPipeline(String xml) throws Exception {
    return new PipelineMeta(
        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
        metadataProvider,
        variables);
  }

  private WorkflowMeta loadWorkflow(String xml) throws Exception {
    return new WorkflowMeta(
        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)),
        metadataProvider,
        variables);
  }

  @Test
  void pipelineKeepsHopVersionsAcrossSaveAndLoad() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("with-hop-versions");
    pipelineMeta.setCreatedHopVersion(CREATED_VERSION);
    pipelineMeta.setModifiedHopVersion(MODIFIED_VERSION);

    String xml = pipelineMeta.getXml(variables);
    assertTrue(xml.contains("<created_hop_version>" + CREATED_VERSION + "</created_hop_version>"));
    assertTrue(
        xml.contains("<modified_hop_version>" + MODIFIED_VERSION + "</modified_hop_version>"));

    PipelineMeta loaded = loadPipeline(xml);
    assertEquals(CREATED_VERSION, loaded.getCreatedHopVersion());
    assertEquals(MODIFIED_VERSION, loaded.getModifiedHopVersion());
  }

  @Test
  void workflowKeepsHopVersionsAcrossSaveAndLoad() throws Exception {
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("with-hop-versions");
    workflowMeta.setCreatedHopVersion(CREATED_VERSION);
    workflowMeta.setModifiedHopVersion(MODIFIED_VERSION);

    String xml = workflowMeta.getXml(variables);
    assertTrue(xml.contains("<created_hop_version>" + CREATED_VERSION + "</created_hop_version>"));
    assertTrue(
        xml.contains("<modified_hop_version>" + MODIFIED_VERSION + "</modified_hop_version>"));

    WorkflowMeta loaded = loadWorkflow(xml);
    assertEquals(CREATED_VERSION, loaded.getCreatedHopVersion());
    assertEquals(MODIFIED_VERSION, loaded.getModifiedHopVersion());
  }

  @Test
  void pipelineWithoutHopVersionsDoesNotClaimTheRunningVersion() throws Exception {
    PipelineMeta loaded = loadPipeline(PIPELINE_WITHOUT_VERSIONS);

    assertEquals("", loaded.getCreatedHopVersion());
    assertEquals("", loaded.getModifiedHopVersion());

    // Empty versions are omitted, same as a file that never had these elements.
    String xml = loaded.getXml(variables);
    assertFalse(xml.contains("<created_hop_version"));
    assertFalse(xml.contains("<modified_hop_version"));
  }

  @Test
  void workflowWithoutHopVersionsDoesNotClaimTheRunningVersion() throws Exception {
    WorkflowMeta loaded = loadWorkflow(WORKFLOW_WITHOUT_VERSIONS);

    assertEquals("", loaded.getCreatedHopVersion());
    assertEquals("", loaded.getModifiedHopVersion());

    String xml = loaded.getXml(variables);
    assertFalse(xml.contains("<created_hop_version"));
    assertFalse(xml.contains("<modified_hop_version"));
  }

  @Test
  void pipelineKeepsAManuallyBackfilledCreatedHopVersion() throws Exception {
    PipelineMeta loaded = loadPipeline(PIPELINE_WITH_BACKFILLED_VERSION);

    // Whatever is in the file is kept verbatim, nothing overwrites or validates it.
    assertEquals("1.2.0", loaded.getCreatedHopVersion());
    assertEquals("", loaded.getModifiedHopVersion());

    String xml = loaded.getXml(variables);
    assertTrue(xml.contains("<created_hop_version>1.2.0</created_hop_version>"));
    assertFalse(xml.contains("<modified_hop_version"));
  }

  @Test
  void workflowKeepsAManuallyBackfilledCreatedHopVersion() throws Exception {
    WorkflowMeta loaded = loadWorkflow(WORKFLOW_WITH_BACKFILLED_VERSION);

    assertEquals("1.2.0", loaded.getCreatedHopVersion());
    assertEquals("", loaded.getModifiedHopVersion());

    String xml = loaded.getXml(variables);
    assertTrue(xml.contains("<created_hop_version>1.2.0</created_hop_version>"));
    assertFalse(xml.contains("<modified_hop_version"));
  }
}
