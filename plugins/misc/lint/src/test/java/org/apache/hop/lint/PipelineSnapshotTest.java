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
package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

/**
 * The background linter copies the open editor's content before handing it to a worker thread, so
 * the user keeps editing a model nobody else is iterating.
 *
 * <p>The copy goes through XML because Hop refuses to clone a {@link PipelineMeta} outright. These
 * tests pin both halves of that: that cloning really is unavailable, so the XML route is not
 * cargo-culted, and that a round-tripped snapshot still carries what the rules read.
 */
public class PipelineSnapshotTest {

  private final IVariables variables = Variables.getADefaultVariableSpace();

  private PipelineMeta pipelineWithContent() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("load-customers");
    pipelineMeta.setFilename("/projects/sales/load-customers.hpl");
    pipelineMeta.setDescription("Loads the customer dimension.");

    TransformMeta first = new TransformMeta();
    first.setName("Read");
    first.setTransformPluginId("TableInput");
    pipelineMeta.addTransform(first);

    TransformMeta second = new TransformMeta();
    second.setName("Write");
    second.setTransformPluginId("TableOutput");
    pipelineMeta.addTransform(second);

    return pipelineMeta;
  }

  /**
   * Hop rejects the obvious approach outright: "a pipeline can't be cloned without building new
   * external references". If that ever changes, the XML round-trip below can be simplified.
   */
  @Test
  public void pipelineMetaCannotSimplyBeCloned() {
    assertThrows(Exception.class, () -> pipelineWithContent().clone());
  }

  /** Mirrors what BackgroundLintService does: XML on the UI thread, parse on the worker. */
  @Test
  public void xmlSnapshotCarriesTheContentRulesRead() throws Exception {
    PipelineMeta source = pipelineWithContent();

    String xml = source.getXml(variables);
    PipelineMeta copy =
        new PipelineMeta(
            new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), null, variables);

    assertEquals("load-customers", copy.getName());
    assertEquals("Loads the customer dimension.", copy.getDescription());
    assertEquals(2, copy.getTransforms().size());
    assertTrue(copy.getTransforms().stream().anyMatch(t -> "Read".equals(t.getName())));
  }

  /** Editing the original after the snapshot must not change what the worker sees. */
  @Test
  public void snapshotIsIndependentOfLaterEdits() throws Exception {
    PipelineMeta source = pipelineWithContent();
    String xml = source.getXml(variables);

    TransformMeta added = new TransformMeta();
    added.setName("Added later");
    added.setTransformPluginId("Dummy");
    source.addTransform(added);
    source.setDescription("changed");

    PipelineMeta copy =
        new PipelineMeta(
            new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)), null, variables);

    assertEquals(2, copy.getTransforms().size());
    assertEquals("Loads the customer dimension.", copy.getDescription());
  }
}
