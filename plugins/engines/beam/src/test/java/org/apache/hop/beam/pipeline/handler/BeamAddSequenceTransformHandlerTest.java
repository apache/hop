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

package org.apache.hop.beam.pipeline.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.coder.HopRowCoder;
import org.apache.hop.beam.engines.direct.BeamDirectPipelineRunConfiguration;
import org.apache.hop.beam.pipeline.HopPipelineMetaToBeamPipelineConverter;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.addsequence.AddSequenceMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Issue #2379: the Add Sequence transform needs a dedicated Beam handler. */
class BeamAddSequenceTransformHandlerTest {

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void sequenceIsAdvertisedAsExplicitlySupportedOnBeam() {
    // This is the contract BeamPipelineEngine.supports() reads, so it has to name the transform.
    assertTrue(
        HopPipelineMetaToBeamPipelineConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains("Sequence"),
        "Sequence must be advertised as explicitly supported on Beam");
    assertEquals("Sequence", BeamConst.STRING_ADD_SEQUENCE_PLUGIN_ID);
  }

  @Test
  void theHandlerIsNeitherAnInputNorAnOutput() {
    BeamAddSequenceTransformHandler handler = new BeamAddSequenceTransformHandler();

    assertFalse(handler.isInput(), "Add Sequence consumes the rows of the transform before it");
    assertFalse(handler.isOutput(), "Add Sequence is not the end of the chain");
  }

  @Test
  void theHandlerIsRegisteredForTheSequencePluginId() {
    // The converter's constructors both need a live pipeline and metadata store, so the
    // registration cannot be exercised from a unit test.  The static id set is the contract
    // BeamPipelineEngine.supports() reads and addDefaultTransformHandlers() is driven from, so
    // assert on that, plus the handler being instantiable and neither input nor output.
    assertTrue(
        HopPipelineMetaToBeamPipelineConverter.EXPLICIT_HANDLER_PLUGIN_IDS.contains(
            BeamConst.STRING_ADD_SEQUENCE_PLUGIN_ID),
        "the Sequence plugin id must be in EXPLICIT_HANDLER_PLUGIN_IDS");

    BeamAddSequenceTransformHandler handler = new BeamAddSequenceTransformHandler();
    assertNotNull(handler);
  }

  @Test
  void theSequenceMetaExposesTheRangeHopNeeds() {
    AddSequenceMeta meta = new AddSequenceMeta();
    meta.setValueName("seq");
    meta.setStartAt("100");
    meta.setIncrementBy("5");
    meta.setMaxValue("999");
    meta.setDatabaseUsed(false);

    assertEquals("seq", meta.getValueName());
    assertEquals("100", meta.getStartAt());
    assertEquals("5", meta.getIncrementBy());
    assertEquals("999", meta.getMaxValue());
    assertFalse(meta.isDatabaseUsed());
  }

  @Test
  void databaseModeUsesTheGenericHandler() throws Exception {
    Pipeline pipeline = Pipeline.create();
    String graph = handledGraph(pipeline, boundedRows(pipeline), true);

    assertFalse(graph.contains("AddSequenceFn"), graph);
    assertFalse(graph.contains("GroupByKey"), graph);
  }

  @Test
  void unboundedInputUsesTheGenericHandler() throws Exception {
    Pipeline pipeline = Pipeline.create();
    String graph = handledGraph(pipeline, unboundedRows(pipeline), false);

    assertFalse(graph.contains("AddSequenceFn"), graph);
    assertFalse(graph.contains("GroupByKey"), graph);
  }

  @Test
  void configurationTransformUsesTheGenericHandler() throws Exception {
    Pipeline pipeline = Pipeline.create();
    String graph = handledGraph(pipeline, boundedRows(pipeline), false, "Sequence settings");

    assertFalse(graph.contains("AddSequenceFn"), graph);
    assertFalse(graph.contains("GroupByKey"), graph);
  }

  @Test
  void boundedCounterUsesTheSequenceFunction() throws Exception {
    Pipeline pipeline = Pipeline.create();
    String graph = handledGraph(pipeline, boundedRows(pipeline), false);

    assertTrue(graph.contains("AddSequenceFn"), graph);
  }

  private static String handledGraph(
      Pipeline pipeline, PCollection<HopRow> input, boolean databaseUsed) throws Exception {
    return handledGraph(pipeline, input, databaseUsed, null);
  }

  private static String handledGraph(
      Pipeline pipeline,
      PCollection<HopRow> input,
      boolean databaseUsed,
      String configurationTransform)
      throws Exception {
    AddSequenceMeta meta = new AddSequenceMeta();
    meta.setValueName("seq");
    meta.setStartAt("1");
    meta.setIncrementBy("1");
    meta.setMaxValue("10");
    meta.setCounterUsed(!databaseUsed);
    meta.setDatabaseUsed(databaseUsed);
    if (databaseUsed) {
      meta.setConnection("customers");
      meta.setSequenceName("seq_id");
    }
    meta.setConfigurationTransform(configurationTransform);

    Map<String, PCollection<HopRow>> collections = new HashMap<>();
    new BeamAddSequenceTransformHandler()
        .handleTransform(
            LogChannel.GENERAL,
            new Variables(),
            "direct",
            new BeamDirectPipelineRunConfiguration(),
            null,
            new MemoryMetadataProvider(),
            new PipelineMeta(),
            new TransformMeta("Sequence", "seq", meta),
            collections,
            pipeline,
            new RowMeta(),
            List.of(),
            input,
            null);

    assertNotNull(collections.get("seq"));
    return graphText(pipeline);
  }

  /** Pipeline.toString() is only the pipeline id. The applied transforms carry the real names. */
  private static String graphText(Pipeline pipeline) {
    StringBuilder graph = new StringBuilder();
    pipeline.traverseTopologically(
        new Pipeline.PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            append(node);
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            append(node);
          }

          private void append(TransformHierarchy.Node node) {
            if (node.getTransform() == null) {
              return;
            }
            graph.append(node.getFullName()).append(' ');
            graph.append(node.getTransform().getClass().getName()).append(' ');
            graph.append(node.getTransform()).append('\n');
          }
        });
    return graph.toString();
  }

  private static PCollection<HopRow> boundedRows(Pipeline pipeline) {
    return pipeline.apply(Create.of(new HopRow(new Object[] {"a"})).withCoder(new HopRowCoder()));
  }

  private static PCollection<HopRow> unboundedRows(Pipeline pipeline) {
    return pipeline
        .apply(GenerateSequence.from(0))
        .apply(ParDo.of(new LongToHopRowFn()))
        .setCoder(new HopRowCoder());
  }

  private static class LongToHopRowFn extends DoFn<Long, HopRow> {
    @ProcessElement
    public void process(ProcessContext context) {
      context.output(new HopRow(new Object[] {context.element().toString()}));
    }
  }
}
