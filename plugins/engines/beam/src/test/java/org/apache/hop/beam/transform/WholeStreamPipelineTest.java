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

package org.apache.hop.beam.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.beam.metadata.FileDefinition;
import org.apache.hop.beam.pipeline.HopPipelineMetaToBeamPipelineConverter;
import org.apache.hop.beam.transforms.io.BeamInputMeta;
import org.apache.hop.beam.transforms.io.BeamOutputMeta;
import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.groupby.Aggregation;
import org.apache.hop.pipeline.transforms.groupby.GroupByMeta;
import org.apache.hop.pipeline.transforms.groupby.GroupingField;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * How the Beam engines feed rows to a Hop transform: row by row with one completed batch per
 * bundle, or, for a transform that needs the whole stream, all rows at once with one end of input.
 */
class WholeStreamPipelineTest extends SingleTransformPipelineTestBase {

  private static final String EMPTY_FILE = "/tmp/customers/io/customers-none.txt";

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void registerTheTransforms() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginClass(
        BatchCountingTransformMeta.class.getName(), TransformPluginType.class, Transform.class);
    registry.registerPluginClass(
        LastRowOnlyTransformMeta.class.getName(), TransformPluginType.class, Transform.class);
    registry.registerPluginClass(
        GroupByMeta.class.getName(), TransformPluginType.class, Transform.class);
    BatchCountingTransform.ROWS.set(0);
    BatchCountingTransform.BATCHES.set(0);
    LastRowOnlyTransform.ROWS_AT_END.clear();
  }

  /**
   * Row by row a transform used to complete a batch after every row: a writer flushed, a group
   * ended, per row. Now the batch completes at the end of a bundle.
   */
  @Test
  void aBatchCompletesPerBundleNotPerRow() throws Exception {
    List<String> lines =
        runAndGetOutputLines(
            pipeline(
                "batch-per-bundle",
                INPUT_CUSTOMERS_FILE,
                new BatchCountingTransformMeta(),
                BatchCountingTransformMeta.ID));

    assertEquals(100, lines.size());
    assertEquals(100, BatchCountingTransform.ROWS.get());
    int batches = BatchCountingTransform.BATCHES.get();
    assertTrue(
        batches >= 1 && batches < 100,
        "expected a completed batch per bundle, not per row: " + batches + " for 100 rows");
  }

  /** Blocking transform and the like get all rows at once, and see the end of their input once. */
  @Test
  void aWholeStreamTransformSeesAllRowsAndOneEndOfInput() throws Exception {
    List<String> lines =
        runAndGetOutputLines(
            pipeline(
                "whole-stream",
                INPUT_CUSTOMERS_FILE,
                new LastRowOnlyTransformMeta(),
                LastRowOnlyTransformMeta.ID));

    assertEquals(1, lines.size(), "only the last row: " + lines);
    assertEquals(List.of(100), LastRowOnlyTransform.ROWS_AT_END);
  }

  /**
   * Without rows the transform still runs and sees the end of its input, as Detect empty stream
   * needs.
   */
  @Test
  void aWholeStreamTransformRunsOnAnEmptyInput() throws Exception {
    Files.writeString(new File(EMPTY_FILE).toPath(), "");
    clearOutputFolder();
    createRunPipeline(
        variables,
        pipeline(
            "whole-stream-empty",
            EMPTY_FILE,
            new LastRowOnlyTransformMeta(),
            LastRowOnlyTransformMeta.ID));

    assertEquals(List.of(0), LastRowOnlyTransform.ROWS_AT_END);
  }

  /** Group by without group fields aggregates all the rows, as on the local engine. */
  @Test
  void aGroupByWithoutGroupFieldsAggregatesAllRows() throws Exception {
    List<String> lines =
        runAndGetOutputLines(pipeline("group-all", INPUT_CUSTOMERS_FILE, countAll(), "GroupBy"));

    assertEquals(List.of("100"), lines.stream().map(String::trim).toList());
  }

  /** With group fields Group by needs sorted rows, which Beam does not keep: it is refused. */
  @Test
  void aGroupByWithGroupFieldsIsRefused() throws Exception {
    GroupByMeta groupBy = countAll();
    groupBy.setGroupingFields(new ArrayList<>(List.of(new GroupingField("state"))));
    PipelineMeta pipelineMeta = pipeline("group-state", INPUT_CUSTOMERS_FILE, groupBy, "GroupBy");

    Exception refused =
        assertThrows(Exception.class, () -> createRunPipeline(variables, pipelineMeta));

    assertTrue(
        causes(refused).contains(HopPipelineMetaToBeamPipelineConverter.GROUP_BY_WITH_GROUP_FIELDS),
        causes(refused));
  }

  private static GroupByMeta countAll() {
    GroupByMeta groupBy = new GroupByMeta();
    groupBy.setGroupingFields(new ArrayList<>());
    groupBy.setAggregations(
        new ArrayList<>(
            List.of(
                new Aggregation(
                    "rows",
                    "id",
                    Aggregation.typeGroupLongDesc[Aggregation.TYPE_GROUP_COUNT_ALL],
                    null))));
    return groupBy;
  }

  private static String causes(Throwable e) {
    StringBuilder messages = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause()) {
      messages.append(t.getMessage()).append('\n');
    }
    return messages.toString();
  }

  /** INPUT -> the transform -> OUTPUT. */
  private PipelineMeta pipeline(
      String name, String inputFile, ITransformMeta transform, String pluginId) throws Exception {
    FileDefinition fileDefinition = BeamPipelineMetaUtil.createCustomersInputFileDefinition();
    metadataProvider.getSerializer(FileDefinition.class).save(fileDefinition);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName(name);
    pipelineMeta.setMetadataProvider(metadataProvider);

    BeamInputMeta inputMeta = new BeamInputMeta();
    inputMeta.setInputLocation(inputFile);
    inputMeta.setFileDefinitionName(fileDefinition.getName());
    TransformMeta input = new TransformMeta("INPUT", inputMeta);
    input.setTransformPluginId("BeamInput");

    TransformMeta middle = new TransformMeta("transform", transform);
    middle.setTransformPluginId(pluginId);

    BeamOutputMeta outputMeta = new BeamOutputMeta();
    outputMeta.setOutputLocation(OUTPUT_FOLDER);
    outputMeta.setFileDefinitionName(null);
    outputMeta.setFilePrefix(name);
    outputMeta.setFileSuffix(".csv");
    outputMeta.setWindowed(false);
    TransformMeta output = new TransformMeta("OUTPUT", outputMeta);
    output.setTransformPluginId("BeamOutput");

    pipelineMeta.addTransform(input);
    pipelineMeta.addTransform(middle);
    pipelineMeta.addTransform(output);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(input, middle));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(middle, output));
    return pipelineMeta;
  }
}
