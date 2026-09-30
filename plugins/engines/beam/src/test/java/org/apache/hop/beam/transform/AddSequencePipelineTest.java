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

package org.apache.hop.beam.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hop.beam.metadata.FileDefinition;
import org.apache.hop.beam.transforms.io.BeamOutputMeta;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.addsequence.AddSequenceMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Issue #2379: the Add Sequence transform has to produce one single increasing sequence on Beam,
 * not one per worker.
 *
 * <p>This is the assertion the generic handler cannot satisfy: it runs the real Hop transform
 * inside a {@code ParDo} per worker, so every worker restarts at the same value.
 */
class AddSequencePipelineTest extends SingleTransformPipelineTestBase {

  private static final int EXPECTED_ROWS = 100;

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @BeforeEach
  void registerTheTransform() throws Exception {
    // PipelineTestBase registers the transforms it knows about; Add Sequence has to be registered
    // explicitly for the plugin registry to resolve the "Sequence" id.
    //
    org.apache.hop.core.plugins.PluginRegistry.getInstance()
        .registerPluginClass(
            AddSequenceMeta.class.getName(),
            org.apache.hop.core.plugins.TransformPluginType.class,
            org.apache.hop.core.annotations.Transform.class);
  }

  private PipelineMeta sequencePipeline(long startAt, long incrementBy, String maxValue)
      throws Exception {
    FileDefinition fileDefinition =
        org.apache.hop.beam.util.BeamPipelineMetaUtil.createCustomersInputFileDefinition();
    fileDefinition.setName("CustomersSequence");
    metadataProvider.getSerializer(FileDefinition.class).save(fileDefinition);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("add-sequence");
    pipelineMeta.setMetadataProvider(metadataProvider);

    org.apache.hop.beam.transforms.io.BeamInputMeta inputMeta =
        new org.apache.hop.beam.transforms.io.BeamInputMeta();
    inputMeta.setInputLocation(INPUT_CUSTOMERS_FILE);
    inputMeta.setFileDefinitionName(fileDefinition.getName());
    TransformMeta inputTransformMeta = new TransformMeta("INPUT", inputMeta);
    inputTransformMeta.setTransformPluginId("BeamInput");
    pipelineMeta.addTransform(inputTransformMeta);

    AddSequenceMeta sequenceMeta = new AddSequenceMeta();
    sequenceMeta.setValueName("seq");
    sequenceMeta.setStartAt(Long.toString(startAt));
    sequenceMeta.setIncrementBy(Long.toString(incrementBy));
    sequenceMeta.setMaxValue(maxValue);
    // Both of these cannot be expressed on Beam and must not be silently ignored.
    sequenceMeta.setDatabaseUsed(false);
    sequenceMeta.setCounterUsed(false);
    TransformMeta sequenceTransformMeta = new TransformMeta("Sequence", sequenceMeta);
    sequenceTransformMeta.setTransformPluginId("Sequence");
    pipelineMeta.addTransform(sequenceTransformMeta);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(inputTransformMeta, sequenceTransformMeta));

    BeamOutputMeta outputMeta = new BeamOutputMeta();
    outputMeta.setOutputLocation(OUTPUT_FOLDER);
    outputMeta.setFileDefinitionName(null);
    outputMeta.setFilePrefix("sequenced");
    outputMeta.setFileSuffix(".csv");
    outputMeta.setWindowed(false);
    TransformMeta outputTransformMeta = new TransformMeta("OUTPUT", outputMeta);
    outputTransformMeta.setTransformPluginId("BeamOutput");
    pipelineMeta.addTransform(outputTransformMeta);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(sequenceTransformMeta, outputTransformMeta));

    return pipelineMeta;
  }

  /** The last column of every output line is the generated sequence value. */
  private List<String> sequenceValues(List<String> lines) {
    return lines.stream().map(line -> line.substring(line.lastIndexOf(',') + 1).trim()).toList();
  }

  @Test
  void everyRowGetsADistinctSequenceValue() throws Exception {
    PipelineMeta pipelineMeta = sequencePipeline(1, 1, "999999");

    List<String> lines = runAndGetOutputLines(pipelineMeta);

    assertEquals(EXPECTED_ROWS, lines.size(), "every input row should come out once");

    Set<String> distinct = new TreeSet<>(sequenceValues(lines));
    assertEquals(
        EXPECTED_ROWS,
        distinct.size(),
        "the sequence values must be unique across all workers, but there were only "
            + distinct.size()
            + " distinct values for "
            + EXPECTED_ROWS
            + " rows");
  }

  @Test
  void theSequenceStartsAtTheConfiguredValueAndIncrementsByTheConfiguredStep() throws Exception {
    PipelineMeta pipelineMeta = sequencePipeline(100, 5, "999999");

    List<String> values = sequenceValues(runAndGetOutputLines(pipelineMeta));

    Set<Long> numbers = new TreeSet<>();
    values.forEach(v -> numbers.add(Long.parseLong(v)));

    // Start at 100, step 5, 100 rows: 100, 105, ... 595.
    assertEquals(100L, numbers.iterator().next(), "the sequence should start at 100");
    assertEquals(100, numbers.size(), "expected 100 distinct sequence values");
    assertEquals(595L, ((TreeSet<Long>) numbers).last(), "the last value should be 595");
    assertTrue(
        numbers.stream().allMatch(n -> (n - 100) % 5 == 0),
        "every value should be 100 + a multiple of 5, got " + numbers);
  }

  @Test
  void rowsPastTheMaximumAreNotEmitted() throws Exception {
    // A max of 105 admits 100 and 105 only, so 98 of the 100 rows drop out.
    PipelineMeta pipelineMeta = sequencePipeline(100, 5, "105");

    List<String> lines = runAndGetOutputLines(pipelineMeta);

    assertEquals(2, lines.size(), "only the values up to the maximum should be emitted");
    Set<String> values = new TreeSet<>(sequenceValues(lines));
    assertEquals(Set.of("100", "105"), values);
  }

  @Test
  void theSequenceFieldIsAppendedToEveryRow() throws Exception {
    PipelineMeta pipelineMeta = sequencePipeline(1, 1, "999999");

    List<String> lines = runAndGetOutputLines(pipelineMeta);

    // The customers file definition has 10 fields, plus the sequence makes 11.
    for (String line : lines) {
      assertEquals(11, line.split(",", -1).length, "expected 11 fields, got: " + line);
    }
  }

  @Test
  void allInputRowsSurviveTheSequence() throws Exception {
    PipelineMeta pipelineMeta = sequencePipeline(1, 1, "999999");

    List<String> lines = runAndGetOutputLines(pipelineMeta);

    // The first column is the customer id; all 100 must still be there and untouched.
    Set<String> ids = new TreeSet<>();
    lines.forEach(line -> ids.add(line.split(",", -1)[0].trim()));
    assertEquals(EXPECTED_ROWS, ids.size(), "the sequence must not drop or alter the input rows");
    assertTrue(ids.contains("1"), "customer 1 should be in the output");
    assertTrue(ids.contains("100"), "customer 100 should be in the output");
    assertEquals(
        Arrays.stream(ids.toArray(new String[0])).distinct().count(),
        EXPECTED_ROWS,
        "each customer should appear exactly once");
  }
}
