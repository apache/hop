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

package org.apache.hop.beam.transforms.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hop.beam.metadata.FileDefinition;
import org.apache.hop.beam.transforms.io.BeamInputMeta;
import org.apache.hop.beam.transforms.io.BeamOutputMeta;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Issue #2040: partitioning support for Beam pipelines.
 *
 * <p>The row count is the assertion that matters here. Beam's Partition transform hands back one
 * PCollection per partition, and reading only the first of those drops every row that landed in the
 * others. That failure is silent, so it gets its own test with several partitions.
 */
class BeamPartitionPipelineTest
    extends org.apache.hop.beam.transform.SingleTransformPipelineTestBase {

  private static final int EXPECTED_ROWS = 100;

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @BeforeEach
  void registerTheTransform() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            BeamPartitionMeta.class.getName(),
            org.apache.hop.core.plugins.TransformPluginType.class,
            org.apache.hop.core.annotations.Transform.class);
  }

  private PipelineMeta partitionPipeline(
      String partitionType, String keyField, String numPartitions) throws Exception {
    FileDefinition fileDefinition =
        org.apache.hop.beam.util.BeamPipelineMetaUtil.createCustomersInputFileDefinition();
    fileDefinition.setName("CustomersPartition");
    metadataProvider.getSerializer(FileDefinition.class).save(fileDefinition);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("beam-partition");
    pipelineMeta.setMetadataProvider(metadataProvider);

    BeamInputMeta inputMeta = new BeamInputMeta();
    inputMeta.setInputLocation(INPUT_CUSTOMERS_FILE);
    inputMeta.setFileDefinitionName(fileDefinition.getName());
    TransformMeta inputTransformMeta = new TransformMeta("INPUT", inputMeta);
    inputTransformMeta.setTransformPluginId("BeamInput");
    pipelineMeta.addTransform(inputTransformMeta);

    BeamPartitionMeta partitionMeta = new BeamPartitionMeta();
    partitionMeta.setPartitionType(partitionType);
    partitionMeta.setKeyField(keyField);
    partitionMeta.setNumPartitions(numPartitions);
    TransformMeta partitionTransformMeta = new TransformMeta("PARTITION", partitionMeta);
    partitionTransformMeta.setTransformPluginId("BeamPartition");
    pipelineMeta.addTransform(partitionTransformMeta);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(inputTransformMeta, partitionTransformMeta));

    BeamOutputMeta outputMeta = new BeamOutputMeta();
    outputMeta.setOutputLocation(OUTPUT_FOLDER);
    outputMeta.setFileDefinitionName(null);
    outputMeta.setFilePrefix("partitioned");
    outputMeta.setFileSuffix(".csv");
    outputMeta.setWindowed(false);
    TransformMeta outputTransformMeta = new TransformMeta("OUTPUT", outputMeta);
    outputTransformMeta.setTransformPluginId("BeamOutput");
    pipelineMeta.addTransform(outputTransformMeta);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(partitionTransformMeta, outputTransformMeta));

    return pipelineMeta;
  }

  @Test
  void partitioningIntoSeveralPartitionsLosesNoRows() throws Exception {
    // The regression this guards: taking one partition out of Beam's PCollectionList drops
    // everything that went to the others.
    //
    PipelineMeta pipelineMeta = partitionPipeline("Key", "id", "8");

    List<String> lines = runAndGetOutputLines(pipelineMeta);

    assertEquals(
        EXPECTED_ROWS,
        lines.size(),
        "all rows have to survive partitioning, got " + lines.size() + " lines");
  }

  @Test
  void theSinglePartitionModeLosesNoRows() throws Exception {
    PipelineMeta pipelineMeta = partitionPipeline("Single", "", "1");

    List<String> lines = runAndGetOutputLines(pipelineMeta);

    assertEquals(EXPECTED_ROWS, lines.size(), "the single-partition mode must pass every row");
  }

  @Test
  void partitioningDoesNotChangeTheRowLayout() throws Exception {
    PipelineMeta pipelineMeta = partitionPipeline("Key", "id", "4");

    List<String> lines = runAndGetOutputLines(pipelineMeta);

    // The customers file has 10 fields; partitioning decides placement, never columns.
    //
    for (String line : lines) {
      assertEquals(10, line.split(",", -1).length, "expected 10 fields, got: " + line);
    }
  }

  @Test
  void everyCustomerComesOutExactlyOnce() throws Exception {
    PipelineMeta pipelineMeta = partitionPipeline("Key", "id", "8");

    List<String> lines = runAndGetOutputLines(pipelineMeta);

    Set<String> ids = new TreeSet<>();
    lines.forEach(line -> ids.add(line.split(",", -1)[0].trim()));

    assertEquals(EXPECTED_ROWS, ids.size(), "expected all 100 customer ids");
    assertTrue(ids.contains("1"), "customer 1 should be present");
    assertTrue(ids.contains("100"), "customer 100 should be present");
  }

  @Test
  void anUnknownKeyFieldIsRejected() throws Exception {
    PipelineMeta pipelineMeta = partitionPipeline("Key", "no_such_field", "4");

    Exception failure = assertThrows(Exception.class, () -> runAndGetOutputLines(pipelineMeta));

    Throwable cause = failure;
    boolean mentionsKey = false;
    while (cause != null) {
      String message = cause.getMessage() == null ? "" : cause.getMessage();
      if (message.contains("no_such_field")) {
        mentionsKey = true;
      }
      cause = cause.getCause();
    }
    assertTrue(mentionsKey, "the error should name the missing field, got: " + failure);
  }

  @Test
  void theKeyedModeNeedsAKeyField() throws Exception {
    PipelineMeta pipelineMeta = partitionPipeline("Key", "", "4");

    Exception failure = assertThrows(Exception.class, () -> runAndGetOutputLines(pipelineMeta));

    Throwable cause = failure;
    boolean mentionsKey = false;
    while (cause != null) {
      String message = cause.getMessage() == null ? "" : cause.getMessage();
      if (message.contains("key field")) {
        mentionsKey = true;
      }
      cause = cause.getCause();
    }
    assertTrue(
        mentionsKey, "keyed partitioning without a key field should say so, got: " + failure);
  }
}
