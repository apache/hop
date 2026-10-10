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

package org.apache.hop.beam.transforms.window;

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
 * Issue #2275: the Beam window transform should support windowing on a key.
 *
 * <p>This is not a metadata-only change. Beam needs KV&lt;K, V&gt; before it can group rows, so the
 * pipeline has to key, window, group and drop the key again. Beam validates the shape of the graph
 * at construction, so running the real pipeline is the only way to know that still works.
 */
class BeamWindowKeyedPipelineTest
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
            BeamWindowMeta.class.getName(),
            org.apache.hop.core.plugins.TransformPluginType.class,
            org.apache.hop.core.annotations.Transform.class);
  }

  private PipelineMeta windowPipeline(String keyField) throws Exception {
    FileDefinition fileDefinition =
        org.apache.hop.beam.util.BeamPipelineMetaUtil.createCustomersInputFileDefinition();
    fileDefinition.setName("CustomersWindow");
    metadataProvider.getSerializer(FileDefinition.class).save(fileDefinition);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("beam-window-keyed");
    pipelineMeta.setMetadataProvider(metadataProvider);

    BeamInputMeta inputMeta = new BeamInputMeta();
    inputMeta.setInputLocation(INPUT_CUSTOMERS_FILE);
    inputMeta.setFileDefinitionName(fileDefinition.getName());
    TransformMeta inputTransformMeta = new TransformMeta("INPUT", inputMeta);
    inputTransformMeta.setTransformPluginId("BeamInput");
    pipelineMeta.addTransform(inputTransformMeta);

    BeamWindowMeta windowMeta = new BeamWindowMeta();
    windowMeta.setWindowType(org.apache.hop.beam.core.BeamDefaults.WINDOW_TYPE_GLOBAL);
    windowMeta.setDuration("60");
    windowMeta.setAllowedLateness("0");
    windowMeta.setDiscardingFiredPanes(false);
    windowMeta.setTriggeringType(org.apache.hop.beam.transforms.window.WindowTriggerType.None);
    windowMeta.setKeyField(keyField);
    TransformMeta windowTransformMeta = new TransformMeta("WINDOW", windowMeta);
    windowTransformMeta.setTransformPluginId("BeamWindow");
    pipelineMeta.addTransform(windowTransformMeta);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(inputTransformMeta, windowTransformMeta));

    BeamOutputMeta outputMeta = new BeamOutputMeta();
    outputMeta.setOutputLocation(OUTPUT_FOLDER);
    outputMeta.setFileDefinitionName(null);
    outputMeta.setFilePrefix("windowed");
    outputMeta.setFileSuffix(".csv");
    outputMeta.setWindowed(false);
    TransformMeta outputTransformMeta = new TransformMeta("OUTPUT", outputMeta);
    outputTransformMeta.setTransformPluginId("BeamOutput");
    pipelineMeta.addTransform(outputTransformMeta);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(windowTransformMeta, outputTransformMeta));

    return pipelineMeta;
  }

  @Test
  void windowingOnAKeyKeepsEveryRow() throws Exception {
    // The first column of the customers file is the customer id, so keying on it exercises the
    // per-key grouping over 100 rows spread across 100 distinct keys.
    //
    PipelineMeta pipelineMeta = windowPipeline("id");

    List<String> lines = runAndGetOutputLines(pipelineMeta);

    assertEquals(
        EXPECTED_ROWS,
        lines.size(),
        "windowing groups and re-emits rows, it must not drop any of them, got: "
            + lines.size()
            + " lines");
  }

  @Test
  void windowingOnAKeyDoesNotAddAColumn() throws Exception {
    PipelineMeta pipelineMeta = windowPipeline("id");

    List<String> lines = runAndGetOutputLines(pipelineMeta);

    // The customers file definition has 10 fields. The key is a field that is already on the row,
    // so it must not turn into an extra output column.
    //
    for (String line : lines) {
      assertEquals(10, line.split(",", -1).length, "expected 10 fields, got: " + line);
    }
  }

  @Test
  void allRowsComeOutUnchangedByTheKeying() throws Exception {
    PipelineMeta pipelineMeta = windowPipeline("id");

    List<String> lines = runAndGetOutputLines(pipelineMeta);

    Set<String> ids = new TreeSet<>();
    lines.forEach(line -> ids.add(line.split(",", -1)[0].trim()));

    assertEquals(
        EXPECTED_ROWS, ids.size(), "every customer should survive keyed windowing exactly once");
    assertTrue(ids.contains("1"), "customer 1 should be in the output");
    assertTrue(ids.contains("100"), "customer 100 should be in the output");
  }

  @Test
  void anUnknownKeyFieldFailsLoudly() throws Exception {
    // The keyed branch is only really exercised if a bad key field is rejected: with a valid one
    // the row count is identical to the global path, because a GLOBAL window per key and a
    // GLOBAL window overall both re-emit every row.
    //
    PipelineMeta pipelineMeta = windowPipeline("no_such_field");

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
    assertTrue(mentionsKey, "the failure should name the missing key field, but got: " + failure);
  }

  @Test
  void noKeyFieldGivesTheOriginalGlobalWindowing() throws Exception {
    // A transform saved before #2275 has no key field, so the blank case must still run.
    //
    PipelineMeta pipelineMeta = windowPipeline("");

    List<String> lines = runAndGetOutputLines(pipelineMeta);

    assertEquals(EXPECTED_ROWS, lines.size(), "the unkeyed path must keep working");
  }
}
