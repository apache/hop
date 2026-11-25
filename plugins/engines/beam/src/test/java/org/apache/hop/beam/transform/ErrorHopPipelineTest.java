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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hop.beam.metadata.FileDefinition;
import org.apache.hop.beam.transforms.io.BeamInputMeta;
import org.apache.hop.beam.transforms.io.BeamOutputMeta;
import org.apache.hop.beam.util.BeamPipelineMetaUtil;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A generic transform with an error hop on Beam: its output rows and its rows with errors leave it
 * on two outputs, each with its own layout.
 */
class ErrorHopPipelineTest extends SingleTransformPipelineTestBase {

  @BeforeAll
  static void initHopEnvironment() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void registerTheTransform() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            ErrorRoutingTransformMeta.class.getName(), TransformPluginType.class, Transform.class);
  }

  private static TransformMeta output(String name, String prefix) {
    BeamOutputMeta outputMeta = new BeamOutputMeta();
    outputMeta.setOutputLocation(OUTPUT_FOLDER);
    outputMeta.setFileDefinitionName(null);
    outputMeta.setFilePrefix(prefix);
    outputMeta.setFileSuffix(".csv");
    outputMeta.setWindowed(false);
    TransformMeta transformMeta = new TransformMeta(name, outputMeta);
    transformMeta.setTransformPluginId("BeamOutput");
    return transformMeta;
  }

  /** INPUT (100 customers) -> route -> OK, and route -(error hop)-> ERRORS. */
  private PipelineMeta errorHopPipeline(IHopMetadataProvider metadataProvider) throws Exception {
    FileDefinition fileDefinition = BeamPipelineMetaUtil.createCustomersInputFileDefinition();
    fileDefinition.setName("CustomersErrorHop");
    metadataProvider.getSerializer(FileDefinition.class).save(fileDefinition);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("error-hop");
    pipelineMeta.setMetadataProvider(metadataProvider);

    BeamInputMeta inputMeta = new BeamInputMeta();
    inputMeta.setInputLocation(INPUT_CUSTOMERS_FILE);
    inputMeta.setFileDefinitionName(fileDefinition.getName());
    TransformMeta input = new TransformMeta("INPUT", inputMeta);
    input.setTransformPluginId("BeamInput");
    TransformMeta route = new TransformMeta("route", new ErrorRoutingTransformMeta());
    route.setTransformPluginId(ErrorRoutingTransformMeta.ID);
    TransformMeta ok = output("OK", "ok");
    TransformMeta errors = output("ERRORS", "errors");
    pipelineMeta.addTransform(input);
    pipelineMeta.addTransform(route);
    pipelineMeta.addTransform(ok);
    pipelineMeta.addTransform(errors);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(input, route));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(route, ok));
    pipelineMeta.addPipelineHop(new PipelineHopMeta(route, errors));
    TransformErrorMeta errorMeta =
        new TransformErrorMeta(
            route, errors, "nr_errors", "error_description", "error_fields", "error_codes");
    errorMeta.setEnabled(true);
    route.setTransformErrorMeta(errorMeta);
    return pipelineMeta;
  }

  @Test
  void outputAndErrorRowsLeaveOnTwoOutputs() throws Exception {
    List<String> okLines = new ArrayList<>();
    List<String> errorLines = new ArrayList<>();
    clearOutputFolder();
    createRunPipeline(variables, errorHopPipeline(metadataProvider));
    for (java.io.File file :
        new java.io.File(OUTPUT_FOLDER).listFiles((dir, name) -> !name.startsWith("."))) {
      List<String> target = file.getName().startsWith("errors") ? errorLines : okLines;
      for (String line : java.nio.file.Files.readAllLines(file.toPath())) {
        if (!line.isBlank()) {
          target.add(line);
        }
      }
    }

    assertEquals(80, okLines.size(), "the rows without errors");
    for (String line : okLines) {
      String[] fields = line.split(",", -1);
      assertEquals(11, fields.length, "the customer fields plus doubled: " + line);
      long id = Long.parseLong(fields[0].trim());
      assertTrue(id % 5 != 0, "an error row in the output: " + line);
      assertEquals(id * 2, Long.parseLong(fields[10].trim()));
    }

    assertEquals(20, errorLines.size(), "the rows with errors");
    Set<Long> ids = new TreeSet<>();
    for (String line : errorLines) {
      String[] fields = line.split(",", -1);
      assertEquals(14, fields.length, "the customer fields plus the error fields: " + line);
      long id = Long.parseLong(fields[0].trim());
      ids.add(id);
      assertEquals("1", fields[10].trim());
      assertEquals("multiple of five: " + id, fields[11]);
      assertEquals("n", fields[12]);
      assertEquals("FIVE", fields[13]);
    }
    assertEquals(20, ids.size());
    assertTrue(ids.stream().allMatch(id -> id % 5 == 0), "ids " + ids);
  }
}
