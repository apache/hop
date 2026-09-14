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

package org.apache.hop.diagram.command;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DiagramExportCommandTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void initHop() throws Exception {
    org.apache.hop.core.HopEnvironment.init();
  }

  @Test
  void testListExporters() {
    DiagramExportCommand cmd = new DiagramExportCommand();
    cmd.setListExporters(true);
    assertDoesNotThrow(cmd::run);
  }

  @Test
  void testExportSinglePipeline() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("cli-test-pipeline");
    String pipelineXml = pipelineMeta.getXml(new org.apache.hop.core.variables.Variables());

    Path pipelineFile = tempDir.resolve("test-pipeline.hpl");
    Files.writeString(pipelineFile, pipelineXml);

    Path outputFile = tempDir.resolve("output.mmd");

    DiagramExportCommand cmd = new DiagramExportCommand();
    cmd.setFilename(pipelineFile.toString());
    cmd.setOutputFilename(outputFile.toString());
    cmd.setFormat("MERMAID");

    assertDoesNotThrow(cmd::run);

    assertTrue(Files.exists(outputFile));
    String content = Files.readString(outputFile);
    assertTrue(content.contains("flowchart LR"));
  }

  @Test
  void testBatchExport() throws Exception {
    Path sourceFolder = tempDir.resolve("source");
    Path targetFolder = tempDir.resolve("target");
    Files.createDirectories(sourceFolder);
    Files.createDirectories(targetFolder);

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("batch-pipe");
    Files.writeString(
        sourceFolder.resolve("pipe.hpl"),
        pipelineMeta.getXml(new org.apache.hop.core.variables.Variables()));

    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("batch-flow");
    Files.writeString(
        sourceFolder.resolve("flow.hwf"),
        workflowMeta.getXml(new org.apache.hop.core.variables.Variables()));

    DiagramExportCommand cmd = new DiagramExportCommand();
    cmd.setSourceFolder(sourceFolder.toString());
    cmd.setTargetFolder(targetFolder.toString());
    cmd.setFormat("SVG");

    assertDoesNotThrow(cmd::run);

    assertTrue(Files.exists(targetFolder.resolve("pipe.svg")));
    assertTrue(Files.exists(targetFolder.resolve("flow.svg")));
  }
}
