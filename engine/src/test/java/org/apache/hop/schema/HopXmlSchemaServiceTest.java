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

package org.apache.hop.schema;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import javax.xml.validation.Schema;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.schema.command.HopXmlSchemaCommand;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HopXmlSchemaServiceTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @Test
  void testGeneratePipelineSchema() throws Exception {
    HopXmlSchemaExportOptions options = new HopXmlSchemaExportOptions();
    String xsd = HopXmlSchemaService.getInstance().generatePipelineSchema(options);
    assertNotNull(xsd);
    assertTrue(xsd.contains("name=\"pipeline\""));

    // Compile schema
    Schema schema = HopXmlSchemaValidator.compileSchema(xsd);
    assertNotNull(schema);

    // Validate a PipelineMeta XML
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("test-pipeline");
    String pipelineXml = pipelineMeta.getXml(Variables.getADefaultVariableSpace());

    List<String> errors = HopXmlSchemaValidator.validateXml(pipelineXml, schema);
    assertTrue(errors.isEmpty(), "Pipeline XML should be valid: " + errors);

    // Validate sample file if present
    File sampleFile =
        new File("../plugins/transforms/abort/src/main/samples/transforms/abort-basic.hpl");
    if (!sampleFile.exists()) {
      sampleFile = new File("plugins/transforms/abort/src/main/samples/transforms/abort-basic.hpl");
    }
    if (sampleFile.exists()) {
      FileObject sampleVfs = HopVfs.getFileObject(sampleFile.getAbsolutePath());
      List<String> sampleErrors = HopXmlSchemaValidator.validateFile(sampleVfs, schema);
      assertTrue(sampleErrors.isEmpty(), "Sample abort-basic.hpl should be valid: " + sampleErrors);
    }
  }

  @Test
  void testGenerateWorkflowSchema() throws Exception {
    HopXmlSchemaExportOptions options = new HopXmlSchemaExportOptions();
    String xsd = HopXmlSchemaService.getInstance().generateWorkflowSchema(options);
    assertNotNull(xsd);
    assertTrue(xsd.contains("name=\"workflow\""));

    // Compile schema
    Schema schema = HopXmlSchemaValidator.compileSchema(xsd);
    assertNotNull(schema);

    // Validate a WorkflowMeta XML
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("test-workflow");
    String workflowXml = workflowMeta.getXml(Variables.getADefaultVariableSpace());

    List<String> errors = HopXmlSchemaValidator.validateXml(workflowXml, schema);
    assertTrue(errors.isEmpty(), "Workflow XML should be valid: " + errors);

    // Validate sample file if present
    File sampleFile =
        new File(
            "../plugins/actions/checkdbconnection/src/main/samples/actions/check-db-connections.hwf");
    if (!sampleFile.exists()) {
      sampleFile =
          new File(
              "plugins/actions/checkdbconnection/src/main/samples/actions/check-db-connections.hwf");
    }
    if (sampleFile.exists()) {
      FileObject sampleVfs = HopVfs.getFileObject(sampleFile.getAbsolutePath());
      List<String> sampleErrors = HopXmlSchemaValidator.validateFile(sampleVfs, schema);
      assertTrue(
          sampleErrors.isEmpty(),
          "Sample check-db-connections.hwf should be valid: " + sampleErrors);
    }
  }

  @Test
  void testExportAllSchemas() throws Exception {
    FileObject targetFolder = HopVfs.getFileObject(tempDir.resolve("schemas").toString());
    HopXmlSchemaExportOptions options = new HopXmlSchemaExportOptions();

    HopXmlSchemaExportResult result =
        HopXmlSchemaService.getInstance().exportAllSchemas(targetFolder, options);

    assertTrue(result.isPipelineSchemaGenerated());
    assertTrue(result.isWorkflowSchemaGenerated());
    assertTrue(targetFolder.resolveFile("pipeline.xsd").exists());
    assertTrue(targetFolder.resolveFile("workflow.xsd").exists());
  }

  @Test
  void testCommandExportAndValidate() throws Exception {
    // 1. Export via command
    Path schemaOut = tempDir.resolve("cmd_schemas");
    HopXmlSchemaCommand cmd = new HopXmlSchemaCommand();
    cmd.setTargetFolder(schemaOut.toString());
    cmd.setExportAll(true);
    assertDoesNotThrow(cmd::run);

    assertTrue(Files.exists(schemaOut.resolve("pipeline.xsd")));
    assertTrue(Files.exists(schemaOut.resolve("workflow.xsd")));

    // 2. Validate via command
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("cmd-validation-pipeline");
    String pipelineXml = pipelineMeta.getXml(Variables.getADefaultVariableSpace());
    Path pipelinePath = tempDir.resolve("cmd-test.hpl");
    Files.writeString(pipelinePath, pipelineXml);

    HopXmlSchemaCommand validateCmd = new HopXmlSchemaCommand();
    validateCmd.setValidateFile(pipelinePath.toString());
    assertDoesNotThrow(validateCmd::run);
  }

  @Test
  void testCommandLineParsingWithMatchingOptions() throws Exception {
    Path outDir = tempDir.resolve("cli_parsed_schemas");
    HopXmlSchemaCommand schemaCommand = new HopXmlSchemaCommand();
    picocli.CommandLine cmd = new picocli.CommandLine(schemaCommand);

    // Test parsing options matching the "Generate XML Schemas" dialog
    String[] args = {
      "-t",
      outDir.toString(),
      "--export-pipeline",
      "--export-workflow",
      "--export-transforms",
      "--export-actions",
      "-f",
      "Abort",
      "-s"
    };

    int exitCode = cmd.execute(args);
    assertTrue(exitCode == 0, "Command should succeed with exit code 0");
    assertTrue(schemaCommand.isExportPipeline());
    assertTrue(schemaCommand.isExportWorkflow());
    assertTrue(schemaCommand.isExportTransforms());
    assertTrue(schemaCommand.isExportActions());
    assertTrue(schemaCommand.isStrictOrder());
    org.junit.jupiter.api.Assertions.assertEquals("Abort", schemaCommand.getFilter());
    assertTrue(Files.exists(outDir.resolve("pipeline.xsd")));
    assertTrue(Files.exists(outDir.resolve("workflow.xsd")));
  }

  @Test
  void testCommandLineAliasRegistration() {
    HopXmlSchemaCommand schemaCmd = new HopXmlSchemaCommand();
    picocli.CommandLine subCmd = new picocli.CommandLine(schemaCmd);

    @picocli.CommandLine.Command(name = "hop")
    class DummyHopRoot implements Runnable {
      @Override
      public void run() {}
    }

    picocli.CommandLine root = new picocli.CommandLine(new DummyHopRoot());

    root.addSubcommand("xml-schema", subCmd);
    for (String alias : subCmd.getCommandSpec().aliases()) {
      if (!root.getSubcommands().containsKey(alias)) {
        root.addSubcommand(alias, subCmd);
      }
    }

    assertTrue(root.getSubcommands().containsKey("xml-schema"));
    assertTrue(root.getSubcommands().containsKey("schema"));
  }
}
