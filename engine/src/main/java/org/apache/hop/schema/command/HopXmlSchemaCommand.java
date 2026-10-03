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

package org.apache.hop.schema.command;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import javax.xml.validation.Schema;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.hop.plugin.HopCommand;
import org.apache.hop.hop.plugin.IHopCommand;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.schema.HopXmlSchemaExportOptions;
import org.apache.hop.schema.HopXmlSchemaExportResult;
import org.apache.hop.schema.HopXmlSchemaService;
import org.apache.hop.schema.HopXmlSchemaValidator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Hop CLI command for generating and validating XML schemas. */
@Getter
@Setter
@Command(
    name = "xml-schema",
    aliases = {"schema"},
    description =
        "Generate or validate XML schemas for Hop pipelines, workflows, transforms, and actions",
    mixinStandardHelpOptions = true,
    versionProvider = HopVersionProvider.class)
@HopCommand(id = "xml-schema", description = "Generate and validate XML schemas")
public class HopXmlSchemaCommand implements Runnable, IHopCommand, IHasHopMetadataProvider {

  @Option(
      names = {"-t", "--target-folder"},
      description = "Target folder for generated XML schemas (supports VFS URLs)")
  private String targetFolder;

  @Option(
      names = {"-p", "--pipeline", "--export-pipeline"},
      description = "Export pipeline XML schema (pipeline.xsd)")
  private boolean exportPipeline;

  @Option(
      names = {"-w", "--workflow", "--export-workflow"},
      description = "Export workflow XML schema (workflow.xsd)")
  private boolean exportWorkflow;

  @Option(
      names = {"--transforms", "--export-transforms"},
      description = "Export individual transform XML schemas (transforms/*.xsd)")
  private boolean exportTransforms;

  @Option(
      names = {"--actions", "--export-actions"},
      description = "Export individual action XML schemas (actions/*.xsd)")
  private boolean exportActions;

  @Option(
      names = {"-a", "--all"},
      description = "Export all schemas (pipeline, workflow, transforms, actions)")
  private boolean exportAll;

  @Option(
      names = {"-f", "--filter"},
      description = "Plugin ID filter pattern (regex or substring) for transforms and actions")
  private String filter;

  @Option(
      names = {"-s", "--strict-order"},
      description = "Enforce strict class declaration order instead of flexible element order")
  private boolean strictOrder;

  @Option(
      names = {"-v", "--validate"},
      description = "Validate a pipeline (.hpl) or workflow (.hwf) file against XML schema")
  private String validateFile;

  private CommandLine cmd;
  private IVariables variables;
  private MultiMetadataProvider metadataProvider;
  private ILogChannel log;

  public HopXmlSchemaCommand() {
    this.variables = Variables.getADefaultVariableSpace();
    this.log = new LogChannel("hop-xml-schema");
  }

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    this.cmd = cmd;
    this.variables = variables != null ? variables : Variables.getADefaultVariableSpace();
    this.metadataProvider = metadataProvider;
    this.log = new LogChannel("hop-xml-schema");
  }

  @Override
  public void run() {
    try {
      System.setProperty(Const.HOP_PLATFORM_RUNTIME, "XML_SCHEMA");
      if (log == null) {
        log = new LogChannel("hop-xml-schema");
      }
      if (variables == null) {
        variables = Variables.getADefaultVariableSpace();
      }

      handleMixinActions();

      if (StringUtils.isNotEmpty(validateFile)) {
        doValidate();
        return;
      }

      if (StringUtils.isNotEmpty(targetFolder)) {
        doExport();
        return;
      }

      log.logBasic(
          "No target folder (-t) or file to validate (-v) specified. Use --help for usage.");
      if (cmd != null) {
        cmd.usage(System.out);
      }
    } catch (Exception e) {
      log.logError("Error executing schema command: " + e.getMessage(), e);
      throw new RuntimeException("Schema command failed", e);
    }
  }

  protected void handleMixinActions() throws HopException {
    if (cmd == null) {
      return;
    }
    Map<String, Object> mixins = cmd.getMixins();
    for (String key : mixins.keySet()) {
      Object mixin = mixins.get(key);
      if (mixin instanceof IConfigOptions configOptions) {
        configOptions.handleOption(log, this, variables);
      }
    }
  }

  private void doExport() throws Exception {
    boolean anySelected = exportPipeline || exportWorkflow || exportTransforms || exportActions;
    if (!anySelected || exportAll) {
      exportPipeline = true;
      exportWorkflow = true;
      exportTransforms = true;
      exportActions = true;
    }

    HopXmlSchemaExportOptions options = new HopXmlSchemaExportOptions();
    options.setExportPipeline(exportPipeline);
    options.setExportWorkflow(exportWorkflow);
    options.setExportTransforms(exportTransforms);
    options.setExportActions(exportActions);
    options.setPluginFilterPattern(filter);
    options.setFlexibleElementOrder(!strictOrder);

    FileObject targetDir = HopVfs.getFileObject(targetFolder, variables);
    log.logBasic("Exporting XML schemas to: " + targetDir.getName().getFriendlyURI());

    HopXmlSchemaExportResult result =
        HopXmlSchemaService.getInstance().exportAllSchemas(targetDir, options);

    for (String warning : result.getWarnings()) {
      log.logError("Warning: " + warning);
    }
    for (String error : result.getErrors()) {
      log.logError("Error: " + error);
    }

    log.logBasic(
        "Successfully exported "
            + result.getGeneratedFiles().size()
            + " XML schemas (pipeline: "
            + (options.isExportPipeline() && result.isPipelineSchemaGenerated() ? "1" : "0")
            + ", workflow: "
            + (options.isExportWorkflow() && result.isWorkflowSchemaGenerated() ? "1" : "0")
            + ", transforms: "
            + result.getTransformSchemasCount()
            + ", actions: "
            + result.getActionSchemasCount()
            + ")");
  }

  private void doValidate() throws Exception {
    FileObject fileObject = HopVfs.getFileObject(validateFile, variables);
    if (!fileObject.exists()) {
      throw new HopException("Validation file does not exist: " + validateFile);
    }

    String ext = fileObject.getName().getExtension();
    String schemaContent;
    HopXmlSchemaExportOptions options = new HopXmlSchemaExportOptions();
    options.setFlexibleElementOrder(!strictOrder);

    if ("hpl".equalsIgnoreCase(ext)) {
      log.logBasic("Validating pipeline file: " + fileObject.getName().getFriendlyURI());
      schemaContent = HopXmlSchemaService.getInstance().generatePipelineSchema(options);
    } else if ("hwf".equalsIgnoreCase(ext)) {
      log.logBasic("Validating workflow file: " + fileObject.getName().getFriendlyURI());
      schemaContent = HopXmlSchemaService.getInstance().generateWorkflowSchema(options);
    } else {
      try (InputStream in = HopVfs.getInputStream(fileObject)) {
        Document doc = XmlHandler.loadXmlFile(in);
        Element root = doc.getDocumentElement();
        if ("pipeline".equalsIgnoreCase(root.getTagName())) {
          log.logBasic("Detected pipeline document: " + fileObject.getName().getFriendlyURI());
          schemaContent = HopXmlSchemaService.getInstance().generatePipelineSchema(options);
        } else if ("workflow".equalsIgnoreCase(root.getTagName())) {
          log.logBasic("Detected workflow document: " + fileObject.getName().getFriendlyURI());
          schemaContent = HopXmlSchemaService.getInstance().generateWorkflowSchema(options);
        } else {
          throw new HopException(
              "Cannot determine file type for: "
                  + validateFile
                  + " (root element is neither <pipeline> nor <workflow>)");
        }
      }
    }

    Schema schema = HopXmlSchemaValidator.compileSchema(schemaContent);
    List<String> errors = HopXmlSchemaValidator.validateFile(fileObject, schema);

    if (errors.isEmpty()) {
      log.logBasic(
          "File is valid according to XML schema: " + fileObject.getName().getFriendlyURI());
    } else {
      log.logError("Validation failed with " + errors.size() + " error(s):");
      for (String error : errors) {
        log.logError(" - " + error);
      }
      throw new HopException("XML validation failed with " + errors.size() + " error(s)");
    }
  }
}
