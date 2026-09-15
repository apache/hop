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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.diagram.DiagramExportFormat;
import org.apache.hop.core.diagram.DiagramExportOptions;
import org.apache.hop.core.diagram.DiagramExportResult;
import org.apache.hop.core.diagram.DiagramExportService;
import org.apache.hop.core.diagram.ExportContext;
import org.apache.hop.core.diagram.IDiagramExporter;
import org.apache.hop.core.diagram.IExportContext;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.hop.Hop;
import org.apache.hop.hop.plugin.HopCommand;
import org.apache.hop.hop.plugin.IHopCommand;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;

/** Hop CLI command for exporting diagrams to SVG, Mermaid, etc. */
@Getter
@Setter
@Command(
    name = "export",
    description =
        "Export pipeline, workflow, or model diagrams to various formats (SVG, Mermaid, etc.)",
    mixinStandardHelpOptions = true,
    versionProvider = HopVersionProvider.class)
@HopCommand(id = "export", description = "Export diagrams")
public class DiagramExportCommand implements Runnable, IHopCommand, IHasHopMetadataProvider {

  @Option(
      names = {"-f", "--file"},
      description = "The single file (.hpl, .hwf, or custom model) to export")
  private String filename;

  @Option(
      names = {"-o", "--output-file"},
      description = "The output filename for single file export")
  private String outputFilename;

  @Option(
      names = {"--format"},
      description = "Target diagram format: svg, mermaid, etc. (default: svg)")
  private String format;

  @Option(
      names = {"-s", "--source-folder"},
      description = "Source folder for batch export")
  private String sourceFolder;

  @Option(
      names = {"-t", "--target-folder"},
      description = "Target folder for batch export")
  private String targetFolder;

  @Option(
      names = {"-r", "--recursive"},
      description = "Recursively scan subfolders for batch export")
  private boolean recursive;

  @Option(
      names = {"-m", "--magnification"},
      description = "Diagram magnification factor (default: 1.0)")
  private float magnification = 1.0f;

  @Option(
      names = {"-in", "--include-notes"},
      description = "Include notes in diagram export (default: true)")
  private boolean includeNotes = true;

  @Option(
      names = {"--list-exporters"},
      description = "List all registered diagram exporters and their supported subjects")
  private boolean listExporters;

  private CommandLine cmd;
  private IVariables variables;
  private MultiMetadataProvider metadataProvider;
  private ILogChannel log;

  public DiagramExportCommand() {
    this.variables = Variables.getADefaultVariableSpace();
    this.log = new LogChannel("hop-export");
  }

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    this.cmd = cmd;
    this.variables = variables != null ? variables : Variables.getADefaultVariableSpace();
    this.metadataProvider = metadataProvider;
    this.log = new LogChannel("hop-export");

    Hop.addMixinPlugins(cmd, ConfigPlugin.CATEGORY_EXPORT);
  }

  @Override
  public void run() {
    try {
      System.setProperty(Const.HOP_PLATFORM_RUNTIME, "EXPORT");
      if (log == null) {
        log = new LogChannel("hop-export");
      }
      if (variables == null) {
        variables = Variables.getADefaultVariableSpace();
      }

      handleMixinActions();
      buildMetadataProvider();

      if (listExporters) {
        doListExporters();
        return;
      }

      if (StringUtils.isNotEmpty(filename)) {
        doExportSingleFile();
      } else if (StringUtils.isNotEmpty(sourceFolder)) {
        doBatchExport();
      } else {
        log.logBasic("No file (-f) or source folder (-s) specified. Use --help for usage.");
        if (cmd != null) {
          cmd.usage(System.out);
        }
      }
    } catch (Exception e) {
      log.logError("Error executing diagram export: " + e.getMessage(), e);
      throw new RuntimeException("Diagram export failed", e);
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

  protected void buildMetadataProvider() {
    if (metadataProvider == null) {
      metadataProvider = org.apache.hop.metadata.util.HopMetadataInstance.getMetadataProvider();
      if (metadataProvider == null) {
        org.apache.hop.metadata.serializer.json.JsonMetadataProvider jsonProvider =
            new org.apache.hop.metadata.serializer.json.JsonMetadataProvider();
        metadataProvider =
            new MultiMetadataProvider(
                org.apache.hop.core.encryption.Encr.getEncoder(),
                java.util.List.of(jsonProvider),
                variables);
      }
    }
  }

  private void doListExporters() {
    List<IDiagramExporter<?>> exporters = DiagramExportService.getInstance().getAllExporters();
    System.out.println("Available Diagram Exporters (" + exporters.size() + "):");
    for (IDiagramExporter<?> exporter : exporters) {
      String formatStr = exporter.getFormat() != null ? exporter.getFormat().getId() : "N/A";
      System.out.println("  ID: " + exporter.getId());
      System.out.println("    Name: " + exporter.getName());
      System.out.println("    Format: " + formatStr);
      System.out.println("    Extension: ." + exporter.getFileExtension());
      if (StringUtils.isNotEmpty(exporter.getDescription())) {
        System.out.println("    Description: " + exporter.getDescription());
      }
      System.out.println();
    }
  }

  private void doExportSingleFile() throws HopException {
    String resolvedFile = variables.resolve(filename);
    String targetFmt = resolveTargetFormat(outputFilename, format);
    String targetFile = resolveOutputFilename(resolvedFile, outputFilename, targetFmt);

    DiagramExportOptions options = new DiagramExportOptions(targetFile, targetFmt);
    options.setMagnification(magnification);
    options.setIncludeNotes(includeNotes);

    IExportContext context =
        new ExportContext(variables, metadataProvider, log, IExportContext.ExportEnvironment.CLI);

    DiagramExportResult result =
        DiagramExportService.getInstance().exportFile(resolvedFile, options, context);
    if (!result.isSuccess()) {
      throw new HopException("Export failed: " + result.getErrorMessage(), result.getException());
    }
    log.logBasic("Exported " + resolvedFile + " -> " + result.getFilename());
  }

  private void doBatchExport() throws HopException {
    if (StringUtils.isBlank(targetFolder)) {
      throw new HopException("Target folder (-t, --target-folder) is required for batch export");
    }

    String resolvedSource = variables.resolve(sourceFolder);
    String resolvedTarget = variables.resolve(targetFolder);

    try {
      FileObject sourceDir = HopVfs.getFileObject(resolvedSource);
      if (!sourceDir.exists()) {
        throw new HopException("Source folder does not exist: " + resolvedSource);
      }

      List<FileObject> filesToExport = new ArrayList<>();
      collectFiles(sourceDir, filesToExport, recursive);

      String targetFmt = resolveTargetFormat(null, format);
      int count = 0;

      for (FileObject file : filesToExport) {
        String relPath = sourceDir.getName().getRelativeName(file.getName());
        int lastDot = relPath.lastIndexOf('.');
        String baseName = lastDot != -1 ? relPath.substring(0, lastDot) : relPath;
        String ext = targetFmt.equalsIgnoreCase("mermaid") ? "mmd" : targetFmt.toLowerCase();
        String targetPath = resolvedTarget + "/" + baseName + "." + ext;

        DiagramExportOptions options = new DiagramExportOptions(targetPath, targetFmt);
        options.setMagnification(magnification);
        options.setIncludeNotes(includeNotes);

        IExportContext context =
            new ExportContext(
                variables, metadataProvider, log, IExportContext.ExportEnvironment.BATCH);

        DiagramExportResult result =
            DiagramExportService.getInstance()
                .exportFile(file.getName().getURI(), options, context);
        if (result.isSuccess()) {
          count++;
          log.logBasic("Exported " + relPath + " -> " + targetPath);
        } else {
          log.logError("Failed to export " + relPath + ": " + result.getErrorMessage());
        }
      }

      log.logBasic("Batch export finished. Total diagrams exported: " + count);
    } catch (Exception e) {
      throw new HopException("Error during batch diagram export", e);
    }
  }

  private void collectFiles(FileObject current, List<FileObject> collector, boolean isRecursive)
      throws Exception {
    if (current.getType() == FileType.FOLDER) {
      FileObject[] children = current.getChildren();
      if (children != null) {
        for (FileObject child : children) {
          if (child.getType() == FileType.FILE) {
            if (DiagramExportService.getInstance().findSubjectLoader(child.getName().getPath())
                != null) {
              collector.add(child);
            }
          } else if (isRecursive && child.getType() == FileType.FOLDER) {
            collectFiles(child, collector, true);
          }
        }
      }
    } else if (current.getType() == FileType.FILE) {
      if (DiagramExportService.getInstance().findSubjectLoader(current.getName().getPath())
          != null) {
        collector.add(current);
      }
    }
  }

  private String resolveTargetFormat(String outputFilePath, String explicitFormat) {
    if (StringUtils.isNotBlank(explicitFormat)) {
      return explicitFormat.trim().toUpperCase();
    }
    if (StringUtils.isNotBlank(outputFilePath)) {
      String lower = outputFilePath.toLowerCase();
      if (lower.endsWith(".mmd")) {
        return DiagramExportFormat.MERMAID.getId();
      }
      if (lower.endsWith(".svg")) {
        return DiagramExportFormat.SVG.getId();
      }
      if (lower.endsWith(".pdf")) {
        return DiagramExportFormat.PDF.getId();
      }
    }
    return DiagramExportFormat.SVG.getId();
  }

  private String resolveOutputFilename(
      String inputFilename, String explicitOutput, String targetFormat) {
    if (StringUtils.isNotBlank(explicitOutput)) {
      return variables.resolve(explicitOutput);
    }
    int lastDot = inputFilename.lastIndexOf('.');
    String base = lastDot != -1 ? inputFilename.substring(0, lastDot) : inputFilename;
    String ext = targetFormat.equalsIgnoreCase("mermaid") ? "mmd" : targetFormat.toLowerCase();
    return base + "." + ext;
  }

  public static void main(String[] args) {
    DiagramExportCommand exportCmd = new DiagramExportCommand();

    try {
      HopEnvironment.init();
      HopLogStore.init();

      exportCmd.cmd = new CommandLine(exportCmd);
      Hop.addMixinPlugins(exportCmd.cmd, ConfigPlugin.CATEGORY_EXPORT);
      exportCmd.setCmd(exportCmd.cmd);
      CommandLine.ParseResult parseResult = exportCmd.cmd.parseArgs(args);
      if (CommandLine.printHelpIfRequested(parseResult)) {
        System.exit(1);
      } else {
        exportCmd.run();
        System.exit(0);
      }
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      exportCmd.cmd.usage(System.err);
      System.exit(9);
    } catch (ExecutionException e) {
      System.err.println("Error found during execution!");
      System.err.println(Const.getStackTracker(e));
      System.exit(1);
    } catch (Exception e) {
      System.err.println("General error found, something went horribly wrong!");
      System.err.println(Const.getStackTracker(e));
      System.exit(2);
    }
  }
}
