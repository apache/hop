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

package org.apache.hop.naming.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.hop.Hop;
import org.apache.hop.hop.plugin.HopCommand;
import org.apache.hop.hop.plugin.IHopCommand;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.naming.engine.NamingSchemeValidator.Finding;
import org.apache.hop.naming.engine.NamingSchemeValidator.Severity;
import org.apache.hop.naming.engine.NamingSchemeWalker;
import org.apache.hop.naming.metadata.NamingScheme;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.workflow.WorkflowMeta;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Getter
@Setter
@HopCommand(id = "naming-check", description = "Validate names against Naming Schemes")
@Command(
    versionProvider = HopVersionProvider.class,
    mixinStandardHelpOptions = true,
    description = "Validate names against project Naming Schemes")
public class HopNamingCheck implements Runnable, IHopCommand, IHasHopMetadataProvider {

  @Option(
      names = {"-v", "--version"},
      versionHelp = true,
      description = "Print version information and exit")
  private boolean versionRequested;

  @Option(
      names = {"-p", "--path"},
      description = "Folder to scan for .hpl and .hwf files (defaults to PROJECT_HOME or cwd)")
  private String path;

  @Option(
      names = {"-t", "--type"},
      description = "Comma-separated naming type codes to check (default: all)")
  private String types;

  @Option(
      names = {"--require-scheme"},
      description = "Treat missing schemes as errors instead of warnings")
  private boolean requireScheme;

  @Option(
      names = {"--format"},
      description = "Output format: text or json (default: text)")
  private String format = "text";

  private CommandLine cmd;
  private IVariables variables;
  private MultiMetadataProvider metadataProvider;

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    this.cmd = cmd;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    Hop.addMixinPlugins(cmd, ConfigPlugin.CATEGORY_NAMING);
  }

  @Override
  public void run() {
    try {
      System.setProperty(Const.HOP_PLATFORM_RUNTIME, "NAMING_CHECK");
      ILogChannel log = new LogChannel("hop-naming-check");
      ((LogChannel) log).setSimplified(true);

      for (Object mixin : cmd.getMixins().values()) {
        if (mixin instanceof IConfigOptions options) {
          options.handleOption(log, this, variables);
        }
      }

      List<NamingScheme> schemes = loadSchemes();
      Set<String> typeFilter = parseTypes();
      List<Finding> findings = new ArrayList<>();

      String scanRoot = resolveScanRoot();
      scanPipelinesAndWorkflows(scanRoot, schemes, typeFilter, findings);
      scanMetadata(schemes, typeFilter, findings);

      printFindings(findings);
      int errors = 0;
      int warnings = 0;
      for (Finding finding : findings) {
        if (finding.getSeverity() == Severity.ERROR
            || (requireScheme && finding.getSeverity() == Severity.WARNING)) {
          errors++;
        } else {
          warnings++;
        }
      }
      if (!"json".equalsIgnoreCase(format)) {
        System.out.println("Naming check: " + errors + " error(s), " + warnings + " warning(s)");
      }
      System.exit(errors > 0 ? 1 : 0);
    } catch (Exception e) {
      System.err.println(Const.getStackTracker(e));
      System.exit(2);
    }
  }

  private List<NamingScheme> loadSchemes() throws HopException {
    if (metadataProvider == null) {
      return List.of();
    }
    return metadataProvider.getSerializer(NamingScheme.class).loadAll();
  }

  private Set<String> parseTypes() {
    Set<String> filter = new HashSet<>();
    if (StringUtils.isNotEmpty(types)) {
      Arrays.stream(types.split(","))
          .map(String::trim)
          .filter(StringUtils::isNotEmpty)
          .forEach(filter::add);
    }
    return filter;
  }

  private String resolveScanRoot() {
    if (StringUtils.isNotEmpty(path)) {
      return variables.resolve(path);
    }
    String home = variables.getVariable("PROJECT_HOME");
    if (StringUtils.isEmpty(home)) {
      home = variables.getVariable(Const.HOP_METADATA_FOLDER);
    }
    if (StringUtils.isEmpty(home)) {
      home = ".";
    }
    return variables.resolve(home);
  }

  private void scanPipelinesAndWorkflows(
      String root, List<NamingScheme> schemes, Set<String> typeFilter, List<Finding> findings)
      throws Exception {
    FileObject folder = HopVfs.getFileObject(root);
    if (!folder.exists()) {
      return;
    }
    FileObject[] files =
        folder.findFiles(
            new FileSelector() {
              @Override
              public boolean includeFile(FileSelectInfo fileInfo) {
                String name = fileInfo.getFile().getName().getBaseName().toLowerCase();
                return name.endsWith(".hpl") || name.endsWith(".hwf");
              }

              @Override
              public boolean traverseDescendents(FileSelectInfo fileInfo) {
                return true;
              }
            });
    if (files == null) {
      return;
    }
    for (FileObject file : files) {
      String filename = HopVfs.getFilename(file);
      try {
        if (filename.toLowerCase().endsWith(".hpl")) {
          PipelineMeta pipeline = new PipelineMeta(filename, metadataProvider, variables);
          findings.addAll(NamingSchemeWalker.walk(pipeline, filename, schemes, typeFilter));
        } else {
          WorkflowMeta workflow = new WorkflowMeta(variables, filename, metadataProvider);
          findings.addAll(NamingSchemeWalker.walk(workflow, filename, schemes, typeFilter));
        }
      } catch (Exception e) {
        Finding load = new Finding();
        load.setLocation(filename);
        load.setFieldPath("");
        load.setSeverity(Severity.WARNING);
        load.setMessage("Could not load file: " + e.getMessage());
        findings.add(load);
      }
    }
  }

  private void scanMetadata(
      List<NamingScheme> schemes, Set<String> typeFilter, List<Finding> findings)
      throws HopException {
    if (metadataProvider == null) {
      return;
    }
    for (Class<IHopMetadata> clazz : metadataProvider.getMetadataClasses()) {
      if (NamingScheme.class.isAssignableFrom(clazz)) {
        continue;
      }
      IHopMetadataProvider provider = metadataProvider;
      for (IHopMetadata object : provider.getSerializer(clazz).loadAll()) {
        String location = "metadata/" + clazz.getSimpleName() + "/" + object.getName();
        findings.addAll(NamingSchemeWalker.walk(object, location, schemes, typeFilter));
      }
    }
  }

  private void printFindings(List<Finding> findings) {
    if ("json".equalsIgnoreCase(format)) {
      System.out.println("[");
      for (int i = 0; i < findings.size(); i++) {
        Finding f = findings.get(i);
        System.out.print(
            "  {\"severity\":\""
                + f.getSeverity()
                + "\",\"location\":"
                + json(f.getLocation())
                + ",\"field\":"
                + json(f.getFieldPath())
                + ",\"type\":"
                + json(f.getTypeCode())
                + ",\"scheme\":"
                + json(f.getSchemeName())
                + ",\"actual\":"
                + json(f.getActual())
                + ",\"expected\":"
                + json(f.getExpected())
                + ",\"message\":"
                + json(f.getMessage())
                + "}");
        System.out.println(i < findings.size() - 1 ? "," : "");
      }
      System.out.println("]");
      return;
    }
    for (Finding f : findings) {
      System.out.println(
          f.getSeverity()
              + " "
              + Const.NVL(f.getLocation(), "")
              + " "
              + Const.NVL(f.getFieldPath(), "")
              + " : "
              + f.getMessage());
    }
  }

  private static String json(String value) {
    if (value == null) {
      return "null";
    }
    return "\""
        + value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
        + "\"";
  }
}
