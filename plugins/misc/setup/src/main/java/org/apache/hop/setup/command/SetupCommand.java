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

package org.apache.hop.setup.command;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.hop.plugin.HopCommand;
import org.apache.hop.hop.plugin.IHopCommand;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.setup.HopEnvironmentApplier;
import org.apache.hop.setup.HopEnvironmentApplyResult;
import org.apache.hop.setup.HopEnvironmentDefaults;
import org.apache.hop.setup.HopEnvironmentSnapshot;
import org.apache.hop.setup.HopEnvironmentSpec;
import org.apache.hop.setup.HopSetupException;
import org.apache.hop.setup.OsFamily;
import org.apache.hop.setup.UserPaths;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * {@code hop setup} — configure {@code HOP_CONFIG_FOLDER} and related launcher environment
 * variables. This is not {@code hop conf}: that command edits hop-config.json.
 */
@Getter
@Setter
@Command(
    name = "setup",
    mixinStandardHelpOptions = true,
    description =
        "Configure Hop launcher environment variables (HOP_CONFIG_FOLDER, HOP_AUDIT_FOLDER, "
            + "HOP_JAVA_HOME, HOP_OPTIONS, HOP_SHARED_JDBC_FOLDERS). Distinct from hop conf, "
            + "which edits hop-config.json.",
    subcommands = {SetupCommand.ApplyCommand.class, SetupCommand.ShowCommand.class})
@HopCommand(id = "setup", description = "Configure Hop environment variables")
public class SetupCommand implements Runnable, IHopCommand, IHasHopMetadataProvider {

  private ILogChannel log;
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
    this.log = new LogChannel("Hop setup");
    wireSubcommands(cmd, log, variables);
  }

  private static void wireSubcommands(
      CommandLine commandLine, ILogChannel log, IVariables variables) {
    for (CommandLine sub : commandLine.getSubcommands().values()) {
      Object userObject = sub.getCommand();
      if (userObject instanceof SetupSubCommand nested) {
        nested.init(log, variables);
      }
      wireSubcommands(sub, log, variables);
    }
  }

  @Override
  public void run() {
    new ShowCommand().init(log, variables).run();
    System.out.println();
    cmd.usage(System.out);
  }

  abstract static class SetupSubCommand implements Runnable {
    protected ILogChannel log;
    protected IVariables variables;

    SetupSubCommand init(ILogChannel log, IVariables variables) {
      this.log = log;
      this.variables = variables;
      return this;
    }
  }

  @Command(
      mixinStandardHelpOptions = true,
      name = "show",
      description = "Print the current environment and the recommended folders")
  static class ShowCommand extends SetupSubCommand {
    @Override
    public void run() {
      OsFamily os = OsFamily.detect();
      UserPaths paths = UserPaths.system();
      HopEnvironmentSnapshot snapshot = HopEnvironmentSnapshot.capture(os, paths);
      System.out.println("Current Hop environment");
      System.out.println(
          "  HOP_CONFIG_FOLDER = "
              + snapshot.getConfigFolder()
              + (snapshot.isConfigFolderFromEnv() ? " (from environment)" : " (install fallback)"));
      System.out.println(
          "  HOP_AUDIT_FOLDER  = "
              + snapshot.getAuditFolder()
              + (snapshot.isAuditFolderFromEnv() ? " (from environment)" : " (install fallback)"));
      System.out.println("  HOP_JAVA_HOME     = " + snapshot.getJavaHome());
      System.out.println("  HOP_OPTIONS       = " + snapshot.getOptions());
      System.out.println("  HOP_SHARED_JDBC_FOLDERS = " + snapshot.getJdbcFolders());
      System.out.println("Recommended folders");
      System.out.println(
          "  HOP_CONFIG_FOLDER = " + HopEnvironmentDefaults.recommendedConfigFolder(os, paths));
      System.out.println(
          "  HOP_AUDIT_FOLDER  = " + HopEnvironmentDefaults.recommendedAuditFolder(os, paths));
      System.out.println("Well-known launcher env file");
      System.out.println("  " + snapshot.getWellKnownEnvFile());
      if (!os.isWindows()) {
        System.out.println("Detected shell rc file");
        System.out.println("  " + HopEnvironmentDefaults.recommendedShellRcFile(paths));
      }
    }
  }

  @Command(
      mixinStandardHelpOptions = true,
      name = "apply",
      description =
          "Write the selected variables to the user environment, a shell rc file, and/or a hop-env"
              + " script. Restart Hop afterwards.")
  static class ApplyCommand extends SetupSubCommand {

    @Option(
        names = {"--config-folder"},
        description = "Value for HOP_CONFIG_FOLDER")
    String configFolder;

    @Option(
        names = {"--audit-folder"},
        description = "Value for HOP_AUDIT_FOLDER")
    String auditFolder;

    @Option(
        names = {"--java-home"},
        description = "Value for HOP_JAVA_HOME")
    String javaHome;

    @Option(
        names = {"--options"},
        description = "Value for HOP_OPTIONS")
    String options;

    @Option(
        names = {"--jdbc-folders"},
        description = "Value for HOP_SHARED_JDBC_FOLDERS")
    String jdbcFolders;

    @Option(
        names = {"--user-env"},
        description = "Write Windows user environment variables")
    boolean userEnv;

    @Option(
        names = {"--shell-rc"},
        description = "Update the bash/zsh rc file with a marked hop setup block")
    boolean shellRc;

    @Option(
        names = {"--rc-file"},
        description = "Override the shell rc file path")
    String rcFile;

    @Option(
        names = {"--script"},
        description = "Write a hop-env.sh / hop-env.cmd script")
    boolean script;

    @Option(
        names = {"--script-file"},
        description = "Override the hop-env script path (default: well-known user path)")
    String scriptFile;

    @Option(
        names = {"--copy-existing"},
        description = "Copy <install>/config into an empty HOP_CONFIG_FOLDER")
    boolean copyExisting;

    @Option(
        names = {"--create-folders"},
        description = "Create the config and audit folders (default: true)",
        defaultValue = "true",
        fallbackValue = "true",
        negatable = true)
    boolean createFolders = true;

    @Option(
        names = {"--dry-run"},
        description = "Print the planned writes without changing anything")
    boolean dryRun;

    @Option(
        names = {"--defaults"},
        description =
            "Fill unspecified config/audit folders with the recommended platform paths and"
                + " register a default project plus the install samples project")
    boolean defaults;

    @Option(
        names = {"--create-default-project"},
        description = "Create a default project in the user's documents folder",
        negatable = true)
    Boolean createDefaultProject;

    @Option(
        names = {"--default-project-home"},
        description = "Home folder of the default project")
    String defaultProjectHome;

    @Option(
        names = {"--register-samples"},
        description = "Register the samples project from this Hop installation",
        negatable = true)
    Boolean registerSamples;

    @Override
    public void run() {
      try {
        HopEnvironmentSpec spec = toSpec();
        HopEnvironmentApplyResult result = new HopEnvironmentApplier().apply(spec);
        System.out.println(result.describe());
        if (spec.isDryRun()) {
          result
              .getPlannedFiles()
              .forEach(
                  (path, content) -> {
                    System.out.println();
                    System.out.println("--- " + path + " ---");
                    System.out.println(content);
                  });
        }
      } catch (HopSetupException e) {
        System.err.println(e.getMessage());
        throw new CommandLine.ExecutionException(
            new CommandLine(this),
            e.getMessage() == null ? "hop setup apply failed" : e.getMessage(),
            e);
      }
    }

    HopEnvironmentSpec toSpec() {
      HopEnvironmentSpec spec = new HopEnvironmentSpec();
      OsFamily os = OsFamily.detect();
      UserPaths paths = UserPaths.system();
      spec.setConfigFolder(configFolder);
      spec.setAuditFolder(auditFolder);
      spec.setJavaHome(javaHome);
      spec.setOptions(options);
      spec.setJdbcFolders(jdbcFolders);
      if (defaults) {
        if (spec.getConfigFolder() == null) {
          spec.setConfigFolder(HopEnvironmentDefaults.recommendedConfigFolder(os, paths));
        }
        if (spec.getAuditFolder() == null) {
          spec.setAuditFolder(HopEnvironmentDefaults.recommendedAuditFolder(os, paths));
        }
      }
      spec.setWriteUserEnv(userEnv);
      spec.setWriteShellRc(shellRc);
      spec.setShellRcFile(rcFile);
      spec.setWriteScript(script || StringUtils.isNotBlank(scriptFile));
      spec.setScriptFile(scriptFile);
      spec.setCopyExisting(copyExisting);
      spec.setCreateFolders(createFolders);
      spec.setDryRun(dryRun);
      boolean createProject = createDefaultProject != null ? createDefaultProject : defaults;
      spec.setCreateDefaultProject(createProject);
      spec.setDefaultProjectHome(defaultProjectHome);
      if (createProject && spec.getDefaultProjectHome() == null) {
        spec.setDefaultProjectHome(HopEnvironmentDefaults.recommendedDefaultProjectHome(os, paths));
      }
      spec.setRegisterSamples(registerSamples != null ? registerSamples : defaults);
      return spec;
    }
  }
}
