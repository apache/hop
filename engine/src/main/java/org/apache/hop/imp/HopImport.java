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

package org.apache.hop.imp;

import java.io.File;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.LogProgressMonitor;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
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

@SuppressWarnings("java:S106")
@Getter
@Setter
@Command(
    description = "Import metadata",
    versionProvider = HopVersionProvider.class,
    mixinStandardHelpOptions = true)
@HopCommand(id = "import", description = "Import ")
public class HopImport implements Runnable, IHasHopMetadataProvider, IHopCommand {
  private static final String CONST_IMPORT = "Import is ";

  @Option(
      names = {"-t", "--type"},
      description = "The type of import plugin to use (e.g. kettle)",
      defaultValue = "kettle")
  private String type;

  @Option(
      names = {"-i", "--input"},
      description = "The input folder to read from")
  private String inputFolderName;

  @Option(
      names = {"-o", "--output"},
      description = "The output folder to write to")
  private String outputFolderName;

  @Option(
      names = {"-s", "--shared-xml"},
      description = "The shared.xml file to read from")
  private String sharedXmlFilename;

  @Option(
      names = {"-k", "--kettle-properties"},
      description = "The kettle.properties file to read from")
  private String kettlePropertiesFilename;

  @Option(
      names = {"-j", "--jdbc-properties"},
      description = "The jdbc.properties file to read from")
  private String jdbcPropertiesFilename;

  @Option(
      names = {"-c", "--target-config-file"},
      description = "The target config file to write variable to")
  private String targetConfigFilename;

  @Option(
      names = {"-e", "--skip-existing"},
      description = "Skip existing files in the target folders ",
      defaultValue = "true")
  private Boolean skippingExistingTargetFiles = true;

  @Option(
      names = {"-p", "--skip-hidden"},
      description = "Skip import of hidden files and folders",
      defaultValue = "true")
  private Boolean skippingHiddenFilesAndFolders = true;

  @Option(
      names = {"-f", "--skip-folders"},
      description = "Skip import of sub-folders",
      defaultValue = "false")
  private Boolean skippingFolders = false;

  @Option(
      names = {"-l", "--list-plugins"},
      description = "List the available import plugins")
  private Boolean listPluginTypes;

  @Option(
      names = {"-v", "--version"},
      versionHelp = true,
      description = "Print version information and exit")
  private boolean versionRequested;

  @Option(
      names = {"-n", "--naming-scheme"},
      description =
          "Naming scheme metadata name in the target folder to apply to relational connections")
  private String namingSchemeName;

  @Option(
      names = {"--no-apply-naming-schemes"},
      description =
          "Do not apply a naming scheme; connection names are still aligned to one spelling")
  private boolean noApplyNamingSchemes;

  @Option(
      names = {"-r", "--pipeline-run-configuration"},
      description =
          "The run configuration to set on every imported pipeline. "
              + "Without it the name found in the source file is kept.")
  private String defaultPipelineRunConfiguration;

  @Option(
      names = {"-w", "--workflow-run-configuration"},
      description =
          "The run configuration to set on every imported workflow. "
              + "Without it the name found in the source file is kept.")
  private String defaultWorkflowRunConfiguration;

  @Option(
      names = {"-P", "--project"},
      description =
          "Import into this project. An existing project's home folder is used as the target "
              + "folder, an unknown project is registered at the target folder.")
  private String projectName;

  private MultiMetadataProvider metadataProvider;
  private IVariables variables;
  private CommandLine cmd;
  private ILogChannel log;
  private boolean finishedWithoutError;

  private IHopImport hopImport;

  public HopImport() {
    variables = new Variables();
  }

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    this.cmd = cmd;
    this.variables = variables;
    this.metadataProvider = metadataProvider;

    Hop.addMixinPlugins(cmd, ConfigPlugin.CATEGORY_IMPORT);
  }

  @Override
  public void run() {
    try {
      log = new LogChannel("HopImport");

      if (listPluginTypes != null && listPluginTypes) {
        printPluginTypes();
        finishedWithoutError = true;
        return;
      }

      resolveExistingProject();

      if (!validateOptions()) {
        cmd.usage(System.err);
        return;
      }

      hopImport = loadImportPlugin();
      if (hopImport == null) {
        return;
      }

      log.logDetailed("Start of Hop Import");

      // Set the options...
      //
      hopImport.setValidateInputFolder(inputFolderName);
      hopImport.setValidateOutputFolder(outputFolderName);

      // Only now that both folders are known to be good is a new project registered. Registering
      // it any earlier left a project behind that pointed at a folder nothing was imported into.
      if (!registerTargetProject(hopImport.getOutputFolderName())) {
        return;
      }

      hopImport.setKettlePropertiesFilename(kettlePropertiesFilename);
      hopImport.setJdbcPropertiesFilename(jdbcPropertiesFilename);
      hopImport.setSharedXmlFilename(sharedXmlFilename);
      if (skippingExistingTargetFiles != null) {
        log.logBasic(
            CONST_IMPORT
                + (skippingExistingTargetFiles ? "" : "not ")
                + "skipping existing target files");
        hopImport.setSkippingExistingTargetFiles(skippingExistingTargetFiles);
      }
      if (skippingHiddenFilesAndFolders != null) {
        log.logBasic(
            CONST_IMPORT
                + (skippingHiddenFilesAndFolders ? "" : "not ")
                + "skipping hidden files and folders");
        hopImport.setSkippingHiddenFilesAndFolders(skippingHiddenFilesAndFolders);
      }
      if (skippingFolders != null) {
        log.logBasic(CONST_IMPORT + (skippingFolders ? "" : "not ") + "skipping sub-folders");
        hopImport.setSkippingFolders(skippingFolders);
      }
      hopImport.setTargetConfigFilename(targetConfigFilename);
      hopImport.setApplyNamingSchemes(!noApplyNamingSchemes);
      hopImport.setNamingSchemeName(namingSchemeName);
      hopImport.setDefaultPipelineRunConfiguration(defaultPipelineRunConfiguration);
      hopImport.setDefaultWorkflowRunConfiguration(defaultWorkflowRunConfiguration);
      warnAboutMissingRunConfiguration("pipeline", defaultPipelineRunConfiguration);
      warnAboutMissingRunConfiguration("workflow", defaultWorkflowRunConfiguration);

      // Allow plugins to modify the elements loaded so far, before a pipeline or workflow is even
      // loaded
      //
      ExtensionPointHandler.callExtensionPoint(
          log, variables, HopExtensionPoint.HopImportStart.id, this);

      // Handle the options of the configuration plugins
      //
      Map<String, Object> mixins = cmd.getMixins();
      for (String key : mixins.keySet()) {
        Object mixin = mixins.get(key);
        if (mixin instanceof IConfigOptions configOptions) {
          configOptions.handleOption(log, this, variables);
        }
      }

      // Text version of a progress monitor...
      //
      IProgressMonitor monitor = new LogProgressMonitor(log);

      // Run the import...
      //
      hopImport.runImport(monitor);

      // Print the report...
      //
      log.logBasic(Const.CR);
      log.logBasic(hopImport.getImportReport());

      ExtensionPointHandler.callExtensionPoint(
          log, variables, HopExtensionPoint.HopImportEnd.id, this);

      // main() turns this into the process exit code. Without it every import, successful or not,
      // exited 1 and no script could tell the difference.
      finishedWithoutError = true;

    } catch (Exception e) {
      throw new ExecutionException(cmd, "There was an error during import", e);
    }
  }

  private IHopImport loadImportPlugin() throws HopException {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin plugin = registry.getPlugin(ImportPluginType.class, type);
    if (plugin == null) {
      System.err.println("Import plugin type '" + type + "' could not be found.");
      printPluginTypes();
      return null;
    }
    IHopImport hi = registry.loadClass(plugin, IHopImport.class);
    hi.init(variables, log);
    return hi;
  }

  /**
   * A missing run configuration name no longer blanks the one the source file carried, but the
   * source may not have carried one either. Say so, the way the import dialog does.
   */
  private void warnAboutMissingRunConfiguration(String subject, String runConfigurationName) {
    if (StringUtils.isEmpty(runConfigurationName)) {
      log.logBasic(
          "No default "
              + subject
              + " run configuration was specified. Imported "
              + subject
              + "s keep the run configuration named in the source file, which can be empty.");
    }
  }

  /**
   * An existing project contributes its home folder as the target, so this runs before the options
   * are validated: the project is where the output folder comes from. An unknown project is left to
   * {@link #registerTargetProject(String)}. A no-op without the projects plugin.
   */
  private void resolveExistingProject() {
    if (StringUtils.isEmpty(projectName)) {
      return;
    }
    String projectHome = findProjectHome(projectName);
    if (StringUtils.isEmpty(projectHome)) {
      return;
    }
    if (StringUtils.isNotEmpty(outputFolderName) && !projectHome.equals(outputFolderName)) {
      log.logBasic(
          "Ignoring output folder '"
              + outputFolderName
              + "': project '"
              + projectName
              + "' is imported into its own home folder");
    }
    outputFolderName = projectHome;
    log.logBasic("Importing into project '" + projectName + "' at " + projectHome);
  }

  /**
   * Register the target folder as a new project, through the same extension point the import dialog
   * uses. Called once the import plugin has validated both folders, so a failing import never
   * leaves a project behind, and before anything is imported, so a project that cannot be
   * registered fails the run before files are written.
   *
   * @param validatedOutputFolder the output folder as the import plugin normalized it
   * @return false when {@code --project} was asked for and could not be honoured
   */
  private boolean registerTargetProject(String validatedOutputFolder) throws HopException {
    if (StringUtils.isEmpty(projectName) || StringUtils.isNotEmpty(findProjectHome(projectName))) {
      // No project asked for, or resolveExistingProject() already found it.
      return true;
    }
    String projectHome = projectHomeToStore(validatedOutputFolder);
    ExtensionPointHandler.callExtensionPoint(
        log,
        variables,
        HopExtensionPoint.HopImportCreateProject.id,
        new Object[] {projectHome, projectName});
    if (StringUtils.isEmpty(findProjectHome(projectName))) {
      log.logError(
          "Unable to register project '"
              + projectName
              + "' at "
              + projectHome
              + ". Is the projects plugin available? Nothing was imported.");
      return false;
    }
    log.logBasic("Registered project '" + projectName + "' at " + projectHome);
    return true;
  }

  /**
   * The folder to store as the new project's home. A folder that resolves to an absolute path is
   * stored as it was given, so that a home written as '${SOME_VARIABLE}/folder' stays portable -
   * the ProjectHome extension point resolves it on every read. A relative folder has to be pinned
   * down now: the next 'hop-import --project' can run from any working directory.
   */
  private String projectHomeToStore(String validatedOutputFolder) throws HopException {
    if (isAbsolute(variables.resolve(outputFolderName))) {
      return outputFolderName;
    }
    try {
      FileName folder = HopVfs.getFileObject(validatedOutputFolder).getName();
      // Keep a plain path plain, the way a project home typed in the GUI looks, but never drop the
      // scheme of a folder that has one: s3://bucket/folder is not /bucket/folder.
      return "file".equals(folder.getScheme()) ? folder.getPathDecoded() : folder.getURI();
    } catch (Exception e) {
      throw new HopException("Error resolving the home folder of project " + projectName, e);
    }
  }

  /** Whether a folder name stands on its own, rather than depending on the working directory. */
  private boolean isAbsolute(String folderName) {
    return folderName.contains("://") || new File(folderName).isAbsolute();
  }

  /** The home folder of a registered project, or null when it is unknown. */
  private String findProjectHome(String name) {
    try {
      return HopImportBase.projectHome(log, variables, name, null);
    } catch (Exception e) {
      // The projects plugin throws when the project isn't registered yet.
      return null;
    }
  }

  private void printPluginTypes() {
    System.err.println("Here are the available import plugins:");
    for (IPlugin importPlugin : PluginRegistry.getInstance().getPlugins(ImportPluginType.class)) {
      System.err.println("  - " + importPlugin.getIds()[0]);
      System.err.println("    Name: " + importPlugin.getName());
      System.err.println("    Description: " + importPlugin.getDescription());
      System.err.println("    Documentation URL: " + importPlugin.getDocumentationUrl());
    }
  }

  private void buildVariableSpace() {
    // Also grabs the system properties from hop.config.
    //
    variables = Variables.getADefaultVariableSpace();
  }

  private boolean validateOptions() {
    boolean ok = true;
    if (StringUtils.isEmpty(inputFolderName)) {
      log.logBasic("Please specify an input folder to read from");

      ok = false;
    }
    if (StringUtils.isEmpty(outputFolderName)) {
      log.logBasic(
          StringUtils.isEmpty(projectName)
              ? "Please specify an output folder to write to"
              : "Please specify an output folder to write to: project '"
                  + projectName
                  + "' is not registered, so it has no home folder to import into");
      ok = false;
    }
    if (StringUtils.isEmpty(type)) {
      log.logBasic("You need to specify the type of import plugin to use");
      printPluginTypes();
      ok = false;
    }
    return ok;
  }

  public static void main(String[] args) {

    HopImport hopImport = new HopImport();

    try {
      // Create the command line options...
      //
      CommandLine cmd = new CommandLine(hopImport);

      // Initialize the Hop environment: load plugins and more
      //
      HopEnvironment.init();

      // Picks up the system settings in the variables
      //
      hopImport.buildVariableSpace();

      // Now add run configuration plugins...
      //
      Hop.addMixinPlugins(cmd, ConfigPlugin.CATEGORY_IMPORT);
      hopImport.setCmd(cmd);

      // This will calculate the option values and put them in HopRun or the plugin classes
      //
      CommandLine.ParseResult parseResult = cmd.parseArgs(args);

      if (CommandLine.printHelpIfRequested(parseResult)) {
        System.exit(1);
      } else {
        // now run!
        //
        hopImport.run();
        if (hopImport.isFinishedWithoutError()) {
          System.exit(0);
        } else {
          System.exit(1);
        }
      }
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      hopImport.cmd.usage(System.err);
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
