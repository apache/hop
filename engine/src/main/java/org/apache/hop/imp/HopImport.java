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

import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
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
        return;
      }

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
      log.logBasic("Please specify an output folder to write to");
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
