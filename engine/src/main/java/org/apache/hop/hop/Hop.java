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

package org.apache.hop.hop;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.ConfigPluginType;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.JarCache;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.hop.plugin.HopCommandPluginType;
import org.apache.hop.hop.plugin.IHopCommand;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;
import org.apache.hop.metadata.util.HopMetadataUtil;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Getter
@Setter
@Command(
    name = "hop",
    mixinStandardHelpOptions = true,
    versionProvider = HopVersionProvider.class,
    subcommands = {CommandLine.HelpCommand.class})
public class Hop {
  @CommandLine.Option(
      names = {"-s", "--system-properties"},
      description = "A comma separated list of KEY=VALUE pairs",
      split = ",")
  private String[] systemProperties = null;

  @CommandLine.Option(
      names = {"--dev-debug"},
      description = "Allow a Hop developer to debug remotely")
  private boolean devDebug;

  @CommandLine.Option(
      names = {"--dev-debug-wait"},
      description =
          "Allow a Hop developer to debug remotely. The script will wait until a debugging session is set up.")
  private boolean devDebugWait;

  private CommandLine cmd;
  private IVariables variables;
  private MultiMetadataProvider metadataProvider;

  public Hop() {
    // Nothing yet
  }

  public static void main(String[] args) throws Exception {
    Hop hop = new Hop();

    hop.cmd = new CommandLine(hop);

    if (args.length > 0) {
      hop.prepareInternalOptions(new CommandLine(hop), args);
    }

    // We want to apply system properties before we boot up Hop, the plugins, and everything
    // associated.
    // There are variables which affect the location of plugins, libraries, and so on.
    //
    // Apply the system properties to the JVM
    //
    hop.applySystemProperties();

    // Initialize the Hop environment: load plugins and more
    //
    HopEnvironment.init();

    // Picks up the system settings in the variables
    //
    hop.variables = Variables.getADefaultVariableSpace();

    // Initialize the logging backend
    //
    HopLogStore.init();

    // Clear the jar file cache so that we don't waste memory...
    //
    JarCache.getInstance().clear();

    // Set up the metadata to use
    //
    hop.metadataProvider = HopMetadataUtil.getStandardHopMetadataProvider(hop.variables);
    HopMetadataInstance.setMetadataProvider(hop.metadataProvider);

    // Add mixin plugins with the ROOT category (e.g. project locations, environments, in-memory)
    //
    addMixinPlugins(hop.cmd, ConfigPlugin.CATEGORY_ROOT);

    // Look in the plugin registry for @HopCommand plugins.
    // Instantiate and initialize each of them.
    //
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> plugins = registry.getPlugins(HopCommandPluginType.class);
    for (IPlugin plugin : plugins) {
      IHopCommand subCommand = (IHopCommand) registry.loadClass(plugin);
      CommandLine subCmd = new CommandLine(subCommand);
      hop.cmd.addSubcommand(plugin.getIds()[0], subCmd);

      subCommand.initialize(subCmd, hop.variables, hop.metadataProvider);
    }

    // Finally we're ready to parse the command line arguments.
    //
    CommandLine.ParseResult parseResult = hop.cmd.parseArgs(args);

    if (CommandLine.printHelpIfRequested(parseResult)) {
      System.exit(1);
    }

    // Process options on root command mixins before executing subcommands
    //
    for (Object mixin : hop.cmd.getMixins().values()) {
      if (mixin instanceof IConfigOptions configOptions) {
        configOptions.handleOption(LogChannel.GENERAL, null, hop.variables);
      }
    }

    // Root mixins (for example --project-locations) may have replaced the metadata provider on
    // HopMetadataInstance. Subcommands were initialized with the original provider, so copy the
    // current one onto the selected command.
    //
    MultiMetadataProvider currentMetadata = HopMetadataInstance.getMetadataProvider();
    if (currentMetadata != null) {
      hop.setMetadataProvider(currentMetadata);
      applyMetadataProviderToParsedCommand(parseResult, currentMetadata);
    }

    int exitCode = hop.cmd.execute(args);
    System.exit(exitCode);
  }

  /**
   * Apply a metadata provider to the user object of the last parsed subcommand, when it holds one.
   *
   * @param parseResult picocli parse result
   * @param metadataProvider provider to set
   */
  public static void applyMetadataProviderToParsedCommand(
      CommandLine.ParseResult parseResult, MultiMetadataProvider metadataProvider) {
    if (parseResult == null || metadataProvider == null) {
      return;
    }
    CommandLine.ParseResult current = parseResult;
    while (current.hasSubcommand()) {
      current = current.subcommand();
    }
    Object userObject = current.commandSpec().userObject();
    if (userObject instanceof IHasHopMetadataProvider hasProvider) {
      hasProvider.setMetadataProvider(metadataProvider);
    }
  }

  private void prepareInternalOptions(CommandLine cmd, String[] args) {
    for (String arg : args) {
      if (arg.startsWith("-h") || arg.startsWith("--help")) {
        return;
      }
    }

    String[] helpArgs = new String[args.length + 1];
    System.arraycopy(args, 0, helpArgs, 0, args.length);
    helpArgs[args.length] = "-h";

    cmd.parseArgs(helpArgs);
  }

  public void applySystemProperties() {
    // Set some System properties if there were any
    //
    if (systemProperties != null) {
      for (String parameter : systemProperties) {
        String[] split = parameter.split("=", 2);
        String key = split.length > 0 ? split[0] : null;
        String value = split.length > 1 ? split[1] : null;
        if (StringUtils.isNotEmpty(key) && StringUtils.isNotEmpty(value)) {
          System.setProperty(key, value);
        }
      }
    }
  }

  public static void addMixinPlugins(CommandLine cmd, String category) throws HopPluginException {
    // Add configuration plugins for the given category (root, run, gui, search, ...).
    // The 'projects' plugin for example configures things like the project metadata provider.
    //
    List<IPlugin> configPlugins = PluginRegistry.getInstance().getPlugins(ConfigPluginType.class);
    for (IPlugin configPlugin : configPlugins) {
      // Load only the plugins of the "run" category
      if (category.equals(configPlugin.getCategory())) {
        IConfigOptions configOptions =
            PluginRegistry.getInstance().loadClass(configPlugin, IConfigOptions.class);
        cmd.addMixin(configPlugin.getIds()[0], configOptions);
      }
    }
  }
}
