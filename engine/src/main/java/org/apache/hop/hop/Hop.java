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
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.JarCache;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.hop.plugin.HopCommandPluginType;
import org.apache.hop.hop.plugin.IHopCommand;
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

  private CommandLine cmd;
  private IVariables variables;
  private MultiMetadataProvider metadataProvider;

  public Hop() throws Exception {}

  public static void main(String[] args) throws Exception {
    Hop hop = new Hop();

    hop.cmd = new CommandLine(new Hop());

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

    int exitCode = hop.cmd.execute(args);
    System.exit(exitCode);
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
}
