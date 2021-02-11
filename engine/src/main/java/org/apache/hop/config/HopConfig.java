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

package org.apache.hop.config;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.ConfigPluginType;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;

import java.util.List;
import java.util.Map;

public class HopConfig implements Runnable, IHasHopMetadataProvider {

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Displays this help message and quits.")
  private boolean helpRequested;

  private CommandLine cmd;
  private IVariables variables;
  private IHopMetadataProvider metadataProvider;

  public void run() {

    try {
      LogChannel logChannel = new LogChannel("hop-config");
      logChannel.setSimplified(true);
      ILogChannel log = logChannel;
      variables = Variables.getADefaultVariableSpace();
      buildMetadataProvider();

      boolean actionTaken = false;

      Map<String, Object> mixins = cmd.getMixins();
      for (String key : mixins.keySet()) {
        Object mixin = mixins.get(key);
        if (mixin instanceof IConfigOptions) {
          IConfigOptions configOptions = (IConfigOptions) mixin;

          actionTaken = configOptions.handleOption(log, this, variables) || actionTaken;
        }
      }

      if (!actionTaken) {
        cmd.usage(System.out);
      }

    } catch (Exception e) {
      throw new ExecutionException(cmd, "There was an error handling options", e);
    }
  }

  private void buildMetadataProvider() throws HopException {
    String folder = variables.getVariable(Const.HOP_METADATA_FOLDER);
    if (StringUtils.isEmpty(folder)) {
      metadataProvider = new JsonMetadataProvider();
    } else {
      ITwoWayPasswordEncoder passwordEncoder = Encr.getEncoder();
      if (passwordEncoder == null) {
        passwordEncoder = new HopTwoWayPasswordEncoder();
      }
      metadataProvider = new JsonMetadataProvider(passwordEncoder, folder, variables);
    }
  }

  /**
   * Gets cmd
   *
   * @return value of cmd
   */
  public CommandLine getCmd() {
    return cmd;
  }

  /** @param cmd The cmd to set */
  public void setCmd(CommandLine cmd) {
    this.cmd = cmd;
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  @Override
  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /** @param metadataProvider The metadataProvider to set */
  @Override
  public void setMetadataProvider(IHopMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }

  public static void main(String[] args) {

    HopConfig hopConfig = new HopConfig();

    try {
      HopEnvironment.init();

      CommandLine cmd = new CommandLine(hopConfig);
      List<IPlugin> configPlugins = PluginRegistry.getInstance().getPlugins(ConfigPluginType.class);
      for (IPlugin configPlugin : configPlugins) {
        // Load only the plugins of the "config" category
        if (ConfigPlugin.CATEGORY_CONFIG.equals(configPlugin.getCategory())) {
          IConfigOptions configOptions =
              PluginRegistry.getInstance().loadClass(configPlugin, IConfigOptions.class);
          cmd.addMixin(configPlugin.getIds()[0], configOptions);
        }
      }

      hopConfig.setCmd(cmd);
      CommandLine.ParseResult parseResult = cmd.parseArgs(args);
      if (CommandLine.printHelpIfRequested(parseResult)) {
        System.exit(1);
      } else {
        hopConfig.run();
        System.exit(0);
      }
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      hopConfig.cmd.usage(System.err);
      e.getCommandLine().usage(System.err);
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
