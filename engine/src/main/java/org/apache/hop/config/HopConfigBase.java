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
 *
 */

package org.apache.hop.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.ConfigPluginType;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import picocli.CommandLine;

@Getter
@Setter
public abstract class HopConfigBase implements Runnable, IHasHopMetadataProvider {
  @CommandLine.Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Displays this help message and quits.")
  protected boolean helpRequested;

  @CommandLine.Option(
      names = {"-v", "--version"},
      versionHelp = true,
      description = "Print version information and exit")
  protected boolean versionRequested;

  @CommandLine.Option(
      names = {"-l", "--level"},
      description =
          "The debug level, one of NOTHING, ERROR, MINIMAL, BASIC, DETAILED, DEBUG, ROWLEVEL")
  protected String level;

  protected CommandLine cmd;
  protected IVariables variables;
  protected MultiMetadataProvider metadataProvider;
  protected LogChannel log;

  @Override
  public void run() {
    try {
      boolean actionTaken = handleConfigMixinsOptions(log);
      if (!actionTaken) {
        cmd.usage(System.out);
      }
    } catch (Exception e) {
      throw new CommandLine.ExecutionException(cmd, "There was an error handling options", e);
    }
  }

  protected void addConfConfigPlugins() throws HopPluginException {
    List<IPlugin> configPlugins = PluginRegistry.getInstance().getPlugins(ConfigPluginType.class);
    for (IPlugin configPlugin : configPlugins) {
      // Load only the plugins of the "config" category
      if (ConfigPlugin.CATEGORY_CONFIG.equals(configPlugin.getCategory())) {
        IConfigOptions configOptions =
            PluginRegistry.getInstance().loadClass(configPlugin, IConfigOptions.class);
        cmd.addMixin(configPlugin.getIds()[0], configOptions);
      }
    }
  }

  private boolean handleConfigMixinsOptions(LogChannel log) throws HopException {
    boolean actionTaken = false;
    Map<String, Object> mixins = cmd.getMixins();
    for (String key : mixins.keySet()) {
      Object mixin = mixins.get(key);
      if (mixin instanceof IConfigOptions configOptions) {
        actionTaken = configOptions.handleOption(log, this, variables) || actionTaken;
      }
    }
    return actionTaken;
  }

  protected LogLevel determineLogLevel() {
    return LogLevel.lookupCode(variables.resolve(level));
  }

  protected void buildMetadataProvider() {
    List<IHopMetadataProvider> providers = new ArrayList<>();
    String folder = variables.getVariable(Const.HOP_METADATA_FOLDER);
    if (StringUtils.isEmpty(folder)) {
      providers.add(new JsonMetadataProvider());
    } else {
      ITwoWayPasswordEncoder passwordEncoder = Encr.getEncoder();
      if (passwordEncoder == null) {
        passwordEncoder = new HopTwoWayPasswordEncoder();
      }
      providers.add(new JsonMetadataProvider(passwordEncoder, folder, variables));
    }
    metadataProvider = new MultiMetadataProvider(Encr.getEncoder(), providers, variables);
  }

  protected void buildLogChannel() {
    log = new LogChannel("hop-config");
    log.setSimplified(true);
    log.setLogLevel(determineLogLevel());
    log.logDetailed("Start of Hop Config");
  }
}
