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

package org.apache.hop.ui.hopgui;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.hop.Hop;
import org.apache.hop.hop.plugin.HopCommand;
import org.apache.hop.hop.plugin.IHopCommand;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import picocli.CommandLine;

@Getter
@Setter
@CommandLine.Command(
    versionProvider = HopVersionProvider.class,
    mixinStandardHelpOptions = true,
    description = "The Hop GUI")
@HopCommand(id = "gui", description = "The Hop GUI")
public class HopCommandGui implements Runnable, IHopCommand, IHasHopMetadataProvider {

  @CommandLine.Option(
      names = {"-f", "--file"},
      description = "The filename of the workflow or pipeline to open")
  private String filename;

  @CommandLine.Unmatched private String[] unmatchedArguments;

  private CommandLine cmd;
  private IVariables variables;
  private MultiMetadataProvider metadataProvider;
  private ILogChannel log;

  public HopCommandGui() {}

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    this.cmd = cmd;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.log = new LogChannel("HopGui");
    Hop.addMixinPlugins(cmd, ConfigPlugin.CATEGORY_GUI);
  }

  @Override
  public void run() {
    System.setProperty(Const.HOP_PLATFORM_RUNTIME, "GUI");
    try {
      handleMixinOptions();
    } catch (Exception e) {
      throw new CommandLine.ExecutionException(cmd, "Error handling Hop GUI options", e);
    }
    HopGui.main(buildHopGuiArguments());
  }

  private void handleMixinOptions() throws HopException {
    if (cmd == null) {
      return;
    }
    Map<String, Object> mixins = cmd.getMixins();
    for (Object mixin : mixins.values()) {
      if (mixin instanceof IConfigOptions configOptions) {
        configOptions.handleOption(log, this, variables);
      }
    }
  }

  String[] buildHopGuiArguments() {
    List<String> args = new ArrayList<>();
    if (unmatchedArguments != null) {
      for (String unmatched : unmatchedArguments) {
        if (StringUtils.isNotEmpty(unmatched)) {
          args.add(unmatched);
        }
      }
    }
    if (StringUtils.isNotEmpty(filename)) {
      String resolved = HopGuiCommandLine.resolveFile(variables, filename);
      args.add("-file=" + resolved);
    }
    return args.toArray(new String[0]);
  }
}
