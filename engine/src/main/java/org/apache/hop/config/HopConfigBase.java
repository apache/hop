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
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
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

  private boolean handleConfigMixinsOptions(LogChannel log) throws HopException {
    boolean actionTaken = false;
    CommandLine.ParseResult parseResult = cmd.getParseResult();
    Map<String, CommandLine.Model.CommandSpec> mixinSpecs = cmd.getCommandSpec().mixins();
    Map<String, Object> mixins = cmd.getMixins();
    for (Map.Entry<String, Object> entry : mixins.entrySet()) {
      // Only hand a plugin its options when at least one of them was given. Plugins that load
      // their current settings when they are created would otherwise save them again, and report
      // a change, on every unrelated hop-conf command.
      //
      if (entry.getValue() instanceof IConfigOptions configOptions
          && hasMatchedOption(parseResult, mixinSpecs.get(entry.getKey()))) {
        actionTaken = configOptions.handleOption(log, this, variables) || actionTaken;
      }
    }
    return actionTaken;
  }

  /**
   * @param parseResult the parsed command line, null if it isn't known
   * @param mixinSpec the specification of a config plugin mixed into the command
   * @return true if any option or positional parameter of the mixin was given on the command line,
   *     or if that can't be determined
   */
  static boolean hasMatchedOption(
      CommandLine.ParseResult parseResult, CommandLine.Model.CommandSpec mixinSpec) {
    if (parseResult == null || mixinSpec == null) {
      return true;
    }
    for (CommandLine.Model.OptionSpec option : mixinSpec.options()) {
      if (parseResult.hasMatchedOption(option)) {
        return true;
      }
    }
    for (CommandLine.Model.PositionalParamSpec positional : mixinSpec.positionalParameters()) {
      if (parseResult.hasMatchedPositional(positional)) {
        return true;
      }
    }
    return false;
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
    metadataProvider = HopMetadataUtil.createMulti(Encr.getEncoder(), providers, variables);
  }

  protected void buildLogChannel() {
    log = new LogChannel("hop-config");
    log.setSimplified(true);
    log.setLogLevel(determineLogLevel());
    log.logDetailed("Start of Hop Config");
  }
}
