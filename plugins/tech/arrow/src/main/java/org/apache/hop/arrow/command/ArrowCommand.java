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
 *
 */

package org.apache.hop.arrow.command;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.arrow.flight.ArrowFlightServer;
import org.apache.hop.core.Const;
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
    mixinStandardHelpOptions = true,
    name = "arrow",
    description = "Run the hop arrow command to start a flight or socket server")
@HopCommand(
    id = "arrow",
    description = "Run the hop arrow command to start a flight or socket server")
public class ArrowCommand implements Runnable, IHopCommand, IHasHopMetadataProvider {
  private ILogChannel log;
  private CommandLine cmd;
  private IVariables variables;
  private MultiMetadataProvider metadataProvider;

  @CommandLine.Option(
      names = {"--arrow-flight-host"},
      description =
          "The hostname on which the Apache Arrow Flight server will listen, defaults to 0.0.0.0")
  private String hostname = "0.0.0.0";

  @CommandLine.Option(
      names = {"--arrow-flight-port"},
      description =
          "The port on which the Apache Arrow Flight server will listen, defaults to 33333")
  private String port = "33333";

  public ArrowCommand() {
    // Nothing specific to set
  }

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    this.cmd = cmd;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.log = new LogChannel("Hop Arrow");

    // Same plugins as for RUN,DOC, etc. It's mainly for loading projects etc.
    Hop.addMixinPlugins(cmd, ConfigPlugin.CATEGORY_DOC);
  }

  protected void handleMixinActions() throws HopException {
    // Handle the options of the configuration plugins
    //
    Map<String, Object> mixins = cmd.getMixins();
    for (Map.Entry<String, Object> entry : mixins.entrySet()) {
      Object mixin = entry.getValue();
      if (mixin instanceof IConfigOptions configOptions) {
        configOptions.handleOption(log, this, variables);
      }
    }
  }

  @Override
  public void run() {
    // Check a few variables...
    //
    try {
      System.setProperty(Const.HOP_PLATFORM_RUNTIME, "ARROW");
      handleMixinActions();

      String realHostname = Const.NVL(variables.resolve(hostname), "0.0.0.0");
      int realPort = Const.toInt(variables.resolve(port), 33333);

      // Start the flight server
      ArrowFlightServer server =
          new ArrowFlightServer(realHostname, realPort, variables, metadataProvider, log);
      server.start();

      // For now, we'll wait indefinitely
      //
      //noinspection StatementWithEmptyBody
      while (!server.getFlightServer().awaitTermination(100, TimeUnit.MILLISECONDS)) {
        // Just wait, nothing else
      }
    } catch (Exception e) {
      log.logError("Error running a Hop Arrow Flight server", e);
    }
  }
}
