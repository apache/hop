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

package org.apache.hop.py4j;

import java.net.InetAddress;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.encryption.Encr;
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
import py4j.GatewayServer;

@Getter
@Setter
@CommandLine.Command(
    mixinStandardHelpOptions = true,
    name = "python",
    description = "Run the Hop Python gateway (py4j)")
@HopCommand(id = "python", description = "Run the Hop Python gateway")
public class PythonCommand implements Runnable, IHopCommand, IHasHopMetadataProvider {
  private ILogChannel log;
  private CommandLine cmd;
  private IVariables variables;
  private MultiMetadataProvider metadataProvider;

  @CommandLine.Option(
      names = {"--gateway-port"},
      description =
          "The port on which to run the Hop Python (py4j) gateway service.  The default port is 25333.")
  private String gatewayPort;

  @CommandLine.Option(
      names = {"--gateway-ip-address"},
      description =
          "The server on which to run the Hop Python (py4j) gateway service.  The default is 127.0.0.1 (localhost).  Use 0.0.0.0 to make the service widely available.")
  private String gatewayAddress;

  @CommandLine.Option(
      names = {"--gateway-token"},
      description =
          "Only allow connections to the Hop Python (py4j) gateway that provide this token")
  private String gatewayToken;

  @CommandLine.Option(
      names = {"--gateway-stop-password"},
      description =
          "If you specify this password it can be used when halting the server in a Python script.  Without a password the server can not be stopped this way.")
  private String stopPassword;

  private PyHop pyHop;

  public PythonCommand() {
    // Empty by design
  }

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    this.cmd = cmd;
    this.variables = variables;
    this.metadataProvider = metadataProvider;
    this.log = new LogChannel("HopPython");

    pyHop = new PyHop();
    pyHop.initialize(variables, metadataProvider, log);

    // Same plugins as for RUN,DOC, etc. It's mainly for loading projects etc.
    Hop.addMixinPlugins(cmd, ConfigPlugin.CATEGORY_PYTHON);
  }

  /**
   * Sets metadataProvider
   *
   * @param metadataProvider value of metadataProvider
   */
  public void setMetadataProvider(MultiMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
    this.pyHop.setMetadataProvider(metadataProvider);
  }

  /**
   * Sets variables
   *
   * @param variables value of variables
   */
  public void setVariables(IVariables variables) {
    this.variables = variables;
    this.pyHop.setVariables(variables);
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
      System.setProperty(Const.HOP_PLATFORM_RUNTIME, "PYTHON");
      handleMixinActions();

      int port = Const.toInt(variables.resolve(gatewayPort), 25333);
      String ipAddress = variables.resolve(gatewayAddress);
      if (StringUtils.isEmpty(ipAddress)) {
        ipAddress = "127.0.0.1";
      }
      String token = Encr.decryptPasswordOptionallyEncrypted(variables.resolve(gatewayToken));

      // Run the gateway
      //
      GatewayServer.GatewayServerBuilder builder = new GatewayServer.GatewayServerBuilder();
      builder =
          builder.entryPoint(this).javaPort(port).javaAddress(InetAddress.getByName(ipAddress));
      if (StringUtils.isNotEmpty(token)) {
        builder.authToken(token);
      }
      GatewayServer gatewayServer = builder.build();
      gatewayServer.start();
      log.logBasic("The Hop Python Gateway server was started on " + ipAddress + ":" + port);

      pyHop.waitUntilStopped();
    } catch (Exception e) {
      log.logError("Error running the Hop Python Gateway server (py4j)", e);
    }
  }
}
