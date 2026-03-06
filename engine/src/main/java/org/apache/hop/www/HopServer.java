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

package org.apache.hop.www;

import com.google.common.annotations.VisibleForTesting;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import java.io.OutputStream;
import java.io.Serial;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.HopVersionProvider;
import org.apache.hop.core.Result;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.ConfigPluginType;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.JarCache;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.hop.Hop;
import org.apache.hop.hop.plugin.HopCommand;
import org.apache.hop.hop.plugin.IHopCommand;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataInstance;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.pipeline.transform.TransformStatus;
import org.apache.hop.server.HopServerMeta;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@SuppressWarnings("java:S106")
@Getter
@Setter
@Command(
    versionProvider = HopVersionProvider.class,
    mixinStandardHelpOptions = true,
    name = "hop-server",
    description = "Apache Hop server",
    abbreviateSynopsis = true,
    usageHelpAutoWidth = true)
@HopCommand(id = "server", description = "Run a Hop server")
public class HopServer implements Runnable, IHasHopMetadataProvider, IHopCommand {
  private static final Class<?> PKG = HopServer.class;
  private static final String CONST_FOUND = " found.";
  private static final String CONST_SPACE = "        ";
  private static final String CONST_USAGE_EXAMPLE = "HopServer.Usage.Example";

  @CommandLine.Parameters(description = "The XML configuration file", arity = "0..1")
  private List<String> parameters;

  @CommandLine.Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Displays this help message and quits.")
  private boolean helpRequested;

  @CommandLine.Option(
      names = {"-v", "--version"},
      versionHelp = true,
      description = "Displays version info and quits.")
  boolean versionRequested;

  @CommandLine.Option(
      names = {"-p", "--password"},
      description =
          "The server password. Required for administrative operations only, not for starting the server.")
  private String password;

  @CommandLine.Option(
      names = {"-u", "--userName"},
      description =
          "The server user name. Required for administrative operations only, not for starting the server.")
  private String username;

  @CommandLine.Option(
      names = {"--host"},
      description = "The server host name or IP address to use.")
  private String host;

  @CommandLine.Option(
      names = {"--port"},
      description = "The server port to use.")
  private String port;

  @CommandLine.Option(
      names = {"--shutdownport"},
      description = "The server shutdown port to use.")
  private String shutdownPort;

  @CommandLine.Option(
      names = {"--ssl"},
      description =
          "Use SSL connections to encrypt client/server communications using TLS protocols for increased security.")
  private Boolean ssl;

  @CommandLine.Option(
      names = {"-l", "--level"},
      description =
          "The debug level, one of NOTHING, ERROR, MINIMAL, BASIC, DETAILED, DEBUG, ROWLEVEL")
  private String level;

  @CommandLine.Option(
      names = {"-s", "--system-properties"},
      description = "A comma separated list of KEY=VALUE pairs.",
      split = ",")
  private String[] systemProperties = null;

  @CommandLine.Option(
      names = {"-gs", "--general-status"},
      description = "List the general status of the server.")
  private boolean generalStatus;

  @CommandLine.Option(
      names = {"-ps", "--pipeline-status"},
      description = "List the status of the pipeline with this name (also specify the -id option).")
  private String pipelineName;

  @CommandLine.Option(
      names = {"-ws", "--workflow-status"},
      description = "List the status of the workflow with this name (also specify the -id option).")
  private String workflowName;

  @CommandLine.Option(
      names = {"--id"},
      description = "Specify the ID of the pipeline or workflow to query.")
  private String id;

  @CommandLine.Option(
      names = {"-n", "--server-name"},
      description = "The name of the server to start as defined in the metadata.")
  private String serverName;

  @CommandLine.Option(
      names = {"-a", "--auth"},
      description = "Does the Hop web server have authentication enabled")
  private Boolean enableAuth;

  @CommandLine.Option(
      names = {"-k", "--kill"},
      description = "Stop the running Hop server. Use the -u and -p options to authenticate.")
  private boolean kill;

  private class ShutdownHook extends Thread {
    public void run() {
      log.logDetailed(BaseMessages.getString(PKG, "HopServer.Log.ShuttingDown"));
      jvmExit = false;
      shutdown();
    }
  }

  private WebServer webServer;
  private HopServerConfig config;
  private boolean allOK;

  private IVariables variables;
  private CommandLine cmd;
  private ILogChannel log;
  private MultiMetadataProvider metadataProvider;
  private Boolean joinOverride;
  private String realFilename;
  private boolean jvmExit;

  public HopServer() {
    this.config = new HopServerConfig();
    this.joinOverride = null;
    this.jvmExit = true;

    HopServerMeta defaultServer =
        new HopServerMeta("local8080", "localhost", "8080", "8079", "cluster", "cluster");
    this.config.setHopServer(defaultServer);
    this.config.setJoining(true);
  }

  @Override
  public void initialize(
      CommandLine cmd, IVariables variables, MultiMetadataProvider metadataProvider)
      throws HopException {
    this.cmd = cmd;
    this.variables = variables;
    this.metadataProvider = metadataProvider;

    HopClientEnvironment.getInstance().setClient(HopClientEnvironment.ClientType.SERVER);
    Hop.addMixinPlugins(cmd, ConfigPlugin.CATEGORY_SERVER);

    // Add optional metadata folder (legacy)
    //
    addMetadataFolderProvider();
  }

  public void runHopServer() throws Exception {
    log.logDetailed(BaseMessages.getString(PKG, "HopServer.Log.StartingServer"));
    allOK = true;

    HopServerSingleton.setHopServerConfig(config);

    final PipelineMap pipelineMap = HopServerSingleton.getInstance().getPipelineMap();
    pipelineMap.setHopServerConfig(config);
    final WorkflowMap workflowMap = HopServerSingleton.getInstance().getWorkflowMap();
    workflowMap.setHopServerConfig(config);

    HopServerMeta hopServerMeta = config.getHopServer();

    String hostname = hopServerMeta.getHostname();
    int port = parsePort(hopServerMeta);
    int shutdownPort = parseShutdownPort(hopServerMeta);

    if (allOK) {
      boolean shouldJoin = config.isJoining();
      if (joinOverride != null) {
        shouldJoin = joinOverride;
      }

      Thread shutdownHook = new ShutdownHook();

      try {
        // Register a virtual-machine shutdown hook to stop the Hop server gracefully
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        webServer =
            new WebServer(
                log,
                pipelineMap,
                workflowMap,
                hostname,
                port,
                shutdownPort,
                shouldJoin,
                config.getPasswordFile(),
                hopServerMeta.getSslConfig());

        // Right after the Hop server has started and is fully functional
        try {
          ExtensionPointHandler.callExtensionPoint(
              log, variables, HopExtensionPoint.HopServerStartup.id, this);
        } catch (Exception e) {
          // Log error but continue regular operations to make sure HopServer continues to run
          // properly
          //
          log.logError("Error calling extension point HopServerStartup", e);
        }

        // Right after the Hop server shutdown
        try {
          ExtensionPointHandler.callExtensionPoint(
              log, variables, HopExtensionPoint.HopServerTerminate.id, this);
        } catch (Exception e) {
          // Log error but continue regular operations to make sure HopServer continues to run
          // properly
          //
          log.logError("Error calling extension point HopServerTerminate", e);
        }

        log.logDetailed(BaseMessages.getString(PKG, "HopServer.Log.StoppedServer"));
      } finally {
        // Shutdown hooks cannot be removed once the shutdown sequence is started.
        if (jvmExit) {
          Runtime.getRuntime().removeShutdownHook(shutdownHook);
        }
      }
    }
  }

  public void shutdown() {

    if (webServer != null) {
      // Right before the Hop server will shut down
      try {
        ExtensionPointHandler.callExtensionPoint(
            log, variables, HopExtensionPoint.HopServerShutdown.id, this);
      } catch (Exception e) {
        // Log error but continue regular operations to make sure HopServer can be shutdown
        // properly.
        //
        log.logError("Error calling extension point HopServerShutdown", e);
      }

      webServer.stopServer();
    }
  }

  private int parsePort(HopServerMeta hopServerMeta) {

    if (Utils.isEmpty(hopServerMeta.getPort())) {
      return WebServer.DEFAULT_PORT;
    }

    try {
      return Integer.parseInt(hopServerMeta.getPort());
    } catch (Exception e) {
      log.logError(
          BaseMessages.getString(
              PKG,
              "HopServer.Error.CanNotPartPort",
              hopServerMeta.getHostname(),
              hopServerMeta.getPort()),
          e);
      allOK = false;
    }
    return -1;
  }

  private int parseShutdownPort(HopServerMeta hopServerMeta) {

    if (Utils.isEmpty(hopServerMeta.getShutdownPort())) {
      return WebServer.DEFAULT_PORT;
    }

    try {
      return Integer.parseInt(hopServerMeta.getShutdownPort());
    } catch (Exception e) {
      log.logError(
          BaseMessages.getString(
              PKG, "HopServer.Error.CanNotPartShutdownPort", hopServerMeta.getShutdownPort()),
          e);
      allOK = false;
    }
    return -1;
  }

  @Override
  public void run() {

    try {
      System.setProperty(Const.HOP_PLATFORM_RUNTIME, "SERVER");
      log = new LogChannel("HopServer");
      log.setLogLevel(determineLogLevel());

      // Allow plugins to modify the elements loaded so far, before the server is started
      //
      ExtensionPointHandler.callExtensionPoint(
          log, variables, HopExtensionPoint.HopServerInit.id, this);

      // Handle the options of the configuration plugins
      //
      Map<String, Object> mixins = cmd.getMixins();
      for (String key : mixins.keySet()) {
        Object mixin = mixins.get(key);
        if (mixin instanceof IConfigOptions configOptions) {
          configOptions.handleOption(log, this, variables);
        }
      }

      // Build the server configuration from the options...

      // 1) Load from an XML file that describes the complete configuration...
      if (CollectionUtils.size(parameters) == 1) {
        setupByFileName();
      } else {
        // 2) If the server name was specified
        if (StringUtils.isNotEmpty(serverName)) {
          setupByServerName();
        }
      }

      // Overwrite configuration with options in command line
      HopServerMeta serverMeta = config.getHopServer();
      if (StringUtils.isNotEmpty(host)) {
        serverMeta.setHostname(host);
      }
      if (StringUtils.isNotEmpty(port)) {
        serverMeta.setPort(port);
      }
      if (StringUtils.isNotEmpty(shutdownPort)) {
        serverMeta.setShutdownPort(shutdownPort);
      }
      if (StringUtils.isNotEmpty(username)) {
        serverMeta.setUsername(username);
      }
      if (StringUtils.isNotEmpty(password)) {
        serverMeta.setPassword(password);
      }
      if (ssl != null) {
        serverMeta.setSslMode(ssl);
      }

      // Resolve variables properties
      serverMeta.setHostname(variables.resolve(serverMeta.getHostname()));
      serverMeta.setPort(variables.resolve(serverMeta.getPort()));
      serverMeta.setUsername(variables.resolve(serverMeta.getUsername()));
      serverMeta.setPassword(variables.resolve(serverMeta.getPassword()));
      serverMeta.setShutdownPort(variables.resolve(serverMeta.getShutdownPort()));
      serverMeta.setProxyHostname(variables.resolve(serverMeta.getProxyHostname()));
      serverMeta.setProxyPort(variables.resolve(serverMeta.getProxyPort()));
      serverMeta.setNonProxyHosts(variables.resolve(serverMeta.getNonProxyHosts()));

      // Check configuration
      if (StringUtils.isEmpty(serverMeta.getUsername())
          || StringUtils.isEmpty(serverMeta.getPassword())) {
        throw new HopException("Please specify the username and password.");
      }

      // Pass the variables and metadata provider
      //
      config.setVariables(variables);
      config.setMetadataProvider(metadataProvider);

      // enable auth
      if (this.enableAuth != null) {
        config.getHopServer().setEnableAuth(this.enableAuth);
      }

      // See if we need to add the metadata folder (legacy)
      //
      addMetadataFolderProvider();

      // Only query?
      //
      if (handleQueryOptions()) {
        return;
      }

      // At long last, run the actual server...
      //
      runHopServer();
    } catch (Exception e) {
      throw new CommandLine.ExecutionException(
          cmd, "There was an error during the startup of the Hop server", e);
    }
  }

  private void setupByFileName() throws HopException {
    // Calculate the filename, allow plugins to intervene...
    //
    calculateRealFilename();
    FileObject file = HopVfs.getFileObject(realFilename);
    Document document = XmlHandler.loadXmlFile(file);
    Node configNode = XmlHandler.getSubNode(document, HopServerConfig.XML_TAG);
    config = new HopServerConfig(new LogChannel("Hop server config"), configNode);
    config.setFilename(parameters.get(0));
  }

  private void setupByServerName() throws HopException {
    IHopMetadataSerializer<HopServerMeta> serializer =
        metadataProvider.getSerializer(HopServerMeta.class);
    String name = variables.resolve(serverName);
    HopServerMeta meta = serializer.load(name);
    if (meta == null) {
      throw new HopException(
          "Unable to find Hop Server '" + name + "' couldn't be found in the server metadata");
    }

    config.setHopServer(meta);
  }

  private boolean handleQueryOptions() {
    boolean queried = false;
    try {

      if (kill) {

        // Special case if shutdown the server started as service whit Apache Daemon.
        // https://commons.apache.org/proper/commons-daemon/procrun.html
        if (HopServerSingleton.getHopServer() != null) {
          jvmExit = false;
          HopServerSingleton.getHopServer().shutdown();
        } else {
          // Create a custom server meta with shutdown port
          HopServerMeta serverMeta = new HopServerMeta(config.getHopServer());
          serverMeta.setPort(config.getHopServer().getShutdownPort());

          log.logBasic("Send server shutdown to " + serverMeta.getName());

          // Shutdown a remote server
          RemoteHopServer remoteServer = new RemoteHopServer(serverMeta);
          remoteServer.requestShutdownServer(variables);
        }

        return true;
      }

      // Handle username / password
      //
      if (generalStatus
          || StringUtils.isNotEmpty(pipelineName)
          || StringUtils.isNotEmpty(workflowName)) {

        queried = true;

        RemoteHopServer remoteServer = new RemoteHopServer(config.getHopServer());

        if (generalStatus) {
          HopServerStatus status = remoteServer.requestServerStatus(variables);
          // List the pipelines...
          //
          System.out.println("Pipelines: " + status.getPipelineStatusList().size() + CONST_FOUND);
          for (HopServerPipelineStatus pipelineStatus : status.getPipelineStatusList()) {
            printPipelineStatus(pipelineStatus, false);
          }
          System.out.println();
          // List the workflows...
          //
          System.out.println("Workflows: " + status.getWorkflowStatusList().size() + CONST_FOUND);
          for (HopServerWorkflowStatus workflowStatus : status.getWorkflowStatusList()) {
            printWorkflowStatus(workflowStatus, false);
          }
        } else if (StringUtils.isNotEmpty(pipelineName)) {
          if (StringUtils.isEmpty(id)) {
            throw new HopException(
                "Please specify the ID of the pipeline execution to see its status.");
          }
          HopServerPipelineStatus pipelineStatus =
              remoteServer.requestPipelineStatus(variables, pipelineName, id, 0);
          printPipelineStatus(pipelineStatus, true);
        } else if (StringUtils.isNotEmpty(workflowName)) {
          if (StringUtils.isEmpty(id)) {
            throw new HopException(
                "Please specify the ID of the workflow execution to see its status.");
          }
          HopServerWorkflowStatus workflowStatus =
              remoteServer.requestWorkflowStatus(variables, workflowName, id, 0);
          printWorkflowStatus(workflowStatus, true);
        }
      }
    } catch (Exception e) {
      log.logError("Error querying server", e);
      System.exit(8);
    }
    return queried;
  }

  private void printPipelineStatus(HopServerPipelineStatus pipelineStatus, boolean printDetails) {
    Result result = pipelineStatus.getResult();
    System.out.println("  ID: " + pipelineStatus.getId());
    System.out.println("      Name:     " + pipelineStatus.getPipelineName());
    System.out.println("      Status:   " + pipelineStatus.getStatusDescription());
    System.out.println("      Start:    " + formatDate(pipelineStatus.getExecutionStartDate()));
    System.out.println("      End:      " + formatDate(pipelineStatus.getExecutionEndDate()));
    System.out.println("      Log date: " + formatDate(pipelineStatus.getLogDate()));
    if (result != null) {
      System.out.println("      Errors:   " + result.getNrErrors());
    }
    if (printDetails) {
      // Print logging & transform metrics
      //
      System.out.println(
          "      Transforms: " + pipelineStatus.getTransformStatusList().size() + CONST_FOUND);
      int nr = 1;
      for (TransformStatus transformStatus : pipelineStatus.getTransformStatusList()) {
        System.out.println(CONST_SPACE + nr++);
        System.out.println("          Name:      " + transformStatus.getTransformName());
        System.out.println("          Copy:      " + transformStatus.getCopy());
        System.out.println("          Status:    " + transformStatus.getStatusDescription());
        System.out.println("          Input:     " + transformStatus.getLinesInput());
        System.out.println("          Output:    " + transformStatus.getLinesOutput());
        System.out.println("          Read:      " + transformStatus.getLinesRead());
        System.out.println("          Written:   " + transformStatus.getLinesWritten());
        System.out.println("          Rejected:  " + transformStatus.getLinesRejected());
        System.out.println("          Updated:   " + transformStatus.getLinesUpdated());
        System.out.println("          Errors:    " + transformStatus.getErrors());
      }
      printLoggingString(pipelineStatus.getLoggingString());
    }
  }

  private void printLoggingString(String loggingString) {
    if (StringUtils.isEmpty(loggingString)) {
      return;
    }
    System.out.println("      Logging: ");
    String[] lines = loggingString.split("\n");
    for (String line : lines) {
      System.out.println("          " + line);
    }
  }

  private void printWorkflowStatus(HopServerWorkflowStatus workflowStatus, boolean printDetails) {
    Result result = workflowStatus.getResult();
    System.out.println("  ID: " + workflowStatus.getId());
    System.out.println("      Name:     " + workflowStatus.getWorkflowName());
    System.out.println("      Status:   " + workflowStatus.getStatusDescription());
    System.out.println("      Log date: " + formatDate(workflowStatus.getLogDate()));
    if (result != null) {
      System.out.println("      Result:   " + result.isResult());
      System.out.println("      Errors:   " + result.getNrErrors());
    }
    if (printDetails) {
      printLoggingString(workflowStatus.getLoggingString());
    }
  }

  private String formatDate(Date date) {
    if (date == null) {
      return "-";
    }
    return XmlHandler.date2string(date);
  }

  /**
   * This way we can actually use environment variables to parse the real filename. We also allow
   * plugins to look for the filename. In our case, relative to a project folder for example.
   */
  private void calculateRealFilename() throws HopException {
    realFilename = variables.resolve(parameters.get(0));

    ExtensionPointHandler.callExtensionPoint(
        log, variables, HopExtensionPoint.HopServerCalculateFilename.id, this);
  }

  private LogLevel determineLogLevel() {
    return LogLevel.lookupCode(variables.resolve(level));
  }

  public void applySystemProperties() {
    // Set some System properties if there were any
    //
    if (systemProperties != null) {
      for (String parameter : systemProperties) {
        String[] split = parameter.split("=");
        String key = split.length > 0 ? split[0] : null;
        String value = split.length > 1 ? split[1] : null;
        if (StringUtils.isNotEmpty(key) && StringUtils.isNotEmpty(value)) {
          System.setProperty(key, value);
        }
      }
    }
  }

  private void buildVariables() {
    // Also grabs the system properties from hop.config.
    //
    variables = Variables.getADefaultVariableSpace();
  }

  public static void main(String[] args) {
    String[] arguments =
        Stream.of(args)
            .flatMap(a -> Stream.of(a.split("(?=--)")))
            .filter(a -> !a.isEmpty())
            .toArray(String[]::new);

    HopServer hopServer = new HopServer();

    try {
      // Create the command line options...
      //
      CommandLine command = new CommandLine(hopServer);

      // Apply the system properties to the JVM
      //
      hopServer.applySystemProperties();

      // Initialize the Hop environment: load plugins and more
      //
      HopClientEnvironment.getInstance().setClient(HopClientEnvironment.ClientType.SERVER);
      HopEnvironment.init();

      // Picks up the system settings in the variables
      //
      hopServer.buildVariables();

      // Clear the jar file cache so that we don't waste memory...
      //
      JarCache.getInstance().clear();

      // Set up the metadata to use
      //
      hopServer.metadataProvider =
          HopMetadataUtil.getStandardHopMetadataProvider(hopServer.variables);
      HopMetadataInstance.setMetadataProvider(hopServer.metadataProvider);

      // Now add server configuration plugins...
      //
      List<IPlugin> configPlugins = PluginRegistry.getInstance().getPlugins(ConfigPluginType.class);
      for (IPlugin configPlugin : configPlugins) {
        // Load only the plugins of the "server" category
        if (ConfigPlugin.CATEGORY_SERVER.equals(configPlugin.getCategory())) {
          IConfigOptions configOptions =
              PluginRegistry.getInstance().loadClass(configPlugin, IConfigOptions.class);
          command.addMixin(configPlugin.getIds()[0], configOptions);
        }
      }
      hopServer.setCmd(command);
      Hop.addMixinPlugins(command, ConfigPlugin.CATEGORY_SERVER);
      hopServer.setCmd(command);

      // Add optional metadata folder (legacy)
      //
      hopServer.addMetadataFolderProvider();

      // This will calculate the option values and put them in HopServer or the plugin classes
      //
      command.parseArgs(arguments);

      if (CollectionUtils.size(hopServer.parameters) > 1) {
        throw new CommandLine.ParameterException(command, "Too many parameters");
      }

      // Execute the command line
      if (command.isUsageHelpRequested()) {
        command.usage(System.out);
      } else if (command.isVersionHelpRequested()) {
        command.printVersionHelp(System.out);
      } else {
        hopServer.run();
      }

      HopEnvironment.shutdown();
    } catch (CommandLine.ParameterException e) {
      System.err.println(e.getMessage());
      hopServer.cmd.usage(System.err);
      printExtraUsageExamples();
      System.exit(9);
    } catch (CommandLine.ExecutionException e) {
      System.err.println("Error found during execution!");
      System.err.println(Const.getStackTracker(e));
      System.exit(1);
    } catch (Exception e) {
      System.err.println("General error found, something went horribly wrong!");
      System.err.println(Const.getStackTracker(e));
      System.exit(2);
    }

    // If we exit now it's because the server was stopped and this is not an error
    //
    if (hopServer.jvmExit) {
      System.exit(0);
    }
  }

  private void addMetadataFolderProvider() {
    // Do we need to add one more provider for the metadata map?
    String metadataFolder = config.getMetadataFolder();
    if (StringUtils.isNotEmpty(metadataFolder)) {
      // Get the metadata from the specified metadata folder...
      //
      metadataProvider
          .getProviders()
          .add(new JsonMetadataProvider(Encr.getEncoder(), metadataFolder, variables));
    }
  }

  private static void printExtraUsageExamples() {
    System.err.println();
    System.err.println(
        BaseMessages.getString(PKG, CONST_USAGE_EXAMPLE)
            + ": hop-server.sh --host 0.0.0.0 --port 8080");
    System.err.println(
        BaseMessages.getString(PKG, CONST_USAGE_EXAMPLE)
            + ": hop-server.sh -host=192.168.1.221 -port=8081 -shutdownport=8082");
    System.err.println(
        BaseMessages.getString(PKG, CONST_USAGE_EXAMPLE)
            + ": hop-server.sh --environment aura-gcp gs://apachehop/hop-server-config.xml");
    System.err.println(
        BaseMessages.getString(PKG, CONST_USAGE_EXAMPLE)
            + ": hop-server.sh --host 127.0.0.1 --kill --shutdownport 8082 --userName cluster --password cluster");
  }

  private static void shutdown(
      String hostname, String port, String shutdownPort, String username, String password) {
    try {
      callStopHopServerRestService(hostname, port, shutdownPort, username, password);
    } catch (Exception e) {
      e.printStackTrace(System.err);
    }
  }

  /**
   * Checks that HopServer is running and if so, shuts down the HopServer server
   *
   * @param hostname The hostname
   * @param port The port
   * @param username The username
   * @param password The password
   * @throws HopServerCommandException In case there was a command line error
   */
  @VisibleForTesting
  static void callStopHopServerRestService(
      String hostname, String port, String shutdownPort, String username, String password)
      throws HopServerCommandException {
    // get information about the remote connection
    try {
      HopClientEnvironment.init();

      HttpAuthenticationFeature authFeature =
          HttpAuthenticationFeature.basicBuilder()
              .credentials(username, Encr.decryptPasswordOptionallyEncrypted(password))
              .build();

      ClientConfig clientConfig = new ClientConfig();
      Client client = ClientBuilder.newClient(clientConfig);
      client.register(authFeature);

      // check if the user can access the hop server. Don't really need this call but may want to
      // check it's output at
      // some point
      String contextURL = "http://" + hostname + ":" + port + "/hop";
      WebTarget target = client.target(contextURL + "/status/?xml=Y");
      String response = target.request().get(String.class);
      if (response == null || !response.contains("<serverstatus>")) {
        throw new HopServerCommandException(
            BaseMessages.getString(PKG, "HopServer.Error.NoServerFound", hostname, port));
      }

      Socket s = new Socket(InetAddress.getByName(hostname), Integer.parseInt(shutdownPort));
      OutputStream out = s.getOutputStream();
      out.write(("\r\n").getBytes());
      out.flush();
      s.close();

    } catch (Exception e) {
      throw new HopServerCommandException(
          BaseMessages.getString(PKG, "HopServer.Error.NoServerFound", hostname, port), e);
    }
  }

  /** Exception generated when command line fails */
  public static class HopServerCommandException extends Exception {
    @Serial private static final long serialVersionUID = 1L;

    public HopServerCommandException(final String message) {
      super(message);
    }

    public HopServerCommandException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }
}
