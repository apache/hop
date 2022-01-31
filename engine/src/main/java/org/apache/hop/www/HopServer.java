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
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.json.JSONConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.HopEnvironment;
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
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.pipeline.transform.TransformStatus;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import picocli.CommandLine;
import picocli.CommandLine.Parameters;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class HopServer implements Runnable, IHasHopMetadataProvider {
  private static final Class<?> PKG = HopServer.class; // For Translator

  @Parameters(description = "One XML configuration file or a hostname and port", arity = "1..2")
  private List<String> parameters;

  @picocli.CommandLine.Option(
      names = {"-k", "--kill"},
      description =
          "Stop the running hopServer server.  This is only allowed when using the hostname/port form of the command. Use the -s and -u options to authenticate")
  private boolean killServer;

  @picocli.CommandLine.Option(
      names = {"-p", "--password"},
      description =
          "The server password.  Required for administrative operations only, not for starting the server.")
  private String password;

  @picocli.CommandLine.Option(
      names = {"-u", "--userName"},
      description =
          "The server user name.  Required for administrative operations only, not for starting the server.")
  private String username;

  @picocli.CommandLine.Option(
      names = {"-l", "--level"},
      description = "The debug level, one of NONE, MINIMAL, BASIC, DETAILED, DEBUG, ROWLEVEL")
  private String level;

  @CommandLine.Option(
      names = {"-s", "--system-properties"},
      description = "A comma separated list of KEY=VALUE pairs",
      split = ",")
  private String[] systemProperties = null;

  @CommandLine.Option(
      names = {"-gs", "--general-status"},
      description = "List the general status of the server")
  private boolean generalStatus;

  @CommandLine.Option(
      names = {"-ps", "--pipeline-status"},
      description = "List the status of the pipeline with this name (also specify the -id option)")
  private String pipelineName;

  @CommandLine.Option(
      names = {"-ws", "--workflow-status"},
      description = "List the status of the workflow with this name (also specify the -id option)")
  private String workflowName;

  @CommandLine.Option(
      names = {"-id"},
      description = "Specify the ID of the pipeline or workflow to query")
  private String id;

  private WebServer webServer;
  private HopServerConfig config;
  private boolean allOK;

  private IVariables variables;
  private picocli.CommandLine cmd;
  private ILogChannel log;
  private MultiMetadataProvider metadataProvider;
  private Boolean joinOverride;
  private String realFilename;

  public HopServer() {
    this.config = new HopServerConfig();
    this.joinOverride = null;

    org.apache.hop.server.HopServer defaultServer =
        new org.apache.hop.server.HopServer("local8080", "localhost", "8080", "cluster", "cluster");
    this.config.setHopServer(defaultServer);
    this.config.setJoining(true);
  }

  public void runHopServer() throws Exception {
    allOK = true;

    HopServerSingleton.setHopServerConfig(config);
    log = HopServerSingleton.getInstance().getLog();

    final PipelineMap pipelineMap = HopServerSingleton.getInstance().getPipelineMap();
    pipelineMap.setHopServerConfig(config);
    final WorkflowMap workflowMap = HopServerSingleton.getInstance().getWorkflowMap();
    workflowMap.setHopServerConfig(config);

    org.apache.hop.server.HopServer hopServer = config.getHopServer();

    String hostname = hopServer.getHostname();
    int port = WebServer.PORT;
    if (!Utils.isEmpty(hopServer.getPort())) {
      try {
        port = Integer.parseInt(hopServer.getPort());
      } catch (Exception e) {
        log.logError(
            BaseMessages.getString(
                PKG, "HopServer.Error.CanNotPartPort", hopServer.getHostname(), "" + port),
            e);
        allOK = false;
      }
    }

    if (allOK) {
      boolean shouldJoin = config.isJoining();
      if (joinOverride != null) {
        shouldJoin = joinOverride;
      }

      this.webServer =
          new WebServer(
              log,
              pipelineMap,
              workflowMap,
              hostname,
              port,
              shouldJoin,
              config.getPasswordFile(),
              hopServer.getSslConfig());
    }

    ExtensionPointHandler.callExtensionPoint(
        log, variables, HopExtensionPoint.HopServerShutdown.id, this);
  }

  @Override
  public void run() {
    try {
      log = new LogChannel("HopServer");
      log.setLogLevel(determineLogLevel());
      log.logDetailed("Start of Hop Server");

      // Allow plugins to modify the elements loaded so far, before the server is started
      //
      ExtensionPointHandler.callExtensionPoint(
          log, variables, HopExtensionPoint.HopServerInit.id, this);

      // Handle the options of the configuration plugins
      //
      Map<String, Object> mixins = cmd.getMixins();
      for (String key : mixins.keySet()) {
        Object mixin = mixins.get(key);
        if (mixin instanceof IConfigOptions) {
          IConfigOptions configOptions = (IConfigOptions) mixin;
          configOptions.handleOption(log, this, variables);
        }
      }

      // Build the server configuration from the options...
      //
      // Load from an XML file that describes the complete configuration...
      //
      if (parameters != null && parameters.size() == 1) {
        if (killServer) {
          throw new HopServerCommandException(
              BaseMessages.getString(PKG, "HopServer.Error.illegalStop"));
        }
        // Calculate the filename, allow plugins to intervene...
        //
        calculateRealFilename();
        FileObject file = HopVfs.getFileObject(realFilename);
        Document document = XmlHandler.loadXmlFile(file);
        Node configNode = XmlHandler.getSubNode(document, HopServerConfig.XML_TAG);
        config = new HopServerConfig(new LogChannel("Hop server config"), configNode);
        if (config.getAutoSequence() != null) {
          config.readAutoSequences();
        }
        config.setFilename(parameters.get(0));
      }
      if (parameters != null
          && parameters.size() == 2
          && StringUtils.isNotEmpty(parameters.get(0))
          && StringUtils.isNotEmpty(parameters.get(1))) {
        String hostname = parameters.get(0);
        String port = parameters.get(1);

        if (killServer) {
          String user = variables.resolve(username);
          String password = variables.resolve(this.password);
          shutdown(hostname, port, user, password);
          System.exit(0);
        }

        org.apache.hop.server.HopServer hopServer =
            new org.apache.hop.server.HopServer(hostname + ":" + port, hostname, port, null, null);

        config = new HopServerConfig();
        config.setHopServer(hopServer);
        config.setJoining(true);
      }

      // Pass the variables and metadata provider
      //
      config.setVariables(variables);
      config.setMetadataProvider(metadataProvider);

      // See if we need to add the metadata folder (legacy)
      //
      addMetadataFolderProvider();

      // Only query?
      //
      if (handleQueryOptions()) {
        System.exit(0);
      }

      // At long last, run the actual server...
      //
      runHopServer();
    } catch (Exception e) {
      throw new picocli.CommandLine.ExecutionException(
          cmd, "There was an error during the startup of the Hop server", e);
    }
  }

  private boolean handleQueryOptions() {
    boolean queried = false;
    try {
      // Handle username / password
      //
      if (generalStatus
          || StringUtils.isNotEmpty(pipelineName)
          || StringUtils.isNotEmpty(workflowName)) {
        if (StringUtils.isEmpty(username) || StringUtils.isEmpty(password)) {
          throw new HopException(
              "Please specify the username and password to query the server status");
        }
        config.getHopServer().setUsername(variables.resolve(username));
        config.getHopServer().setPassword(variables.resolve(password));
      }

      if (generalStatus) {
        queried = true;
        HopServerStatus status = config.getHopServer().getStatus(variables);
        // List the pipelines...
        //
        System.out.println("Pipelines: " + status.getPipelineStatusList().size() + " found.");
        for (HopServerPipelineStatus pipelineStatus : status.getPipelineStatusList()) {
          printPipelineStatus(pipelineStatus, false);
        }
        System.out.println();
        // List the workflows...
        //
        System.out.println("Workflows: " + status.getWorkflowStatusList().size() + " found.");
        for (HopServerWorkflowStatus workflowStatus : status.getWorkflowStatusList()) {
          printWorkflowStatus(workflowStatus, false);
        }
      } else if (StringUtils.isNotEmpty(pipelineName)) {
        queried = true;
        if (StringUtils.isEmpty(id)) {
          throw new HopException(
              "Please specify the ID of the pipeline execution to see its status.");
        }
        HopServerPipelineStatus pipelineStatus =
            config.getHopServer().getPipelineStatus(variables, pipelineName, id, 0);
        printPipelineStatus(pipelineStatus, true);
      } else if (StringUtils.isNotEmpty(workflowName)) {
        queried = true;
        if (StringUtils.isEmpty(id)) {
          throw new HopException(
              "Please specify the ID of the workflow execution to see its status.");
        }
        HopServerWorkflowStatus workflowStatus =
            config.getHopServer().getWorkflowStatus(variables, workflowName, id, 0);
        printWorkflowStatus(workflowStatus, true);
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
          "      Transforms: " + pipelineStatus.getTransformStatusList().size() + " found.");
      int nr = 1;
      for (TransformStatus transformStatus : pipelineStatus.getTransformStatusList()) {
        System.out.println("        " + nr++);
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
      System.out.println("      Result:   " + result.getResult());
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
    return LogLevel.getLogLevelForCode(variables.resolve(level));
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

  private void buildVariableSpace() {
    // Also grabs the system properties from hop.config.
    //
    variables = Variables.getADefaultVariableSpace();
  }

  public static void main(String[] args) {

    HopServer hopServer = new HopServer();

    try {
      // Create the command line options...
      //
      picocli.CommandLine cmd = new picocli.CommandLine(hopServer);

      // Apply the system properties to the JVM
      //
      hopServer.applySystemProperties();

      // Initialize the Hop environment: load plugins and more
      //
      HopClientEnvironment.getInstance().setClient(HopClientEnvironment.ClientType.SERVER);
      HopEnvironment.init();

      // Picks up the system settings in the variables
      //
      hopServer.buildVariableSpace();

      // Set up the metadata to use
      //
      hopServer.metadataProvider =
          HopMetadataUtil.getStandardHopMetadataProvider(hopServer.variables);

      // Now add server configuration plugins...
      //
      List<IPlugin> configPlugins = PluginRegistry.getInstance().getPlugins(ConfigPluginType.class);
      for (IPlugin configPlugin : configPlugins) {
        // Load only the plugins of the "run" category
        if (ConfigPlugin.CATEGORY_SERVER.equals(configPlugin.getCategory())) {
          IConfigOptions configOptions =
              PluginRegistry.getInstance().loadClass(configPlugin, IConfigOptions.class);
          cmd.addMixin(configPlugin.getIds()[0], configOptions);
        }
      }
      hopServer.setCmd(cmd);

      // Add optional metadata folder (legacy)
      //
      hopServer.addMetadataFolderProvider();

      // This will calculate the option values and put them in HopRun or the plugin classes
      //
      picocli.CommandLine.ParseResult parseResult = cmd.parseArgs(args);

      if (picocli.CommandLine.printHelpIfRequested(parseResult)) {
        printExtraUsageExamples();
        System.exit(1);
      } else {
        hopServer.run();

        // If we exit now it's because the server was stopped and this is not an error
        //
        System.exit(0);
      }
    } catch (picocli.CommandLine.ParameterException e) {
      System.err.println(e.getMessage());
      hopServer.cmd.usage(System.err);
      printExtraUsageExamples();
      System.exit(9);
    } catch (picocli.CommandLine.ExecutionException e) {
      System.err.println("Error found during execution!");
      System.err.println(Const.getStackTracker(e));
      System.exit(1);
    } catch (Exception e) {
      System.err.println("General error found, something went horribly wrong!");
      System.err.println(Const.getStackTracker(e));
      System.exit(2);
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
        BaseMessages.getString(PKG, "HopServer.Usage.Example") + ": hop-server.sh 0.0.0.0 8080");
    System.err.println(
        BaseMessages.getString(PKG, "HopServer.Usage.Example")
            + ": hop-server.sh 192.168.1.221 8081");
    System.err.println();
    System.err.println(
        BaseMessages.getString(PKG, "HopServer.Usage.Example")
            + ": hop-server.sh -e aura-gcp gs://apachehop/hop-server-config.xml");
    System.err.println(
        BaseMessages.getString(PKG, "HopServer.Usage.Example")
            + ": hop-server.sh 127.0.0.1 8080 --kill --userName cluster --password cluster");
  }

  /** @return the webServer */
  public WebServer getWebServer() {
    return webServer;
  }

  /** @param webServer the webServer to set */
  public void setWebServer(WebServer webServer) {
    this.webServer = webServer;
  }

  /** @return the hop server (HopServer) configuration */
  public HopServerConfig getConfig() {
    return config;
  }

  /** @param config the hop server (HopServer) configuration */
  public void setConfig(HopServerConfig config) {
    this.config = config;
  }

  private static void shutdown(String hostname, String port, String username, String password) {
    try {
      callStopHopServerRestService(hostname, port, username, password);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Checks that HopServer is running and if so, shuts down the HopServer server
   *
   * @param hostname
   * @param port
   * @param username
   * @param password
   * @throws HopServerCommandException
   */
  @VisibleForTesting
  static void callStopHopServerRestService(
      String hostname, String port, String username, String password)
      throws HopServerCommandException {
    // get information about the remote connection
    try {
      HopClientEnvironment.init();

      ClientConfig clientConfig = new DefaultClientConfig();
      clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
      Client client = Client.create(clientConfig);

      client.addFilter(
          new HTTPBasicAuthFilter(username, Encr.decryptPasswordOptionallyEncrypted(password)));

      // check if the user can access the hop server. Don't really need this call but may want to
      // check it's output at
      // some point
      String contextURL = "http://" + hostname + ":" + port + "/hop";
      WebResource resource = client.resource(contextURL + "/status/?xml=Y");
      String response = resource.get(String.class);
      if (response == null || !response.contains("<serverstatus>")) {
        throw new HopServerCommandException(
            BaseMessages.getString(PKG, "HopServer.Error.NoServerFound", hostname, "" + port));
      }

      // This is the call that matters
      resource = client.resource(contextURL + "/stopHopServer");
      response = resource.get(String.class);
      if (response == null || !response.contains("Shutting Down")) {
        throw new HopServerCommandException(
            BaseMessages.getString(PKG, "HopServer.Error.NoShutdown", hostname, "" + port));
      }
    } catch (Exception e) {
      throw new HopServerCommandException(
          BaseMessages.getString(PKG, "HopServer.Error.NoServerFound", hostname, "" + port), e);
    }
  }

  /** Exception generated when command line fails */
  public static class HopServerCommandException extends Exception {
    private static final long serialVersionUID = 1L;

    public HopServerCommandException(final String message) {
      super(message);
    }

    public HopServerCommandException(final String message, final Throwable cause) {
      super(message, cause);
    }

  }

  /**
   * Gets parameters
   *
   * @return value of parameters
   */
  public List<String> getParameters() {
    return parameters;
  }

  /** @param parameters The parameters to set */
  public void setParameters(List<String> parameters) {
    this.parameters = parameters;
  }

  /**
   * Gets systemProperties
   *
   * @return value of systemProperties
   */
  public String[] getSystemProperties() {
    return systemProperties;
  }

  /** @param systemProperties The systemProperties to set */
  public void setSystemProperties(String[] systemProperties) {
    this.systemProperties = systemProperties;
  }

  /**
   * Gets stopServer
   *
   * @return value of stopServer
   */
  public boolean isKillServer() {
    return killServer;
  }

  /** @param killServer The stopServer to set */
  public void setKillServer(boolean killServer) {
    this.killServer = killServer;
  }

  /**
   * Gets stopPassword
   *
   * @return value of stopPassword
   */
  public String getPassword() {
    return password;
  }

  /** @param password The stopPassword to set */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * Gets stopUsername
   *
   * @return value of stopUsername
   */
  public String getUsername() {
    return username;
  }

  /** @param username The stopUsername to set */
  public void setUsername(String username) {
    this.username = username;
  }

  /**
   * Gets level
   *
   * @return value of level
   */
  public String getLevel() {
    return level;
  }

  /** @param level The level to set */
  public void setLevel(String level) {
    this.level = level;
  }

  /**
   * Gets allOK
   *
   * @return value of allOK
   */
  public boolean isAllOK() {
    return allOK;
  }

  /** @param allOK The allOK to set */
  public void setAllOK(boolean allOK) {
    this.allOK = allOK;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /** @param variables The variables to set */
  public void setVariables(IVariables variables) {
    this.variables = variables;
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
   * Gets log
   *
   * @return value of log
   */
  public ILogChannel getLog() {
    return log;
  }

  /** @param log The log to set */
  public void setLog(ILogChannel log) {
    this.log = log;
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  @Override
  public MultiMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /** @param metadataProvider The metadataProvider to set */
  public void setMetadataProvider(MultiMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }

  /**
   * Gets joinOverride
   *
   * @return value of joinOverride
   */
  public Boolean getJoinOverride() {
    return joinOverride;
  }

  /** @param joinOverride The joinOverride to set */
  public void setJoinOverride(Boolean joinOverride) {
    this.joinOverride = joinOverride;
  }

  /**
   * Gets realFilename
   *
   * @return value of realFilename
   */
  public String getRealFilename() {
    return realFilename;
  }

  /** @param realFilename The realFilename to set */
  public void setRealFilename(String realFilename) {
    this.realFilename = realFilename;
  }
}
