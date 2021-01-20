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

package org.apache.hop.workflow.actions.as400command;

import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.Plugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.w3c.dom.Node;
import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Message;
import com.ibm.as400.access.CommandCall;

/** Action for executing a AS/400 CL Commands. */
@Action(
    id = "AS400Command",
    name = "i18n::ActionAs400Command.Name",
    description = "i18n::ActionAs400Command.Description",
    image = "as400command.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.Utility",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/as400command.html")
public class ActionAs400Command extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionAs400Command.class; // For Translator

  private static final String TAG_SERVER = "server";

  private static final String TAG_USER = "user";

  private static final String TAG_PASSWORD = "password";

  private static final String TAG_COMMAND = "command";

  private static final String TAG_PROXY_HOST = "proxyHost";

  private static final String TAG_PROXY_PORT = "proxyPort";

  private String server;

  private String user;

  private String password;

  private String command;

  private String proxyHost;

  private String proxyPort;

  public ActionAs400Command(String name, String description) {
    super(name, description);

    server = null;
    user = null;
    password = null;
    proxyHost = null;
    proxyPort = null;
    command = null;
  }

  public ActionAs400Command() {
    this("", "");
  }

  public ActionAs400Command(ActionAs400Command other) {
    this("", "");
    this.server = other.server;
    this.user = other.user;
    this.password = other.password;
    this.proxyHost = other.proxyHost;
    this.proxyPort = other.proxyPort;
    this.command = other.command;
  }

  public Object clone() {
    return new ActionAs400Command(this);
  }

  @Override
  public String getXml() {
    StringBuilder xml = new StringBuilder(100);

    xml.append(super.getXml());
    xml.append(XmlHandler.addTagValue(TAG_SERVER, server));
    xml.append(XmlHandler.addTagValue(TAG_USER, user));
    xml.append(
        XmlHandler.addTagValue(TAG_PASSWORD, Encr.encryptPasswordIfNotUsingVariables(password)));
    xml.append(XmlHandler.addTagValue(TAG_PROXY_HOST, proxyHost));
    xml.append(XmlHandler.addTagValue(TAG_PROXY_PORT, proxyPort));
    xml.append(XmlHandler.addTagValue(TAG_COMMAND, command));

    return xml.toString();
  }

  @Override
  public void loadXml(Node node, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(node);

      server = XmlHandler.getTagValue(node, TAG_SERVER);
      user = XmlHandler.getTagValue(node, TAG_USER);
      password =
          Encr.decryptPasswordOptionallyEncrypted(XmlHandler.getTagValue(node, TAG_PASSWORD));
      proxyHost = XmlHandler.getTagValue(node, TAG_PROXY_HOST);
      proxyPort = XmlHandler.getTagValue(node, TAG_PROXY_PORT);
      command = XmlHandler.getTagValue(node, TAG_COMMAND);
    } catch (Exception e) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ActionAS400Command.UnableToLoadFromXml.Label"), e);
    }
  }

  /** @return Returns the userName. */
  public String getUserName() {
    return user;
  }

  /**
   * Set the username that this job entry should use for connections.
   *
   * @param userName The userName to set.
   */
  public void setUserName(final String userName) {
    this.user = userName;
  }

  /** @return Returns the password. */
  public String getPassword() {
    return password;
  }

  /**
   * Set the password that this job entry should use for connections.
   *
   * @param password The password to set.
   */
  public void setPassword(final String password) {
    this.password = password;
  }

  /** @return Returns the serverName. */
  public String getServerName() {
    return server;
  }

  /** @param serverName The serverName to set. */
  public void setServerName(String serverName) {
    this.server = serverName;
  }

  /**
   * Get the name and port of the proxy server in the format serverName[:port]. If no port is
   * specified, a default will be used.
   *
   * @return
   */
  protected String getProxyServer(String host, String port) {
    String proxyServer = "";
    if (!Utils.isEmpty(host)) {
      proxyServer = resolve(port);
      if (!Utils.isEmpty(port)) proxyServer = proxyServer + ":" + resolve(port);
    }

    return proxyServer;
  }

  public String getProxyHost() {
    return proxyHost;
  }

  public void setProxyHost(final String value) {
    this.proxyHost = value;
  }

  public String getProxyPort() {
    return proxyPort;
  }

  public void setProxyPort(final String value) {
    this.proxyPort = value;
  }

  /**
   * The command to run on the AS/400
   *
   * @return
   */
  public String getCommand() {
    return command;
  }

  public void setCommand(final String command) {
    this.command = command;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return true;
  }

  @Override
  public Result execute(final Result result, int nr) throws HopException {

    AS400 system = null;

    if (isBasic()) {
      logBasic(BaseMessages.getString(PKG, "ActionAs400Command.Log.Started"));
    }

    // Resolve variables
    String serverString = resolve(server);
    String userString = resolve(user);
    String passwordString = Utils.resolvePassword(this, password);
    String commandString = resolve(command);

    try {
      // Create proxy server
      String proxyServer = this.getProxyServer(resolve(proxyHost), resolve(proxyPort));

      // Create an AS400 object
      if (isBasic()) {
        logBasic(
            BaseMessages.getString(
                PKG, "ActionAs400Command.Log.Connecting", serverString, userString));
      }
      system = new AS400(serverString, userString, passwordString, proxyServer);

      // Connect to service
      if (isBasic()) {
        logBasic(BaseMessages.getString(PKG, "ActionAs400Command.Log.Connected", serverString));
      }

      // Run the command
      if (isBasic()) {
        logBasic(BaseMessages.getString(PKG, "ActionAs400Command.Log.CommandRun", commandString));
      }

      final CommandCall command = new CommandCall(system);

      if (command.run(commandString)) {
        if (isBasic()) {
          logBasic(
              BaseMessages.getString(
                  PKG, "ActionAs400Command.Log.CommandSuccess", serverString, commandString));
        }

        result.setNrErrors(0);
        result.setResult(true);
      } else {
        logError(
            BaseMessages.getString(
                PKG, "ActionAs400Command.Log.CommandFailed", serverString, commandString));

        // Get the command results
        for (AS400Message message : command.getMessageList()) {          
          logError(message.getID()+':'+message.getText());
          logError(message.getHelp());
        }
        result.setNrErrors(1);
        result.setResult(false);
      }
    } catch (Exception e) {
      logError(
          BaseMessages.getString(
              PKG, "ActionAs400Command.Log.CommandFailed", serverString, commandString),
          e);      
      result.setNrErrors(1);
      result.setResult(false);
    } finally {
      try {
        // Make sure to disconnect
        system.disconnectService(AS400.COMMAND);
      } catch (Exception e) {
        // Ignore
      }
    }

    return result;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (!Utils.isEmpty(server)) {
      String realServername = variables.resolve(server);
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(realServername, ResourceType.SERVER));
      references.add(reference);
    }
    return references;
  }

  public boolean test(
      IVariables variables,
      final String server,
      final String user,
      final String password,
      final String proxyHost,
      final String proxyPort)
      throws Exception {
    
    // Create proxy server
    String proxyServer =
        this.getProxyServer(variables.resolve(proxyHost), variables.resolve(proxyPort));

    // Create an AS400 object
    AS400 system =
        new AS400(
            variables.resolve(server),
            variables.resolve(user),
            Utils.resolvePassword(this, password),
            proxyServer);
    system.connectService(AS400.COMMAND);

    return true;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {

    ActionValidatorUtils.andValidator()
        .validate(
            this,
            TAG_SERVER,
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            TAG_USER,
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            TAG_PASSWORD,
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            TAG_COMMAND,
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
  }
}
