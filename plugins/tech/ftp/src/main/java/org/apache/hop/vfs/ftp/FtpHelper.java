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

package org.apache.hop.vfs.ftp;

import java.util.List;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.resource.IResourceHolder;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;

/**
 * What the FTP actions have in common on top of {@link FtpClientFactory}.
 *
 * <p>Every action carries its server settings inline - it did so long before named connections
 * existed and workflows out there still hold them - and can point at a named connection instead.
 * The methods here are what turns those two into one.
 */
@UtilityClass
public class FtpHelper {

  /**
   * Open the connection an action should work with: the named FTP connection when it has one, its
   * own inline server settings otherwise.
   *
   * @param action the action, which describes its inline settings as an {@link IFtpConnection}
   * @param connectionName the name of a connection in the metadata, empty to use the inline
   *     settings
   * @return a connected and logged in client; the caller disconnects it
   */
  public static <T extends ActionBase & IFtpConnection> FTPClient connect(
      T action, String connectionName) throws HopException {
    return FtpClientFactory.connectAndLogin(
        action.getLogChannel(), action, connectionOf(action, connectionName));
  }

  /**
   * The connection an action works with: the named FTP connection when it has one, the inline
   * settings of the action otherwise.
   */
  public static <T extends ActionBase & IFtpConnection> IFtpConnection connectionOf(
      T action, String connectionName) throws HopException {
    String name = action.resolve(connectionName);
    if (StringUtils.isEmpty(name)) {
      return action;
    }
    return FtpConnections.load(action.getMetadataProvider(), name);
  }

  /** Log out and disconnect, logging but never throwing whatever goes wrong on the way out. */
  public static void disconnect(ILogChannel log, FTPClient client) {
    FtpClientFactory.disconnectQuietly(log, client);
  }

  /**
   * What to call the server in a log line: the name of the connection when there is one, the
   * resolved server name otherwise. Never the unresolved setting, which would log {@code
   * ${FTP_SERVER}} rather than the host it stands for.
   */
  public static String describeTarget(ActionBase action, String connectionName, String serverName) {
    String name = action.resolve(connectionName);
    if (StringUtils.isNotEmpty(name)) {
      return name;
    }
    return Const.NVL(action.resolve(serverName), "");
  }

  /** Whether a connection name points at a named connection rather than at inline settings. */
  public static boolean isUsingConnection(String connectionName) {
    return StringUtils.isNotEmpty(connectionName);
  }

  /**
   * Adds a server {@link ResourceReference} to the given list if the provided server name is not
   * empty.
   *
   * @param references the list of {@link ResourceReference} objects to add to
   * @param serverName the server name to resolve and reference
   * @param action the action the reference belongs to
   * @param holder the holder of the reference
   */
  public static void addServerResourceReferenceIfPresent(
      List<ResourceReference> references,
      String serverName,
      ActionBase action,
      IResourceHolder holder) {
    if (Utils.isEmpty(serverName)) {
      return;
    }

    String realServerName = action.resolve(serverName);
    ResourceReference reference = new ResourceReference(holder);
    reference
        .getEntries()
        .add(new ResourceEntry(realServerName, ResourceEntry.ResourceType.SERVER));
    references.add(reference);
  }

  /**
   * Check that a directory setting names a directory which is there.
   *
   * <p>The file-exists validator reads the variables out of the context it's handed, so it needs a
   * context of its own rather than the bare list of validators the other checks use. Without it it
   * reports "Key missing from context" against every action it's asked about.
   */
  public static void checkDirectoryExists(
      List<ICheckResult> remarks, ActionBase action, IVariables variables, String propertyName) {
    ValidatorContext context = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(
        context, variables == null ? new Variables() : variables);
    AndValidator.putValidators(
        context,
        ActionValidatorUtils.notBlankValidator(),
        ActionValidatorUtils.fileExistsValidator());
    ActionValidatorUtils.andValidator().validate(action, propertyName, remarks, context);
  }

  /**
   * The checks every FTP action shares: it needs a server to talk to and credentials to talk with,
   * unless a named connection brings those along.
   */
  public static void checkServerSettings(
      List<ICheckResult> remarks, ActionBase action, String connectionName) {
    if (isUsingConnection(connectionName)) {
      // Everything below lives in the metadata and is checked when the connection is loaded.
      return;
    }
    ActionValidatorUtils.andValidator()
        .validate(
            action,
            "serverName",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            action,
            "userName",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            action,
            "password",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));
  }
}
