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

package org.apache.hop.workflow.actions.ftpput;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
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
import org.apache.hop.workflow.actions.util.FtpClientUtil;
import org.apache.hop.workflow.actions.util.IFtpConnection;
import org.w3c.dom.Node;

/** This defines an FTP put action. */
@Action(
    id = "FTP_PUT",
    name = "i18n::ActionFTPPut.Name",
    description = "i18n::ActionFTPPut.Description",
    image = "FTPPut.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileTransfer",
    keywords = "i18n::ActionFtpPut.keyword",
    documentationUrl = "/workflow/actions/ftpput.html")
public class ActionFtpPut extends ActionBase implements Cloneable, IAction, IFtpConnection {
  private static final Class<?> PKG = ActionFtpPut.class;
  private static final String CONST_SPACE_SHORT = "      ";
  private static final String CONST_PASSWORD = "password";
  private static final String CONST_LOCAL_DIRECTORY = "localDirectory";

  public static final int FTP_DEFAULT_PORT = 21;

  private String serverName;
  private String serverPort;
  private String userName;
  private String password;
  private String remoteDirectory;
  private String localDirectory;
  private String wildcard;
  private boolean binaryMode;
  private int timeout;
  private boolean remove;
  private boolean onlyPuttingNewFiles; /* Don't overwrite files */
  private boolean activeConnection;
  private String controlEncoding; /* how to convert list of filenames e.g. */
  private String proxyHost;

  private String proxyPort; /* string to allow variable substitution */

  private String proxyUsername;

  private String proxyPassword;

  private String socksProxyHost;
  private String socksProxyPort;
  private String socksProxyUsername;
  private String socksProxyPassword;

  /** Implicit encoding used before older version v2.4.1 */
  private static final String LEGACY_CONTROL_ENCODING = "US-ASCII";

  /** Default encoding when making a new ftp action instance. */
  private static final String DEFAULT_CONTROL_ENCODING = "ISO-8859-1";

  public ActionFtpPut(String n) {
    super(n, "");
    serverName = null;
    serverPort = "21";
    socksProxyPort = "1080";
    remoteDirectory = null;
    localDirectory = null;
    setControlEncoding(DEFAULT_CONTROL_ENCODING);
  }

  public ActionFtpPut() {
    this("");
  }

  @Override
  public Object clone() {
    ActionFtpPut je = (ActionFtpPut) super.clone();
    return je;
  }

  @Override
  public String getXml() {
    StringBuilder xml = new StringBuilder(450); // 365 characters in spaces and tag names alone

    xml.append(super.getXml());

    xml.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("servername", serverName));
    xml.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("serverport", serverPort));
    xml.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("username", userName));
    xml.append(CONST_SPACE_SHORT)
        .append(
            XmlHandler.addTagValue(
                CONST_PASSWORD, Encr.encryptPasswordIfNotUsingVariables(getPassword())));
    xml.append(CONST_SPACE_SHORT)
        .append(XmlHandler.addTagValue("remoteDirectory", remoteDirectory));
    xml.append(CONST_SPACE_SHORT)
        .append(XmlHandler.addTagValue(CONST_LOCAL_DIRECTORY, localDirectory));
    xml.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("wildcard", wildcard));
    xml.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("binary", binaryMode));
    xml.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("timeout", timeout));
    xml.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("remove", remove));
    xml.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("only_new", onlyPuttingNewFiles));
    xml.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("active", activeConnection));
    xml.append(CONST_SPACE_SHORT)
        .append(XmlHandler.addTagValue("control_encoding", controlEncoding));

    xml.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("proxy_host", proxyHost));
    xml.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("proxy_port", proxyPort));
    xml.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("proxy_username", proxyUsername));
    xml.append(CONST_SPACE_SHORT)
        .append(
            XmlHandler.addTagValue(
                "proxy_password", Encr.encryptPasswordIfNotUsingVariables(proxyPassword)));
    xml.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("socksproxy_host", socksProxyHost));
    xml.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("socksproxy_port", socksProxyPort));
    xml.append(CONST_SPACE_SHORT)
        .append(XmlHandler.addTagValue("socksproxy_username", socksProxyUsername));
    xml.append(CONST_SPACE_SHORT)
        .append(
            XmlHandler.addTagValue(
                "socksproxy_password",
                Encr.encryptPasswordIfNotUsingVariables(socksProxyPassword)));

    return xml.toString();
  }

  @Override
  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      serverName = XmlHandler.getTagValue(entrynode, "servername");
      serverPort = XmlHandler.getTagValue(entrynode, "serverport");
      userName = XmlHandler.getTagValue(entrynode, "username");
      password =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(entrynode, CONST_PASSWORD));
      remoteDirectory = XmlHandler.getTagValue(entrynode, "remoteDirectory");
      localDirectory = XmlHandler.getTagValue(entrynode, CONST_LOCAL_DIRECTORY);
      wildcard = XmlHandler.getTagValue(entrynode, "wildcard");
      binaryMode = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "binary"));
      timeout = Const.toInt(XmlHandler.getTagValue(entrynode, "timeout"), 10000);
      remove = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "remove"));
      onlyPuttingNewFiles = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "only_new"));
      activeConnection = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "active"));
      controlEncoding = XmlHandler.getTagValue(entrynode, "control_encoding");

      proxyHost = XmlHandler.getTagValue(entrynode, "proxy_host");
      proxyPort = XmlHandler.getTagValue(entrynode, "proxy_port");
      proxyUsername = XmlHandler.getTagValue(entrynode, "proxy_username");
      proxyPassword =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(entrynode, "proxy_password"));
      socksProxyHost = XmlHandler.getTagValue(entrynode, "socksproxy_host");
      socksProxyPort = XmlHandler.getTagValue(entrynode, "socksproxy_port");
      socksProxyUsername = XmlHandler.getTagValue(entrynode, "socksproxy_username");
      socksProxyPassword =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(entrynode, "socksproxy_password"));

      if (controlEncoding == null) {
        // if we couldn't retrieve an encoding, assume it's an old instance and
        // put in the the encoding used before v 2.4.0
        controlEncoding = LEGACY_CONTROL_ENCODING;
      }
    } catch (HopXmlException xe) {
      throw new HopXmlException(
          BaseMessages.getString(PKG, "ActionFtpPut.Log.UnableToLoadFromXml"), xe);
    }
  }

  /**
   * @return Returns the binaryMode.
   */
  @Override
  public boolean isBinaryMode() {
    return binaryMode;
  }

  /**
   * @param binaryMode The binaryMode to set.
   */
  public void setBinaryMode(boolean binaryMode) {
    this.binaryMode = binaryMode;
  }

  /**
   * @param timeout The timeout to set.
   */
  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  /**
   * @return Returns the timeout.
   */
  @Override
  public int getTimeout() {
    return timeout;
  }

  /**
   * @return Returns the onlyGettingNewFiles.
   */
  public boolean isOnlyPuttingNewFiles() {
    return onlyPuttingNewFiles;
  }

  /**
   * @param onlyPuttingNewFiles Only transfer new files to the remote host
   */
  public void setOnlyPuttingNewFiles(boolean onlyPuttingNewFiles) {
    this.onlyPuttingNewFiles = onlyPuttingNewFiles;
  }

  /**
   * Get the control encoding to be used for ftp'ing
   *
   * @return the used encoding
   */
  @Override
  public String getControlEncoding() {
    return controlEncoding;
  }

  /**
   * Set the encoding to be used for ftp'ing. This determines how names are translated in dir e.g.
   * It does impact the contents of the files being ftp'ed.
   *
   * @param encoding The encoding to be used.
   */
  public void setControlEncoding(String encoding) {
    this.controlEncoding = encoding;
  }

  /**
   * @return Returns the remoteDirectory.
   */
  public String getRemoteDirectory() {
    return remoteDirectory;
  }

  /**
   * @param directory The remoteDirectory to set.
   */
  public void setRemoteDirectory(String directory) {
    this.remoteDirectory = directory;
  }

  /**
   * @return Returns the password.
   */
  @Override
  public String getPassword() {
    return password;
  }

  /**
   * @param password The password to set.
   */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * @return Returns the serverName.
   */
  @Override
  public String getServerName() {
    return serverName;
  }

  /**
   * @param serverName The serverName to set.
   */
  public void setServerName(String serverName) {
    this.serverName = serverName;
  }

  /**
   * @return Returns the userName.
   */
  @Override
  public String getUserName() {
    return userName;
  }

  /**
   * @param userName The userName to set.
   */
  public void setUserName(String userName) {
    this.userName = userName;
  }

  /**
   * @return Returns the wildcard.
   */
  public String getWildcard() {
    return wildcard;
  }

  /**
   * @param wildcard The wildcard to set.
   */
  public void setWildcard(String wildcard) {
    this.wildcard = wildcard;
  }

  /**
   * @return Returns the localDirectory.
   */
  public String getLocalDirectory() {
    return localDirectory;
  }

  /**
   * @param directory The localDirectory to set.
   */
  public void setLocalDirectory(String directory) {
    this.localDirectory = directory;
  }

  /**
   * @param remove The remove to set.
   */
  public void setRemove(boolean remove) {
    this.remove = remove;
  }

  /**
   * @return Returns the remove.
   */
  public boolean getRemove() {
    return remove;
  }

  @Override
  public String getServerPort() {
    return serverPort;
  }

  public void setServerPort(String serverPort) {
    this.serverPort = serverPort;
  }

  /**
   * @return the activeConnection
   */
  @Override
  public boolean isActiveConnection() {
    return activeConnection;
  }

  /**
   * @param activeConnection set to true to get an active FTP connection
   */
  public void setActiveConnection(boolean activeConnection) {
    this.activeConnection = activeConnection;
  }

  /**
   * @return Returns the hostname of the ftp-proxy.
   */
  @Override
  public String getProxyHost() {
    return proxyHost;
  }

  /**
   * @param proxyHost The hostname of the proxy.
   */
  public void setProxyHost(String proxyHost) {
    this.proxyHost = proxyHost;
  }

  /**
   * @return Returns the password which is used to authenticate at the proxy.
   */
  @Override
  public String getProxyPassword() {
    return proxyPassword;
  }

  /**
   * @param proxyPassword The password which is used to authenticate at the proxy.
   */
  public void setProxyPassword(String proxyPassword) {
    this.proxyPassword = proxyPassword;
  }

  /**
   * @return Returns the port of the ftp-proxy.
   */
  @Override
  public String getProxyPort() {
    return proxyPort;
  }

  /**
   * @param proxyPort The port of the ftp-proxy.
   */
  public void setProxyPort(String proxyPort) {
    this.proxyPort = proxyPort;
  }

  /**
   * @return Returns the username which is used to authenticate at the proxy.
   */
  @Override
  public String getProxyUsername() {
    return proxyUsername;
  }

  /**
   * @param socksProxyHost The socks proxy host to set
   */
  public void setSocksProxyHost(String socksProxyHost) {
    this.socksProxyHost = socksProxyHost;
  }

  /**
   * @param socksProxyPort The socks proxy port to set
   */
  public void setSocksProxyPort(String socksProxyPort) {
    this.socksProxyPort = socksProxyPort;
  }

  /**
   * @param socksProxyUsername The socks proxy username to set
   */
  public void setSocksProxyUsername(String socksProxyUsername) {
    this.socksProxyUsername = socksProxyUsername;
  }

  /**
   * @param socksProxyPassword The socks proxy password to set
   */
  public void setSocksProxyPassword(String socksProxyPassword) {
    this.socksProxyPassword = socksProxyPassword;
  }

  /**
   * @return The sox proxy host name
   */
  @Override
  public String getSocksProxyHost() {
    return this.socksProxyHost;
  }

  /**
   * @return The socks proxy port
   */
  @Override
  public String getSocksProxyPort() {
    return this.socksProxyPort;
  }

  /**
   * @return The socks proxy username
   */
  @Override
  public String getSocksProxyUsername() {
    return this.socksProxyUsername;
  }

  /**
   * @return The socks proxy password
   */
  @Override
  public String getSocksProxyPassword() {
    return this.socksProxyPassword;
  }

  /**
   * @param proxyUsername The username which is used to authenticate at the proxy.
   */
  public void setProxyUsername(String proxyUsername) {
    this.proxyUsername = proxyUsername;
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    result.setResult(false);
    long filesPut = 0;

    if (isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "ActionFtpPut.Log.Starting"));
    }

    FTPClient ftpclient = null;
    try {
      // Create ftp client to host:port ...
      ftpclient = createAndSetUpFtpClient();

      // move to spool dir ...
      String realRemoteDirectory = resolve(remoteDirectory);
      if (!Utils.isEmpty(realRemoteDirectory)) {
        ftpclient.changeWorkingDirectory(realRemoteDirectory);
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionFtpPut.Log.ChangedDirectory", realRemoteDirectory));
        }
      }

      String realLocalDirectory = resolve(localDirectory);
      if (realLocalDirectory == null) {
        throw new HopException(BaseMessages.getString(PKG, "ActionFtpPut.LocalDir.NotSpecified"));
      } else {
        // handle file:/// prefix
        if (realLocalDirectory.startsWith("file:")) {
          realLocalDirectory = new URI(realLocalDirectory).getPath();
        }
      }

      final List<String> files;
      File localFiles = new File(realLocalDirectory);
      File[] children = localFiles.listFiles();
      if (children == null) {
        files = Collections.emptyList();
      } else {
        files = new ArrayList<>(children.length);
        for (File child : children) {
          // Get filename of file or directory
          if (!child.isDirectory()) {
            files.add(child.getName());
          }
        }
      }
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ActionFtpPut.Log.FoundFileLocalDirectory",
                "" + files.size(),
                realLocalDirectory));
      }

      String realWildcard = resolve(wildcard);
      Pattern pattern;
      if (!Utils.isEmpty(realWildcard)) {
        pattern = Pattern.compile(realWildcard);
      } else {
        pattern = null;
      }

      for (String file : files) {
        if (parentWorkflow.isStopped()) {
          break;
        }

        boolean toBeProcessed = true;

        // First see if the file matches the regular expression!
        if (pattern != null) {
          Matcher matcher = pattern.matcher(file);
          toBeProcessed = matcher.matches();
        }

        if (toBeProcessed) {
          // File exists?
          boolean fileExist = false;
          try {
            fileExist = FtpClientUtil.fileExists(ftpclient, file);
          } catch (Exception e) {
            logError("Error checking for file existence on FTP server for file: " + file, e);
            // Assume file does not exist !!
          }

          if (isDebug()) {
            if (fileExist) {
              logDebug(BaseMessages.getString(PKG, "ActionFtpPut.Log.FileExists", file));
            } else {
              logDebug(BaseMessages.getString(PKG, "ActionFtpPut.Log.FileDoesNotExists", file));
            }
          }

          if (!fileExist || !onlyPuttingNewFiles) {
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG,
                      "ActionFtpPut.Log.PuttingFileToRemoteDirectory",
                      file,
                      realRemoteDirectory));
            }

            String localFilename = realLocalDirectory + Const.FILE_SEPARATOR + file;
            try (InputStream inputStream = HopVfs.getInputStream(localFilename)) {
              if (fileExist) {
                boolean deleted = ftpclient.deleteFile(file);
                if (!deleted) {
                  logError(
                      "Deletion of (existing) file '"
                          + file
                          + "' on the FTP server was not successful with reply string: "
                          + ftpclient.getReplyString());
                }
              }
              if (binaryMode) {
                ftpclient.setFileType(FTP.BINARY_FILE_TYPE);
              }
              boolean success = ftpclient.storeFile(file, inputStream);
              if (success) {
                filesPut++;
              } else {
                logError(
                    "Transfer of file '"
                        + localFilename
                        + "' to the FTP server was not successful with reply string: "
                        + ftpclient.getReplyString());
              }
            }

            // Delete the file if this is needed!
            if (remove) {
              new File(localFilename).delete();
              if (isDetailed()) {
                logDetailed(
                    BaseMessages.getString(PKG, "ActionFtpPut.Log.DeletedFile", localFilename));
              }
            }
          }
        }
      }

      result.setResult(true);
      if (isBasic()) {
        logBasic(BaseMessages.getString(PKG, "ActionFtpPut.Log.WeHavePut", "" + filesPut));
      }
    } catch (Exception e) {
      result.setNrErrors(1);
      logError(BaseMessages.getString(PKG, "ActionFtpPut.Log.ErrorPuttingFiles", e.getMessage()));
      logError(Const.getStackTracker(e));
    } finally {
      if (ftpclient != null && ftpclient.isConnected()) {
        try {
          ftpclient.quit();
        } catch (Exception e) {
          logError(BaseMessages.getString(PKG, "ActionFtpPut.Log.ErrorQuitingFTP", e.getMessage()));
        }
      }
      FtpClientUtil.clearSocksJvmSettings();
    }

    return result;
  }

  // package-local visibility for testing purposes
  FTPClient createAndSetUpFtpClient() throws HopException {

    return FtpClientUtil.connectAndLogin(getLogChannel(), this, this, getName());
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(this, workflowMeta);
    if (!Utils.isEmpty(serverName)) {
      String realServerName = resolve(serverName);
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(realServerName, ResourceType.SERVER));
      references.add(reference);
    }
    return references;
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
            "serverName",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            CONST_LOCAL_DIRECTORY,
            remarks,
            AndValidator.putValidators(
                ActionValidatorUtils.notBlankValidator(),
                ActionValidatorUtils.fileExistsValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "userName",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notBlankValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            CONST_PASSWORD,
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));
    ActionValidatorUtils.andValidator()
        .validate(
            this,
            "serverPort",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.integerValidator()));
  }
}
