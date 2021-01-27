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

package org.apache.hop.workflow.actions.ftpdelete;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
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
import org.apache.hop.workflow.actions.sftp.SftpClient;
import org.apache.hop.workflow.actions.util.FtpClientUtil;
import org.apache.hop.workflow.actions.util.IFtpConnection;
import org.w3c.dom.Node;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines an FTP action.
 *
 * @author Matt
 * @since 05-11-2003
 */
@Action(
    id = "FTP_DELETE",
    name = "i18n::ActionFTPDelete.Name",
    description = "i18n::ActionFTPDelete.Description",
    image = "FTPDelete.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileTransfer",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/ftpdelete.html")
public class ActionFtpDelete extends ActionBase implements Cloneable, IAction, IFtpConnection {
  private static final Class<?> PKG = ActionFtpDelete.class; // For Translator

  private String serverName;

  private String serverPort;

  private String userName;

  private String password;

  private String remoteDirectory;

  private String wildcard;

  private int timeout;

  private boolean activeConnection;

  private boolean publicPublicKey;

  private String keyFilename;

  private String keyFilePass;

  private boolean useProxy;

  private String proxyHost;

  private String proxyPort; /* string to allow variable substitution */

  private String proxyUsername;

  private String proxyPassword;

  private String socksProxyHost;

  private String socksProxyPort;

  private String socksProxyUsername;

  private String socksProxyPassword;

  private String protocol;

  public static final String PROTOCOL_FTP = "FTP";

  public static final String PROTOCOL_SFTP = "SFTP";

  public String SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED = "success_when_at_least";

  public String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";

  public String SUCCESS_IF_ALL_FILES_DOWNLOADED = "success_is_all_files_downloaded";

  private String nrLimitSuccess;

  private String successCondition;

  private boolean copyPrevious;

  long nrErrors = 0;

  long nrFilesDeleted = 0;

  boolean successConditionBroken = false;

  String targetFilename = null;

  int limitFiles = 0;

  FTPClient ftpclient = null;

  SftpClient sftpclient = null;

  public ActionFtpDelete(String n) {
    super(n, "");
    copyPrevious = false;
    protocol = PROTOCOL_FTP;
    serverPort = "21";
    socksProxyPort = "1080";
    nrLimitSuccess = "10";
    successCondition = SUCCESS_IF_ALL_FILES_DOWNLOADED;
    publicPublicKey = false;
    keyFilename = null;
    keyFilePass = null;
    serverName = null;
  }

  public ActionFtpDelete() {
    this("");
  }

  public Object clone() {
    ActionFtpDelete je = (ActionFtpDelete) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(550); // 448 characters in spaces and tag names alone

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("protocol", protocol));
    retval.append("      ").append(XmlHandler.addTagValue("servername", serverName));
    retval.append("      ").append(XmlHandler.addTagValue("port", serverPort));
    retval.append("      ").append(XmlHandler.addTagValue("username", userName));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "password", Encr.encryptPasswordIfNotUsingVariables(getPassword())));
    retval.append("      ").append(XmlHandler.addTagValue("ftpdirectory", remoteDirectory));
    retval.append("      ").append(XmlHandler.addTagValue("wildcard", wildcard));
    retval.append("      ").append(XmlHandler.addTagValue("timeout", timeout));
    retval.append("      ").append(XmlHandler.addTagValue("active", activeConnection));

    retval.append("      ").append(XmlHandler.addTagValue("useproxy", useProxy));
    retval.append("      ").append(XmlHandler.addTagValue("proxy_host", proxyHost));
    retval.append("      ").append(XmlHandler.addTagValue("proxy_port", proxyPort));
    retval.append("      ").append(XmlHandler.addTagValue("proxy_username", proxyUsername));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "proxy_password", Encr.encryptPasswordIfNotUsingVariables(proxyPassword)));

    retval.append("      ").append(XmlHandler.addTagValue("publicpublickey", publicPublicKey));
    retval.append("      ").append(XmlHandler.addTagValue("keyfilename", keyFilename));
    retval.append("      ").append(XmlHandler.addTagValue("keyfilepass", keyFilePass));

    retval.append("      ").append(XmlHandler.addTagValue("nr_limit_success", nrLimitSuccess));
    retval.append("      ").append(XmlHandler.addTagValue("success_condition", successCondition));
    retval.append("      ").append(XmlHandler.addTagValue("copyprevious", copyPrevious));

    retval.append("      ").append(XmlHandler.addTagValue("socksproxy_host", socksProxyHost));
    retval.append("      ").append(XmlHandler.addTagValue("socksproxy_port", socksProxyPort));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("socksproxy_username", socksProxyUsername));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "socksproxy_password",
                Encr.encryptPasswordIfNotUsingVariables(getSocksProxyPassword())));

    return retval.toString();
  }

  @Override
  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);

      protocol = XmlHandler.getTagValue(entrynode, "protocol");
      serverPort = XmlHandler.getTagValue(entrynode, "port");
      serverName = XmlHandler.getTagValue(entrynode, "servername");
      userName = XmlHandler.getTagValue(entrynode, "username");
      password =
          Encr.decryptPasswordOptionallyEncrypted(XmlHandler.getTagValue(entrynode, "password"));
      remoteDirectory = XmlHandler.getTagValue(entrynode, "ftpdirectory");
      wildcard = XmlHandler.getTagValue(entrynode, "wildcard");
      timeout = Const.toInt(XmlHandler.getTagValue(entrynode, "timeout"), 10000);
      activeConnection = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "active"));

      useProxy = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "useproxy"));
      proxyHost = XmlHandler.getTagValue(entrynode, "proxy_host");
      proxyPort = XmlHandler.getTagValue(entrynode, "proxy_port");
      proxyUsername = XmlHandler.getTagValue(entrynode, "proxy_username");
      proxyPassword =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(entrynode, "proxy_password"));

      publicPublicKey = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "publicpublickey"));
      keyFilename = XmlHandler.getTagValue(entrynode, "keyfilename");
      keyFilePass = XmlHandler.getTagValue(entrynode, "keyfilepass");

      nrLimitSuccess = XmlHandler.getTagValue(entrynode, "nr_limit_success");
      successCondition = XmlHandler.getTagValue(entrynode, "success_condition");
      copyPrevious = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "copyprevious"));

      socksProxyHost = XmlHandler.getTagValue(entrynode, "socksproxy_host");
      socksProxyPort = XmlHandler.getTagValue(entrynode, "socksproxy_port");
      socksProxyUsername = XmlHandler.getTagValue(entrynode, "socksproxy_username");
      socksProxyPassword =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(entrynode, "socksproxy_password"));

    } catch (HopXmlException xe) {
      throw new HopXmlException("Unable to load action of type 'ftp' from XML node", xe);
    }
  }

  private boolean getStatus() {
    boolean retval = false;

    if ((nrErrors == 0 && getSuccessCondition().equals(SUCCESS_IF_ALL_FILES_DOWNLOADED))
        || (nrFilesDeleted >= limitFiles
            && getSuccessCondition().equals(SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED))
        || (nrErrors <= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS))) {
      retval = true;
    }

    return retval;
  }

  public boolean isCopyPrevious() {
    return copyPrevious;
  }

  public void setCopyPrevious(boolean copyprevious) {
    this.copyPrevious = copyprevious;
  }

  /** @param publickey The publicpublickey to set. */
  public void setUsePublicKey(boolean publickey) {
    this.publicPublicKey = publickey;
  }

  /** @return Returns the use public key. */
  public boolean isUsePublicKey() {
    return publicPublicKey;
  }

  /** @param keyfilename The key filename to set. */
  public void setKeyFilename(String keyfilename) {
    this.keyFilename = keyfilename;
  }

  /** @return Returns the key filename. */
  public String getKeyFilename() {
    return keyFilename;
  }

  /** @param keyFilePass The key file pass to set. */
  public void setKeyFilePass(String keyFilePass) {
    this.keyFilePass = keyFilePass;
  }

  /** @return Returns the key file pass. */
  public String getKeyFilePass() {
    return keyFilePass;
  }

  public void setLimitSuccess(String nrLimitSuccessin) {
    this.nrLimitSuccess = nrLimitSuccessin;
  }

  public String getLimitSuccess() {
    return nrLimitSuccess;
  }

  public void setSuccessCondition(String successCondition) {
    this.successCondition = successCondition;
  }

  public String getSuccessCondition() {
    return successCondition;
  }

  /** @return Returns the directory. */
  public String getRemoteDirectory() {
    return remoteDirectory;
  }

  /** @param directory The directory to set. */
  public void setRemoteDirectory(String directory) {
    this.remoteDirectory = directory;
  }

  /** @return Returns the password. */
  public String getPassword() {
    return password;
  }

  @Override
  public boolean isBinaryMode() {
    return true;
  }

  /** @param password The password to set. */
  public void setPassword(String password) {
    this.password = password;
  }

  /** @return Returns the serverName. */
  public String getServerName() {
    return serverName;
  }

  /** @param serverName The serverName to set. */
  public void setServerName(String serverName) {
    this.serverName = serverName;
  }

  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  public String getProtocol() {
    return protocol;
  }

  /** @return Returns the userName. */
  public String getUserName() {
    return userName;
  }

  /** @param userName The userName to set. */
  public void setUserName(String userName) {
    this.userName = userName;
  }

  /** @return Returns the wildcard. */
  public String getWildcard() {
    return wildcard;
  }

  /** @param wildcard The wildcard to set. */
  public void setWildcard(String wildcard) {
    this.wildcard = wildcard;
  }

  /** @param timeout The timeout to set. */
  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  /** @return Returns the timeout. */
  public int getTimeout() {
    return timeout;
  }

  @Override
  public String getControlEncoding() {
    return null;
  }

  /** @return Returns the hostname of the ftp-proxy. */
  public String getProxyHost() {
    return proxyHost;
  }

  /** @param proxyHost The hostname of the proxy. */
  public void setProxyHost(String proxyHost) {
    this.proxyHost = proxyHost;
  }

  public boolean isUseProxy() {
    return useProxy;
  }

  public void setUseProxy(boolean useproxy) {
    this.useProxy = useproxy;
  }

  /** @return Returns the password which is used to authenticate at the proxy. */
  public String getProxyPassword() {
    return proxyPassword;
  }

  /** @param proxyPassword The password which is used to authenticate at the proxy. */
  public void setProxyPassword(String proxyPassword) {
    this.proxyPassword = proxyPassword;
  }

  /** @return Returns the port of the ftp. */
  public String getServerPort() {
    return serverPort;
  }

  /** @param serverPort The port of the ftp. */
  public void setServerPort(String serverPort) {
    this.serverPort = serverPort;
  }

  /** @return Returns the port of the ftp-proxy. */
  public String getProxyPort() {
    return proxyPort;
  }

  /** @param proxyPort The port of the ftp-proxy. */
  public void setProxyPort(String proxyPort) {
    this.proxyPort = proxyPort;
  }

  /** @return Returns the username which is used to authenticate at the proxy. */
  public String getProxyUsername() {
    return proxyUsername;
  }

  /** @param proxyUsername The username which is used to authenticate at the proxy. */
  public void setProxyUsername(String proxyUsername) {
    this.proxyUsername = proxyUsername;
  }

  /** Needed for the Vector coming from sshclient.ls() * */
  @SuppressWarnings("unchecked")
  public Result execute(Result previousResult, int nr) {
    log.logBasic(BaseMessages.getString(PKG, "ActionFTPDelete.Started", serverName));
    RowMetaAndData resultRow = null;
    Result result = previousResult;
    List<RowMetaAndData> rows = result.getRows();

    result.setResult(false);
    nrErrors = 0;
    nrFilesDeleted = 0;
    successConditionBroken = false;
    HashSet<String> listPreviousFiles = new HashSet<>();

    // Here let's put some controls before stating the workflow

    String realServerName = resolve(serverName);
    String realServerPassword = Utils.resolvePassword(this, password);
    String realFtpDirectory = resolve(remoteDirectory);

    int realServerPort = Const.toInt(resolve(serverPort), 0);
    String realUsername = resolve(userName);
    String realPassword = Utils.resolvePassword(this, password);
    String realProxyHost = resolve(proxyHost);
    String realProxyUsername = resolve(proxyUsername);
    String realProxyPassword = Utils.resolvePassword(this, proxyPassword);
    int realProxyPort = Const.toInt(resolve(proxyPort), 0);
    String realKeyFilename = resolve(keyFilename);
    String realKeyPass = resolve(keyFilePass);

    // The following is used to apply a path for SSH because the SFTPv3Client doesn't let us
    // specify/change dirs
    //
    String sourceFolder = "";

    if (isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.Start"));
    }

    if (copyPrevious && rows.size() == 0) {
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.ArgsFromPreviousNothing"));
      }
      result.setResult(true);
      return result;
    }

    try {

      // Get all the files in the current directory...
      String[] filelist = null;
      if (protocol.equals(PROTOCOL_FTP)) {

        // establish the connection
        ftpConnect(realFtpDirectory);

        filelist = ftpclient.listNames();

        // Some FTP servers return a message saying no files found as a string in the filenlist
        // e.g. Solaris 8
        // CHECK THIS !!!
        if (filelist.length == 1) {
          String translatedWildcard = resolve(wildcard);
          if (!Utils.isEmpty(translatedWildcard)) {
            if (filelist[0].startsWith(translatedWildcard)) {
              throw new HopException(filelist[0]);
            }
          }
        }
      } else if (protocol.equals(PROTOCOL_SFTP)) {
        // establish the secure connection
        sftpConnect(realServerName, realUsername, realServerPort, realPassword, realFtpDirectory);

        // Get all the files in the current directory...
        filelist = sftpclient.dir();
      }

      if (isDetailed()) {
        logDetailed("ActionFTPDelete.FoundNFiles", String.valueOf(filelist.length));
      }
      int found = filelist == null ? 0 : filelist.length;
      if (found == 0) {
        result.setResult(true);
        return result;
      }

      Pattern pattern = null;
      if (copyPrevious) {
        // Copy the input row to the (command line) arguments
        for (int iteration = 0; iteration < rows.size(); iteration++) {
          resultRow = rows.get(iteration);

          // Get file names
          String filePrevious = resultRow.getString(0, null);
          if (!Utils.isEmpty(filePrevious)) {
            listPreviousFiles.add(filePrevious);
          }
        }
      } else {
        if (!Utils.isEmpty(wildcard)) {
          String realWildcard = resolve(wildcard);
          pattern = Pattern.compile(realWildcard);
        }
      }

      if (!getSuccessCondition().equals(SUCCESS_IF_ALL_FILES_DOWNLOADED)) {
        limitFiles = Const.toInt(resolve(getLimitSuccess()), 10);
      }

      // Get the files in the list...
      for (int i = 0; i < filelist.length && !parentWorkflow.isStopped(); i++) {
        if (successConditionBroken) {
          throw new Exception(BaseMessages.getString(PKG, "ActionFTPDelete.SuccesConditionBroken"));
        }

        boolean getIt = false;

        if (isDebug()) {
          logDebug(BaseMessages.getString(PKG, "ActionFTPDelete.AnalysingFile", filelist[i]));
        }

        try {
          // First see if the file matches the regular expression!
          if (copyPrevious) {
            if (listPreviousFiles.contains(filelist[i])) {
              getIt = true;
            }
          } else {
            if (pattern != null) {
              Matcher matcher = pattern.matcher(filelist[i]);
              getIt = matcher.matches();
            }
          }

          if (getIt) {
            // Delete file
            if (protocol.equals(PROTOCOL_FTP)) {
              ftpclient.deleteFile(filelist[i]);
            } else if (protocol.equals(PROTOCOL_SFTP)) {
              sftpclient.delete(filelist[i]);
            }
            if (isDetailed()) {
              logDetailed("ActionFTPDelete.RemotefileDeleted", filelist[i]);
            }
            updateDeletedFiles();
          }
        } catch (Exception e) {
          // Update errors number
          updateErrors();
          logError(BaseMessages.getString(PKG, "ActionFtp.UnexpectedError", e.getMessage()));

          if (successConditionBroken) {
            throw new Exception(
                BaseMessages.getString(PKG, "ActionFTPDelete.SuccesConditionBroken"));
          }
        }
      } // end for
    } catch (Exception e) {
      updateErrors();
      logError(BaseMessages.getString(PKG, "ActionFTPDelete.ErrorGetting", e.getMessage()));
      logError(Const.getStackTracker(e));
    } finally {
      if (ftpclient != null && ftpclient.isConnected()) {
        try {
          ftpclient.quit();
          ftpclient = null;
        } catch (Exception e) {
          logError(BaseMessages.getString(PKG, "ActionFTPDelete.ErrorQuitting", e.getMessage()));
        }
      }
      if (sftpclient != null) {
        try {
          sftpclient.disconnect();
          sftpclient = null;
        } catch (Exception e) {
          logError(BaseMessages.getString(PKG, "ActionFTPDelete.ErrorQuitting", e.getMessage()));
        }
      }
      FtpClientUtil.clearSocksJvmSettings();
    }

    result.setResult(!successConditionBroken);
    result.setNrFilesRetrieved(nrFilesDeleted);
    result.setNrErrors(nrErrors);

    return result;
  }

  /**
   * Checks if file is a directory
   *
   * @param sftpClient
   * @param filename
   * @return true, if filename is a directory
   */
  public boolean isDirectory(SftpClient sftpClient, String filename) {
    try {
      return sftpClient.folderExists(filename);
    } catch (Exception e) {
      // Ignore FTP errors
    }
    return false;
  }

  private void sftpConnect(
      String realservername,
      String realusername,
      int realport,
      String realpassword,
      String realFtpDirectory)
      throws Exception {
    // Create sftp client to host ...
    sftpclient = new SftpClient(InetAddress.getByName(realservername), realport, realusername);

    // login to ftp host ...
    sftpclient.login(realpassword);

    // move to spool dir ...
    if (!Utils.isEmpty(realFtpDirectory)) {
      sftpclient.chdir(realFtpDirectory);
      if (isDetailed()) {
        logDetailed("Changed to directory [" + realFtpDirectory + "]");
      }
    }
  }


  private void ftpConnect(String realFtpDirectory) throws Exception {

    // Create ftp client to host:port ...
    ftpclient = FtpClientUtil.connectAndLogin(log, this, this, getName());

    // move to spool dir ...
    if (!Utils.isEmpty(realFtpDirectory)) {
      ftpclient.changeWorkingDirectory(realFtpDirectory);
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.ChangedDir", realFtpDirectory));
      }
    }
  }

  private void updateErrors() {
    nrErrors++;
    if (!getStatus()) {
      // Success condition was broken
      successConditionBroken = true;
    }
  }

  private void updateDeletedFiles() {
    nrFilesDeleted++;
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  /** @return the activeConnection */
  public boolean isActiveConnection() {
    return activeConnection;
  }

  /** @param activeConnection the activeConnection to set */
  public void setActiveConnection(boolean activeConnection) {
    this.activeConnection = activeConnection;
  }

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
            "targetDirectory",
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
            "password",
            remarks,
            AndValidator.putValidators(ActionValidatorUtils.notNullValidator()));
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (!Utils.isEmpty(serverName)) {
      String realServername = variables.resolve(serverName);
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(realServername, ResourceType.SERVER));
      references.add(reference);
    }
    return references;
  }

  /** @return Socks proxy host */
  public String getSocksProxyHost() {
    return this.socksProxyHost;
  }

  /** @return Socks proxy port */
  public String getSocksProxyPort() {
    return this.socksProxyPort;
  }

  /** @return Socks proxy username */
  public String getSocksProxyUsername() {
    return this.socksProxyUsername;
  }

  /** @return Socks proxy username */
  public String getSocksProxyPassword() {
    return this.socksProxyPassword;
  }

  /** @return Sets socks proxy host */
  public void setSocksProxyHost(String socksProxyHost) {
    this.socksProxyHost = socksProxyHost;
  }

  /** @return Sets socks proxy port */
  public void setSocksProxyPort(String socksProxyPort) {
    this.socksProxyPort = socksProxyPort;
  }

  /** @return Sets socks proxy username */
  public void setSocksProxyUsername(String socksProxyUsername) {
    this.socksProxyUsername = socksProxyUsername;
  }

  /** @return Sets socks proxy username */
  public void setSocksProxyPassword(String socksProxyPassword) {
    this.socksProxyPassword = socksProxyPassword;
  }
}
