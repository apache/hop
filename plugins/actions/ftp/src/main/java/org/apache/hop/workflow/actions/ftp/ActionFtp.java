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

package org.apache.hop.workflow.actions.ftp;

import com.enterprisedt.net.ftp.FTPClient;
import com.enterprisedt.net.ftp.FTPConnectMode;
import com.enterprisedt.net.ftp.FTPException;
import com.enterprisedt.net.ftp.FTPFile;
import com.enterprisedt.net.ftp.FTPFileFactory;
import com.enterprisedt.net.ftp.FTPFileParser;
import com.enterprisedt.net.ftp.FTPTransferType;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.StringUtil;
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
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.w3c.dom.Node;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
    id = "FTP",
    name = "i18n::ActionFTP.Name",
    description = "i18n::ActionFTP.Description",
    image = "FTP.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileTransfer",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/ftp.html")
public class ActionFtp extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionFtp.class; // Needed by Translator

  private String serverName;

  private String userName;

  private String password;

  private String ftpDirectory;

  private String targetDirectory;

  private String wildcard;

  private boolean binaryMode;

  private int timeout;

  private boolean remove;

  private boolean onlyGettingNewFiles; /* Don't overwrite files */

  private boolean activeConnection;

  private String controlEncoding; /* how to convert list of filenames e.g. */

  /** Implicit encoding used before PDI v2.4.1 */
  private static String LEGACY_CONTROL_ENCODING = "US-ASCII";

  /** Default encoding when making a new ftp action instance. */
  private static String DEFAULT_CONTROL_ENCODING = "ISO-8859-1";

  private boolean movefiles;

  private String movetodirectory;

  private boolean adddate;

  private boolean addtime;

  private boolean specifyFormat;

  private String dateTimeFormat;

  private boolean addDateBeforeExtension;

  private boolean isaddresult;

  private boolean createmovefolder;

  private String port;

  private String proxyHost;

  private String proxyPort; /* string to allow variable substitution */

  private String proxyUsername;

  private String proxyPassword;

  private String socksProxyHost;

  private String socksProxyPort;

  private String socksProxyUsername;

  private String socksProxyPassword;

  public int ifFileExistsSkip = 0;

  public String SifFileExistsSkip = "ifFileExistsSkip";

  public int ifFileExistsCreateUniq = 1;

  public String SifFileExistsCreateUniq = "ifFileExistsCreateUniq";

  public int ifFileExistsFail = 2;

  public String SifFileExistsFail = "ifFileExistsFail";

  public int ifFileExists;

  public String SifFileExists;

  public String SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED = "success_when_at_least";

  public String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";

  public String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";

  private String nrLimit;

  private String successCondition;

  long nrErrors = 0;

  long NrfilesRetrieved = 0;

  boolean successConditionBroken = false;

  int limitFiles = 0;

  String targetFilename = null;

  static String FILE_SEPARATOR = "/";

  public ActionFtp(String n) {
    super(n, "");
    nrLimit = "10";
    port = "21";
    socksProxyPort = "1080";
    successCondition = SUCCESS_IF_NO_ERRORS;
    ifFileExists = ifFileExistsSkip;
    SifFileExists = SifFileExistsSkip;

    serverName = null;
    movefiles = false;
    movetodirectory = null;
    adddate = false;
    addtime = false;
    specifyFormat = false;
    addDateBeforeExtension = false;
    isaddresult = true;
    createmovefolder = false;

    setControlEncoding(DEFAULT_CONTROL_ENCODING);
  }

  public ActionFtp() {
    this("");
  }

  public Object clone() {
    ActionFtp je = (ActionFtp) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(650); // 528 chars in spaces and tags alone

    retval.append(super.getXml());
    retval.append("      ").append(XmlHandler.addTagValue("port", port));
    retval.append("      ").append(XmlHandler.addTagValue("servername", serverName));
    retval.append("      ").append(XmlHandler.addTagValue("username", userName));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue("password", Encr.encryptPasswordIfNotUsingVariables(password)));
    retval.append("      ").append(XmlHandler.addTagValue("ftpdirectory", ftpDirectory));
    retval.append("      ").append(XmlHandler.addTagValue("targetdirectory", targetDirectory));
    retval.append("      ").append(XmlHandler.addTagValue("wildcard", wildcard));
    retval.append("      ").append(XmlHandler.addTagValue("binary", binaryMode));
    retval.append("      ").append(XmlHandler.addTagValue("timeout", timeout));
    retval.append("      ").append(XmlHandler.addTagValue("remove", remove));
    retval.append("      ").append(XmlHandler.addTagValue("only_new", onlyGettingNewFiles));
    retval.append("      ").append(XmlHandler.addTagValue("active", activeConnection));
    retval.append("      ").append(XmlHandler.addTagValue("control_encoding", controlEncoding));
    retval.append("      ").append(XmlHandler.addTagValue("movefiles", movefiles));
    retval.append("      ").append(XmlHandler.addTagValue("movetodirectory", movetodirectory));

    retval.append("      ").append(XmlHandler.addTagValue("adddate", adddate));
    retval.append("      ").append(XmlHandler.addTagValue("addtime", addtime));
    retval.append("      ").append(XmlHandler.addTagValue("SpecifyFormat", specifyFormat));
    retval.append("      ").append(XmlHandler.addTagValue("date_time_format", dateTimeFormat));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("AddDateBeforeExtension", addDateBeforeExtension));
    retval.append("      ").append(XmlHandler.addTagValue("isaddresult", isaddresult));
    retval.append("      ").append(XmlHandler.addTagValue("createmovefolder", createmovefolder));

    retval.append("      ").append(XmlHandler.addTagValue("proxy_host", proxyHost));
    retval.append("      ").append(XmlHandler.addTagValue("proxy_port", proxyPort));
    retval.append("      ").append(XmlHandler.addTagValue("proxy_username", proxyUsername));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "proxy_password", Encr.encryptPasswordIfNotUsingVariables(proxyPassword)));
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
                Encr.encryptPasswordIfNotUsingVariables(socksProxyPassword)));

    retval.append("      ").append(XmlHandler.addTagValue("ifFileExists", SifFileExists));

    retval.append("      ").append(XmlHandler.addTagValue("nr_limit", nrLimit));
    retval.append("      ").append(XmlHandler.addTagValue("success_condition", successCondition));

    return retval.toString();
  }

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);
      port = XmlHandler.getTagValue(entrynode, "port");
      serverName = XmlHandler.getTagValue(entrynode, "servername");
      userName = XmlHandler.getTagValue(entrynode, "username");
      password =
          Encr.decryptPasswordOptionallyEncrypted(XmlHandler.getTagValue(entrynode, "password"));
      ftpDirectory = XmlHandler.getTagValue(entrynode, "ftpdirectory");
      targetDirectory = XmlHandler.getTagValue(entrynode, "targetdirectory");
      wildcard = XmlHandler.getTagValue(entrynode, "wildcard");
      binaryMode = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "binary"));
      timeout = Const.toInt(XmlHandler.getTagValue(entrynode, "timeout"), 10000);
      remove = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "remove"));
      onlyGettingNewFiles = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "only_new"));
      activeConnection = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "active"));
      controlEncoding = XmlHandler.getTagValue(entrynode, "control_encoding");
      if (controlEncoding == null) {
        // if we couldn't retrieve an encoding, assume it's an old instance and
        // put in the the encoding used before v 2.4.0
        controlEncoding = LEGACY_CONTROL_ENCODING;
      }
      movefiles = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "movefiles"));
      movetodirectory = XmlHandler.getTagValue(entrynode, "movetodirectory");

      adddate = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "adddate"));
      addtime = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "addtime"));
      specifyFormat = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "SpecifyFormat"));
      dateTimeFormat = XmlHandler.getTagValue(entrynode, "date_time_format");
      addDateBeforeExtension =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "AddDateBeforeExtension"));

      String addresult = XmlHandler.getTagValue(entrynode, "isaddresult");

      if (Utils.isEmpty(addresult)) {
        isaddresult = true;
      } else {
        isaddresult = "Y".equalsIgnoreCase(addresult);
      }

      createmovefolder =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "createmovefolder"));

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
      SifFileExists = XmlHandler.getTagValue(entrynode, "ifFileExists");
      if (Utils.isEmpty(SifFileExists)) {
        ifFileExists = ifFileExistsSkip;
      } else {
        if (SifFileExists.equals(SifFileExistsCreateUniq)) {
          ifFileExists = ifFileExistsCreateUniq;
        } else if (SifFileExists.equals(SifFileExistsFail)) {
          ifFileExists = ifFileExistsFail;
        } else {
          ifFileExists = ifFileExistsSkip;
        }
      }
      nrLimit = XmlHandler.getTagValue(entrynode, "nr_limit");
      successCondition =
          Const.NVL(XmlHandler.getTagValue(entrynode, "success_condition"), SUCCESS_IF_NO_ERRORS);

    } catch (HopXmlException xe) {
      throw new HopXmlException("Unable to load action of type 'ftp' from XML node", xe);
    }
  }

  public void setLimit(String nrLimitin) {
    this.nrLimit = nrLimitin;
  }

  public String getLimit() {
    return nrLimit;
  }

  public void setSuccessCondition(String successCondition) {
    this.successCondition = successCondition;
  }

  public String getSuccessCondition() {
    return successCondition;
  }

  public void setCreateMoveFolder(boolean createmovefolderin) {
    this.createmovefolder = createmovefolderin;
  }

  public boolean isCreateMoveFolder() {
    return createmovefolder;
  }

  public void setAddDateBeforeExtension(boolean addDateBeforeExtension) {
    this.addDateBeforeExtension = addDateBeforeExtension;
  }

  public boolean isAddDateBeforeExtension() {
    return addDateBeforeExtension;
  }

  public void setAddToResult(boolean isaddresultin) {
    this.isaddresult = isaddresultin;
  }

  public boolean isAddToResult() {
    return isaddresult;
  }

  public void setDateInFilename(boolean adddate) {
    this.adddate = adddate;
  }

  public boolean isDateInFilename() {
    return adddate;
  }

  public void setTimeInFilename(boolean addtime) {
    this.addtime = addtime;
  }

  public boolean isTimeInFilename() {
    return addtime;
  }

  public boolean isSpecifyFormat() {
    return specifyFormat;
  }

  public void setSpecifyFormat(boolean specifyFormat) {
    this.specifyFormat = specifyFormat;
  }

  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  public void setDateTimeFormat(String dateTimeFormat) {
    this.dateTimeFormat = dateTimeFormat;
  }

  /** @return Returns the movefiles. */
  public boolean isMoveFiles() {
    return movefiles;
  }

  /** @param movefilesin The movefiles to set. */
  public void setMoveFiles(boolean movefilesin) {
    this.movefiles = movefilesin;
  }

  /** @return Returns the movetodirectory. */
  public String getMoveToDirectory() {
    return movetodirectory;
  }

  /** @param movetoin The movetodirectory to set. */
  public void setMoveToDirectory(String movetoin) {
    this.movetodirectory = movetoin;
  }

  /** @return Returns the binaryMode. */
  public boolean isBinaryMode() {
    return binaryMode;
  }

  /** @param binaryMode The binaryMode to set. */
  public void setBinaryMode(boolean binaryMode) {
    this.binaryMode = binaryMode;
  }

  /** @return Returns the directory. */
  public String getFtpDirectory() {
    return ftpDirectory;
  }

  /** @param directory The directory to set. */
  public void setFtpDirectory(String directory) {
    this.ftpDirectory = directory;
  }

  /** @return Returns the password. */
  public String getPassword() {
    return password;
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

  /** @return Returns the port. */
  public String getPort() {
    return port;
  }

  /** @param port The port to set. */
  public void setPort(String port) {
    this.port = port;
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

  /** @return Returns the targetDirectory. */
  public String getTargetDirectory() {
    return targetDirectory;
  }

  /** @param targetDirectory The targetDirectory to set. */
  public void setTargetDirectory(String targetDirectory) {
    this.targetDirectory = targetDirectory;
  }

  /** @param timeout The timeout to set. */
  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  /** @return Returns the timeout. */
  public int getTimeout() {
    return timeout;
  }

  /** @param remove The remove to set. */
  public void setRemove(boolean remove) {
    this.remove = remove;
  }

  /** @return Returns the remove. */
  public boolean getRemove() {
    return remove;
  }

  /** @return Returns the onlyGettingNewFiles. */
  public boolean isOnlyGettingNewFiles() {
    return onlyGettingNewFiles;
  }

  /** @param onlyGettingNewFilesin The onlyGettingNewFiles to set. */
  public void setOnlyGettingNewFiles(boolean onlyGettingNewFilesin) {
    this.onlyGettingNewFiles = onlyGettingNewFilesin;
  }

  /**
   * Get the control encoding to be used for ftp'ing
   *
   * @return the used encoding
   */
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

  /** @return Returns the hostname of the ftp-proxy. */
  public String getProxyHost() {
    return proxyHost;
  }

  /** @param proxyHost The hostname of the proxy. */
  public void setProxyHost(String proxyHost) {
    this.proxyHost = proxyHost;
  }

  /** @param proxyPassword The password which is used to authenticate at the socks proxy. */
  public void setProxyPassword(String proxyPassword) {
    this.proxyPassword = proxyPassword;
  }

  /** @return Returns the password which is used to authenticate at the proxy. */
  public String getProxyPassword() {
    return proxyPassword;
  }

  /** @param socksProxyPassword The password which is used to authenticate at the proxy. */
  public void setSocksProxyPassword(String socksProxyPassword) {
    this.socksProxyPassword = socksProxyPassword;
  }

  /** @return Returns the password which is used to authenticate at the socks proxy. */
  public String getSocksProxyPassword() {
    return socksProxyPassword;
  }

  /** @param proxyPort The port of the ftp-proxy. */
  public void setProxyPort(String proxyPort) {
    this.proxyPort = proxyPort;
  }

  /** @return Returns the port of the ftp-proxy. */
  public String getProxyPort() {
    return proxyPort;
  }

  /** @return Returns the username which is used to authenticate at the proxy. */
  public String getProxyUsername() {
    return proxyUsername;
  }

  /** @param proxyUsername The username which is used to authenticate at the proxy. */
  public void setProxyUsername(String proxyUsername) {
    this.proxyUsername = proxyUsername;
  }

  /** @return Returns the username which is used to authenticate at the socks proxy. */
  public String getSocksProxyUsername() {
    return socksProxyUsername;
  }

  /** @param socksPoxyUsername The username which is used to authenticate at the socks proxy. */
  public void setSocksProxyUsername(String socksPoxyUsername) {
    this.socksProxyUsername = socksPoxyUsername;
  }

  /** @param socksProxyHost The host name of the socks proxy host */
  public void setSocksProxyHost(String socksProxyHost) {
    this.socksProxyHost = socksProxyHost;
  }

  /** @return The host name of the socks proxy host */
  public String getSocksProxyHost() {
    return this.socksProxyHost;
  }

  /** @param socksProxyPort The port number the socks proxy host is using */
  public void setSocksProxyPort(String socksProxyPort) {
    this.socksProxyPort = socksProxyPort;
  }

  /** @return The port number the socks proxy host is using */
  public String getSocksProxyPort() {
    return this.socksProxyPort;
  }

  protected FTPClient initFtpClient() {
    return new FTPClient();
  }

  protected InetAddress getInetAddress(String realServername) throws UnknownHostException {
    return InetAddress.getByName(realServername);
  }

  public Result execute(Result previousResult, int nr) {
    log.logBasic(BaseMessages.getString(PKG, "ActionFTP.Started", serverName));

    Result result = previousResult;
    result.setNrErrors(1);
    result.setResult(false);
    nrErrors = 0;
    NrfilesRetrieved = 0;
    successConditionBroken = false;
    boolean exitaction = false;
    limitFiles = Const.toInt(environmentSubstitute(getLimit()), 10);

    // Here let's put some controls before stating the workflow
    if (movefiles) {
      if (Utils.isEmpty(movetodirectory)) {
        logError(BaseMessages.getString(PKG, "ActionFTP.MoveToFolderEmpty"));
        return result;
      }
    }

    if (isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "ActionFTP.Start"));
    }

    FTPClient ftpclient = null;
    String realMoveToFolder = null;

    try {
      // Create ftp client to host:port ...
      ftpclient = initFtpClient();
      String realServername = environmentSubstitute(serverName);
      String realServerPort = environmentSubstitute(port);
      ftpclient.setRemoteAddr(getInetAddress(realServername));
      if (!Utils.isEmpty(realServerPort)) {
        ftpclient.setRemotePort(Const.toInt(realServerPort, 21));
      }

      if (!Utils.isEmpty(proxyHost)) {
        String realProxyHost = environmentSubstitute(proxyHost);
        ftpclient.setRemoteAddr(InetAddress.getByName(realProxyHost));
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "ActionFTP.OpenedProxyConnectionOn", realProxyHost));
        }

        // FIXME: Proper default port for proxy
        int port = Const.toInt(environmentSubstitute(proxyPort), 21);
        if (port != 0) {
          ftpclient.setRemotePort(port);
        }
      } else {
        ftpclient.setRemoteAddr(getInetAddress(realServername));

        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionFTP.OpenedConnectionTo", realServername));
        }
      }

      // set activeConnection connectmode ...
      if (activeConnection) {
        ftpclient.setConnectMode(FTPConnectMode.ACTIVE);
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionFTP.SetActive"));
        }
      } else {
        ftpclient.setConnectMode(FTPConnectMode.PASV);
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionFTP.SetPassive"));
        }
      }

      // Set the timeout
      ftpclient.setTimeout(timeout);
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionFTP.SetTimeout", String.valueOf(timeout)));
      }

      ftpclient.setControlEncoding(controlEncoding);
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionFTP.SetEncoding", controlEncoding));
      }

      // If socks proxy server was provided
      if (!Utils.isEmpty(socksProxyHost)) {
        if (!Utils.isEmpty(socksProxyPort)) {
          FTPClient.initSOCKS(
              environmentSubstitute(socksProxyPort), environmentSubstitute(socksProxyHost));
        } else {
          throw new FTPException(
              BaseMessages.getString(
                  PKG,
                  "ActionFTP.SocksProxy.PortMissingException",
                  environmentSubstitute(socksProxyHost),
                  getName()));
        }
        // then if we have authentication information
        if (!Utils.isEmpty(socksProxyUsername) && !Utils.isEmpty(socksProxyPassword)) {
          FTPClient.initSOCKSAuthentication(
              environmentSubstitute(socksProxyUsername),
              Utils.resolvePassword(this, socksProxyPassword));
        } else if (!Utils.isEmpty(socksProxyUsername) && Utils.isEmpty(socksProxyPassword)
            || Utils.isEmpty(socksProxyUsername) && !Utils.isEmpty(socksProxyPassword)) {
          // we have a username without a password or vica versa
          throw new FTPException(
              BaseMessages.getString(
                  PKG,
                  "ActionFTP.SocksProxy.IncompleteCredentials",
                  environmentSubstitute(socksProxyHost),
                  getName()));
        }
      }

      // login to ftp host ...
      ftpclient.connect();

      String realUsername =
          environmentSubstitute(userName)
              + (!Utils.isEmpty(proxyHost) ? "@" + realServername : "")
              + (!Utils.isEmpty(proxyUsername) ? " " + environmentSubstitute(proxyUsername) : "");

      String realPassword =
          Utils.resolvePassword(this, password)
              + (!Utils.isEmpty(proxyPassword)
                  ? " " + Utils.resolvePassword(this, proxyPassword)
                  : "");

      ftpclient.login(realUsername, realPassword);
      // Remove password from logging, you don't know where it ends up.
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionFTP.LoggedIn", realUsername));
      }

      // Fix for PDI-2534 - add auxilliary FTP File List parsers to the ftpclient object.
      this.hookInOtherParsers(ftpclient);

      // move to spool dir ...
      if (!Utils.isEmpty(ftpDirectory)) {
        String realFtpDirectory = environmentSubstitute(ftpDirectory);
        realFtpDirectory = normalizePath(realFtpDirectory);
        ftpclient.chdir(realFtpDirectory);
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionFTP.ChangedDir", realFtpDirectory));
        }
      }

      // Create move to folder if necessary
      if (movefiles && !Utils.isEmpty(movetodirectory)) {
        realMoveToFolder = environmentSubstitute(movetodirectory);
        realMoveToFolder = normalizePath(realMoveToFolder);
        // Folder exists?
        boolean folderExist = true;
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionFTP.CheckMoveToFolder", realMoveToFolder));
        }
        String originalLocation = ftpclient.pwd();
        try {
          // does not work for folders, see PDI-2567:
          // folderExist=ftpclient.exists(realMoveToFolder);
          // try switching to the 'move to' folder.
          ftpclient.chdir(realMoveToFolder);
          // Switch back to the previous location.
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionFTP.CheckMoveToFolderSwitchBack", originalLocation));
          }
          ftpclient.chdir(originalLocation);
        } catch (Exception e) {
          folderExist = false;
          // Assume folder does not exist !!
        }

        if (!folderExist) {
          if (createmovefolder) {
            ftpclient.mkdir(realMoveToFolder);
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(PKG, "ActionFTP.MoveToFolderCreated", realMoveToFolder));
            }
          } else {
            logError(BaseMessages.getString(PKG, "ActionFTP.MoveToFolderNotExist"));
            exitaction = true;
            nrErrors++;
          }
        }
      }

      if (!exitaction) {
        // Get all the files in the current directory...
        FTPFile[] ftpFiles = ftpclient.dirDetails(null);

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionFTP.FoundNFiles", String.valueOf(ftpFiles.length)));
        }

        // set transfertype ...
        if (binaryMode) {
          ftpclient.setType(FTPTransferType.BINARY);
          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "ActionFTP.SetBinary"));
          }
        } else {
          ftpclient.setType(FTPTransferType.ASCII);
          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "ActionFTP.SetAscii"));
          }
        }

        // Some FTP servers return a message saying no files found as a string in the filenlist
        // e.g. Solaris 8
        // CHECK THIS !!!

        if (ftpFiles.length == 1) {
          String translatedWildcard = environmentSubstitute(wildcard);
          if (!Utils.isEmpty(translatedWildcard)) {
            if (ftpFiles[0].getName().startsWith(translatedWildcard)) {
              throw new FTPException(ftpFiles[0].getName());
            }
          }
        }

        Pattern pattern = null;
        if (!Utils.isEmpty(wildcard)) {
          String realWildcard = environmentSubstitute(wildcard);
          pattern = Pattern.compile(realWildcard);
        }

        if (!getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS)) {
          limitFiles = Const.toInt(environmentSubstitute(getLimit()), 10);
        }

        // Get the files in the list...
        for (FTPFile ftpFile : ftpFiles) {

          if (parentWorkflow.isStopped()) {
            exitaction = true;
            throw new Exception(BaseMessages.getString(PKG, "ActionFTP.JobStopped"));
          }

          if (successConditionBroken) {
            throw new Exception(
                BaseMessages.getString(PKG, "ActionFTP.SuccesConditionBroken", "" + nrErrors));
          }

          boolean getIt = true;

          String filename = ftpFile.getName();
          if (isDebug()) {
            logDebug(BaseMessages.getString(PKG, "ActionFTP.AnalysingFile", filename));
          }

          // We get only files
          if (ftpFile.isDir()) {
            // not a file..so let's skip it!
            getIt = false;
            if (isDebug()) {
              logDebug(BaseMessages.getString(PKG, "ActionFTP.SkippingNotAFile", filename));
            }
          }
          if (getIt) {
            try {
              // See if the file matches the regular expression!
              if (pattern != null) {
                Matcher matcher = pattern.matcher(filename);
                getIt = matcher.matches();
              }
              if (getIt) {
                downloadFile(ftpclient, filename, realMoveToFolder, parentWorkflow, result);
              }
            } catch (Exception e) {
              // Update errors number
              updateErrors();
              logError(BaseMessages.getString(PKG, "JobFTP.UnexpectedError", e.toString()));
            }
          }
        } // end for
      }
    } catch (Exception e) {
      if (!successConditionBroken && !exitaction) {
        updateErrors();
      }
      logError(BaseMessages.getString(PKG, "ActionFTP.ErrorGetting", e.getMessage()));
    } finally {
      if (ftpclient != null) {
        try {
          ftpclient.quit();
        } catch (Exception e) {
          logError(BaseMessages.getString(PKG, "ActionFTP.ErrorQuitting", e.getMessage()));
        }
      }
      FTPClient.clearSOCKS();
    }

    result.setNrErrors(nrErrors);
    result.setNrFilesRetrieved(NrfilesRetrieved);
    if (getSuccessStatus()) {
      result.setResult(true);
    }
    if (exitaction) {
      result.setResult(false);
    }
    displayResults();
    return result;
  }

  private void downloadFile(
      FTPClient ftpclient,
      String filename,
      String realMoveToFolder,
      IWorkflowEngine<WorkflowMeta> parentWorkflow,
      Result result)
      throws Exception {
    String localFilename = filename;
    targetFilename = HopVfs.getFilename(HopVfs.getFileObject(returnTargetFilename(localFilename)));

    if ((!onlyGettingNewFiles) || (onlyGettingNewFiles && needsDownload(targetFilename))) {
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG, "ActionFTP.GettingFile", filename, environmentSubstitute(targetDirectory)));
      }
      ftpclient.get(targetFilename, filename);

      // Update retrieved files
      updateRetrievedFiles();
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionFTP.GotFile", filename));
      }

      // Add filename to result filenames
      addFilenameToResultFilenames(result, parentWorkflow, targetFilename);

      // Delete the file if this is needed!
      if (remove) {
        ftpclient.delete(filename);
        if (isDetailed()) {
          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "ActionFTP.DeletedFile", filename));
          }
        }
      } else {
        if (movefiles) {
          // Try to move file to destination folder ...
          ftpclient.rename(filename, realMoveToFolder + FILE_SEPARATOR + filename);

          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(PKG, "ActionFTP.MovedFile", filename, realMoveToFolder));
          }
        }
      }
    }
  }

  /**
   * normalize / to \ and remove trailing slashes from a path
   *
   * @param path
   * @return normalized path
   * @throws Exception
   */
  public String normalizePath(String path) throws Exception {

    String normalizedPath = path.replaceAll("\\\\", FILE_SEPARATOR);
    while (normalizedPath.endsWith("\\") || normalizedPath.endsWith(FILE_SEPARATOR)) {
      normalizedPath = normalizedPath.substring(0, normalizedPath.length() - 1);
    }

    return normalizedPath;
  }

  private void addFilenameToResultFilenames(
      Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow, String filename)
      throws HopException {
    if (isaddresult) {
      FileObject targetFile = null;
      try {
        targetFile = HopVfs.getFileObject(filename);

        // Add to the result files...
        ResultFile resultFile =
            new ResultFile(
                ResultFile.FILE_TYPE_GENERAL,
                targetFile,
                parentWorkflow.getWorkflowName(),
                toString());
        resultFile.setComment(BaseMessages.getString(PKG, "ActionFTP.Downloaded", serverName));
        result.getResultFiles().put(resultFile.getFile().toString(), resultFile);

        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionFTP.FileAddedToResult", filename));
        }
      } catch (Exception e) {
        throw new HopException(e);
      } finally {
        try {
          targetFile.close();
          targetFile = null;
        } catch (Exception e) {
          // Ignore close errors
        }
      }
    }
  }

  private void displayResults() {
    if (isDetailed()) {
      logDetailed("=======================================");
      logDetailed(BaseMessages.getString(PKG, "ActionFTP.Log.Info.FilesInError", "" + nrErrors));
      logDetailed(
          BaseMessages.getString(PKG, "ActionFTP.Log.Info.FilesRetrieved", "" + NrfilesRetrieved));
      logDetailed("=======================================");
    }
  }

  private boolean getSuccessStatus() {
    boolean retval = false;

    if ((nrErrors == 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (NrfilesRetrieved >= limitFiles
            && getSuccessCondition().equals(SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED))
        || (nrErrors <= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS))) {
      retval = true;
    }

    return retval;
  }

  private void updateErrors() {
    nrErrors++;
    if (checkIfSuccessConditionBroken()) {
      // Success condition was broken
      successConditionBroken = true;
    }
  }

  private boolean checkIfSuccessConditionBroken() {
    boolean retval = false;
    if ((nrErrors > 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrErrors >= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS))) {
      retval = true;
    }
    return retval;
  }

  private void updateRetrievedFiles() {
    NrfilesRetrieved++;
  }

  /**
   * @param filename the filename from the FTP server
   * @return the calculated target filename
   */
  @VisibleForTesting
  String returnTargetFilename(String filename) {
    String retval = null;
    // Replace possible environment variables...
    if (filename != null) {
      retval = filename;
    } else {
      return null;
    }

    int lenstring = retval.length();
    int lastindexOfDot = retval.lastIndexOf(".");
    if (lastindexOfDot == -1) {
      lastindexOfDot = lenstring;
    }

    String fileExtension = retval.substring(lastindexOfDot, lenstring);

    if (isAddDateBeforeExtension()) {
      retval = retval.substring(0, lastindexOfDot);
    }

    SimpleDateFormat daf = new SimpleDateFormat();
    Date now = new Date();

    if (specifyFormat && !Utils.isEmpty(dateTimeFormat)) {
      daf.applyPattern(dateTimeFormat);
      String dt = daf.format(now);
      retval += dt;
    } else {
      if (adddate) {
        daf.applyPattern("yyyyMMdd");
        String d = daf.format(now);
        retval += "_" + d;
      }
      if (addtime) {
        daf.applyPattern("HHmmssSSS");
        String t = daf.format(now);
        retval += "_" + t;
      }
    }

    if (isAddDateBeforeExtension()) {
      retval += fileExtension;
    }

    // Add foldername to filename
    retval = environmentSubstitute(targetDirectory) + Const.FILE_SEPARATOR + retval;
    return retval;
  }

  public boolean evaluates() {
    return true;
  }

  /**
   * See if the filename on the FTP server needs downloading. The default is to check the presence
   * of the file in the target directory. If you need other functionality, extend this class and
   * build it into a plugin.
   *
   * @param filename The local filename to check
   * @return true if the file needs downloading
   */
  protected boolean needsDownload(String filename) {
    boolean retval = false;

    File file = new File(filename);

    if (!file.exists()) {
      // Local file not exists!
      if (isDebug()) {
        logDebug(BaseMessages.getString(PKG, "ActionFTP.LocalFileNotExists"), filename);
      }
      return true;
    } else {

      // Local file exists!
      if (ifFileExists == ifFileExistsCreateUniq) {
        if (isDebug()) {
          logDebug(toString(), BaseMessages.getString(PKG, "ActionFTP.LocalFileExists"), filename);
          // Create file with unique name
        }

        int lenstring = targetFilename.length();
        int lastindexOfDot = targetFilename.lastIndexOf('.');
        if (lastindexOfDot == -1) {
          lastindexOfDot = lenstring;
        }

        targetFilename =
            targetFilename.substring(0, lastindexOfDot)
                + StringUtil.getFormattedDateTimeNow(true)
                + targetFilename.substring(lastindexOfDot, lenstring);

        return true;
      } else if (ifFileExists == ifFileExistsFail) {
        log.logError(BaseMessages.getString(PKG, "ActionFTP.LocalFileExists"), filename);
        updateErrors();
      } else {
        if (isDebug()) {
          logDebug(toString(), BaseMessages.getString(PKG, "ActionFTP.LocalFileExists"), filename);
        }
      }
    }

    return retval;
  }

  /** @return the activeConnection */
  public boolean isActiveConnection() {
    return activeConnection;
  }

  /** @param activeConnection Sets the active connection to passive or not */
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

  public List<ResourceReference> getResourceDependencies(WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(workflowMeta);
    if (!Utils.isEmpty(serverName)) {
      String realServername = environmentSubstitute(serverName);
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(realServername, ResourceType.SERVER));
      references.add(reference);
    }
    return references;
  }

  /**
   * Hook in known parsers, and then those that have been specified in the variable
   * ftp.file.parser.class.names
   *
   * @param ftpClient
   * @throws FTPException
   * @throws IOException
   */
  protected void hookInOtherParsers(FTPClient ftpClient) throws FTPException, IOException {
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "ActionFTP.DEBUG.Hooking.Parsers"));
    }
    String system = ftpClient.system();
    MVSFileParser parser = new MVSFileParser(log);
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "ActionFTP.DEBUG.Created.MVS.Parser"));
    }
    FTPFileFactory factory = new FTPFileFactory(system);
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "ActionFTP.DEBUG.Created.Factory"));
    }
    factory.addParser(parser);
    ftpClient.setFTPFileFactory(factory);
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "ActionFTP.DEBUG.Get.Variable.Space"));
    }
    IVariables vs = this.getVariables();
    if (vs != null) {
      if (log.isDebug()) {
        logDebug(BaseMessages.getString(PKG, "ActionFTP.DEBUG.Getting.Other.Parsers"));
      }
      String otherParserNames = vs.getVariable("ftp.file.parser.class.names");
      if (otherParserNames != null) {
        if (log.isDebug()) {
          logDebug(BaseMessages.getString(PKG, "ActionFTP.DEBUG.Creating.Parsers"));
        }
        String[] parserClasses = otherParserNames.split("|");
        String cName = null;
        Class<?> clazz = null;
        Object parserInstance = null;
        for (int i = 0; i < parserClasses.length; i++) {
          cName = parserClasses[i].trim();
          if (cName.length() > 0) {
            try {
              clazz = Class.forName(cName);
              parserInstance = clazz.newInstance();
              if (parserInstance instanceof FTPFileParser) {
                if (log.isDetailed()) {
                  logDetailed(
                      BaseMessages.getString(PKG, "ActionFTP.DEBUG.Created.Other.Parser", cName));
                }
                factory.addParser((FTPFileParser) parserInstance);
              }
            } catch (Exception ignored) {
              if (log.isDebug()) {
                ignored.printStackTrace();
                logError(BaseMessages.getString(PKG, "ActionFTP.ERROR.Creating.Parser", cName));
              }
            }
          }
        }
      }
    }
  }
}
