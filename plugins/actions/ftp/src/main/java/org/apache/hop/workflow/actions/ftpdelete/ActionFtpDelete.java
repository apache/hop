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

import com.enterprisedt.net.ftp.FTPClient;
import com.enterprisedt.net.ftp.FTPConnectMode;
import com.enterprisedt.net.ftp.FTPException;
import com.trilead.ssh2.Connection;
import com.trilead.ssh2.HTTPProxyData;
import com.trilead.ssh2.SFTPv3Client;
import com.trilead.ssh2.SFTPv3DirectoryEntry;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
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
import org.apache.hop.workflow.actions.ftpsget.FtpsConnection;
import org.apache.hop.workflow.actions.sftp.SftpClient;
import org.w3c.dom.Node;

import java.io.File;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
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
public class ActionFtpDelete extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionFtpDelete.class; // For Translator

  private String serverName;

  private String port;

  private String userName;

  private String password;

  private String ftpDirectory;

  private String wildcard;

  private int timeout;

  private boolean activeConnection;

  private boolean publicpublickey;

  private String keyFilename;

  private String keyFilePass;

  private boolean useproxy;

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

  public static final String PROTOCOL_FTPS = "FTPS";

  public static final String PROTOCOL_SFTP = "SFTP";

  public static final String PROTOCOL_SSH = "SSH";

  public String SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED = "success_when_at_least";

  public String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";

  public String SUCCESS_IF_ALL_FILES_DOWNLOADED = "success_is_all_files_downloaded";

  private String nrLimitSuccess;

  private String successCondition;

  private boolean copyprevious;

  private int FtpsConnectionType;

  long nrErrors = 0;

  long NrfilesDeleted = 0;

  boolean successConditionBroken = false;

  String targetFilename = null;

  int limitFiles = 0;

  FTPClient ftpclient = null;

  FtpsConnection ftpsclient = null;

  SftpClient sftpclient = null;

  SFTPv3Client sshclient = null;

  public ActionFtpDelete(String n) {
    super(n, "");
    copyprevious = false;
    protocol = PROTOCOL_FTP;
    port = "21";
    socksProxyPort = "1080";
    nrLimitSuccess = "10";
    successCondition = SUCCESS_IF_ALL_FILES_DOWNLOADED;
    publicpublickey = false;
    keyFilename = null;
    keyFilePass = null;
    serverName = null;
    FtpsConnectionType = FtpsConnection.CONNECTION_TYPE_FTP;
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
    retval.append("      ").append(XmlHandler.addTagValue("port", port));
    retval.append("      ").append(XmlHandler.addTagValue("username", userName));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "password", Encr.encryptPasswordIfNotUsingVariables(getPassword())));
    retval.append("      ").append(XmlHandler.addTagValue("ftpdirectory", ftpDirectory));
    retval.append("      ").append(XmlHandler.addTagValue("wildcard", wildcard));
    retval.append("      ").append(XmlHandler.addTagValue("timeout", timeout));
    retval.append("      ").append(XmlHandler.addTagValue("active", activeConnection));

    retval.append("      ").append(XmlHandler.addTagValue("useproxy", useproxy));
    retval.append("      ").append(XmlHandler.addTagValue("proxy_host", proxyHost));
    retval.append("      ").append(XmlHandler.addTagValue("proxy_port", proxyPort));
    retval.append("      ").append(XmlHandler.addTagValue("proxy_username", proxyUsername));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "proxy_password", Encr.encryptPasswordIfNotUsingVariables(proxyPassword)));

    retval.append("      ").append(XmlHandler.addTagValue("publicpublickey", publicpublickey));
    retval.append("      ").append(XmlHandler.addTagValue("keyfilename", keyFilename));
    retval.append("      ").append(XmlHandler.addTagValue("keyfilepass", keyFilePass));

    retval.append("      ").append(XmlHandler.addTagValue("nr_limit_success", nrLimitSuccess));
    retval.append("      ").append(XmlHandler.addTagValue("success_condition", successCondition));
    retval.append("      ").append(XmlHandler.addTagValue("copyprevious", copyprevious));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "ftps_connection_type", FtpsConnection.getConnectionTypeCode(FtpsConnectionType)));

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

  public void loadXml(Node entrynode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    try {
      super.loadXml(entrynode);

      protocol = XmlHandler.getTagValue(entrynode, "protocol");
      port = XmlHandler.getTagValue(entrynode, "port");
      serverName = XmlHandler.getTagValue(entrynode, "servername");
      userName = XmlHandler.getTagValue(entrynode, "username");
      password =
          Encr.decryptPasswordOptionallyEncrypted(XmlHandler.getTagValue(entrynode, "password"));
      ftpDirectory = XmlHandler.getTagValue(entrynode, "ftpdirectory");
      wildcard = XmlHandler.getTagValue(entrynode, "wildcard");
      timeout = Const.toInt(XmlHandler.getTagValue(entrynode, "timeout"), 10000);
      activeConnection = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "active"));

      useproxy = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "useproxy"));
      proxyHost = XmlHandler.getTagValue(entrynode, "proxy_host");
      proxyPort = XmlHandler.getTagValue(entrynode, "proxy_port");
      proxyUsername = XmlHandler.getTagValue(entrynode, "proxy_username");
      proxyPassword =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(entrynode, "proxy_password"));

      publicpublickey = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "publicpublickey"));
      keyFilename = XmlHandler.getTagValue(entrynode, "keyfilename");
      keyFilePass = XmlHandler.getTagValue(entrynode, "keyfilepass");

      nrLimitSuccess = XmlHandler.getTagValue(entrynode, "nr_limit_success");
      successCondition = XmlHandler.getTagValue(entrynode, "success_condition");
      copyprevious = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "copyprevious"));
      FtpsConnectionType =
          FtpsConnection.getConnectionTypeByCode(
              Const.NVL(XmlHandler.getTagValue(entrynode, "ftps_connection_type"), ""));
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
        || (NrfilesDeleted >= limitFiles
            && getSuccessCondition().equals(SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED))
        || (nrErrors <= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS))) {
      retval = true;
    }

    return retval;
  }

  public boolean isCopyPrevious() {
    return copyprevious;
  }

  public void setCopyPrevious(boolean copyprevious) {
    this.copyprevious = copyprevious;
  }

  /** @param publickey The publicpublickey to set. */
  public void setUsePublicKey(boolean publickey) {
    this.publicpublickey = publickey;
  }

  /** @return Returns the use public key. */
  public boolean isUsePublicKey() {
    return publicpublickey;
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

  /** @return the connection type */
  public int getFtpsConnectionType() {
    return FtpsConnectionType;
  }

  /** @param type the connectionType to set */
  public void setFtpsConnectionType(int type) {
    FtpsConnectionType = type;
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

  /** @return Returns the hostname of the ftp-proxy. */
  public String getProxyHost() {
    return proxyHost;
  }

  /** @param proxyHost The hostname of the proxy. */
  public void setProxyHost(String proxyHost) {
    this.proxyHost = proxyHost;
  }

  public boolean isUseProxy() {
    return useproxy;
  }

  public void setUseProxy(boolean useproxy) {
    this.useproxy = useproxy;
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
  public String getPort() {
    return port;
  }

  /** @param port The port of the ftp. */
  public void setPort(String port) {
    this.port = port;
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
    NrfilesDeleted = 0;
    successConditionBroken = false;
    HashSet<String> listPreviousFiles = new HashSet<>();

    // Here let's put some controls before stating the workflow

    String realservername = environmentSubstitute(serverName);
    String realserverpassword = Utils.resolvePassword(this, password);
    String realFtpDirectory = environmentSubstitute(ftpDirectory);

    int realserverport = Const.toInt(environmentSubstitute(port), 0);
    String realUsername = environmentSubstitute(userName);
    String realPassword = Utils.resolvePassword(this, password);
    String realproxyhost = environmentSubstitute(proxyHost);
    String realproxyusername = environmentSubstitute(proxyUsername);
    String realproxypassword = Utils.resolvePassword(this, proxyPassword);
    int realproxyport = Const.toInt(environmentSubstitute(proxyPort), 0);
    String realkeyFilename = environmentSubstitute(keyFilename);
    String realkeyPass = environmentSubstitute(keyFilePass);

    // PDI The following is used to apply a path for SSH because the SFTPv3Client doesn't let us
    // specify/change dirs
    String sourceFolder = "";

    if (isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.Start"));
    }

    if (copyprevious && rows.size() == 0) {
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
        // If socks proxy server was provided
        if (!Utils.isEmpty(socksProxyHost)) {
          if (!Utils.isEmpty(socksProxyPort)) {
            FTPClient.initSOCKS(
                environmentSubstitute(socksProxyPort), environmentSubstitute(socksProxyHost));
          } else {
            throw new FTPException(
                BaseMessages.getString(
                    PKG,
                    "ActionFTPDelete.SocksProxy.PortMissingException",
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
                    "ActionFTPDelete.SocksProxy.IncompleteCredentials",
                    environmentSubstitute(socksProxyHost),
                    getName()));
          }
        }

        // establish the connection
        FtpConnect(
            realservername,
            realUsername,
            realPassword,
            realserverport,
            realFtpDirectory,
            realproxyhost,
            realproxyusername,
            realproxypassword,
            realproxyport,
            timeout);

        filelist = ftpclient.dir();

        // Some FTP servers return a message saying no files found as a string in the filenlist
        // e.g. Solaris 8
        // CHECK THIS !!!
        if (filelist.length == 1) {
          String translatedWildcard = environmentSubstitute(wildcard);
          if (!Utils.isEmpty(translatedWildcard)) {
            if (filelist[0].startsWith(translatedWildcard)) {
              throw new FTPException(filelist[0]);
            }
          }
        }
      } else if (protocol.equals(PROTOCOL_FTPS)) {
        // establish the secure connection
        FtpsConnect(
            realservername, realUsername, realserverport, realPassword, realFtpDirectory, timeout);
        // Get all the files in the current directory...
        filelist = ftpsclient.getFileNames();
      } else if (protocol.equals(PROTOCOL_SFTP)) {
        // establish the secure connection
        SFTPConnect(realservername, realUsername, realserverport, realPassword, realFtpDirectory);

        // Get all the files in the current directory...
        filelist = sftpclient.dir();
      } else if (protocol.equals(PROTOCOL_SSH)) {
        // establish the secure connection
        SSHConnect(
            realservername,
            realserverpassword,
            realserverport,
            realUsername,
            realPassword,
            realproxyhost,
            realproxyusername,
            realproxypassword,
            realproxyport,
            realkeyFilename,
            realkeyPass);

        sourceFolder = ".";
        if (realFtpDirectory != null) {
          sourceFolder = realFtpDirectory + "/";
        } else {
          sourceFolder = "./";
        }

        // NOTE: Source of the unchecked warning suppression for the declaration of this method.
        Vector<SFTPv3DirectoryEntry> vfilelist = sshclient.ls(sourceFolder);
        if (vfilelist != null) {
          // Make one pass through the vfilelist to get an accurate count
          // Using the two-pass method with arrays is faster than using ArrayList
          int fileCount = 0;
          Iterator<SFTPv3DirectoryEntry> iterator = vfilelist.iterator();
          while (iterator.hasNext()) {
            SFTPv3DirectoryEntry dirEntry = iterator.next();

            if (dirEntry != null
                && !dirEntry.filename.equals(".")
                && !dirEntry.filename.equals("src/test")
                && !isDirectory(sshclient, sourceFolder + dirEntry.filename)) {
              fileCount++;
            }
          }

          // Now that we have the correct count, create and fill in the array
          filelist = new String[fileCount];
          iterator = vfilelist.iterator();
          int i = 0;
          while (iterator.hasNext()) {
            SFTPv3DirectoryEntry dirEntry = iterator.next();

            if (dirEntry != null
                && !dirEntry.filename.equals(".")
                && !dirEntry.filename.equals("src/test")
                && !isDirectory(sshclient, sourceFolder + dirEntry.filename)) {
              filelist[i] = dirEntry.filename;
              i++;
            }
          }
        }
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
      if (copyprevious) {
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
          String realWildcard = environmentSubstitute(wildcard);
          pattern = Pattern.compile(realWildcard);
        }
      }

      if (!getSuccessCondition().equals(SUCCESS_IF_ALL_FILES_DOWNLOADED)) {
        limitFiles = Const.toInt(environmentSubstitute(getLimitSuccess()), 10);
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
          if (copyprevious) {
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
              ftpclient.delete(filelist[i]);
            }
            if (protocol.equals(PROTOCOL_FTPS)) {
              ftpsclient.deleteFile(filelist[i]);
            } else if (protocol.equals(PROTOCOL_SFTP)) {
              sftpclient.delete(filelist[i]);
            } else if (protocol.equals(PROTOCOL_SSH)) {
              sshclient.rm(sourceFolder + filelist[i]);
            }
            if (isDetailed()) {
              logDetailed("ActionFTPDelete.RemotefileDeleted", filelist[i]);
            }
            updateDeletedFiles();
          }
        } catch (Exception e) {
          // Update errors number
          updateErrors();
          logError(BaseMessages.getString(PKG, "JobFTP.UnexpectedError", e.getMessage()));

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
      if (ftpclient != null && ftpclient.connected()) {
        try {
          ftpclient.quit();
          ftpclient = null;
        } catch (Exception e) {
          logError(BaseMessages.getString(PKG, "ActionFTPDelete.ErrorQuitting", e.getMessage()));
        }
      }
      if (ftpsclient != null) {
        try {
          ftpsclient.disconnect();
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
      if (sshclient != null) {
        try {
          sshclient.close();
          sshclient = null;
        } catch (Exception e) {
          logError(BaseMessages.getString(PKG, "ActionFTPDelete.ErrorQuitting", e.getMessage()));
        }
      }

      FTPClient.clearSOCKS();
    }

    result.setResult(!successConditionBroken);
    result.setNrFilesRetrieved(NrfilesDeleted);
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
  public boolean isDirectory(SFTPv3Client sftpClient, String filename) {
    try {
      return sftpClient.stat(filename).isDirectory();
    } catch (Exception e) {
      // Ignore FTP errors
    }
    return false;
  }

  private void SSHConnect(
      String realservername,
      String realserverpassword,
      int realserverport,
      String realUsername,
      String realPassword,
      String realproxyhost,
      String realproxyusername,
      String realproxypassword,
      int realproxyport,
      String realkeyFilename,
      String realkeyPass)
      throws Exception {

    /* Create a connection instance */

    Connection conn = new Connection(realservername, realserverport);

    /* We want to connect through a HTTP proxy */
    if (useproxy) {
      conn.setProxyData(new HTTPProxyData(realproxyhost, realproxyport));

      /* Now connect */
      // if the proxy requires basic authentication:
      if (!Utils.isEmpty(realproxyusername) || !Utils.isEmpty(realproxypassword)) {
        conn.setProxyData(
            new HTTPProxyData(realproxyhost, realproxyport, realproxyusername, realproxypassword));
      }
    }

    if (timeout > 0) {
      // Use timeout
      conn.connect(null, 0, timeout * 1000);

    } else {
      // Cache Host Key
      conn.connect();
    }

    // Authenticate

    boolean isAuthenticated = false;
    if (publicpublickey) {
      isAuthenticated =
          conn.authenticateWithPublicKey(realUsername, new File(realkeyFilename), realkeyPass);
    } else {
      isAuthenticated = conn.authenticateWithPassword(realUsername, realserverpassword);
    }

    if (!isAuthenticated) {
      throw new Exception("Can not connect to ");
    }

    sshclient = new SFTPv3Client(conn);
  }

  private void SFTPConnect(
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

  private void FtpsConnect(
      String realservername,
      String realusername,
      int realport,
      String realpassword,
      String realFtpDirectory,
      int realtimeout)
      throws Exception {
    // Create ftps client to host ...
    ftpsclient =
        new FtpsConnection(
            getFtpsConnectionType(), realservername, realport, realusername, realpassword);

    if (!Utils.isEmpty(proxyHost)) {
      String realProxyHost = environmentSubstitute(proxyHost);
      String realProxyUsername = environmentSubstitute(proxyUsername);
      String realProxyPassword = Utils.resolvePassword(this, proxyPassword);

      ftpsclient.setProxyHost(realProxyHost);
      if (!Utils.isEmpty(realProxyUsername)) {
        ftpsclient.setProxyUser(realProxyUsername);
      }
      if (!Utils.isEmpty(realProxyPassword)) {
        ftpsclient.setProxyPassword(realProxyPassword);
      }
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "ActionFTPDelete.OpenedProxyConnectionOn", realProxyHost));
      }

      int proxyport = Const.toInt(environmentSubstitute(proxyPort), 21);
      if (proxyport != 0) {
        ftpsclient.setProxyPort(proxyport);
      }
    } else {
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "ActionFTPDelete.OpenedConnectionTo", realservername));
      }
    }

    // set activeConnection connectmode ...
    if (activeConnection) {
      ftpsclient.setPassiveMode(false);
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.SetActive"));
      }
    } else {
      ftpsclient.setPassiveMode(true);
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.SetPassive"));
      }
    }

    // Set the timeout
    ftpsclient.setTimeOut(realtimeout);
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(PKG, "ActionFTPDelete.SetTimeout", String.valueOf(realtimeout)));
    }

    // now connect
    ftpsclient.connect();

    // move to spool dir ...
    if (!Utils.isEmpty(realFtpDirectory)) {
      ftpsclient.changeDirectory(realFtpDirectory);
      if (isDetailed()) {
        logDetailed("Changed to directory [" + realFtpDirectory + "]");
      }
    }
  }

  private void FtpConnect(
      String realServername,
      String realusername,
      String realpassword,
      int realport,
      String realFtpDirectory,
      String realProxyhost,
      String realproxyusername,
      String realproxypassword,
      int realproxyport,
      int realtimeout)
      throws Exception {

    // Create ftp client to host:port ...
    ftpclient = new FTPClient();
    ftpclient.setRemoteAddr(InetAddress.getByName(realServername));
    if (realport != 0) {
      ftpclient.setRemotePort(realport);
    }

    if (!Utils.isEmpty(realProxyhost)) {
      ftpclient.setRemoteAddr(InetAddress.getByName(realProxyhost));
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "ActionFTPDelete.OpenedProxyConnectionOn", realProxyhost));
      }

      // FIXME: Proper default port for proxy
      if (realproxyport != 0) {
        ftpclient.setRemotePort(realproxyport);
      }
    } else {
      ftpclient.setRemoteAddr(InetAddress.getByName(realServername));

      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "ActionFTPDelete.OpenedConnectionTo", realServername));
      }
    }

    // set activeConnection connectmode ...
    if (activeConnection) {
      ftpclient.setConnectMode(FTPConnectMode.ACTIVE);
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.SetActive"));
      }
    } else {
      ftpclient.setConnectMode(FTPConnectMode.PASV);
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.SetPassive"));
      }
    }

    // Set the timeout
    ftpclient.setTimeout(realtimeout);
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(PKG, "ActionFTPDelete.SetTimeout", String.valueOf(realtimeout)));
    }

    // login to ftp host ...
    ftpclient.connect();

    String realUsername =
        realusername
            + (!Utils.isEmpty(realProxyhost) ? "@" + realServername : "")
            + (!Utils.isEmpty(realproxyusername) ? " " + realproxyusername : "");

    String realPassword =
        realpassword + (!Utils.isEmpty(realproxypassword) ? " " + realproxypassword : "");

    ftpclient.login(realUsername, realPassword);
    // Remove password from logging, you don't know where it ends up.
    if (isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.LoggedIn", realUsername));
    }

    // move to spool dir ...
    if (!Utils.isEmpty(realFtpDirectory)) {
      ftpclient.chdir(realFtpDirectory);
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
    NrfilesDeleted++;
  }

  public boolean evaluates() {
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
