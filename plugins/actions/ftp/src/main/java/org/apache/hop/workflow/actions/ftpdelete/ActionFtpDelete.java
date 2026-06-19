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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.actions.sftp.FileItem;
import org.apache.hop.workflow.actions.sftp.SftpClient;
import org.apache.hop.workflow.actions.util.FtpClientUtil;
import org.apache.hop.workflow.actions.util.IFtpConnection;

/** This defines an FTP action. */
@Action(
    id = "FTP_DELETE",
    name = "i18n::ActionFTPDelete.Name",
    description = "i18n::ActionFTPDelete.Description",
    image = "FTPDelete.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileTransfer",
    keywords = "i18n::ActionFtpDelete.keyword",
    documentationUrl = "/workflow/actions/ftpdelete.html")
@Getter
@Setter
public class ActionFtpDelete extends ActionBase implements Cloneable, IAction, IFtpConnection {
  private static final Class<?> PKG = ActionFtpDelete.class;
  private static final String CONST_SPACE_SHORT = "      ";
  private static final String CONST_PASSWORD = "password";

  @HopMetadataProperty(key = "servername")
  private String serverName;

  @HopMetadataProperty(key = "port")
  private String serverPort;

  @HopMetadataProperty(key = "username")
  private String userName;

  @HopMetadataProperty(key = "password", password = true)
  private String password;

  @HopMetadataProperty(key = "ftpdirectory")
  private String remoteDirectory;

  @HopMetadataProperty(key = "wildcard")
  private String wildcard;

  @HopMetadataProperty(key = "timeout")
  private int timeout;

  @HopMetadataProperty(key = "active")
  private boolean activeConnection;

  @HopMetadataProperty(key = "publicpublickey")
  private boolean usingPublicKey;

  @HopMetadataProperty(key = "keyfilename")
  private String keyFilename;

  @HopMetadataProperty(key = "keyfilepass", password = true)
  private String keyFilePass;

  @HopMetadataProperty(key = "useproxy")
  private boolean useProxy;

  @HopMetadataProperty(key = "proxy_host")
  private String proxyHost;

  @HopMetadataProperty(key = "proxy_port")
  private String proxyPort; /* string to allow variable substitution */

  @HopMetadataProperty(key = "proxy_username")
  private String proxyUsername;

  @HopMetadataProperty(key = "proxy_password", password = true)
  private String proxyPassword;

  @HopMetadataProperty(key = "socksproxy_host")
  private String socksProxyHost;

  @HopMetadataProperty(key = "socksproxy_port")
  private String socksProxyPort;

  @HopMetadataProperty(key = "socksproxy_username")
  private String socksProxyUsername;

  @HopMetadataProperty(key = "socksproxy_password", password = true)
  private String socksProxyPassword;

  @HopMetadataProperty(key = "protocol")
  private String protocol;

  @HopMetadataProperty(key = "nr_limit_success")
  private String nrLimitSuccess;

  @HopMetadataProperty(key = "success_condition")
  private String successCondition;

  @HopMetadataProperty(key = "copyprevious")
  private boolean copyPrevious;

  public static final String PROTOCOL_FTP = "FTP";
  public static final String PROTOCOL_SFTP = "SFTP";
  public static final String SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED = "success_when_at_least";
  public static final String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public static final String SUCCESS_IF_ALL_FILES_DOWNLOADED = "success_is_all_files_downloaded";

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
    usingPublicKey = false;
    keyFilename = null;
    keyFilePass = null;
    serverName = null;
  }

  public ActionFtpDelete() {
    this("");
  }

  public ActionFtpDelete(ActionFtpDelete a) {
    super(a);
    this.serverName = a.serverName;
    this.serverPort = a.serverPort;
    this.userName = a.userName;
    this.password = a.password;
    this.remoteDirectory = a.remoteDirectory;
    this.wildcard = a.wildcard;
    this.timeout = a.timeout;
    this.activeConnection = a.activeConnection;
    this.usingPublicKey = a.usingPublicKey;
    this.keyFilename = a.keyFilename;
    this.keyFilePass = a.keyFilePass;
    this.useProxy = a.useProxy;
    this.proxyHost = a.proxyHost;
    this.proxyPort = a.proxyPort;
    this.proxyUsername = a.proxyUsername;
    this.proxyPassword = a.proxyPassword;
    this.socksProxyHost = a.socksProxyHost;
    this.socksProxyPort = a.socksProxyPort;
    this.socksProxyUsername = a.socksProxyUsername;
    this.socksProxyPassword = a.socksProxyPassword;
    this.protocol = a.protocol;
    this.nrLimitSuccess = a.nrLimitSuccess;
    this.successCondition = a.successCondition;
    this.copyPrevious = a.copyPrevious;

    this.nrErrors = 0;
    this.nrFilesDeleted = 0;
    this.successConditionBroken = false;
    this.targetFilename = null;
    this.limitFiles = 0;
    this.ftpclient = null;
    this.sftpclient = null;
  }

  @Override
  public Object clone() {
    return new ActionFtpDelete(this);
  }

  private boolean getStatus() {
    return (nrErrors == 0 && getSuccessCondition().equals(SUCCESS_IF_ALL_FILES_DOWNLOADED))
        || (nrFilesDeleted >= limitFiles
            && getSuccessCondition().equals(SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED))
        || (nrErrors <= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS));
  }

  @Override
  public boolean isBinaryMode() {
    return true;
  }

  @Override
  public String getControlEncoding() {
    return null;
  }

  /** Needed for the Vector coming from sshclient.ls() * */
  @Override
  public Result execute(Result result, int nr) {
    logBasic(BaseMessages.getString(PKG, "ActionFTPDelete.Started", serverName));
    RowMetaAndData resultRow;
    List<RowMetaAndData> rows = result.getRows();

    result.setResult(false);
    nrErrors = 0;
    nrFilesDeleted = 0;
    successConditionBroken = false;
    HashSet<String> listPreviousFiles = new HashSet<>();

    // Here let's put some controls before stating the workflow

    String realServerName = resolve(serverName);
    String realFtpDirectory = resolve(remoteDirectory);

    int realServerPort = Const.toInt(resolve(serverPort), 0);
    String realUsername = resolve(userName);
    String realPassword = Utils.resolvePassword(this, password);

    // The following is used to apply a path for SSH because the SFTPv3Client doesn't let us
    // specify/change dirs
    //
    logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.Start"));

    if (copyPrevious && rows.isEmpty()) {
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
          if (!Utils.isEmpty(translatedWildcard) && filelist[0].startsWith(translatedWildcard)) {
            throw new HopException(filelist[0]);
          }
        }
      } else if (protocol.equals(PROTOCOL_SFTP)) {
        // establish the secure connection
        sftpConnect(realServerName, realUsername, realServerPort, realPassword, realFtpDirectory);

        // Get all the files in the current directory...
        ArrayList<FileItem> dirlist = sftpclient.dir();

        if (!dirlist.isEmpty()) {
          filelist =
              dirlist.stream()
                  .map(FileItem::getFileName) // adapt getter if it's named differently
                  .toArray(String[]::new);
        }
      }

      if (isDetailed()) {
        logDetailed("ActionFTPDelete.FoundNFiles", (filelist == null ? "0" : filelist.length));
      }
      int found = filelist == null ? 0 : filelist.length;
      if (found == 0) {
        result.setResult(true);
        return result;
      }

      Pattern pattern = null;
      if (copyPrevious) {
        // Copy the input row to the (command line) arguments
        for (RowMetaAndData row : rows) {
          resultRow = row;

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
        limitFiles = Const.toInt(resolve(getNrLimitSuccess()), 10);
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
            throw new HopException(
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
   * @param sftpClient The SFTP client
   * @param filename The file or folder to check
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
      String realServerName,
      String realUserName,
      int realPort,
      String realPassword,
      String realFtpDirectory)
      throws HopException {
    try {
      // Create sftp client to host ...
      sftpclient = new SftpClient(InetAddress.getByName(realServerName), realPort, realUserName);

      // login to ftp host ...
      sftpclient.login(realPassword);

      // move to spool dir ...
      if (!Utils.isEmpty(realFtpDirectory)) {
        sftpclient.chdir(realFtpDirectory);
        if (isDetailed()) {
          logDetailed("Changed to directory [" + realFtpDirectory + "]");
        }
      }
    } catch (Exception e) {
      throw new HopException("Error connecting to server " + realServerName, e);
    }
  }

  private void ftpConnect(String realFtpDirectory) throws HopException {
    try {
      // Create ftp client to host:port ...
      ftpclient = FtpClientUtil.connectAndLogin(getLogChannel(), this, this, getName());

      // move to spool dir ...
      if (!Utils.isEmpty(realFtpDirectory)) {
        ftpclient.changeWorkingDirectory(realFtpDirectory);
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.ChangedDir", realFtpDirectory));
        }
      }
    } catch (Exception e) {
      throw new HopException("Error connecting to FTP server " + getServerName(), e);
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

  @Override
  public boolean isEvaluation() {
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
            CONST_PASSWORD,
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
}
