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
import java.util.Set;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.vfs.ftp.FtpHelper;
import org.apache.hop.vfs.ftp.IFtpConnection;
import org.apache.hop.vfs.sftp.SftpConnections;
import org.apache.hop.vfs.sftp.client.FileItem;
import org.apache.hop.vfs.sftp.client.SftpClient;
import org.apache.hop.vfs.sftp.metadata.SftpConnection;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;

/** This defines an FTP delete action. */
@Action(
    id = "FTP_DELETE",
    name = "i18n::ActionFTPDelete.Name",
    description = "i18n::ActionFTPDelete.Description",
    image = "FTPDelete.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileTransfer",
    keywords = "i18n::ActionFtpDelete.keyword",
    documentationUrl = "/workflow/actions/ftpdelete.html",
    classLoaderGroup = "sftp")
@Getter
@Setter
public class ActionFtpDelete extends ActionBase implements Cloneable, IAction, IFtpConnection {
  private static final Class<?> PKG = ActionFtpDelete.class;

  public static final String PROTOCOL_FTP = "FTP";

  /**
   * Deprecated. This action grew an SFTP mode long before Hop could reach an SFTP server any other
   * way. It can now: the generic Delete files action resolves its paths through VFS, so a named
   * SFTP connection or a plain {@code sftp://} path deletes remote files without a second
   * implementation of the same thing. The mode keeps working for the workflows which use it, and
   * says so in the log.
   */
  public static final String PROTOCOL_SFTP = "SFTP";

  public static final String SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED = "success_when_at_least";
  public static final String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public static final String SUCCESS_IF_ALL_FILES_DOWNLOADED = "success_is_all_files_downloaded";

  /**
   * The name of an FTP connection in the metadata, used when the protocol is FTP. When it's set,
   * the server settings below are ignored: the connection has them all.
   */
  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.VFS_FTP_CONNECTION)
  private String connection;

  /** The same, for the SFTP protocol: SFTP servers live in their own kind of connection. */
  @HopMetadataProperty(
      key = "sftp_connection",
      hopMetadataPropertyType = HopMetadataPropertyType.VFS_SFTP_CONNECTION)
  private String sftpConnection;

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

  /** Authenticate with a private key instead of a password. Only used by the SFTP protocol. */
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
    this.connection = a.connection;
    this.sftpConnection = a.sftpConnection;
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
  }

  @Override
  public String getFtpConnectionName() {
    return Const.NVL(getName(), "FTP Delete");
  }

  /** Deleting is a command on the control connection, the transfer mode never comes into play. */
  @Override
  public boolean isBinaryMode() {
    return true;
  }

  @Override
  public String getControlEncoding() {
    return null;
  }

  /** The legacy inline setting is a number of milliseconds, the connection contract is a string. */
  @Override
  public String getConnectTimeout() {
    return timeout <= 0 ? null : Integer.toString(timeout);
  }

  public boolean isUsingSftp() {
    return PROTOCOL_SFTP.equals(protocol);
  }

  /** Whether this action gets its server settings from a named connection. */
  public boolean isUsingConnection() {
    return FtpHelper.isUsingConnection(isUsingSftp() ? sftpConnection : connection);
  }

  @Override
  public Result execute(Result result, int nr) {
    logBasic(
        BaseMessages.getString(
            PKG,
            "ActionFTPDelete.Started",
            FtpHelper.describeTarget(
                this, isUsingSftp() ? sftpConnection : connection, serverName)));
    logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.Start"));

    result.setResult(false);
    List<RowMetaAndData> rows = result.getRows();

    if (isUsingSftp()) {
      logBasic(BaseMessages.getString(PKG, "ActionFTPDelete.SftpDeprecated"));
    }

    if (copyPrevious && rows.isEmpty()) {
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.ArgsFromPreviousNothing"));
      }
      result.setResult(true);
      return result;
    }

    Deletion deletion = new Deletion();
    FTPClient ftpClient = null;
    SftpClient sftpClient = null;

    try {
      // Work out which files we're after before connecting: an action which can't name any has
      // nothing to go to the server for.
      FilenameSelector selector = selector(rows);

      List<String> filenames;
      if (isUsingSftp()) {
        sftpClient = sftpConnect();
        filenames = listSftp(sftpClient);
      } else {
        ftpClient = ftpConnect();
        filenames = listFtp(ftpClient);
      }

      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG, "ActionFTPDelete.FoundNFiles", String.valueOf(filenames.size())));
      }
      if (filenames.isEmpty()) {
        result.setResult(true);
        return result;
      }

      deleteFiles(filenames, selector, ftpClient, sftpClient, deletion);
    } catch (Exception e) {
      deletion.updateErrors();
      logError(BaseMessages.getString(PKG, "ActionFTPDelete.ErrorGetting", e.getMessage()), e);
    } finally {
      FtpHelper.disconnect(getLogChannel(), ftpClient);
      if (sftpClient != null) {
        try {
          sftpClient.disconnect();
        } catch (Exception e) {
          logError(BaseMessages.getString(PKG, "ActionFTPDelete.ErrorQuitting", e.getMessage()), e);
        }
      }
    }

    result.setResult(deletion.isSuccess());
    result.setNrFilesRetrieved(deletion.nrFilesDeleted);
    result.setNrErrors(deletion.nrErrors);
    return result;
  }

  /**
   * Which of the files on the server this action is after: the ones named in the incoming rows, or
   * the ones matching the wildcard.
   *
   * @throws HopException when neither is configured - deleting every file in the folder is not
   *     something to do by accident.
   */
  private FilenameSelector selector(List<RowMetaAndData> rows) throws HopException {
    if (copyPrevious) {
      Set<String> previousFiles = new HashSet<>();
      for (RowMetaAndData row : rows) {
        String filePrevious = row.getString(0, null);
        if (!Utils.isEmpty(filePrevious)) {
          previousFiles.add(filePrevious);
        }
      }
      return previousFiles::contains;
    }

    if (Utils.isEmpty(wildcard)) {
      throw new HopException(BaseMessages.getString(PKG, "ActionFTPDelete.NoWildcard"));
    }
    Pattern pattern = Pattern.compile(resolve(wildcard));
    return filename -> pattern.matcher(filename).matches();
  }

  private void deleteFiles(
      List<String> filenames,
      FilenameSelector selector,
      FTPClient ftpClient,
      SftpClient sftpClient,
      Deletion deletion)
      throws HopException {

    for (String filename : filenames) {
      if (parentWorkflow.isStopped()) {
        return;
      }
      if (deletion.successConditionBroken) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionFTPDelete.SuccesConditionBroken"));
      }
      if (isDebug()) {
        logDebug(BaseMessages.getString(PKG, "ActionFTPDelete.AnalysingFile", filename));
      }
      if (!selector.matches(filename)) {
        continue;
      }

      try {
        if (sftpClient != null) {
          sftpClient.delete(filename);
        } else if (!ftpClient.deleteFile(filename)) {
          throw new HopException(Const.NVL(ftpClient.getReplyString(), "").trim());
        }
        deletion.nrFilesDeleted++;
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.RemotefileDeleted", filename));
        }
      } catch (Exception e) {
        deletion.updateErrors();
        logError(BaseMessages.getString(PKG, "ActionFtp.UnexpectedError", e.getMessage()));
        if (deletion.successConditionBroken) {
          throw new HopException(
              BaseMessages.getString(PKG, "ActionFTPDelete.SuccesConditionBroken"));
        }
      }
    }
  }

  private List<String> listFtp(FTPClient ftpClient) throws HopException {
    try {
      String[] filenames = ftpClient.listNames();
      return filenames == null ? List.of() : List.of(filenames);
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionFTPDelete.ErrorGetting", e.getMessage()), e);
    }
  }

  private List<String> listSftp(SftpClient sftpClient) throws HopException {
    ArrayList<FileItem> dirList = sftpClient.dir();
    if (dirList == null) {
      return List.of();
    }
    return dirList.stream().map(FileItem::getFileName).toList();
  }

  private FTPClient ftpConnect() throws HopException {
    FTPClient ftpClient = FtpHelper.connect(this, connection);
    String realFtpDirectory = resolve(remoteDirectory);
    if (Utils.isEmpty(realFtpDirectory)) {
      return ftpClient;
    }
    try {
      if (!ftpClient.changeWorkingDirectory(realFtpDirectory)) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionFTPDelete.NonExistentFolder", realFtpDirectory));
      }
    } catch (HopException e) {
      FtpHelper.disconnect(getLogChannel(), ftpClient);
      throw e;
    } catch (Exception e) {
      FtpHelper.disconnect(getLogChannel(), ftpClient);
      throw new HopException(
          BaseMessages.getString(PKG, "ActionFTPDelete.NonExistentFolder", realFtpDirectory), e);
    }
    if (isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.ChangedDir", realFtpDirectory));
    }
    return ftpClient;
  }

  /**
   * Connect over SFTP: through the named SFTP connection when there is one, on the inline settings
   * of this action otherwise.
   */
  private SftpClient sftpConnect() throws HopException {
    SftpClient sftpClient;
    String namedConnection = resolve(sftpConnection);
    if (StringUtils.isNotEmpty(namedConnection)) {
      SftpConnection metadataConnection =
          SftpConnections.load(getMetadataProvider(), namedConnection);
      sftpClient = SftpConnections.createClient(this, metadataConnection);
    } else {
      sftpClient = inlineSftpClient();
    }

    try {
      String realFtpDirectory = resolve(remoteDirectory);
      if (!Utils.isEmpty(realFtpDirectory)) {
        sftpClient.chdir(realFtpDirectory);
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionFTPDelete.ChangedDir", realFtpDirectory));
        }
      }
      return sftpClient;
    } catch (Exception e) {
      sftpClient.disconnect();
      throw new HopException("Error connecting to server " + resolve(serverName), e);
    }
  }

  /** An SFTP client on the settings kept in this action, private key and all. */
  private SftpClient inlineSftpClient() throws HopException {
    String realServerName = resolve(serverName);
    int realPort = Const.toInt(resolve(serverPort), 22);
    String realUserName = resolve(userName);

    String realKeyFilename = null;
    String realKeyPassPhrase = null;
    if (usingPublicKey) {
      realKeyFilename = resolve(keyFilename);
      if (Utils.isEmpty(realKeyFilename)) {
        throw new HopException(BaseMessages.getString(PKG, "ActionFTPDelete.KeyFileMissing"));
      }
      if (!HopVfs.fileExists(realKeyFilename)) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionFTPDelete.KeyFileNotFound", realKeyFilename));
      }
      realKeyPassPhrase = Utils.resolvePassword(this, keyFilePass);
    }

    SftpClient sftpClient;
    try {
      sftpClient =
          new SftpClient(
              InetAddress.getByName(realServerName),
              realPort,
              realUserName,
              realKeyFilename,
              realKeyPassPhrase);
    } catch (Exception e) {
      throw new HopException("Error connecting to server " + realServerName, e);
    }

    try {
      if (useProxy && !Utils.isEmpty(resolve(proxyHost))) {
        sftpClient.setProxy(
            resolve(proxyHost),
            resolve(proxyPort),
            resolve(proxyUsername),
            Utils.resolvePassword(this, proxyPassword),
            SftpClient.PROXY_TYPE_HTTP);
      }
      sftpClient.login(Utils.resolvePassword(this, password));
      return sftpClient;
    } catch (Exception e) {
      sftpClient.disconnect();
      throw new HopException("Error connecting to server " + realServerName, e);
    }
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
    FtpHelper.checkServerSettings(remarks, this, isUsingSftp() ? sftpConnection : connection);
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    FtpHelper.addServerResourceReferenceIfPresent(references, serverName, this, this);
    return references;
  }

  /** Which of the files on the server should go. */
  @FunctionalInterface
  private interface FilenameSelector {
    boolean matches(String filename);
  }

  /** What a single run of this action counts, and what it makes of those counts. */
  final class Deletion {
    private long nrErrors = 0;
    private long nrFilesDeleted = 0;
    private boolean successConditionBroken = false;
    private final int limitFiles = Const.toInt(resolve(getNrLimitSuccess()), 10);

    void updateErrors() {
      nrErrors++;
      // Only the conditions which an error can break: with "at least X files" it's the count at
      // the end that decides, so a single error along the way must not stop the run.
      if ((nrErrors > 0 && SUCCESS_IF_ALL_FILES_DOWNLOADED.equals(getSuccessCondition()))
          || (nrErrors >= limitFiles && SUCCESS_IF_ERRORS_LESS.equals(getSuccessCondition()))) {
        successConditionBroken = true;
      }
    }

    boolean isSuccess() {
      return (nrErrors == 0 && SUCCESS_IF_ALL_FILES_DOWNLOADED.equals(getSuccessCondition()))
          || (nrFilesDeleted >= limitFiles
              && SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED.equals(getSuccessCondition()))
          || (nrErrors <= limitFiles && SUCCESS_IF_ERRORS_LESS.equals(getSuccessCondition()));
    }
  }
}
