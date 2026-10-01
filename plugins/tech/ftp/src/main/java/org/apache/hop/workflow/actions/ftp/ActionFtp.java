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

import com.google.common.annotations.VisibleForTesting;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.vfs.ftp.FtpHelper;
import org.apache.hop.vfs.ftp.IFtpConnection;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;

/** This defines an FTP action. */
@Action(
    id = "FTP",
    name = "i18n::ActionFTP.Name",
    description = "i18n::ActionFTP.Description",
    image = "FTP.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileTransfer",
    keywords = "i18n::ActionFtp.keyword",
    documentationUrl = "/workflow/actions/ftp.html",
    classLoaderGroup = "sftp")
@Getter
@Setter
public class ActionFtp extends ActionBase implements Cloneable, IAction, IFtpConnection {
  private static final Class<?> PKG = ActionFtp.class;
  private static final String CONST_LOCAL_FILE_EXISTS = "ActionFTP.LocalFileExists";

  /** Default encoding when making a new ftp action instance. */
  private static final String DEFAULT_CONTROL_ENCODING = "ISO-8859-1";

  public static final String FILE_SEPARATOR = "/";

  @Getter
  public enum IfFileExistsOperation implements IEnumHasCodeAndDescription {
    SKIP("ifFileExistsSkip", "ActionFtp.Skip.Label"),
    CREATE_UNIQUE("ifFileExistsCreateUniq", "ActionFtp.Give_Unique_Name.Label"),
    FAIL("ifFileExistsFail", "ActionFtp.Fail.Label"),
    ;
    private final String code;
    private final String descriptionKey;

    IfFileExistsOperation(String code, String descriptionKey) {
      this.code = code;
      this.descriptionKey = descriptionKey;
    }

    /** Looked up per call: the language of the UI is picked after this class is loaded. */
    @Override
    public String getDescription() {
      return BaseMessages.getString(PKG, descriptionKey);
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(IfFileExistsOperation.class);
    }

    public static IfFileExistsOperation lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          IfFileExistsOperation.class, description, SKIP);
    }
  }

  public static final String SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED = "success_when_at_least";
  public static final String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";
  public static final String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";

  /**
   * The name of an FTP connection in the metadata. When it's set, the server settings below are
   * ignored: the connection has them all.
   */
  @HopMetadataProperty(
      key = "connection",
      hopMetadataPropertyType = HopMetadataPropertyType.VFS_FTP_CONNECTION)
  private String connection;

  @HopMetadataProperty(key = "servername")
  private String serverName;

  @HopMetadataProperty(key = "username")
  private String userName;

  @HopMetadataProperty(key = "password", password = true)
  private String password;

  @HopMetadataProperty(key = "ftpdirectory")
  private String remoteDirectory;

  @HopMetadataProperty(key = "targetdirectory")
  private String targetDirectory;

  @HopMetadataProperty(key = "wildcard")
  private String wildcard;

  @HopMetadataProperty(key = "binary")
  private boolean binaryMode;

  @HopMetadataProperty(key = "timeout")
  private int timeout;

  @HopMetadataProperty(key = "remove")
  private boolean remove;

  @HopMetadataProperty(key = "only_new")
  private boolean onlyGettingNewFiles; /* Don't overwrite files */

  @HopMetadataProperty(key = "active")
  private boolean activeConnection;

  @HopMetadataProperty(key = "control_encoding")
  private String controlEncoding; /* how to convert list of filenames e.g. */

  @HopMetadataProperty(key = "movefiles")
  private boolean moveFiles;

  @HopMetadataProperty(key = "movetodirectory")
  private String moveToDirectory;

  @HopMetadataProperty(key = "adddate")
  private boolean addDate;

  @HopMetadataProperty(key = "addtime")
  private boolean addTime;

  @HopMetadataProperty(key = "SpecifyFormat")
  private boolean specifyFormat;

  @HopMetadataProperty(key = "date_time_format")
  private String dateTimeFormat;

  @HopMetadataProperty(key = "AddDateBeforeExtension")
  private boolean addDateBeforeExtension;

  @HopMetadataProperty(key = "isaddresult")
  private boolean addResult;

  @HopMetadataProperty(key = "createmovefolder")
  private boolean createMoveFolder;

  @HopMetadataProperty(key = "port")
  private String serverPort;

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

  @HopMetadataProperty(key = "ifFileExists", storeWithCode = true)
  private IfFileExistsOperation ifFileExistsOperation;

  @HopMetadataProperty(key = "nr_limit")
  private String nrLimit;

  @HopMetadataProperty(key = "success_condition")
  private String successCondition;

  public ActionFtp(String n) {
    super(n, "");
    nrLimit = "10";
    serverPort = "21";
    socksProxyPort = "1080";
    successCondition = SUCCESS_IF_NO_ERRORS;
    ifFileExistsOperation = IfFileExistsOperation.SKIP;

    serverName = null;
    moveFiles = false;
    moveToDirectory = null;
    addDate = false;
    addTime = false;
    specifyFormat = false;
    addDateBeforeExtension = false;
    addResult = true;
    createMoveFolder = false;

    setControlEncoding(DEFAULT_CONTROL_ENCODING);
  }

  public ActionFtp() {
    this("");
  }

  @Override
  public String getFtpConnectionName() {
    return Const.NVL(getName(), "FTP");
  }

  /** The legacy inline setting is a number of milliseconds, the connection contract is a string. */
  @Override
  public String getConnectTimeout() {
    return timeout <= 0 ? null : Integer.toString(timeout);
  }

  /** Whether this action gets its server settings from a named FTP connection. */
  public boolean isUsingConnection() {
    return FtpHelper.isUsingConnection(connection);
  }

  @Override
  public Result execute(Result result, int nr) {
    logBasic(
        BaseMessages.getString(
            PKG, "ActionFTP.Started", FtpHelper.describeTarget(this, connection, serverName)));

    result.setNrErrors(1);
    result.setResult(false);

    // Here let's put some controls before stating the workflow
    if (moveFiles && Utils.isEmpty(moveToDirectory)) {
      logError(BaseMessages.getString(PKG, "ActionFTP.MoveToFolderEmpty"));
      return result;
    }

    logDetailed(BaseMessages.getString(PKG, "ActionFTP.Start"));

    // Everything a single run counts, kept out of the action itself: an action instance is shared
    // between the runs of a workflow and two of those may well overlap.
    //
    Download download = new Download();
    FTPClient ftpClient = null;
    boolean exitAction = false;

    try {
      ftpClient = FtpHelper.connect(this, connection);

      // move to spool dir ...
      if (!Utils.isEmpty(remoteDirectory)) {
        String realFtpDirectory = normalizePath(resolve(remoteDirectory));
        if (!ftpClient.changeWorkingDirectory(realFtpDirectory)) {
          logError(BaseMessages.getString(PKG, "ActionFtp.NonExistentFolder"));
          return result;
        }
        logDetailed(BaseMessages.getString(PKG, "ActionFTP.ChangedDir", realFtpDirectory));
      }

      String realMoveToFolder = null;
      if (moveFiles) {
        realMoveToFolder = normalizePath(resolve(moveToDirectory));
        exitAction = !prepareMoveToFolder(ftpClient, realMoveToFolder, download);
      }

      if (!exitAction) {
        downloadFiles(ftpClient, realMoveToFolder, download, result);
      }
    } catch (Exception e) {
      if (!download.successConditionBroken) {
        download.updateErrors();
      }
      logError(BaseMessages.getString(PKG, "ActionFTP.ErrorGetting", e.getMessage()), e);
    } finally {
      FtpHelper.disconnect(getLogChannel(), ftpClient);
    }

    result.setNrErrors(download.nrErrors);
    result.setNrFilesRetrieved(download.nrFilesRetrieved);
    result.setResult(!exitAction && download.isSuccess());
    download.log();
    return result;
  }

  /**
   * Make sure the folder files are moved to is there, creating it when the action is set up to.
   *
   * @return false when the action can't go on without it
   */
  private boolean prepareMoveToFolder(
      FTPClient ftpClient, String realMoveToFolder, Download download) throws Exception {
    logDetailed(BaseMessages.getString(PKG, "ActionFTP.CheckMoveToFolder", realMoveToFolder));

    String originalLocation = ftpClient.printWorkingDirectory();
    boolean folderExists = ftpClient.changeWorkingDirectory(realMoveToFolder);
    if (folderExists) {
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "ActionFTP.CheckMoveToFolderSwitchBack", originalLocation));
      }
      ftpClient.changeWorkingDirectory(originalLocation);
      return true;
    }

    if (!createMoveFolder) {
      logError(BaseMessages.getString(PKG, "ActionFTP.MoveToFolderNotExist"));
      download.updateErrors();
      return false;
    }

    if (!ftpClient.makeDirectory(realMoveToFolder)) {
      logError(BaseMessages.getString(PKG, "ActionFTP.MoveToFolderNotExist"));
      download.updateErrors();
      return false;
    }
    if (isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "ActionFTP.MoveToFolderCreated", realMoveToFolder));
    }
    return true;
  }

  private void downloadFiles(
      FTPClient ftpClient, String realMoveToFolder, Download download, Result result)
      throws HopException {
    FTPFile[] ftpFiles;
    try {
      ftpFiles = ftpClient.listFiles();
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionFTP.ErrorGetting", e.getMessage()), e);
    }
    if (ftpFiles == null) {
      ftpFiles = new FTPFile[0];
    }

    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(PKG, "ActionFTP.FoundNFiles", String.valueOf(ftpFiles.length)));
    }

    Pattern pattern = Utils.isEmpty(wildcard) ? null : Pattern.compile(resolve(wildcard));

    for (FTPFile ftpFile : ftpFiles) {
      if (parentWorkflow.isStopped()) {
        throw new HopException(BaseMessages.getString(PKG, "ActionFTP.WorkflowStopped"));
      }
      if (download.successConditionBroken) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "ActionFTP.SuccesConditionBroken", String.valueOf(download.nrErrors)));
      }

      String filename = ftpFile.getName();
      if (isDebug()) {
        logDebug(BaseMessages.getString(PKG, "ActionFTP.AnalysingFile", filename));
      }

      if (ftpFile.isDirectory()) {
        if (isDebug()) {
          logDebug(BaseMessages.getString(PKG, "ActionFTP.SkippingNotAFile", filename));
        }
        continue;
      }
      if (pattern != null && !pattern.matcher(filename).matches()) {
        continue;
      }

      try {
        downloadFile(ftpClient, filename, realMoveToFolder, download, result);
      } catch (Exception e) {
        download.updateErrors();
        logError(BaseMessages.getString(PKG, "ActionFtp.UnexpectedError", e.toString()));
      }
    }
  }

  private void downloadFile(
      FTPClient ftpclient,
      String filename,
      String realMoveToFolder,
      Download download,
      Result result)
      throws HopException {
    try {
      String targetFilename =
          HopVfs.getFilename(HopVfs.getFileObject(returnTargetFilename(filename)));

      if (onlyGettingNewFiles) {
        targetFilename = targetFilenameToUse(targetFilename, download);
        if (targetFilename == null) {
          return;
        }
      }

      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG, "ActionFTP.GettingFile", filename, resolve(targetDirectory)));
      }
      try (OutputStream outputStream = HopVfs.getOutputStream(targetFilename, false)) {
        if (!ftpclient.retrieveFile(filename, outputStream)) {
          throw new HopException(
              BaseMessages.getString(
                  PKG,
                  "ActionFTP.ErrorGetting",
                  filename + " : " + Const.NVL(ftpclient.getReplyString(), "").trim()));
        }
      }

      download.nrFilesRetrieved++;
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionFTP.GotFile", filename));
      }

      addFilenameToResultFilenames(result, parentWorkflow, targetFilename);

      // Delete the file if this is needed!
      if (remove) {
        ftpclient.deleteFile(filename);
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionFTP.DeletedFile", filename));
        }
      } else if (moveFiles) {
        // Try to move file to destination folder ...
        ftpclient.rename(filename, realMoveToFolder + FILE_SEPARATOR + filename);

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "ActionFTP.MovedFile", filename, realMoveToFolder));
        }
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Error downloading file", e);
    }
  }

  /**
   * normalize \ to / and remove trailing slashes from a path
   *
   * @param path The path
   * @return normalized path
   */
  public String normalizePath(String path) {
    String normalizedPath = path.replace('\\', '/');
    while (normalizedPath.endsWith(FILE_SEPARATOR)) {
      normalizedPath = normalizedPath.substring(0, normalizedPath.length() - 1);
    }
    return normalizedPath;
  }

  private void addFilenameToResultFilenames(
      Result result, IWorkflowEngine<WorkflowMeta> parentWorkflow, String filename)
      throws HopException {
    if (addResult) {
      try (FileObject targetFile = HopVfs.getFileObject(filename)) {

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
      }
    }
  }

  /**
   * @param filename the filename from the FTP server
   * @return the calculated target filename
   */
  @VisibleForTesting
  String returnTargetFilename(String filename) {
    if (filename == null) {
      return null;
    }
    String retval = filename;

    int stringLength = retval.length();
    int lastIndexOfDot = retval.lastIndexOf(".");
    if (lastIndexOfDot == -1) {
      lastIndexOfDot = stringLength;
    }

    String fileExtension = retval.substring(lastIndexOfDot, stringLength);

    if (isAddDateBeforeExtension()) {
      retval = retval.substring(0, lastIndexOfDot);
    }

    SimpleDateFormat daf = new SimpleDateFormat();
    Date now = new Date();

    if (specifyFormat && !Utils.isEmpty(dateTimeFormat)) {
      daf.applyPattern(dateTimeFormat);
      retval += daf.format(now);
    } else {
      if (addDate) {
        daf.applyPattern("yyyyMMdd");
        retval += "_" + daf.format(now);
      }
      if (addTime) {
        daf.applyPattern("HHmmssSSS");
        retval += "_" + daf.format(now);
      }
    }

    if (isAddDateBeforeExtension()) {
      retval += fileExtension;
    }

    // Add folder name to filename
    return resolve(targetDirectory) + Const.FILE_SEPARATOR + retval;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public boolean isUnconditional() {
    return false;
  }

  /**
   * Decide what to do about a target file which is already there, following {@link
   * #getIfFileExistsOperation()}.
   *
   * <p>The check goes through VFS rather than through {@code java.io.File} because the target
   * directory is allowed to be any location VFS can reach, which is also where the download is
   * written to.
   *
   * @param targetFilename the file the download would be written to
   * @param download the counters of this run
   * @return the file name to download to, or null to skip this file
   */
  @VisibleForTesting
  String targetFilenameToUse(String targetFilename, Download download) throws HopException {
    boolean exists;
    try (FileObject targetFile = HopVfs.getFileObject(targetFilename)) {
      exists = targetFile.exists();
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, CONST_LOCAL_FILE_EXISTS, targetFilename), e);
    }

    if (!exists) {
      logDebug(BaseMessages.getString(PKG, "ActionFTP.LocalFileNotExists", targetFilename));
      return targetFilename;
    }

    switch (ifFileExistsOperation) {
      case CREATE_UNIQUE -> {
        logDebug(BaseMessages.getString(PKG, CONST_LOCAL_FILE_EXISTS, targetFilename));
        int stringLength = targetFilename.length();
        int lastIndexOfDot = targetFilename.lastIndexOf('.');
        if (lastIndexOfDot == -1) {
          lastIndexOfDot = stringLength;
        }
        return targetFilename.substring(0, lastIndexOfDot)
            + StringUtil.getFormattedDateTimeNow(true)
            + targetFilename.substring(lastIndexOfDot, stringLength);
      }
      case FAIL -> {
        logError(BaseMessages.getString(PKG, CONST_LOCAL_FILE_EXISTS, targetFilename));
        download.updateErrors();
        return null;
      }
      default -> {
        logDebug(BaseMessages.getString(PKG, CONST_LOCAL_FILE_EXISTS, targetFilename));
        return null;
      }
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    FtpHelper.checkServerSettings(remarks, this, connection);
    FtpHelper.checkDirectoryExists(remarks, this, variables, "targetDirectory");
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    FtpHelper.addServerResourceReferenceIfPresent(references, serverName, this, this);
    return references;
  }

  /** What a single run of this action counts, and what it makes of those counts. */
  final class Download {
    private long nrErrors = 0;
    private long nrFilesRetrieved = 0;
    private boolean successConditionBroken = false;
    private final int limitFiles = Const.toInt(resolve(getNrLimit()), 10);

    void updateErrors() {
      nrErrors++;
      if ((nrErrors > 0 && SUCCESS_IF_NO_ERRORS.equals(getSuccessCondition()))
          || (nrErrors >= limitFiles && SUCCESS_IF_ERRORS_LESS.equals(getSuccessCondition()))) {
        successConditionBroken = true;
      }
    }

    boolean isSuccess() {
      return (nrErrors == 0 && SUCCESS_IF_NO_ERRORS.equals(getSuccessCondition()))
          || (nrFilesRetrieved >= limitFiles
              && SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED.equals(getSuccessCondition()))
          || (nrErrors <= limitFiles && SUCCESS_IF_ERRORS_LESS.equals(getSuccessCondition()));
    }

    void log() {
      if (isDetailed()) {
        logDetailed("=======================================");
        logDetailed(
            BaseMessages.getString(
                PKG, "ActionFTP.Log.Info.FilesInError", String.valueOf(nrErrors)));
        logDetailed(
            BaseMessages.getString(
                PKG, "ActionFTP.Log.Info.FilesRetrieved", String.valueOf(nrFilesRetrieved)));
        logDetailed("=======================================");
      }
    }
  }
}
