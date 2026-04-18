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
import java.io.File;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.net.ftp.FTP;
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
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
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
import org.apache.hop.workflow.engine.IWorkflowEngine;

/** This defines an FTP action. */
@Action(
    id = "FTP",
    name = "i18n::ActionFTP.Name",
    description = "i18n::ActionFTP.Description",
    image = "FTP.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileTransfer",
    keywords = "i18n::ActionFtp.keyword",
    documentationUrl = "/workflow/actions/ftp.html")
@Getter
@Setter
@SuppressWarnings("java:S1104")
public class ActionFtp extends ActionBase implements Cloneable, IAction, IFtpConnection {
  private static final Class<?> PKG = ActionFtp.class;
  private static final String CONST_SPACE_SHORT = "      ";
  private static final String CONST_PASSWORD = "password";
  private static final String CONST_LOCAL_FILE_EXISTS = "ActionFTP.LocalFileExists";

  /** Implicit encoding used before older version v2.4.1 */
  private static final String LEGACY_CONTROL_ENCODING = "US-ASCII";

  /** Default encoding when making a new ftp action instance. */
  private static final String DEFAULT_CONTROL_ENCODING = "ISO-8859-1";

  @Getter
  public enum IfFileExistsOperation implements IEnumHasCodeAndDescription {
    SKIP("ifFileExistsSkip", BaseMessages.getString(PKG, "ActionFtp.Skip.Label")),
    CREATE_UNIQUE(
        "ifFileExistsCreateUniq", BaseMessages.getString(PKG, "ActionFtp.Give_Unique_Name.Label")),
    FAIL("ifFileExistsFail", BaseMessages.getString(PKG, "ActionFtp.Fail.Label")),
    ;
    final String code;
    final String description;

    IfFileExistsOperation(String code, String description) {
      this.code = code;
      this.description = description;
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
  public IfFileExistsOperation ifFileExistsOperation;

  @HopMetadataProperty(key = "nr_limit")
  private String nrLimit;

  @HopMetadataProperty(key = "success_condition")
  private String successCondition;

  long nrErrors = 0;
  long nrFilesRetrieved = 0;
  boolean successConditionBroken = false;
  int limitFiles = 0;
  String targetFilename = null;
  public static final String FILE_SEPARATOR = "/";

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
  public Object clone() {
    ActionFtp je = (ActionFtp) super.clone();
    return je;
  }

  protected InetAddress getInetAddress(String realServername) throws UnknownHostException {
    return InetAddress.getByName(realServername);
  }

  @Override
  public Result execute(Result result, int nr) {
    logBasic(BaseMessages.getString(PKG, "ActionFTP.Started", serverName));

    result.setNrErrors(1);
    result.setResult(false);
    nrErrors = 0;
    nrFilesRetrieved = 0;
    successConditionBroken = false;
    boolean exitaction = false;
    limitFiles = Const.toInt(resolve(getNrLimit()), 10);

    // Here let's put some controls before stating the workflow
    if (moveFiles && Utils.isEmpty(moveToDirectory)) {
      logError(BaseMessages.getString(PKG, "ActionFTP.MoveToFolderEmpty"));
      return result;
    }

    logDetailed(BaseMessages.getString(PKG, "ActionFTP.Start"));

    FTPClient ftpClient = null;
    String realMoveToFolder = null;

    try {
      ftpClient = FtpClientUtil.connectAndLogin(getLogChannel(), this, this, getName());

      // move to spool dir ...
      if (!Utils.isEmpty(remoteDirectory)) {
        String realFtpDirectory = resolve(remoteDirectory);
        realFtpDirectory = normalizePath(realFtpDirectory);
        boolean exists = ftpClient.changeWorkingDirectory(realFtpDirectory);
        if (!exists) {
          logError(BaseMessages.getString(PKG, "ActionFTP.NonExistentFolder"));
          return result;
        }
        logDetailed(BaseMessages.getString(PKG, "ActionFTP.ChangedDir", realFtpDirectory));
      }

      // Create move to folder if necessary
      if (moveFiles && !Utils.isEmpty(moveToDirectory)) {
        realMoveToFolder = resolve(moveToDirectory);
        realMoveToFolder = normalizePath(realMoveToFolder);
        // Folder exists?
        boolean folderExist = true;
        logDetailed(BaseMessages.getString(PKG, "ActionFTP.CheckMoveToFolder", realMoveToFolder));
        String originalLocation = ftpClient.printWorkingDirectory();
        try {
          // try switching to the 'move to' folder.
          ftpClient.changeWorkingDirectory(realMoveToFolder);
          // Switch back to the previous location.
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionFTP.CheckMoveToFolderSwitchBack", originalLocation));
          }
          ftpClient.changeWorkingDirectory(originalLocation);
        } catch (Exception e) {
          folderExist = false;
          // Assume folder does not exist !!
        }

        if (!folderExist) {
          if (createMoveFolder) {
            ftpClient.makeDirectory(realMoveToFolder);
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
        //
        FTPFile[] ftpFiles = ftpClient.listFiles();

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionFTP.FoundNFiles", String.valueOf(ftpFiles.length)));
        }

        // Some FTP servers return a message saying no files found as a string in the filenlist
        // e.g. Solaris 8
        // CHECK THIS !!!

        if (ftpFiles.length == 1) {
          String translatedWildcard = resolve(wildcard);
          if (!Utils.isEmpty(translatedWildcard)
              && ftpFiles[0].getName().startsWith(translatedWildcard)) {
            throw new HopException(ftpFiles[0].getName());
          }
        }

        Pattern pattern = null;
        if (!Utils.isEmpty(wildcard)) {
          String realWildcard = resolve(wildcard);
          pattern = Pattern.compile(realWildcard);
        }

        if (!getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS)) {
          limitFiles = Const.toInt(resolve(getNrLimit()), 10);
        }

        if (binaryMode) {
          ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        }

        // Get the files in the list...
        for (FTPFile ftpFile : ftpFiles) {

          if (parentWorkflow.isStopped()) {
            exitaction = true;
            throw new HopException(BaseMessages.getString(PKG, "ActionFTP.WorkflowStopped"));
          }

          if (successConditionBroken) {
            throw new HopException(
                BaseMessages.getString(PKG, "ActionFTP.SuccesConditionBroken", "" + nrErrors));
          }

          boolean getIt = true;

          String filename = ftpFile.getName();
          if (isDebug()) {
            logDebug(BaseMessages.getString(PKG, "ActionFTP.AnalysingFile", filename));
          }

          // We get only files
          if (ftpFile.isDirectory()) {
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
                downloadFile(ftpClient, filename, realMoveToFolder, parentWorkflow, result);
              }
            } catch (Exception e) {
              // Update errors number
              updateErrors();
              logError(BaseMessages.getString(PKG, "ActionFtp.UnexpectedError", e.toString()));
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
      if (ftpClient != null) {
        try {
          ftpClient.quit();
        } catch (Exception e) {
          logError(BaseMessages.getString(PKG, "ActionFTP.ErrorQuitting", e.getMessage()));
        }
      }
      FtpClientUtil.clearSocksJvmSettings();
    }

    result.setNrErrors(nrErrors);
    result.setNrFilesRetrieved(nrFilesRetrieved);
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
      throws HopException {
    try {
      targetFilename = HopVfs.getFilename(HopVfs.getFileObject(returnTargetFilename(filename)));

      if (!onlyGettingNewFiles || needsDownload(targetFilename)) {
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(
                  PKG, "ActionFTP.GettingFile", filename, resolve(targetDirectory)));
        }
        try (OutputStream outputStream = HopVfs.getOutputStream(targetFilename, false)) {
          ftpclient.retrieveFile(filename, outputStream);
        }

        // Update retrieved files
        updateRetrievedFiles();
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionFTP.GotFile", filename));
        }

        // Add filename to result filenames
        addFilenameToResultFilenames(result, parentWorkflow, targetFilename);

        // Delete the file if this is needed!
        if (remove) {
          ftpclient.deleteFile(filename);
          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "ActionFTP.DeletedFile", filename));
          }
        } else {
          if (moveFiles) {
            // Try to move file to destination folder ...
            ftpclient.rename(filename, realMoveToFolder + FILE_SEPARATOR + filename);

            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(PKG, "ActionFTP.MovedFile", filename, realMoveToFolder));
            }
          }
        }
      }
    } catch (Exception e) {
      throw new HopException("Error downloading file", e);
    }
  }

  /**
   * normalize / to \ and remove trailing slashes from a path
   *
   * @param path The path
   * @return normalized path
   */
  public String normalizePath(String path) {

    String normalizedPath = path.replaceAll("\\\\", FILE_SEPARATOR);
    while (normalizedPath.endsWith("\\") || normalizedPath.endsWith(FILE_SEPARATOR)) {
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

  private void displayResults() {
    if (isDetailed()) {
      logDetailed("=======================================");
      logDetailed(BaseMessages.getString(PKG, "ActionFTP.Log.Info.FilesInError", "" + nrErrors));
      logDetailed(
          BaseMessages.getString(PKG, "ActionFTP.Log.Info.FilesRetrieved", "" + nrFilesRetrieved));
      logDetailed("=======================================");
    }
  }

  private boolean getSuccessStatus() {
    return (nrErrors == 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrFilesRetrieved >= limitFiles
            && getSuccessCondition().equals(SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED))
        || (nrErrors <= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS));
  }

  private void updateErrors() {
    nrErrors++;
    if (checkIfSuccessConditionBroken()) {
      // Success condition was broken
      successConditionBroken = true;
    }
  }

  private boolean checkIfSuccessConditionBroken() {
    return (nrErrors > 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || (nrErrors >= limitFiles && getSuccessCondition().equals(SUCCESS_IF_ERRORS_LESS));
  }

  private void updateRetrievedFiles() {
    nrFilesRetrieved++;
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
      String dt = daf.format(now);
      retval += dt;
    } else {
      if (addDate) {
        daf.applyPattern("yyyyMMdd");
        String d = daf.format(now);
        retval += "_" + d;
      }
      if (addTime) {
        daf.applyPattern("HHmmssSSS");
        String t = daf.format(now);
        retval += "_" + t;
      }
    }

    if (isAddDateBeforeExtension()) {
      retval += fileExtension;
    }

    // Add folder name to filename
    retval = resolve(targetDirectory) + Const.FILE_SEPARATOR + retval;
    return retval;
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
   * See if the filename on the FTP server needs downloading. The default is to check the presence
   * of the file in the target directory. If you need other functionality, extend this class and
   * build it into a plugin.
   *
   * @param filename The local filename to check
   * @return true if the file needs downloading
   */
  protected boolean needsDownload(String filename) {
    boolean need = false;

    File file = new File(filename);

    if (!file.exists()) {
      // Local file not exists!
      logDebug(BaseMessages.getString(PKG, "ActionFTP.LocalFileNotExists"), filename);
      return true;
    } else {

      // Local file exists!
      switch (ifFileExistsOperation) {
        case CREATE_UNIQUE -> {
          logDebug(toString(), BaseMessages.getString(PKG, CONST_LOCAL_FILE_EXISTS), filename);

          int stringLength = targetFilename.length();
          int lastIndexOfDot = targetFilename.lastIndexOf('.');
          if (lastIndexOfDot == -1) {
            lastIndexOfDot = stringLength;
          }

          targetFilename =
              targetFilename.substring(0, lastIndexOfDot)
                  + StringUtil.getFormattedDateTimeNow(true)
                  + targetFilename.substring(lastIndexOfDot, stringLength);

          return true;
        }
        case FAIL -> {
          logError(BaseMessages.getString(PKG, CONST_LOCAL_FILE_EXISTS), filename);
          updateErrors();
        }
        case SKIP ->
            logDebug(toString(), BaseMessages.getString(PKG, CONST_LOCAL_FILE_EXISTS), filename);
      }
    }

    return need;
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
      String realServername = resolve(serverName);
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(realServername, ResourceType.SERVER));
      references.add(reference);
    }
    return references;
  }
}
