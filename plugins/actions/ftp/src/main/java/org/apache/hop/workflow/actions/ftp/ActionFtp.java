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
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
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
import org.apache.hop.workflow.actions.util.FtpClientUtil;
import org.apache.hop.workflow.actions.util.IFtpConnection;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.w3c.dom.Node;

import java.io.File;
import java.io.OutputStream;
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
public class ActionFtp extends ActionBase implements Cloneable, IAction, IFtpConnection {
  private static final Class<?> PKG = ActionFtp.class; // For Translator

  private String serverName;

  private String userName;

  private String password;

  private String remoteDirectory;

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

  private boolean moveFiles;

  private String moveToDirectory;

  private boolean addDate;

  private boolean addTime;

  private boolean specifyFormat;

  private String dateTimeFormat;

  private boolean addDateBeforeExtension;

  private boolean isAddResult;

  private boolean createMoveFolder;

  private String serverPort;

  private String proxyHost;

  private String proxyPort; /* string to allow variable substitution */

  private String proxyUsername;

  private String proxyPassword;

  private String socksProxyHost;

  private String socksProxyPort;

  private String socksProxyUsername;

  private String socksProxyPassword;

  public int ifFileExistsSkip = 0;

  public static final String STRING_IF_FILE_EXISTS_SKIP = "ifFileExistsSkip";

  public int ifFileExistsCreateUniq = 1;

  public static final String STRING_IF_FILE_EXISTS_CREATE_UNIQ = "ifFileExistsCreateUniq";

  public int ifFileExistsFail = 2;

  public static final String STRING_IF_FILE_EXISTS_FAIL = "ifFileExistsFail";

  public int ifFileExists;

  public String stringIfFileExists;

  public String SUCCESS_IF_AT_LEAST_X_FILES_DOWNLOADED = "success_when_at_least";

  public String SUCCESS_IF_ERRORS_LESS = "success_if_errors_less";

  public String SUCCESS_IF_NO_ERRORS = "success_if_no_errors";

  private String nrLimit;

  private String successCondition;

  long nrErrors = 0;

  long nrFilesRetrieved = 0;

  boolean successConditionBroken = false;

  int limitFiles = 0;

  String targetFilename = null;

  static String FILE_SEPARATOR = "/";

  public ActionFtp(String n) {
    super(n, "");
    nrLimit = "10";
    serverPort = "21";
    socksProxyPort = "1080";
    successCondition = SUCCESS_IF_NO_ERRORS;
    ifFileExists = ifFileExistsSkip;
    stringIfFileExists = STRING_IF_FILE_EXISTS_SKIP;

    serverName = null;
    moveFiles = false;
    moveToDirectory = null;
    addDate = false;
    addTime = false;
    specifyFormat = false;
    addDateBeforeExtension = false;
    isAddResult = true;
    createMoveFolder = false;

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
    StringBuilder xml = new StringBuilder(650); // 528 chars in spaces and tags alone

    xml.append(super.getXml());
    xml.append("      ").append(XmlHandler.addTagValue("port", serverPort ));
    xml.append("      ").append(XmlHandler.addTagValue("servername", serverName));
    xml.append("      ").append(XmlHandler.addTagValue("username", userName));
    xml
        .append("      ")
        .append(
            XmlHandler.addTagValue("password", Encr.encryptPasswordIfNotUsingVariables(password)));
    xml.append("      ").append(XmlHandler.addTagValue("ftpdirectory", remoteDirectory ));
    xml.append("      ").append(XmlHandler.addTagValue("targetdirectory", targetDirectory));
    xml.append("      ").append(XmlHandler.addTagValue("wildcard", wildcard));
    xml.append("      ").append(XmlHandler.addTagValue("binary", binaryMode));
    xml.append("      ").append(XmlHandler.addTagValue("timeout", timeout));
    xml.append("      ").append(XmlHandler.addTagValue("remove", remove));
    xml.append("      ").append(XmlHandler.addTagValue("only_new", onlyGettingNewFiles));
    xml.append("      ").append(XmlHandler.addTagValue("active", activeConnection));
    xml.append("      ").append(XmlHandler.addTagValue("control_encoding", controlEncoding));
    xml.append("      ").append(XmlHandler.addTagValue("movefiles", moveFiles ));
    xml.append("      ").append(XmlHandler.addTagValue("movetodirectory", moveToDirectory ));

    xml.append("      ").append(XmlHandler.addTagValue("adddate", addDate ));
    xml.append("      ").append(XmlHandler.addTagValue("addtime", addTime ));
    xml.append("      ").append(XmlHandler.addTagValue("SpecifyFormat", specifyFormat));
    xml.append("      ").append(XmlHandler.addTagValue("date_time_format", dateTimeFormat));
    xml
        .append("      ")
        .append(XmlHandler.addTagValue("AddDateBeforeExtension", addDateBeforeExtension));
    xml.append("      ").append(XmlHandler.addTagValue("isaddresult", isAddResult ));
    xml.append("      ").append(XmlHandler.addTagValue("createmovefolder", createMoveFolder ));

    xml.append("      ").append(XmlHandler.addTagValue("proxy_host", proxyHost));
    xml.append("      ").append(XmlHandler.addTagValue("proxy_port", proxyPort));
    xml.append("      ").append(XmlHandler.addTagValue("proxy_username", proxyUsername));
    xml
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "proxy_password", Encr.encryptPasswordIfNotUsingVariables(proxyPassword)));
    xml.append("      ").append(XmlHandler.addTagValue("socksproxy_host", socksProxyHost));
    xml.append("      ").append(XmlHandler.addTagValue("socksproxy_port", socksProxyPort));
    xml
        .append("      ")
        .append(XmlHandler.addTagValue("socksproxy_username", socksProxyUsername));
    xml
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "socksproxy_password",
                Encr.encryptPasswordIfNotUsingVariables(socksProxyPassword)));

    xml.append("      ").append(XmlHandler.addTagValue("ifFileExists", stringIfFileExists ));

    xml.append("      ").append(XmlHandler.addTagValue("nr_limit", nrLimit));
    xml.append("      ").append(XmlHandler.addTagValue("success_condition", successCondition));

    return xml.toString();
  }

  @Override
  public void loadXml(Node entryNode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    try {
      super.loadXml(entryNode);
      serverPort = XmlHandler.getTagValue(entryNode, "port");
      serverName = XmlHandler.getTagValue(entryNode, "servername");
      userName = XmlHandler.getTagValue(entryNode, "username");
      password =
          Encr.decryptPasswordOptionallyEncrypted(XmlHandler.getTagValue(entryNode, "password"));
      remoteDirectory = XmlHandler.getTagValue(entryNode, "ftpdirectory");
      targetDirectory = XmlHandler.getTagValue(entryNode, "targetdirectory");
      wildcard = XmlHandler.getTagValue(entryNode, "wildcard");
      binaryMode = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, "binary"));
      timeout = Const.toInt(XmlHandler.getTagValue(entryNode, "timeout"), 10000);
      remove = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, "remove"));
      onlyGettingNewFiles = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, "only_new"));
      activeConnection = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, "active"));
      controlEncoding = XmlHandler.getTagValue(entryNode, "control_encoding");
      if (controlEncoding == null) {
        // if we couldn't retrieve an encoding, assume it's an old instance and
        // put in the the encoding used before v 2.4.0
        controlEncoding = LEGACY_CONTROL_ENCODING;
      }
      moveFiles = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, "movefiles"));
      moveToDirectory = XmlHandler.getTagValue(entryNode, "movetodirectory");

      addDate = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, "adddate"));
      addTime = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, "addtime"));
      specifyFormat = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, "SpecifyFormat"));
      dateTimeFormat = XmlHandler.getTagValue(entryNode, "date_time_format");
      addDateBeforeExtension =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, "AddDateBeforeExtension"));

      String addresult = XmlHandler.getTagValue(entryNode, "isaddresult");

      if (Utils.isEmpty(addresult)) {
        isAddResult = true;
      } else {
        isAddResult = "Y".equalsIgnoreCase(addresult);
      }

      createMoveFolder =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, "createmovefolder"));

      proxyHost = XmlHandler.getTagValue(entryNode, "proxy_host");
      proxyPort = XmlHandler.getTagValue(entryNode, "proxy_port");
      proxyUsername = XmlHandler.getTagValue(entryNode, "proxy_username");
      proxyPassword =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(entryNode, "proxy_password"));
      socksProxyHost = XmlHandler.getTagValue(entryNode, "socksproxy_host");
      socksProxyPort = XmlHandler.getTagValue(entryNode, "socksproxy_port");
      socksProxyUsername = XmlHandler.getTagValue(entryNode, "socksproxy_username");
      socksProxyPassword =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(entryNode, "socksproxy_password"));
      stringIfFileExists = XmlHandler.getTagValue(entryNode, "ifFileExists");
      if (Utils.isEmpty( stringIfFileExists )) {
        ifFileExists = ifFileExistsSkip;
      } else {
        if ( stringIfFileExists.equals( STRING_IF_FILE_EXISTS_CREATE_UNIQ )) {
          ifFileExists = ifFileExistsCreateUniq;
        } else if ( stringIfFileExists.equals( STRING_IF_FILE_EXISTS_FAIL )) {
          ifFileExists = ifFileExistsFail;
        } else {
          ifFileExists = ifFileExistsSkip;
        }
      }
      nrLimit = XmlHandler.getTagValue(entryNode, "nr_limit");
      successCondition =
          Const.NVL(XmlHandler.getTagValue(entryNode, "success_condition"), SUCCESS_IF_NO_ERRORS);

    } catch (HopXmlException xe) {
      throw new HopXmlException("Unable to load action of type 'ftp' from XML node", xe);
    }
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
    nrFilesRetrieved = 0;
    successConditionBroken = false;
    boolean exitaction = false;
    limitFiles = Const.toInt(resolve(getNrLimit()), 10);

    // Here let's put some controls before stating the workflow
    if ( moveFiles ) {
      if (Utils.isEmpty( moveToDirectory )) {
        logError(BaseMessages.getString(PKG, "ActionFTP.MoveToFolderEmpty"));
        return result;
      }
    }

    if (isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "ActionFTP.Start"));
    }

    FTPClient ftpClient = null;
    String realMoveToFolder = null;

    try {
      ftpClient = FtpClientUtil.connectAndLogin( log, this, this, getName() );

      // move to spool dir ...
      if (!Utils.isEmpty( remoteDirectory )) {
        String realFtpDirectory = resolve( remoteDirectory );
        realFtpDirectory = normalizePath(realFtpDirectory);
        ftpClient.changeWorkingDirectory(realFtpDirectory);
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionFTP.ChangedDir", realFtpDirectory));
        }
      }

      // Create move to folder if necessary
      if ( moveFiles && !Utils.isEmpty( moveToDirectory )) {
        realMoveToFolder = resolve( moveToDirectory );
        realMoveToFolder = normalizePath(realMoveToFolder);
        // Folder exists?
        boolean folderExist = true;
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionFTP.CheckMoveToFolder", realMoveToFolder));
        }
        String originalLocation = ftpClient.printWorkingDirectory();
        try {
          // does not work for folders, see PDI-2567:
          // folderExist=ftpClient.exists(realMoveToFolder);
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
          if ( createMoveFolder ) {
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
          if (!Utils.isEmpty(translatedWildcard)) {
            if (ftpFiles[0].getName().startsWith(translatedWildcard)) {
              throw new HopException(ftpFiles[0].getName());
            }
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

        // Get the files in the list...
        for (FTPFile ftpFile : ftpFiles) {

          if (parentWorkflow.isStopped()) {
            exitaction = true;
            throw new Exception(BaseMessages.getString(PKG, "ActionFTP.WorkflowStopped"));
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
    result.setNrFilesRetrieved( nrFilesRetrieved );
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
                PKG, "ActionFTP.GettingFile", filename, resolve(targetDirectory)));
      }
      try ( OutputStream outputStream = HopVfs.getOutputStream(targetFilename, false)) {
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
          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "ActionFTP.DeletedFile", filename));
          }
        }
      } else {
        if ( moveFiles ) {
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
    if ( isAddResult ) {
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
          BaseMessages.getString(PKG, "ActionFTP.Log.Info.FilesRetrieved", "" + nrFilesRetrieved ));
      logDetailed("=======================================");
    }
  }

  private boolean getSuccessStatus() {
    boolean retval = false;

    if ((nrErrors == 0 && getSuccessCondition().equals(SUCCESS_IF_NO_ERRORS))
        || ( nrFilesRetrieved >= limitFiles
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
      if ( addDate ) {
        daf.applyPattern("yyyyMMdd");
        String d = daf.format(now);
        retval += "_" + d;
      }
      if ( addTime ) {
        daf.applyPattern("HHmmssSSS");
        String t = daf.format(now);
        retval += "_" + t;
      }
    }

    if (isAddDateBeforeExtension()) {
      retval += fileExtension;
    }

    // Add foldername to filename
    retval = resolve(targetDirectory) + Const.FILE_SEPARATOR + retval;
    return retval;
  }

  @Override public boolean isEvaluation() {
    return true;
  }

  @Override public boolean isUnconditional() {
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

  @Override public List<ResourceReference> getResourceDependencies( IVariables variables, WorkflowMeta workflowMeta ) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    if (!Utils.isEmpty(serverName)) {
      String realServername = resolve(serverName);
      ResourceReference reference = new ResourceReference(this);
      reference.getEntries().add(new ResourceEntry(realServername, ResourceType.SERVER));
      references.add(reference);
    }
    return references;
  }



  /**
   * Gets serverName
   *
   * @return value of serverName
   */
  public String getServerName() {
    return serverName;
  }

  /**
   * @param serverName The serverName to set
   */
  public void setServerName( String serverName ) {
    this.serverName = serverName;
  }

  /**
   * Gets userName
   *
   * @return value of userName
   */
  public String getUserName() {
    return userName;
  }

  /**
   * @param userName The userName to set
   */
  public void setUserName( String userName ) {
    this.userName = userName;
  }

  /**
   * Gets password
   *
   * @return value of password
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password The password to set
   */
  public void setPassword( String password ) {
    this.password = password;
  }

  /**
   * Gets ftpDirectory
   *
   * @return value of ftpDirectory
   */
  public String getRemoteDirectory() {
    return remoteDirectory;
  }

  /**
   * @param remoteDirectory The ftpDirectory to set
   */
  public void setRemoteDirectory( String remoteDirectory ) {
    this.remoteDirectory = remoteDirectory;
  }

  /**
   * Gets targetDirectory
   *
   * @return value of targetDirectory
   */
  public String getTargetDirectory() {
    return targetDirectory;
  }

  /**
   * @param targetDirectory The targetDirectory to set
   */
  public void setTargetDirectory( String targetDirectory ) {
    this.targetDirectory = targetDirectory;
  }

  /**
   * Gets wildcard
   *
   * @return value of wildcard
   */
  public String getWildcard() {
    return wildcard;
  }

  /**
   * @param wildcard The wildcard to set
   */
  public void setWildcard( String wildcard ) {
    this.wildcard = wildcard;
  }

  /**
   * Gets binaryMode
   *
   * @return value of binaryMode
   */
  public boolean isBinaryMode() {
    return binaryMode;
  }

  /**
   * @param binaryMode The binaryMode to set
   */
  public void setBinaryMode( boolean binaryMode ) {
    this.binaryMode = binaryMode;
  }

  /**
   * Gets timeout
   *
   * @return value of timeout
   */
  public int getTimeout() {
    return timeout;
  }

  /**
   * @param timeout The timeout to set
   */
  public void setTimeout( int timeout ) {
    this.timeout = timeout;
  }

  /**
   * Gets remove
   *
   * @return value of remove
   */
  public boolean isRemove() {
    return remove;
  }

  /**
   * @param remove The remove to set
   */
  public void setRemove( boolean remove ) {
    this.remove = remove;
  }

  /**
   * Gets onlyGettingNewFiles
   *
   * @return value of onlyGettingNewFiles
   */
  public boolean isOnlyGettingNewFiles() {
    return onlyGettingNewFiles;
  }

  /**
   * @param onlyGettingNewFiles The onlyGettingNewFiles to set
   */
  public void setOnlyGettingNewFiles( boolean onlyGettingNewFiles ) {
    this.onlyGettingNewFiles = onlyGettingNewFiles;
  }

  /**
   * Gets controlEncoding
   *
   * @return value of controlEncoding
   */
  public String getControlEncoding() {
    return controlEncoding;
  }

  /**
   * @param controlEncoding The controlEncoding to set
   */
  public void setControlEncoding( String controlEncoding ) {
    this.controlEncoding = controlEncoding;
  }

  /**
   * Gets movefiles
   *
   * @return value of movefiles
   */
  public boolean isMoveFiles() {
    return moveFiles;
  }

  /**
   * @param moveFiles The movefiles to set
   */
  public void setMoveFiles( boolean moveFiles ) {
    this.moveFiles = moveFiles;
  }

  /**
   * Gets movetodirectory
   *
   * @return value of movetodirectory
   */
  public String getMoveToDirectory() {
    return moveToDirectory;
  }

  /**
   * @param moveToDirectory The movetodirectory to set
   */
  public void setMoveToDirectory( String moveToDirectory ) {
    this.moveToDirectory = moveToDirectory;
  }

  /**
   * Gets adddate
   *
   * @return value of adddate
   */
  public boolean isAddDate() {
    return addDate;
  }

  /**
   * @param addDate The adddate to set
   */
  public void setAddDate( boolean addDate ) {
    this.addDate = addDate;
  }

  /**
   * Gets addtime
   *
   * @return value of addtime
   */
  public boolean isAddTime() {
    return addTime;
  }

  /**
   * @param addTime The addtime to set
   */
  public void setAddTime( boolean addTime ) {
    this.addTime = addTime;
  }

  /**
   * Gets specifyFormat
   *
   * @return value of specifyFormat
   */
  public boolean isSpecifyFormat() {
    return specifyFormat;
  }

  /**
   * @param specifyFormat The specifyFormat to set
   */
  public void setSpecifyFormat( boolean specifyFormat ) {
    this.specifyFormat = specifyFormat;
  }

  /**
   * Gets dateTimeFormat
   *
   * @return value of dateTimeFormat
   */
  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  /**
   * @param dateTimeFormat The dateTimeFormat to set
   */
  public void setDateTimeFormat( String dateTimeFormat ) {
    this.dateTimeFormat = dateTimeFormat;
  }

  /**
   * Gets addDateBeforeExtension
   *
   * @return value of addDateBeforeExtension
   */
  public boolean isAddDateBeforeExtension() {
    return addDateBeforeExtension;
  }

  /**
   * @param addDateBeforeExtension The addDateBeforeExtension to set
   */
  public void setAddDateBeforeExtension( boolean addDateBeforeExtension ) {
    this.addDateBeforeExtension = addDateBeforeExtension;
  }

  /**
   * Gets isaddresult
   *
   * @return value of isaddresult
   */
  public boolean isAddResult() {
    return isAddResult;
  }

  /**
   * @param addResult The isaddresult to set
   */
  public void setAddResult( boolean addResult ) {
    this.isAddResult = addResult;
  }

  /**
   * Gets createmovefolder
   *
   * @return value of createmovefolder
   */
  public boolean isCreateMoveFolder() {
    return createMoveFolder;
  }

  /**
   * @param createMoveFolder The createmovefolder to set
   */
  public void setCreateMoveFolder( boolean createMoveFolder ) {
    this.createMoveFolder = createMoveFolder;
  }

  /**
   * Gets port
   *
   * @return value of port
   */
  public String getServerPort() {
    return serverPort;
  }

  /**
   * @param serverPort The port to set
   */
  public void setServerPort( String serverPort ) {
    this.serverPort = serverPort;
  }

  /**
   * Gets proxyHost
   *
   * @return value of proxyHost
   */
  public String getProxyHost() {
    return proxyHost;
  }

  /**
   * @param proxyHost The proxyHost to set
   */
  public void setProxyHost( String proxyHost ) {
    this.proxyHost = proxyHost;
  }

  /**
   * Gets proxyPort
   *
   * @return value of proxyPort
   */
  public String getProxyPort() {
    return proxyPort;
  }

  /**
   * @param proxyPort The proxyPort to set
   */
  public void setProxyPort( String proxyPort ) {
    this.proxyPort = proxyPort;
  }

  /**
   * Gets proxyUsername
   *
   * @return value of proxyUsername
   */
  public String getProxyUsername() {
    return proxyUsername;
  }

  /**
   * @param proxyUsername The proxyUsername to set
   */
  public void setProxyUsername( String proxyUsername ) {
    this.proxyUsername = proxyUsername;
  }

  /**
   * Gets proxyPassword
   *
   * @return value of proxyPassword
   */
  public String getProxyPassword() {
    return proxyPassword;
  }

  /**
   * @param proxyPassword The proxyPassword to set
   */
  public void setProxyPassword( String proxyPassword ) {
    this.proxyPassword = proxyPassword;
  }

  /**
   * Gets socksProxyHost
   *
   * @return value of socksProxyHost
   */
  public String getSocksProxyHost() {
    return socksProxyHost;
  }

  /**
   * @param socksProxyHost The socksProxyHost to set
   */
  public void setSocksProxyHost( String socksProxyHost ) {
    this.socksProxyHost = socksProxyHost;
  }

  /**
   * Gets socksProxyPort
   *
   * @return value of socksProxyPort
   */
  public String getSocksProxyPort() {
    return socksProxyPort;
  }

  /**
   * @param socksProxyPort The socksProxyPort to set
   */
  public void setSocksProxyPort( String socksProxyPort ) {
    this.socksProxyPort = socksProxyPort;
  }

  /**
   * Gets socksProxyUsername
   *
   * @return value of socksProxyUsername
   */
  public String getSocksProxyUsername() {
    return socksProxyUsername;
  }

  /**
   * @param socksProxyUsername The socksProxyUsername to set
   */
  public void setSocksProxyUsername( String socksProxyUsername ) {
    this.socksProxyUsername = socksProxyUsername;
  }

  /**
   * Gets socksProxyPassword
   *
   * @return value of socksProxyPassword
   */
  public String getSocksProxyPassword() {
    return socksProxyPassword;
  }

  /**
   * @param socksProxyPassword The socksProxyPassword to set
   */
  public void setSocksProxyPassword( String socksProxyPassword ) {
    this.socksProxyPassword = socksProxyPassword;
  }

  /**
   * Gets ifFileExistsSkip
   *
   * @return value of ifFileExistsSkip
   */
  public int getIfFileExistsSkip() {
    return ifFileExistsSkip;
  }

  /**
   * @param ifFileExistsSkip The ifFileExistsSkip to set
   */
  public void setIfFileExistsSkip( int ifFileExistsSkip ) {
    this.ifFileExistsSkip = ifFileExistsSkip;
  }

  /**
   * Gets ifFileExistsCreateUniq
   *
   * @return value of ifFileExistsCreateUniq
   */
  public int getIfFileExistsCreateUniq() {
    return ifFileExistsCreateUniq;
  }

  /**
   * @param ifFileExistsCreateUniq The ifFileExistsCreateUniq to set
   */
  public void setIfFileExistsCreateUniq( int ifFileExistsCreateUniq ) {
    this.ifFileExistsCreateUniq = ifFileExistsCreateUniq;
  }

  /**
   * Gets ifFileExistsFail
   *
   * @return value of ifFileExistsFail
   */
  public int getIfFileExistsFail() {
    return ifFileExistsFail;
  }

  /**
   * @param ifFileExistsFail The ifFileExistsFail to set
   */
  public void setIfFileExistsFail( int ifFileExistsFail ) {
    this.ifFileExistsFail = ifFileExistsFail;
  }

  /**
   * Gets ifFileExists
   *
   * @return value of ifFileExists
   */
  public int getIfFileExists() {
    return ifFileExists;
  }

  /**
   * @param ifFileExists The ifFileExists to set
   */
  public void setIfFileExists( int ifFileExists ) {
    this.ifFileExists = ifFileExists;
  }

  /**
   * Gets SifFileExists
   *
   * @return value of SifFileExists
   */
  public String getStringIfFileExists() {
    return stringIfFileExists;
  }

  /**
   * @param stringIfFileExists The stringIfFileExists to set
   */
  public void setStringIfFileExists( String stringIfFileExists ) {
    this.stringIfFileExists = stringIfFileExists;
  }

  /**
   * Gets nrLimit
   *
   * @return value of nrLimit
   */
  public String getNrLimit() {
    return nrLimit;
  }

  /**
   * @param nrLimit The nrLimit to set
   */
  public void setNrLimit( String nrLimit ) {
    this.nrLimit = nrLimit;
  }

  /**
   * Gets successCondition
   *
   * @return value of successCondition
   */
  public String getSuccessCondition() {
    return successCondition;
  }

  /**
   * @param successCondition The successCondition to set
   */
  public void setSuccessCondition( String successCondition ) {
    this.successCondition = successCondition;
  }

  /**
   * Gets nrErrors
   *
   * @return value of nrErrors
   */
  public long getNrErrors() {
    return nrErrors;
  }

  /**
   * @param nrErrors The nrErrors to set
   */
  public void setNrErrors( long nrErrors ) {
    this.nrErrors = nrErrors;
  }

  /**
   * Gets nrFilesRetrieved
   *
   * @return value of nrFilesRetrieved
   */
  public long getNrFilesRetrieved() {
    return nrFilesRetrieved;
  }

  /**
   * @param nrFilesRetrieved The nrFilesRetrieved to set
   */
  public void setNrFilesRetrieved( long nrFilesRetrieved ) {
    this.nrFilesRetrieved = nrFilesRetrieved;
  }

  /**
   * Gets successConditionBroken
   *
   * @return value of successConditionBroken
   */
  public boolean isSuccessConditionBroken() {
    return successConditionBroken;
  }

  /**
   * @param successConditionBroken The successConditionBroken to set
   */
  public void setSuccessConditionBroken( boolean successConditionBroken ) {
    this.successConditionBroken = successConditionBroken;
  }

  /**
   * Gets limitFiles
   *
   * @return value of limitFiles
   */
  public int getLimitFiles() {
    return limitFiles;
  }

  /**
   * @param limitFiles The limitFiles to set
   */
  public void setLimitFiles( int limitFiles ) {
    this.limitFiles = limitFiles;
  }

  /**
   * Gets targetFilename
   *
   * @return value of targetFilename
   */
  public String getTargetFilename() {
    return targetFilename;
  }

  /**
   * @param targetFilename The targetFilename to set
   */
  public void setTargetFilename( String targetFilename ) {
    this.targetFilename = targetFilename;
  }
}
