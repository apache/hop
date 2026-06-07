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

package org.apache.hop.workflow.actions.sftpput;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
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
import org.apache.hop.workflow.actions.sftp.SftpClient;
import org.w3c.dom.Node;

/** This defines an SFTP put action. */
@Action(
    id = "SFTPPUT",
    name = "i18n::ActionSFTPPut.Name",
    description = "i18n::ActionSFTPPut.Description",
    image = "SFTPPut.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileTransfer",
    keywords = "i18n::ActionSftpPut.keyword",
    documentationUrl = "/workflow/actions/sftpput.html")
@Getter
@Setter
public class ActionSftpPut extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionSftpPut.class;
  private static final String CONST_PASSWORD = "password";

  @HopMetadataProperty(key = "servername")
  private String serverName;

  @HopMetadataProperty(key = "serverport")
  private String serverPort;

  @HopMetadataProperty(key = "username")
  private String userName;

  @HopMetadataProperty(key = "password", password = true)
  private String password;

  @HopMetadataProperty(key = "sftpdirectory")
  private String remoteDirectory;

  @HopMetadataProperty(key = "localdirectory")
  private String localDirectory;

  @HopMetadataProperty(key = "wildcard")
  private String wildcard;

  @HopMetadataProperty(key = "copyprevious")
  private boolean copyingPrevious;

  @HopMetadataProperty(key = "copypreviousfiles")
  private boolean copyingPreviousFiles;

  @HopMetadataProperty(key = "addFilenameResut")
  private boolean addFilenameResut;

  @HopMetadataProperty(key = "usekeyfilename")
  private boolean useKeyFilename;

  @HopMetadataProperty(key = "keyfilename")
  private String keyFilename;

  @HopMetadataProperty(key = "keyfilepass", password = true)
  private String keyFilePassword;

  @HopMetadataProperty(key = "compression")
  private String compression;

  @HopMetadataProperty(key = "createRemoteFolder")
  private boolean createRemoteFolder;

  @HopMetadataProperty(key = "aftersftpput", storeWithCode = true)
  private AfterFtpAction afterSftpAction;

  @HopMetadataProperty(key = "preserveTargetFileTimestamp")
  private boolean preserveTargetFileTimestamp;

  @HopMetadataProperty(key = "proxyType")
  private String proxyType;

  @HopMetadataProperty(key = "proxyHost")
  private String proxyHost;

  @HopMetadataProperty(key = "proxyPort")
  private String proxyPort;

  @HopMetadataProperty(key = "proxyUsername")
  private String proxyUsername;

  @HopMetadataProperty(key = "proxyPassword", password = true)
  private String proxyPassword;

  @HopMetadataProperty(key = "destinationfolder")
  private String destinationFolder;

  @HopMetadataProperty(key = "createdestinationfolder")
  private boolean createDestinationFolder;

  @HopMetadataProperty(key = "successWhenNoFile")
  private boolean successWhenNoFile;

  public ActionSftpPut(String n) {
    super(n, "");
    serverPort = "22";
    compression = "none";
    afterSftpAction = AfterFtpAction.NOTHING;
    preserveTargetFileTimestamp = true;
  }

  public ActionSftpPut() {
    this("");
  }

  public ActionSftpPut(ActionSftpPut a) {
    super(a);
    this.serverName = a.serverName;
    this.serverPort = a.serverPort;
    this.userName = a.userName;
    this.password = a.password;
    this.remoteDirectory = a.remoteDirectory;
    this.localDirectory = a.localDirectory;
    this.wildcard = a.wildcard;
    this.copyingPrevious = a.copyingPrevious;
    this.copyingPreviousFiles = a.copyingPreviousFiles;
    this.addFilenameResut = a.addFilenameResut;
    this.useKeyFilename = a.useKeyFilename;
    this.keyFilename = a.keyFilename;
    this.keyFilePassword = a.keyFilePassword;
    this.compression = a.compression;
    this.createRemoteFolder = a.createRemoteFolder;
    this.afterSftpAction = a.afterSftpAction;
    this.preserveTargetFileTimestamp = a.preserveTargetFileTimestamp;
    this.proxyType = a.proxyType;
    this.proxyHost = a.proxyHost;
    this.proxyPort = a.proxyPort;
    this.proxyUsername = a.proxyUsername;
    this.proxyPassword = a.proxyPassword;
    this.destinationFolder = a.destinationFolder;
    this.createDestinationFolder = a.createDestinationFolder;
    this.successWhenNoFile = a.successWhenNoFile;
  }

  @Override
  public void loadXml(Node entryNode, IHopMetadataProvider metadataProvider, IVariables variables)
      throws HopXmlException {
    super.loadXml(entryNode, metadataProvider, variables);

    // A missing/empty <aftersftpput> deserializes the enum to null; default it to NOTHING so the
    // dialog and the execute() switch never hit a NullPointerException on legacy pipelines.
    if (afterSftpAction == null) {
      afterSftpAction = AfterFtpAction.NOTHING;
    }

    // Backward compatibility: pre-2.18 files stored "delete after put" as <remove>Y</remove>
    // instead of <aftersftpput>delete</aftersftpput>. Promote it so those workflows keep deleting.
    if (afterSftpAction == AfterFtpAction.NOTHING
        && "Y".equalsIgnoreCase(XmlHandler.getTagValue(entryNode, "remove"))) {
      afterSftpAction = AfterFtpAction.DELETE;
    }
  }

  @Override
  public Object clone() {
    return new ActionSftpPut(this);
  }

  @Override
  public Result execute(Result result, int nr) throws HopException {
    List<RowMetaAndData> rows = result.getRows();
    result.setResult(false);

    if (isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "ActionSftpPut.Log.StartAction"));
    }
    ArrayList<FileObject> myFileList = new ArrayList<>();

    if (copyingPrevious) {
      if (rows.isEmpty()) {
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionSftpPut.ArgsFromPreviousNothing"));
        }
        result.setResult(true);
        return result;
      }

      try {
        RowMetaAndData resultRow = null;
        // Copy the input row to the (command line) arguments
        for (RowMetaAndData row : rows) {
          resultRow = row;

          // Get file names
          String filePrevious = resultRow.getString(0, null);
          if (!Utils.isEmpty(filePrevious)) {
            FileObject file = HopVfs.getFileObject(filePrevious);
            if (!file.exists()) {
              logError(
                  BaseMessages.getString(
                      PKG, "ActionSftpPut.Log.FilefromPreviousNotFound", filePrevious));
            } else {
              myFileList.add(file);
              if (isDebug()) {
                logDebug(
                    BaseMessages.getString(
                        PKG, "ActionSftpPut.Log.FilenameFromResult", filePrevious));
              }
            }
          }
        }
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "ActionSftpPut.Error.ArgFromPrevious"));
        result.setNrErrors(1);
        return result;
      }
    }

    if (copyingPreviousFiles) {
      List<ResultFile> resultFiles = result.getResultFilesList();
      if (Utils.isEmpty(resultFiles)) {
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionSftpPut.ArgsFromPreviousNothingFiles"));
        }
        result.setResult(true);
        return result;
      }

      try {
        for (Iterator<ResultFile> it = resultFiles.iterator();
            it.hasNext() && !parentWorkflow.isStopped(); ) {
          ResultFile resultFile = it.next();
          FileObject file = resultFile.getFile();
          if (file != null) {
            if (!file.exists()) {
              logError(
                  BaseMessages.getString(
                      PKG, "ActionSftpPut.Log.FilefromPreviousNotFound", file.toString()));
            } else {
              myFileList.add(file);
              if (isDebug()) {
                logDebug(
                    BaseMessages.getString(
                        PKG, "ActionSftpPut.Log.FilenameFromResult", file.toString()));
              }
            }
          }
        }
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "ActionSftpPut.Error.ArgFromPrevious"));
        result.setNrErrors(1);
        return result;
      }
    }

    SftpClient sftpclient = null;

    // String substitution..
    String realServerName = resolve(serverName);
    String realServerPort = resolve(serverPort);
    String realUsername = resolve(userName);
    String realPassword = Encr.decryptPasswordOptionallyEncrypted(resolve(password));
    String realSftpDirString = resolve(remoteDirectory);
    String realWildcard = resolve(wildcard);
    String realLocalDirectory = resolve(localDirectory);
    String realKeyFilename = null;
    String realPassPhrase = null;
    // Destination folder (Move to)
    String realDestinationFolder = resolve(getDestinationFolder());

    try {
      // Let's perform some checks before starting

      if (afterSftpAction == AfterFtpAction.MOVE) {
        if (Utils.isEmpty(realDestinationFolder)) {
          logError(BaseMessages.getString(PKG, "ActionSSH2PUT.Log.DestinatFolderMissing"));
          result.setNrErrors(1);
          return result;
        } else {
          try (FileObject folder = HopVfs.getFileObject(realDestinationFolder)) {
            // Let's check if folder exists...
            if (!folder.exists()) {
              // Do we need to create it?
              if (createDestinationFolder) {
                folder.createFolder();
              } else {
                logError(
                    BaseMessages.getString(
                        PKG, "ActionSSH2PUT.Log.DestinatFolderNotExist", realDestinationFolder));
                result.setNrErrors(1);
                return result;
              }
            }
            realDestinationFolder = HopVfs.getFilename(folder);
          } catch (Exception e) {
            throw new HopException(e);
          }
        }
      }

      if (isUseKeyFilename()) {
        // We must have here a private keyfilename
        realKeyFilename = resolve(getKeyFilename());
        if (Utils.isEmpty(realKeyFilename)) {
          // Error..Missing keyfile
          logError(BaseMessages.getString(PKG, "ActionSftp.Error.KeyFileMissing"));
          result.setNrErrors(1);
          return result;
        }
        if (!HopVfs.fileExists(realKeyFilename)) {
          // Error.. can not reach keyfile
          logError(BaseMessages.getString(PKG, "ActionSftp.Error.KeyFileNotFound"));
          result.setNrErrors(1);
          return result;
        }
        realPassPhrase = resolve(getKeyFilePassword());
      }

      // Create sftp client to host ...
      sftpclient =
          new SftpClient(
              InetAddress.getByName(realServerName),
              Const.toInt(realServerPort, 22),
              realUsername,
              realKeyFilename,
              realPassPhrase);
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ActionSftpPut.Log.OpenedConnection",
                realServerName,
                "" + realServerPort,
                realUsername));
      }

      // Set compression
      sftpclient.setCompression(getCompression());

      // Set proxy?
      String realProxyHost = resolve(getProxyHost());
      if (!Utils.isEmpty(realProxyHost)) {
        // Set proxy
        sftpclient.setProxy(
            realProxyHost,
            resolve(getProxyPort()),
            resolve(getProxyUsername()),
            resolve(getProxyPassword()),
            getProxyType());
      }

      // login to ftp host ...
      sftpclient.login(realPassword);
      // Don't show the password in the logs, it's not good for security audits
      // logDetailed("logged in using password "+realPassword); // Logging this seems a bad idea! Oh
      // well.

      // move to spool dir ...
      if (!Utils.isEmpty(realSftpDirString)) {
        boolean folderExists = sftpclient.folderExists(realSftpDirString);
        if (!folderExists) {
          if (!isCreateRemoteFolder()) {
            throw new HopException(
                BaseMessages.getString(
                    PKG, "ActionSftpPut.Error.CanNotFindRemoteFolder", realSftpDirString));
          }
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionSftpPut.Error.CanNotFindRemoteFolder", realSftpDirString));
          }

          // Let's create folder
          sftpclient.createFolder(realSftpDirString);
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionSftpPut.Log.RemoteFolderCreated", realSftpDirString));
          }
        }
        sftpclient.chdir(realSftpDirString);
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "ActionSftpPut.Log.ChangedDirectory", realSftpDirString));
        }
      } // end if

      if (!copyingPrevious && !copyingPreviousFiles) {
        // Get all the files in the local directory...
        myFileList = new ArrayList<>();

        FileObject localFiles = HopVfs.getFileObject(realLocalDirectory);
        FileObject[] children = localFiles.getChildren();
        if (children != null) {
          for (FileObject child : children) {
            // Get filename of file or directory
            if (child.getType().equals(FileType.FILE)) {
              myFileList.add(child);
            }
          } // end for
        }
      }

      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG, "ActionSftpPut.Log.RowsFromPreviousResult", myFileList.size()));
      }

      Pattern pattern = null;
      if (!copyingPrevious && !copyingPreviousFiles && !Utils.isEmpty(realWildcard)) {
        pattern = Pattern.compile(realWildcard);
      }

      int nrFilesSent = 0;
      int nrFilesMatched = 0;

      // Get the files in the list and execute sftp.put() for each file
      Iterator<FileObject> it = myFileList.iterator();
      while (it.hasNext() && !parentWorkflow.isStopped()) {
        FileObject myFile = it.next();
        try {
          String localFilename = myFile.toString();
          String destinationFilename = myFile.getName().getBaseName();
          boolean getIt = true;

          // First see if the file matches the regular expression!
          if (pattern != null) {
            Matcher matcher = pattern.matcher(destinationFilename);
            getIt = matcher.matches();
          }

          if (getIt) {
            nrFilesMatched++;

            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "ActionSftpPut.Log.PuttingFile", localFilename, realSftpDirString));
            }

            sftpclient.put(myFile, destinationFilename, preserveTargetFileTimestamp);
            nrFilesSent++;

            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(PKG, "ActionSftpPut.Log.TransferredFile", localFilename));
            }

            // We successfully uploaded the file
            // what's next ...
            switch (afterSftpAction) {
              case DELETE:
                myFile.delete();
                if (isDetailed()) {
                  logDetailed(
                      BaseMessages.getString(PKG, "ActionSftpPut.Log.DeletedFile", localFilename));
                }
                break;
              case MOVE:
                FileObject destination = null;
                try {
                  destination =
                      HopVfs.getFileObject(
                          realDestinationFolder
                              + Const.FILE_SEPARATOR
                              + myFile.getName().getBaseName());
                  myFile.moveTo(destination);
                  if (isDetailed()) {
                    logDetailed(
                        BaseMessages.getString(
                            PKG, "ActionSftpPut.Log.FileMoved", myFile, destination));
                  }
                } finally {
                  if (destination != null) {
                    destination.close();
                  }
                }
                break;
              default:
                if (addFilenameResut) {
                  // Add to the result files...
                  ResultFile resultFile =
                      new ResultFile(
                          ResultFile.FILE_TYPE_GENERAL,
                          myFile,
                          parentWorkflow.getWorkflowName(),
                          toString());
                  result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
                  if (isDetailed()) {
                    logDetailed(
                        BaseMessages.getString(
                            PKG,
                            "ActionSftpPut.Log.FilenameAddedToResultFilenames",
                            localFilename));
                  }
                }
                break;
            }
          }
        } finally {
          if (myFile != null) {
            myFile.close();
          }
        }
      } // end for

      result.setResult(true);

      // Abuse the nr of files retrieved vector in the results
      //
      result.setNrFilesRetrieved(nrFilesSent);

      // If we didn't upload any files, say something about it...
      //
      if (nrFilesMatched == 0) {
        if (isSuccessWhenNoFile()) {
          // Just warn user
          if (isBasic()) {
            logBasic(BaseMessages.getString(PKG, "ActionSftpPut.Error.NoFileToSend"));
          }
        } else {
          // Fail
          logError(BaseMessages.getString(PKG, "ActionSftpPut.Error.NoFileToSend"));
          result.setNrErrors(1);
          result.setResult(false);
          return result;
        }
      }

    } catch (Exception e) {
      result.setNrErrors(1);
      logError(BaseMessages.getString(PKG, "ActionSftpPut.Exception", e.getMessage()));
      logError(Const.getStackTracker(e));
    } finally {
      // close connection, if possible
      try {
        if (sftpclient != null) {
          sftpclient.disconnect();
        }
      } catch (Exception e) {
        // just ignore this, makes no big difference
      }
    }

    return result;
  } // JKU: end function execute()

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
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
            "localDirectory",
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

  @Getter
  public enum AfterFtpAction implements IEnumHasCodeAndDescription {
    NOTHING("nothing", BaseMessages.getString(PKG, "ActionSftpPut.AfterSFTP.DoNothing.Label")),
    DELETE("delete", BaseMessages.getString(PKG, "ActionSftpPut.AfterSFTP.Delete.Label")),
    MOVE("move", BaseMessages.getString(PKG, "ActionSftpPut.AfterSFTP.Move.Label")),
    ;
    final String code;
    final String description;

    private AfterFtpAction(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public static String[] getDescriptions() {
      return IEnumHasCodeAndDescription.getDescriptions(AfterFtpAction.class);
    }

    public static AfterFtpAction lookupDescription(String description) {
      return IEnumHasCodeAndDescription.lookupDescription(
          AfterFtpAction.class, description, NOTHING);
    }
  }
}
