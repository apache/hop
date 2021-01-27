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
 *
 */

package org.apache.hop.workflow.actions.sftpput;

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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This defines an SFTP put action.
 *
 * @author Matt
 * @since 05-11-2003
 */
@Action(
    id = "SFTPPUT",
    name = "i18n::ActionSFTPPut.Name",
    description = "i18n::ActionSFTPPut.Description",
    image = "SFTPPut.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileTransfer",
    documentationUrl = "https://hop.apache.org/manual/latest/plugins/actions/sftpput.html")
public class ActionSftpPut extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionSftpPut.class; // For Translator

  private int afterFtps;

  public static final String[] afterFtpsDesc =
      new String[] {
        BaseMessages.getString(PKG, "ActionSftpPut.AfterSFTP.DoNothing.Label"),
        BaseMessages.getString(PKG, "ActionSftpPut.AfterSFTP.Delete.Label"),
        BaseMessages.getString(PKG, "ActionSftpPut.AfterSFTP.Move.Label"),
      };
  public static final String[] afterFtpsCode = new String[] {"nothing", "delete", "move"};

  public static final int AFTER_FTPSPUT_NOTHING = 0;
  public static final int AFTER_FTPSPUT_DELETE = 1;
  public static final int AFTER_FTPSPUT_MOVE = 2;

  private String serverName;
  private String serverPort;
  private String userName;
  private String password;
  private String remoteDirectory;
  private String localDirectory;
  private String wildcard;
  private boolean copyingPrevious;
  private boolean copyingPreviousFiles;
  private boolean addFilenameResut;
  private boolean useKeyFilename;
  private String keyFilename;
  private String keyFilePassword;
  private String compression;
  private boolean createRemoteFolder;
  // proxy
  private String proxyType;
  private String proxyHost;
  private String proxyPort;
  private String proxyUsername;
  private String proxyPassword;

  private String destinationFolder;
  private boolean createDestinationFolder;

  private boolean successWhenNoFile;

  public ActionSftpPut(String n) {
    super(n, "");
    serverName = null;
    serverPort = "22";
    copyingPrevious = false;
    copyingPreviousFiles = false;
    addFilenameResut = false;
    useKeyFilename = false;
    keyFilename = null;
    keyFilePassword = null;
    compression = "none";
    proxyType = null;
    proxyHost = null;
    proxyPort = null;
    proxyUsername = null;
    proxyPassword = null;
    createRemoteFolder = false;
    afterFtps = AFTER_FTPSPUT_NOTHING;
    destinationFolder = null;
    createDestinationFolder = false;
    successWhenNoFile = false;
  }

  public ActionSftpPut() {
    this("");
  }

  public Object clone() {
    ActionSftpPut je = (ActionSftpPut) super.clone();
    return je;
  }

  public String getXml() {
    StringBuilder retval = new StringBuilder(300);

    retval.append(super.getXml());

    retval.append("      ").append(XmlHandler.addTagValue("servername", serverName));
    retval.append("      ").append(XmlHandler.addTagValue("serverport", serverPort));
    retval.append("      ").append(XmlHandler.addTagValue("username", userName));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue("password", Encr.encryptPasswordIfNotUsingVariables(password)));
    retval.append("      ").append(XmlHandler.addTagValue("sftpdirectory", remoteDirectory ));
    retval.append("      ").append(XmlHandler.addTagValue("localdirectory", localDirectory));
    retval.append("      ").append(XmlHandler.addTagValue("wildcard", wildcard));
    retval.append("      ").append(XmlHandler.addTagValue("copyprevious", copyingPrevious ));
    retval.append("      ").append(XmlHandler.addTagValue("copypreviousfiles", copyingPreviousFiles ));
    retval.append("      ").append(XmlHandler.addTagValue("addFilenameResut", addFilenameResut));
    retval.append("      ").append(XmlHandler.addTagValue("usekeyfilename", useKeyFilename ));
    retval.append("      ").append(XmlHandler.addTagValue("keyfilename", keyFilename ));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "keyfilepass", Encr.encryptPasswordIfNotUsingVariables( keyFilePassword )));
    retval.append("      ").append(XmlHandler.addTagValue("compression", compression));
    retval.append("      ").append(XmlHandler.addTagValue("proxyType", proxyType));
    retval.append("      ").append(XmlHandler.addTagValue("proxyHost", proxyHost));
    retval.append("      ").append(XmlHandler.addTagValue("proxyPort", proxyPort));
    retval.append("      ").append(XmlHandler.addTagValue("proxyUsername", proxyUsername));
    retval
        .append("      ")
        .append(
            XmlHandler.addTagValue(
                "proxyPassword", Encr.encryptPasswordIfNotUsingVariables(proxyPassword)));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("createRemoteFolder", createRemoteFolder));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("aftersftpput", getAfterSftpPutCode(getAfterFtps())));
    retval.append("      ").append(XmlHandler.addTagValue("destinationfolder", destinationFolder ));
    retval
        .append("      ")
        .append(XmlHandler.addTagValue("createdestinationfolder", createDestinationFolder));

    retval.append("      ").append(XmlHandler.addTagValue("successWhenNoFile", successWhenNoFile));

    return retval.toString();
  }

  public static String getAfterSftpPutCode(int i) {
    if (i < 0 || i >= afterFtpsCode.length) {
      return afterFtpsCode[0];
    }
    return afterFtpsCode[i];
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
          Encr.decryptPasswordOptionallyEncrypted(XmlHandler.getTagValue(entrynode, "password"));
      remoteDirectory = XmlHandler.getTagValue(entrynode, "sftpdirectory");
      localDirectory = XmlHandler.getTagValue(entrynode, "localdirectory");
      wildcard = XmlHandler.getTagValue(entrynode, "wildcard");
      copyingPrevious = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "copyprevious"));
      copyingPreviousFiles =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "copypreviousfiles"));
      addFilenameResut =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "addFilenameResut"));

      useKeyFilename = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "usekeyfilename"));
      keyFilename = XmlHandler.getTagValue(entrynode, "keyfilename");
      keyFilePassword =
          Encr.decryptPasswordOptionallyEncrypted(XmlHandler.getTagValue(entrynode, "keyfilepass"));
      compression = XmlHandler.getTagValue(entrynode, "compression");
      proxyType = XmlHandler.getTagValue(entrynode, "proxyType");
      proxyHost = XmlHandler.getTagValue(entrynode, "proxyHost");
      proxyPort = XmlHandler.getTagValue(entrynode, "proxyPort");
      proxyUsername = XmlHandler.getTagValue(entrynode, "proxyUsername");
      proxyPassword =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(entrynode, "proxyPassword"));

      createRemoteFolder =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "createRemoteFolder"));

      boolean remove = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "remove"));
      setAfterFtps(
          getAfterSftpPutByCode(Const.NVL(XmlHandler.getTagValue(entrynode, "aftersftpput"), "")));
      if (remove && getAfterFtps() == AFTER_FTPSPUT_NOTHING) {
        setAfterFtps(AFTER_FTPSPUT_DELETE);
      }
      destinationFolder = XmlHandler.getTagValue(entrynode, "destinationfolder");
      createDestinationFolder =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "createdestinationfolder"));
      successWhenNoFile =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "successWhenNoFile"));

    } catch (HopXmlException xe) {
      throw new HopXmlException("Unable to load action of type 'SFTPPUT' from XML node", xe);
    }
  }

  public static int getAfterSftpPutByCode(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < afterFtpsCode.length; i++) {
      if (afterFtpsCode[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }
    return 0;
  }

  public static String getAfterSftpPutDesc(int i) {
    if (i < 0 || i >= afterFtpsDesc.length) {
      return afterFtpsDesc[0];
    }
    return afterFtpsDesc[i];
  }

  public static int getAfterSftpPutByDesc(String tt) {
    if (tt == null) {
      return 0;
    }

    for (int i = 0; i < afterFtpsDesc.length; i++) {
      if (afterFtpsDesc[i].equalsIgnoreCase(tt)) {
        return i;
      }
    }

    // If this fails, try to match using the code.
    return getAfterSftpPutByCode(tt);
  }

  /** @param createDestinationFolder The create destination folder flag to set. */
  public void setCreateDestinationFolder(boolean createDestinationFolder) {
    this.createDestinationFolder = createDestinationFolder;
  }

  /** @return Returns the create destination folder flag */
  public boolean isCreateDestinationFolder() {
    return createDestinationFolder;
  }

  /** @param successWhenNoFile The successWhenNoFile flag to set. */
  public void setSuccessWhenNoFile(boolean successWhenNoFile) {
    this.successWhenNoFile = successWhenNoFile;
  }

  /** @return Returns the create successWhenNoFile folder flag */
  public boolean isSuccessWhenNoFile() {
    return successWhenNoFile;
  }

  public void setDestinationFolder(String destinationfolderin) {
    this.destinationFolder = destinationfolderin;
  }

  public String getDestinationFolder() {
    return destinationFolder;
  }

  /** @return Returns the afterFTPS. */
  public int getAfterFtps() {
    return afterFtps;
  }

  /** @param value The afterFTPS to set. */
  public void setAfterFtps(int value) {
    this.afterFtps = value;
  }

  /** @return Returns the directory. */
  public String getScpDirectory() {
    return remoteDirectory;
  }

  /** @param directory The directory to set. */
  public void setScpDirectory(String directory) {
    this.remoteDirectory = directory;
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

  /** @return Returns the localdirectory. */
  public String getLocalDirectory() {
    return localDirectory;
  }

  /** @param localDirectory The localDirectory to set. */
  public void setLocalDirectory(String localDirectory) {
    this.localDirectory = localDirectory;
  }

  public boolean isCopyPrevious() {
    return copyingPrevious;
  }

  public void setCopyPrevious(boolean copyprevious) {
    this.copyingPrevious = copyprevious;
  }

  public boolean isCopyPreviousFiles() {
    return copyingPreviousFiles;
  }

  public void setCopyPreviousFiles(boolean copypreviousfiles) {
    this.copyingPreviousFiles = copypreviousfiles;
  }

  public boolean isAddFilenameResut() {
    return addFilenameResut;
  }

  public boolean isUseKeyFile() {
    return useKeyFilename;
  }

  public void setUseKeyFile(boolean value) {
    this.useKeyFilename = value;
  }

  public String getKeyFilename() {
    return keyFilename;
  }

  public void setKeyFilename(String value) {
    this.keyFilename = value;
  }

  public String getKeyPassPhrase() {
    return keyFilePassword;
  }

  public void setKeyPassPhrase(String value) {
    this.keyFilePassword = value;
  }

  public void setAddFilenameResut(boolean addFilenameResut) {
    this.addFilenameResut = addFilenameResut;
  }

  /** @return Returns the compression. */
  public String getCompression() {
    return compression;
  }

  /** @param compression The compression to set. */
  public void setCompression(String compression) {
    this.compression = compression;
  }

  public String getServerPort() {
    return serverPort;
  }

  public void setServerPort(String serverPort) {
    this.serverPort = serverPort;
  }

  public String getProxyType() {
    return proxyType;
  }

  public void setProxyType(String value) {
    this.proxyType = value;
  }

  public String getProxyHost() {
    return proxyHost;
  }

  public void setProxyHost(String value) {
    this.proxyHost = value;
  }

  public String getProxyPort() {
    return proxyPort;
  }

  public void setProxyPort(String value) {
    this.proxyPort = value;
  }

  public String getProxyUsername() {
    return proxyUsername;
  }

  public void setProxyUsername(String value) {
    this.proxyUsername = value;
  }

  public String getProxyPassword() {
    return proxyPassword;
  }

  public void setProxyPassword(String value) {
    this.proxyPassword = value;
  }

  public boolean isCreateRemoteFolder() {
    return this.createRemoteFolder;
  }

  public void setCreateRemoteFolder(boolean value) {
    this.createRemoteFolder = value;
  }

  public Result execute(Result previousResult, int nr) throws HopException {
    Result result = previousResult;
    List<RowMetaAndData> rows = result.getRows();
    result.setResult(false);

    if (log.isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "ActionSftpPut.Log.StartAction"));
    }
    ArrayList<FileObject> myFileList = new ArrayList<FileObject>();

    if ( copyingPrevious ) {
      if (rows.size() == 0) {
        if (log.isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionSftpPut.ArgsFromPreviousNothing"));
        }
        result.setResult(true);
        return result;
      }

      try {
        RowMetaAndData resultRow = null;
        // Copy the input row to the (command line) arguments
        for (int iteration = 0; iteration < rows.size(); iteration++) {
          resultRow = rows.get(iteration);

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
              if (log.isDebug()) {
                logDebug(
                    BaseMessages.getString(PKG, "ActionSftpPut.Log.FilenameFromResult", filePrevious));
              }
            }
          }
        }
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "ActionSftpPut.Error.ArgFromPrevious"));
        result.setNrErrors(1);
        // free resource
        myFileList = null;
        return result;
      }
    }

    if ( copyingPreviousFiles ) {
      List<ResultFile> resultFiles = result.getResultFilesList();
      if (resultFiles == null || resultFiles.size() == 0) {
        if (log.isDetailed()) {
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
              if (log.isDebug()) {
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
        // free resource
        myFileList = null;
        return result;
      }
    }

    SftpClient sftpclient = null;

    // String substitution..
    String realServerName = resolve(serverName);
    String realServerPort = resolve(serverPort);
    String realUsername = resolve(userName);
    String realPassword = Encr.decryptPasswordOptionallyEncrypted(resolve(password));
    String realSftpDirString = resolve( remoteDirectory );
    String realWildcard = resolve(wildcard);
    String realLocalDirectory = resolve(localDirectory);
    String realKeyFilename = null;
    String realPassPhrase = null;
    // Destination folder (Move to)
    String realDestinationFolder = resolve(getDestinationFolder());

    try {
      // Let's perform some checks before starting

      if (getAfterFtps() == AFTER_FTPSPUT_MOVE) {
        if (Utils.isEmpty(realDestinationFolder)) {
          logError(BaseMessages.getString(PKG, "JobSSH2PUT.Log.DestinatFolderMissing"));
          result.setNrErrors(1);
          return result;
        } else {
          FileObject folder = null;
          try {
            folder = HopVfs.getFileObject(realDestinationFolder);
            // Let's check if folder exists...
            if (!folder.exists()) {
              // Do we need to create it?
              if (createDestinationFolder) {
                folder.createFolder();
              } else {
                logError(
                    BaseMessages.getString(
                        PKG, "JobSSH2PUT.Log.DestinatFolderNotExist", realDestinationFolder));
                result.setNrErrors(1);
                return result;
              }
            }
            realDestinationFolder = HopVfs.getFilename(folder);
          } catch (Exception e) {
            throw new HopException(e);
          } finally {
            if (folder != null) {
              try {
                folder.close();
              } catch (Exception e) {
                /* Ignore */
              }
            }
          }
        }
      }

      if (isUseKeyFile()) {
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
        realPassPhrase = resolve(getKeyPassPhrase());
      }

      // Create sftp client to host ...
      sftpclient =
          new SftpClient(
              InetAddress.getByName(realServerName),
              Const.toInt(realServerPort, 22),
              realUsername,
              realKeyFilename,
              realPassPhrase);
      if (log.isDetailed()) {
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
        boolean existfolder = sftpclient.folderExists(realSftpDirString);
        if (!existfolder) {
          if (!isCreateRemoteFolder()) {
            throw new HopException(
                BaseMessages.getString(
                    PKG, "ActionSftpPut.Error.CanNotFindRemoteFolder", realSftpDirString));
          }
          if (log.isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionSftpPut.Error.CanNotFindRemoteFolder", realSftpDirString));
          }

          // Let's create folder
          sftpclient.createFolder(realSftpDirString);
          if (log.isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionSftpPut.Log.RemoteFolderCreated", realSftpDirString));
          }
        }
        sftpclient.chdir(realSftpDirString);
        if (log.isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "ActionSftpPut.Log.ChangedDirectory", realSftpDirString));
        }
      } // end if

      if (!copyingPrevious && !copyingPreviousFiles ) {
        // Get all the files in the local directory...
        myFileList = new ArrayList<>();

        FileObject localFiles = HopVfs.getFileObject(realLocalDirectory);
        FileObject[] children = localFiles.getChildren();
        if (children != null) {
          for (int i = 0; i < children.length; i++) {
            // Get filename of file or directory
            if (children[i].getType().equals(FileType.FILE)) {
              // myFileList.add(children[i].getAbsolutePath());
              myFileList.add(children[i]);
            }
          } // end for
        }
      }

      if (log.isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG, "ActionSftpPut.Log.RowsFromPreviousResult", myFileList.size()));
      }

      Pattern pattern = null;
      if (!copyingPrevious && !copyingPreviousFiles ) {
        if (!Utils.isEmpty(realWildcard)) {
          pattern = Pattern.compile(realWildcard);
        }
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

            if (log.isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "ActionSftpPut.Log.PuttingFile", localFilename, realSftpDirString));
            }

            sftpclient.put(myFile, destinationFilename);
            nrFilesSent++;

            if (log.isDetailed()) {
              logDetailed(
                  BaseMessages.getString(PKG, "ActionSftpPut.Log.TransferredFile", localFilename));
            }

            // We successfully uploaded the file
            // what's next ...
            switch (getAfterFtps()) {
              case AFTER_FTPSPUT_DELETE:
                myFile.delete();
                if (log.isDetailed()) {
                  logDetailed(
                      BaseMessages.getString(PKG, "ActionSftpPut.Log.DeletedFile", localFilename));
                }
                break;
              case AFTER_FTPSPUT_MOVE:
                FileObject destination = null;
                try {
                  destination =
                      HopVfs.getFileObject(
                          realDestinationFolder
                              + Const.FILE_SEPARATOR
                              + myFile.getName().getBaseName());
                  myFile.moveTo(destination);
                  if (log.isDetailed()) {
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
                  if (log.isDetailed()) {
                    logDetailed(
                        BaseMessages.getString(
                            PKG, "ActionSftpPut.Log.FilenameAddedToResultFilenames", localFilename));
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
      result.setNrFilesRetrieved( nrFilesSent );

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
          result.setResult( false );
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
      } // end catch
      myFileList = null;
    } // end finally

    return result;
  } // JKU: end function execute()

  @Override public boolean isEvaluation() {
    return true;
  }

  @Override public List<ResourceReference> getResourceDependencies( IVariables variables, WorkflowMeta workflowMeta ) {
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
            "password",
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
