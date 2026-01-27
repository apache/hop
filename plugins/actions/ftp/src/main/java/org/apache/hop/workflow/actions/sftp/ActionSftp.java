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

package org.apache.hop.workflow.actions.sftp;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.encryption.Encr;
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
import org.apache.hop.workflow.action.validator.AbstractFileValidator;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.action.validator.ValidatorContext;
import org.w3c.dom.Node;

/** This defines a SFTP action. */
@Action(
    id = "SFTP",
    name = "i18n::ActionSFTP.Name",
    description = "i18n::ActionSFTP.Description",
    image = "SFTP.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileTransfer",
    keywords = "i18n::ActionSftp.keyword",
    documentationUrl = "/workflow/actions/sftp.html")
public class ActionSftp extends ActionBase implements Cloneable, IAction {
  private static final Class<?> PKG = ActionSftp.class;
  private static final String CONST_SPACE_SHORT = "      ";
  private static final String CONST_PASSWORD = "password";

  private static final int DEFAULT_PORT = 22;
  @Getter @Setter private String serverName;
  @Getter @Setter private String serverPort;
  @Getter @Setter private String userName;
  @Getter @Setter private String password;
  @Getter @Setter private String sftpDirectory;
  @Getter @Setter private String targetDirectory;
  @Getter @Setter private String wildcard;
  @Getter @Setter private boolean remove;
  @Getter @Setter private boolean addFilenameToResult;
  @Getter @Setter private boolean createTargetFolder;
  @Getter @Setter private boolean copyPrevious;
  @Getter @Setter private boolean useKeyFilename;
  @Getter @Setter private boolean preserveTargetFileTimestamp;
  @Getter @Setter private String keyFilename;
  @Getter @Setter private String keyPassPhrase;
  @Getter @Setter private String compression;
  // proxy
  @Getter @Setter private String proxyType;
  @Getter @Setter private String proxyHost;
  @Getter @Setter private String proxyPort;
  @Getter @Setter private String proxyUsername;
  @Getter @Setter private String proxyPassword;

  public ActionSftp(String n) {
    super(n, "");
    serverName = null;
    serverPort = "22";
    addFilenameToResult = true;
    preserveTargetFileTimestamp = true;
    createTargetFolder = false;
    copyPrevious = false;
    useKeyFilename = false;
    keyFilename = null;
    keyPassPhrase = null;
    compression = "none";
    proxyType = null;
    proxyHost = null;
    proxyPort = null;
    proxyUsername = null;
    proxyPassword = null;
  }

  public ActionSftp() {
    this("");
  }

  @Override
  public Object clone() {
    ActionSftp je = (ActionSftp) super.clone();
    return je;
  }

  @Override
  public String getXml() {
    StringBuilder retval = new StringBuilder(200);

    retval.append(super.getXml());

    retval.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("servername", serverName));
    retval.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("serverport", serverPort));
    retval.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("username", userName));
    retval
        .append(CONST_SPACE_SHORT)
        .append(
            XmlHandler.addTagValue(
                CONST_PASSWORD, Encr.encryptPasswordIfNotUsingVariables(getPassword())));
    retval.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("sftpdirectory", sftpDirectory));
    retval
        .append(CONST_SPACE_SHORT)
        .append(XmlHandler.addTagValue("targetdirectory", targetDirectory));
    retval.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("wildcard", wildcard));
    retval.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("remove", remove));
    retval
        .append(CONST_SPACE_SHORT)
        .append(XmlHandler.addTagValue("isaddresult", addFilenameToResult));
    retval
        .append(CONST_SPACE_SHORT)
        .append(XmlHandler.addTagValue("preserveTargetFileTimestamp", preserveTargetFileTimestamp));
    retval
        .append(CONST_SPACE_SHORT)
        .append(XmlHandler.addTagValue("createtargetfolder", createTargetFolder));
    retval.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("copyprevious", copyPrevious));

    retval
        .append(CONST_SPACE_SHORT)
        .append(XmlHandler.addTagValue("usekeyfilename", useKeyFilename));
    retval.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("keyfilename", keyFilename));
    retval
        .append(CONST_SPACE_SHORT)
        .append(
            XmlHandler.addTagValue(
                "keyfilepass", Encr.encryptPasswordIfNotUsingVariables(keyPassPhrase)));
    retval.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("compression", compression));

    retval.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("proxyType", proxyType));
    retval.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("proxyHost", proxyHost));
    retval.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("proxyPort", proxyPort));
    retval.append(CONST_SPACE_SHORT).append(XmlHandler.addTagValue("proxyUsername", proxyUsername));
    retval
        .append(CONST_SPACE_SHORT)
        .append(
            XmlHandler.addTagValue(
                "proxyPassword", Encr.encryptPasswordIfNotUsingVariables(proxyPassword)));

    return retval.toString();
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
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(entrynode, CONST_PASSWORD));
      sftpDirectory = XmlHandler.getTagValue(entrynode, "sftpdirectory");
      targetDirectory = XmlHandler.getTagValue(entrynode, "targetdirectory");
      wildcard = XmlHandler.getTagValue(entrynode, "wildcard");
      remove = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "remove"));

      String addResult = XmlHandler.getTagValue(entrynode, "isaddresult");
      String preserveTimestamp = XmlHandler.getTagValue(entrynode, "preserveTargetFileTimestamp");

      addFilenameToResult = Utils.isEmpty(addResult) || "Y".equalsIgnoreCase(addResult);
      preserveTargetFileTimestamp =
          Utils.isEmpty(preserveTimestamp) || "Y".equalsIgnoreCase(preserveTimestamp);

      createTargetFolder =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "createtargetfolder"));
      copyPrevious = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "copyprevious"));

      useKeyFilename = "Y".equalsIgnoreCase(XmlHandler.getTagValue(entrynode, "usekeyfilename"));
      keyFilename = XmlHandler.getTagValue(entrynode, "keyfilename");
      keyPassPhrase =
          Encr.decryptPasswordOptionallyEncrypted(XmlHandler.getTagValue(entrynode, "keyfilepass"));
      compression = XmlHandler.getTagValue(entrynode, "compression");

      proxyType = XmlHandler.getTagValue(entrynode, "proxyType");
      proxyHost = XmlHandler.getTagValue(entrynode, "proxyHost");
      proxyPort = XmlHandler.getTagValue(entrynode, "proxyPort");
      proxyUsername = XmlHandler.getTagValue(entrynode, "proxyUsername");
      proxyPassword =
          Encr.decryptPasswordOptionallyEncrypted(
              XmlHandler.getTagValue(entrynode, "proxyPassword"));
    } catch (HopXmlException xe) {
      throw new HopXmlException("Unable to load action of type 'SFTP' from XML node", xe);
    }
  }

  @Override
  public Result execute(Result previousResult, int nr) {
    Result result = previousResult;
    List<RowMetaAndData> rows = result.getRows();
    RowMetaAndData resultRow = null;

    result.setResult(false);
    long filesRetrieved = 0;

    if (isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "ActionSftp.Log.StartAction"));
    }
    HashSet<String> listPreviousFilenames = new HashSet<>();

    if (copyPrevious) {
      if (rows.isEmpty()) {
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionSftp.ArgsFromPreviousNothing"));
        }
        result.setResult(true);
        return result;
      }
      try {

        // Copy the input row to the (command line) arguments
        for (RowMetaAndData row : rows) {
          resultRow = row;

          // Get file names
          String filePrevious = resultRow.getString(0, null);
          if (!Utils.isEmpty(filePrevious)) {
            listPreviousFilenames.add(filePrevious);
            if (isDebug()) {
              logDebug(
                  BaseMessages.getString(PKG, "ActionSftp.Log.FilenameFromResult", filePrevious));
            }
          }
        }
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "ActionSftp.Error.ArgFromPrevious"));
        result.setNrErrors(1);
        return result;
      }
    }

    SftpClient sftpClient = null;

    // String substitution..
    String realServerName = resolve(serverName);
    String realServerPort = resolve(serverPort);
    String realUsername = resolve(userName);
    String realPassword = Encr.decryptPasswordOptionallyEncrypted(resolve(password));
    String realSftpDirString = resolve(sftpDirectory);
    String realWildcard = resolve(wildcard);
    String realTargetDirectory = resolve(targetDirectory);
    String realKeyFilename = null;
    String realPassPhrase = null;
    FileObject targetFolder = null;

    try {
      // Let's perform some checks before starting
      if (isUseKeyFilename()) {
        // We must have here a private keyFilename
        realKeyFilename = resolve(getKeyFilename());
        if (Utils.isEmpty(realKeyFilename)) {
          // Error..Missing keyfile
          logError(BaseMessages.getString(PKG, "ActionSftp.Error.KeyFileMissing"));
          result.setNrErrors(1);
          return result;
        }
        if (!HopVfs.fileExists(realKeyFilename)) {
          // Error.. can not reach keyfile
          logError(
              BaseMessages.getString(PKG, "ActionSftp.Error.KeyFileNotFound", realKeyFilename));
          result.setNrErrors(1);
          return result;
        }
        realPassPhrase = resolve(getKeyPassPhrase());
      }

      if (!Utils.isEmpty(realTargetDirectory)) {
        targetFolder = HopVfs.getFileObject(realTargetDirectory);
        boolean targetFolderExists = targetFolder.exists();
        if (targetFolderExists) {
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(
                    PKG, "ActionSftp.Log.TargetFolderExists", realTargetDirectory));
          }
        } else {
          if (!createTargetFolder) {
            // Error..Target folder can not be found !
            logError(
                BaseMessages.getString(
                    PKG, "ActionSftp.Error.TargetFolderNotExists", realTargetDirectory));
            result.setNrErrors(1);
            return result;
          } else {
            // create target folder
            targetFolder.createFolder();
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG, "ActionSftp.Log.TargetFolderCreated", realTargetDirectory));
            }
          }
        }
      }

      if (targetFolder != null) {
        targetFolder.close();
        targetFolder = null;
      }

      // Create sftp client to host ...
      sftpClient =
          new SftpClient(
              InetAddress.getByName(realServerName),
              Const.toInt(realServerPort, DEFAULT_PORT),
              realUsername,
              realKeyFilename,
              realPassPhrase);
      if (isDetailed()) {
        logDetailed(
            BaseMessages.getString(
                PKG,
                "ActionSftp.Log.OpenedConnection",
                realServerName,
                realServerPort,
                realUsername));
      }

      // Set compression
      sftpClient.setCompression(getCompression());

      // Set proxy?
      String realProxyHost = resolve(getProxyHost());
      if (!Utils.isEmpty(realProxyHost)) {
        // Set proxy
        String password = getRealPassword(getProxyPassword());
        sftpClient.setProxy(
            realProxyHost,
            resolve(getProxyPort()),
            resolve(getProxyUsername()),
            password,
            getProxyType());
      }

      // login to ftp host ...
      sftpClient.login(realPassword);
      // Passwords should not appear in log files.
      // logDetailed("logged in using password "+realPassword); // Logging this seems a bad idea! Oh
      // well.

      // move to spool dir ...
      if (!Utils.isEmpty(realSftpDirString)) {
        try {
          sftpClient.chdir(realSftpDirString);
        } catch (Exception e) {
          logError(
              BaseMessages.getString(
                  PKG, "ActionSftp.Error.CanNotFindRemoteFolder", realSftpDirString));
          throw new Exception(e);
        }
        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "ActionSftp.Log.ChangedDirectory", realSftpDirString));
        }
      }
      Pattern pattern = null;
      // Get all the files in the current directory...
      ArrayList<FileItem> filelist = sftpClient.dir();
      if (filelist.isEmpty()) {
        // Nothing was found !!! exit
        result.setResult(true);
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "ActionSftp.Log.Found", "" + 0));
        }
        return result;
      }
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionSftp.Log.Found", "" + filelist.size()));
      }

      if (!copyPrevious && !Utils.isEmpty(realWildcard)) {
        pattern = Pattern.compile(realWildcard);
      }

      // Get the files in the list...
      for (int i = 0; i < filelist.size() && !parentWorkflow.isStopped(); i++) {
        boolean getIt = true;
        String sourceFilename = filelist.get(i).getFileName();
        if (copyPrevious) {
          // filenames list is send by previous action
          // download if the current file is in this list
          getIt = listPreviousFilenames.contains(sourceFilename);
        } else {
          // download files
          // but before see if the file matches the regular expression!
          if (pattern != null) {
            Matcher matcher = pattern.matcher(sourceFilename);
            getIt = matcher.matches();
          }
        }

        if (getIt) {
          if (isDebug()) {
            logDebug(
                BaseMessages.getString(
                    PKG, "ActionSftp.Log.GettingFiles", sourceFilename, realTargetDirectory));
          }

          FileObject targetFile =
              HopVfs.getFileObject(realTargetDirectory + Const.FILE_SEPARATOR + sourceFilename);
          sftpClient.get(targetFile, filelist.get(i));
          if (preserveTargetFileTimestamp) {
            targetFile.getContent().setLastModifiedTime(filelist.get(i).getLastModified());
          }

          filesRetrieved++;

          if (addFilenameToResult) {
            // Add to the result files...
            ResultFile resultFile =
                new ResultFile(
                    ResultFile.FILE_TYPE_GENERAL,
                    targetFile,
                    parentWorkflow.getWorkflowName(),
                    toString());
            result.getResultFiles().put(resultFile.getFile().toString(), resultFile);
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(
                      PKG, "ActionSftp.Log.FilenameAddedToResultFilenames", sourceFilename));
            }
          }
          if (isDetailed()) {
            logDetailed(
                BaseMessages.getString(PKG, "ActionSftp.Log.TransferedFile", sourceFilename));
          }

          // Delete the file if this is needed!
          if (remove) {
            sftpClient.delete(sourceFilename);
            if (isDetailed()) {
              logDetailed(
                  BaseMessages.getString(PKG, "ActionSftp.Log.DeletedFile", sourceFilename));
            }
          }
        }
      }

      result.setResult(true);
      result.setNrFilesRetrieved(filesRetrieved);
    } catch (Exception e) {
      result.setNrErrors(1);
      logError(BaseMessages.getString(PKG, "ActionSftp.Error.GettingFiles", e.getMessage()));
      logError(Const.getStackTracker(e));
    } finally {
      // close connection, if possible
      try {
        if (sftpClient != null) {
          sftpClient.disconnect();
        }
      } catch (Exception e) {
        // just ignore this, makes no big difference
      }

      try {
        if (targetFolder != null) {
          targetFolder.close();
          targetFolder = null;
        }
        if (listPreviousFilenames != null) {
          listPreviousFilenames = null;
        }
      } catch (Exception e) {
        // Ignore errors
      }
    }

    return result;
  }

  public String getRealPassword(String password) {
    return Utils.resolvePassword(this, password);
  }

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

    ValidatorContext ctx = new ValidatorContext();
    AbstractFileValidator.putVariableSpace(ctx, getVariables());
    AndValidator.putValidators(
        ctx, ActionValidatorUtils.notBlankValidator(), ActionValidatorUtils.fileExistsValidator());
    ActionValidatorUtils.andValidator().validate(this, "targetDirectory", remarks, ctx);

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
}
