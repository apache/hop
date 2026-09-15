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

package org.apache.hop.workflow.actions.ftpput;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileType;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Result;
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
import org.apache.hop.vfs.ftp.FtpClientFactory;
import org.apache.hop.vfs.ftp.FtpHelper;
import org.apache.hop.vfs.ftp.IFtpConnection;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;

/** This defines an FTP put action. */
@Getter
@Setter
@Action(
    id = "FTP_PUT",
    name = "i18n::ActionFTPPut.Name",
    description = "i18n::ActionFTPPut.Description",
    image = "FTPPut.svg",
    categoryDescription = "i18n:org.apache.hop.workflow:ActionCategory.Category.FileTransfer",
    keywords = "i18n::ActionFtpPut.keyword",
    documentationUrl = "/workflow/actions/ftpput.html",
    classLoaderGroup = "sftp")
public class ActionFtpPut extends ActionBase implements Cloneable, IAction, IFtpConnection {
  private static final Class<?> PKG = ActionFtpPut.class;
  private static final String CONST_LOCAL_DIRECTORY = "localDirectory";

  public static final String FTP_DEFAULT_PORT = "21";
  public static final String FTP_DEFAULT_PROXY_PORT = "1080";

  /** Default encoding when making a new ftp action instance. */
  private static final String DEFAULT_CONTROL_ENCODING = "ISO-8859-1";

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

  @HopMetadataProperty(key = "serverport")
  private String serverPort;

  @HopMetadataProperty(key = "username")
  private String userName;

  @HopMetadataProperty(key = "password", password = true)
  private String password;

  @HopMetadataProperty(key = "remoteDirectory")
  private String remoteDirectory;

  @HopMetadataProperty(key = "localDirectory")
  private String localDirectory;

  @HopMetadataProperty(key = "wildcard")
  private String wildcard;

  @HopMetadataProperty(key = "binary")
  private boolean binaryMode;

  @HopMetadataProperty(key = "timeout")
  private int timeout;

  @HopMetadataProperty(key = "remove")
  private boolean remove;

  /* Don't overwrite files */
  @HopMetadataProperty(key = "only_new")
  private boolean onlyPuttingNewFiles;

  @HopMetadataProperty(key = "active")
  private boolean activeConnection;

  /* how to convert list of filenames e.g. */
  @HopMetadataProperty(key = "control_encoding")
  private String controlEncoding;

  @HopMetadataProperty(key = "proxy_host")
  private String proxyHost;

  /* string to allow variable substitution */
  @HopMetadataProperty(key = "proxy_port")
  private String proxyPort;

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

  public ActionFtpPut(String n) {
    super(n, "");
    serverName = null;
    serverPort = FTP_DEFAULT_PORT;
    socksProxyPort = FTP_DEFAULT_PROXY_PORT;
    remoteDirectory = null;
    localDirectory = null;
    setControlEncoding(DEFAULT_CONTROL_ENCODING);
  }

  public ActionFtpPut() {
    this("");
  }

  public ActionFtpPut(ActionFtpPut a) {
    super(a.getName(), a.getDescription(), a.getPluginId());
    this.connection = a.connection;
    this.serverName = a.serverName;
    this.serverPort = a.serverPort;
    this.userName = a.userName;
    this.password = a.password;
    this.remoteDirectory = a.remoteDirectory;
    this.localDirectory = a.localDirectory;
    this.wildcard = a.wildcard;
    this.binaryMode = a.binaryMode;
    this.timeout = a.timeout;
    this.remove = a.remove;
    this.onlyPuttingNewFiles = a.onlyPuttingNewFiles;
    this.activeConnection = a.activeConnection;
    this.controlEncoding = a.controlEncoding;
    this.proxyHost = a.proxyHost;
    this.proxyPort = a.proxyPort;
    this.proxyUsername = a.proxyUsername;
    this.proxyPassword = a.proxyPassword;
    this.socksProxyHost = a.socksProxyHost;
    this.socksProxyPort = a.socksProxyPort;
    this.socksProxyUsername = a.socksProxyUsername;
    this.socksProxyPassword = a.socksProxyPassword;
  }

  @Override
  public String getFtpConnectionName() {
    return Const.NVL(getName(), "FTP Put");
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
  public Result execute(Result prevResult, int nr) throws HopException {
    prevResult.setResult(false);
    long filesPut = 0;
    long errors = 0;

    if (isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "ActionFtpPut.Log.Starting"));
    }

    FTPClient ftpClient = null;
    try {
      ftpClient = createAndSetUpFtpClient();
      changeRemoteDirectory(ftpClient);

      String realLocalDirectory = resolveLocalDirectory();
      List<String> files = listLocalFiles(realLocalDirectory);
      Pattern pattern = createPattern(resolve(wildcard));

      for (String file : files) {
        if (parentWorkflow.isStopped()) {
          break;
        }
        if (!shouldProcessFile(file, pattern)) {
          continue;
        }
        if (uploadFile(ftpClient, realLocalDirectory, file)) {
          filesPut++;
          deleteLocalFileIfNeeded(childUri(realLocalDirectory, file));
        } else {
          errors++;
        }
      }

      if (isBasic()) {
        logBasic(
            BaseMessages.getString(PKG, "ActionFtpPut.Log.WeHavePut", String.valueOf(filesPut)));
      }
    } catch (Exception e) {
      errors++;
      logError(
          BaseMessages.getString(PKG, "ActionFtpPut.Log.ErrorPuttingFiles", e.getMessage()), e);
    } finally {
      FtpHelper.disconnect(getLogChannel(), ftpClient);
    }

    // A file we couldn't upload is a failed action: reporting success here would let the workflow
    // carry on as if the files had arrived.
    //
    prevResult.setNrErrors(errors);
    prevResult.setResult(errors == 0);
    return prevResult;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(variables, workflowMeta);
    FtpHelper.addServerResourceReferenceIfPresent(references, serverName, this, this);
    return references;
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    FtpHelper.checkServerSettings(remarks, this, connection);
    FtpHelper.checkDirectoryExists(remarks, this, variables, CONST_LOCAL_DIRECTORY);
    if (!isUsingConnection()) {
      ActionValidatorUtils.andValidator()
          .validate(
              this,
              "serverPort",
              remarks,
              AndValidator.putValidators(ActionValidatorUtils.integerValidator()));
    }
  }

  /**
   * Changes the current working directory on the FTP server to the resolved remote directory.
   *
   * @param ftpClient the {@link FTPClient} instance
   * @throws HopException if the directory isn't there
   */
  private void changeRemoteDirectory(FTPClient ftpClient) throws IOException, HopException {
    String realRemoteDirectory = resolve(remoteDirectory);
    if (Utils.isEmpty(realRemoteDirectory)) {
      return;
    }

    if (!ftpClient.changeWorkingDirectory(realRemoteDirectory)) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "ActionFtpPut.Log.RemoteDirectoryNotFound", realRemoteDirectory));
    }
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(PKG, "ActionFtpPut.Log.ChangedDirectory", realRemoteDirectory));
    }
  }

  /**
   * Resolves the local directory. It goes through VFS, so the "local" directory is allowed to be
   * any location VFS can reach.
   *
   * @return the resolved local directory
   * @throws HopException if the local directory is not specified or can't be reached
   */
  private String resolveLocalDirectory() throws HopException {
    String realLocalDirectory = resolve(localDirectory);
    if (Utils.isEmpty(realLocalDirectory)) {
      throw new HopException(BaseMessages.getString(PKG, "ActionFtpPut.LocalDir.NotSpecified"));
    }
    return realLocalDirectory;
  }

  /**
   * Lists all non-directory files in the given directory.
   *
   * @param localDir the directory
   * @return the file names in it, without the subdirectories
   */
  private List<String> listLocalFiles(String localDir) throws HopException {
    try (FileObject folder = HopVfs.getFileObject(localDir)) {
      if (!folder.exists() || folder.getType() != FileType.FOLDER) {
        throw new HopException(
            BaseMessages.getString(PKG, "ActionFtpPut.LocalDir.NotFound", localDir));
      }
      FileObject[] children = folder.getChildren();
      if (children == null) {
        return Collections.emptyList();
      }
      List<String> files = new ArrayList<>();
      for (FileObject child : children) {
        if (child.getType() == FileType.FILE) {
          files.add(child.getName().getBaseName());
        }
      }
      return files;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionFtpPut.LocalDir.NotFound", localDir), e);
    }
  }

  /**
   * Compiles a regex {@link Pattern} from the given wildcard string.
   *
   * @param wildcard the wildcard string (may be null or empty)
   * @return a {@link Pattern} object, or null if wildcard is empty
   */
  private Pattern createPattern(String wildcard) {
    return Utils.isEmpty(wildcard) ? null : Pattern.compile(wildcard);
  }

  /**
   * Checks whether a file should be processed based on the optional regex pattern.
   *
   * @param file the file name
   * @param pattern the compiled regex pattern (may be null)
   * @return true if the file should be processed
   */
  private boolean shouldProcessFile(String file, Pattern pattern) {
    return pattern == null || pattern.matcher(file).matches();
  }

  /**
   * Uploads a single file to the FTP server.
   *
   * @param ftpClient the {@link FTPClient} instance
   * @param localDir the directory containing the file
   * @param file the file name
   * @return true if the file was uploaded, or deliberately skipped because it was already there
   */
  private boolean uploadFile(FTPClient ftpClient, String localDir, String file) {
    String localFilename = childUri(localDir, file);
    try {
      if (FtpClientFactory.fileExists(ftpClient, file)) {
        if (onlyPuttingNewFiles) {
          // "Only put new files" means leaving the one on the server alone, not overwriting it
          // after a delete that didn't happen.
          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "ActionFtpPut.Log.SkippedExisting", file));
          }
          return true;
        }
        ftpClient.deleteFile(file);
      }

      try (InputStream inputStream = HopVfs.getInputStream(localFilename)) {
        if (!ftpClient.storeFile(file, inputStream)) {
          logError(
              BaseMessages.getString(
                  PKG,
                  "ActionFtpPut.Log.UploadFailed",
                  localFilename,
                  Const.NVL(ftpClient.getReplyString(), "").trim()));
          return false;
        }
      }
      return true;
    } catch (Exception e) {
      logError(BaseMessages.getString(PKG, "ActionFtpPut.Log.UploadFailed", localFilename, ""), e);
      return false;
    }
  }

  /**
   * Deletes the source file if the "remove" option is enabled. Goes through VFS, like everything
   * else this action reads.
   *
   * @param localFilename the file to delete
   */
  private void deleteLocalFileIfNeeded(String localFilename) throws HopException {
    if (!remove) {
      return;
    }
    try (FileObject file = HopVfs.getFileObject(localFilename)) {
      if (file.delete() && isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionFtpPut.Log.DeletedFile", localFilename));
      }
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "ActionFtpPut.Log.DeleteFailed", localFilename), e);
    }
  }

  /** A child of a folder, in a form both VFS and a plain path understand. */
  private String childUri(String folder, String filename) {
    String separator = folder.endsWith("/") || folder.endsWith("\\") ? "" : "/";
    return folder + separator + filename;
  }

  // package-local visibility for testing purposes
  FTPClient createAndSetUpFtpClient() throws HopException {
    return FtpHelper.connect(this, connection);
  }
}
