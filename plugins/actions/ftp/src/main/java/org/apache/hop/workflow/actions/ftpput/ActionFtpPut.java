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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
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
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.resource.ResourceReference;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.validator.ActionValidatorUtils;
import org.apache.hop.workflow.action.validator.AndValidator;
import org.apache.hop.workflow.actions.util.FtpClientUtil;
import org.apache.hop.workflow.actions.util.FtpHelper;
import org.apache.hop.workflow.actions.util.IFtpConnection;

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
    documentationUrl = "/workflow/actions/ftpput.html")
@EqualsAndHashCode(callSuper = false)
public class ActionFtpPut extends ActionBase implements Cloneable, IAction, IFtpConnection {
  private static final Class<?> PKG = ActionFtpPut.class;
  private static final String CONST_SPACE_SHORT = "      ";
  private static final String CONST_PASSWORD = "password";
  private static final String CONST_LOCAL_DIRECTORY = "localDirectory";

  public static final String FTP_DEFAULT_PORT = "21";
  public static final String FTP_DEFAULT_PROXY_PORT = "1080";

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

  /** Implicit encoding used before older version v2.4.1 */
  private static final String LEGACY_CONTROL_ENCODING = "US-ASCII";

  /** Default encoding when making a new ftp action instance. */
  private static final String DEFAULT_CONTROL_ENCODING = "ISO-8859-1";

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
    super(a.getName(), a.getDescription());
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
  @SuppressWarnings("java:S2975")
  public ActionFtpPut clone() {
    return new ActionFtpPut(this);
  }

  @Override
  public Result execute(Result prevResult, int nr) throws HopException {
    prevResult.setResult(false);
    long filesPut = 0;

    if (isDetailed()) {
      logDetailed(BaseMessages.getString(PKG, "ActionFtpPut.Log.Starting"));
    }

    FTPClient ftpClient = null;
    try {
      ftpClient = prepareFtpClient();
      changeRemoteDirectory(ftpClient);

      String realLocalDirectory = resolveLocalDirectory();
      List<String> files = listLocalFiles(realLocalDirectory);
      Pattern pattern = createPattern(resolve(wildcard));
      // for the files and upload file
      for (String file : files) {
        if (parentWorkflow.isStopped()) {
          break;
        }

        if (shouldProcessFile(file, pattern) && uploadFile(ftpClient, realLocalDirectory, file)) {
          filesPut++;
          deleteLocalFileIfNeeded(realLocalDirectory + Const.FILE_SEPARATOR + file);
        }
      }
      // upload success.
      prevResult.setResult(true);
      if (isBasic()) {
        logBasic(BaseMessages.getString(PKG, "ActionFtpPut.Log.WeHavePut", "" + filesPut));
      }
    } catch (Exception e) {
      prevResult.setNrErrors(1);
      logError(Const.getStackTracker(e));
    } finally {
      closeFtpClient(ftpClient);
    }
    return prevResult;
  }

  @Override
  public boolean isEvaluation() {
    return true;
  }

  @Override
  public List<ResourceReference> getResourceDependencies(
      IVariables variables, WorkflowMeta workflowMeta) {
    List<ResourceReference> references = super.getResourceDependencies(this, workflowMeta);
    // add resource entity
    FtpHelper.addServerResourceReferenceIfPresent(references, serverName, this, this);
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
            CONST_LOCAL_DIRECTORY,
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

  /**
   * Creates the ftp client for this action.
   *
   * @return an initialized and connected {@link FTPClient}
   * @throws HopException Exception if connection or setup fails
   */
  private FTPClient prepareFtpClient() throws HopException {
    FTPClient ftpClient = createAndSetUpFtpClient();
    if (ftpClient == null || !ftpClient.isConnected()) {
      throw new HopException("Failed to connect FTP server.");
    }

    int code = ftpClient.getReplyCode();
    String msg = ftpClient.getReplyString();
    if (!FTPReply.isPositiveCompletion(code)) {
      throw new HopException(
          "FTP server refused connection. reply code: " + code + ", result: " + msg);
    }

    if (isBasic()) {
      logBasic("FTP connection success, reply code: {0}, result: {1}", code, msg);
    }
    return ftpClient;
  }

  /**
   * Changes the current working directory on the FTP server to the resolved remote directory.
   *
   * @param ftpClient ftpClient the {@link FTPClient} instance
   * @throws IOException Exception if changing directory fails
   */
  private void changeRemoteDirectory(FTPClient ftpClient) throws IOException {
    String realRemoteDirectory = resolve(remoteDirectory);
    if (Utils.isEmpty(realRemoteDirectory)) {
      return;
    }

    ftpClient.changeWorkingDirectory(realRemoteDirectory);
    if (isDetailed()) {
      logDetailed(
          BaseMessages.getString(PKG, "ActionFtpPut.Log.ChangedDirectory", realRemoteDirectory));
    }
  }

  /**
   * Resolves the local directory path, handling "file:" prefixes
   *
   * @return the resolved local directory path
   * @throws HopException if the local directory is not specified
   * @throws URISyntaxException if the local directory is not specified
   */
  private String resolveLocalDirectory() throws HopException, URISyntaxException {
    String realLocalDirectory = resolve(localDirectory);
    if (realLocalDirectory == null) {
      throw new HopException(BaseMessages.getString(PKG, "ActionFtpPut.LocalDir.NotSpecified"));
    }

    if (realLocalDirectory.startsWith("file:")) {
      realLocalDirectory = new URI(realLocalDirectory).getPath();
    }
    return realLocalDirectory;
  }

  /**
   * Lists all non-directory files in the given local directory.
   *
   * @param localDir the directory path
   * @return a list of file names (excluding subdirectories)
   */
  private List<String> listLocalFiles(String localDir) {
    File[] children = new File(localDir).listFiles();
    if (children == null) {
      return Collections.emptyList();
    }
    return Arrays.stream(children).filter(f -> !f.isDirectory()).map(File::getName).toList();
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
   * Uploads a single file to the FTP server, handling existing file deletion and binary mode.
   *
   * @param ftpClient the {@link FTPClient} instance
   * @param localDir the local directory containing the file
   * @param file the file name
   * @return true if the file was uploaded successfully, false otherwise
   */
  private boolean uploadFile(FTPClient ftpClient, String localDir, String file) {
    String localFilename = localDir + Const.FILE_SEPARATOR + file;
    try (InputStream inputStream = HopVfs.getInputStream(localFilename)) {
      boolean fileExist = FtpClientUtil.fileExists(ftpClient, file);
      if (fileExist && !onlyPuttingNewFiles) {
        ftpClient.deleteFile(file);
      }

      if (binaryMode) {
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
      }

      boolean success = ftpClient.storeFile(file, inputStream);
      if (!success) {
        logError("Failed to upload file '" + localFilename + "' → " + ftpClient.getReplyString());
      }
      return success;
    } catch (Exception e) {
      logError("Error uploading file: " + localFilename, e);
      return false;
    }
  }

  /**
   * Deletes the local file if the "remove" option is enabled.
   *
   * @param localFilename the full path of the local file to delete
   */
  private void deleteLocalFileIfNeeded(String localFilename) throws IOException {
    if (remove) {
      Files.deleteIfExists(Path.of(localFilename));
      if (isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "ActionFtpPut.Log.DeletedFile", localFilename));
      }
    }
  }

  /**
   * Safely closes the FTP client connection.
   *
   * @param ftpClient the {@link FTPClient} instance to close
   */
  private void closeFtpClient(FTPClient ftpClient) {
    if (ftpClient != null && ftpClient.isConnected()) {
      try {
        ftpClient.quit();
      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "ActionFtpPut.Log.ErrorQuitingFTP", e.getMessage()));
      }
    }

    FtpClientUtil.clearSocksJvmSettings();
  }

  // package-local visibility for testing purposes
  FTPClient createAndSetUpFtpClient() throws HopException {
    return FtpClientUtil.connectAndLogin(getLogChannel(), this, this, getName());
  }
}
