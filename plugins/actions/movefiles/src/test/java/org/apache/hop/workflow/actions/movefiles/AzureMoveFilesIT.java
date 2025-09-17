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

package org.apache.hop.workflow.actions.movefiles;

import static org.apache.hop.workflow.actions.movefiles.MoveFilesActionHelper.defaultAction;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.microsoft.azure.storage.StorageException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.vfs.plugin.VfsPluginType;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.apache.hop.vfs.azure.config.AzureConfig;
import org.apache.hop.vfs.azure.config.AzureConfigSingleton;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class AzureMoveFilesIT {

  private static String CONNECTION_STRING;
  private static String account;
  private static String key;
  private static String emulatorUrl;
  private static Boolean useTestContainer;

  private static final String CONTAINER_NAME = "mycontainer";
  private static final String ANOTHER_CONTAINER_NAME = "anothercontainer";
  public static final String AZURITE_DEFAULT_ACCOUNT = "devstoreaccount1";
  public static final String AZURITE_DEFAULT_KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

  private static BlobServiceClient blobServiceClient;

  private String basePath;

  public AzureMoveFilesIT(String basePath) {
    this.basePath = basePath;
  }

  @Parameterized.Parameters
  public static Collection paths() {
    return Arrays.asList(new Object[][] {{"azfs:///${currentAccount}/"}, {"azure:///"}});
  }

  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  //    @ClassRule
  //    public static GenericContainer azuriteContainer =
  //        new GenericContainer<>("mcr.microsoft.com/azure-storage/azurite:3.30.0")
  //            .withExposedPorts(10000)
  //
  //            .withCommand("azurite-blob", "--blobHost", "0.0.0.0", "--loose", "--debug",
  // "/home/debug.log");

  @BeforeClass
  public static void init() throws HopException {
    loadAzureProperties();
    blobServiceClient =
        new BlobServiceClientBuilder().connectionString(CONNECTION_STRING).buildClient();
    blobServiceClient.createBlobContainerIfNotExists(CONTAINER_NAME);
    blobServiceClient.createBlobContainerIfNotExists(ANOTHER_CONTAINER_NAME);
    HopClientEnvironment.init(List.of(VfsPluginType.getInstance()));
    HopEnvironment.init();
    HopLogStore.init(true, true);
  }

  @Before
  public void setup()
      throws URISyntaxException, InvalidKeyException, StorageException, IOException, HopException {
    // Retrieve storage account from connection-string.
    final BlobContainerClient container = blobServiceClient.getBlobContainerClient(CONTAINER_NAME);
    deleteFiles(CONTAINER_NAME);
    deleteFiles(ANOTHER_CONTAINER_NAME);
    if (basePath.contains("${currentAccount}")) {
      basePath =
          basePath.replace("${currentAccount}", AzureConfigSingleton.getConfig().getAccount());
    }

    uploadFileIntoContainer(
        container.getBlobContainerName(),
        "artists.csv",
        "artists2.csv",
        "alreadythere.csv",
        "artists3.csv",
        "artists4.csv",
        "canbeoverwritten.csv",
        "artists-wildcard1.csv",
        "artists-wildcard2.csv");
  }

  @Test
  void whenTargetFileDoesNotExist_thenRenameFile() throws HopException {

    ActionMoveFiles action = defaultAction();

    String fileToRename = getFilePath("artists.csv");
    action.sourceFileFolder[0] = fileToRename;
    action.destinationFileFolder[0] = fileToRename + "done";

    action.execute(new Result(), 1);

    assertThatFileExists(CONTAINER_NAME, "artists.csvdone");
    assertThatFileDoesNotExist(CONTAINER_NAME, "artists.csv");
  }

  @Test
  void whenTargetFileAlreadyExists_thenDoNotRename() throws HopException {
    ActionMoveFiles action = defaultAction();
    IWorkflowEngine<WorkflowMeta> parentWorkflow = new LocalWorkflowEngine();

    action.sourceFileFolder[0] = getFilePath("artists2.csv");
    action.destinationFileFolder[0] = getFilePath("alreadythere.csv");

    action.execute(new Result(), 1);

    assertThatFileExists(CONTAINER_NAME, "artists2.csv");
    assertThatFileExists(CONTAINER_NAME, "alreadythere.csv");
  }

  @Test
  void whenTargetFileAlreadyExistsAndExplicitlySettingDoNothing_thenDoNotRename()
      throws HopException {

    ActionMoveFiles action = defaultAction();
    IWorkflowEngine<WorkflowMeta> parentWorkflow = new LocalWorkflowEngine();

    action.sourceFileFolder[0] = getFilePath("artists2.csv");
    action.destinationFileFolder[0] = getFilePath("alreadythere.csv");
    action.setIfFileExists("do_nothing");
    action.execute(new Result(), 1);

    assertThatFileExists(CONTAINER_NAME, "artists2.csv");
    assertThatFileExists(CONTAINER_NAME, "alreadythere.csv");
  }

  @Test
  void whenTargetFileAlreadyExistsAndOverwriteIsChosen_thenDoRename() throws HopException {

    ActionMoveFiles action = defaultAction();
    action.sourceFileFolder[0] = getFilePath("artists3.csv");
    action.destinationFileFolder[0] = getFilePath("canbeoverwritten.csv");
    action.setIfFileExists("overwrite_file");

    action.execute(new Result(), 1);

    assertThatFileDoesNotExist(CONTAINER_NAME, "artists3.csv");
    assertThatFileExists(CONTAINER_NAME, "canbeoverwritten.csv");
  }

  @Test
  void whenDestinationIsFileIsNotFlaggedAndFileSpecifiedAsDestination_shouldNotMove()
      throws HopException {
    ActionMoveFiles action = defaultAction();
    action.sourceFileFolder[0] = getFilePath("artists3.csv");
    action.destinationFileFolder[0] = getFolderPath();
    action.setDestinationIsAFile(false);

    action.execute(new Result(), 1);

    assertThatFileExists(CONTAINER_NAME, "artists3.csv");
    assertThatFileDoesNotExist(ANOTHER_CONTAINER_NAME, "artists3.csv");
  }

  @Test
  void whenCreateDestinationFolderNotFlagged_shouldNotMoveFile() throws HopException {
    ActionMoveFiles action = defaultAction();
    action.sourceFileFolder[0] = getFilePath("artists3.csv");
    action.destinationFileFolder[0] = getFilePath("fakecontainer", "artists-do-not-create-folder");
    action.setCreateDestinationFolder(false);

    action.execute(new Result(), 1);

    assertThatFileExists(CONTAINER_NAME, "artists3.csv");
    assertThatFileDoesNotExist("fakecontainer", "artists3.csv");
  }

  @Test
  void whenUseWildCards_thenMoveFiles() throws HopException {
    ActionMoveFiles action = defaultAction();
    action.sourceFileFolder[0] = getFolderPath(CONTAINER_NAME);
    action.destinationFileFolder[0] = getFolderPath(ANOTHER_CONTAINER_NAME);
    action.wildcard[0] = ".*wildcard.*.csv";
    action.destinationIsAFile = false;
    action.execute(new Result(), 1);

    assertThatFileExists(ANOTHER_CONTAINER_NAME, "artists-wildcard1.csv");
    assertThatFileExists(ANOTHER_CONTAINER_NAME, "artists-wildcard2.csv");

    assertThatFileDoesNotExist(CONTAINER_NAME, "artists-wildcard1.csv");
    assertThatFileDoesNotExist(CONTAINER_NAME, "artists-wildcard2.csv");
  }

  @AfterClass
  public static void shutDown() {
    // azuriteContainer.stop();
  }

  private void deleteFiles(String containerName)
      throws URISyntaxException, InvalidKeyException, StorageException {
    final BlobContainerClient container = blobServiceClient.getBlobContainerClient(containerName);
    container.listBlobs().forEach(blob -> container.getBlobClient(blob.getName()).deleteIfExists());
  }

  @After
  public void tearDown() throws Exception {
    // Clean up the Hop environment
    HopEnvironment.shutdown();
  }

  private void uploadFileIntoContainer(String containerName, String... fileNames)
      throws URISyntaxException, StorageException, IOException {

    for (String fileName : fileNames) {
      BlobServiceClient blobServiceClient =
          new BlobServiceClientBuilder().connectionString(CONNECTION_STRING).buildClient();

      blobServiceClient.createBlobContainerIfNotExists(containerName);
      final BlobContainerClient blobContainerClient =
          blobServiceClient.getBlobContainerClient(CONTAINER_NAME);
      BlobClient blobClient =
          blobServiceClient.getBlobContainerClient(containerName).getBlobClient(fileName);
      AppendBlobClient appendBlobClient = blobClient.getAppendBlobClient();
      appendBlobClient.create();

      try (OutputStream outputStream = appendBlobClient.getBlobOutputStream()) {

        // Path to the file you want to upload
        Path filePath = Paths.get("src/test/resources/" + fileName);

        // Write the file to the blob
        Files.copy(filePath, outputStream);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private void assertThatFileExists(String containerName, String blobName) {
    // Get a BlobClient object which represents a blob in the container
    BlobClient blobClient =
        blobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobName);

    // Get the properties of the blob
    var blobProperties = blobClient.getProperties();

    // Get the size of the blob
    long blobSize = blobProperties.getBlobSize();

    assertEquals(true, blobClient.exists());
    assertTrue(blobSize >= 140);
  }

  private void assertThatFileDoesNotExist(String containerName, String blobName) {
    // Get a BlobClient object which represents a blob in the container
    BlobClient blobClient =
        blobServiceClient.getBlobContainerClient(containerName).getBlobClient(blobName);

    assertEquals(false, blobClient.exists());
  }

  private String getFilePath(String filename) {
    return basePath + CONTAINER_NAME + "/" + filename;
  }

  private String getFilePath(String container, String filename) {
    return basePath + container + "/" + filename;
  }

  private String getFolderPath() {
    return basePath + ANOTHER_CONTAINER_NAME + "/";
  }

  private String getFolderPath(String container) {
    return basePath + container;
  }

  private static void loadAzureProperties() {
    Properties prop = new Properties();
    String host = null; // azuriteContainer.getContainerIpAddress();
    Integer port = null; // azuriteContainer.getMappedPort(10000);
    try (InputStream input =
        AzureMoveFilesIT.class.getClassLoader().getResourceAsStream("azure.properties")) {
      prop.load(input);
      useTestContainer = Boolean.parseBoolean(prop.getProperty("testcontainers.enabled", "true"));
      account = prop.getProperty("account");
      key = prop.getProperty("key");
      emulatorUrl = useTestContainer ? String.format("http://%s:%d", host, port) : "";
      CONNECTION_STRING =
          useTestContainer
              ? String.format(
                  "AccountName=%s;AccountKey=%s;DefaultEndpointsProtocol=http;BlobEndpoint=http://%s:%d;",
                  AZURITE_DEFAULT_ACCOUNT, AZURITE_DEFAULT_KEY, host, port)
              : String.format(
                  "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net;",
                  account, key);
      setAzureConfiguration(account, key, emulatorUrl);
    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }

  private static void setAzureConfiguration(
      String accountName, String accountKey, String emulatorUrl) {
    AzureConfig config = new AzureConfig();
    config.setAccount(accountName);
    config.setKey(accountKey);
    config.setEmulatorUrl(emulatorUrl);
    HopConfig.getInstance().saveOption("azure", config);
  }
}
