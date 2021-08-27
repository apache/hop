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

package org.apache.hop.imp;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;

import javax.xml.transform.dom.DOMSource;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

public abstract class HopImportBase implements IHopImport {
  protected ILogChannel log;
  protected IVariables variables;

  protected String inputFolderName;
  protected String outputFolderName;
  protected String sharedXmlFilename;
  protected String kettlePropertiesFilename;
  protected String jdbcPropertiesFilename;
  protected boolean skippingExistingTargetFiles;
  protected String targetConfigFilename;
  protected boolean skippingHiddenFilesAndFolders;
  protected boolean skippingFolders;

  protected TreeMap<String, String> connectionFileMap;
  protected List<DatabaseMeta> connectionsList;
  protected IVariables collectedVariables;

  protected FileObject inputFolder;
  protected FileObject outputFolder;

  protected HashMap<String, DOMSource> migratedFilesMap;

  protected int connectionCounter;
  protected int variableCounter;

  protected IHopMetadataProvider metadataProvider;
  protected IProgressMonitor monitor;
  protected String metadataTargetFolder;

  public HopImportBase() {
    this.variables = new Variables();
    this.log = LogChannel.GENERAL;

    targetConfigFilename = "imported-env-conf.json";
    connectionsList = new ArrayList<>();
    connectionFileMap = new TreeMap<>();
    migratedFilesMap = new HashMap<>();
    collectedVariables = new Variables();
  }

  @Override
  public void init(IVariables variables, ILogChannel log) throws HopException {
    this.variables = variables;
    this.log = log;
  }

  @Override
  public void runImport(IProgressMonitor monitor) throws HopException {
    this.monitor = monitor;

    // Create a new metadata provider for the target folder...
    //
    if (metadataProvider == null) {
      this.metadataTargetFolder = outputFolder.getName().getURI() + "/metadata";
      metadataProvider =
          new JsonMetadataProvider(Encr.getEncoder(), this.metadataTargetFolder, variables);
    }
    if (monitor != null) {
      monitor.setTaskName("Finding files to import");
    }
    findFilesToImport();

    if (monitor != null) {
      if (monitor.isCanceled()) {
        return;
      }
      monitor.worked(1);
      monitor.setTaskName("Importing files");
    }
    importFiles();
    if (monitor != null) {
      if (monitor.isCanceled()) {
        return;
      }
      monitor.worked(1);
      monitor.setTaskName("Importing connections");
    }
    importConnections();
    if (monitor != null) {
      if (monitor.isCanceled()) {
        return;
      }
      monitor.worked(1);
      monitor.setTaskName("Importing variables");
    }
    importVariables();
    if (monitor != null) {
      monitor.worked(1);
    }
  }

  @Override
  public abstract void importFiles() throws HopException;

  public abstract void findFilesToImport() throws HopException;

  @Override
  public abstract void importConnections() throws HopException;

  @Override
  public abstract void importVariables() throws HopException;

  protected void collectVariablesFromKettleProperties() throws HopException {
    try {
      Properties properties = new Properties();
      FileObject varFile = HopVfs.getFileObject(kettlePropertiesFilename);
      InputStream inputStream = HopVfs.getInputStream(varFile);
      properties.load(inputStream);

      properties.forEach(
          (k, v) -> {
            collectedVariables.setVariable((String) k, (String) v);
            variableCounter++;
            log.logDetailed("Saved variable " + (String) k + ": " + (String) v);
          });
    } catch (Exception e) {
      throw new HopException("Error collecting variables from file " + kettlePropertiesFilename, e);
    }
  }

  public void addDatabaseMeta(String filename, DatabaseMeta databaseMeta) throws HopException {
    // build a list of all jobs, transformations with their connections
    connectionFileMap.put(filename, databaseMeta.getName());

    // only add new connection names to the list
    if (connectionsList.stream()
            .filter(dbMeta -> dbMeta.getName().equals(databaseMeta.getName()))
            .collect(Collectors.toList())
            .size()
        == 0) {
      connectionsList.add(databaseMeta);
      connectionCounter++;
    }
  }

  @Override
  public FileObject getInputFolder() {
    return inputFolder;
  }

  @Override
  public void setValidateInputFolder(String inputFolderName) throws HopException {
    try {
      inputFolder = HopVfs.getFileObject(inputFolderName);
      if (!inputFolder.exists() || !inputFolder.isFolder()) {
        throw new HopException(
            "input folder '" + inputFolderName + "' doesn't exist or is not a folder.");
      }
      this.inputFolderName = inputFolder.getName().getURI();
    } catch (Exception e) {
      throw new HopFileException("Error verifying input folder " + inputFolderName, e);
    }
  }

  @Override
  public FileObject getOutputFolder() {
    return outputFolder;
  }

  @Override
  public void setValidateOutputFolder(String outputFolderName) throws HopException {
    this.outputFolder = HopVfs.getFileObject(outputFolderName);
    try {
      if (!outputFolder.exists() || !outputFolder.isFolder()) {
        log.logBasic("output folder '" + outputFolderName + "' doesn't exist or is not a folder.");
        outputFolder.createFolder();
      }
      this.outputFolderName = outputFolder.getName().getURI();
    } catch (Exception e) {
      throw new HopFileException("Error setting output folder " + outputFolderName, e);
    }
  }

  /**
   * Gets inputFolderName
   *
   * @return value of inputFolderName
   */
  @Override
  public String getInputFolderName() {
    return inputFolderName;
  }

  /** @param inputFolderName The inputFolderName to set */
  public void setInputFolderName(String inputFolderName) {
    this.inputFolderName = inputFolderName;
  }

  /**
   * Gets outputFolderName
   *
   * @return value of outputFolderName
   */
  @Override
  public String getOutputFolderName() {
    return outputFolderName;
  }

  /** @param outputFolderName The outputFolderName to set */
  public void setOutputFolderName(String outputFolderName) {
    this.outputFolderName = outputFolderName;
  }

  /**
   * Gets connectionFileList
   *
   * @return value of connectionFileList
   */
  public TreeMap<String, String> getConnectionFileMap() {
    return connectionFileMap;
  }

  /** @param connectionFileMap The connectionFileList to set */
  public void setConnectionFileMap(TreeMap<String, String> connectionFileMap) {
    this.connectionFileMap = connectionFileMap;
  }

  /**
   * Gets connectionsList
   *
   * @return value of connectionsList
   */
  public List<DatabaseMeta> getConnectionsList() {
    return connectionsList;
  }

  /** @param connectionsList The connectionsList to set */
  public void setConnectionsList(List<DatabaseMeta> connectionsList) {
    this.connectionsList = connectionsList;
  }

  /** @param inputFolder The inputFolder to set */
  public void setInputFolder(FileObject inputFolder) {
    this.inputFolder = inputFolder;
  }

  /** @param outputFolder The outputFolder to set */
  public void setOutputFolder(FileObject outputFolder) {
    this.outputFolder = outputFolder;
  }

  /**
   * Gets log
   *
   * @return value of log
   */
  public ILogChannel getLog() {
    return log;
  }

  /**
   * Gets migratedFilesMap
   *
   * @return value of migratedFilesMap
   */
  public HashMap<String, DOMSource> getMigratedFilesMap() {
    return migratedFilesMap;
  }

  /** @param migratedFilesMap The migratedFilesMap to set */
  public void setMigratedFilesMap(HashMap<String, DOMSource> migratedFilesMap) {
    this.migratedFilesMap = migratedFilesMap;
  }

  /**
   * Gets connectionCounter
   *
   * @return value of connectionCounter
   */
  public int getConnectionCounter() {
    return connectionCounter;
  }

  /** @param connectionCounter The connectionCounter to set */
  public void setConnectionCounter(int connectionCounter) {
    this.connectionCounter = connectionCounter;
  }

  /**
   * Gets variableCounter
   *
   * @return value of variableCounter
   */
  public int getVariableCounter() {
    return variableCounter;
  }

  /** @param variableCounter The variableCounter to set */
  public void setVariableCounter(int variableCounter) {
    this.variableCounter = variableCounter;
  }

  /**
   * Gets skippingExistingTargetFiles
   *
   * @return value of skippingExistingTargetFiles
   */
  @Override
  public boolean isSkippingExistingTargetFiles() {
    return skippingExistingTargetFiles;
  }

  /** @param skippingExistingTargetFiles The skippingExistingTargetFiles to set */
  @Override
  public void setSkippingExistingTargetFiles(boolean skippingExistingTargetFiles) {
    this.skippingExistingTargetFiles = skippingExistingTargetFiles;
  }

  /**
   * Gets sharedXmlFilename
   *
   * @return value of sharedXmlFilename
   */
  @Override
  public String getSharedXmlFilename() {
    return sharedXmlFilename;
  }

  /** @param sharedXmlFilename The sharedXmlFilename to set */
  @Override
  public void setSharedXmlFilename(String sharedXmlFilename) {
    this.sharedXmlFilename = sharedXmlFilename;
  }

  /**
   * Gets kettlePropertiesFilename
   *
   * @return value of kettlePropertiesFilename
   */
  @Override
  public String getKettlePropertiesFilename() {
    return kettlePropertiesFilename;
  }

  /** @param kettlePropertiesFilename The kettlePropertiesFilename to set */
  @Override
  public void setKettlePropertiesFilename(String kettlePropertiesFilename) {
    this.kettlePropertiesFilename = kettlePropertiesFilename;
  }

  /**
   * Gets jdbcPropertiesFilename
   *
   * @return value of jdbcPropertiesFilename
   */
  @Override
  public String getJdbcPropertiesFilename() {
    return jdbcPropertiesFilename;
  }

  /** @param jdbcPropertiesFilename The jdbcPropertiesFilename to set */
  @Override
  public void setJdbcPropertiesFilename(String jdbcPropertiesFilename) {
    this.jdbcPropertiesFilename = jdbcPropertiesFilename;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  @Override
  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /** @param metadataProvider The metadataProvider to set */
  @Override
  public void setMetadataProvider(IHopMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }

  /**
   * Gets targetConfigFilename
   *
   * @return value of targetConfigFilename
   */
  @Override
  public String getTargetConfigFilename() {
    return targetConfigFilename;
  }

  /** @param targetConfigFilename The targetConfigFilename to set */
  @Override
  public void setTargetConfigFilename(String targetConfigFilename) {
    this.targetConfigFilename = targetConfigFilename;
  }

  /**
   * Gets collectedVariables
   *
   * @return value of collectedVariables
   */
  public IVariables getCollectedVariables() {
    return collectedVariables;
  }

  /** @param collectedVariables The collectedVariables to set */
  public void setCollectedVariables(IVariables collectedVariables) {
    this.collectedVariables = collectedVariables;
  }

  /**
   * Gets monitor
   *
   * @return value of monitor
   */
  public IProgressMonitor getMonitor() {
    return monitor;
  }

  /** @param monitor The monitor to set */
  public void setMonitor(IProgressMonitor monitor) {
    this.monitor = monitor;
  }

  /**
   * Gets metadataTargetFolder
   *
   * @return value of metadataTargetFolder
   */
  public String getMetadataTargetFolder() {
    return metadataTargetFolder;
  }

  /** @param metadataTargetFolder The metadataTargetFolder to set */
  public void setMetadataTargetFolder(String metadataTargetFolder) {
    this.metadataTargetFolder = metadataTargetFolder;
  }

  /**
   * Gets skippingHiddenFilesAndFolders
   *
   * @return value of skippingHiddenFilesAndFolders
   */
  @Override
  public boolean isSkippingHiddenFilesAndFolders() {
    return skippingHiddenFilesAndFolders;
  }

  /** @param skippingHiddenFilesAndFolders The skippingHiddenFilesAndFolders to set */
  @Override
  public void setSkippingHiddenFilesAndFolders(boolean skippingHiddenFilesAndFolders) {
    this.skippingHiddenFilesAndFolders = skippingHiddenFilesAndFolders;
  }

  /**
   * Gets skippingFolders
   *
   * @return value of skippingFolders
   */
  @Override
  public boolean isSkippingFolders() {
    return skippingFolders;
  }

  /** @param skippingFolders The skippingFolders to set */
  @Override
  public void setSkippingFolders(boolean skippingFolders) {
    this.skippingFolders = skippingFolders;
  }

  /**
   * Generate a report with statistics and advice.
   *
   * @return The import report
   */
  @Override
  public abstract String getImportReport();
}
