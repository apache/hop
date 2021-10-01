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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;

public interface IHopImport {

  /**
   * Initialize the importer
   *
   * @param variables
   * @param log
   * @throws HopException
   */
  void init(IVariables variables, ILogChannel log) throws HopException;

  /**
   * Perform the actual import of all files
   *
   * @param monitor
   */
  void runImport(IProgressMonitor monitor) throws HopException;

  /**
   * Import all files from the specified source folder to the target
   *
   * @throws HopException
   */
  void importFiles() throws HopException;

  /**
   * Import connections from the specified source files/folders
   *
   * @throws HopException
   */
  void importConnections() throws HopException;

  /**
   * Generate an import report in text format with statistics and advice.
   *
   * @return The import report
   */
  String getImportReport();

  /**
   * Import variables from the specified source files/folders
   *
   * @throws HopException
   */
  void importVariables() throws HopException;

  void setValidateInputFolder(String inputFolderName) throws HopException;

  FileObject getInputFolder();

  void setValidateOutputFolder(String outputFolderName) throws HopException;

  FileObject getOutputFolder();

  String getInputFolderName();

  String getOutputFolderName();

  /**
   * Gets skippingExistingTargetFiles
   *
   * @return value of skippingExistingTargetFiles
   */
  boolean isSkippingExistingTargetFiles();

  /** @param skippingExistingTargetFiles The skippingExistingTargetFiles to set */
  void setSkippingExistingTargetFiles(boolean skippingExistingTargetFiles);

  /**
   * Gets sharedXmlFilename
   *
   * @return value of sharedXmlFilename
   */
  String getSharedXmlFilename();

  /** @param sharedXmlFilename The sharedXmlFilename to set */
  void setSharedXmlFilename(String sharedXmlFilename);

  /**
   * Gets kettlePropertiesFilename
   *
   * @return value of kettlePropertiesFilename
   */
  String getKettlePropertiesFilename();

  /** @param kettlePropertiesFilename The kettlePropertiesFilename to set */
  void setKettlePropertiesFilename(String kettlePropertiesFilename);
  /**
   * Gets jdbcPropertiesFilename
   *
   * @return value of jdbcPropertiesFilename
   */
  String getJdbcPropertiesFilename();

  /** @param jdbcPropertiesFilename The jdbcPropertiesFilename to set */
  void setJdbcPropertiesFilename(String jdbcPropertiesFilename);

  /**
   * Gets targetConfigFilename
   *
   * @return value of targetConfigFilename
   */
  String getTargetConfigFilename();

  /**
   * Gets skippingHiddenFilesAndFolders
   *
   * @return value of skippingHiddenFilesAndFolders
   */
  boolean isSkippingHiddenFilesAndFolders();

  /** @param skippingHiddenFilesAndFolders The skippingHiddenFilesAndFolders to set */
  void setSkippingHiddenFilesAndFolders(boolean skippingHiddenFilesAndFolders);

  /** @param targetConfigFilename The targetConfigFilename to set */
  void setTargetConfigFilename(String targetConfigFilename);

  boolean isSkippingFolders();

  /** @param skippingFolders The skippingFolders to set */
  void setSkippingFolders(boolean skippingFolders);

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  MultiMetadataProvider getMetadataProvider();

  /** @param metadataProvider The metadataProvider to set */
  void setMetadataProvider(MultiMetadataProvider metadataProvider);
}
