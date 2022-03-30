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

package org.apache.hop.neo4j.transforms.importer;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
    id = "Neo4jImport",
    name = "i18n::ImporterMeta.name",
    description = "i18n::ImporterMeta.description",
    image = "neo4j_import.svg",
    categoryDescription = "i18n::ImporterMeta.categoryDescription",
    keywords = "i18n::ImporterMeta.keyword",
    documentationUrl = "/pipeline/transforms/neo4j-import.html")
public class ImporterMeta extends BaseTransformMeta<Importer, ImporterData> {

  @HopMetadataProperty(key = "filename_field_name")
  protected String filenameField;

  @HopMetadataProperty(key = "file_type_field_name")
  protected String fileTypeField;

  @HopMetadataProperty(key = "base_folder")
  protected String baseFolder;

  @HopMetadataProperty(key = "admin_command")
  protected String adminCommand;

  @HopMetadataProperty(key = "report_file")
  protected String reportFile;

  @HopMetadataProperty(key = "db_name")
  protected String databaseName;

  @HopMetadataProperty protected boolean verbose;

  @HopMetadataProperty(key = "high_io")
  protected boolean highIo;

  @HopMetadataProperty(key = "cache_on_heap")
  protected boolean cacheOnHeap;

  @HopMetadataProperty(key = "ignore_empty_strings")
  protected boolean ignoringEmptyStrings;

  @HopMetadataProperty(key = "ignore_extra_columns")
  protected boolean ignoringExtraColumns;

  @HopMetadataProperty(key = "legacy_style_quoting")
  protected boolean quotingLegacyStyle;

  @HopMetadataProperty(key = "multi_line")
  protected boolean multiLine;

  @HopMetadataProperty(key = "normalize_types")
  protected boolean normalizingTypes;

  @HopMetadataProperty(key = "skip_bad_entries_logging")
  protected boolean skippingBadEntriesLogging;

  @HopMetadataProperty(key = "skip_bad_relationships")
  protected boolean skippingBadRelationships;

  @HopMetadataProperty(key = "skip_duplicate_nodes")
  protected boolean skippingDuplicateNodes;

  @HopMetadataProperty(key = "trim_strings")
  protected boolean trimmingStrings;

  @HopMetadataProperty(key = "bad_tolerance")
  protected String badTolerance;

  @HopMetadataProperty(key = "max_memory")
  protected String maxMemory;

  @HopMetadataProperty(key = "read_buffer_size")
  protected String readBufferSize;

  @HopMetadataProperty protected String processors;

  public ImporterMeta() {
    databaseName = "neo4j";
    adminCommand = "neo4j-admin";
    baseFolder = "/var/lib/neo4j/";
    reportFile = "import.report";

    ignoringExtraColumns = false;
    multiLine = false;
    ignoringEmptyStrings = false;
    trimmingStrings = false;
    quotingLegacyStyle = false;
    readBufferSize = "4M";
    maxMemory = "90%";
    highIo = true;
    cacheOnHeap = false;
    badTolerance = "1000";
    skippingBadEntriesLogging = false;
    skippingBadRelationships = false;
    skippingDuplicateNodes = false;
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextStep,
      IVariables space,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    // No fields are added by default
  }

  /**
   * Gets filenameField
   *
   * @return value of filenameField
   */
  public String getFilenameField() {
    return filenameField;
  }

  /** @param filenameField The filenameField to set */
  public void setFilenameField(String filenameField) {
    this.filenameField = filenameField;
  }

  /**
   * Gets fileTypeField
   *
   * @return value of fileTypeField
   */
  public String getFileTypeField() {
    return fileTypeField;
  }

  /** @param fileTypeField The fileTypeField to set */
  public void setFileTypeField(String fileTypeField) {
    this.fileTypeField = fileTypeField;
  }

  /**
   * Gets baseFolder
   *
   * @return value of baseFolder
   */
  public String getBaseFolder() {
    return baseFolder;
  }

  /** @param baseFolder The baseFolder to set */
  public void setBaseFolder(String baseFolder) {
    this.baseFolder = baseFolder;
  }

  /**
   * Gets adminCommand
   *
   * @return value of adminCommand
   */
  public String getAdminCommand() {
    return adminCommand;
  }

  /** @param adminCommand The adminCommand to set */
  public void setAdminCommand(String adminCommand) {
    this.adminCommand = adminCommand;
  }

  /**
   * Gets reportFile
   *
   * @return value of reportFile
   */
  public String getReportFile() {
    return reportFile;
  }

  /** @param reportFile The reportFile to set */
  public void setReportFile(String reportFile) {
    this.reportFile = reportFile;
  }

  /**
   * Gets maxMemory
   *
   * @return value of maxMemory
   */
  public String getMaxMemory() {
    return maxMemory;
  }

  /** @param maxMemory The maxMemory to set */
  public void setMaxMemory(String maxMemory) {
    this.maxMemory = maxMemory;
  }

  /**
   * Gets ignoringDuplicateNodes
   *
   * @return value of ignoringDuplicateNodes
   */
  public boolean isSkippingDuplicateNodes() {
    return skippingDuplicateNodes;
  }

  /** @param skippingDuplicateNodes The ignoringDuplicateNodes to set */
  public void setSkippingDuplicateNodes(boolean skippingDuplicateNodes) {
    this.skippingDuplicateNodes = skippingDuplicateNodes;
  }

  /**
   * Gets ignoringExtraColumns
   *
   * @return value of ignoringExtraColumns
   */
  public boolean isIgnoringExtraColumns() {
    return ignoringExtraColumns;
  }

  /** @param ignoringExtraColumns The ignoringExtraColumns to set */
  public void setIgnoringExtraColumns(boolean ignoringExtraColumns) {
    this.ignoringExtraColumns = ignoringExtraColumns;
  }

  /**
   * Gets highIo
   *
   * @return value of highIo
   */
  public boolean isHighIo() {
    return highIo;
  }

  /** @param highIo The highIo to set */
  public void setHighIo(boolean highIo) {
    this.highIo = highIo;
  }

  /**
   * Gets databaseFilename
   *
   * @return value of databaseFilename
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /** @param databaseName The databaseFilename to set */
  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  /**
   * Gets multiLine
   *
   * @return value of multiLine
   */
  public boolean isMultiLine() {
    return multiLine;
  }

  /** @param multiLine The multiLine to set */
  public void setMultiLine(boolean multiLine) {
    this.multiLine = multiLine;
  }

  /**
   * Gets skippingBadRelationships
   *
   * @return value of skippingBadRelationships
   */
  public boolean isSkippingBadRelationships() {
    return skippingBadRelationships;
  }

  /** @param skippingBadRelationships The skippingBadRelationships to set */
  public void setSkippingBadRelationships(boolean skippingBadRelationships) {
    this.skippingBadRelationships = skippingBadRelationships;
  }

  /**
   * Gets readBufferSize
   *
   * @return value of readBufferSize
   */
  public String getReadBufferSize() {
    return readBufferSize;
  }

  /** @param readBufferSize The readBufferSize to set */
  public void setReadBufferSize(String readBufferSize) {
    this.readBufferSize = readBufferSize;
  }

  /**
   * Gets cacheOnHeap
   *
   * @return value of cacheOnHeap
   */
  public boolean isCacheOnHeap() {
    return cacheOnHeap;
  }

  /** @param cacheOnHeap The cacheOnHeap to set */
  public void setCacheOnHeap(boolean cacheOnHeap) {
    this.cacheOnHeap = cacheOnHeap;
  }

  /**
   * Gets ignoringEmptyStrings
   *
   * @return value of ignoringEmptyStrings
   */
  public boolean isIgnoringEmptyStrings() {
    return ignoringEmptyStrings;
  }

  /** @param ignoringEmptyStrings The ignoringEmptyStrings to set */
  public void setIgnoringEmptyStrings(boolean ignoringEmptyStrings) {
    this.ignoringEmptyStrings = ignoringEmptyStrings;
  }

  /**
   * Gets quotingLegacyStyle
   *
   * @return value of quotingLegacyStyle
   */
  public boolean isQuotingLegacyStyle() {
    return quotingLegacyStyle;
  }

  /** @param quotingLegacyStyle The quotingLegacyStyle to set */
  public void setQuotingLegacyStyle(boolean quotingLegacyStyle) {
    this.quotingLegacyStyle = quotingLegacyStyle;
  }

  /**
   * Gets normalizingTypes
   *
   * @return value of normalizingTypes
   */
  public boolean isNormalizingTypes() {
    return normalizingTypes;
  }

  /** @param normalizingTypes The normalizingTypes to set */
  public void setNormalizingTypes(boolean normalizingTypes) {
    this.normalizingTypes = normalizingTypes;
  }

  /**
   * Gets skippingBadEntriesLogging
   *
   * @return value of skippingBadEntriesLogging
   */
  public boolean isSkippingBadEntriesLogging() {
    return skippingBadEntriesLogging;
  }

  /** @param skippingBadEntriesLogging The skippingBadEntriesLogging to set */
  public void setSkippingBadEntriesLogging(boolean skippingBadEntriesLogging) {
    this.skippingBadEntriesLogging = skippingBadEntriesLogging;
  }

  /**
   * Gets trimmingStrings
   *
   * @return value of trimmingStrings
   */
  public boolean isTrimmingStrings() {
    return trimmingStrings;
  }

  /** @param trimmingStrings The trimmingStrings to set */
  public void setTrimmingStrings(boolean trimmingStrings) {
    this.trimmingStrings = trimmingStrings;
  }

  /**
   * Gets badTollerance
   *
   * @return value of badTollerance
   */
  public String getBadTolerance() {
    return badTolerance;
  }

  /** @param badTolerance The badTollerance to set */
  public void setBadTolerance(String badTolerance) {
    this.badTolerance = badTolerance;
  }

  /**
   * Gets processors
   *
   * @return value of processors
   */
  public String getProcessors() {
    return processors;
  }

  /** @param processors The processors to set */
  public void setProcessors(String processors) {
    this.processors = processors;
  }

  /**
   * Gets verbose
   *
   * @return value of verbose
   */
  public boolean isVerbose() {
    return verbose;
  }

  /** @param verbose The verbose to set */
  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }
}
