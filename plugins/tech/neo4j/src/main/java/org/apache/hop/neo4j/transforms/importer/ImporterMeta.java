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
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
    id = "Neo4jImport",
    name = "Neo4j Import",
    description = "Runs an import command using the provided CSV files ",
    image = "neo4j_import.svg",
    categoryDescription = "Neo4j",
    documentationUrl = "https://hop.apache.org/manual/latest/pipeline/transforms/neo4j-import.html")
public class ImporterMeta extends BaseTransformMeta
    implements ITransformMeta<Importer, ImporterData> {

  public static final String DB_NAME = "db_name";
  public static final String FILENAME_FIELD = "filename_field_name";
  public static final String FILE_TYPE_FIELD = "file_type_field_name";
  public static final String BASE_FOLDER = "base_folder";
  public static final String REPORT_FILE = "report_file";
  public static final String ADMIN_COMMAND = "admin_command";

  public static final String VERBOSE = "verbose";
  public static final String HIGH_IO = "high_io";
  public static final String CACHE_ON_HEAP = "cache_on_heap";
  public static final String IGNORE_EMPTY_STRINGS = "ignore_empty_strings";
  public static final String IGNORE_EXTRA_COLUMNS = "ignore_extra_columns";
  public static final String LEGACY_STYLE_QUOTING = "legacy_style_quoting";
  public static final String MULTI_LINE = "multi_line";
  public static final String NORMALIZE_TYPES = "normalize_types";
  public static final String SKIP_BAD_ENTRIES_LOGGING = "skip_bad_entries_logging";
  public static final String SKIP_BAD_RELATIONSHIPS = "skip_bad_relationships";
  public static final String SKIP_DUPLICATE_NODES = "skip_duplicate_nodes";
  public static final String TRIM_STRINGS = "trim_strings";
  public static final String BAD_TOLERANCE = "bad_tolerance";
  public static final String MAX_MEMORY = "max_memory";
  public static final String READ_BUFFER_SIZE = "read_buffer_size";
  public static final String PROCESSORS = "processors";

  protected String filenameField;
  protected String fileTypeField;
  protected String baseFolder;
  protected String adminCommand;
  protected String reportFile;
  protected String databaseName;

  protected boolean verbose;
  protected boolean highIo;
  protected boolean cacheOnHeap;
  protected boolean ignoringEmptyStrings;
  protected boolean ignoringExtraColumns;
  protected boolean quotingLegacyStyle;
  protected boolean multiLine;
  protected boolean normalizingTypes;
  protected boolean skippingBadEntriesLogging;
  protected boolean skippingBadRelationships;
  protected boolean skippingDuplicateNodes;
  protected boolean trimmingStrings;
  protected String badTolerance;
  protected String maxMemory;
  protected String readBufferSize;
  protected String processors;

  @Override
  public void setDefault() {
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

  @Override
  public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer();
    xml.append(XmlHandler.addTagValue(FILENAME_FIELD, filenameField));
    xml.append(XmlHandler.addTagValue(FILE_TYPE_FIELD, fileTypeField));
    xml.append(XmlHandler.addTagValue(ADMIN_COMMAND, adminCommand));
    xml.append(XmlHandler.addTagValue(BASE_FOLDER, baseFolder));
    xml.append(XmlHandler.addTagValue(REPORT_FILE, reportFile));
    xml.append(XmlHandler.addTagValue(DB_NAME, databaseName));

    xml.append(XmlHandler.addTagValue(VERBOSE, verbose));
    xml.append(XmlHandler.addTagValue(HIGH_IO, highIo));
    xml.append(XmlHandler.addTagValue(CACHE_ON_HEAP, cacheOnHeap));
    xml.append(XmlHandler.addTagValue(IGNORE_EMPTY_STRINGS, ignoringEmptyStrings));
    xml.append(XmlHandler.addTagValue(IGNORE_EXTRA_COLUMNS, ignoringExtraColumns));
    xml.append(XmlHandler.addTagValue(LEGACY_STYLE_QUOTING, quotingLegacyStyle));
    xml.append(XmlHandler.addTagValue(MULTI_LINE, multiLine));
    xml.append(XmlHandler.addTagValue(NORMALIZE_TYPES, normalizingTypes));
    xml.append(XmlHandler.addTagValue(SKIP_BAD_ENTRIES_LOGGING, skippingBadEntriesLogging));
    xml.append(XmlHandler.addTagValue(SKIP_BAD_RELATIONSHIPS, skippingBadRelationships));
    xml.append(XmlHandler.addTagValue(MAX_MEMORY, maxMemory));
    xml.append(XmlHandler.addTagValue(TRIM_STRINGS, trimmingStrings));
    xml.append(XmlHandler.addTagValue(BAD_TOLERANCE, badTolerance));
    xml.append(XmlHandler.addTagValue(MAX_MEMORY, maxMemory));
    xml.append(XmlHandler.addTagValue(READ_BUFFER_SIZE, readBufferSize));
    xml.append(XmlHandler.addTagValue(PROCESSORS, processors));
    return xml.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    filenameField = XmlHandler.getTagValue(transformNode, FILENAME_FIELD);
    fileTypeField = XmlHandler.getTagValue(transformNode, FILE_TYPE_FIELD);
    adminCommand = XmlHandler.getTagValue(transformNode, ADMIN_COMMAND);
    baseFolder = XmlHandler.getTagValue(transformNode, BASE_FOLDER);
    reportFile = XmlHandler.getTagValue(transformNode, REPORT_FILE);
    databaseName = XmlHandler.getTagValue(transformNode, DB_NAME);

    highIo = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, HIGH_IO));
    cacheOnHeap = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, CACHE_ON_HEAP));
    ignoringEmptyStrings =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, IGNORE_EMPTY_STRINGS));
    ignoringExtraColumns =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, IGNORE_EXTRA_COLUMNS));
    quotingLegacyStyle =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, LEGACY_STYLE_QUOTING));
    multiLine = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, MULTI_LINE));
    normalizingTypes = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, NORMALIZE_TYPES));
    skippingBadEntriesLogging =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, SKIP_BAD_ENTRIES_LOGGING));
    skippingBadRelationships =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, SKIP_BAD_RELATIONSHIPS));
    skippingDuplicateNodes =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, SKIP_DUPLICATE_NODES));
    trimmingStrings = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, TRIM_STRINGS));
    badTolerance = XmlHandler.getTagValue(transformNode, BAD_TOLERANCE);
    maxMemory = XmlHandler.getTagValue(transformNode, MAX_MEMORY);
    readBufferSize = XmlHandler.getTagValue(transformNode, READ_BUFFER_SIZE);
    processors = XmlHandler.getTagValue(transformNode, PROCESSORS);
  }

  @Override
  public Importer createTransform(
      TransformMeta transformMeta,
      ImporterData iTransformData,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new Importer(transformMeta, this, iTransformData, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public ImporterData getTransformData() {
    return new ImporterData();
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
