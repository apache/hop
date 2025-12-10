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

package org.apache.hop.neo4j.transforms.importer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.neo4j.transforms.gencsv.StreamConsumer;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class Importer extends BaseTransform<ImporterMeta, ImporterData> {

  public static final String CONST_FALSE = "false";

  public Importer(
      TransformMeta transformMeta,
      ImporterMeta meta,
      ImporterData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] row = getRow();
    if (row == null) {
      if ((!Utils.isEmpty(data.nodesFiles)) || (!Utils.isEmpty(data.relsFiles))) {
        runImport();
      }
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      data.nodesFiles = new ArrayList<>();
      data.relsFiles = new ArrayList<>();

      data.filenameFieldIndex = getInputRowMeta().indexOfValue(meta.getFilenameField());
      if (data.filenameFieldIndex < 0) {
        throw new HopException(
            "Unable to find filename field "
                + meta.getFilenameField()
                + "' in the transform input");
      }
      data.fileTypeFieldIndex = getInputRowMeta().indexOfValue(meta.getFileTypeField());
      if (data.fileTypeFieldIndex < 0) {
        throw new HopException(
            "Unable to find file type field "
                + meta.getFileTypeField()
                + "' in the transform input");
      }

      if (StringUtils.isEmpty(meta.getAdminCommand())) {
        data.adminCommand = "neo4j-admin";
      } else {
        data.adminCommand = resolve(meta.getAdminCommand());
      }

      data.databaseFilename = resolve(meta.getDatabaseName());
      data.reportFile = resolve(meta.getReportFile());

      data.baseFolder = resolve(meta.getBaseFolder());
      if (!data.baseFolder.endsWith(File.separator)) {
        data.baseFolder += File.separator;
      }

      data.importFolder = data.baseFolder + "import/";

      data.badTolerance = resolve(meta.getBadTolerance());
      data.readBufferSize = resolve(meta.getReadBufferSize());
      data.maxMemory = resolve(meta.getMaxMemory());
      data.processors = resolve(meta.getProcessors());
    }

    String filename = getInputRowMeta().getString(row, data.filenameFieldIndex);
    String fileType = getInputRowMeta().getString(row, data.fileTypeFieldIndex);

    if (StringUtils.isNotEmpty(filename) && StringUtils.isNotEmpty(fileType)) {

      if ("Node".equalsIgnoreCase(fileType)
          || "Nodes".equalsIgnoreCase(fileType)
          || "N".equalsIgnoreCase(fileType)) {
        data.nodesFiles.add(filename);
      }
      if ("Relationship".equalsIgnoreCase(fileType)
          || "Relationships".equalsIgnoreCase(fileType)
          || "Rel".equalsIgnoreCase(fileType)
          || "Rels".equalsIgnoreCase(fileType)
          || "R".equalsIgnoreCase(fileType)
          || "Edge".equalsIgnoreCase(fileType)
          || "Edges".equalsIgnoreCase(fileType)
          || "E".equalsIgnoreCase(fileType)) {
        data.relsFiles.add(filename);
      }
    }

    // Pay it forward
    //
    putRow(getInputRowMeta(), row);

    return true;
  }

  private void runImport() throws HopException {

    // See if we need to delete the existing database folder...
    String targetDbFolder = data.baseFolder + "data/databases/" + data.databaseFilename;
    try {
      if (new File(targetDbFolder).exists()) {
        logBasic("Removing existing folder: " + targetDbFolder);
        FileUtils.deleteDirectory(new File(targetDbFolder));
      }
    } catch (Exception e) {
      throw new HopException(
          "Unable to remove old database files from '" + targetDbFolder + "'", e);
    }

    List<String> arguments = new ArrayList<>();

    // Determine Neo4j version (default to 4.x for backward compatibility)
    String neo4jVersion =
        StringUtils.isNotEmpty(meta.getNeo4jVersion()) ? meta.getNeo4jVersion() : "4.x";
    boolean isNeo4j5 = "5.x".equals(neo4jVersion) || neo4jVersion.startsWith("5.");

    // Build command based on Neo4j version
    if (isNeo4j5) {
      // Neo4j 5.0+ syntax: neo4j-admin database import full <database> [options] --nodes=...
      // --relationships=...
      arguments.add(data.adminCommand);
      arguments.add("database");
      arguments.add("import");
      arguments.add("full");
      arguments.add(data.databaseFilename); // Database name comes right after "full"
      if (meta.isOverwriteDestination()) {
        arguments.add("--overwrite-destination"); // Allow overwriting existing database
      }
      arguments.add("--id-type=STRING");
      arguments.add("--report-file=" + data.reportFile);
    } else {
      // Neo4j 4.x syntax: neo4j-admin import --database=name [options]
      arguments.add(data.adminCommand);
      arguments.add("import");
      arguments.add("--database=" + data.databaseFilename);
      arguments.add("--id-type=STRING");
      arguments.add("--report-file=" + data.reportFile);
    }

    // Options that are only valid in Neo4j 4.x (deprecated in 5.0)
    if (!isNeo4j5) {
      arguments.add("--high-io=" + (meta.isHighIo() ? "true" : CONST_FALSE));
      arguments.add("--cache-on-heap=" + (meta.isHighIo() ? "true" : CONST_FALSE));
      if (StringUtils.isNotEmpty(data.maxMemory)) {
        arguments.add("--max-memory=" + data.maxMemory);
      }
    }

    // Options valid in both versions
    arguments.add(
        "--ignore-empty-strings=" + (meta.isIgnoringEmptyStrings() ? "true" : CONST_FALSE));
    arguments.add(
        "--ignore-extra-columns=" + (meta.isIgnoringExtraColumns() ? "true" : CONST_FALSE));
    arguments.add("--legacy-style-quoting=" + (meta.isQuotingLegacyStyle() ? "true" : CONST_FALSE));
    arguments.add("--multiline-fields=" + (meta.isMultiLine() ? "true" : CONST_FALSE));
    arguments.add("--normalize-types=" + (meta.isNormalizingTypes() ? "true" : CONST_FALSE));
    arguments.add(
        "--skip-duplicate-nodes=" + (meta.isSkippingDuplicateNodes() ? "true" : CONST_FALSE));
    arguments.add(
        "--skip-bad-relationships=" + (meta.isSkippingBadRelationships() ? "true" : CONST_FALSE));
    arguments.add("--trim-strings=" + (meta.isTrimmingStrings() ? "true" : CONST_FALSE));

    if (StringUtils.isNotEmpty(data.badTolerance)) {
      arguments.add("--bad-tolerance=" + data.badTolerance);
    }
    if (StringUtils.isNotEmpty(data.readBufferSize)) {
      arguments.add("--read-buffer-size=" + data.readBufferSize);
    }

    // Finally specify the nodes and relationship files
    //
    for (String nodesFile : data.nodesFiles) {
      arguments.add("--nodes=" + nodesFile);
    }
    for (String relsFile : data.relsFiles) {
      arguments.add("--relationships=" + relsFile);
    }

    StringBuffer command = new StringBuffer();
    for (String argument : arguments) {
      command.append(argument).append(" ");
    }
    logBasic("Running command : " + command);
    logBasic("Running from base folder: " + data.baseFolder);

    ProcessBuilder pb = new ProcessBuilder(arguments);
    pb.directory(new File(data.baseFolder));
    try {
      Process process = pb.start();

      StreamConsumer errorConsumer =
          new StreamConsumer(getLogChannel(), process.getErrorStream(), LogLevel.ERROR);
      errorConsumer.start();
      StreamConsumer outputConsumer =
          new StreamConsumer(getLogChannel(), process.getInputStream(), LogLevel.BASIC);
      outputConsumer.start();

      // Wait for the process to complete
      int exitCode = process.waitFor();

      // Wait for stream consumers to finish reading output
      try {
        errorConsumer.join(5000); // Wait up to 5 seconds for error stream
      } catch (InterruptedException e) {
        // Ignore interruption
      }
      try {
        outputConsumer.join(5000); // Wait up to 5 seconds for output stream
      } catch (InterruptedException e) {
        // Ignore interruption
      }

      // Check exit code and throw exception if import failed
      if (exitCode != 0) {
        String guidance =
            "Neo4j import command failed with exit code "
                + exitCode
                + ". Common issues:\n"
                + "1. Database must be stopped before import. "
                + "For Community Edition, use 'neo4j stop' to stop Neo4j entirely. "
                + "For Enterprise Edition, use Cypher 'STOP DATABASE <name>' to stop the specific database.";
        logError(guidance);
        throw new HopException(
            "Neo4j import failed with exit code " + exitCode + ". See error log for details.");
      }
    } catch (Exception e) {
      throw new HopException("Error running command: " + arguments, e);
    }
  }
}
