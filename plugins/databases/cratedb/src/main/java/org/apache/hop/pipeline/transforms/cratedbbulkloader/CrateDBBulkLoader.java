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

package org.apache.hop.pipeline.transforms.cratedbbulkloader;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.cratedbbulkloader.http.BulkImportClient;
import org.apache.hop.pipeline.transforms.cratedbbulkloader.http.HttpBulkImportResponse;
import org.apache.hop.pipeline.transforms.cratedbbulkloader.http.exceptions.CrateDBHopException;

public class CrateDBBulkLoader extends BaseTransform<CrateDBBulkLoaderMeta, CrateDBBulkLoaderData> {
  private static final Class<?> PKG =
      CrateDBBulkLoader.class; // for i18n purposes, needed by Translator2!!
  public static final String TIMESTAMP_CONVERSION_MASK = "yyyy-MM-dd HH:mm:ss.SSS";
  public static final String DATE_CONVERSION_MASK = "yyyy-MM-dd";
  public static final String NUMBER_CONVERSION_MASK = "#############0.##############";

  /** Size of the buffer we put in front of the (remote) output stream. */
  private static final int OUTPUT_BUFFER_SIZE = 128 * 1024;

  /** Placeholder written instead of the AWS credentials when the COPY statement is logged. */
  private static final String CREDENTIALS_MASK = "<credentials hidden>";

  /**
   * Only built when the HTTP endpoint is actually used, and from resolved values: at construction
   * time the transform's variables are not available yet.
   */
  private BulkImportClient bulkImportClient;

  public CrateDBBulkLoader(
      TransformMeta transformMeta,
      CrateDBBulkLoaderMeta meta,
      CrateDBBulkLoaderData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {

    if (super.init()) {
      try {
        // Validating that the connection and the load settings have been defined.
        verifyDatabaseConnection();
        verifyLoadSettings();
        data.databaseMeta = this.getPipelineMeta().findDatabase(meta.getConnection(), variables);

        if (meta.isUseHttpEndpoint()) {
          bulkImportClient =
              new BulkImportClient(
                  resolve(meta.getHttpEndpoint()),
                  resolve(meta.getHttpLogin()),
                  resolve(meta.getHttpPassword()));
          data.maxBatchSize = resolveBatchSize();
        } else if (meta.isStreamToS3Csv()) {
          String readFromFilename = resolve(meta.getReadFromFilename());
          String localPath = resolve(meta.getVolumeMapping());
          String target =
              !StringUtils.isEmpty(localPath) && isURIOfScheme(readFromFilename, Scheme.FILE)
                  ? FilenameUtils.concat(localPath, extractFilename(readFromFilename))
                  : readFromFilename;
          ensureParentFolderExists(target);
          // Every field is written as a separate chunk of bytes, so a buffer in front of the
          // (remote) stream matters a lot here.
          data.writer =
              new BufferedOutputStream(
                  HopVfs.getOutputStream(target, false, variables), OUTPUT_BUFFER_SIZE);
        }

        data.db = new Database(this, this, data.databaseMeta);
        data.db.connect();
        getDbFields();
        verifyTableFields();

        if (isBasic()) {
          logBasic(
              BaseMessages.getString(
                  PKG, "CrateDBBulkLoader.Connection.Connected", data.db.getDatabaseMeta()));
        }
        initBinaryDataFields();

        data.db.setAutoCommit(false);

        return true;
      } catch (HopException e) {
        logError("An error occurred initializing this transform: " + e.getMessage());
        stopAll();
        setErrors(1);
      }
    }
    return false;
  }

  private boolean isURIOfScheme(String uriStr, Scheme expectedScheme) throws HopException {

    URI uri = null;
    try {
      uri = new URI(uriStr);
      return expectedScheme.name().equalsIgnoreCase(uri.getScheme());
    } catch (URISyntaxException e) {
      throw new HopException(e);
    }
  }

  private String extractFilename(String uriStr) throws HopException {
    URI uri = null;
    try {
      uri = new URI(uriStr);
      return FilenameUtils.getName(uri.getPath());
    } catch (URISyntaxException e) {
      throw new HopException(e);
    }
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // this also waits for a previous transform to be finished.

    if (r == null) { // no more input to be expected...
      endOfStream();
      return false;
    }

    if (first) {
      first = false;
      data.rowsReceived = true;

      if (meta.isTruncateTable()) {
        truncateTable();
      }

      prepareRowMapping();
    }

    if (meta.isUseHttpEndpoint()) {
      appendRowAsJsonLine(r);
      try {
        writeIfBatchSizeRecordsAreReached();
      } catch (IOException | CrateDBHopException e) {
        throw new HopException(e);
      }
    } else if (meta.isStreamToS3Csv()) {
      writeRowToFile(r);
    }
    putRow(data.outputRowMeta, r);

    return true;
  }

  /**
   * Flush whatever is left: close the CSV file and fire the COPY statement, or send the last HTTP
   * batch.
   *
   * @throws HopException in case the load failed or resources could not be released
   */
  private void endOfStream() throws HopException {
    if (!data.rowsReceived) {
      if (meta.isTruncateTable() && !meta.isOnlyWhenHaveRows()) {
        truncateTable();
      }
      return;
    }

    if (meta.isUseHttpEndpoint()) {
      try {
        writeBatchToCrateDB();
      } catch (IOException | CrateDBHopException e) {
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        throw new HopException(e);
      }
      return;
    }

    // The file has to be complete before CrateDB reads it.
    if (!closeFile()) {
      setErrors(1);
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      throw new HopTransformException("Error releasing resources");
    }

    try {
      String copyStmt = buildCopyStatementSqlString(false);
      if (isDetailed()) {
        logDetailed("Copy stmt: " + buildCopyStatementSqlString(true));
      }
      int errorCount = 0;
      try (Statement stmt = data.db.getConnection().createStatement();
          ResultSet resultSet = stmt.executeQuery(copyStmt)) {
        while (resultSet.next()) {
          String node = resultSet.getString("node");
          String uri = resultSet.getString("uri");
          int successCount = resultSet.getInt("success_count");
          errorCount = resultSet.getInt("error_count");
          String errors = resultSet.getString("errors");
          logError(
              "Node: "
                  + node
                  + " URI: "
                  + uri
                  + " Success Count: "
                  + successCount
                  + " Error Count: "
                  + errorCount
                  + " Errors: "
                  + errors);
          incrementLinesOutput(successCount);
          incrementLinesRejected(errorCount);
        }
      }
      data.db.commit();
      if (errorCount > 0) {
        throw new HopException(
            "Failed to COPY FROM CSV file to CrateDB: " + errorCount + " rows failed");
      }
    } catch (SQLException sqle) {
      setErrors(1);
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      throw new HopDatabaseException("Error executing COPY statements", sqle);
    }
  }

  /**
   * Resolve, once, which field of the input row ends up in which column, together with the value
   * meta used to render it. Doing this per row is what used to make this transform slow: it cloned
   * the whole input row meta twice for every row.
   *
   * @throws HopException in case a configured field is not present on the input stream
   */
  void prepareRowMapping() throws HopException {
    IRowMeta inputRowMeta = getInputRowMeta();
    data.outputRowMeta = inputRowMeta.clone();
    data.insertRowMeta = new RowMeta();

    if (meta.isSpecifyFields()) {
      List<CrateDBBulkLoaderField> fields = meta.getFields();
      data.selectedRowFieldIndices = new int[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        CrateDBBulkLoaderField field = fields.get(i);
        int index = inputRowMeta.indexOfValue(field.getStreamField());
        if (index < 0) {
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "CrateDBBulkLoader.Exception.FieldRequired", field.getStreamField()));
        }
        data.selectedRowFieldIndices[i] = index;

        IValueMeta insertValueMeta = inputRowMeta.getValueMeta(index).clone();
        insertValueMeta.setName(field.getDatabaseField());
        data.insertRowMeta.addValueMeta(insertValueMeta);
      }
    } else {
      // Take the whole input row, the columns are named after the fields on the stream.
      data.selectedRowFieldIndices = new int[inputRowMeta.size()];
      for (int i = 0; i < inputRowMeta.size(); i++) {
        data.selectedRowFieldIndices[i] = i;
        data.insertRowMeta.addValueMeta(inputRowMeta.getValueMeta(i).clone());
      }
    }

    // Both the COPY statement and the HTTP bulk insert name these columns.
    data.columnNames = data.insertRowMeta.getFieldNames();
  }

  private void incrementLinesRejected(int count) {
    for (int i = 0; i < count; i++) {
      incrementLinesRejected();
    }
  }

  private void incrementLinesOutput(int count) {
    for (int i = 0; i < count; i++) {
      incrementLinesOutput();
    }
  }

  private void writeIfBatchSizeRecordsAreReached()
      throws HopException, CrateDBHopException, IOException {
    if (data.httpBulkArgs.size() >= data.maxBatchSize) {
      writeBatchToCrateDB();
    }
  }

  /**
   * Resolve the batch size once, at init time, instead of parsing it again for every row.
   *
   * @return the number of rows to send in one HTTP bulk request
   * @throws HopException when the configured batch size is not a positive number
   */
  int resolveBatchSize() throws HopException {
    String batchSize = resolve(meta.getBatchSize());
    String expandedBatchSize = Const.expandIntegerString(batchSize);
    try {
      int size = Integer.parseInt(expandedBatchSize != null ? expandedBatchSize : batchSize);
      if (size <= 0) {
        throw new NumberFormatException(batchSize);
      }
      return size;
    } catch (NumberFormatException | NullPointerException e) {
      throw new HopException(
          BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.Error.InvalidBatchSize", batchSize));
    }
  }

  private void writeBatchToCrateDB() throws HopException, CrateDBHopException, IOException {
    if (data.httpBulkArgs.isEmpty()) {
      return;
    }
    try {
      final HttpBulkImportResponse httpResponse =
          bulkImportClient.batchInsert(
              resolve(meta.getSchemaName()),
              resolve(meta.getTableName()),
              data.columnNames,
              data.httpBulkArgs);

      for (int i = 0; i < httpResponse.outputRows(); i++) {
        incrementLinesOutput();
      }
      for (int i = 0; i < httpResponse.rejectedRows(); i++) {
        incrementLinesRejected();
      }
      switch (httpResponse.statusCode()) {
        case 200:
          data.httpBulkArgs.clear();
          break;
        case 401:
          throw new HopException("Unauthorized access to CrateDB");
        default:
          throw new HopException("Error sending bulk import request");
      }
      if (200 == httpResponse.statusCode()) {
        data.httpBulkArgs.clear();
      } else {
        throw new HopException("Error sending bulk import request");
      }
    } catch (JsonProcessingException e) {
      throw new HopException("Error sending bulk import request ", e);
    }
  }

  private void appendRowAsJsonLine(Object[] row) throws HopTransformException {
    Object[] args = new Object[data.insertRowMeta.size()];
    try {
      for (int i = 0; i < data.insertRowMeta.size(); i++) {
        IValueMeta v = data.insertRowMeta.getValueMeta(i);
        args[i] = convertDatatypeIfNeeded(v, row[data.selectedRowFieldIndices[i]], i);
      }

      data.convertedRowMetaReady = true;

      data.httpBulkArgs.add(args);
    } catch (Exception e) {
      throw new HopTransformException("Error writing JSON line to file", e);
    }
  }

  private String convertDatatypeIfNeeded(IValueMeta v, Object rowItem, int pos)
      throws HopException {
    IValueMeta vc = null;
    String convertedValue = null;

    if (!data.convertedRowMetaReady && data.convertedRowMeta == null) {
      data.convertedRowMeta = data.insertRowMeta.clone();
    }

    // Whatever comes out of here is text, so the row meta the value is written with has to say so
    // as well. Leaving the original meta in place made writing a number fail outright: by then the
    // value was a String and the meta still claimed an Integer.
    vc = new ValueMetaString(v.getName());

    if (rowItem != null) {
      switch (v.getType()) {
        case IValueMeta.TYPE_STRING:
          // The value can be stored as a binary string, so never cast it straight to String.
          convertedValue = v.getString(rowItem);
          break;
        case IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER:
          convertedValue = String.valueOf(rowItem);
          break;
        case IValueMeta.TYPE_TIMESTAMP:
          v.setConversionMask(TIMESTAMP_CONVERSION_MASK);
          vc.setConversionMask(TIMESTAMP_CONVERSION_MASK);
          convertedValue = (String) vc.convertData(v, rowItem);
          break;
        case IValueMeta.TYPE_DATE:
          v.setConversionMask(DATE_CONVERSION_MASK);
          vc.setConversionMask(DATE_CONVERSION_MASK);
          convertedValue = (String) vc.convertData(v, rowItem);
          break;
        default:
          convertedValue = v.getString(rowItem);
          break;
      }
    }
    if (isDetailed()) {
      logDetailed("Field: " + v.getName() + " - Converted Value: " + convertedValue);
    }

    if (!data.convertedRowMetaReady) {
      data.convertedRowMeta.setValueMeta(pos, vc);
    }

    return convertedValue;
  }

  /**
   * Closes a file so that its file handle is no longer open
   *
   * @return true if we successfully closed the file
   */
  private boolean closeFile() {
    boolean returnValue = false;

    try {
      if (data.writer != null) {
        data.writer.flush();
        data.writer.close();
      }
      data.writer = null;
      if (isDebug()) {
        logDebug("Closing normal file ...");
      }

      returnValue = true;
    } catch (Exception e) {
      logError("Exception trying to close file: " + e.toString());
      setErrors(1);
      returnValue = false;
    }
    return returnValue;
  }

  /**
   * Build the CrateDB COPY statement for the CSV file.
   *
   * @param maskCredentials when true the AWS credentials are replaced by a placeholder, so the
   *     statement can safely be written to the log
   * @return the COPY statement
   */
  String buildCopyStatementSqlString(boolean maskCredentials) {
    final DatabaseMeta databaseMeta = data.db.getDatabaseMeta();

    StringBuilder sb = new StringBuilder(150);
    sb.append("COPY ");

    sb.append(
        databaseMeta.getQuotedSchemaTableCombination(
            variables,
            data.db.resolve(meta.getSchemaName()),
            data.db.resolve(meta.getTableName())));

    // The CSV file has no header, so CrateDB needs to be told which columns it holds.
    String[] columns = copyColumnNames();
    if (columns.length > 0) {
      sb.append(" (");
      for (int i = 0; i < columns.length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(columns[i]);
      }
      sb.append(")");
    }

    sb.append(" FROM '").append(buildCopyFromUri(maskCredentials)).append("'");
    sb.append(" WITH (format='csv', wait_for_completion=true");
    sb.append(", header=false");
    sb.append(", delimiter='" + CrateDBBulkLoaderMeta.DEFAULT_CSV_DELIMITER + "'");
    sb.append(")");
    sb.append(" RETURN SUMMARY");

    return sb.toString();
  }

  /**
   * CrateDB reads S3 credentials from the URI itself, so they have to be spliced into the file
   * name.
   *
   * @param maskCredentials when true the credentials are replaced by a placeholder
   * @return the URI the COPY statement reads from
   */
  String[] copyColumnNames() {
    if (meta.isSpecifyFields()) {
      return meta.getFields().stream()
          .map(CrateDBBulkLoaderField::getDatabaseField)
          .toArray(String[]::new);
    }
    if (meta.isStreamToS3Csv() && data.columnNames != null) {
      return data.columnNames;
    }
    return new String[0];
  }

  private String buildCopyFromUri(boolean maskCredentials) {
    String filename = Const.NVL(resolve(meta.getReadFromFilename()), "");

    int schemeEnd = filename.indexOf("://");
    if (schemeEnd < 0) {
      // Not a URI at all: nothing to splice credentials into.
      return filename;
    }
    String scheme = filename.substring(0, schemeEnd);
    String rest = filename.substring(schemeEnd + 3);

    if (!"s3".equals(scheme)) {
      return filename;
    }
    if (maskCredentials) {
      return scheme + "://" + CREDENTIALS_MASK + "@" + rest;
    }

    String awsAccessKeyId;
    String awsSecretAccessKey;
    if (meta.isUseSystemEnvVars()) {
      awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
      awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    } else {
      awsAccessKeyId = resolve(meta.getAwsAccessKeyId());
      awsSecretAccessKey = resolve(meta.getAwsSecretAccessKey());
    }
    String credentials = Const.NVL(awsAccessKeyId, "") + ":" + Const.NVL(awsSecretAccessKey, "");

    // No credentials configured: let CrateDB fall back to its own configuration.
    return ":".equals(credentials)
        ? scheme + "://" + rest
        : scheme + "://" + credentials + "@" + rest;
  }

  /**
   * Runs a desc table to get the fields, and field types from the database. Uses a desc table as
   * opposed to the select * from table limit 0 that Hop normally uses to get the fields and types,
   * due to the need to handle the Time type. The select * method through Hop does not give us the
   * ability to differentiate time from timestamp.
   *
   * @throws HopException
   */
  private void getDbFields() throws HopException {
    data.dbFields = new ArrayList<>();

    String schemaName = resolve(meta.getSchemaName());
    String tableName = resolve(meta.getTableName());

    IRowMeta rowMeta =
        StringUtils.isEmpty(schemaName)
            ? data.db.getTableFields(tableName)
            : data.db.getTableFields(schemaName + "." + tableName);
    try {
      if (rowMeta == null || rowMeta.isEmpty()) {
        throw new HopException("No fields found in table");
      }

      for (int i = 0; i < rowMeta.size(); i++) {
        String field[] = new String[2];
        field[0] = rowMeta.getValueMeta(i).getName().toUpperCase();
        field[1] = rowMeta.getValueMeta(i).getTypeDesc().toUpperCase();
        data.dbFields.add(field);
      }
    } catch (Exception ex) {
      throw new HopException("Error getting database fields", ex);
    }
  }

  protected void verifyDatabaseConnection() throws HopException {
    // Confirming Database Connection is defined.
    if (meta.getConnection() == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.Error.NoConnection"));
    }
  }

  /**
   * Make sure the folder the staging file goes in exists. This file is the transform's own
   * business, so there is nothing to ask the user about: on S3 a prefix only exists while an object
   * sits under it, and a brand new path would otherwise be refused with "Parent directory ... does
   * not exist".
   *
   * @param filename the file about to be written
   * @throws HopException when the folder is missing and cannot be created
   */
  void ensureParentFolderExists(String filename) throws HopException {
    try {
      FileObject parent = HopVfs.getFileObject(filename, variables).getParent();
      if (parent == null || parent.exists()) {
        return;
      }
      parent.createFolder();
      if (isDetailed()) {
        logDetailed("Created the parent folder " + HopVfs.getFriendlyURI(parent));
      }
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "CrateDBBulkLoaderMeta.Error.CannotCreateParentFolder", filename),
          e);
    }
  }

  /**
   * The HTTP endpoint needs a URL and the COPY statement needs a file, so fail before any row is
   * read when either is missing.
   *
   * @throws HopException when the load settings are incomplete
   */
  protected void verifyLoadSettings() throws HopException {
    if (meta.isUseHttpEndpoint()) {
      if (StringUtils.isEmpty(resolve(meta.getHttpEndpoint()))) {
        throw new HopException(
            BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.Error.NoHttpEndpoint"));
      }
    } else if (StringUtils.isEmpty(resolve(meta.getReadFromFilename()))) {
      throw new HopException(
          BaseMessages.getString(PKG, "CrateDBBulkLoaderMeta.Error.NoReadFromFilename"));
    }
  }

  /**
   * Fail before any row is read when a column was mapped to a table column that does not exist. The
   * COPY statement would reject it anyway, but only after the whole file was written.
   *
   * @throws HopException when a selected column is not a column of the target table
   */
  protected void verifyTableFields() throws HopException {
    if (!meta.isSpecifyFields()) {
      return;
    }
    for (CrateDBBulkLoaderField field : meta.getFields()) {
      boolean found = false;
      for (String[] dbField : data.dbFields) {
        if (dbField[0].equalsIgnoreCase(field.getDatabaseField())) {
          found = true;
          break;
        }
      }
      if (!found) {
        throw new HopException(
            "Field [" + field.getDatabaseField() + "] couldn't be found in the table!");
      }
    }
  }

  /**
   * Initialize the binary values of delimiters, enclosures, and escape characters
   *
   * @throws HopException
   */
  void initBinaryDataFields() throws HopException {
    try {
      data.binarySeparator = new byte[] {};
      data.binaryEnclosure = new byte[] {};
      data.binaryNewline = new byte[] {};
      data.escapeCharacters = new byte[] {};

      data.binarySeparator =
          resolve(CrateDBBulkLoaderMeta.DEFAULT_CSV_DELIMITER).getBytes(StandardCharsets.UTF_8);
      data.binaryEnclosure =
          resolve(CrateDBBulkLoaderMeta.ENCLOSURE).getBytes(StandardCharsets.UTF_8);
      data.binaryNewline =
          CrateDBBulkLoaderMeta.CSV_RECORD_DELIMITER.getBytes(StandardCharsets.UTF_8);
      data.escapeCharacters =
          CrateDBBulkLoaderMeta.CSV_ESCAPE_CHAR.getBytes(StandardCharsets.UTF_8);

      data.binaryNullValue = "".getBytes(StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new HopException("Unexpected error while encoding binary fields", e);
    }
  }

  /**
   * Writes an individual row of data to a temp file
   *
   * @param rowMeta The metadata about the row
   * @param row The input row
   * @throws HopTransformException
   */
  void writeRowToFile(Object[] row) throws HopTransformException {

    try {
      byte[] nullString = meta.isSpecifyFields() ? data.binaryNullValue : null;
      for (int i = 0; i < data.insertRowMeta.size(); i++) {
        if (i > 0) {
          data.writer.write(data.binarySeparator);
        }

        IValueMeta v = data.insertRowMeta.getValueMeta(i);
        Object convertedValue = convertDatatypeIfNeeded(v, row[data.selectedRowFieldIndices[i]], i);
        writeField(
            data.convertedRowMeta.getValueMeta(i), convertedValue, nullString, enclosedByType(v));
      }
      data.convertedRowMetaReady = true;
      data.writer.write(data.binaryNewline);
    } catch (Exception e) {
      throw new HopTransformException("Error writing line", e);
    }
  }

  /**
   * Writes an individual field to the temp file.
   *
   * @param v The metadata about the column
   * @param valueData The data for the column
   * @param nullString The bytes to put in the temp file if the value is null
   * @throws HopTransformException
   */
  /**
   * @param v the value meta of the field on the stream, before it was rendered as text
   * @return true if a value of this type is enclosed whatever it holds
   */
  private boolean enclosedByType(IValueMeta v) {
    // A number or a boolean can never hold a separator, a quote or a line break.
    return !v.isNumeric() && v.getType() != IValueMeta.TYPE_BOOLEAN;
  }

  private void writeField(IValueMeta v, Object valueData, byte[] nullString, boolean enclosedByType)
      throws HopTransformException {
    try {
      byte[] str;

      // First check whether or not we have a null string set
      // These values should be set when a null value passes
      //
      if (nullString != null && v.isNull(valueData)) {
        str = nullString;
      } else {
        str = formatField(v, valueData);
      }

      if (str != null && str.length > 0) {
        // Strings are always enclosed, the COPY statement reads the same quote character.
        // Anything else is enclosed only when its own content would otherwise break the row --
        // JSON above all, which Hop does not treat as a string yet is full of commas and quotes.
        if (enclosedByType || needsEnclosure(str)) {
          data.writer.write(data.binaryEnclosure);
          writeEscaped(str);
          data.writer.write(data.binaryEnclosure);
        } else {
          data.writer.write(str);
        }
      }
    } catch (Exception e) {
      throw new HopTransformException("Error writing field content to file", e);
    }
  }

  /**
   * Takes an input field and converts it to bytes to be stored in the temp file.
   *
   * @param v The metadata about the column
   * @param valueData The column data
   * @return The bytes for the value
   * @throws HopValueException
   */
  private byte[] formatField(IValueMeta v, Object valueData) throws HopValueException {
    if (v.isString()) {
      if (v.isStorageBinaryString()
          && v.getTrimType() == IValueMeta.TRIM_TYPE_NONE
          && v.getLength() < 0
          && StringUtils.isEmpty(v.getStringEncoding())) {
        return (byte[]) valueData;
      } else {
        String svalue = (valueData instanceof String string) ? string : v.getString(valueData);

        // trim or cut to size if needed.
        //
        return convertStringToBinaryString(v, Const.trimToType(svalue, v.getTrimType()));
      }
    } else {
      return v.getBinaryString(valueData);
    }
  }

  /**
   * Converts an input string to the bytes for the string
   *
   * @param v The metadata about the column
   * @param string The column data
   * @return The bytes for the value
   * @throws HopValueException
   */
  private byte[] convertStringToBinaryString(IValueMeta v, String string) {
    int length = v.getLength();

    if (string == null) {
      return new byte[] {};
    }

    if (length > -1 && length < string.length()) {
      // we need to truncate
      String tmp = string.substring(0, length);
      return tmp.getBytes(StandardCharsets.UTF_8);

    } else {
      byte[] text;
      text = string.getBytes(StandardCharsets.UTF_8);

      if (length > string.length()) {
        // we need to pad this

        int size = 0;
        byte[] filler;
        filler = " ".getBytes(StandardCharsets.UTF_8);
        size = text.length + filler.length * (length - string.length());

        byte[] bytes = new byte[size];
        System.arraycopy(text, 0, bytes, 0, text.length);
        if (filler.length == 1) {
          java.util.Arrays.fill(bytes, text.length, size, filler[0]);
        } else {
          int currIndex = text.length;
          for (int i = 0; i < (length - string.length()); i++) {
            for (byte aFiller : filler) {
              bytes[currIndex++] = aFiller;
            }
          }
        }
        return bytes;
      } else {
        // do not need to pad or truncate
        return text;
      }
    }
  }

  /**
   * Whether a value has to be enclosed to survive the trip through the CSV file: it is only safe to
   * write bare when it holds no separator, no quote and no line break.
   *
   * @param str The bytes of the value
   * @return true if the value must be enclosed
   */
  private boolean needsEnclosure(byte[] str) {
    byte separator = data.binarySeparator.length > 0 ? data.binarySeparator[0] : 0;
    byte enclosure = data.binaryEnclosure.length > 0 ? data.binaryEnclosure[0] : 0;

    for (byte b : str) {
      if (b == separator || b == enclosure || b == '\n' || b == '\r') {
        return true;
      }
    }
    return false;
  }

  /**
   * Write the value, doubling every occurrence of the enclosure character so the CSV file stays
   * readable for the COPY statement. Only bytes that actually need escaping are copied separately,
   * a value without enclosures is written in one go.
   *
   * @param str The bytes of the value
   * @throws IOException in case the value could not be written
   */
  private void writeEscaped(byte[] str) throws IOException {
    if (data.binaryEnclosure.length == 0) {
      data.writer.write(str);
      return;
    }
    byte enclosure = data.binaryEnclosure[0];

    int from = 0;
    for (int i = 0; i < str.length; i++) {
      if (str[i] == enclosure) {
        data.writer.write(str, from, i - from);
        data.writer.write(data.escapeCharacters); // write the enclosure a second time
        from = i;
      }
    }
    data.writer.write(str, from, str.length - from);
  }

  @Override
  public void stopRunning() throws HopException {
    setStopped(true);
    super.stopRunning();
  }

  void truncateTable() throws HopDatabaseException {
    if ((getCopy() == 0) || !Utils.isEmpty(getPartitionId())) {
      data.db.truncateTable(resolve(meta.getSchemaName()), resolve(meta.getTableName()));
    }
  }

  @Override
  public void dispose() {

    setOutputDone();

    // The file is normally closed before the COPY statement runs, this catches the error paths.
    closeFile();

    if (data.db != null) {
      try {
        if (getErrors() > 0) {
          data.db.rollback();
        }
      } catch (HopDatabaseException e) {
        logError("Unexpected error rolling back the database connection.", e);
      }
      data.db.disconnect();
    }
    super.dispose();
  }
}
