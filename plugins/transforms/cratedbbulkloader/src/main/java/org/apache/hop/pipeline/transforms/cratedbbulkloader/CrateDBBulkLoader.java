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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
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

  private final BulkImportClient bulkImportClient =
      new BulkImportClient(meta.getHttpEndpoint(), meta.getHttpLogin(), meta.getHttpPassword());

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
        // Validating that the connection has been defined.
        verifyDatabaseConnection();
        data.databaseMeta = this.getPipelineMeta().findDatabase(meta.getConnection(), variables);

        if (meta.isStreamToS3Csv() && !meta.isUseHttpEndpoint()) {
          String readFromFilename = resolve(meta.getReadFromFilename());
          String localPath = resolve(meta.getVolumeMapping());
          if (!StringUtils.isEmpty(localPath) && isURIOfScheme(readFromFilename, Scheme.FILE)) {
            data.writer =
                HopVfs.getOutputStream(
                    FilenameUtils.concat(localPath, extractFilename(readFromFilename)), false);
          } else {
            data.writer = HopVfs.getOutputStream(readFromFilename, false);
          }
          // get the file output stream to write to S3
        }

        data.db = new Database(this, this, data.databaseMeta);
        data.db.connect();
        getDbFields();

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
      if (first && meta.isTruncateTable() && !meta.isOnlyWhenHaveRows()) {
        truncateTable();
      }

      if (!first) {
        try {
          data.close();
          closeFile();
          if (meta.isUseHttpEndpoint()) {
            String[] columns =
                meta.getFields().stream()
                    .map(CrateDBBulkLoaderField::getDatabaseField)
                    .toArray(String[]::new);
            data.outputRowMeta.getValueMetaList().forEach(v -> logBasic(v.toString()));
            String schema = meta.getSchemaName();
            String table = meta.getTableName();
            writeBatchToCrateDB(schema, table, columns);
          } else {
            String copyStmt = buildCopyStatementSqlString();
            Connection conn = data.db.getConnection();
            Statement stmt = conn.createStatement();
            final ResultSet resultSet = stmt.executeQuery(copyStmt);
            int errorCount = 0;
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
            conn.commit();
            stmt.close();
            conn.close();
            if (errorCount > 0) {
              throw new HopException(
                  "Failed to COPY FROM CSV file to CrateDB: " + errorCount + " rows failed");
            }
          }

        } catch (SQLException sqle) {
          setErrors(1);
          stopAll();
          setOutputDone(); // signal end to receiver(s)
          throw new HopDatabaseException("Error executing COPY statements", sqle);
        } catch (IOException ioe) {
          setErrors(1);
          stopAll();
          setOutputDone(); // signal end to receiver(s)
          throw new HopTransformException("Error releasing resources", ioe);
        } catch (CrateDBHopException e) {
          throw new HopException(e);
        }
      }

      return false;
    }

    if (first) {
      first = false;
      if (meta.isStreamToS3Csv()) {

        data.fieldnrs = new HashMap<>();

        meta.getFields(data.insertRowMeta, getTransformName(), null, null, this, metadataProvider);

        if (!meta.specifyFields()) {
          // write all fields in the stream to CrateDB
          // Just take the whole input row
          data.insertRowMeta = getInputRowMeta().clone();
          data.selectedRowFieldIndices = new int[data.insertRowMeta.size()];
          // TODO Serasoft
          // Is the statement below really needed??
          try {
            getDbFields();
          } catch (HopException e) {
            logError("Error getting database fields", e);
            setErrors(1);
            stopAll();
            setOutputDone(); // signal end to receiver(s)
            return false;
          }

          defineAllFieldsMetadataList();
        } else {
          defineSelectedFieldsMetadataList();
        }
      } else {
        if (meta.isUseHttpEndpoint()) {
          data.fieldnrs = new HashMap<>();
          if (meta.specifyFields()) {
            defineSelectedFieldsMetadataList();
          } else {
            defineAllFieldsMetadataList();
          }
        }
      }
    }

    data.outputRowMeta = getInputRowMeta().clone();

    if (!meta.isUseHttpEndpoint()) {
      if (meta.isStreamToS3Csv()) {
        writeRowToFile(data.outputRowMeta, r);
      }
    } else {
      appendRowAsJsonLine(data.outputRowMeta, r);
      try {
        writeIfBatchSizeRecordsAreReached();
      } catch (IOException | CrateDBHopException e) {
        throw new HopException(e);
      }
    }
    putRow(getInputRowMeta().clone(), r);

    return true;
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

  private void defineAllFieldsMetadataList() throws HopException {
    data.insertRowMeta = new RowMeta();
    for (int i = 0; i < meta.getFields().size(); i++) {
      int streamFieldLocation =
          data.insertRowMeta.indexOfValue(meta.getFields().get(i).getStreamField());
      if (streamFieldLocation < 0) {
        throw new HopTransformException(
            "Field ["
                + meta.getFields().get(i).getStreamField()
                + "] couldn't be found in the input stream!");
      }

      data.selectedRowFieldIndices[i] = streamFieldLocation;

      int dbFieldLocation = -1;
      for (int e = 0; e < data.dbFields.size(); e++) {
        String[] field = data.dbFields.get(e);
        if (field[0].equalsIgnoreCase(meta.getFields().get(i).getDatabaseField())) {
          dbFieldLocation = e;
          break;
        }
      }
      if (dbFieldLocation < 0) {
        throw new HopException(
            "Field ["
                + meta.getFields().get(i).getDatabaseField()
                + "] couldn't be found in the table!");
      }
      IValueMeta inputValueMeta = getInputRowMeta().getValueMeta(streamFieldLocation);

      IValueMeta insertValueMeta = inputValueMeta.clone();
      insertValueMeta.setName(data.dbFields.get(dbFieldLocation)[0]);

      data.insertRowMeta.addValueMeta(insertValueMeta);

      data.fieldnrs.put(
          meta.getFields().get(i).getDatabaseField().toUpperCase(), streamFieldLocation);
    }
  }

  private void defineSelectedFieldsMetadataList() throws HopTransformException {
    // use the columns/fields mapping.
    int numberOfInsertFields = meta.getFields().size();
    data.insertRowMeta = new RowMeta();

    // Cache the position of the selected fields in the row array
    data.selectedRowFieldIndices = new int[numberOfInsertFields];
    for (int i = 0; i < meta.getFields().size(); i++) {
      CrateDBBulkLoaderField vbf = meta.getFields().get(i);
      String inputFieldName = vbf.getStreamField();
      int inputFieldIdx = getInputRowMeta().indexOfValue(inputFieldName);
      if (inputFieldIdx < 0) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "CrateDBBulkLoader.Exception.FieldRequired", inputFieldName)); // $NON-NLS-1$
      }
      data.selectedRowFieldIndices[i] = inputFieldIdx;
      String insertFieldName = vbf.getDatabaseField();
      IValueMeta inputValueMeta = getInputRowMeta().getValueMeta(inputFieldIdx);
      if (inputValueMeta == null) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG,
                "CrateDBBulkLoader.Exception.FailedToFindField",
                vbf.getStreamField())); // $NON-NLS-1$
      }
      IValueMeta insertValueMeta = inputValueMeta.clone();
      insertValueMeta.setName(insertFieldName);
      data.insertRowMeta.addValueMeta(insertValueMeta);
      data.fieldnrs.put(meta.getFields().get(i).getDatabaseField().toUpperCase(), inputFieldIdx);
    }
  }

  private void writeIfBatchSizeRecordsAreReached()
      throws HopException, CrateDBHopException, IOException {
    int maxBatchSize = Integer.parseInt(meta.getBatchSize());
    if (data.httpBulkArgs.size() >= maxBatchSize) {
      String[] columns =
          meta.getFields().stream()
              .map(CrateDBBulkLoaderField::getDatabaseField)
              .toArray(String[]::new);
      String schema = meta.getSchemaName();
      String table = meta.getTableName();
      writeBatchToCrateDB(schema, table, columns);
    }
  }

  private void writeBatchToCrateDB(String schema, String table, String[] columns)
      throws HopException, CrateDBHopException, IOException {
    try {
      final HttpBulkImportResponse httpResponse =
          bulkImportClient.batchInsert(schema, table, columns, data.httpBulkArgs);

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

  private void appendRowAsJsonLine(IRowMeta rowMeta, Object[] row) throws HopTransformException {
    Object[] args = new Object[rowMeta.size()];
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

    if (rowItem != null) {
      switch (v.getType()) {
        case IValueMeta.TYPE_STRING:
          convertedValue = (String) rowItem;
          break;
        case IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER:
          convertedValue = String.valueOf(rowItem);
          break;
        case IValueMeta.TYPE_TIMESTAMP:
          vc = new ValueMetaString();
          vc.setName(v.getName());
          v.setConversionMask(TIMESTAMP_CONVERSION_MASK);
          vc.setConversionMask(TIMESTAMP_CONVERSION_MASK);
          convertedValue = (String) vc.convertData(v, rowItem);
          break;
        case IValueMeta.TYPE_DATE:
          vc = new ValueMetaString();
          vc.setName(v.getName());
          v.setConversionMask(DATE_CONVERSION_MASK);
          vc.setConversionMask(DATE_CONVERSION_MASK);
          convertedValue = (String) vc.convertData(v, rowItem);
          break;
        default:
          convertedValue = String.valueOf(rowItem);
          break;
      }
    }
    logDetailed("Field: " + v.getName() + " - Converted Value: " + convertedValue);

    if (vc != null && !data.convertedRowMetaReady) data.convertedRowMeta.setValueMeta(pos, vc);

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

  private String buildCopyStatementSqlString() {
    final DatabaseMeta databaseMeta = data.db.getDatabaseMeta();

    StringBuilder sb = new StringBuilder(150);
    sb.append("COPY ");

    sb.append(
        databaseMeta.getQuotedSchemaTableCombination(
            variables,
            data.db.resolve(meta.getSchemaName()),
            data.db.resolve(meta.getTableName())));

    sb.append(" (");
    List<CrateDBBulkLoaderField> fieldList = meta.getFields();
    for (int i = 0; i < fieldList.size(); i++) {
      CrateDBBulkLoaderField field = fieldList.get(i);
      if (i > 0) {
        sb.append(", ").append(field.getDatabaseField());
      } else {
        sb.append(field.getDatabaseField());
      }
    }
    sb.append(")");

    String awsAccessKeyId = "";
    String awsSecretAccessKey = "";
    if (meta.isUseSystemEnvVars()) {
      awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
      awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    } else {
      awsAccessKeyId = resolve(meta.getAwsAccessKeyId());
      awsSecretAccessKey = resolve(meta.getAwsSecretAccessKey());
    }

    String filename = resolve(meta.getReadFromFilename());

    String[] parts = filename.split("://", 2);
    String uriLeft = parts[0];
    String uriRight = parts[1];
    String awsSec = awsAccessKeyId + ":" + awsSecretAccessKey;

    if ("s3".equals(uriLeft)) {
      filename =
          ":".equals(awsSec)
              ? uriLeft + "://" + uriRight
              : uriLeft + "://" + awsSec + "@" + uriRight;
    } else {
      filename = uriLeft + "://" + uriRight;
    }

    sb.append(" FROM '" + filename + "'");
    sb.append(" WITH (format='csv', wait_for_completion=true");
    sb.append(", header=false");
    sb.append(", delimiter='" + CrateDBBulkLoaderMeta.DEFAULT_CSV_DELIMITER + "'");
    sb.append(")");
    sb.append(" RETURN SUMMARY");

    logDetailed("Copy stmt: " + sb.toString());

    return sb.toString();
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

    IRowMeta rowMeta = null;

    if (!StringUtils.isEmpty(resolve(meta.getSchemaName()))) {
      rowMeta = data.db.getTableFields(meta.getSchemaName() + "." + meta.getTableName());
    } else {
      rowMeta = data.db.getTableFields(meta.getTableName());
    }
    try {
      if (rowMeta.isEmpty()) {
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
   * Initialize the binary values of delimiters, enclosures, and escape characters
   *
   * @throws HopException
   */
  private void initBinaryDataFields() throws HopException {
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
  private void writeRowToFile(IRowMeta rowMeta, Object[] row) throws HopTransformException {

    try {
      for (int i = 0; i < data.insertRowMeta.size(); i++) {
        if (i > 0 && data.binarySeparator.length > 0) {
          data.writer.write(data.binarySeparator);
        }

        Object convertedValue = null;
        IValueMeta v = data.insertRowMeta.getValueMeta(i);
        convertedValue = convertDatatypeIfNeeded(v, row[data.selectedRowFieldIndices[i]], i);
        writeField(
            data.convertedRowMeta.getValueMeta(i),
            convertedValue,
            (!meta.isSpecifyFields() ? null : data.binaryNullValue));
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
  private void writeField(IValueMeta v, Object valueData, byte[] nullString)
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
        List<Integer> enclosures = null;
        boolean writeEnclosures = false;

        if (v.isString()) {
          writeEnclosures = true;

          if (containsSeparatorOrEnclosure(
              str, data.binarySeparator, data.binaryEnclosure, data.escapeCharacters)) {
            writeEnclosures = true;
          }
        }

        if (writeEnclosures) {
          data.writer.write(data.binaryEnclosure);
          enclosures = getEnclosurePositions(str);
        }

        if (enclosures == null) {
          data.writer.write(str);
        } else {
          // Skip the enclosures, escape them instead...
          int from = 0;
          for (Integer enclosure : enclosures) {
            // Minus one to write the escape before the enclosure
            int position = enclosure;
            data.writer.write(str, from, position - from);
            data.writer.write(data.escapeCharacters); // write enclosure a second time
            from = position;
          }
          if (from < str.length) {
            data.writer.write(str, from, str.length - from);
          }
        }

        if (writeEnclosures) {
          data.writer.write(data.binaryEnclosure);
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
   * Check if a string contains separators or enclosures. Can be used to determine if the string
   * needs enclosures around it or not.
   *
   * @param source The string to check
   * @param separator The separator character(s)
   * @param enclosure The enclosure character(s)
   * @param escape The escape character(s)
   * @return True if the string contains separators or enclosures
   */
  @SuppressWarnings("Duplicates")
  private boolean containsSeparatorOrEnclosure(
      byte[] source, byte[] separator, byte[] enclosure, byte[] escape) {
    boolean result = false;

    boolean enclosureExists = enclosure != null && enclosure.length > 0;
    boolean separatorExists = separator != null && separator.length > 0;
    boolean escapeExists = escape != null && escape.length > 0;

    // Skip entire test if neither separator nor enclosure exist
    if (separatorExists || enclosureExists || escapeExists) {

      // Search for the first occurrence of the separator or enclosure
      for (int index = 0; !result && index < source.length; index++) {
        if (enclosureExists && source[index] == enclosure[0]) {

          // Potential match found, make sure there are enough bytes to support a full match
          if (index + enclosure.length <= source.length) {
            // First byte of enclosure found
            result = true; // Assume match
            for (int i = 1; i < enclosure.length; i++) {
              if (source[index + i] != enclosure[i]) {
                // Enclosure match is proven false
                result = false;
                break;
              }
            }
          }

        } else if (separatorExists && source[index] == separator[0]) {

          // Potential match found, make sure there are enough bytes to support a full match
          if (index + separator.length <= source.length) {
            // First byte of separator found
            result = true; // Assume match
            for (int i = 1; i < separator.length; i++) {
              if (source[index + i] != separator[i]) {
                // Separator match is proven false
                result = false;
                break;
              }
            }
          }

        } else if (escapeExists
            && source[index] == escape[0]
            && index + escape.length <= source.length) {
          // Potential match found, make sure there are enough bytes to support a full match
          // First byte of separator found
          result = true; // Assume match
          for (int i = 1; i < escape.length; i++) {
            if (source[index + i] != escape[i]) {
              // Separator match is proven false
              result = false;
              break;
            }
          }
        }
      }
    }
    return result;
  }

  /**
   * Gets the positions of any double quotes or backslashes in the string
   *
   * @param str The string to check
   * @return The positions within the string of double quotes and backslashes.
   */
  private List<Integer> getEnclosurePositions(byte[] str) {
    List<Integer> positions = null;
    // +1 because otherwise we will not find it at the end
    for (int i = 0, len = str.length; i < len; i++) {
      // verify if on position i there is an enclosure
      //
      boolean found = true;
      for (int x = 0; found && x < data.binaryEnclosure.length; x++) {
        if (str[i + x] != data.binaryEnclosure[x]) {
          found = false;
        }
      }

      if (!found) {
        found = true;
        for (int x = 0; found && x < data.escapeCharacters.length; x++) {
          if (str[i + x] != data.escapeCharacters[x]) {
            found = false;
          }
        }
      }

      if (found) {
        if (positions == null) {
          positions = new ArrayList<>();
        }
        positions.add(i);
      }
    }
    return positions;
  }

  @Override
  public void stopRunning() throws HopException {
    setStopped(true);
    if (data.workerThread != null) {
      synchronized (data.workerThread) {
        if (data.workerThread.isAlive() && !data.workerThread.isInterrupted()) {
          try {
            data.workerThread.interrupt();
            data.workerThread.join();
          } catch (InterruptedException e) { // Checkstyle:OFF:
          }
          // Checkstyle:ONN:
        }
      }
    }

    super.stopRunning();
  }

  void truncateTable() throws HopDatabaseException {
    if (meta.isTruncateTable() && ((getCopy() == 0) || !Utils.isEmpty(getPartitionId()))) {
      data.db.truncateTable(resolve(meta.getSchemaName()), resolve(meta.getTableName()));
    }
  }

  @Override
  public void dispose() {

    setOutputDone();

    try {
      if (getErrors() > 0) {
        data.db.rollback();
      }
    } catch (HopDatabaseException e) {
      logError("Unexpected error rolling back the database connection.", e);
    }

    if (data.workerThread != null) {
      try {
        data.workerThread.join();
      } catch (InterruptedException e) { // Checkstyle:OFF:
      }
      // Checkstyle:ONN:
    }

    if (data.db != null) {
      data.db.disconnect();
    }
    super.dispose();
  }
}
