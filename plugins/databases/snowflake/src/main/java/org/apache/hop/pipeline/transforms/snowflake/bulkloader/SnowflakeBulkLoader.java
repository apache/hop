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

package org.apache.hop.pipeline.transforms.snowflake.bulkloader;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.compress.CompressionProviderFactory;
import org.apache.hop.core.compress.ICompressionProvider;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Bulk loads data to Snowflake */
public class SnowflakeBulkLoader
    extends BaseTransform<SnowflakeBulkLoaderMeta, SnowflakeBulkLoaderData> {
  private static final Class<?> PKG =
      SnowflakeBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!
  public static final String CONST_FIELD = "Field [";
  public static final String CONST_EXECUTING_SQL = "Executing SQL ";

  public SnowflakeBulkLoader(
      TransformMeta transformMeta,
      SnowflakeBulkLoaderMeta meta,
      SnowflakeBulkLoaderData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  /**
   * Receive an input row from the stream, and write it to a local temp file. After receiving the
   * last row, run the put and copy commands to copy the data into Snowflake.
   *
   * @return Was the row successfully processed.
   * @throws HopException
   */
  @Override
  public synchronized boolean processRow() throws HopException {

    Object[] row = getRow(); // This also waits for a row to be finished.

    if (row != null && first) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // Open a new file here
      //
      openNewFile(buildFilename());
      data.oneFileOpened = true;
      initBinaryDataFields();

      if (meta.isSpecifyFields()
          && meta.getDataType()
              .equals(
                  SnowflakeBulkLoaderMeta.DATA_TYPE_CODES[SnowflakeBulkLoaderMeta.DATA_TYPE_CSV])) {
        // Get input field mapping
        data.fieldnrs = new HashMap<>();
        getDbFields();
        for (int i = 0; i < meta.getSnowflakeBulkLoaderFields().size(); i++) {
          int streamFieldLocation =
              data.outputRowMeta.indexOfValue(
                  meta.getSnowflakeBulkLoaderFields().get(i).getStreamField());
          if (streamFieldLocation < 0) {
            throw new HopTransformException(
                CONST_FIELD
                    + meta.getSnowflakeBulkLoaderFields().get(i).getStreamField()
                    + "] couldn't be found in the input stream!");
          }

          int dbFieldLocation = -1;
          for (int e = 0; e < data.dbFields.size(); e++) {
            String[] field = data.dbFields.get(e);
            if (field[0].equalsIgnoreCase(
                meta.getSnowflakeBulkLoaderFields().get(i).getTableField())) {
              dbFieldLocation = e;
              break;
            }
          }
          if (dbFieldLocation < 0) {
            throw new HopException(
                CONST_FIELD
                    + meta.getSnowflakeBulkLoaderFields().get(i).getTableField()
                    + "] couldn't be found in the table!");
          }

          data.fieldnrs.put(
              meta.getSnowflakeBulkLoaderFields().get(i).getTableField().toUpperCase(),
              streamFieldLocation);
        }
      } else if (meta.getDataType()
          .equals(
              SnowflakeBulkLoaderMeta.DATA_TYPE_CODES[SnowflakeBulkLoaderMeta.DATA_TYPE_JSON])) {
        data.fieldnrs = new HashMap<>();
        int streamFieldLocation = data.outputRowMeta.indexOfValue(meta.getJsonField());
        if (streamFieldLocation < 0) {
          throw new HopTransformException(
              CONST_FIELD + meta.getJsonField() + "] couldn't be found in the input stream!");
        }
        data.fieldnrs.put("json", streamFieldLocation);
      }
    }

    // Create a new split?
    if ((row != null
        && data.outputCount > 0
        && Const.toInt(resolve(meta.getSplitSize()), 0) > 0
        && (data.outputCount % Const.toInt(resolve(meta.getSplitSize()), 0)) == 0)) {

      // Done with this part or with everything.
      closeFile();

      // Not finished: open another file...
      openNewFile(buildFilename());
    }

    if (row == null) {
      // no more input to be expected...
      closeFile();
      loadDatabase();
      setOutputDone();
      return false;
    }

    writeRowToFile(data.outputRowMeta, row);
    putRow(data.outputRowMeta, row); // in case we want it to go further...

    if (checkFeedback(data.outputCount)) {
      if (isBasic()) {
        logBasic("linenr " + data.outputCount);
      }
    }

    return true;
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
    String sql = "desc table ";
    if (!StringUtils.isEmpty(resolve(meta.getTargetSchema()))) {
      sql += resolve(meta.getTargetSchema()) + ".";
    }
    sql += resolve(meta.getTargetTable());
    if (isDetailed()) {
      logDetailed(CONST_EXECUTING_SQL + sql);
    }
    try {
      try (ResultSet resultSet =
          data.db.openQuery(sql, null, null, ResultSet.FETCH_FORWARD, false)) {

        IRowMeta rowMeta = data.db.getReturnRowMeta();
        int nameField = rowMeta.indexOfValue("NAME");
        int typeField = rowMeta.indexOfValue("TYPE");
        if (nameField < 0 || typeField < 0) {
          throw new HopException("Unable to get database fields");
        }

        Object[] row = data.db.getRow(resultSet);
        if (row == null) {
          throw new HopException("No fields found in table");
        }
        while (row != null) {
          String[] field = new String[2];
          field[0] = rowMeta.getString(row, nameField).toUpperCase();
          field[1] = rowMeta.getString(row, typeField);
          data.dbFields.add(field);
          row = data.db.getRow(resultSet);
        }
        data.db.closeQuery(resultSet);
      }
    } catch (Exception ex) {
      throw new HopException("Error getting database fields", ex);
    }
  }

  /**
   * Runs the commands to put the data to the Snowflake stage, the copy command to load the table,
   * and finally a commit to commit the transaction.
   *
   * @throws HopDatabaseException
   * @throws HopFileException
   * @throws HopValueException
   */
  private void loadDatabase() throws HopDatabaseException, HopFileException, HopValueException {
    boolean endsWithSlash =
        resolve(meta.getWorkDirectory()).endsWith("\\")
            || resolve(meta.getWorkDirectory()).endsWith("/");
    String sql =
        "PUT 'file://"
            + resolve(meta.getWorkDirectory()).replaceAll("\\\\", "/")
            + (endsWithSlash ? "" : "/")
            + resolve(meta.getTargetTable())
            + "_"
            + meta.getFileDate()
            + "_*' "
            + meta.getStage(this)
            + ";";

    if (isDebug()) {
      logDebug(CONST_EXECUTING_SQL + sql);
    }
    try (ResultSet putResultSet =
        data.db.openQuery(sql, null, null, ResultSet.FETCH_FORWARD, false)) {
      IRowMeta putRowMeta = data.db.getReturnRowMeta();
      Object[] putRow = data.db.getRow(putResultSet);
      if (isDebug()) {
        logDebug("=========================Put File Results======================");
      }
      int fileNum = 0;
      while (putRow != null) {
        if (isDebug()) {
          logDebug("------------------------ File " + fileNum + "--------------------------");
        }
        for (int i = 0; i < putRowMeta.getFieldNames().length; i++) {
          if (isDebug()) {
            logDebug(putRowMeta.getFieldNames()[i] + " = " + putRowMeta.getString(putRow, i));
          }
          if (putRowMeta.getFieldNames()[i].equalsIgnoreCase("status")
              && putRowMeta.getString(putRow, i).equalsIgnoreCase("ERROR")) {
            throw new HopDatabaseException(
                "Error putting file to Snowflake stage \n"
                    + putRowMeta.getString(putRow, "message", ""));
          }
        }
        fileNum++;

        putRow = data.db.getRow(putResultSet);
      }
      data.db.closeQuery(putResultSet);
    } catch (SQLException exception) {
      throw new HopDatabaseException(exception);
    }
    String copySQL = meta.getCopyStatement(this, data.getPreviouslyOpenedFiles());
    if (isDebug()) {
      logDebug(CONST_EXECUTING_SQL + copySQL);
    }
    try (ResultSet resultSet =
        data.db.openQuery(copySQL, null, null, ResultSet.FETCH_FORWARD, false)) {
      IRowMeta rowMeta = data.db.getReturnRowMeta();

      Object[] row = data.db.getRow(resultSet);
      int rowsLoaded = 0;
      int rowsLoadedField = rowMeta.indexOfValue("rows_loaded");
      int rowsError = 0;
      int errorField = rowMeta.indexOfValue("errors_seen");
      if (isDetailed()) {
        logDetailed("====================== Bulk Load Results======================");
      }
      int rowNum = 1;
      while (row != null) {
        if (isDetailed()) {
          logDetailed("---------------------- Row " + rowNum + " ----------------------");
          for (int i = 0; i < rowMeta.getFieldNames().length; i++) {
            logDetailed(rowMeta.getFieldNames()[i] + " = " + rowMeta.getString(row, i));
          }
        }

        if (rowsLoadedField >= 0) {
          rowsLoaded += rowMeta.getInteger(row, rowsLoadedField);
        }

        if (errorField >= 0) {
          rowsError += rowMeta.getInteger(row, errorField);
        }

        rowNum++;
        row = data.db.getRow(resultSet);
      }
      data.db.closeQuery(resultSet);
      setLinesOutput(rowsLoaded);
      setLinesRejected(rowsError);
    } catch (SQLException exception) {
      throw new HopDatabaseException(exception);
    }
    data.db.execStatement("commit");
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
      if (meta.getDataTypeId() == SnowflakeBulkLoaderMeta.DATA_TYPE_CSV
          && !meta.isSpecifyFields()) {
        /*
         * Write all values in stream to text file.
         */
        for (int i = 0; i < rowMeta.size(); i++) {
          if (i > 0 && data.binarySeparator.length > 0) {
            data.writer.write(data.binarySeparator);
          }
          IValueMeta v = rowMeta.getValueMeta(i);
          Object valueData = row[i];

          // no special null value default was specified since no fields are specified at all
          // As such, we pass null
          //
          writeField(v, valueData, null);
        }
        data.writer.write(data.binaryNewline);
      } else if (meta.getDataTypeId() == SnowflakeBulkLoaderMeta.DATA_TYPE_CSV) {
        /*
         * Only write the fields specified!
         */
        for (int i = 0; i < data.dbFields.size(); i++) {
          if (data.dbFields.get(i) != null) {
            if (i > 0 && data.binarySeparator.length > 0) {
              data.writer.write(data.binarySeparator);
            }

            String[] field = data.dbFields.get(i);
            IValueMeta v;

            if (field[1].toUpperCase().startsWith("TIMESTAMP")) {
              v = new ValueMetaDate();
              v.setConversionMask("yyyy-MM-dd HH:mm:ss.SSS");
            } else if (field[1].toUpperCase().startsWith("DATE")) {
              v = new ValueMetaDate();
              v.setConversionMask("yyyy-MM-dd");
            } else if (field[1].toUpperCase().startsWith("TIME")) {
              v = new ValueMetaDate();
              v.setConversionMask("HH:mm:ss.SSS");
            } else if (field[1].toUpperCase().startsWith("NUMBER")
                || field[1].toUpperCase().startsWith("FLOAT")) {
              v = new ValueMetaBigNumber();
            } else {
              v = new ValueMetaString();
              v.setLength(-1);
            }

            int fieldIndex = -1;
            if (data.fieldnrs.get(data.dbFields.get(i)[0]) != null) {
              fieldIndex = data.fieldnrs.get(data.dbFields.get(i)[0]);
            }
            Object valueData = null;
            if (fieldIndex >= 0) {
              valueData = v.convertData(rowMeta.getValueMeta(fieldIndex), row[fieldIndex]);
            } else if (meta.isErrorColumnMismatch()) {
              throw new HopException(
                  "Error column mismatch: Database field "
                      + data.dbFields.get(i)[0]
                      + " not found on stream.");
            }
            writeField(v, valueData, data.binaryNullValue);
          }
        }
        data.writer.write(data.binaryNewline);
      } else {
        int jsonField = data.fieldnrs.get("json");
        data.writer.write(
            data.outputRowMeta.getString(row, jsonField).getBytes(StandardCharsets.UTF_8));
        data.writer.write(data.binaryNewline);
      }

      data.outputCount++;
    } catch (Exception e) {
      throw new HopTransformException("Error writing line", e);
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
        String svalue =
            (valueData instanceof String stringValueData)
                ? stringValueData
                : v.getString(valueData);

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

        if (v.isString()
            && containsSeparatorOrEnclosure(
                str, data.binarySeparator, data.binaryEnclosure, data.escapeCharacters)) {
          writeEnclosures = true;
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

  /**
   * Get the filename to wrtie
   *
   * @return The filename to use
   */
  private String buildFilename() {
    return meta.buildFilename(this, getCopy(), getPartitionId(), data.splitnr);
  }

  /**
   * Opens a file for writing
   *
   * @param baseFilename The filename to write to
   * @throws HopException
   */
  private void openNewFile(String baseFilename) throws HopException {
    if (baseFilename == null) {
      throw new HopFileException(
          BaseMessages.getString(PKG, "SnowflakeBulkLoader.Exception.FileNameNotSet"));
    }

    data.writer = null;

    String filename = resolve(baseFilename);

    try {
      ICompressionProvider compressionProvider =
          CompressionProviderFactory.getInstance().getCompressionProviderByName("GZip");

      if (compressionProvider == null) {
        throw new HopException("No compression provider found with name = GZip");
      }

      if (!compressionProvider.supportsOutput()) {
        throw new HopException("Compression provider GZip does not support output streams!");
      }

      if (isDetailed()) {
        logDetailed("Opening output stream using provider: " + compressionProvider.getName());
      }

      if (checkPreviouslyOpened(filename)) {
        data.fos = getOutputStream(filename, variables, true);
      } else {
        data.fos = getOutputStream(filename, variables, false);
        data.previouslyOpenedFiles.add(filename);
      }

      data.out = compressionProvider.createOutputStream(data.fos);

      // The compression output stream may also archive entries. For this we create the filename
      // (with appropriate extension) and add it as an entry to the output stream. For providers
      // that do not archive entries, they should use the default no-op implementation.
      data.out.addEntry(filename, "gz");

      data.writer = new BufferedOutputStream(data.out, 5000);

      if (isDetailed()) {
        logDetailed(
            "Opened new file with name [" + HopVfs.getFriendlyURI(filename, variables) + "]");
      }

    } catch (Exception e) {
      throw new HopException("Error opening new file : " + e.toString());
    }

    data.splitnr++;
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
      }
      data.writer = null;
      if (isDebug()) {
        logDebug("Closing normal file ...");
      }
      if (data.out != null) {
        data.out.close();
      }
      if (data.fos != null) {
        data.fos.close();
        data.fos = null;
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
   * Checks if a filename was previously opened by the transform
   *
   * @param filename The filename to check
   * @return True if the transform had previously opened the file
   */
  private boolean checkPreviouslyOpened(String filename) {

    return data.getPreviouslyOpenedFiles().contains(filename);
  }

  /**
   * Initialize the transform by connecting to the database and calculating some constants that will
   * be used.
   *
   * @return True if successfully initialized
   */
  @Override
  public boolean init() {

    if (super.init()) {
      data.splitnr = 0;

      try {
        data.databaseMeta = this.getPipelineMeta().findDatabase(meta.getConnection(), variables);

        data.db = new Database(this, variables, data.databaseMeta);
        data.db.connect();

        if (isBasic()) {
          logBasic("Connected to database [" + meta.getConnection() + "]");
        }

        data.db.setCommit(Integer.MAX_VALUE);

        initBinaryDataFields();
      } catch (Exception e) {
        logError("Couldn't initialize binary data fields", e);
        setErrors(1L);
        stopAll();
      }

      return true;
    }

    return false;
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
          resolve(SnowflakeBulkLoaderMeta.CSV_DELIMITER).getBytes(StandardCharsets.UTF_8);
      data.binaryEnclosure =
          resolve(SnowflakeBulkLoaderMeta.ENCLOSURE).getBytes(StandardCharsets.UTF_8);
      data.binaryNewline =
          SnowflakeBulkLoaderMeta.CSV_RECORD_DELIMITER.getBytes(StandardCharsets.UTF_8);
      data.escapeCharacters =
          SnowflakeBulkLoaderMeta.CSV_ESCAPE_CHAR.getBytes(StandardCharsets.UTF_8);

      data.binaryNullValue = "".getBytes(StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new HopException("Unexpected error while encoding binary fields", e);
    }
  }

  /**
   * Clean up after the transform. Close any open files, remove temp files, close any database
   * connections.
   */
  @Override
  public void dispose() {
    if (data.oneFileOpened) {
      closeFile();
    }

    try {
      if (data.fos != null) {
        data.fos.close();
      }
    } catch (Exception e) {
      logError("Unexpected error closing file", e);
      setErrors(1);
    }

    try {
      if (data.db != null) {
        data.db.disconnect();
      }
    } catch (Exception e) {
      logError("Unable to close connection to database", e);
      setErrors(1);
    }

    if (meta.isRemoveFiles()
        || !Boolean.parseBoolean(resolve(SnowflakeBulkLoaderMeta.DEBUG_MODE_VAR))) {
      for (String filename : data.previouslyOpenedFiles) {
        try {
          HopVfs.getFileObject(filename, variables).delete();
          if (isDetailed()) {
            logDetailed("Deleted temp file " + filename);
          }
        } catch (Exception ex) {
          logMinimal("Unable to delete temp file", ex);
        }
      }
    }

    super.dispose();
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
   * Gets a file handle
   *
   * @param vfsFilename The file name
   * @return The file handle
   * @throws HopFileException
   */
  protected FileObject getFileObject(String vfsFilename) throws HopFileException {
    return HopVfs.getFileObject(vfsFilename, variables);
  }

  /**
   * Gets a file handle
   *
   * @param vfsFilename The file name
   * @param variables The variable space
   * @return The file handle
   * @throws HopFileException
   */
  protected FileObject getFileObject(String vfsFilename, IVariables variables)
      throws HopFileException {
    return HopVfs.getFileObject(vfsFilename, variables);
  }

  /**
   * Gets the output stream to write to
   *
   * @param vfsFilename The file name
   * @param variables The variable space
   * @param append Should the file be appended
   * @return The output stream to write to
   * @throws HopFileException
   */
  private OutputStream getOutputStream(String vfsFilename, IVariables variables, boolean append)
      throws HopFileException {
    return HopVfs.getOutputStream(vfsFilename, append, variables);
  }
}
