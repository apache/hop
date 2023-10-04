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

package org.apache.hop.pipeline.transforms.redshift.bulkloader;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RedshiftBulkLoader extends BaseTransform<RedshiftBulkLoaderMeta, RedshiftBulkLoaderData> {
  private static final Class<?> PKG =
      RedshiftBulkLoader.class; // for i18n purposes, needed by Translator2!!

  private static final SimpleDateFormat SIMPLE_DATE_FORMAT =
      new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  private FileOutputStream exceptionLog;
  private FileOutputStream rejectedLog;

  public RedshiftBulkLoader(
      TransformMeta transformMeta,
      RedshiftBulkLoaderMeta meta,
      RedshiftBulkLoaderData data,
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

        // get the file output stream to write to S3
        data.writer = HopVfs.getOutputStream(meta.getCopyFromFilename(), false);

        data.db = new Database(this, this, data.databaseMeta);
        data.db.connect();

        if (log.isBasic()) {
          logBasic("Connected to database [" + data.db.getDatabaseMeta() + "]");
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

  @Override
  public boolean processRow() throws HopException {

    Object[] r = getRow(); // this also waits for a previous transform to be finished.

    if (r == null) { // no more input to be expected...
      if (first && meta.isTruncateTable() && !meta.isOnlyWhenHaveRows()) {
        truncateTable();
      }

      try {
        data.close();
        String copyStmt = buildCopyStatementSqlString();
        data.db.execStatement(copyStmt);
        setOutputDone();
      } catch (IOException ioe) {
        throw new HopTransformException("Error releasing resources", ioe);
      }
      return false;
    }

    if (first) {

      first = false;

      if (meta.isTruncateTable()) {
        truncateTable();
      }

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

//      IRowMeta tableMeta = meta.getRequiredFields(variables);

      if (!meta.specifyFields()) {

        // Just take the whole input row
        data.insertRowMeta = getInputRowMeta().clone();
        data.selectedRowFieldIndices = new int[data.insertRowMeta.size()];

        data.fieldnrs = new HashMap<>();
        getDbFields();

        for (int i = 0; i < meta.getFields().size(); i++) {
          int streamFieldLocation =
                  data.outputRowMeta.indexOfValue(
                          meta.getFields().get(i).getStreamField());
          if (streamFieldLocation < 0) {
            throw new HopTransformException(
                    "Field ["
                            + meta.getFields().get(i).getStreamField()
                            + "] couldn't be found in the input stream!");
          }

          int dbFieldLocation = -1;
          for (int e = 0; e < data.dbFields.size(); e++) {
            String[] field = data.dbFields.get(e);
            if (field[0].equalsIgnoreCase(
                    meta.getFields().get(i).getDatabaseField())) {
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

          data.fieldnrs.put(
                  meta.getFields().get(i).getDatabaseField().toUpperCase(),
                  streamFieldLocation);
        }

      } else {

        int numberOfInsertFields = meta.getFields().size();
        data.insertRowMeta = new RowMeta();
//        data.colSpecs = new ArrayList<>(numberOfInsertFields);

        // Cache the position of the selected fields in the row array
        data.selectedRowFieldIndices = new int[numberOfInsertFields];
        for (int insertFieldIdx = 0; insertFieldIdx < numberOfInsertFields; insertFieldIdx++) {
          RedshiftBulkLoaderField vbf = meta.getFields().get(insertFieldIdx);
          String inputFieldName = vbf.getStreamField();
          int inputFieldIdx = getInputRowMeta().indexOfValue(inputFieldName);
          if (inputFieldIdx < 0) {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG,
                    "RedshiftBulkLoader.Exception.FieldRequired",
                    inputFieldName)); //$NON-NLS-1$
          }
          data.selectedRowFieldIndices[insertFieldIdx] = inputFieldIdx;

          String insertFieldName = vbf.getDatabaseField();
          IValueMeta inputValueMeta = getInputRowMeta().getValueMeta(inputFieldIdx);
          if (inputValueMeta == null) {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG,
                    "RedshiftBulkLoader.Exception.FailedToFindField",
                    vbf.getStreamField())); // $NON-NLS-1$
          }
          IValueMeta insertValueMeta = inputValueMeta.clone();
          insertValueMeta.setName(insertFieldName);
          data.insertRowMeta.addValueMeta(insertValueMeta);
        }
      }
    }

/*
    try {
      Object[] outputRowData = writeToOutputStream(r);
      if (outputRowData != null) {
        putRow(data.outputRowMeta, outputRowData); // in case we want it
        // go further...
        incrementLinesOutput();
      }

      if (checkFeedback(getLinesRead())) {
        if (log.isBasic()) {
          logBasic("linenr " + getLinesRead());
        } //$NON-NLS-1$
      }
    } catch (HopException e) {
      logError("Because of an error, this transform can't continue: ", e);
      setErrors(1);
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    } catch (IOException e) {
      e.printStackTrace();
    }
*/

    return true;
  }


/*
  */
/**
   * Runs the commands to put the data to the Snowflake stage, the copy command to load the table,
   * and finally a commit to commit the transaction.
   *
   * @throws HopDatabaseException
   * @throws HopFileException
   * @throws HopValueException
   *//*

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

    logDebug("Executing SQL " + sql);
    try (ResultSet putResultSet = data.db.openQuery(sql, null, null, ResultSet.FETCH_FORWARD, false)) {
      IRowMeta putRowMeta = data.db.getReturnRowMeta();
      Object[] putRow = data.db.getRow(putResultSet);
      logDebug("=========================Put File Results======================");
      int fileNum = 0;
      while (putRow != null) {
        logDebug("------------------------ File " + fileNum + "--------------------------");
        for (int i = 0; i < putRowMeta.getFieldNames().length; i++) {
          logDebug(putRowMeta.getFieldNames()[i] + " = " + putRowMeta.getString(putRow, i));
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
    } catch(SQLException exception) {
      throw new HopDatabaseException(exception);
    }
    String copySQL = meta.getCopyStatement(this, data.getPreviouslyOpenedFiles());
    logDebug("Executing SQL " + copySQL);
    try (ResultSet resultSet = data.db.openQuery(copySQL, null, null, ResultSet.FETCH_FORWARD, false)) {
      IRowMeta rowMeta = data.db.getReturnRowMeta();

      Object[] row = data.db.getRow(resultSet);
      int rowsLoaded = 0;
      int rowsLoadedField = rowMeta.indexOfValue("rows_loaded");
      int rowsError = 0;
      int errorField = rowMeta.indexOfValue("errors_seen");
      logBasic("====================== Bulk Load Results======================");
      int rowNum = 1;
      while (row != null) {
        logBasic("---------------------- Row " + rowNum + " ----------------------");
        for (int i = 0; i < rowMeta.getFieldNames().length; i++) {
          logBasic(rowMeta.getFieldNames()[i] + " = " + rowMeta.getString(row, i));
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
    } catch(SQLException exception) {
      throw new HopDatabaseException(exception);
    }
    data.db.execStatement("commit");
  }
*/

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
    final IRowMeta fields = data.insertRowMeta;
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
    }
    sb.append(")");

    sb.append(" FROM " + meta.getCopyFromFilename());

    logDebug("copy stmt: " + sb.toString());

    return sb.toString();
  }

  private Object[] writeToOutputStream(Object[] r) throws HopException, IOException {
    assert (r != null);

    Object[] insertRowData = r;
    Object[] outputRowData = r;

    if (meta.specifyFields()) {
      insertRowData = new Object[data.selectedRowFieldIndices.length];
      for (int idx = 0; idx < data.selectedRowFieldIndices.length; idx++) {
        insertRowData[idx] = r[data.selectedRowFieldIndices[idx]];
      }
    }

    return outputRowData;
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
    if (!StringUtils.isEmpty(resolve(meta.getSchemaName()))) {
      sql += resolve(meta.getSchemaName()) + ".";
    }
    sql += resolve(meta.getTableName());
    logDetailed("Executing SQL " + sql);
    try {
      try (ResultSet resultSet = data.db.openQuery(sql, null, null, ResultSet.FETCH_FORWARD, false)) {

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


  protected void verifyDatabaseConnection() throws HopException {
    // Confirming Database Connection is defined.
    if (meta.getConnection() == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "RedshiftBulkLoaderMeta.Error.NoConnection"));
    }
  }



/*
  @Override
  public void markStop() {
    // Close the exception/rejected loggers at the end
    try {
      closeLogFiles();
    } catch (HopException ex) {
      logError(BaseMessages.getString(PKG, "RedshiftBulkLoader.Exception.ClosingLogError", ex));
    }
    super.markStop();
  }
*/

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
              resolve(RedshiftBulkLoaderMeta.CSV_DELIMITER).getBytes(StandardCharsets.UTF_8);
      data.binaryEnclosure =
              resolve(RedshiftBulkLoaderMeta.ENCLOSURE).getBytes(StandardCharsets.UTF_8);
      data.binaryNewline =
              RedshiftBulkLoaderMeta.CSV_RECORD_DELIMITER.getBytes(StandardCharsets.UTF_8);
      data.escapeCharacters =
              RedshiftBulkLoaderMeta.CSV_ESCAPE_CHAR.getBytes(StandardCharsets.UTF_8);

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

      if(meta.isStreamToS3Csv() && !meta.isSpecifyFields()) {
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
      } else if (meta.isStreamToS3Csv()) {
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

//      data.outputCount++;
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
        String svalue = (valueData instanceof String) ? (String) valueData : v.getString(valueData);

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

        } else if (escapeExists && source[index] == escape[0]) {

          // Potential match found, make sure there are enough bytes to support a full match
          if (index + escape.length <= source.length) {
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

    // allow data to be garbage collected immediately:
//    data.colSpecs = null;
//    data.encoder = null;

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

/*
  @VisibleForTesting
  StreamEncoder createStreamEncoder(List<ColumnSpec> colSpecs, PipedInputStream pipedInputStream)
      throws IOException {
    return new StreamEncoder(colSpecs, pipedInputStream);
  }

  @VisibleForTesting
  VerticaCopyStream createVerticaCopyStream(String dml)
      throws SQLException, ClassNotFoundException, HopDatabaseException {
    return new VerticaCopyStream(getVerticaConnection(), dml);
  }

  @VisibleForTesting
  VerticaConnection getVerticaConnection()
      throws SQLException, ClassNotFoundException, HopDatabaseException {

    Connection conn = data.db.getConnection();
    if (conn != null) {
      if (conn instanceof VerticaConnection) {
        return (VerticaConnection) conn;
      } else {
        Connection underlyingConn = null;
        if (conn instanceof DelegatingConnection) {
          DelegatingConnection pooledConn = (DelegatingConnection) conn;
          underlyingConn = pooledConn.getInnermostDelegate();
        } else if (conn instanceof PooledConnection) {
          PooledConnection pooledConn = (PooledConnection) conn;
          underlyingConn = pooledConn.getConnection();
        } else {
          // Last resort - attempt to use unwrap to get at the connection.
          try {
            if (conn.isWrapperFor(VerticaConnection.class)) {
              VerticaConnection vc = conn.unwrap(VerticaConnection.class);
              return vc;
            }
          } catch (SQLException ignored) {
            // ignored - the connection doesn't support unwrap or the connection cannot be
            // unwrapped into a VerticaConnection.
          }
        }
        if ((underlyingConn != null) && (underlyingConn instanceof VerticaConnection)) {
          return (VerticaConnection) underlyingConn;
        }
      }
      throw new IllegalStateException(
          "Could not retrieve a RedshiftConnection from " + conn.getClass().getName());
    } else {
      throw new IllegalStateException("Could not retrieve a RedshiftConnection from null");
    }
  }
*/
}
