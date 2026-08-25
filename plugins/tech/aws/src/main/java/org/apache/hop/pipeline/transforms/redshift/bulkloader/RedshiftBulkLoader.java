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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.databases.redshift.RedshiftDatabaseMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

public class RedshiftBulkLoader
    extends BaseTransform<RedshiftBulkLoaderMeta, RedshiftBulkLoaderData> {
  private static final Class<?> PKG =
      RedshiftBulkLoader.class; // for i18n purposes, needed by Translator2!!

  /** Size of the buffer we put in front of the (remote) output stream. */
  private static final int OUTPUT_BUFFER_SIZE = 128 * 1024;

  /** Placeholder written instead of the AWS credentials when the COPY statement is logged. */
  private static final String CREDENTIALS_MASK = "<credentials hidden>";

  /**
   * ISO 8601, with milliseconds. Redshift coerces this into either a DATE or a TIMESTAMP column --
   * a DATE truncates the time, a TIMESTAMP keeps it -- so both Hop types can be written with full
   * precision and let the target column decide what to keep.
   */
  private static final String TIMESTAMP_CONVERSION_MASK = "yyyy-MM-dd HH:mm:ss.SSS";

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
        // Validating that the connection and the S3 file have been defined.
        verifyDatabaseConnection();
        verifyFileSettings();
        data.databaseMeta = this.getPipelineMeta().findDatabase(meta.getConnection(), variables);

        if (meta.isStreamToS3Csv()) {
          String target = resolve(meta.getCopyFromFilename());
          ensureParentFolderExists(target);
          // Get the file output stream to write to S3. Every field is written as a separate
          // chunk of bytes, so a buffer in front of the (remote) stream matters a lot here.
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
                  PKG, "RedshiftBulkLoader.Connection.Connected", data.db.getDatabaseMeta()));
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
      endOfStream();
      return false;
    }

    if (first) {
      first = false;
      data.rowsReceived = true;

      if (meta.isTruncateTable()) {
        truncateTable();
      }

      if (meta.isStreamToS3Csv()) {
        prepareRowMapping();
      }
    }

    if (meta.isStreamToS3Csv()) {
      writeRowToFile(data.outputRowMeta, r);
      putRow(data.outputRowMeta, r);
    } else {
      // We are loading a file that already exists on S3, the stream itself is only a trigger.
      putRow(getInputRowMeta(), r);
    }

    return true;
  }

  /**
   * Close the file we streamed to S3 (if any) and fire the COPY statement.
   *
   * @throws HopException in case the statement failed or resources could not be released
   */
  private void endOfStream() throws HopException {
    if (!data.rowsReceived && meta.isTruncateTable() && !meta.isOnlyWhenHaveRows()) {
      truncateTable();
    }

    if (!shouldExecuteCopy()) {
      return;
    }

    // The file has to be complete on S3 before Redshift reads it.
    if (!closeFile()) {
      setErrors(1);
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      throw new HopTransformException("Error releasing resources");
    }

    try {
      String copyStmt = buildCopyStatementSqlString(false);
      if (isDebug()) {
        logDebug("copy stmt: " + buildCopyStatementSqlString(true));
      }
      try (Statement stmt = data.db.getConnection().createStatement()) {
        stmt.executeUpdate(copyStmt);
      }
      data.db.commit();
    } catch (SQLException sqle) {
      setErrors(1);
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      throw new HopDatabaseException("Error executing COPY statements", sqle);
    }
  }

  /**
   * When we stream the rows to S3 ourselves there is nothing to load if the stream was empty. When
   * we load a file that is already on S3 the load only depends on the "only when we have rows"
   * option.
   *
   * @return true if the COPY statement has to be executed
   */
  boolean shouldExecuteCopy() {
    if (meta.isStreamToS3Csv()) {
      return data.rowsReceived;
    }
    return data.rowsReceived || !meta.isOnlyWhenHaveRows();
  }

  /**
   * Resolve, once, which field of the input row ends up in which column of the CSV file, together
   * with the value meta used to render it. Doing this per row is what used to make this transform
   * slow: it allocated and scanned the list of field names for every single field of every row.
   *
   * @throws HopException in case a configured field is not present on the input stream
   */
  void prepareRowMapping() throws HopException {
    IRowMeta inputRowMeta = getInputRowMeta();
    data.outputRowMeta = inputRowMeta.clone();

    // Both modes build the same mapping: without an explicit field list every field of the row is
    // written, in the order it arrives. Keeping one shape means date handling and everything else
    // below applies either way -- when the two paths were separate, only the explicit one ever
    // converted its dates.
    int count = meta.isSpecifyFields() ? meta.getFields().size() : inputRowMeta.size();
    data.insertRowMeta = new RowMeta();
    data.streamFieldIndexes = new int[count];
    data.writeValueMeta = new IValueMeta[count];
    data.sourceValueMeta = new IValueMeta[count];

    for (int i = 0; i < count; i++) {
      String streamField =
          meta.isSpecifyFields()
              ? meta.getFields().get(i).getStreamField()
              : inputRowMeta.getValueMeta(i).getName();
      String databaseField =
          meta.isSpecifyFields() ? meta.getFields().get(i).getDatabaseField() : streamField;

      int index = meta.isSpecifyFields() ? inputRowMeta.indexOfValue(streamField) : i;
      if (index < 0 && meta.isErrorColumnMismatch()) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "RedshiftBulkLoader.Exception.FailedToFindField", streamField));
      }
      data.streamFieldIndexes[i] = index;

      IValueMeta inputValueMeta =
          index >= 0 ? inputRowMeta.getValueMeta(index) : new ValueMetaString(streamField);

      // Dates and timestamps are written in the format the COPY statement declares. A Hop Date
      // carries a time of day just as a Timestamp does, so both are written whole and the target
      // column decides whether to keep the time.
      IValueMeta writeValueMeta = inputValueMeta;
      IValueMeta sourceValueMeta = null;
      if (inputValueMeta.getType() == IValueMeta.TYPE_TIMESTAMP
          || inputValueMeta.getType() == IValueMeta.TYPE_DATE) {
        writeValueMeta = new ValueMetaDate();
        writeValueMeta.setConversionMask(TIMESTAMP_CONVERSION_MASK);
        sourceValueMeta = inputValueMeta;
      }
      data.writeValueMeta[i] = writeValueMeta;
      data.sourceValueMeta[i] = sourceValueMeta;

      IValueMeta insertValueMeta = inputValueMeta.clone();
      insertValueMeta.setName(databaseField);
      data.insertRowMeta.addValueMeta(insertValueMeta);
    }

    // What the COPY statement has to name, since the file holds these columns and no others.
    data.columnNames = data.insertRowMeta.getFieldNames();
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
   * Build the Redshift COPY statement for the file on S3.
   *
   * @param maskCredentials when true the AWS credentials are replaced by a placeholder, so the
   *     statement can safely be written to the log
   * @return the COPY statement
   */
  String buildCopyStatementSqlString(boolean maskCredentials) throws HopException {
    final DatabaseMeta databaseMeta = data.db.getDatabaseMeta();
    boolean csv = isCsvFormat();

    StringBuilder sb = new StringBuilder(150);
    sb.append("COPY ");

    sb.append(
        databaseMeta.getQuotedSchemaTableCombination(
            variables,
            data.db.resolve(meta.getSchemaName()),
            data.db.resolve(meta.getTableName())));

    // Name the columns the file holds. Without this Redshift expects a value for every column of
    // the table, in table order, and a narrower file fails with "Delimiter not found".
    String[] columns = copyColumnNames();
    if (csv && columns.length > 0) {
      sb.append(" (");
      for (int i = 0; i < columns.length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(columns[i]);
      }
      sb.append(")");
    }

    sb.append(" FROM '").append(resolve(meta.getCopyFromFilename())).append("'");
    if (csv) {
      sb.append(" DELIMITER ',' ");
      sb.append(" CSV QUOTE AS '\"'");
      sb.append(" NULL '' ");
      sb.append(" EMPTYASNULL ");
      // 'auto' is the only setting that accepts fractional seconds; the explicit TIMEFORMAT
      // patterns have no token for them, so a value carrying milliseconds is rejected.
      sb.append("DATEFORMAT AS 'auto' ");
      sb.append("TIMEFORMAT AS 'auto'");
    }
    if (meta.isUseAwsIamRole()) {
      sb.append(" iam_role '")
          .append(maskCredentials ? CREDENTIALS_MASK : resolve(meta.getAwsIamRole()))
          .append("'");
    } else if (meta.isUseConnectionCredentials() || meta.isUseCredentials()) {
      if (maskCredentials) {
        sb.append(" CREDENTIALS '").append(CREDENTIALS_MASK).append("'");
      } else {
        sb.append(" CREDENTIALS '").append(buildCredentialsClause()).append("'");
      }
    }
    if (RedshiftBulkLoaderMeta.FILE_FORMAT_PARQUET.equals(meta.getLoadFromExistingFileFormat())) {
      sb.append(" FORMAT AS PARQUET;");
    }

    return sb.toString();
  }

  /**
   * The body of the COPY statement's CREDENTIALS clause, from wherever this transform is configured
   * to get its AWS credentials.
   *
   * @return the credential key/value pairs, without the surrounding quotes
   * @throws HopException when the credentials cannot be worked out
   */
  private String buildCredentialsClause() throws HopException {
    String accessKeyId;
    String secretAccessKey;
    String sessionToken = null;

    if (meta.isUseConnectionCredentials()) {
      AwsCredentials credentials = resolveConnectionCredentials();
      accessKeyId = credentials.accessKeyId();
      secretAccessKey = credentials.secretAccessKey();
      if (credentials instanceof AwsSessionCredentials session) {
        sessionToken = session.sessionToken();
      }
    } else if (meta.isUseSystemEnvVars()) {
      accessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
      secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
      sessionToken = System.getenv("AWS_SESSION_TOKEN");
    } else {
      accessKeyId = resolve(meta.getAwsAccessKeyId());
      secretAccessKey = resolve(meta.getAwsSecretAccessKey());
    }

    StringBuilder clause = new StringBuilder();
    clause
        .append("aws_access_key_id=")
        .append(Const.NVL(accessKeyId, ""))
        .append(";aws_secret_access_key=")
        .append(Const.NVL(secretAccessKey, ""));
    if (StringUtils.isNotEmpty(sessionToken)) {
      clause.append(";token=").append(sessionToken);
    }
    return clause.toString();
  }

  /**
   * Take the credentials the Redshift connection is configured with. A connection holding an access
   * key hands it straight over; one pointing at a profile or leaving it to the AWS default chain is
   * resolved here, so the COPY statement gets a concrete key -- including a session token when the
   * credentials are temporary.
   *
   * @return the credentials to put in the COPY statement
   * @throws HopException when the connection has no AWS credentials to give
   */
  private AwsCredentials resolveConnectionCredentials() throws HopException {
    IDatabase database = data.databaseMeta == null ? null : data.databaseMeta.getIDatabase();
    if (!(database instanceof RedshiftDatabaseMeta redshift)) {
      throw new HopException(
          BaseMessages.getString(PKG, "RedshiftBulkLoaderMeta.Error.NotARedshiftConnection"));
    }

    try {
      switch (redshift.getAuthenticationType()) {
        case IAM_CREDENTIALS:
          String key = Const.NVL(resolve(redshift.getAwsAccessKeyId()), "");
          String secret = Const.NVL(decrypt(redshift.getAwsSecretAccessKey()), "");
          String token = decrypt(redshift.getAwsSessionToken());
          return StringUtils.isEmpty(token)
              ? AwsBasicCredentials.create(key, secret)
              : AwsSessionCredentials.create(key, secret, token);
        case IAM_PROFILE:
          return ProfileCredentialsProvider.builder()
              .profileName(resolve(redshift.getAwsProfile()))
              .build()
              .resolveCredentials();
        case IAM_DEFAULT_CHAIN:
          return DefaultCredentialsProvider.create().resolveCredentials();
        default:
          throw new HopException(
              BaseMessages.getString(
                  PKG, "RedshiftBulkLoaderMeta.Error.ConnectionHasNoAwsCredentials"));
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(PKG, "RedshiftBulkLoaderMeta.Error.CredentialsNotResolved"), e);
    }
  }

  private String decrypt(String value) {
    return Encr.decryptPasswordOptionallyEncrypted(resolve(value));
  }

  /**
   * The columns the COPY statement should name.
   *
   * <p>An explicit mapping says it outright. Without one, the columns are known only for a file
   * this transform wrote itself, where they are the fields of the stream. A pre-existing file with
   * no mapping is the one case we cannot speak for, so the statement stays silent and Redshift
   * matches the file against the table positionally.
   *
   * @return the column names, empty when they cannot be established
   */
  private String[] copyColumnNames() {
    if (meta.isSpecifyFields()) {
      return meta.getFields().stream()
          .map(RedshiftBulkLoaderField::getDatabaseField)
          .toArray(String[]::new);
    }
    if (meta.isStreamToS3Csv() && data.columnNames != null) {
      return data.columnNames;
    }
    return new String[0];
  }

  /**
   * @return true when the file we load is a CSV file: either one we streamed to S3 ourselves, or an
   *     existing one the user declared as CSV
   */
  private boolean isCsvFormat() {
    return meta.isStreamToS3Csv()
        || RedshiftBulkLoaderMeta.FILE_FORMAT_CSV.equals(meta.getLoadFromExistingFileFormat());
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
          BaseMessages.getString(PKG, "RedshiftBulkLoaderMeta.Error.NoConnection"));
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
        logDetailed(
            BaseMessages.getString(
                PKG, "RedshiftBulkLoader.Log.ParentFolderCreated", HopVfs.getFriendlyURI(parent)));
      }
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "RedshiftBulkLoaderMeta.Error.CannotCreateParentFolder", filename),
          e);
    }
  }

  /**
   * The COPY statement always reads a file from S3, so we need to know which one. When we do not
   * write that file ourselves we also need to know its format.
   *
   * @throws HopException when the file settings are incomplete
   */
  /**
   * Fail before any row is read when a column was mapped to a table column that does not exist. The
   * COPY statement would reject it anyway, but only after the whole file was written to S3.
   *
   * @throws HopException when a selected column is not a column of the target table
   */
  protected void verifyTableFields() throws HopException {
    if (!meta.isSpecifyFields()) {
      return;
    }
    for (RedshiftBulkLoaderField field : meta.getFields()) {
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

  protected void verifyFileSettings() throws HopException {
    if (StringUtils.isEmpty(resolve(meta.getCopyFromFilename()))) {
      throw new HopException(
          BaseMessages.getString(PKG, "RedshiftBulkLoaderMeta.Error.NoCopyFromFilename"));
    }
    if (!meta.isStreamToS3Csv() && StringUtils.isEmpty(meta.getLoadFromExistingFileFormat())) {
      throw new HopException(
          BaseMessages.getString(PKG, "RedshiftBulkLoaderMeta.Error.NoFileFormat"));
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
  void writeRowToFile(IRowMeta rowMeta, Object[] row) throws HopTransformException {
    try {
      // The columns, in the order the COPY statement names them.
      for (int i = 0; i < data.streamFieldIndexes.length; i++) {
        if (i > 0) {
          data.writer.write(data.binarySeparator);
        }
        int index = data.streamFieldIndexes[i];
        IValueMeta valueMeta = data.writeValueMeta[i];
        Object valueData = null;
        if (index >= 0) {
          IValueMeta sourceMeta = data.sourceValueMeta[i];
          valueData =
              sourceMeta == null ? row[index] : valueMeta.convertData(sourceMeta, row[index]);
        }
        writeField(valueMeta, valueData, data.binaryNullValue);
      }
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
        // Strings are always enclosed, the COPY statement declares the same quote character.
        // Anything else is enclosed only when its own content would otherwise break the row.
        // JSON is the case that matters: Hop does not consider it a string, yet it is full of
        // commas and quotes and is often pretty printed across several lines.
        boolean writeEnclosures = v.isString() || needsEnclosure(str);

        if (writeEnclosures) {
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
      // no need to truncate
      byte[] text;
      text = string.getBytes(StandardCharsets.UTF_8);
      return text;
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

    // The stream is normally closed before the COPY statement runs, this catches the error paths.
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
