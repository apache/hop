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

package org.apache.hop.pipeline.transforms.vertica.bulkloader;

import com.google.common.annotations.VisibleForTesting;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PipedInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.sql.PooledConnection;
import org.apache.commons.dbcp2.DelegatingConnection;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.vertica.bulkloader.nativebinary.ColumnSpec;
import org.apache.hop.pipeline.transforms.vertica.bulkloader.nativebinary.ColumnType;
import org.apache.hop.pipeline.transforms.vertica.bulkloader.nativebinary.StreamEncoder;

public class VerticaBulkLoader extends BaseTransform<VerticaBulkLoaderMeta, VerticaBulkLoaderData> {
  private static final Class<?> PKG =
      VerticaBulkLoader.class; // for i18n purposes, needed by Translator2!!

  private static final SimpleDateFormat SIMPLE_DATE_FORMAT =
      new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  public static final String CONST_FIELD = "Field ";
  public static final String CONST_MUST_BE_A_DATE_COMPATIBLE_TYPE_TO_MATCH_TARGET_COLUMN =
      " must be a Date compatible type to match target column ";
  private FileOutputStream exceptionLog;
  private FileOutputStream rejectedLog;

  public VerticaBulkLoader(
      TransformMeta transformMeta,
      VerticaBulkLoaderMeta meta,
      VerticaBulkLoaderData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] r = getRow(); // this also waits for a previous transform to be
    // finished.
    if (r == null) { // no more input to be expected...
      if (first && meta.isTruncateTable() && !meta.isOnlyWhenHaveRows()) {
        truncateTable();
      }

      try {
        data.close();
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

      IRowMeta tableMeta = meta.getRequiredFields(variables);

      if (!meta.specifyFields()) {

        // Just take the whole input row
        data.insertRowMeta = getInputRowMeta().clone();
        data.selectedRowFieldIndices = new int[data.insertRowMeta.size()];

        data.colSpecs = new ArrayList<>(data.insertRowMeta.size());

        for (int insertFieldIdx = 0; insertFieldIdx < data.insertRowMeta.size(); insertFieldIdx++) {
          data.selectedRowFieldIndices[insertFieldIdx] = insertFieldIdx;
          IValueMeta inputValueMeta = data.insertRowMeta.getValueMeta(insertFieldIdx);
          IValueMeta insertValueMeta = inputValueMeta.clone();
          IValueMeta targetValueMeta = tableMeta.getValueMeta(insertFieldIdx);
          insertValueMeta.setName(targetValueMeta.getName());
          data.insertRowMeta.setValueMeta(insertFieldIdx, insertValueMeta);
          ColumnSpec cs = getColumnSpecFromField(inputValueMeta, insertValueMeta, targetValueMeta);
          data.colSpecs.add(insertFieldIdx, cs);
        }

      } else {

        int numberOfInsertFields = meta.getFields().size();
        data.insertRowMeta = new RowMeta();
        data.colSpecs = new ArrayList<>(numberOfInsertFields);

        // Cache the position of the selected fields in the row array
        data.selectedRowFieldIndices = new int[numberOfInsertFields];
        for (int insertFieldIdx = 0; insertFieldIdx < numberOfInsertFields; insertFieldIdx++) {
          VerticaBulkLoaderField vbf = meta.getFields().get(insertFieldIdx);
          String inputFieldName = vbf.getFieldStream();
          int inputFieldIdx = getInputRowMeta().indexOfValue(inputFieldName);
          if (inputFieldIdx < 0) {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG,
                    "VerticaBulkLoader.Exception.FieldRequired",
                    inputFieldName)); //$NON-NLS-1$
          }
          data.selectedRowFieldIndices[insertFieldIdx] = inputFieldIdx;

          String insertFieldName = vbf.getFieldDatabase();
          IValueMeta inputValueMeta = getInputRowMeta().getValueMeta(inputFieldIdx);
          if (inputValueMeta == null) {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG,
                    "VerticaBulkLoader.Exception.FailedToFindField",
                    vbf.getFieldStream())); // $NON-NLS-1$
          }
          IValueMeta insertValueMeta = inputValueMeta.clone();
          insertValueMeta.setName(insertFieldName);
          data.insertRowMeta.addValueMeta(insertValueMeta);

          IValueMeta targetValueMeta = tableMeta.searchValueMeta(insertFieldName);
          ColumnSpec cs = getColumnSpecFromField(inputValueMeta, insertValueMeta, targetValueMeta);
          data.colSpecs.add(insertFieldIdx, cs);
        }
      }

      try {
        data.pipedInputStream = new PipedInputStream();
        if (Utils.isEmpty(data.colSpecs)) {
          return false;
        }
        data.encoder = createStreamEncoder(data.colSpecs, data.pipedInputStream);

        initializeWorker();
        data.encoder.writeHeader();

      } catch (IOException ioe) {
        throw new HopTransformException("Error creating stream encoder", ioe);
      }
    }

    try {
      Object[] outputRowData = writeToOutputStream(r);
      if (outputRowData != null) {
        putRow(data.outputRowMeta, outputRowData); // in case we want it
        // go further...
        incrementLinesOutput();
      }

      if (checkFeedback(getLinesRead()) && isBasic()) {
        logBasic("linenr " + getLinesRead());
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

    return true;
  }

  @VisibleForTesting
  void initializeLogFiles() throws HopException {
    try {
      if (!StringUtil.isEmpty(meta.getExceptionsFileName())) {
        exceptionLog = new FileOutputStream(meta.getExceptionsFileName(), true);
      }
      if (!StringUtil.isEmpty(meta.getRejectedDataFileName())) {
        rejectedLog = new FileOutputStream(meta.getRejectedDataFileName(), true);
      }
    } catch (FileNotFoundException ex) {
      throw new HopException(ex);
    }
  }

  @VisibleForTesting
  void writeExceptionRejectionLogs(HopValueException valueException, Object[] outputRowData)
      throws IOException {
    String dateTimeString =
        (SIMPLE_DATE_FORMAT.format(new Date(System.currentTimeMillis()))) + " - ";
    logError(
        BaseMessages.getString(
            PKG,
            "VerticaBulkLoader.Exception.RowRejected",
            Arrays.stream(outputRowData).map(Object::toString).collect(Collectors.joining(" | "))));

    if (exceptionLog != null) {
      // Replace used to ensure timestamps are being added appropriately (some messages are
      // multi-line)
      exceptionLog.write(
          (dateTimeString
                  + valueException
                      .getMessage()
                      .replace(System.lineSeparator(), System.lineSeparator() + dateTimeString))
              .getBytes());
      exceptionLog.write(System.lineSeparator().getBytes());
      for (StackTraceElement element : valueException.getStackTrace()) {
        exceptionLog.write(
            (dateTimeString + "at " + element.toString() + System.lineSeparator()).getBytes());
      }
      exceptionLog.write(
          (dateTimeString
                  + "Caused by: "
                  + valueException.getClass().toString()
                  + System.lineSeparator())
              .getBytes());
      // Replace used to ensure timestamps are being added appropriately (some messages are
      // multi-line)
      exceptionLog.write(
          ((dateTimeString
                  + valueException
                      .getCause()
                      .getMessage()
                      .replace(System.lineSeparator(), System.lineSeparator() + dateTimeString))
              .getBytes()));
      exceptionLog.write(System.lineSeparator().getBytes());
    }
    if (rejectedLog != null) {
      rejectedLog.write(
          (dateTimeString
                  + BaseMessages.getString(
                      PKG,
                      "VerticaBulkLoader.Exception.RowRejected",
                      Arrays.stream(outputRowData)
                          .map(Object::toString)
                          .collect(Collectors.joining(" | "))))
              .getBytes());
      for (Object outputRowDatum : outputRowData) {
        rejectedLog.write((outputRowDatum.toString() + " | ").getBytes());
      }
      rejectedLog.write(System.lineSeparator().getBytes());
    }
  }

  @VisibleForTesting
  void closeLogFiles() throws HopException {
    try {
      if (exceptionLog != null) {
        exceptionLog.close();
      }
      if (rejectedLog != null) {
        rejectedLog.close();
      }
    } catch (IOException exception) {
      throw new HopException(exception);
    }
  }

  private ColumnSpec getColumnSpecFromField(
      IValueMeta inputValueMeta, IValueMeta insertValueMeta, IValueMeta targetValueMeta) {
    logBasic(
        "Mapping input field "
            + inputValueMeta.getName()
            + " ("
            + inputValueMeta.getTypeDesc()
            + ")"
            + " to target column "
            + insertValueMeta.getName()
            + " ("
            + targetValueMeta.getOriginalColumnTypeName()
            + ") ");

    String targetColumnTypeName = targetValueMeta.getOriginalColumnTypeName().toUpperCase();

    switch (targetColumnTypeName) {
      case "INTEGER", "BIGINT" -> {
        return new ColumnSpec(ColumnSpec.ConstantWidthType.INTEGER_64);
      }
      case "BOOLEAN" -> {
        return new ColumnSpec(ColumnSpec.ConstantWidthType.BOOLEAN);
      }
      case "FLOAT", "DOUBLE PRECISION" -> {
        return new ColumnSpec(ColumnSpec.ConstantWidthType.FLOAT);
      }
      case "CHAR" -> {
        return new ColumnSpec(ColumnSpec.UserDefinedWidthType.CHAR, targetValueMeta.getLength());
      }
      case "VARCHAR", "CHARACTER VARYING" -> {
        return new ColumnSpec(ColumnSpec.VariableWidthType.VARCHAR, targetValueMeta.getLength());
      }
      case "DATE" -> {
        if (inputValueMeta.isDate() == false) {
          throw new IllegalArgumentException(
              CONST_FIELD
                  + inputValueMeta.getName()
                  + CONST_MUST_BE_A_DATE_COMPATIBLE_TYPE_TO_MATCH_TARGET_COLUMN
                  + insertValueMeta.getName());
        } else {
          return new ColumnSpec(ColumnSpec.ConstantWidthType.DATE);
        }
      }
      case "TIME" -> {
        if (inputValueMeta.isDate() == false) {
          throw new IllegalArgumentException(
              CONST_FIELD
                  + inputValueMeta.getName()
                  + CONST_MUST_BE_A_DATE_COMPATIBLE_TYPE_TO_MATCH_TARGET_COLUMN
                  + insertValueMeta.getName());
        } else {
          return new ColumnSpec(ColumnSpec.ConstantWidthType.TIME);
        }
      }
      case "TIMETZ" -> {
        if (inputValueMeta.isDate() == false) {
          throw new IllegalArgumentException(
              CONST_FIELD
                  + inputValueMeta.getName()
                  + CONST_MUST_BE_A_DATE_COMPATIBLE_TYPE_TO_MATCH_TARGET_COLUMN
                  + insertValueMeta.getName());
        } else {
          return new ColumnSpec(ColumnSpec.ConstantWidthType.TIMETZ);
        }
      }
      case "TIMESTAMP" -> {
        if (inputValueMeta.isDate() == false) {
          throw new IllegalArgumentException(
              CONST_FIELD
                  + inputValueMeta.getName()
                  + CONST_MUST_BE_A_DATE_COMPATIBLE_TYPE_TO_MATCH_TARGET_COLUMN
                  + insertValueMeta.getName());
        } else {
          return new ColumnSpec(ColumnSpec.ConstantWidthType.TIMESTAMP);
        }
      }
      case "TIMESTAMPTZ" -> {
        if (inputValueMeta.isDate() == false) {
          throw new IllegalArgumentException(
              CONST_FIELD
                  + inputValueMeta.getName()
                  + CONST_MUST_BE_A_DATE_COMPATIBLE_TYPE_TO_MATCH_TARGET_COLUMN
                  + insertValueMeta.getName());
        } else {
          return new ColumnSpec(ColumnSpec.ConstantWidthType.TIMESTAMPTZ);
        }
      }
      case "INTERVAL", "INTERVAL DAY TO SECOND" -> {
        if (inputValueMeta.isDate() == false) {
          throw new IllegalArgumentException(
              CONST_FIELD
                  + inputValueMeta.getName()
                  + CONST_MUST_BE_A_DATE_COMPATIBLE_TYPE_TO_MATCH_TARGET_COLUMN
                  + insertValueMeta.getName());
        } else {
          return new ColumnSpec(ColumnSpec.ConstantWidthType.INTERVAL);
        }
      }
      case "BINARY" -> {
        return new ColumnSpec(ColumnSpec.VariableWidthType.VARBINARY, targetValueMeta.getLength());
      }
      case "VARBINARY" -> {
        return new ColumnSpec(ColumnSpec.VariableWidthType.VARBINARY, targetValueMeta.getLength());
      }
      case "NUMERIC" -> {
        return new ColumnSpec(
            ColumnSpec.PrecisionScaleWidthType.NUMERIC,
            targetValueMeta.getLength(),
            targetValueMeta.getPrecision());
      }
    }
    throw new IllegalArgumentException(
        "Column type " + targetColumnTypeName + " not supported."); // $NON-NLS-1$
  }

  private void initializeWorker() {
    final String dml = buildCopyStatementSqlString();

    data.workerThread =
        Executors.defaultThreadFactory()
            .newThread(
                () -> {
                  try {
                    VerticaCopyStream stream = createVerticaCopyStream(dml);
                    stream.start();
                    stream.addStream(data.pipedInputStream);
                    setLinesRejected(stream.getRejects().size());
                    stream.execute();
                    long rowsLoaded = stream.finish();
                    if (getLinesOutput() != rowsLoaded) {
                      logMinimal(
                          String.format(
                              "%d records loaded out of %d records sent.",
                              rowsLoaded, getLinesOutput()));
                    }
                    data.db.disconnect();
                  } catch (SQLException
                      | IllegalStateException
                      | ClassNotFoundException
                      | HopException e) {
                    if (e.getCause() instanceof InterruptedIOException) {
                      logBasic("SQL statement interrupted by halt of pipeline");
                    } else {
                      logError("SQL Error during statement execution.", e);
                      setErrors(1);
                      stopAll();
                      setOutputDone(); // signal end to receiver(s)
                    }
                  }
                });

    data.workerThread.start();
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
    final IRowMeta fields = data.insertRowMeta;
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      ColumnType columnType = data.colSpecs.get(i).type;
      IValueMeta valueMeta = fields.getValueMeta(i);
      switch (columnType) {
        case NUMERIC:
          sb.append("TMPFILLERCOL").append(i).append(" FILLER VARCHAR(1000), ");
          // Force columns to be quoted:
          sb.append(
              databaseMeta.getStartQuote() + valueMeta.getName() + databaseMeta.getEndQuote());
          sb.append(" AS CAST(").append("TMPFILLERCOL").append(i).append(" AS NUMERIC");
          sb.append(")");
          break;
        default:
          // Force columns to be quoted:
          sb.append(
              databaseMeta.getStartQuote() + valueMeta.getName() + databaseMeta.getEndQuote());
          break;
      }
    }
    sb.append(")");

    sb.append(" FROM STDIN NATIVE ");

    if (!StringUtil.isEmpty(meta.getExceptionsFileName())) {
      sb.append("EXCEPTIONS E'")
          .append(meta.getExceptionsFileName().replace("'", "\\'"))
          .append("' ");
    }

    if (!StringUtil.isEmpty(meta.getRejectedDataFileName())) {
      sb.append("REJECTED DATA E'")
          .append(meta.getRejectedDataFileName().replace("'", "\\'"))
          .append("' ");
    }

    // TODO: Should eventually get a preference for this, but for now, be backward compatible.
    sb.append("ENFORCELENGTH ");

    if (meta.isAbortOnError()) {
      sb.append("ABORT ON ERROR ");
    }

    if (meta.isDirect()) {
      sb.append("DIRECT ");
    }

    if (!StringUtil.isEmpty(meta.getStreamName())) {
      sb.append("STREAM NAME E'")
          .append(data.db.resolve(meta.getStreamName()).replace("'", "\\'"))
          .append("' ");
    }

    // XXX: I believe the right thing to do here is always use NO COMMIT since we want Hop's
    // configuration to drive.
    // NO COMMIT does not seem to work even when the pipeline setting 'make the pipeline database
    // transactional' is on

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

    try {
      data.encoder.writeRow(data.insertRowMeta, insertRowData);
    } catch (HopValueException valueException) {
      /*
       *  If we are to abort, we should continue throwing the exception. If we are not aborting, we need to set the
       *  outputRowData to null, so the next transform knows not to add it and continue. We also need to write to the
       *  rejected log what data failed (print out the outputRowData before null'ing it) and write to the error log the
       *  issue.
       */
      // write outputRowData -> Rejected Row
      // write Error Log as to why it was rejected
      writeExceptionRejectionLogs(valueException, outputRowData);
      if (meta.isAbortOnError()) {
        throw valueException;
      }
      outputRowData = null;
    } catch (IOException e) {
      if (!data.isStopped()) {
        throw new HopException("I/O Error during row write.", e);
      }
    }

    return outputRowData;
  }

  protected void verifyDatabaseConnection() throws HopException {
    // Confirming Database Connection is defined.
    if (meta.getConnection() == null) {
      throw new HopException(
          BaseMessages.getString(PKG, "VerticaBulkLoaderMeta.Error.NoConnection"));
    }
  }

  @Override
  public boolean init() {

    if (super.init()) {
      try {
        // Validating that the connection has been defined.
        verifyDatabaseConnection();
        data.databaseMeta = this.getPipelineMeta().findDatabase(meta.getConnection(), variables);
        initializeLogFiles();

        data.db = new Database(this, this, data.databaseMeta);
        data.db.connect();

        if (isBasic()) {
          logBasic("Connected to database [" + meta.getDatabaseMeta() + "]");
        }

        data.db.setAutoCommit(false);

        return true;
      } catch (HopException e) {
        logError("An error occurred intialising this transform: " + e.getMessage());
        stopAll();
        setErrors(1);
      }
    }
    return false;
  }

  @Override
  public void markStop() {
    // Close the exception/rejected loggers at the end
    try {
      closeLogFiles();
    } catch (HopException ex) {
      logError(BaseMessages.getString(PKG, "VerticaBulkLoader.Exception.ClosingLogError", ex));
    }
    super.markStop();
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
          } catch (InterruptedException e) {
            // ignore this and continue processing rows
          }
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
    data.colSpecs = null;
    data.encoder = null;

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
      } catch (InterruptedException e) {
        // ignore this and continue processing rows
      }
    }

    if (data.db != null) {
      data.db.disconnect();
    }
    super.dispose();
  }

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
  VerticaConnection getVerticaConnection() throws SQLException {

    Connection conn = data.db.getConnection();
    if (conn != null) {
      if (conn instanceof VerticaConnection verticaConnection) {
        return verticaConnection;
      } else {
        Connection underlyingConn = null;
        if (conn instanceof DelegatingConnection delegatingConnection) {
          DelegatingConnection pooledConn = delegatingConnection;
          underlyingConn = pooledConn.getInnermostDelegate();
        } else if (conn instanceof javax.sql.PooledConnection pooledConnection) {
          PooledConnection pooledConn = pooledConnection;
          underlyingConn = pooledConn.getConnection();
        } else {
          // Last resort - attempt to use unwrap to get at the connection.
          try {
            if (conn.isWrapperFor(VerticaConnection.class)) {
              return conn.unwrap(VerticaConnection.class);
            }
          } catch (SQLException ignored) {
            // ignored - the connection doesn't support unwrap or the connection cannot be
            // unwrapped into a VerticaConnection.
          }
        }
        if ((underlyingConn != null)
            && (underlyingConn instanceof VerticaConnection verticaConnection)) {
          return verticaConnection;
        }
      }
      throw new IllegalStateException(
          "Could not retrieve a VerticaConnection from " + conn.getClass().getName());
    } else {
      throw new IllegalStateException("Could not retrieve a VerticaConnection from null");
    }
  }
}
