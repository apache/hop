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

package org.apache.hop.pipeline.transforms.dbproc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Retrieves values from a database by calling database stored procedures or functions */
public class DBProc extends BaseTransform<DBProcMeta, DBProcData> {
  private static final Class<?> PKG = DBProcMeta.class;

  public DBProc(
      TransformMeta transformMeta,
      DBProcMeta meta,
      DBProcData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private boolean scalarResult() {
    return !meta.isResultRows() && StringUtils.isNotEmpty(meta.getResultName());
  }

  private void prepareProcedure(IRowMeta rowMeta) throws HopException {
    if (!first) {
      return;
    }
    first = false;

    data.outputMeta = data.inputRowMeta.clone();
    meta.getFields(data.outputMeta, getTransformName(), null, null, this, metadataProvider);

    List<DBProcMeta.ProcArgument> arguments =
        meta.getArguments() == null ? List.of() : meta.getArguments();
    data.argnrs = new int[arguments.size()];
    for (int i = 0; i < arguments.size(); i++) {
      DBProcMeta.ProcArgument argument = arguments.get(i);
      if (!argument.getDirection().equalsIgnoreCase("OUT")) { // IN or INOUT
        data.argnrs[i] = rowMeta.indexOfValue(argument.getName());
        if (data.argnrs[i] < 0) {
          logError(
              BaseMessages.getString(PKG, "DBProc.Log.ErrorFindingField")
                  + argument.getName()
                  + "]");
          throw new HopTransformException(
              BaseMessages.getString(
                  PKG, "DBProc.Exception.CouldnotFindField", argument.getName()));
        }
      } else {
        data.argnrs[i] = -1;
      }
    }

    // A Row result is a result set, not a JDBC function return value.
    String resultName = scalarResult() ? meta.getResult().getName() : null;
    int resultType = scalarResult() ? meta.getResult().getHopType() : IValueMeta.TYPE_NONE;
    data.db.setProcLookup(
        resolve(meta.getProcedure()),
        meta.argumentNames(),
        meta.argumentDirections(),
        meta.argumentTypes(),
        resultName,
        resultType);
  }

  private void runProc(IRowMeta rowMeta, Object[] rowData) throws HopException {
    prepareProcedure(rowMeta);
    boolean scalar = scalarResult();
    data.db.setProcValues(rowMeta, rowData, data.argnrs, meta.argumentDirections(), scalar);

    if (meta.isResultRows()) {
      boolean executed = false;
      ResultSet resultSet = null;
      try {
        RowMetaAndData add =
            data.db.callProcedure(
                meta.argumentNames(),
                meta.argumentDirections(),
                meta.argumentTypes(),
                null,
                IValueMeta.TYPE_NONE,
                true);
        executed = true;
        resultSet = data.db.takeProcedureResultSet();
        writeResultRows(rowMeta, rowData, add, resultSet);
      } finally {
        try {
          if (resultSet != null) {
            resultSet.close();
          }
        } catch (SQLException e) {
          throw new HopDatabaseException(
              BaseMessages.getString(PKG, "DBProc.Exception.UnableToReadResultSet"), e);
        } finally {
          if (executed) {
            data.db.discardProcedureResults();
          }
        }
      }
      return;
    }

    String resultName = scalar ? meta.getResult().getName() : null;
    int resultType = scalar ? meta.getResult().getHopType() : IValueMeta.TYPE_NONE;
    RowMetaAndData add =
        data.db.callProcedure(
            meta.argumentNames(),
            meta.argumentDirections(),
            meta.argumentTypes(),
            resultName,
            resultType);
    Object[] outputRowData =
        buildOutputRow(
            rowData,
            rowMeta.size(),
            data.outputMeta.size(),
            add.getData(),
            data.argnrs,
            meta.getArguments(),
            scalar,
            0,
            false);
    putRow(data.outputMeta, outputRowData);
  }

  private void writeResultRows(
      IRowMeta rowMeta, Object[] rowData, RowMetaAndData procedureData, ResultSet resultSet)
      throws HopException {
    if (resultSet == null) {
      return;
    }
    List<DBProcField> fields = meta.activeResultFields();
    int inputSize = rowMeta == null ? 0 : rowMeta.size();
    Object[] template =
        buildOutputRow(
            rowData,
            inputSize,
            data.outputMeta.size(),
            procedureData == null ? null : procedureData.getData(),
            data.argnrs,
            meta.getArguments(),
            false,
            fields.size(),
            true);
    try {
      int[] indexes = resultColumnIndexes(resultColumnNames(resultSet.getMetaData()), fields, this);
      while (resultSet.next()) {
        Object[] output = RowDataUtil.createResizedCopy(template, data.outputMeta.size());
        for (int i = 0; i < indexes.length; i++) {
          if (indexes[i] < 0) {
            continue;
          }
          IValueMeta valueMeta = data.outputMeta.getValueMeta(inputSize + i);
          output[inputSize + i] =
              data.db.getDatabaseMeta().getValueFromResultSet(resultSet, valueMeta, indexes[i]);
        }
        putRow(data.outputMeta, output);
      }
    } catch (SQLException e) {
      throw new HopDatabaseException(
          BaseMessages.getString(PKG, "DBProc.Exception.UnableToReadResultSet"), e);
    }
  }

  /**
   * @param copy when {@code true}, always allocate a new row. {@link RowDataUtil#resizeArray}
   *     returns the same array when it is already large enough, which aliases every result row.
   */
  static Object[] buildOutputRow(
      Object[] rowData,
      int inputSize,
      int outputSize,
      Object[] procedureData,
      int[] argnrs,
      List<DBProcMeta.ProcArgument> arguments,
      boolean scalarResult,
      int resultFieldCount,
      boolean copy) {
    Object[] source = rowData == null ? new Object[0] : rowData;
    Object[] output =
        copy
            ? RowDataUtil.createResizedCopy(source, outputSize)
            : RowDataUtil.resizeArray(source, outputSize);
    int outputIndex = inputSize + resultFieldCount;
    int addIndex = 0;
    if (scalarResult) {
      output[outputIndex++] = procedureData[addIndex++];
    }
    if (arguments == null) {
      return output;
    }
    for (int i = 0; i < arguments.size(); i++) {
      DBProcMeta.ProcArgument argument = arguments.get(i);
      if (argument.getDirection().equalsIgnoreCase("OUT")) {
        output[outputIndex++] = procedureData[addIndex++];
      } else if (argument.getDirection().equalsIgnoreCase("INOUT")) {
        output[argnrs[i]] = procedureData[addIndex++];
      }
    }
    return output;
  }

  static String[] resultColumnNames(ResultSetMetaData metadata) throws SQLException {
    if (metadata == null) {
      return new String[0];
    }
    int count = metadata.getColumnCount();
    String[] names = new String[count];
    for (int i = 0; i < count; i++) {
      String label = metadata.getColumnLabel(i + 1);
      if (Utils.isEmpty(label)) {
        label = metadata.getColumnName(i + 1);
      }
      names[i] = label;
    }
    return names;
  }

  static int[] resultColumnIndexes(
      String[] columnNames, List<DBProcField> fields, IVariables variables) {
    if (fields == null || fields.isEmpty()) {
      return new int[0];
    }
    Map<String, Integer> byName = new HashMap<>();
    if (columnNames != null) {
      for (int i = 0; i < columnNames.length; i++) {
        if (columnNames[i] != null) {
          byName.putIfAbsent(columnNames[i].toLowerCase(Locale.ROOT), i);
        }
      }
    }
    int[] indexes = new int[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      String name = fields.get(i).getName();
      if (variables != null && name != null) {
        name = variables.resolve(name);
      }
      Integer index = name == null ? null : byName.get(name.toLowerCase(Locale.ROOT));
      indexes[i] = index == null ? -1 : index;
    }
    return indexes;
  }

  @Override
  public boolean processRow() throws HopException {
    boolean sendToErrorRow = false;
    String errorMessage = null;

    // A procedure/function could also have no input at all
    // However, we would still need to know how many times it gets executed.
    // In short: the procedure gets executed once for every input row.
    //
    Object[] r;

    if (data.readsRows) {
      r = getRow(); // Get row from input rowset & set row busy!
      if (r == null) { // no more input to be expected...

        setOutputDone();
        return false;
      }
      data.inputRowMeta = getInputRowMeta();
    } else {
      r = new Object[] {}; // empty row
      incrementLinesRead();
      data.inputRowMeta = new RowMeta(); // empty row metadata too
      data.readsRows = true; // make it drop out of the loop at the next entrance to this method
    }

    try {
      runProc(data.inputRowMeta, r); // add new values to the row in rowset[0].

      if (checkFeedback(getLinesRead()) && isBasic()) {
        logBasic(BaseMessages.getString(PKG, "DBProc.LineNumber") + getLinesRead());
      }
    } catch (HopException e) {

      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
        // CHE: Read the chained SQL exceptions and add them
        // to the errorMessage
        SQLException nextSqlExOnChain = null;
        if ((e.getCause() != null) && (e.getCause() instanceof SQLException sqlException)) {
          nextSqlExOnChain = sqlException.getNextException();
          while (nextSqlExOnChain != null) {
            errorMessage = errorMessage + nextSqlExOnChain.getMessage() + Const.CR;
            nextSqlExOnChain = nextSqlExOnChain.getNextException();
          }
        }
      } else {

        logError(BaseMessages.getString(PKG, "DBProc.ErrorInTransformRunning") + e.getMessage());
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }

      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), r, 1, errorMessage, null, "DBP001");
      }
    }

    return true;
  }

  @Override
  public boolean init() {

    if (super.init()) {
      List<TransformMeta> previous = getPipelineMeta().findPreviousTransforms(getTransformMeta());
      if (!Utils.isEmpty(previous)) {
        data.readsRows = true;
      }

      DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
      data.db = new Database(this, this, databaseMeta);
      try {
        data.db.connect();

        if (!meta.isAutoCommit()) {
          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "DBProc.Log.AutoCommit"));
          }
          data.db.setCommit(9999);
        }
        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "DBProc.Log.ConnectedToDB"));
        }

        return true;
      } catch (HopException e) {
        logError(BaseMessages.getString(PKG, "DBProc.Log.DBException") + e.getMessage());
        if (data.db != null) {
          data.db.disconnect();
        }
      }
    }
    return false;
  }

  @Override
  public void dispose() {

    if (data.db != null) {
      // CHE: Properly close the callable statement
      try {
        data.db.closeProcedureStatement();
      } catch (HopDatabaseException e) {
        logError(BaseMessages.getString(PKG, "DBProc.Log.CloseProcedureError") + e.getMessage());
      }

      try {
        if (!meta.isAutoCommit()) {
          data.db.commit();
        }
      } catch (HopDatabaseException e) {
        logError(BaseMessages.getString(PKG, "DBProc.Log.CommitError") + e.getMessage());
      } finally {
        data.db.disconnect();
      }
    }
    super.dispose();
  }
}
