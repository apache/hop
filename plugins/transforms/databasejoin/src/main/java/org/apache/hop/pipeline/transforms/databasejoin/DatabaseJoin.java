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

package org.apache.hop.pipeline.transforms.databasejoin;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.databasejoin.cache.DatabaseCache;

/**
 * Use values from input streams to joins with values in a database. Freehand SQL can be used to do
 * this.
 */
public class DatabaseJoin extends BaseTransform<DatabaseJoinMeta, DatabaseJoinData> {

  private static final Class<?> PKG = DatabaseJoinMeta.class; // For Translator

  private final ReentrantLock dbLock = new ReentrantLock();

  public DatabaseJoin(
      TransformMeta transformMeta,
      DatabaseJoinMeta meta,
      DatabaseJoinData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  private void lookupValues(IRowMeta rowMeta, Object[] rowData) throws HopException {

    dbLock.lock();

    if (first) {
      first = false;

      data.outputRowMeta = rowMeta.clone();
      meta.getFields(
          data.outputRowMeta,
          getTransformName(),
          new IRowMeta[] {
            meta.getTableFields(this),
          },
          null,
          this,
          metadataProvider);

      data.lookupRowMeta = new RowMeta();

      if (log.isDetailed()) {
        logDetailed(
            BaseMessages.getString(PKG, "DatabaseJoin.Log.CheckingRow")
                + rowMeta.getString(rowData));
      }

      data.keynrs = new int[meta.getParameters().size()];

      for (int i = 0; i < data.keynrs.length; i++) {
        ParameterField field = meta.getParameters().get(i);
        data.keynrs[i] = rowMeta.indexOfValue(field.getName());
        if (data.keynrs[i] < 0) {
          throw new HopTransformException(
              BaseMessages.getString(PKG, "DatabaseJoin.Exception.FieldNotFound", field.getName()));
        }

        data.lookupRowMeta.addValueMeta(rowMeta.getValueMeta(data.keynrs[i]).clone());
      }
    }
    final ResultSet rs;
    try {
      // Construct the parameters row...
      Object[] lookupRowData = new Object[data.lookupRowMeta.size()];
      for (int i = 0; i < data.keynrs.length; i++) {
        lookupRowData[i] = rowData[data.keynrs[i]];
      }

      List<Object[]> adds = getFromCacheOrFetch(lookupRowData);

      IRowMeta addMeta = data.db.getReturnRowMeta();

      int counter = 0;
      for (Object[] add : adds) {
        if (add != null && (meta.getRowLimit() == 0 || counter < meta.getRowLimit())) {
          counter++;

          Object[] newRow = RowDataUtil.resizeArray(rowData, data.outputRowMeta.size());
          int newIndex = rowMeta.size();
          for (int i = 0; i < addMeta.size(); i++) {
            newRow[newIndex++] = add[i];
          }
          // we have to clone, otherwise we only get the last new value
          putRow(data.outputRowMeta, data.outputRowMeta.cloneRow(newRow));

          if (log.isRowLevel()) {
            logRowlevel(
                BaseMessages.getString(PKG, "DatabaseJoin.Log.PutoutRow")
                    + data.outputRowMeta.getString(newRow));
          }
        }
      }

      // Nothing found? Perhaps we have to put something out after all?
      if (counter == 0 && meta.isOuterJoin()) {
        if (data.notfound == null) {
          // Just return null values for all values...
          //
          data.notfound = new Object[data.db.getReturnRowMeta().size()];
        }
        Object[] newRow = RowDataUtil.resizeArray(rowData, data.outputRowMeta.size());
        int newIndex = rowMeta.size();
        for (int i = 0; i < data.notfound.length; i++) {
          newRow[newIndex++] = data.notfound[i];
        }
        putRow(data.outputRowMeta, newRow);
      }

    } finally {
      dbLock.unlock();
    }
  }

  private List<Object[]> getFromCacheOrFetch(Object[] lookupRowData) throws HopDatabaseException {
    if (meta.isCached()) {
      List<Object[]> adds = data.cache.getRowsFromCache(data.lookupRowMeta, lookupRowData);
      if (adds != null) {
        return adds;
      } else {
        List<Object[]> fromDatabase = fetchFromDatabase(lookupRowData);
        data.cache.putRowsIntoCache(data.lookupRowMeta, lookupRowData, fromDatabase);
        return fromDatabase;
      }
    }
    return fetchFromDatabase(lookupRowData);
  }

  private List<Object[]> fetchFromDatabase(Object[] lookupRowData) throws HopDatabaseException {
    List<Object[]> result = new ArrayList<>();

    ResultSet rs = data.db.openQuery(data.pstmt, data.lookupRowMeta, lookupRowData);
    Object[] add = data.db.getRow(rs);
    while (add != null) {
      result.add(add);
      incrementLinesInput();
      add = data.db.getRow(rs);
    }
    data.db.closeQuery(rs);
    return result;
  }

  @Override
  public boolean processRow() throws HopException {

    boolean sendToErrorRow = false;
    String errorMessage = null;

    Object[] r = getRow(); // Get row from input rowset & set row busy!
    if (r == null) { // no more input to be expected...
      setOutputDone();
      return false;
    }

    try {
      lookupValues(getInputRowMeta(), r); // add new values to the row in rowset[0].

      if (checkFeedback(getLinesRead())) {
        if (log.isBasic()) {
          logBasic(BaseMessages.getString(PKG, "DatabaseJoin.Log.LineNumber") + getLinesRead());
        }
      }
    } catch (HopException e) {

      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {

        logError(
            BaseMessages.getString(PKG, "DatabaseJoin.Log.ErrorInTransformRunning")
                + e.getMessage(),
            e);
        setErrors(1);
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), r, 1, errorMessage, null, "DBJOIN001");
      }
    }

    return true;
  }

  /**
   * Stop the running query In the Database Join transform data.isCancelled is checked before
   * synchronization and set after synchronization is completed.
   *
   * <p>To cancel a prepared statement we need a valid database connection which we do not have if
   * disposed has already been called
   */
  @Override
  public void stopRunning() throws HopException {
    if (this.isStopped() || data.isDisposed()) {
      return;
    }

    dbLock.lock();

    try {
      if (data.db != null && data.db.getConnection() != null && !data.isCanceled) {
        data.db.cancelStatement(data.pstmt);
        setStopped(true);
        data.isCanceled = true;
      }
    } finally {
      dbLock.unlock();
    }
  }

  @Override
  public boolean init() {
    if (super.init()) {

      if (Utils.isEmpty(meta.getConnection())) {
        logError(
            BaseMessages.getString(PKG, "DatabaseJoin.Init.ConnectionMissing", getTransformName()));
        return false;
      }
      dbLock.lock();
      try {
        DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
        if (databaseMeta == null) {
          logError(
              BaseMessages.getString(
                  PKG, "DatabaseJoin.Init.ConnectionMissing", getTransformName()));
          return false;
        }

        data.db = new Database(this, this, databaseMeta);

        try {
          data.db.connect();

          if (log.isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "DatabaseJoin.Log.ConnectedToDB"));
          }

          String sql = meta.getSql();
          if (meta.isReplaceVariables()) {
            sql = resolve(sql);
          }
          // Prepare the SQL statement
          data.pstmt = data.db.prepareSql(sql);
          if (log.isDebug()) {
            logDebug(BaseMessages.getString(PKG, "DatabaseJoin.Log.SQLStatement", sql));
          }
          data.db.setQueryLimit(meta.getRowLimit());

          if (meta.isCached()) {
            data.cache = new DatabaseCache(meta.getCacheSize());
          }

          return true;
        } catch (HopException e) {
          logError(
              BaseMessages.getString(PKG, "DatabaseJoin.Log.DatabaseError") + e.getMessage(), e);
          if (data.db != null) {
            data.db.disconnect();
          }
        }
      } finally {
        dbLock.unlock();
      }
    }

    return false;
  }

  @Override
  public void dispose() {
    dbLock.lock();
    try {
      if (data.pstmt != null) {
        data.db.closePreparedStatement(data.pstmt);
        data.pstmt = null;
      }
      super.dispose();
    } catch (HopDatabaseException e) {
      logError("Unexpected error closing statement : " + e.toString());
      setErrors(1);
      stopAll();
    } finally {
      if (data.db != null) {
        data.db.disconnect();
        data.db = null;
      }
      data.cache = null;
      dbLock.unlock();
    }
  }
}
