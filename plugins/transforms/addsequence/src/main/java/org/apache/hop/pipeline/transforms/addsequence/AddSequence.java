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

package org.apache.hop.pipeline.transforms.addsequence;

import org.apache.hop.core.Counter;
import org.apache.hop.core.Counters;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Adds a sequential number to a stream of rows. */
public class AddSequence extends BaseTransform<AddSequenceMeta, AddSequenceData> {

  private static final Class<?> PKG = AddSequence.class;
  public static final String CONST_ADD_SEQUENCE_LOG_COULD_NOT_PARSE_COUNTER_VALUE =
      "AddSequence.Log.CouldNotParseCounterValue";

  public AddSequence(
      TransformMeta transformMeta,
      AddSequenceMeta meta,
      AddSequenceData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  public synchronized Object[] addSequence(IRowMeta inputRowMeta, Object[] inputRowData)
      throws HopException {
    Object next;

    if (meta.isCounterUsed()) {
      next = data.counter.getAndNext();
      logDetailed("count name: {0}, next: {1}", data.getLookup(), next);
    } else if (meta.isDatabaseUsed()) {
      try {
        next =
            data.getDb()
                .getNextSequenceValue(
                    data.realSchemaName, data.realSequenceName, meta.getValueName());
      } catch (HopDatabaseException dbe) {
        throw new HopTransformException(
            BaseMessages.getString(
                PKG, "AddSequence.Exception.ErrorReadingSequence", data.realSequenceName),
            dbe);
      }
    } else {
      // This should never happen, but if it does, don't continue!!!
      throw new HopTransformException(
          BaseMessages.getString(PKG, "AddSequence.Exception.NoSpecifiedMethod"));
    }

    if (next != null) {
      Object[] outputRowData = inputRowData;
      if (inputRowData.length < inputRowMeta.size() + 1) {
        outputRowData = RowDataUtil.resizeArray(inputRowData, inputRowMeta.size() + 1);
      }
      outputRowData[inputRowMeta.size()] = next;
      return outputRowData;
    } else {
      throw new HopTransformException(
          BaseMessages.getString(PKG, "AddSequence.Exception.CouldNotFindNextValueForSequence")
              + meta.getValueName());
    }
  }

  @Override
  public boolean processRow() throws HopException {
    // Get row from input rowset & set row busy!
    Object[] r = getRow();
    if (r == null) {
      // no more input to be expected...
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    }

    if (isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(PKG, "AddSequence.Log.ReadRow")
              + getLinesRead()
              + " : "
              + getInputRowMeta().getString(r));
    }

    try {
      putRow(data.outputRowMeta, addSequence(getInputRowMeta(), r));

      if (isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(PKG, "AddSequence.Log.WriteRow")
                + getLinesWritten()
                + " : "
                + getInputRowMeta().getString(r));
      }
      if (checkFeedback(getLinesRead()) && isBasic()) {
        logBasic(BaseMessages.getString(PKG, "AddSequence.Log.LineNumber") + getLinesRead());
      }
    } catch (HopException e) {
      logError(BaseMessages.getString(PKG, "AddSequence.Log.ErrorInTransform") + e.getMessage());
      setErrors(1);
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }

    return true;
  }

  @Override
  public boolean init() {

    if (super.init()) {
      data.realSchemaName = resolve(meta.getSchemaName());
      data.realSequenceName = resolve(meta.getSequenceName());
      if (meta.isDatabaseUsed()) {

        if (Utils.isEmpty(meta.getConnection())) {
          logError(
              BaseMessages.getString(
                  PKG, "InsertUpdate.Init.ConnectionMissing", getTransformName()));
          return false;
        }

        DatabaseMeta databaseMeta = getPipelineMeta().findDatabase(meta.getConnection(), variables);
        Database db = new Database(this, this, databaseMeta);
        data.setDb(db);
        try {
          data.getDb().connect();
          if (isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "AddSequence.Log.ConnectedDB"));
          }
          return true;
        } catch (HopDatabaseException dbe) {
          logError(
              BaseMessages.getString(PKG, "AddSequence.Log.CouldNotConnectToDB")
                  + dbe.getMessage());
        }
      } else if (meta.isCounterUsed()) {
        // Do the environment translations of the counter values.
        boolean doAbort = false;
        try {
          data.start = Long.parseLong(resolve(meta.getStartAt()));
        } catch (NumberFormatException ex) {
          logError(
              BaseMessages.getString(
                  PKG,
                  CONST_ADD_SEQUENCE_LOG_COULD_NOT_PARSE_COUNTER_VALUE,
                  "start",
                  meta.getStartAt(),
                  resolve(meta.getStartAt()),
                  ex.getMessage()));
          doAbort = true;
        }

        try {
          data.increment = Long.parseLong(resolve(meta.getIncrementBy()));
        } catch (NumberFormatException ex) {
          logError(
              BaseMessages.getString(
                  PKG,
                  CONST_ADD_SEQUENCE_LOG_COULD_NOT_PARSE_COUNTER_VALUE,
                  "increment",
                  meta.getIncrementBy(),
                  resolve(meta.getIncrementBy()),
                  ex.getMessage()));
          doAbort = true;
        }

        try {
          data.maximum = Long.parseLong(resolve(meta.getMaxValue()));
        } catch (NumberFormatException ex) {
          logError(
              BaseMessages.getString(
                  PKG,
                  CONST_ADD_SEQUENCE_LOG_COULD_NOT_PARSE_COUNTER_VALUE,
                  "increment",
                  meta.getMaxValue(),
                  resolve(meta.getMaxValue()),
                  ex.getMessage()));
          doAbort = true;
        }

        if (doAbort) {
          return false;
        }

        String realCounterName = resolve(meta.getCounterName());
        if (!Utils.isEmpty(realCounterName)) {
          data.setLookup(lookupCounterName(realCounterName));
        } else {
          data.setLookup(lookupCounterName(meta.getValueName()));
        }

        // We need to synchronize over the whole pipeline to make sure that we always get the same
        // counter
        // regardless of the number of transform copies asking for it.
        //
        synchronized (getPipeline()) {
          logBasic("init counter name: {0}", data.getLookup());
          data.counter =
              Counters.getInstance()
                  .getOrUpdateCounter(
                      data.getLookup(), new Counter(data.start, data.increment, data.maximum));
        }
        return true;
      } else {
        logError(
            BaseMessages.getString(PKG, "AddSequence.Log.PipelineCountersHashtableNotAllocated"));
      }
    } else {
      logError(BaseMessages.getString(PKG, "AddSequence.Log.NeedToSelectSequence"));
    }

    return false;
  }

  @Override
  public void dispose() {

    if (meta.isCounterUsed()) {
      if (data.getLookup() != null) {
        Counters.getInstance().removeCounter(data.getLookup());
      }
      data.counter = null;
    }

    if (meta.isDatabaseUsed() && data.getDb() != null) {
      data.getDb().disconnect();
    }

    super.dispose();
  }

  @Override
  public void cleanup() {
    super.cleanup();
  }

  /**
   * Build a unique identifier, eg: seq_sequence_value_1_1_999999999_tId
   *
   * @param counterName counter name
   * @return unique key
   */
  private String lookupCounterName(String counterName) {
    return String.format(
        "seq_%s_%d_%d_%d_%d",
        counterName, data.start, data.increment, data.maximum, Thread.currentThread().getId());
  }
}
