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

package org.apache.hop.pipeline.transforms.mongodboutput;

import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.WriteResult;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.wrapper.MongoWrapperUtil;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class providing an output transform for writing data to a MongoDB collection. Supports insert,
 * truncate, upsert, multi-update (update all matching docs) and modifier update (update only
 * certain fields) operations. Can also create and drop indexes based on one or more fields.
 */
public class MongoDbOutput extends BaseTransform<MongoDbOutputMeta, MongoDbOutputData>
    implements ITransform<MongoDbOutputMeta, MongoDbOutputData> {
  private static Class<?> PKG = MongoDbOutputMeta.class; // For Translator

  protected MongoDbOutputData.MongoTopLevel m_mongoTopLevelStructure =
      MongoDbOutputData.MongoTopLevel.INCONSISTENT;

  /** The batch size to use for insert operation */
  protected int m_batchInsertSize = 100;

  /** Holds a batch of rows converted to documents */
  protected List<DBObject> m_batch;

  /** Holds an original batch of rows (corresponding to the converted documents) */
  protected List<Object[]> m_batchRows;

  protected int m_writeRetries = MongoDbOutputMeta.RETRIES;
  protected int m_writeRetryDelay = MongoDbOutputMeta.RETRY_DELAY;

  public MongoDbOutput(
      TransformMeta transformMeta,
      MongoDbOutputMeta meta,
      MongoDbOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] row = getRow();

    if (row == null) {
      // no more output

      // check any remaining buffered objects
      if (m_batch != null && m_batch.size() > 0) {
        try {
          doBatch();
        } catch (MongoDbException e) {
          throw new HopException(e);
        }
      }

      // INDEXING - http://www.mongodb.org/display/DOCS/Indexes
      // Indexing is computationally expensive - it needs to be
      // done after all data is inserted and done in the BACKGROUND.

      // UNIQUE indexes (prevent duplicates on the
      // keys in the index) and SPARSE indexes (don't index docs that
      // don't have the key field) - current limitation is that SPARSE
      // indexes can only have a single field

      List<MongoDbOutputMeta.MongoIndex> indexes = meta.getMongoIndexes();
      if (indexes != null && indexes.size() > 0) {
        logBasic(BaseMessages.getString(PKG, "MongoDbOutput.Messages.ApplyingIndexOpps"));
        try {
          data.applyIndexes(indexes, log, meta.getTruncate());
        } catch (MongoDbException e) {
          throw new HopException(e);
        }
      }

      disconnect();
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      m_batchInsertSize = 100;

      String batchInsert = resolve(meta.getBatchInsertSize());
      if (!StringUtils.isEmpty(batchInsert)) {
        m_batchInsertSize = Integer.parseInt(batchInsert);
      }
      m_batch = new ArrayList<>( m_batchInsertSize );
      m_batchRows = new ArrayList<>();

      // output the same as the input
      data.setOutputRowMeta(getInputRowMeta());

      // scan for top-level JSON document insert and validate
      // field specification in this case.
      data.m_hasTopLevelJSONDocInsert =
          MongoDbOutputData.scanForInsertTopLevelJSONDoc(meta.getMongoFields());

      // first check our incoming fields against our meta data for
      // fields to
      // insert
      // this fields is came to transform input
      IRowMeta rmi = getInputRowMeta();
      // this fields we are going to use for mongo output
      List<MongoDbOutputMeta.MongoField> mongoFields = meta.getMongoFields();
      checkInputFieldsMatch(rmi, mongoFields);

      // copy and initialize mongo fields
      data.setMongoFields(meta.getMongoFields());
      data.init(MongoDbOutput.this);

      // check truncate
      if (meta.getTruncate()) {
        try {
          logBasic(BaseMessages.getString(PKG, "MongoDbOutput.Messages.TruncatingCollection"));
          data.getCollection().remove();
        } catch (Exception m) {
          disconnect();
          throw new HopException(m.getMessage(), m);
        }
      }
    }

    if (!isStopped()) {

      if (meta.getUpdate()) {
        DBObject updateQuery =
            MongoDbOutputData.getQueryObject(
                data.getMongoFields(),
                getInputRowMeta(),
                row,
                MongoDbOutput.this,
                m_mongoTopLevelStructure);

        if (log.isDebug()) {
          logDebug(
              BaseMessages.getString(
                  PKG, "MongoDbOutput.Messages.Debug.QueryForUpsert", updateQuery));
        }

        if (updateQuery != null) {
          // i.e. we have some non-null incoming query field values
          DBObject insertUpdate = null;

          // get the record to update the match with
          if (!meta.getModifierUpdate()) {
            // complete record replace or insert

            insertUpdate =
                MongoDbOutputData.hopRowToMongo(
                    data.getMongoFields(),
                    getInputRowMeta(),
                    row,
                    m_mongoTopLevelStructure,
                    data.m_hasTopLevelJSONDocInsert);
            if (log.isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "MongoDbOutput.Messages.Debug.InsertUpsertObject", insertUpdate));
            }

          } else {

            // specific field update (or insert)
            try {
              insertUpdate =
                  data.getModifierUpdateObject(
                      data.getMongoFields(),
                      getInputRowMeta(),
                      row,
                      MongoDbOutput.this,
                      m_mongoTopLevelStructure);
            } catch (MongoDbException e) {
              throw new HopException(e);
            }
            if (log.isDebug()) {
              logDebug(
                  BaseMessages.getString(
                      PKG, "MongoDbOutput.Messages.Debug.ModifierUpdateObject", insertUpdate));
            }
          }

          if (insertUpdate != null) {
            commitUpdate(updateQuery, insertUpdate, row);
          }
        }
      } else {
        // straight insert

        DBObject mongoInsert =
            MongoDbOutputData.hopRowToMongo(
                data.getMongoFields(),
                getInputRowMeta(),
                row,
                m_mongoTopLevelStructure,
                data.m_hasTopLevelJSONDocInsert);

        if (mongoInsert != null) {
          m_batch.add(mongoInsert);
          m_batchRows.add(row);
        }
        if (m_batch.size() == m_batchInsertSize) {
          logDetailed(BaseMessages.getString(PKG, "MongoDbOutput.Messages.CommitingABatch"));
          try {
            doBatch();
          } catch (MongoDbException e) {
            throw new HopException(e);
          }
        }
      }
      putRow(data.getOutputRowMeta(), row);
      incrementLinesOutput();
    }

    return true;
  }

  protected void commitUpdate(DBObject updateQuery, DBObject insertUpdate, Object[] row)
      throws HopException {

    int retrys = 0;
    MongoException lastEx = null;

    while (retrys <= m_writeRetries && !isStopped()) {
      WriteResult result = null;
      try {
        // TODO It seems that doing an update() via a secondary node does not
        // generate any sort of exception or error result! (at least via
        // driver version 2.11.1). Pipeline completes successfully
        // but no updates are made to the collection.
        // This is unlike doing an insert(), which generates
        // a MongoException if you are not talking to the primary. So we need
        // some logic to check whether or not the connection configuration
        // contains the primary in the replica set and give feedback if it
        // doesnt
        try {
          result =
              data.getCollection()
                  .update(updateQuery, insertUpdate, meta.getUpsert(), meta.getMulti());
        } catch (MongoDbException e) {
          throw new MongoException(e.getMessage(), e);
        }
      } catch (MongoException me) {
        lastEx = me;
        retrys++;
        if (retrys <= m_writeRetries) {
          logError(
              BaseMessages.getString(
                  PKG, "MongoDbOutput.Messages.Error.ErrorWritingToMongo", me.toString()));
          logBasic(
              BaseMessages.getString(
                  PKG, "MongoDbOutput.Messages.Message.Retry", m_writeRetryDelay));
          try {
            Thread.sleep(m_writeRetryDelay * 1000);
            // CHECKSTYLE:OFF
          } catch (InterruptedException e) {
            // CHECKSTYLE:ON
          }
        }
      }

      if (result != null) {
        break;
      }
    }

    if ((retrys > m_writeRetries || isStopped()) && lastEx != null) {

      // Send this one to the error stream if doing error handling
      if (getTransformMeta().isDoingErrorHandling()) {
        putError(getInputRowMeta(), row, 1, lastEx.getMessage(), "", "MongoDbOutput");
      } else {
        throw new HopException(lastEx);
      }
    }
  }

  protected WriteResult batchRetryUsingSave(boolean lastRetry)
      throws MongoException, HopException, MongoDbException {
    WriteResult result = null;
    int count = 0;
    logBasic(
        BaseMessages.getString(PKG, "MongoDbOutput.Messages.CurrentBatchSize", m_batch.size()));
    for (int i = 0, len = m_batch.size(); i < len; i++) {
      DBObject toTry = m_batch.get(i);
      Object[] correspondingRow = m_batchRows.get(i);
      try {
        result = data.getCollection().save(toTry);
        count++;
      } catch (MongoException ex) {
        if (!lastRetry) {
          logBasic(
              BaseMessages.getString(
                  PKG, "MongoDbOutput.Messages.SuccessfullySavedXDocuments", count));
          m_batch = copyExceptFirst(count, m_batch);
          m_batchRows = copyExceptFirst(count, m_batchRows);
          throw ex;
        }

        // Send this one to the error stream if doing error handling
        if (getTransformMeta().isDoingErrorHandling()) {
          putError(getInputRowMeta(), correspondingRow, 1, ex.getMessage(), "", "MongoDbOutput");
        } else {
          m_batch = copyExceptFirst(i + 1, m_batch);
          m_batchRows = copyExceptFirst(i + 1, m_batchRows);
          throw ex;
        }
      }
    }

    m_batch.clear();
    m_batchRows.clear();

    logBasic(
        BaseMessages.getString(PKG, "MongoDbOutput.Messages.SuccessfullySavedXDocuments", count));

    return result;
  }

  private static <T> List<T> copyExceptFirst(int amount, List<T> list) {
    return new ArrayList<>( list.subList( amount, list.size() ) );
  }

  protected void doBatch() throws HopException, MongoDbException {
    int retries = 0;
    MongoException lastEx = null;

    while (retries <= m_writeRetries && !isStopped()) {
      WriteResult result = null;
      try {
        if (retries == 0) {
          result = data.getCollection().insert(m_batch);
        } else {
          // fall back to save
          logBasic(
              BaseMessages.getString(
                  PKG, "MongoDbOutput.Messages.SavingIndividualDocsInCurrentBatch"));
          result = batchRetryUsingSave(retries == m_writeRetries);
        }
      } catch (MongoException me) {
        // avoid exception if a timeout issue occurred and it was exactly the first attempt
        boolean shouldNotBeAvoided = !isTimeoutException(me) && (retries == 0);
        if (shouldNotBeAvoided) {
          lastEx = me;
        }
        retries++;
        if (retries <= m_writeRetries) {
          if (shouldNotBeAvoided) {
            // skip logging error
            // however do not skip saving elements separately during next attempt to prevent losing
            // data
            logError(
                BaseMessages.getString(
                    PKG, "MongoDbOutput.Messages.Error.ErrorWritingToMongo", me.toString()));
            logBasic(
                BaseMessages.getString(
                    PKG, "MongoDbOutput.Messages.Message.Retry", m_writeRetryDelay));
          }
          try {
            Thread.sleep(m_writeRetryDelay * 1000);
            // CHECKSTYLE:OFF
          } catch (InterruptedException e) {
            // CHECKSTYLE:ON
          }
        }
        // throw new HopException(me.getMessage(), me);
      }

      if (result != null) {
        break;
      }
    }

    if ((retries > m_writeRetries || isStopped()) && lastEx != null) {
      throw new HopException(lastEx);
    }

    m_batch.clear();
    m_batchRows.clear();
  }

  private static boolean isTimeoutException(MongoException me) {
    return (me instanceof MongoExecutionTimeoutException);
  }

  @Override
  public boolean init() {
    if (super.init()) {

      if (!StringUtils.isEmpty(meta.getWriteRetries())) {
        m_writeRetries = Const.toInt(meta.getWriteRetries(), MongoDbOutputMeta.RETRIES);
      }

      if (!StringUtils.isEmpty(meta.getWriteRetryDelay())) {
        m_writeRetryDelay = Const.toInt(meta.getWriteRetryDelay(), MongoDbOutputMeta.RETRY_DELAY);
      }

      String hostname = resolve(meta.getHostnames());
      int port = Const.toInt( resolve(meta.getPort()), 27017);
      String db = resolve(meta.getDbName());
      String collection = resolve(meta.getCollection());

      try {
        if (StringUtils.isEmpty(db)) {
          throw new Exception(
              BaseMessages.getString(PKG, "MongoDbOutput.Messages.Error.NoDBSpecified"));
        }

        if (StringUtils.isEmpty(collection)) {
          throw new Exception(
              BaseMessages.getString(PKG, "MongoDbOutput.Messages.Error.NoCollectionSpecified"));
        }

        if (!StringUtils.isEmpty(meta.getAuthenticationUser())) {
          String authInfo =
              (meta.getUseKerberosAuthentication()
                  ? BaseMessages.getString(
                      PKG,
                      "MongoDbOutput.Message.KerberosAuthentication",
                      resolve(meta.getAuthenticationUser()))
                  : BaseMessages.getString(
                      PKG,
                      "MongoDbOutput.Message.NormalAuthentication",
                      resolve(meta.getAuthenticationUser())));

          logBasic(authInfo);
        }
        data.setConnection(
            MongoWrapperUtil.createMongoClientWrapper(
                meta, this, log)); // MongoDbOutputData.connect( meta, this, log ) );

        if (StringUtils.isEmpty(collection)) {
          throw new HopException(
              BaseMessages.getString(PKG, "MongoDbOutput.Messages.Error.NoCollectionSpecified"));
        }
        data.createCollection(db, collection);
        data.setCollection(data.getConnection().getCollection(db, collection));

        try {
          m_mongoTopLevelStructure =
              MongoDbOutputData.checkTopLevelConsistency(meta.getMongoFields(), MongoDbOutput.this);

          if (m_mongoTopLevelStructure == MongoDbOutputData.MongoTopLevel.INCONSISTENT) {
            logError(
                BaseMessages.getString(
                    PKG, "MongoDbOutput.Messages.Error.InconsistentMongoTopLevel"));
            return false;
          }
        } catch (HopException e) {
          logError(e.getMessage());
          return false;
        }

        return true;
      } catch (UnknownHostException ex) {
        logError(
            BaseMessages.getString(PKG, "MongoDbOutput.Messages.Error.UnknownHost", hostname), ex);
        return false;
      } catch (Exception e) {
        logError(
            BaseMessages.getString(
                PKG, "MongoDbOutput.Messages.Error.ProblemConnecting", hostname, "" + port),
            e);
        return false;
      }
    }

    return false;
  }

  protected void disconnect() {
    if (data != null) {
      try {
        data.getConnection().dispose();
      } catch (MongoDbException e) {
        log.logError(e.getMessage());
      }
    }
  }

  @Override
  public void dispose() {
    disconnect();
    super.dispose();
  }

  final void checkInputFieldsMatch(IRowMeta rmi, List<MongoDbOutputMeta.MongoField> mongoFields)
      throws HopException {
    Set<String> expected = new HashSet<>( mongoFields.size(), 1 );
    Set<String> actual = new HashSet<>( rmi.getFieldNames().length, 1 );
    for (MongoDbOutputMeta.MongoField field : mongoFields) {
      String mongoMatch = resolve(field.m_incomingFieldName);
      expected.add(mongoMatch);
    }
    for (int i = 0; i < rmi.size(); i++) {
      String metaFieldName = rmi.getValueMeta(i).getName();
      actual.add(metaFieldName);
    }

    // check that all expected fields is available in transform input meta
    if (!actual.containsAll(expected)) {
      // in this case some fields willn't be found in input transform meta
      expected.removeAll(actual);
      StringBuffer b = new StringBuffer();
      for (String name : expected) {
        b.append("'").append(name).append("', ");
      }
      throw new HopException(
          BaseMessages.getString(
              PKG,
              "MongoDbOutput.Messages.MongoField.Error.FieldsNotFoundInMetadata",
              b.toString()));
    }

    boolean found = actual.removeAll(expected);
    if (!found) {
      throw new HopException(
          BaseMessages.getString(PKG, "MongoDbOutput.Messages.Error.NotInsertingAnyFields"));
    }

    if (!actual.isEmpty()) {
      // we have some fields that will not be inserted.
      StringBuffer b = new StringBuffer();
      for (String name : actual) {
        b.append("'").append(name).append("', ");
      }
      // just put a log record on it
      logBasic(
          BaseMessages.getString(
              PKG, "MongoDbOutput.Messages.FieldsNotToBeInserted", b.toString()));
    }
  }
}
