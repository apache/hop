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

package org.apache.hop.pipeline.transforms.mongodbinput;

import com.mongodb.Cursor;
import com.mongodb.DBObject;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.wrapper.MongoWrapperUtil;
import org.apache.hop.mongo.wrapper.field.MongodbInputDiscoverFieldsImpl;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;

public class MongoDbInput extends BaseTransform<MongoDbInputMeta, MongoDbInputData>
    implements ITransform<MongoDbInputMeta, MongoDbInputData> {
  private static Class<?> PKG = MongoDbInputMeta.class; // For i18n - Translator

  private boolean m_serverDetermined;
  private Object[] m_currentInputRowDrivingQuery = null;

  public MongoDbInput(
      TransformMeta transformMeta,
      MongoDbInputMeta meta,
      MongoDbInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    try {
      if (meta.getExecuteForEachIncomingRow() && m_currentInputRowDrivingQuery == null) {
        m_currentInputRowDrivingQuery = getRow();

        if (m_currentInputRowDrivingQuery == null) {
          // no more input, no more queries to make
          setOutputDone();
          return false;
        }

        if (!first) {
          initQuery();
        }
      }

      if (first) {
        data.outputRowMeta = new RowMeta();
        meta.getFields(
            data.outputRowMeta,
            getTransformName(),
            null,
            null,
            MongoDbInput.this,
            metadataProvider);

        initQuery();
        first = false;

        data.init();
      }

      boolean hasNext =
          ((meta.isQueryIsPipeline() ? data.m_pipelineResult.hasNext() : data.cursor.hasNext())
              && !isStopped());
      if (hasNext) {
        DBObject nextDoc = null;
        Object[] row = null;
        if (meta.isQueryIsPipeline()) {
          nextDoc = data.m_pipelineResult.next();
        } else {
          nextDoc = data.cursor.next();
        }

        if (!meta.isQueryIsPipeline() && !m_serverDetermined) {
          ServerAddress s = data.cursor.getServerAddress();
          if (s != null) {
            m_serverDetermined = true;
            logBasic(
                BaseMessages.getString(
                    PKG, "MongoDbInput.Message.QueryPulledDataFrom", s.toString()));
          }
        }

        if (meta.isOutputJson()
            || meta.getMongoFields() == null
            || meta.getMongoFields().size() == 0) {
          String json = JSON.serialize(nextDoc);
          row = RowDataUtil.allocateRowData(data.outputRowMeta.size());
          int index = 0;

          row[index++] = json;
          putRow(data.outputRowMeta, row);
        } else {
          Object[][] outputRows = data.mongoDocumentToHop(nextDoc, MongoDbInput.this);

          // there may be more than one row if the paths contain an array
          // unwind
          for (int i = 0; i < outputRows.length; i++) {
            putRow(data.outputRowMeta, outputRows[i]);
          }
        }
      } else {
        if (!meta.getExecuteForEachIncomingRow()) {
          setOutputDone();

          return false;
        } else {
          m_currentInputRowDrivingQuery = null; // finished with this row
        }
      }

      return true;
    } catch (Exception e) {
      if (e instanceof HopException) {
        throw (HopException) e;
      } else {
        throw new HopException(e);
      }
    }
  }

  protected void initQuery() throws HopException, MongoDbException {

    // close any previous cursor
    if (data.cursor != null) {
      data.cursor.close();
    }

    // check logging level and only set to false if
    // logging level at least detailed
    if (log.isDetailed()) {
      m_serverDetermined = false;
    }

    String query = resolve(meta.getJsonQuery());
    String fields = resolve(meta.getFieldsName());
    if (StringUtils.isEmpty(query) && StringUtils.isEmpty(fields)) {
      if (meta.isQueryIsPipeline()) {
        throw new HopException(
            BaseMessages.getString(
                MongoDbInputMeta.PKG, "MongoDbInput.ErrorMessage.EmptyAggregationPipeline"));
      }

      data.cursor = data.collection.find();
    } else {

      if (meta.isQueryIsPipeline()) {
        if (StringUtils.isEmpty(query)) {
          throw new HopException(
              BaseMessages.getString(
                  MongoDbInputMeta.PKG, "MongoDbInput.ErrorMessage.EmptyAggregationPipeline"));
        }

        if (meta.getExecuteForEachIncomingRow() && m_currentInputRowDrivingQuery != null) {
          // do field value substitution
          query = resolve(query, getInputRowMeta(), m_currentInputRowDrivingQuery);
        }

        logDetailed(BaseMessages.getString(PKG, "MongoDbInput.Message.QueryPulledDataFrom", query));

        List<DBObject> pipeline = MongodbInputDiscoverFieldsImpl.jsonPipelineToDBObjectList(query);
        DBObject firstP = pipeline.get(0);
        DBObject[] remainder = null;
        if (pipeline.size() > 1) {
          remainder = new DBObject[pipeline.size() - 1];
          for (int i = 1; i < pipeline.size(); i++) {
            remainder[i - 1] = pipeline.get(i);
          }
        } else {
          remainder = new DBObject[0];
        }

        // Utilize MongoDB cursor class
        Cursor cursor = data.collection.aggregate(firstP, remainder);
        data.m_pipelineResult = cursor;
      } else {
        if (meta.getExecuteForEachIncomingRow() && m_currentInputRowDrivingQuery != null) {
          // do field value substitution
          query = resolve(query, getInputRowMeta(), m_currentInputRowDrivingQuery);

          fields = resolve(fields, getInputRowMeta(), m_currentInputRowDrivingQuery);
        }

        logDetailed(BaseMessages.getString(PKG, "MongoDbInput.Message.ExecutingQuery", query));

        DBObject dbObject = (DBObject) JSON.parse(StringUtils.isEmpty(query) ? "{}" : query);
        DBObject dbObject2 = (DBObject) JSON.parse(fields);
        data.cursor = data.collection.find(dbObject, dbObject2);
      }
    }
  }

  @Override
  public boolean init() {
    if (super.init()) {
      String hostname = resolve(meta.getHostnames());
      int port =
          Const.toInt( resolve(meta.getPort()), MongoDbInputData.MONGO_DEFAULT_PORT);
      String db = resolve(meta.getDbName());
      String collection = resolve(meta.getCollection());

      try {
        if (StringUtils.isEmpty(db)) {
          throw new Exception(BaseMessages.getString(PKG, "MongoInput.ErrorMessage.NoDBSpecified"));
        }

        if (StringUtils.isEmpty(collection)) {
          throw new Exception(
              BaseMessages.getString(PKG, "MongoInput.ErrorMessage.NoCollectionSpecified"));
        }

        if (!StringUtils.isEmpty(meta.getAuthenticationUser())) {
          String authInfo =
              (meta.getUseKerberosAuthentication()
                  ? BaseMessages.getString(
                      PKG,
                      "MongoDbInput.Message.KerberosAuthentication",
                      resolve(meta.getAuthenticationUser()))
                  : BaseMessages.getString(
                      PKG,
                      "MongoDbInput.Message.NormalAuthentication",
                      resolve(meta.getAuthenticationUser())));

          logBasic(authInfo);
        }

        // init connection constructs a MongoCredentials object if necessary
        data.clientWrapper = MongoWrapperUtil.createMongoClientWrapper(meta, this, log);
        data.collection = data.clientWrapper.getCollection(db, collection);

        if (!meta.isOutputJson()) {
          data.setMongoFields(meta.getMongoFields());
        }

        return true;
      } catch (Exception e) {
        logError(
            BaseMessages.getString(
                PKG,
                "MongoDbInput.ErrorConnectingToMongoDb.Exception",
                hostname,
                "" + port,
                db,
                collection),
            e);
        return false;
      }
    } else {
      return false;
    }
  }

  @Override
  public void dispose() {
    if (data.cursor != null) {
      try {
        data.cursor.close();
      } catch (MongoDbException e) {
        log.logError(e.getMessage());
      }
    }
    if (data.clientWrapper != null) {
      try {
        data.clientWrapper.dispose();
      } catch (MongoDbException e) {
        log.logError(e.getMessage());
      }
    }

    super.dispose();
  }
}
