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

import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.metadata.MongoDbConnection;
import org.apache.hop.mongo.wrapper.field.MongodbInputDiscoverFieldsImpl;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.bson.Document;

public class MongoDbInput extends BaseTransform<MongoDbInputMeta, MongoDbInputData> {
  private static final Class<?> PKG = MongoDbInputMeta.class; // For i18n - Translator

  private boolean serverDetermined;
  private Object[] currentInputRowDrivingQuery = null;

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
      if (meta.isExecuteForEachIncomingRow() && currentInputRowDrivingQuery == null) {
        currentInputRowDrivingQuery = getRow();

        if (currentInputRowDrivingQuery == null) {
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
          ((meta.isAggPipeline() ? data.pipelineResult.hasNext() : data.cursor.hasNext())
              && !isStopped());
      if (hasNext) {
        Document nextDoc = null;
        Object[] row = null;
        if (meta.isAggPipeline()) {
          nextDoc = data.pipelineResult.next();
        } else {
          nextDoc = data.cursor.next();
        }

        if (!meta.isAggPipeline() && !serverDetermined) {
          ServerAddress s = data.cursor.getServerAddress();
          if (s != null) {
            serverDetermined = true;
            if (isBasic()) {
              logBasic(
                  BaseMessages.getString(
                      PKG, "MongoDbInput.Message.QueryPulledDataFrom", s.toString()));
            }
          }
        }

        if (meta.isOutputJson() || meta.getFields() == null || meta.getFields().isEmpty()) {
          String json = nextDoc.toJson();
          row = RowDataUtil.allocateRowData(data.outputRowMeta.size());
          int index = 0;

          row[index++] = json;
          putRow(data.outputRowMeta, row);
        } else {
          Object[][] outputRows = data.mongoDocumentToHop(nextDoc, MongoDbInput.this);

          // there may be more than one row if the paths contain an array
          // unwind
          for (Object[] outputRow : outputRows) {
            putRow(data.outputRowMeta, outputRow);
          }
        }
      } else {
        if (!meta.isExecuteForEachIncomingRow()) {
          setOutputDone();

          return false;
        } else {
          currentInputRowDrivingQuery = null; // finished with this row
        }
      }

      return true;
    } catch (Exception e) {
      if (e instanceof HopException hopException) {
        throw hopException;
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
    if (isDetailed()) {
      serverDetermined = false;
    }

    String query = resolve(meta.getJsonQuery());
    String fields = resolve(meta.getJsonField());
    if (StringUtils.isEmpty(query) && StringUtils.isEmpty(fields)) {
      if (meta.isAggPipeline()) {
        throw new HopException(
            BaseMessages.getString(
                MongoDbInputMeta.PKG, "MongoDbInput.ErrorMessage.EmptyAggregationPipeline"));
      }

      data.cursor = data.collection.find();
    } else {

      if (meta.isAggPipeline()) {
        if (StringUtils.isEmpty(query)) {
          throw new HopException(
              BaseMessages.getString(
                  MongoDbInputMeta.PKG, "MongoDbInput.ErrorMessage.EmptyAggregationPipeline"));
        }

        if (meta.isExecuteForEachIncomingRow() && currentInputRowDrivingQuery != null) {
          // do field value substitution
          query = resolve(query, getInputRowMeta(), currentInputRowDrivingQuery);
        }

        if (isDetailed()) {
          logDetailed(
              BaseMessages.getString(PKG, "MongoDbInput.Message.QueryPulledDataFrom", query));
        }

        List<Document> pipeline = MongodbInputDiscoverFieldsImpl.jsonPipelineToDocumentList(query);
        Document firstP = pipeline.get(0);
        Document[] remainder = null;
        if (pipeline.size() > 1) {
          remainder = new Document[pipeline.size() - 1];
          for (int i = 1; i < pipeline.size(); i++) {
            remainder[i - 1] = pipeline.get(i);
          }
        } else {
          remainder = new Document[0];
        }

        // Utilize MongoDB cursor class
        AggregateIterable<Document> aggregateIterable =
            data.collection.aggregate(firstP, remainder);
        data.pipelineResult = aggregateIterable.iterator();
      } else {
        if (meta.isExecuteForEachIncomingRow() && currentInputRowDrivingQuery != null) {
          // do field value substitution
          query = resolve(query, getInputRowMeta(), currentInputRowDrivingQuery);

          fields = resolve(fields, getInputRowMeta(), currentInputRowDrivingQuery);
        }

        if (isDetailed()) {
          logDetailed(BaseMessages.getString(PKG, "MongoDbInput.Message.ExecutingQuery", query));
        }

        Document queryDoc = Document.parse(StringUtils.isEmpty(query) ? "{}" : query);
        Document fieldsDoc = StringUtils.isEmpty(fields) ? null : Document.parse(fields);
        data.cursor = data.collection.find(queryDoc, fieldsDoc);
      }
    }
  }

  @Override
  public boolean init() {
    if (super.init()) {
      String connectionName = resolve(meta.getConnectionName());

      try {

        try {
          data.connection =
              metadataProvider.getSerializer(MongoDbConnection.class).load(connectionName);
        } catch (Exception e) {
          throw new Exception(
              BaseMessages.getString(
                  PKG, "MongoInput.ErrorMessage.ErrorLoadingMongoDbConnection", connectionName));
        }
        if (data.connection == null) {
          throw new Exception(
              BaseMessages.getString(
                  PKG, "MongoInput.ErrorMessage.MongoDbConnection.NotFound", connectionName));
        }

        String databaseName = resolve(data.connection.getDbName());
        if (StringUtils.isEmpty(databaseName)) {
          throw new Exception(BaseMessages.getString(PKG, "MongoInput.ErrorMessage.NoDBSpecified"));
        }

        String collection = resolve(meta.getCollection());
        if (StringUtils.isEmpty(collection)) {
          throw new Exception(
              BaseMessages.getString(PKG, "MongoInput.ErrorMessage.NoCollectionSpecified"));
        }

        if (!StringUtils.isEmpty(data.connection.getAuthenticationUser())) {
          String authInfo =
              BaseMessages.getString(
                  PKG,
                  "MongoDbInput.Message.NormalAuthentication",
                  resolve(data.connection.getAuthenticationUser()));
          logBasic(authInfo);
        }

        // init connection constructs a MongoCredentials object if necessary
        data.clientWrapper = data.connection.createWrapper(this, getLogChannel());
        data.collection = data.clientWrapper.getCollection(databaseName, collection);

        if (!meta.isOutputJson()) {
          data.setMongoFields(meta.getFields());
        }

        return true;
      } catch (Exception e) {
        String hostname = data.connection != null ? data.connection.getHostname() : "unknown";
        String port = data.connection != null ? data.connection.getPort() : "unknown";
        String dbName = data.connection != null ? data.connection.getDbName() : "unknown";
        logError(
            BaseMessages.getString(
                PKG,
                "MongoDbInput.ErrorConnectingToMongoDb.Exception",
                hostname,
                port,
                dbName,
                meta.getCollection()),
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
        logError(e.getMessage());
      }
    }
    if (data.clientWrapper != null) {
      try {
        data.clientWrapper.dispose();
      } catch (MongoDbException e) {
        logError(e.getMessage());
      }
    }

    super.dispose();
  }
}
