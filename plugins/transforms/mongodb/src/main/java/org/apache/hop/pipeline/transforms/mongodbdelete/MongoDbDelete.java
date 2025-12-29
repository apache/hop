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

package org.apache.hop.pipeline.transforms.mongodbdelete;

import com.mongodb.MongoException;
import com.mongodb.client.result.DeleteResult;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.metadata.MongoDbConnection;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.bson.Document;

/**
 * Class MongoDbDelete, providing MongoDB delete functionality. User able to create criteria base on
 * incoming fields.
 */
public class MongoDbDelete extends BaseTransform<MongoDbDeleteMeta, MongoDbDeleteData> {

  private static final Class<?> PKG = MongoDbDelete.class;

  private int writeRetries;

  protected long writeRetryDelay;

  public MongoDbDelete(
      TransformMeta transformMeta,
      MongoDbDeleteMeta meta,
      MongoDbDeleteData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();

    if (meta.isUseJsonQuery()) {
      if (first) {
        first = false;

        if (getInputRowMeta() == null) {
          data.outputRowMeta = new RowMeta();
        } else {
          data.setOutputRowMeta(getInputRowMeta());
        }
        data.init(MongoDbDelete.this);

        Document query = getQueryFromJSON(meta.getJsonQuery(), row);
        commitDelete(query, row);
      } else if (meta.isExecuteForEachIncomingRow()) {

        if (row == null) {
          disconnect();
          setOutputDone();
          return false;
        }

        Document query = getQueryFromJSON(meta.getJsonQuery(), row);
        commitDelete(query, row);
      }

      if (row == null) {
        disconnect();
        setOutputDone();
        return false;
      }

      if (!isStopped()) {
        putRow(data.getOutputRowMeta(), row);
      }

      return true;
    } else {

      if (row == null) {
        disconnect();
        setOutputDone();
        return false;
      }

      if (first) {
        first = false;

        data.setOutputRowMeta(getInputRowMeta());
        // first check our incoming fields against our meta data for
        // fields to delete
        IRowMeta rmi = getInputRowMeta();
        // this fields we are going to use for mongo output
        List<MongoDbDeleteField> mongoFields = meta.getMongoFields();
        checkInputFieldsMatch(rmi, mongoFields);

        data.setMongoFields(meta.getMongoFields());
        data.init(MongoDbDelete.this);
      }

      if (!isStopped()) {

        putRow(data.getOutputRowMeta(), row);

        Document query =
            MongoDbDeleteData.getQueryObject(
                data.mUserFields, getInputRowMeta(), row, MongoDbDelete.this);
        if (isDebug()) {
          logDebug(
              BaseMessages.getString(PKG, "MongoDbDelete.Message.Debug.QueryForDelete", query));
        }
        // We have query delete
        if (query != null) {
          commitDelete(query, row);
        }
      }

      return true;
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

        if (!StringUtil.isEmpty(meta.getWriteRetries())) {
          try {
            setWriteRetries(Integer.parseInt(meta.getWriteRetries()));
          } catch (NumberFormatException ex) {
            setWriteRetries(meta.nbRetries);
          }
        }

        if (!StringUtil.isEmpty(meta.getWriteRetryDelay())) {
          try {
            writeRetryDelay = Long.parseLong(meta.getWriteRetryDelay());
          } catch (NumberFormatException ex) {
            writeRetryDelay = meta.retryDelay;
          }
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
    }

    return false;
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

  protected void disconnect() {
    if (data != null) {
      try {
        data.getConnection().dispose();
      } catch (MongoDbException e) {
        logError(e.getMessage());
      }
    }
  }

  protected void commitDelete(Document deleteQuery, Object[] row) throws HopException {
    int retrys = 0;
    MongoException lastEx = null;

    while (retrys <= writeRetries && !isStopped()) {
      DeleteResult result = null;
      try {
        try {
          logDetailed(
              BaseMessages.getString(PKG, "MongoDbDelete.Message.ExecutingQuery", deleteQuery));
          result = data.getCollection().remove(deleteQuery);
        } catch (MongoDbException e) {
          throw new MongoException(e.getMessage(), e);
        }
      } catch (MongoException me) {
        lastEx = me;
        retrys++;
        if (retrys <= writeRetries) {
          logError(
              BaseMessages.getString(
                  PKG, "MongoDbDelete.ErrorMessage.ErrorWritingToMongo", me.toString()));
          logBasic(BaseMessages.getString(PKG, "MongoDbDelete.Message.Retry", writeRetryDelay));
          try {
            Thread.sleep(writeRetryDelay * 1000);
          } catch (InterruptedException e) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
          }
        }
      }
      if (result != null) {
        break;
      }
    }

    if ((retrys > writeRetries || isStopped()) && lastEx != null) {

      // Send this one to the error stream if doing error handling
      if (getTransformMeta().isDoingErrorHandling()) {
        putError(getInputRowMeta(), row, 1, lastEx.getMessage(), "", "MongoDbDelete");
      } else {
        throw new HopException(lastEx);
      }
    }
  }

  public Document getQueryFromJSON(String json, Object[] row) throws HopException {
    Document query;
    String jsonQuery = resolve(json);
    if (StringUtil.isEmpty(jsonQuery)) {
      query = new Document();
    } else {
      if (meta.isExecuteForEachIncomingRow() && row != null) {
        jsonQuery = resolve(jsonQuery, getInputRowMeta(), row);
      }

      query = Document.parse(jsonQuery);
    }
    return query;
  }

  final void checkInputFieldsMatch(IRowMeta rmi, List<MongoDbDeleteField> mongoFields)
      throws HopException {
    if (Utils.isEmpty(mongoFields)) {
      throw new HopException(
          BaseMessages.getString(PKG, "MongoDbDeleteDialog.ErrorMessage.NoFieldPathsDefined"));
    }

    Set<String> expected = new HashSet<>(mongoFields.size(), 1);
    Set<String> actual = new HashSet<>(rmi.getFieldNames().length, 1);
    for (MongoDbDeleteField field : mongoFields) {
      String field1 = resolve(field.incomingField1);
      String field2 = resolve(field.incomingField2);
      expected.add(field1);
      if (!StringUtil.isEmpty(field2)) {
        expected.add(field2);
      }
    }
    for (int i = 0; i < rmi.size(); i++) {
      String metaFieldName = rmi.getValueMeta(i).getName();
      actual.add(metaFieldName);
    }

    // check that all expected fields is available in step input meta
    if (!actual.containsAll(expected)) {
      // in this case some fields willn't be found in input step meta
      expected.removeAll(actual);
      StringBuilder builder = new StringBuilder();
      for (String name : expected) {
        builder.append("'").append(name).append("', ");
      }
      throw new HopException(
          BaseMessages.getString(
              PKG, "MongoDbDelete.MongoField.Error.FieldsNotFoundInMetadata", builder.toString()));
    }

    boolean found = actual.removeAll(expected);
    if (!found) {
      throw new HopException(
          BaseMessages.getString(PKG, "MongoDbDelete.ErrorMessage.NotDeleteAnyFields"));
    }
  }

  public void setWriteRetries(int writeRetries) {
    this.writeRetries = writeRetries;
  }
}
