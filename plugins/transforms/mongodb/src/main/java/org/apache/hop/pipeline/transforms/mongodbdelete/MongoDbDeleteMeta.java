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

package org.apache.hop.pipeline.transforms.mongodbdelete;

import java.util.List;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transforms.mongodb.MongoDbMeta;

/** Meta data class for MongoDbDelete transform. */
@Transform(
    id = "MongoDbDelete",
    image = "mongodb-delete.svg",
    name = "i18n::MongoDbDelete.Name",
    description = "i18n::MongoDbDelete.Description",
    documentationUrl = "/pipeline/transforms/mongodbdelete.html",
    keywords = "i18n::MongoDbDelete.keyword",
    categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Output")
public class MongoDbDeleteMeta extends MongoDbMeta<MongoDbDelete, MongoDbDeleteData> {

  @HopMetadataProperty(key = "connection", injectionKey = "CONNECTION")
  private String connectionName;

  @HopMetadataProperty(key = "retries", injectionKey = "RETRIES")
  public int nbRetries = 5;

  public int retryDelay = 10; // seconds

  @HopMetadataProperty(key = "write_retries", injectionKey = "WRITE_RETRIES")
  private String writeRetries = "" + nbRetries;

  @HopMetadataProperty(key = "retry_delay", injectionKey = "RETRY_DELAY")
  private String writeRetryDelay = "" + retryDelay;

  /** The list of paths to document fields for incoming Hop values */
  @HopMetadataProperty(key = "use_json_query", injectionKey = "USE_JSON_QUERY")
  private boolean useJsonQuery = false;

  @HopMetadataProperty(key = "json_query", injectionKey = "JSON_QUERY")
  private String jsonQuery = "";

  @HopMetadataProperty(key = "execute_for_each_row", injectionKey = "EXECUTE_FOR_EACH_ROW")
  private boolean executeForEachIncomingRow = false; // only apply when use json query

  @HopMetadataProperty(
      groupKey = "fields",
      key = "field",
      injectionKey = "MONGODB_FIELDS",
      injectionGroupKey = "MONGODB_FIELDS")
  protected List<MongoDbDeleteField> mongoFields;

  @HopMetadataProperty(key = "collection", injectionKey = "COLLECTION")
  private String collection;

  @Override
  public void setDefault() {
    // Do nothing
  }

  @Override
  public void setConnectionName(String connectionName) {
    this.connectionName = connectionName;
  }

  @Override
  public String getConnectionName() {
    return connectionName;
  }

  public void setMongoFields(List<MongoDbDeleteField> mongoFields) {
    this.mongoFields = mongoFields;
  }

  public List<MongoDbDeleteField> getMongoFields() {
    return mongoFields;
  }

  public void setUseJsonQuery(boolean useJsonQuery) {
    this.useJsonQuery = useJsonQuery;
  }

  public boolean isUseJsonQuery() {
    return useJsonQuery;
  }

  public boolean isExecuteForEachIncomingRow() {
    return executeForEachIncomingRow;
  }

  public void setExecuteForEachIncomingRow(boolean executeForEachIncomingRow) {
    this.executeForEachIncomingRow = executeForEachIncomingRow;
  }

  public void setJsonQuery(String jsonQuery) {
    this.jsonQuery = jsonQuery;
  }

  public String getJsonQuery() {
    return jsonQuery;
  }

  /**
   * Sets write retries.
   *
   * @param r the number of retry attempts to make
   */
  public void setWriteRetries(String r) {
    writeRetries = r;
  }

  /**
   * Get the number of retry attempts to make if a particular write operation fails
   *
   * @return the number of retry attempts to make
   */
  public String getWriteRetries() {
    return writeRetries;
  }

  /**
   * Set the delay (in seconds) between write retry attempts
   *
   * @param d the delay in seconds between retry attempts
   */
  public void setWriteRetryDelay(String d) {
    writeRetryDelay = d;
  }

  /**
   * Get the delay (in seconds) between write retry attempts
   *
   * @return the delay in seconds between retry attempts
   */
  public String getWriteRetryDelay() {
    return writeRetryDelay;
  }

  /**
   * @return the collection
   */
  @Override
  public String getCollection() {
    return collection;
  }

  /**
   * @param collection the collection to set
   */
  @Override
  public void setCollection(String collection) {
    this.collection = collection;
  }

  public int getNbRetries() {
    return nbRetries;
  }

  public void setNbRetries(int nbRetries) {
    this.nbRetries = nbRetries;
  }
}
