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

package org.apache.hop.mongo.wrapper;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import java.util.List;

public interface MongoClientFactory {

  MongoClient getMongoClient(
      List<ServerAddress> serverAddressList,
      List<MongoCredential> credList,
      MongoClientSettings.Builder settingsBuilder,
      boolean useReplicaSet);

  /**
   * Create a MongoClient from a connection string (e.g., mongodb+srv://cluster.mongodb.net/dbname).
   * This supports both standard mongodb:// and SRV-based mongodb+srv:// connection strings.
   * Credentials should be supplied separately via MongoCredential (best practice).
   *
   * @param connectionString the MongoDB connection string (should be clean, without credentials)
   * @param settingsBuilder additional settings to apply (may be null)
   * @param credList credentials to use (may be null or empty)
   * @return a configured MongoClient
   */
  MongoClient getMongoClient(
      String connectionString,
      MongoClientSettings.Builder settingsBuilder,
      List<MongoCredential> credList);
}
