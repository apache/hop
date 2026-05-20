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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.util.Collections;
import java.util.List;

public class DefaultMongoClientFactory implements MongoClientFactory {

  @Override
  public MongoClient getMongoClient(
      List<ServerAddress> serverAddressList,
      List<MongoCredential> credList,
      MongoClientSettings.Builder settingsBuilder,
      boolean useReplicaSet) {

    List<ServerAddress> hosts =
        (useReplicaSet || serverAddressList.size() > 1)
            ? serverAddressList
            : Collections.singletonList(serverAddressList.get(0));

    settingsBuilder.applyToClusterSettings(
        builder -> builder.hosts(hosts).mode(getClusterMode(hosts, useReplicaSet)));

    // Apply credentials if provided
    if (credList != null && !credList.isEmpty()) {
      // MongoDB 5.x only supports a single credential
      settingsBuilder.credential(credList.get(0));
    }

    return MongoClients.create(settingsBuilder.build());
  }

  private com.mongodb.connection.ClusterConnectionMode getClusterMode(
      List<ServerAddress> hosts, boolean useReplicaSet) {
    if (useReplicaSet || hosts.size() > 1) {
      return com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
    }
    return com.mongodb.connection.ClusterConnectionMode.SINGLE;
  }

  @Override
  public MongoClient getMongoClient(
      String connectionString,
      MongoClientSettings.Builder settingsBuilder,
      List<MongoCredential> credList) {
    ConnectionString connString = new ConnectionString(connectionString);

    if (settingsBuilder != null) {
      // Apply credentials FIRST before applying connection string
      // This ensures credentials are not overridden by applyConnectionString
      if (credList != null && !credList.isEmpty()) {
        // MongoDB 5.x only supports a single credential
        settingsBuilder.credential(credList.get(0));
      }

      // Apply the connection string settings (this won't override credentials if they're already
      // set)
      settingsBuilder.applyConnectionString(connString);

      // Re-apply credentials AFTER applyConnectionString to ensure they take precedence
      // (applyConnectionString might clear credentials if connection string has different ones)
      if (credList != null && !credList.isEmpty()) {
        settingsBuilder.credential(credList.get(0));
      }

      return MongoClients.create(settingsBuilder.build());
    } else {
      // If no settings builder, create from connection string
      // But still apply credentials if provided
      if (credList != null && !credList.isEmpty()) {
        MongoClientSettings.Builder builder = MongoClientSettings.builder();
        builder.applyConnectionString(connString);
        builder.credential(credList.get(0));
        return MongoClients.create(builder.build());
      } else {
        // Just use the connection string directly
        return MongoClients.create(connString);
      }
    }
  }
}
