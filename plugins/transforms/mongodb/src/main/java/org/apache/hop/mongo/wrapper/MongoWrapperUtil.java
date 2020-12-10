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

import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.MongoProp;
import org.apache.hop.mongo.MongoProperties;
import org.apache.hop.mongo.MongoUtilLogger;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transforms.mongodb.MongoDbMeta;

import java.util.List;

/** Created by bryan on 8/7/14. */
public class MongoWrapperUtil {
  private static MongoWrapperClientFactory mongoWrapperClientFactory =
      new MongoWrapperClientFactory() {
        @Override
        public MongoClientWrapper createMongoClientWrapper(
            MongoProperties props, MongoUtilLogger log) throws MongoDbException {
          return MongoClientWrapperFactory.createMongoClientWrapper(props, log);
        }
      };

  public static void setMongoWrapperClientFactory(
      MongoWrapperClientFactory mongoWrapperClientFactory) {
    MongoWrapperUtil.mongoWrapperClientFactory = mongoWrapperClientFactory;
  }

  protected static MongoWrapperClientFactory
      getMongoWrapperClientFactory() {
    return mongoWrapperClientFactory;
  }

  public static MongoClientWrapper createMongoClientWrapper(
      MongoDbMeta mongoDbMeta, IVariables vars, ILogChannel log) throws MongoDbException {
    MongoProperties.Builder propertiesBuilder = createPropertiesBuilder(mongoDbMeta, vars);

    return mongoWrapperClientFactory.createMongoClientWrapper(
        propertiesBuilder.build(), new HopMongoUtilLogger(log));
  }

  public static MongoProperties.Builder createPropertiesBuilder(
      MongoDbMeta<? extends ITransform, ? extends ITransformData> mongoDbMeta, IVariables vars) {
    MongoProperties.Builder propertiesBuilder = new MongoProperties.Builder();

    setIfNotNullOrEmpty(
        propertiesBuilder, MongoProp.HOST, vars.resolve(mongoDbMeta.getHostnames()));
    setIfNotNullOrEmpty(
        propertiesBuilder, MongoProp.PORT, vars.resolve(mongoDbMeta.getPort()));
    setIfNotNullOrEmpty(
        propertiesBuilder, MongoProp.DBNAME, vars.resolve(mongoDbMeta.getDbName()));
    setIfNotNullOrEmpty(
        propertiesBuilder,
        MongoProp.connectTimeout,
        vars.resolve(mongoDbMeta.getConnectTimeout()));
    setIfNotNullOrEmpty(
        propertiesBuilder,
        MongoProp.socketTimeout,
        vars.resolve(mongoDbMeta.getSocketTimeout()));
    setIfNotNullOrEmpty(
        propertiesBuilder, MongoProp.readPreference, mongoDbMeta.getReadPreference());
    setIfNotNullOrEmpty(propertiesBuilder, MongoProp.writeConcern, mongoDbMeta.getWriteConcern());
    setIfNotNullOrEmpty(propertiesBuilder, MongoProp.wTimeout, mongoDbMeta.getWTimeout());
    setIfNotNullOrEmpty(
        propertiesBuilder, MongoProp.JOURNALED, Boolean.toString(mongoDbMeta.getJournal()));
    setIfNotNullOrEmpty(
        propertiesBuilder,
        MongoProp.USE_ALL_REPLICA_SET_MEMBERS,
        Boolean.toString(mongoDbMeta.getUseAllReplicaSetMembers()));
    setIfNotNullOrEmpty(
        propertiesBuilder,
        MongoProp.AUTH_DATABASE,
        vars.resolve(mongoDbMeta.getAuthenticationDatabaseName()));
    setIfNotNullOrEmpty(
        propertiesBuilder,
        MongoProp.USERNAME,
        vars.resolve(mongoDbMeta.getAuthenticationUser()));
    setIfNotNullOrEmpty(
        propertiesBuilder,
        MongoProp.PASSWORD,
        vars.resolve(mongoDbMeta.getAuthenticationPassword()));
    setIfNotNullOrEmpty(
        propertiesBuilder, MongoProp.AUTH_MECHA, mongoDbMeta.getAuthenticationMechanism());
    setIfNotNullOrEmpty(
        propertiesBuilder,
        MongoProp.USE_KERBEROS,
        Boolean.toString(mongoDbMeta.getUseKerberosAuthentication()));
    setIfNotNullOrEmpty(
        propertiesBuilder, MongoProp.useSSL, Boolean.toString(mongoDbMeta.isUseSSLSocketFactory()));
    if (mongoDbMeta.getReadPrefTagSets() != null) {
      StringBuilder tagSet = new StringBuilder();
      List<String> readPrefTagSets = mongoDbMeta.getReadPrefTagSets();
      for (String tag : readPrefTagSets) {
        tagSet.append(tag);
        tagSet.append(",");
      }
      // Remove trailing comma
      if (tagSet.length() > 0) {
        tagSet.setLength(tagSet.length() - 1);
      }
      setIfNotNullOrEmpty(propertiesBuilder, MongoProp.tagSet, tagSet.toString());
    }

    return propertiesBuilder;
  }

  public static MongoClientWrapper createMongoClientWrapper(
      MongoProperties.Builder properties, ILogChannel log) throws MongoDbException {
    return mongoWrapperClientFactory.createMongoClientWrapper(
        properties.build(), new HopMongoUtilLogger(log));
  }

  private static void setIfNotNullOrEmpty(
      MongoProperties.Builder builder, MongoProp prop, String value) {
    if (value != null && value.trim().length() > 0) {
      boolean isPassword = MongoProp.PASSWORD.equals(prop);
      if (isPassword) {
        value = Encr.decryptPasswordOptionallyEncrypted(value);
      }
      builder.set(prop, value);
    }
  }
}
