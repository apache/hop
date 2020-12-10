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

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.MongoProp;
import org.apache.hop.mongo.MongoProperties;
import org.apache.hop.mongo.MongoUtilLogger;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of MongoClientWrapper which uses no credentials. Should only be instantiated by
 * MongoClientWrapperFactory.
 */
class UsernamePasswordMongoClientWrapper
    extends NoAuthMongoClientWrapper {
  private final String user;

  /**
   * Create a connection to a Mongo server based on parameters supplied in the transform meta data
   *
   * @param props properties to use
   * @param log for logging
   * @throws MongoDbException if a problem occurs
   */
  UsernamePasswordMongoClientWrapper(MongoProperties props, MongoUtilLogger log)
      throws MongoDbException {
    super(props, log);
    user = props.get(MongoProp.USERNAME);
  }

  UsernamePasswordMongoClientWrapper(MongoClient mongo, MongoUtilLogger log, String user) {
    super(mongo, null, log);
    props = null;
    this.user = user;
  }

  public String getUser() {
    return user;
  }

  /**
   * Create a credentials object
   *
   * @return a configured MongoCredential object
   */
  @Override
  public List<MongoCredential> getCredentialList() {
    List<MongoCredential> credList = new ArrayList<>();
    String authDatabase = props.get(MongoProp.AUTH_DATABASE);
    String authMecha = props.get(MongoProp.AUTH_MECHA);
    // if not value on AUTH_MECHA set "MONGODB-CR" default authentication mechanism
    if (authMecha == null) {
      authMecha = "";
    }

    // Use the AuthDatabase if one was supplied, otherwise use the connecting database
    authDatabase =
        (authDatabase == null || authDatabase.trim().isEmpty())
            ? props.get(MongoProp.DBNAME)
            : authDatabase;

    if (authMecha.equalsIgnoreCase("SCRAM-SHA-1")) {
      credList.add(
          MongoCredential.createScramSha1Credential(
              props.get(MongoProp.USERNAME),
              authDatabase,
              props.get(MongoProp.PASSWORD).toCharArray()));
    } else if (authMecha.equalsIgnoreCase("PLAIN")) {
      credList.add(
          MongoCredential.createPlainCredential(
              props.get(MongoProp.USERNAME),
              authDatabase,
              props.get(MongoProp.PASSWORD).toCharArray()));
    } else {
      credList.add(
          MongoCredential.createCredential(
              props.get(MongoProp.USERNAME),
              authDatabase,
              props.get(MongoProp.PASSWORD).toCharArray()));
    }
    return credList;
  }
}
