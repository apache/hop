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

package org.apache.hop.mongo;

import com.mongodb.MongoClientOptions;

import javax.net.ssl.SSLSocketFactory;

/**
 * Enumeration of the available properties that can be used when configuring a MongoDB client via a
 * MongoClientWrapper. These properties roughly break down into a set relevant to MongoCredentials,
 * a set relevant to the ServerAddress(es) being used, and the raw MongoClientOptions.
 */
public enum MongoProp {

  // Properties relevant to MongoCredential creation
  /** Authentication database. The database under which db.createUser() command were executed. */
  AUTH_DATABASE,
  /** The user name. Blank or null in the case of NoAuth. */
  USERNAME,
  /** The user's password. Blank or null in the case of NoAuth or GSSAPI. */
  PASSWORD,

  /**
   * Indicates whether to use GSSAPI credentials. Defaults to "false" unless the string value can be
   * parsed as true.
   */
  USE_KERBEROS,

  /**
   * The variable name that may specify the authentication mode to use when creating a JAAS
   * LoginContext. See {@link org.apache.hop.mongo.KerberosUtil.JaasAuthenticationMode} for possible
   * values.
   */
  HOP_JAAS_AUTH_MODE,

  /**
   * The variable name that may specify the location of the keytab file to use when authenticating
   * with "KERBEROS_KEYTAB" mode.
   */
  HOP_JAAS_KEYTAB_FILE,

  /** The database to be used during initial authentication. */
  DBNAME,

  // HOST and PORT correspond to ServerAddress list construction.
  /**
   * Either a single server name, or a comma separted list of server references optionally including
   * port following the form: server1[:port1],server2[:port2]
   */
  HOST,

  /** The port to use, if not specified in the HOST expression. */
  PORT,

  /**
   * A true|false property indicating how WriteConcern should be configured. I.e. indicating whether
   * to block until write operations have been committed to the journal. While cause an exception if
   * set to true when connecting to a server without journaling in use.
   */
  JOURNALED,

  /**
   * Governs whether the MongoClient is instantiated with a List of ServerAddresses, indicating the
   * driver should act against a replica set (even if there's only a single server specified). If
   * there are 2 or more servers specified in HOST a List of ServerAddress will be used, even if
   * this property is unset or false. Hence this property is useful primarily for the case where a
   * single server is specified but replica set behavior is desired.
   */
  USE_ALL_REPLICA_SET_MEMBERS,

  // MongoClientOptions values.  The following properties correspond to
  // http://api.mongodb.org/java/2.12/com/mongodb/MongoClientOptions.html

  /** The maximum num of allowed connections. */
  connectionsPerHost {
    @Override
    public void setOption(
        MongoClientOptions.Builder builder,
        org.apache.hop.mongo.MongoProperties props,
        MongoPropToOption propToOption) {
      builder.connectionsPerHost(propToOption.intValue(props.get(connectionsPerHost), 100));
    }
  },

  /** The connection timeout in millis. 0 means no timeout. */
  connectTimeout {
    @Override
    public void setOption(
        MongoClientOptions.Builder builder,
        org.apache.hop.mongo.MongoProperties props,
        MongoPropToOption propToOption) {
      builder.connectTimeout(propToOption.intValue(props.get(connectTimeout), 10000));
    }
  },

  /** The max wait for a connection to be available (millis). */
  maxWaitTime {
    @Override
    public void setOption(
        MongoClientOptions.Builder builder,
        org.apache.hop.mongo.MongoProperties props,
        MongoPropToOption propToOption) {
      builder.maxWaitTime(propToOption.intValue(props.get(maxWaitTime), 120000));
    }
  },

  /**
   * Sets whether DBCursors should be cleaned up in finalize to guard against programming errors in
   * which .close is not called.
   */
  cursorFinalizerEnabled {
    @Override
    public void setOption(
        MongoClientOptions.Builder builder,
        org.apache.hop.mongo.MongoProperties props,
        MongoPropToOption propToOption) {
      builder.cursorFinalizerEnabled(
          propToOption.boolValue(props.get(cursorFinalizerEnabled), true));
    }
  },

  /** Boolean which controls whether connections are kept alive through firewalls. */
  socketKeepAlive {
    @Override
    public void setOption(
        MongoClientOptions.Builder builder,
        org.apache.hop.mongo.MongoProperties props,
        MongoPropToOption propToOption) {
      builder.socketKeepAlive(propToOption.boolValue(props.get(socketKeepAlive), false));
    }
  },

  /** Socket timeout (miilis). 0 (default) means no timeout. */
  socketTimeout {
    @Override
    public void setOption(
        MongoClientOptions.Builder builder,
        org.apache.hop.mongo.MongoProperties props,
        MongoPropToOption propToOption) {
      builder.socketTimeout(propToOption.intValue(props.get(socketTimeout), 0));
    }
  },

  /** Boolean which controls whether connections will use SSL */
  useSSL {
    @Override
    public void setOption(
        MongoClientOptions.Builder builder,
        org.apache.hop.mongo.MongoProperties props,
        MongoPropToOption propToOption) {
      if ("true".equalsIgnoreCase(props.get(useSSL))) {
        builder.socketFactory(SSLSocketFactory.getDefault());
      }
    }
  },

  /**
   * Read preference to use: [primary, primaryPreferred, secondary, secondaryPreferred, nearest] If
   * set to anything besides primary, the preferance is "taggable", meaning the tag sets defined in
   * the MongoProp.tagSet property will be used if present.
   */
  readPreference {
    @Override
    public void setOption(
        MongoClientOptions.Builder builder,
        org.apache.hop.mongo.MongoProperties props,
        MongoPropToOption propToOption)
        throws MongoDbException {
      builder.readPreference(propToOption.readPrefValue(props));
    }
  },

  /**
   * A comma separated list of JSON docs representing the tag set to associate with readPreference.
   * E.g. { "disk": "ssd", "use": "reporting", "rack": "a" },{ "disk": "ssd", "use": "reporting",
   * "rack": "d" }
   *
   * <p>This will govern the preferred order when determining which mongos to read from. See
   * http://docs.mongodb.org/manual/tutorial/configure-replica-set-tag-sets/ Note that the expected
   * syntax for tag sets differs from the format specified for MongoURI, which uses semi-colons as a
   * delimiter. That would make connection string parsing more difficult, since semi-colon delimits
   * each connection string parameter as well.
   */
  tagSet,

  /**
   * Specifies the number of servers to wait on before acknowledging a write. Corresponds to the
   * String parameter of the constructor {@link com.mongodb.WriteConcern(String, int, boolean,
   * boolean)}.
   */
  writeConcern {
    @Override
    public void setOption(
        MongoClientOptions.Builder builder,
        org.apache.hop.mongo.MongoProperties props,
        MongoPropToOption propToOption)
        throws MongoDbException {
      builder.writeConcern(propToOption.writeConcernValue(props));
    }
  },

  /** Specifies the write timeout in millis for the WriteConcern. */
  wTimeout,
  /** MongoDB 3.0 changed the default authentication mechanism from MONGODB-CR to SCRAM-SHA-1 */
  AUTH_MECHA;

  public void setOption(
      MongoClientOptions.Builder builder,
      org.apache.hop.mongo.MongoProperties props,
      MongoPropToOption propToOption)
      throws MongoDbException {
    // default is do nothing since some of the Props are not a MongoClientOption
  }
}
