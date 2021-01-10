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
package org.apache.hop.pipeline.transforms.mongodb;

import org.apache.hop.core.injection.Injection;
import org.apache.hop.mongo.NamedReadPreference;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transforms.mongodbinput.MongoDbInputMeta;

import java.util.List;

public abstract class MongoDbMeta<Main extends ITransform, Data extends ITransformData>
    extends BaseTransformMeta implements ITransformMeta<Main, Data> {
  protected static Class<?> PKG = MongoDbInputMeta.class; // For Translator

  @Injection(name = "HOSTNAME")
  private String hostname = "localhost";

  @Injection(name = "PORT")
  private String port = "27017";

  @Injection(name = "DATABASE_NAME")
  private String dbName;

  @Injection(name = "COLLECTION")
  private String collection;

  @Injection(name = "AUTH_DATABASE")
  private String authenticationDatabaseName;

  @Injection(name = "AUTH_USERNAME")
  private String authenticationUser;

  @Injection(name = "AUTH_PASSWORD")
  private String authenticationPassword;

  @Injection(name = "AUTH_MECHANISM")
  private String authenticationMechanism = "";

  @Injection(name = "AUTH_KERBEROS")
  private boolean m_kerberos;

  @Injection(name = "TIMEOUT_CONNECTION")
  private String m_connectTimeout = ""; // default - never time out

  @Injection(name = "TIMEOUT_SOCKET")
  private String m_socketTimeout = ""; // default - never time out

  /** primary, primaryPreferred, secondary, secondaryPreferred, nearest */
  @Injection(name = "READ_PREFERENCE")
  private String m_readPreference = NamedReadPreference.PRIMARY.getName();

  /**
   * whether to discover and use all replica set members (if not already specified in the hosts
   * field)
   */
  @Injection(name = "USE_ALL_REPLICA_SET_MEMBERS")
  private boolean m_useAllReplicaSetMembers;

  /** optional tag sets to use with read preference settings */
  @Injection(name = "TAG_SET")
  private List<String> m_readPrefTagSets;

  @Injection(name = "USE_SSL_SOCKET_FACTORY")
  private boolean m_useSSLSocketFactory;

  /**
   * default = 1 (standalone or primary acknowledges writes; -1 no acknowledgement and all errors
   * suppressed; 0 no acknowledgement, but socket/network errors passed to client; "majority"
   * returns after a majority of the replica set members have acknowledged; n (>1) returns after n
   * replica set members have acknowledged; tags (string) specific replica set members with the tags
   * need to acknowledge
   */
  private String m_writeConcern = "";

  /**
   * The time in milliseconds to wait for replication to succeed, as specified in the w option,
   * before timing out
   */
  private String m_wTimeout = "";

  /**
   * whether write operations will wait till the mongod acknowledges the write operations and
   * commits the data to the on disk journal
   */
  private boolean m_journal;

  public void setReadPrefTagSets(List<String> tagSets) {
    m_readPrefTagSets = tagSets;
  }

  public List<String> getReadPrefTagSets() {
    return m_readPrefTagSets;
  }

  public void setUseAllReplicaSetMembers(boolean u) {
    m_useAllReplicaSetMembers = u;
  }

  public boolean getUseAllReplicaSetMembers() {
    return m_useAllReplicaSetMembers;
  }

  /** @return the hostnames (comma separated: host:<port>) */
  public String getHostnames() {
    return hostname;
  }

  /** @param hostname the hostnames to set (comma separated: host:<port>) */
  public void setHostnames(String hostname) {
    this.hostname = hostname;
  }

  /**
   * @return the port. This is a port to use for all hostnames (avoids having to specify the same
   *     port for each hostname in the hostnames list
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port the port. This is a port to use for all hostnames (avoids having to specify the
   *     same port for each hostname in the hostnames list
   */
  public void setPort(String port) {
    this.port = port;
  }

  /** @return the dbName */
  public String getDbName() {
    return dbName;
  }

  /** @param dbName the dbName to set */
  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  /** @return the collection */
  public String getCollection() {
    return collection;
  }

  /** @param collection the collection to set */
  public void setCollection(String collection) {
    this.collection = collection;
  }

  /**
   * Get the AuthenticationDatabase parameter.
   *
   * @return an authentication database.
   */
  public String getAuthenticationDatabaseName() {
    return authenticationDatabaseName;
  }

  /**
   * Set the AuthenticationDatabase parameter.
   *
   * @param authenticationDatabaseName an authentication database to set.
   */
  public void setAuthenticationDatabaseName(String authenticationDatabaseName) {
    this.authenticationDatabaseName = authenticationDatabaseName;
  }

  /** @return the authenticationUser */
  public String getAuthenticationUser() {
    return authenticationUser;
  }

  /** @param authenticationUser the authenticationUser to set */
  public void setAuthenticationUser(String authenticationUser) {
    this.authenticationUser = authenticationUser;
  }

  /** @return the authenticationPassword */
  public String getAuthenticationPassword() {
    return authenticationPassword;
  }

  /** @param authenticationPassword the authenticationPassword to set */
  public void setAuthenticationPassword(String authenticationPassword) {
    this.authenticationPassword = authenticationPassword;
  }

  /**
   * Set whether to use kerberos authentication
   *
   * @param k true if kerberos is to be used
   */
  public void setUseKerberosAuthentication(boolean k) {
    m_kerberos = k;
  }

  /**
   * Get whether to use kerberos authentication
   *
   * @return true if kerberos is to be used
   */
  public boolean getUseKerberosAuthentication() {
    return m_kerberos;
  }

  /**
   * Set the connection timeout. The default is never timeout
   *
   * @param to the connection timeout in milliseconds
   */
  public void setConnectTimeout(String to) {
    m_connectTimeout = to;
  }

  /**
   * Get the connection timeout. The default is never timeout
   *
   * @return the connection timeout in milliseconds
   */
  public String getConnectTimeout() {
    return m_connectTimeout;
  }

  /**
   * Set the number of milliseconds to attempt a send or receive on a socket before timing out.
   *
   * @param so the number of milliseconds before socket timeout
   */
  public void setSocketTimeout(String so) {
    m_socketTimeout = so;
  }

  /**
   * Get the number of milliseconds to attempt a send or receive on a socket before timing out.
   *
   * @return the number of milliseconds before socket timeout
   */
  public String getSocketTimeout() {
    return m_socketTimeout;
  }

  /**
   * Set the read preference to use - primary, primaryPreferred, secondary, secondaryPreferred or
   * nearest.
   *
   * @param preference the read preference to use
   */
  public void setReadPreference(String preference) {
    m_readPreference = preference;
  }

  /**
   * Get the read preference to use - primary, primaryPreferred, secondary, secondaryPreferred or
   * nearest.
   *
   * @return the read preference to use
   */
  public String getReadPreference() {
    return m_readPreference;
  }

  /**
   * Set the write concern to use
   *
   * @param concern the write concern to use
   */
  public void setWriteConcern(String concern) {
    m_writeConcern = concern;
  }

  /** Get the write concern to use */
  public String getWriteConcern() {
    return m_writeConcern;
  }

  /**
   * Set the time in milliseconds to wait for replication to succeed, as specified in the w option,
   * before timing out
   *
   * @param w the timeout to use
   */
  public void setWTimeout(String w) {
    m_wTimeout = w;
  }

  /**
   * Get the time in milliseconds to wait for replication to succeed, as specified in the w option,
   * before timing out
   *
   * @return the timeout to use
   */
  public String getWTimeout() {
    return m_wTimeout;
  }

  /**
   * Set whether to use journaled writes
   *
   * @param j true for journaled writes
   */
  public void setJournal(boolean j) {
    m_journal = j;
  }

  /**
   * Get whether to use journaled writes
   *
   * @return true for journaled writes
   */
  public boolean getJournal() {
    return m_journal;
  }

  /** Get Mongo authentication mechanism */
  public String getAuthenticationMechanism() {
    return authenticationMechanism;
  }
  /** Set Mongo authentication mechanism */
  public void setAuthenticationMechanism(String authenticationMechanism) {
    this.authenticationMechanism = authenticationMechanism;
  }

  public boolean isUseSSLSocketFactory() {
    return m_useSSLSocketFactory;
  }

  public void setUseSSLSocketFactory(boolean value) {
    this.m_useSSLSocketFactory = value;
  }
}
