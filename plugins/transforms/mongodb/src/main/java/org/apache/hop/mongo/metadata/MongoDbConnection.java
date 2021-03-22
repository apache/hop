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
 *
 */

package org.apache.hop.mongo.metadata;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.mongo.MongoDbException;
import org.apache.hop.mongo.MongoProp;
import org.apache.hop.mongo.MongoProperties;
import org.apache.hop.mongo.NamedReadPreference;
import org.apache.hop.mongo.wrapper.HopMongoUtilLogger;
import org.apache.hop.mongo.wrapper.MongoClientWrapper;
import org.apache.hop.mongo.wrapper.MongoClientWrapperFactory;
import org.apache.hop.mongo.wrapper.MongoWrapperClientFactory;

@GuiPlugin
@HopMetadata(
    key = "mongodb-connection",
    name = "MongoDB Connection",
    description = "Describes a MongoDB connection",
    image = "MongoDB_Leaf_FullColor_RGB.svg")
public class MongoDbConnection extends HopMetadataBase implements IHopMetadata {

  public static final String WIDGET_ID_HOSTNAME = "10000-hostname";
  public static final String WIDGET_ID_PORT = "10100-port";
  public static final String WIDGET_ID_DB_NAME = "10200-database-name";
  public static final String WIDGET_ID_COLLECTION = "10300-collection";
  public static final String WIDGET_ID_AUTH_DB_NAME = "10400-auth-database-name";
  public static final String WIDGET_ID_AUTH_USER = "10500-auth-user";
  public static final String WIDGET_ID_AUTH_PASSWORD = "10600-auth-password";
  public static final String WIDGET_ID_AUTH_MECHANISM = "10700-auth-mechanism";
  public static final String WIDGET_ID_USE_KERBEROS = "10800-use-kerberos";
  public static final String WIDGET_ID_CONNECTION_TIMEOUT_MS = "10900-connection-timeout-ms";
  public static final String WIDGET_ID_SOCKET_TIMEOUT_MS = "11000-socket-timeout-ms";
  public static final String WIDGET_ID_READ_PREFERENCE = "11100-read-preference";
  public static final String WIDGET_ID_USE_ALL_REPLICA_SET_MEMBERS =
      "11200-use-all-replica-set-members";
  public static final String WIDGET_ID_READ_PREF_TAG_SETS = "11300-read-pref-tag-sets";
  public static final String WIDGET_ID_USE_SSL_SOCKET_FACTORY = "11400-use-ssl-socket-factory";
  public static final String WIDGET_ID_WRITE_CONCERN = "11500-write-concern";
  public static final String WIDGET_ID_TIMEOUT_MS = "11600-timeout-ms";
  public static final String WIDGET_ID_JOURNALED = "11700-journaled";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_HOSTNAME,
      type = GuiElementType.TEXT,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Hostname",
      toolTip = "Specify the hostname of your MongoDB server")
  private String hostname = "localhost";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_PORT,
      type = GuiElementType.TEXT,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Port",
      toolTip = "The default port of a MongoDB server is 27017")
  private String port = "27017";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_DB_NAME,
      type = GuiElementType.COMBO,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Database name",
      toolTip = "The MongoDB database name to use")
  private String dbName;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_AUTH_DB_NAME,
      type = GuiElementType.COMBO,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Authentication database",
      toolTip = "The MongoDB database to authenticate against")
  private String authenticationDatabaseName;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_AUTH_USER,
      type = GuiElementType.TEXT,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Username",
      toolTip = "The username to authenticate with")
  private String authenticationUser;

  @HopMetadataProperty(password = true)
  @GuiWidgetElement(
      id = WIDGET_ID_AUTH_PASSWORD,
      type = GuiElementType.TEXT,
      password = true,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Password",
      toolTip = "The password to authenticate with")
  private String authenticationPassword;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_AUTH_MECHANISM,
      type = GuiElementType.COMBO,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Authentication mechanism",
      toolTip = "The authentication mechanism to use",
      variables = false)
  private MongoDbAuthenticationMechanism authenticationMechanism =
      MongoDbAuthenticationMechanism.PLAIN;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_USE_KERBEROS,
      type = GuiElementType.CHECKBOX,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Use Kerberos?",
      toolTip = "Check this option if you want to use Kerberos to authenticate")
  private boolean usingKerberos;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_CONNECTION_TIMEOUT_MS,
      type = GuiElementType.TEXT,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Connection timeout (ms)",
      toolTip = "Specify the connection timeout in milliseconds. Leave blank for the default.")
  private String connectTimeoutMs = ""; // default - never time out

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_SOCKET_TIMEOUT_MS,
      type = GuiElementType.TEXT,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Socket timeout (ms)",
      toolTip = "Specify the socket timeout in milliseconds. Leave blank for the default.")
  private String socketTimeoutMs = ""; // default - never time out

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_READ_PREFERENCE,
      type = GuiElementType.COMBO,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Read preference",
      toolTip =
          "Select the read preference: primary, primaryPreferred, secondary, secondaryPreferred, nearest",
      variables = false)
  private NamedReadPreference readPreference = NamedReadPreference.PRIMARY;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_USE_ALL_REPLICA_SET_MEMBERS,
      type = GuiElementType.CHECKBOX,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Use all replica set members?",
      toolTip =
          "Select this option to discover and use all replica set members "
              + "(if not already specified in the hosts field")
  private boolean usingAllReplicaSetMembers;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_READ_PREF_TAG_SETS,
      type = GuiElementType.COMBO,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Specify the read preference tag sets",
      toolTip =
          "optional tag sets to use with read preference settings (JSON String).  See the MongoDB documentation on 'Replica Set Tag Sets'")
  private String readPrefTagSets;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_USE_SSL_SOCKET_FACTORY,
      type = GuiElementType.CHECKBOX,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Use an SSL socket factory?",
      toolTip = "Check this option if you want to use use an SSL socket factory")
  private boolean usingSslSocketFactory;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_WRITE_CONCERN,
      type = GuiElementType.TEXT,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Write concern",
      toolTip =
          "default = 1 (standalone or primary acknowledges writes; -1 no acknowledgement and all errors "
              + "suppressed; 0 no acknowledgement, but socket/network errors passed to client; \"majority\" "
              + "returns after a majority of the replica set members have acknowledged; n (>1) returns after n "
              + "replica set members have acknowledged; tags (string) specific replica set members with the tags "
              + "need to acknowledge")
  private String writeConcern = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_TIMEOUT_MS,
      type = GuiElementType.TEXT,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Replication Timeout (ms)",
      toolTip =
          "The time in milliseconds to wait for replication to succeed, as specified in the w option, "
              + "before timing out")
  private String replicationTimeoutMs = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_JOURNALED,
      type = GuiElementType.CHECKBOX,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "Journaled?",
      toolTip =
          "Select for write operations to wait until MongoDB acknowledges the write operations and "
              + "commits the data to the journal on disk")
  private boolean journaled = true;

  public MongoDbConnection() {}

  public MongoDbConnection(MongoDbConnection m) {
    super(m.name);
    this.hostname = m.hostname;
    this.port = m.port;
    this.dbName = m.dbName;
    this.authenticationDatabaseName = m.authenticationDatabaseName;
    this.authenticationUser = m.authenticationUser;
    this.authenticationPassword = m.authenticationPassword;
    this.authenticationMechanism = m.authenticationMechanism;
    this.usingKerberos = m.usingKerberos;
    this.connectTimeoutMs = m.connectTimeoutMs;
    this.socketTimeoutMs = m.socketTimeoutMs;
    this.readPreference = m.readPreference;
    this.usingAllReplicaSetMembers = m.usingAllReplicaSetMembers;
    this.readPrefTagSets = m.readPrefTagSets;
    this.usingSslSocketFactory = m.usingSslSocketFactory;
    this.writeConcern = m.writeConcern;
    this.replicationTimeoutMs = m.replicationTimeoutMs;
    this.journaled = m.journaled;
  }

  private static MongoWrapperClientFactory mongoWrapperClientFactory =
      (props, log) -> MongoClientWrapperFactory.createMongoClientWrapper(props, log);

  /**
   * Test this connection
   *
   * @param variables
   * @param log
   * @throws MongoDbException in case we couldn't connect
   */
  public void test(IVariables variables, ILogChannel log) throws MongoDbException {
    MongoClientWrapper wrapper = createWrapper(variables, log);
    try {
      wrapper.test();
    } finally {
      wrapper.dispose();
    }
  }

  public MongoClientWrapper createWrapper(IVariables variables, ILogChannel log)
      throws MongoDbException {
    return mongoWrapperClientFactory.createMongoClientWrapper(
        createPropertiesBuilder(variables).build(), new HopMongoUtilLogger(log));
  }

  public MongoProperties.Builder createPropertiesBuilder(IVariables variables) {
    MongoProperties.Builder propertiesBuilder = new MongoProperties.Builder();

    setIfNotNullOrEmpty(variables, propertiesBuilder, MongoProp.HOST, hostname);
    setIfNotNullOrEmpty(variables, propertiesBuilder, MongoProp.PORT, port);
    setIfNotNullOrEmpty(variables, propertiesBuilder, MongoProp.DBNAME, dbName);
    setIfNotNullOrEmpty(variables, propertiesBuilder, MongoProp.connectTimeout, connectTimeoutMs);
    setIfNotNullOrEmpty(variables, propertiesBuilder, MongoProp.socketTimeout, socketTimeoutMs);
    setIfNotNullOrEmpty(
        variables, propertiesBuilder, MongoProp.readPreference, readPreference.getName());
    setIfNotNullOrEmpty(variables, propertiesBuilder, MongoProp.writeConcern, writeConcern);
    setIfNotNullOrEmpty(variables, propertiesBuilder, MongoProp.wTimeout, replicationTimeoutMs );
    setIfNotNullOrEmpty(
        variables, propertiesBuilder, MongoProp.JOURNALED, Boolean.toString(journaled));
    setIfNotNullOrEmpty(
        variables,
        propertiesBuilder,
        MongoProp.USE_ALL_REPLICA_SET_MEMBERS,
        Boolean.toString(usingAllReplicaSetMembers));
    setIfNotNullOrEmpty(
        variables, propertiesBuilder, MongoProp.AUTH_DATABASE, authenticationDatabaseName);
    setIfNotNullOrEmpty(variables, propertiesBuilder, MongoProp.USERNAME, authenticationUser);
    setIfNotNullOrEmpty(variables, propertiesBuilder, MongoProp.PASSWORD, authenticationPassword);
    setIfNotNullOrEmpty(
        variables, propertiesBuilder, MongoProp.AUTH_MECHA, authenticationMechanism.name());
    setIfNotNullOrEmpty(
        variables, propertiesBuilder, MongoProp.USE_KERBEROS, Boolean.toString(usingKerberos));
    setIfNotNullOrEmpty(
        variables, propertiesBuilder, MongoProp.useSSL, Boolean.toString(usingSslSocketFactory));
    setIfNotNullOrEmpty(variables, propertiesBuilder, MongoProp.tagSet, readPrefTagSets);

    return propertiesBuilder;
  }

  private static void setIfNotNullOrEmpty(
      IVariables variables, MongoProperties.Builder builder, MongoProp prop, String value) {
    if (StringUtils.isNotEmpty(value)) {
      builder.set(prop, variables.resolve(value));
    }
  }

  /**
   * Gets hostname
   *
   * @return value of hostname
   */
  public String getHostname() {
    return hostname;
  }

  /** @param hostname The hostname to set */
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  /**
   * Gets port
   *
   * @return value of port
   */
  public String getPort() {
    return port;
  }

  /** @param port The port to set */
  public void setPort(String port) {
    this.port = port;
  }

  /**
   * Gets dbName
   *
   * @return value of dbName
   */
  public String getDbName() {
    return dbName;
  }

  /** @param dbName The dbName to set */
  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  /**
   * Gets authenticationDatabaseName
   *
   * @return value of authenticationDatabaseName
   */
  public String getAuthenticationDatabaseName() {
    return authenticationDatabaseName;
  }

  /** @param authenticationDatabaseName The authenticationDatabaseName to set */
  public void setAuthenticationDatabaseName(String authenticationDatabaseName) {
    this.authenticationDatabaseName = authenticationDatabaseName;
  }

  /**
   * Gets authenticationUser
   *
   * @return value of authenticationUser
   */
  public String getAuthenticationUser() {
    return authenticationUser;
  }

  /** @param authenticationUser The authenticationUser to set */
  public void setAuthenticationUser(String authenticationUser) {
    this.authenticationUser = authenticationUser;
  }

  /**
   * Gets authenticationPassword
   *
   * @return value of authenticationPassword
   */
  public String getAuthenticationPassword() {
    return authenticationPassword;
  }

  /** @param authenticationPassword The authenticationPassword to set */
  public void setAuthenticationPassword(String authenticationPassword) {
    this.authenticationPassword = authenticationPassword;
  }

  /**
   * Gets authenticationMechanism
   *
   * @return value of authenticationMechanism
   */
  public MongoDbAuthenticationMechanism getAuthenticationMechanism() {
    return authenticationMechanism;
  }

  /** @param authenticationMechanism The authenticationMechanism to set */
  public void setAuthenticationMechanism(MongoDbAuthenticationMechanism authenticationMechanism) {
    this.authenticationMechanism = authenticationMechanism;
  }

  /**
   * Gets usingKerberos
   *
   * @return value of usingKerberos
   */
  public boolean isUsingKerberos() {
    return usingKerberos;
  }

  /** @param usingKerberos The usingKerberos to set */
  public void setUsingKerberos(boolean usingKerberos) {
    this.usingKerberos = usingKerberos;
  }

  /**
   * Gets connectTimeoutMs
   *
   * @return value of connectTimeoutMs
   */
  public String getConnectTimeoutMs() {
    return connectTimeoutMs;
  }

  /** @param connectTimeoutMs The connectTimeoutMs to set */
  public void setConnectTimeoutMs(String connectTimeoutMs) {
    this.connectTimeoutMs = connectTimeoutMs;
  }

  /**
   * Gets socketTimeoutMs
   *
   * @return value of socketTimeoutMs
   */
  public String getSocketTimeoutMs() {
    return socketTimeoutMs;
  }

  /** @param socketTimeoutMs The socketTimeoutMs to set */
  public void setSocketTimeoutMs(String socketTimeoutMs) {
    this.socketTimeoutMs = socketTimeoutMs;
  }

  /**
   * Gets readPreference
   *
   * @return value of readPreference
   */
  public NamedReadPreference getReadPreference() {
    return readPreference;
  }

  /** @param readPreference The readPreference to set */
  public void setReadPreference(NamedReadPreference readPreference) {
    this.readPreference = readPreference;
  }

  /**
   * Gets usingAllReplicaSetMembers
   *
   * @return value of usingAllReplicaSetMembers
   */
  public boolean isUsingAllReplicaSetMembers() {
    return usingAllReplicaSetMembers;
  }

  /** @param usingAllReplicaSetMembers The usingAllReplicaSetMembers to set */
  public void setUsingAllReplicaSetMembers(boolean usingAllReplicaSetMembers) {
    this.usingAllReplicaSetMembers = usingAllReplicaSetMembers;
  }

  /**
   * Gets readPrefTagSets
   *
   * @return value of readPrefTagSets
   */
  public String getReadPrefTagSets() {
    return readPrefTagSets;
  }

  /** @param readPrefTagSets The readPrefTagSets to set */
  public void setReadPrefTagSets(String readPrefTagSets) {
    this.readPrefTagSets = readPrefTagSets;
  }

  /**
   * Gets usingSslSocketFactory
   *
   * @return value of usingSslSocketFactory
   */
  public boolean isUsingSslSocketFactory() {
    return usingSslSocketFactory;
  }

  /** @param usingSslSocketFactory The usingSslSocketFactory to set */
  public void setUsingSslSocketFactory(boolean usingSslSocketFactory) {
    this.usingSslSocketFactory = usingSslSocketFactory;
  }

  /**
   * Gets writeConcern
   *
   * @return value of writeConcern
   */
  public String getWriteConcern() {
    return writeConcern;
  }

  /** @param writeConcern The writeConcern to set */
  public void setWriteConcern(String writeConcern) {
    this.writeConcern = writeConcern;
  }

  /**
   * Gets replicationTimeoutMs
   *
   * @return value of replicationTimeoutMs
   */
  public String getReplicationTimeoutMs() {
    return replicationTimeoutMs;
  }

  /**
   * @param replicationTimeoutMs The replicationTimeoutMs to set
   */
  public void setReplicationTimeoutMs( String replicationTimeoutMs ) {
    this.replicationTimeoutMs = replicationTimeoutMs;
  }

  /**
   * Gets journaled
   *
   * @return value of journaled
   */
  public boolean isJournaled() {
    return journaled;
  }

  /** @param journaled The journaled to set */
  public void setJournaled(boolean journaled) {
    this.journaled = journaled;
  }
}
