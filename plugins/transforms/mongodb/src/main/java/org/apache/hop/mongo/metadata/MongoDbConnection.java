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
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
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
    name = "i18n::MongoDbConnection.name",
    description = "i18n::MongoDbConnection.description",
    image = "MongoDB_Leaf_FullColor_RGB.svg",
    documentationUrl = "/metadata-types/mongodb-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.MONGODB_CONNECTION)
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
      label = "i18n::MongoMetadata.Hostname.Label",
      toolTip = "i18n::MongoMetadata.Hostname.ToolTip")
  private String hostname = "localhost";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_PORT,
      type = GuiElementType.TEXT,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.Port.Label",
      toolTip = "i18n::MongoMetadata.Port.ToolTip")
  private String port = "27017";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_DB_NAME,
      type = GuiElementType.COMBO,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.DbName.Label",
      toolTip = "i18n::MongoMetadata.DbName.ToolTip")
  private String dbName;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_AUTH_DB_NAME,
      type = GuiElementType.COMBO,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.AuthDatabaseName.Label",
      toolTip = "i18n::MongoMetadata.AuthDatabaseName.ToolTip")
  private String authenticationDatabaseName;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_AUTH_USER,
      type = GuiElementType.TEXT,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.Username.Label",
      toolTip = "i18n::MongoMetadata.Username.ToolTip")
  private String authenticationUser;

  @HopMetadataProperty(password = true)
  @GuiWidgetElement(
      id = WIDGET_ID_AUTH_PASSWORD,
      type = GuiElementType.TEXT,
      password = true,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.Password.Label",
      toolTip = "i18n::MongoMetadata.Password.ToolTip")
  private String authenticationPassword;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_AUTH_MECHANISM,
      type = GuiElementType.COMBO,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.AuthenticationMechanism.Label",
      toolTip = "i18n::MongoMetadata.AuthenticationMechanism.ToolTip",
      variables = false)
  private MongoDbAuthenticationMechanism authenticationMechanism =
      MongoDbAuthenticationMechanism.PLAIN;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_USE_KERBEROS,
      type = GuiElementType.CHECKBOX,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.useKerberos.Label",
      toolTip = "i18n::MongoMetadata.useKerberos.ToolTip")
  private boolean usingKerberos;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_CONNECTION_TIMEOUT_MS,
      type = GuiElementType.TEXT,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.ConnectionTimeoutMs.Label",
      toolTip = "i18n::MongoMetadata.ConnectionTimeoutMs.ToolTip")
  private String connectTimeoutMs = ""; // default - never time out

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_SOCKET_TIMEOUT_MS,
      type = GuiElementType.TEXT,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.SocketTimeoutMs.Label",
      toolTip = "i18n::MongoMetadata.SocketTimeoutMs.ToolTip")
  private String socketTimeoutMs = ""; // default - never time out

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_READ_PREFERENCE,
      type = GuiElementType.COMBO,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.ReadPreference.Label",
      toolTip = "i18n::MongoMetadata.ReadPreference.ToolTip",
      variables = false)
  private NamedReadPreference readPreference = NamedReadPreference.PRIMARY;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_USE_ALL_REPLICA_SET_MEMBERS,
      type = GuiElementType.CHECKBOX,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.UseAllReplicaSetMembers.Label",
      toolTip = "i18n::MongoMetadata.UseAllReplicaSetMembers.ToolTip")
  private boolean usingAllReplicaSetMembers;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_READ_PREF_TAG_SETS,
      type = GuiElementType.COMBO,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.ReadPrefTagSets.Label",
      toolTip = "i18n::MongoMetadata.ReadPrefTagSets.ToolTip")
  private String readPrefTagSets;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_USE_SSL_SOCKET_FACTORY,
      type = GuiElementType.CHECKBOX,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.UseSslSocketFactory.Label",
      toolTip = "i18n::MongoMetadata.UseSslSocketFactory.ToolTip")
  private boolean usingSslSocketFactory;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_WRITE_CONCERN,
      type = GuiElementType.TEXT,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.WriteConcern.Label",
      toolTip = "i18n::MongoMetadata.WriteConcern.ToolTip")
  private String writeConcern = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_TIMEOUT_MS,
      type = GuiElementType.TEXT,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.ReplicaTimeoutMs.Label",
      toolTip = "i18n::MongoMetadata.ReplicaTimeoutMs.ToolTip")
  private String replicationTimeoutMs = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_JOURNALED,
      type = GuiElementType.CHECKBOX,
      parentId = MongoDbConnectionEditor.PARENT_WIDGET_ID,
      label = "i18n::MongoMetadata.Journaled.Label",
      toolTip = "i18n::MongoMetadata.Journaled.ToolTip")
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
      MongoClientWrapperFactory::createMongoClientWrapper;

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
    setIfNotNullOrEmpty(variables, propertiesBuilder, MongoProp.wTimeout, replicationTimeoutMs);
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
    setIfNotNullOrEmptyPassword(
        variables, propertiesBuilder, MongoProp.PASSWORD, authenticationPassword);
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

  private static void setIfNotNullOrEmptyPassword(
      IVariables variables, MongoProperties.Builder builder, MongoProp prop, String value) {
    if (StringUtils.isNotEmpty(value)) {
      builder.set(prop, Utils.resolvePassword(variables, variables.resolve(value)));
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

  /**
   * @param hostname The hostname to set
   */
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

  /**
   * @param port The port to set
   */
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

  /**
   * @param dbName The dbName to set
   */
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

  /**
   * @param authenticationDatabaseName The authenticationDatabaseName to set
   */
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

  /**
   * @param authenticationUser The authenticationUser to set
   */
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

  /**
   * @param authenticationPassword The authenticationPassword to set
   */
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

  /**
   * @param authenticationMechanism The authenticationMechanism to set
   */
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

  /**
   * @param usingKerberos The usingKerberos to set
   */
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

  /**
   * @param connectTimeoutMs The connectTimeoutMs to set
   */
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

  /**
   * @param socketTimeoutMs The socketTimeoutMs to set
   */
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

  /**
   * @param readPreference The readPreference to set
   */
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

  /**
   * @param usingAllReplicaSetMembers The usingAllReplicaSetMembers to set
   */
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

  /**
   * @param readPrefTagSets The readPrefTagSets to set
   */
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

  /**
   * @param usingSslSocketFactory The usingSslSocketFactory to set
   */
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

  /**
   * @param writeConcern The writeConcern to set
   */
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
  public void setReplicationTimeoutMs(String replicationTimeoutMs) {
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

  /**
   * @param journaled The journaled to set
   */
  public void setJournaled(boolean journaled) {
    this.journaled = journaled;
  }
}
