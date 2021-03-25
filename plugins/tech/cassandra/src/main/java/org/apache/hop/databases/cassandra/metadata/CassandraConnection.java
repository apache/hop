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

package org.apache.hop.databases.cassandra.metadata;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.databases.cassandra.ConnectionFactory;
import org.apache.hop.databases.cassandra.spi.Connection;
import org.apache.hop.databases.cassandra.spi.Keyspace;
import org.apache.hop.databases.cassandra.util.CassandraUtils;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

import java.util.HashMap;
import java.util.Map;

@GuiPlugin
@HopMetadata(
    key = "cassandra-connection",
    name = "Cassandra Connection",
    description = "Describes a connection to a Cassandra cluster",
    image = "Cassandra_logo.svg")
public class CassandraConnection extends HopMetadataBase implements IHopMetadata {

  public static final String WIDGET_ID_HOSTNAME = "10000-hostname";
  public static final String WIDGET_ID_PORT = "10100-port";
  public static final String WIDGET_ID_USERNAME = "10200-username";
  public static final String WIDGET_ID_PASSWORD = "10300-password";
  public static final String WIDGET_ID_SOCKET_TIMEOUT = "10400-socket-timeout";
  public static final String WIDGET_ID_KEYSPACE = "10500-keyspace";
  public static final String WIDGET_ID_SCHEMA_HOSTNAME = "10800-schema-hostname";
  public static final String WIDGET_ID_SCHEMA_PORT = "10900-schema-port";
  public static final String WIDGET_ID_USE_COMPRESSION = "11000-use-compression";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_HOSTNAME,
      type = GuiElementType.TEXT,
      parentId = CassandraConnectionEditor.PARENT_WIDGET_ID,
      label = "Hostname",
      toolTip = "Specify the hostname of your cassandra server")
  private String hostname;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_PORT,
      type = GuiElementType.TEXT,
      parentId = CassandraConnectionEditor.PARENT_WIDGET_ID,
      label = "Port",
      toolTip = "The default port of a cassandra server is 9042")
  private String port = "9042";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_USERNAME,
      type = GuiElementType.TEXT,
      parentId = CassandraConnectionEditor.PARENT_WIDGET_ID,
      label = "Username",
      toolTip = "The user to log in with")
  private String username;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_PASSWORD,
      type = GuiElementType.TEXT,
      password = true,
      parentId = CassandraConnectionEditor.PARENT_WIDGET_ID,
      label = "Password",
      toolTip = "The password to log in with")
  private String password;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_SOCKET_TIMEOUT,
      type = GuiElementType.TEXT,
      parentId = CassandraConnectionEditor.PARENT_WIDGET_ID,
      label = "Socket timeout",
      toolTip = "The socket timeout to use")
  private String socketTimeout;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_KEYSPACE,
      type = GuiElementType.TEXT,
      parentId = CassandraConnectionEditor.PARENT_WIDGET_ID,
      label = "Keyspace",
      toolTip = "The keyspace to use")
  private String keyspace;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_SCHEMA_HOSTNAME,
      type = GuiElementType.TEXT,
      parentId = CassandraConnectionEditor.PARENT_WIDGET_ID,
      label = "Schema hostname",
      toolTip = "The Cassandra node to put schema updates through")
  private String schemaHostname;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_SCHEMA_PORT,
      type = GuiElementType.TEXT,
      parentId = CassandraConnectionEditor.PARENT_WIDGET_ID,
      label = "Schema port",
      toolTip = "The port of the Cassandra node to put schema updates through")
  private String schemaPort = "";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_USE_COMPRESSION,
      type = GuiElementType.CHECKBOX,
      parentId = CassandraConnectionEditor.PARENT_WIDGET_ID,
      label = "Using compression?",
      toolTip = "Check this option if you want to use compression over the connection")
  private boolean usingCompression;

  public CassandraConnection() {}

  public CassandraConnection(CassandraConnection c) {
    super(c.name);
    this.hostname = c.hostname;
    this.port = c.port;
    this.username = c.username;
    this.password = c.password;
    this.socketTimeout = c.socketTimeout;
    this.keyspace = c.keyspace;
    this.schemaHostname = c.schemaHostname;
    this.schemaPort = c.schemaPort;
    this.usingCompression = c.usingCompression;
  }

  public Connection createConnection(IVariables variables, boolean output) throws Exception {
    return createConnection(variables, getOptionsMap(variables), output);
  }

  public Connection createConnection(
      IVariables variables, Map<String, String> options, boolean output) throws Exception {

    // See if we have a hostname.  The rest is optional in some way.
    //
    if (StringUtils.isEmpty(hostname)) {
      throw new HopException(
          "Please specify a hostname in Cassandra connection '" + getName() + "'");
    }

    String chosenHostname = variables.resolve(hostname);
    String chosenPort = variables.resolve(port);

    // Consider the schema host for output connections...
    //
    if (output) {
      if (StringUtils.isNotEmpty(schemaHostname)) {
        chosenHostname = variables.resolve(schemaHostname);
      }
      if (StringUtils.isNotEmpty(schemaPort)) {
        chosenPort = variables.resolve(schemaPort);
      }
    }

    return CassandraUtils.getCassandraConnection(
        chosenHostname,
        Const.toInt(chosenPort, 9042),
        variables.resolve(username),
        variables.resolve(password),
        ConnectionFactory.Driver.BINARY_CQL3_PROTOCOL,
        options);
  }

  public Map<String, String> getOptionsMap(IVariables variables) {
    Map<String, String> options = new HashMap<>();
    if (!Utils.isEmpty(socketTimeout)) {
      options.put(
          CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT, variables.resolve(socketTimeout));
    }
    options.put(
        CassandraUtils.CQLOptions.DATASTAX_DRIVER_VERSION, CassandraUtils.CQLOptions.CQL3_STRING);

    if (usingCompression) {
      options.put(CassandraUtils.ConnectionOptions.COMPRESSION, Boolean.TRUE.toString());
    }
    return options;
  }

  public Keyspace lookupKeyspace(Connection connection, IVariables variables) throws Exception {
    return connection.getKeyspace(variables.resolve(keyspace));
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
   * Gets username
   *
   * @return value of username
   */
  public String getUsername() {
    return username;
  }

  /** @param username The username to set */
  public void setUsername(String username) {
    this.username = username;
  }

  /**
   * Gets password
   *
   * @return value of password
   */
  public String getPassword() {
    return password;
  }

  /** @param password The password to set */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * Gets socketTimeout
   *
   * @return value of socketTimeout
   */
  public String getSocketTimeout() {
    return socketTimeout;
  }

  /** @param socketTimeout The socketTimeout to set */
  public void setSocketTimeout(String socketTimeout) {
    this.socketTimeout = socketTimeout;
  }

  /**
   * Gets keyspace
   *
   * @return value of keyspace
   */
  public String getKeyspace() {
    return keyspace;
  }

  /** @param keyspace The keyspace to set */
  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }

  /**
   * Gets schemaHostname
   *
   * @return value of schemaHostname
   */
  public String getSchemaHostname() {
    return schemaHostname;
  }

  /** @param schemaHostname The schemaHostname to set */
  public void setSchemaHostname(String schemaHostname) {
    this.schemaHostname = schemaHostname;
  }

  /**
   * Gets schemaPort
   *
   * @return value of schemaPort
   */
  public String getSchemaPort() {
    return schemaPort;
  }

  /** @param schemaPort The schemaPort to set */
  public void setSchemaPort(String schemaPort) {
    this.schemaPort = schemaPort;
  }

  /**
   * Gets usingCompression
   *
   * @return value of usingCompression
   */
  public boolean isUsingCompression() {
    return usingCompression;
  }

  /** @param usingCompression The usingCompression to set */
  public void setUsingCompression(boolean usingCompression) {
    this.usingCompression = usingCompression;
  }
}
