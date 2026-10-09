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

package org.apache.hop.neo4j.bolt;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.BaseGraphDatabase;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.neo4j.driver.Driver;

/**
 * The settings shared by all graph databases spoken to over the Bolt protocol with the Neo4j Java
 * driver. The fields are those of {@link NeoConnection}: the driver code is shared through {@link
 * #toNeoConnection(String)}, so a Bolt connection behaves exactly like a Neo4j connection.
 */
@Getter
@Setter
public abstract class BoltGraphDatabase extends BaseGraphDatabase {

  @GuiWidgetElement(
      id = "server",
      order = "0010",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.server.Label",
      toolTip = "i18n::BoltGraphDatabase.server.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Connection",
      groupOrder = "10")
  @HopMetadataProperty
  private String server;

  @GuiWidgetElement(
      id = "databaseName",
      order = "0020",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.databaseName.Label",
      toolTip = "i18n::BoltGraphDatabase.databaseName.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Connection",
      groupOrder = "10")
  @HopMetadataProperty
  private String databaseName;

  @GuiWidgetElement(
      id = "boltPort",
      order = "0030",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.boltPort.Label",
      toolTip = "i18n::BoltGraphDatabase.boltPort.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Connection",
      groupOrder = "10")
  @HopMetadataProperty
  private String boltPort;

  @GuiWidgetElement(
      id = "browserPort",
      order = "0040",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.browserPort.Label",
      toolTip = "i18n::BoltGraphDatabase.browserPort.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Connection",
      groupOrder = "10")
  @HopMetadataProperty
  private String browserPort;

  @GuiWidgetElement(
      id = "username",
      order = "0050",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.username.Label",
      toolTip = "i18n::BoltGraphDatabase.username.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Connection",
      groupOrder = "10")
  @HopMetadataProperty
  private String username;

  @GuiWidgetElement(
      id = "password",
      order = "0060",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::BoltGraphDatabase.password.Label",
      toolTip = "i18n::BoltGraphDatabase.password.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Connection",
      groupOrder = "10")
  @HopMetadataProperty(password = true)
  private String password;

  @GuiWidgetElement(
      id = "automatic",
      order = "0070",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::BoltGraphDatabase.automatic.Label",
      toolTip = "i18n::BoltGraphDatabase.automatic.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Protocol",
      groupOrder = "20")
  @HopMetadataProperty
  private boolean automatic;

  @GuiWidgetElement(
      id = "automaticVariable",
      order = "0080",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.automaticVariable.Label",
      toolTip = "i18n::BoltGraphDatabase.automaticVariable.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Protocol",
      groupOrder = "20")
  @HopMetadataProperty
  private String automaticVariable;

  @GuiWidgetElement(
      id = "protocol",
      order = "0090",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.protocol.Label",
      toolTip = "i18n::BoltGraphDatabase.protocol.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Protocol",
      groupOrder = "20")
  @HopMetadataProperty
  private String protocol;

  @GuiWidgetElement(
      id = "routing",
      order = "0100",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::BoltGraphDatabase.routing.Label",
      toolTip = "i18n::BoltGraphDatabase.routing.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Protocol",
      groupOrder = "20")
  @HopMetadataProperty
  private boolean routing;

  @GuiWidgetElement(
      id = "routingVariable",
      order = "0110",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.routingVariable.Label",
      toolTip = "i18n::BoltGraphDatabase.routingVariable.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Protocol",
      groupOrder = "20")
  @HopMetadataProperty
  private String routingVariable;

  @GuiWidgetElement(
      id = "routingPolicy",
      order = "0120",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.routingPolicy.Label",
      toolTip = "i18n::BoltGraphDatabase.routingPolicy.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Protocol",
      groupOrder = "20")
  @HopMetadataProperty
  private String routingPolicy;

  @GuiWidgetElement(
      id = "usingEncryption",
      order = "0130",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::BoltGraphDatabase.usingEncryption.Label",
      toolTip = "i18n::BoltGraphDatabase.usingEncryption.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Protocol",
      groupOrder = "20")
  @HopMetadataProperty
  private boolean usingEncryption;

  @GuiWidgetElement(
      id = "usingEncryptionVariable",
      order = "0140",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.usingEncryptionVariable.Label",
      toolTip = "i18n::BoltGraphDatabase.usingEncryptionVariable.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Protocol",
      groupOrder = "20")
  @HopMetadataProperty
  private String usingEncryptionVariable;

  @GuiWidgetElement(
      id = "trustAllCertificates",
      order = "0150",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::BoltGraphDatabase.trustAllCertificates.Label",
      toolTip = "i18n::BoltGraphDatabase.trustAllCertificates.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Protocol",
      groupOrder = "20")
  @HopMetadataProperty
  private boolean trustAllCertificates;

  @GuiWidgetElement(
      id = "trustAllCertificatesVariable",
      order = "0160",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.trustAllCertificatesVariable.Label",
      toolTip = "i18n::BoltGraphDatabase.trustAllCertificatesVariable.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Protocol",
      groupOrder = "20")
  @HopMetadataProperty
  private String trustAllCertificatesVariable;

  @GuiWidgetElement(
      id = "connectionLivenessCheckTimeout",
      order = "0170",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.connectionLivenessCheckTimeout.Label",
      toolTip = "i18n::BoltGraphDatabase.connectionLivenessCheckTimeout.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Advanced",
      groupOrder = "30")
  @HopMetadataProperty
  private String connectionLivenessCheckTimeout;

  @GuiWidgetElement(
      id = "maxConnectionLifetime",
      order = "0180",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.maxConnectionLifetime.Label",
      toolTip = "i18n::BoltGraphDatabase.maxConnectionLifetime.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Advanced",
      groupOrder = "30")
  @HopMetadataProperty
  private String maxConnectionLifetime;

  @GuiWidgetElement(
      id = "maxConnectionPoolSize",
      order = "0190",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.maxConnectionPoolSize.Label",
      toolTip = "i18n::BoltGraphDatabase.maxConnectionPoolSize.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Advanced",
      groupOrder = "30")
  @HopMetadataProperty
  private String maxConnectionPoolSize;

  @GuiWidgetElement(
      id = "connectionAcquisitionTimeout",
      order = "0200",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.connectionAcquisitionTimeout.Label",
      toolTip = "i18n::BoltGraphDatabase.connectionAcquisitionTimeout.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Advanced",
      groupOrder = "30")
  @HopMetadataProperty
  private String connectionAcquisitionTimeout;

  @GuiWidgetElement(
      id = "connectionTimeout",
      order = "0210",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.connectionTimeout.Label",
      toolTip = "i18n::BoltGraphDatabase.connectionTimeout.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Advanced",
      groupOrder = "30")
  @HopMetadataProperty
  private String connectionTimeout;

  @GuiWidgetElement(
      id = "maxTransactionRetryTime",
      order = "0220",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BoltGraphDatabase.maxTransactionRetryTime.Label",
      toolTip = "i18n::BoltGraphDatabase.maxTransactionRetryTime.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.Advanced",
      groupOrder = "30")
  @HopMetadataProperty
  private String maxTransactionRetryTime;

  @GuiWidgetElement(
      id = "manualUrls",
      order = "0230",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TABLE,
      toolTip = "i18n::BoltGraphDatabase.manualUrls.Tooltip",
      groupType = GuiWidgetGroupType.TABS,
      group = "i18n::BoltGraphDatabase.Group.ManualUrls",
      groupOrder = "40",
      tableRows = 4)
  @HopMetadataProperty
  private List<BoltManualUrl> manualUrls = new ArrayList<>();

  protected BoltGraphDatabase() {
    NeoConnection defaults = new NeoConnection();
    boltPort = defaults.getBoltPort();
    browserPort = defaults.getBrowserPort();
    protocol = defaults.getProtocol();
    automatic = defaults.isAutomatic();
  }

  @Override
  public BoltGraphDatabase clone() {
    BoltGraphDatabase clone = (BoltGraphDatabase) super.clone();
    clone.manualUrls = new ArrayList<>();
    for (BoltManualUrl manualUrl : manualUrls) {
      clone.manualUrls.add(new BoltManualUrl(manualUrl.getUrl()));
    }
    return clone;
  }

  /** The dialect of this database type. */
  @Override
  public abstract BoltGraphDialect getGraphDialect();

  /**
   * The settings of this Bolt database as a Neo4j connection, the form the transforms and actions
   * of the Neo4j plugin work with.
   *
   * @param name The name of the graph database connection
   */
  public NeoConnection toNeoConnection(String name) {
    NeoConnection neo = new NeoConnection();
    neo.setName(name);
    neo.setDialect(getGraphDialect());
    neo.setServer(server);
    neo.setDatabaseName(databaseName);
    neo.setBoltPort(boltPort);
    neo.setBrowserPort(browserPort);
    neo.setUsername(username);
    neo.setPassword(password);
    neo.setAutomatic(automatic);
    neo.setAutomaticVariable(automaticVariable);
    neo.setProtocol(protocol);
    neo.setRouting(routing);
    neo.setRoutingVariable(routingVariable);
    neo.setRoutingPolicy(routingPolicy);
    neo.setUsingEncryption(usingEncryption);
    neo.setUsingEncryptionVariable(usingEncryptionVariable);
    neo.setTrustAllCertificates(trustAllCertificates);
    neo.setTrustAllCertificatesVariable(trustAllCertificatesVariable);
    neo.setConnectionLivenessCheckTimeout(connectionLivenessCheckTimeout);
    neo.setMaxConnectionLifetime(maxConnectionLifetime);
    neo.setMaxConnectionPoolSize(maxConnectionPoolSize);
    neo.setConnectionAcquisitionTimeout(connectionAcquisitionTimeout);
    neo.setConnectionTimeout(connectionTimeout);
    neo.setMaxTransactionRetryTime(maxTransactionRetryTime);
    List<String> urls = new ArrayList<>();
    for (BoltManualUrl manualUrl : manualUrls) {
      urls.add(manualUrl.getUrl());
    }
    neo.setManualUrls(urls);
    return neo;
  }

  /** Copy the settings of a Neo4j connection into this Bolt database. */
  public void copyFrom(NeoConnection neo) {
    server = neo.getServer();
    databaseName = neo.getDatabaseName();
    boltPort = neo.getBoltPort();
    browserPort = neo.getBrowserPort();
    username = neo.getUsername();
    password = neo.getPassword();
    automatic = neo.isAutomatic();
    automaticVariable = neo.getAutomaticVariable();
    protocol = neo.getProtocol();
    routing = neo.isRouting();
    routingVariable = neo.getRoutingVariable();
    routingPolicy = neo.getRoutingPolicy();
    usingEncryption = neo.isUsingEncryption();
    usingEncryptionVariable = neo.getUsingEncryptionVariable();
    trustAllCertificates = neo.isTrustAllCertificates();
    trustAllCertificatesVariable = neo.getTrustAllCertificatesVariable();
    connectionLivenessCheckTimeout = neo.getConnectionLivenessCheckTimeout();
    maxConnectionLifetime = neo.getMaxConnectionLifetime();
    maxConnectionPoolSize = neo.getMaxConnectionPoolSize();
    connectionAcquisitionTimeout = neo.getConnectionAcquisitionTimeout();
    connectionTimeout = neo.getConnectionTimeout();
    maxTransactionRetryTime = neo.getMaxTransactionRetryTime();
    manualUrls = new ArrayList<>();
    if (neo.getManualUrls() != null) {
      for (String url : neo.getManualUrls()) {
        manualUrls.add(new BoltManualUrl(url));
      }
    }
  }

  @Override
  public IGraphConnection connect(ILogChannel log, IVariables variables, String connectionName)
      throws HopException {
    NeoConnection neo = toNeoConnection(connectionName);
    Driver driver = neo.getDriver(log, variables);
    try {
      return new BoltGraphConnection(
          driver, neo.getSession(log, driver, variables), getGraphDialect(), log);
    } catch (RuntimeException e) {
      driver.close();
      throw new HopException("Unable to open a session on " + connectionName, e);
    }
  }

  @Override
  public String test(IVariables variables, String connectionName) throws HopException {
    NeoConnection neo = toNeoConnection(connectionName);
    neo.test(variables);
    return neo.getUrl(variables);
  }
}
