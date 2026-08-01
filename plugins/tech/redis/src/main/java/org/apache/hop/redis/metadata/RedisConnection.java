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

package org.apache.hop.redis.metadata;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.redis.client.RedisClientFactory;
import org.apache.hop.redis.client.RedisClientSession;

@GuiPlugin
@HopMetadata(
    key = "redis-connection",
    name = "i18n::RedisConnection.name",
    description = "i18n::RedisConnection.description",
    image = "redis.svg",
    category = HopMetadataCategory.CONNECTIONS,
    documentationUrl = "/metadata-types/redis/redis-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.REDIS_CONNECTION,
    supportsGlobalReplace = true)
public class RedisConnection extends HopMetadataBase implements IHopMetadata {

  // --- Connection config widgets ---
  public static final String WIDGET_ID_DEPLOYMENT_MODE = "10000-deployment-mode";
  public static final String WIDGET_ID_HOSTNAME = "10100-hostname";
  public static final String WIDGET_ID_PORT = "10200-port";
  public static final String WIDGET_ID_DATABASE = "10300-database";
  public static final String WIDGET_ID_MASTER_NAME = "10400-master-name";
  public static final String WIDGET_ID_SENTINEL_NODES = "10500-sentinel-nodes";
  public static final String WIDGET_ID_CLUSTER_NODES = "10600-cluster-nodes";
  public static final String WIDGET_ID_USERNAME = "10700-username";
  public static final String WIDGET_ID_PASSWORD = "10800-password";
  public static final String WIDGET_ID_USE_SSL = "10900-use-ssl";
  public static final String WIDGET_ID_TIMEOUT_MS = "11000-timeout-ms";

  // --- Pool config widgets (shared by all deployment modes) ---
  public static final String WIDGET_ID_ENABLE_POOLING = "20000-enable-pooling";
  public static final String WIDGET_ID_POOL_MAX_TOTAL = "20100-pool-max-total";
  public static final String WIDGET_ID_POOL_MAX_IDLE = "20200-pool-max-idle";
  public static final String WIDGET_ID_POOL_MIN_IDLE = "20300-pool-min-idle";
  public static final String WIDGET_ID_POOL_MAX_WAIT_MS = "20400-pool-max-wait-ms";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_DEPLOYMENT_MODE,
      type = GuiElementType.COMBO,
      parentId = RedisConnectionEditor.CONNECTION_WIDGET_ID,
      label = "i18n::RedisMetadata.DeploymentMode.Label",
      toolTip = "i18n::RedisMetadata.DeploymentMode.ToolTip",
      variables = false)
  private RedisDeploymentMode deploymentMode = RedisDeploymentMode.STANDALONE;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_HOSTNAME,
      type = GuiElementType.TEXT,
      parentId = RedisConnectionEditor.CONNECTION_WIDGET_ID,
      label = "i18n::RedisMetadata.Hostname.Label",
      toolTip = "i18n::RedisMetadata.Hostname.ToolTip")
  private String hostname = "localhost";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_PORT,
      type = GuiElementType.TEXT,
      parentId = RedisConnectionEditor.CONNECTION_WIDGET_ID,
      label = "i18n::RedisMetadata.Port.Label",
      toolTip = "i18n::RedisMetadata.Port.ToolTip")
  private String port = "6379";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_DATABASE,
      type = GuiElementType.TEXT,
      parentId = RedisConnectionEditor.CONNECTION_WIDGET_ID,
      label = "i18n::RedisMetadata.Database.Label",
      toolTip = "i18n::RedisMetadata.Database.ToolTip")
  private String database = "0";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_MASTER_NAME,
      type = GuiElementType.TEXT,
      parentId = RedisConnectionEditor.CONNECTION_WIDGET_ID,
      label = "i18n::RedisMetadata.MasterName.Label",
      toolTip = "i18n::RedisMetadata.MasterName.ToolTip")
  private String masterName;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_SENTINEL_NODES,
      type = GuiElementType.TEXT,
      parentId = RedisConnectionEditor.CONNECTION_WIDGET_ID,
      label = "i18n::RedisMetadata.SentinelNodes.Label",
      toolTip = "i18n::RedisMetadata.SentinelNodes.ToolTip")
  private String sentinelNodes;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_CLUSTER_NODES,
      type = GuiElementType.TEXT,
      parentId = RedisConnectionEditor.CONNECTION_WIDGET_ID,
      label = "i18n::RedisMetadata.ClusterNodes.Label",
      toolTip = "i18n::RedisMetadata.ClusterNodes.ToolTip")
  private String clusterNodes;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_USERNAME,
      type = GuiElementType.TEXT,
      parentId = RedisConnectionEditor.CONNECTION_WIDGET_ID,
      label = "i18n::RedisMetadata.Username.Label",
      toolTip = "i18n::RedisMetadata.Username.ToolTip")
  private String username;

  @HopMetadataProperty(password = true)
  @GuiWidgetElement(
      id = WIDGET_ID_PASSWORD,
      type = GuiElementType.TEXT,
      password = true,
      parentId = RedisConnectionEditor.CONNECTION_WIDGET_ID,
      label = "i18n::RedisMetadata.Password.Label",
      toolTip = "i18n::RedisMetadata.Password.ToolTip")
  private String password;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_USE_SSL,
      type = GuiElementType.CHECKBOX,
      parentId = RedisConnectionEditor.CONNECTION_WIDGET_ID,
      label = "i18n::RedisMetadata.UseSsl.Label",
      toolTip = "i18n::RedisMetadata.UseSsl.ToolTip")
  private boolean useSsl;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_TIMEOUT_MS,
      type = GuiElementType.TEXT,
      parentId = RedisConnectionEditor.CONNECTION_WIDGET_ID,
      label = "i18n::RedisMetadata.TimeoutMs.Label",
      toolTip = "i18n::RedisMetadata.TimeoutMs.ToolTip")
  private String timeoutMs = "10000";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_ENABLE_POOLING,
      type = GuiElementType.CHECKBOX,
      parentId = RedisConnectionEditor.POOL_WIDGET_ID,
      label = "i18n::RedisMetadata.EnablePooling.Label",
      toolTip = "i18n::RedisMetadata.EnablePooling.ToolTip")
  private boolean enablePooling;

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_POOL_MAX_TOTAL,
      type = GuiElementType.TEXT,
      parentId = RedisConnectionEditor.POOL_WIDGET_ID,
      label = "i18n::RedisMetadata.PoolMaxTotal.Label",
      toolTip = "i18n::RedisMetadata.PoolMaxTotal.ToolTip")
  private String poolMaxTotal = "8";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_POOL_MAX_IDLE,
      type = GuiElementType.TEXT,
      parentId = RedisConnectionEditor.POOL_WIDGET_ID,
      label = "i18n::RedisMetadata.PoolMaxIdle.Label",
      toolTip = "i18n::RedisMetadata.PoolMaxIdle.ToolTip")
  private String poolMaxIdle = "8";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_POOL_MIN_IDLE,
      type = GuiElementType.TEXT,
      parentId = RedisConnectionEditor.POOL_WIDGET_ID,
      label = "i18n::RedisMetadata.PoolMinIdle.Label",
      toolTip = "i18n::RedisMetadata.PoolMinIdle.ToolTip")
  private String poolMinIdle = "0";

  @HopMetadataProperty
  @GuiWidgetElement(
      id = WIDGET_ID_POOL_MAX_WAIT_MS,
      type = GuiElementType.TEXT,
      parentId = RedisConnectionEditor.POOL_WIDGET_ID,
      label = "i18n::RedisMetadata.PoolMaxWaitMs.Label",
      toolTip = "i18n::RedisMetadata.PoolMaxWaitMs.ToolTip")
  private String poolMaxWaitMs = "-1";

  public RedisConnection() {}

  public RedisConnection(RedisConnection c) {
    super(c.name);
    this.deploymentMode = c.deploymentMode;
    this.hostname = c.hostname;
    this.port = c.port;
    this.database = c.database;
    this.masterName = c.masterName;
    this.sentinelNodes = c.sentinelNodes;
    this.clusterNodes = c.clusterNodes;
    this.username = c.username;
    this.password = c.password;
    this.useSsl = c.useSsl;
    this.timeoutMs = c.timeoutMs;
    this.enablePooling = c.enablePooling;
    this.poolMaxTotal = c.poolMaxTotal;
    this.poolMaxIdle = c.poolMaxIdle;
    this.poolMinIdle = c.poolMinIdle;
    this.poolMaxWaitMs = c.poolMaxWaitMs;
  }

  /**
   * Test this connection with a Redis PING.
   *
   * @param variables variables for resolving connection fields
   * @throws HopException if the connection or PING fails
   */
  public void test(IVariables variables) throws HopException {
    try (RedisClientSession session = RedisClientFactory.create(this, variables)) {
      String pong = session.getCommands().ping();
      if (!"PONG".equalsIgnoreCase(pong)) {
        throw new HopException("Unexpected Redis PING response: " + pong);
      }
    }
  }

  public RedisDeploymentMode getDeploymentMode() {
    return deploymentMode;
  }

  public void setDeploymentMode(RedisDeploymentMode deploymentMode) {
    this.deploymentMode = deploymentMode;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getPort() {
    return port;
  }

  public void setPort(String port) {
    this.port = port;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getMasterName() {
    return masterName;
  }

  public void setMasterName(String masterName) {
    this.masterName = masterName;
  }

  public String getSentinelNodes() {
    return sentinelNodes;
  }

  public void setSentinelNodes(String sentinelNodes) {
    this.sentinelNodes = sentinelNodes;
  }

  public String getClusterNodes() {
    return clusterNodes;
  }

  public void setClusterNodes(String clusterNodes) {
    this.clusterNodes = clusterNodes;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public boolean isUseSsl() {
    return useSsl;
  }

  public void setUseSsl(boolean useSsl) {
    this.useSsl = useSsl;
  }

  public String getTimeoutMs() {
    return timeoutMs;
  }

  public void setTimeoutMs(String timeoutMs) {
    this.timeoutMs = timeoutMs;
  }

  public boolean isEnablePooling() {
    return enablePooling;
  }

  public void setEnablePooling(boolean enablePooling) {
    this.enablePooling = enablePooling;
  }

  public String getPoolMaxTotal() {
    return poolMaxTotal;
  }

  public void setPoolMaxTotal(String poolMaxTotal) {
    this.poolMaxTotal = poolMaxTotal;
  }

  public String getPoolMaxIdle() {
    return poolMaxIdle;
  }

  public void setPoolMaxIdle(String poolMaxIdle) {
    this.poolMaxIdle = poolMaxIdle;
  }

  public String getPoolMinIdle() {
    return poolMinIdle;
  }

  public void setPoolMinIdle(String poolMinIdle) {
    this.poolMinIdle = poolMinIdle;
  }

  public String getPoolMaxWaitMs() {
    return poolMaxWaitMs;
  }

  public void setPoolMaxWaitMs(String poolMaxWaitMs) {
    this.poolMaxWaitMs = poolMaxWaitMs;
  }
}
