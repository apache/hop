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

package org.apache.hop.falkordb;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.BaseGraphDatabase;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.GraphDatabasePlugin;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** FalkorDB: Cypher over the Redis protocol, one graph per connection. */
@GraphDatabasePlugin(
    id = "FALKORDB",
    name = "i18n::FalkorDbGraphDatabase.name",
    description = "i18n::FalkorDbGraphDatabase.description",
    documentationUrl = "/metadata-types/graphs/graph-database-connection.html")
@GuiPlugin
@Getter
@Setter
public class FalkorDbGraphDatabase extends BaseGraphDatabase {
  private static final String GROUP = "i18n::FalkorDbGraphDatabase.Group.Connection";

  @GuiWidgetElement(
      id = "hostname",
      order = "0010",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::FalkorDbGraphDatabase.hostname.Label",
      toolTip = "i18n::FalkorDbGraphDatabase.hostname.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private String hostname;

  @GuiWidgetElement(
      id = "port",
      order = "0020",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::FalkorDbGraphDatabase.port.Label",
      toolTip = "i18n::FalkorDbGraphDatabase.port.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private String port;

  @GuiWidgetElement(
      id = "graphName",
      order = "0030",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::FalkorDbGraphDatabase.graphName.Label",
      toolTip = "i18n::FalkorDbGraphDatabase.graphName.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private String graphName;

  @GuiWidgetElement(
      id = "username",
      order = "0040",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::FalkorDbGraphDatabase.username.Label",
      toolTip = "i18n::FalkorDbGraphDatabase.username.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private String username;

  @GuiWidgetElement(
      id = "password",
      order = "0050",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      password = true,
      label = "i18n::FalkorDbGraphDatabase.password.Label",
      toolTip = "i18n::FalkorDbGraphDatabase.password.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty(password = true)
  private String password;

  @GuiWidgetElement(
      id = "usingTls",
      order = "0060",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::FalkorDbGraphDatabase.usingTls.Label",
      toolTip = "i18n::FalkorDbGraphDatabase.usingTls.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private boolean usingTls;

  public FalkorDbGraphDatabase() {
    hostname = "localhost";
    port = "6379";
    graphName = "hop";
  }

  @Override
  public IGraphDialect getGraphDialect() {
    return FalkorDbGraphDialect.INSTANCE;
  }

  /** The address of the server, for messages. */
  public String getUrl(IVariables variables) {
    return (usingTls ? "rediss://" : "redis://")
        + variables.resolve(hostname)
        + ":"
        + variables.resolve(port)
        + ", graph "
        + variables.resolve(graphName);
  }

  @Override
  public IGraphConnection connect(ILogChannel log, IVariables variables, String connectionName)
      throws HopException {
    String realGraphName = variables.resolve(graphName);
    if (StringUtils.isEmpty(realGraphName)) {
      throw new HopException(
          "Please specify the name of the graph in connection " + connectionName);
    }
    RedisURI.Builder builder =
        RedisURI.builder()
            .withHost(Const.NVL(variables.resolve(hostname), "localhost"))
            .withPort(Const.toInt(variables.resolve(port), 6379))
            .withSsl(usingTls);
    String realPassword = variables.resolve(password);
    if (StringUtils.isNotEmpty(realPassword)) {
      realPassword = Encr.decryptPasswordOptionallyEncrypted(realPassword);
      String realUsername = variables.resolve(username);
      if (StringUtils.isEmpty(realUsername)) {
        builder.withPassword(realPassword.toCharArray());
      } else {
        builder.withAuthentication(realUsername, realPassword);
      }
    }
    RedisClient client = FalkorDbClientResources.createClient(builder.build());
    try {
      StatefulRedisConnection<String, String> connection = client.connect();
      return new FalkorDbGraphConnection(client, connection, realGraphName, log);
    } catch (Exception e) {
      client.shutdown();
      throw new HopException(
          "Unable to connect to FalkorDB at " + getUrl(variables) + " for " + connectionName, e);
    }
  }

  @Override
  public String test(IVariables variables, String connectionName) throws HopException {
    try (IGraphConnection connection = connect(null, variables, connectionName)) {
      connection.execute("RETURN 0", Map.of());
    }
    return getUrl(variables);
  }
}
