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

package org.apache.hop.gremlin;

import java.util.ArrayList;
import java.util.List;
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
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV2;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;

/**
 * A Gremlin server: Apache TinkerPop Gremlin Server, JanusGraph or Amazon Neptune, over the
 * TinkerPop driver.
 */
@GraphDatabasePlugin(
    id = "GREMLIN",
    name = "i18n::GremlinGraphDatabase.name",
    description = "i18n::GremlinGraphDatabase.description",
    documentationUrl = "/metadata-types/graphs/graph-database-connection.html")
@GuiPlugin
@Getter
@Setter
public class GremlinGraphDatabase extends BaseGraphDatabase {
  private static final String GROUP = "i18n::GremlinGraphDatabase.Group.Connection";

  public static final String SERIALIZER_GRAPHBINARY = "GraphBinary";
  public static final String SERIALIZER_GRAPHSON3 = "GraphSON 3";
  public static final String SERIALIZER_GRAPHSON2 = "GraphSON 2";

  @GuiWidgetElement(
      id = "hostnames",
      order = "0010",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::GremlinGraphDatabase.hostnames.Label",
      toolTip = "i18n::GremlinGraphDatabase.hostnames.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private String hostnames;

  @GuiWidgetElement(
      id = "port",
      order = "0020",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::GremlinGraphDatabase.port.Label",
      toolTip = "i18n::GremlinGraphDatabase.port.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private String port;

  @GuiWidgetElement(
      id = "traversalSource",
      order = "0030",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::GremlinGraphDatabase.traversalSource.Label",
      toolTip = "i18n::GremlinGraphDatabase.traversalSource.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private String traversalSource;

  @GuiWidgetElement(
      id = "username",
      order = "0040",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::GremlinGraphDatabase.username.Label",
      toolTip = "i18n::GremlinGraphDatabase.username.Tooltip",
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
      label = "i18n::GremlinGraphDatabase.password.Label",
      toolTip = "i18n::GremlinGraphDatabase.password.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty(password = true)
  private String password;

  @GuiWidgetElement(
      id = "usingTls",
      order = "0060",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::GremlinGraphDatabase.usingTls.Label",
      toolTip = "i18n::GremlinGraphDatabase.usingTls.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private boolean usingTls;

  @GuiWidgetElement(
      id = "serializer",
      order = "0070",
      parentId = GraphDatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      comboValuesMethod = "getSerializerNames",
      variables = false,
      label = "i18n::GremlinGraphDatabase.serializer.Label",
      toolTip = "i18n::GremlinGraphDatabase.serializer.Tooltip",
      groupType = GuiWidgetGroupType.BOXES,
      group = GROUP)
  @HopMetadataProperty
  private String serializer;

  public GremlinGraphDatabase() {
    hostnames = "localhost";
    port = "8182";
    traversalSource = "g";
    serializer = SERIALIZER_GRAPHBINARY;
  }

  /** The names of the serializers for the serializer combo. */
  public List<String> getSerializerNames(ILogChannel log, IHopMetadataProvider metadataProvider) {
    return List.of(SERIALIZER_GRAPHBINARY, SERIALIZER_GRAPHSON3, SERIALIZER_GRAPHSON2);
  }

  @Override
  public IGraphDialect getGraphDialect() {
    return GremlinGraphDialect.INSTANCE;
  }

  /** The address of the server, for messages. */
  public String getUrl(IVariables variables) {
    return (usingTls ? "wss://" : "ws://")
        + variables.resolve(hostnames)
        + ":"
        + variables.resolve(port)
        + "/gremlin, traversal source "
        + variables.resolve(traversalSource);
  }

  /**
   * The serializer, with the JanusGraph types registered: JanusGraph sends its own ids and geo
   * shapes, which other servers don't use.
   */
  private MessageSerializer<?> getMessageSerializer() {
    if (SERIALIZER_GRAPHSON3.equals(serializer) || SERIALIZER_GRAPHSON2.equals(serializer)) {
      boolean v3 = SERIALIZER_GRAPHSON3.equals(serializer);
      GraphSONMapper.Builder mapper =
          GraphSONMapper.build()
              .version(v3 ? GraphSONVersion.V3_0 : GraphSONVersion.V2_0)
              .addRegistry(JanusGraphIoRegistry.instance());
      return v3 ? new GraphSONMessageSerializerV3(mapper) : new GraphSONMessageSerializerV2(mapper);
    }
    TypeSerializerRegistry registry =
        TypeSerializerRegistry.build().addRegistry(JanusGraphIoRegistry.instance()).create();
    return new GraphBinaryMessageSerializerV1(registry);
  }

  @Override
  public IGraphConnection connect(ILogChannel log, IVariables variables, String connectionName)
      throws HopException {
    List<String> hosts = new ArrayList<>();
    for (String host : Const.NVL(variables.resolve(hostnames), "localhost").split(",")) {
      if (StringUtils.isNotBlank(host)) {
        hosts.add(host.trim());
      }
    }
    String source = Const.NVL(variables.resolve(traversalSource), "g");
    Cluster.Builder builder =
        Cluster.build()
            .addContactPoints(hosts.toArray(new String[0]))
            .port(Const.toInt(variables.resolve(port), 8182))
            .serializer(getMessageSerializer())
            .enableSsl(usingTls);
    String realUsername = variables.resolve(username);
    if (StringUtils.isNotEmpty(realUsername)) {
      String realPassword = variables.resolve(password);
      builder.credentials(
          realUsername,
          StringUtils.isEmpty(realPassword)
              ? ""
              : Encr.decryptPasswordOptionallyEncrypted(realPassword));
    }
    Cluster cluster = null;
    try {
      cluster = builder.create();
      Client client = cluster.connect().alias(source);
      client.init();
      GraphTraversalSource g =
          AnonymousTraversalSource.traversal()
              .withRemote(DriverRemoteConnection.using(cluster, source));
      return new GremlinGraphConnection(cluster, client, g, log);
    } catch (Exception e) {
      if (cluster != null) {
        cluster.close();
      }
      throw new HopException(
          "Unable to connect to the Gremlin server at "
              + getUrl(variables)
              + " for "
              + connectionName,
          e);
    }
  }

  @Override
  public String test(IVariables variables, String connectionName) throws HopException {
    try (IGraphConnection connection = connect(null, variables, connectionName)) {
      connection.execute(
          Const.NVL(variables.resolve(traversalSource), "g") + ".inject(0)", Map.of());
    }
    return getUrl(variables);
  }
}
