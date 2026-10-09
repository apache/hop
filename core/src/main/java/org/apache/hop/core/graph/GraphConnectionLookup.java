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

package org.apache.hop.core.graph;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;

/**
 * Find a graph connection by name in any of the metadata types holding one: the metadata types
 * implementing {@link IGraphDatabaseMetaConvertible}, like the deprecated Neo4j connection, come
 * first so a name resolves to the same connection as in the Neo4j transforms and actions. Then the
 * graph database connections.
 */
public final class GraphConnectionLookup {

  private GraphConnectionLookup() {}

  /** A graph connection found by name and the key of the metadata type it is stored as. */
  public record Found(GraphDatabaseMeta graphDatabaseMeta, String metadataKey) {}

  /**
   * @return The connection or null if there is no graph connection with that name
   */
  public static Found find(IHopMetadataProvider metadataProvider, String name) throws HopException {
    if (metadataProvider == null || StringUtils.isEmpty(name)) {
      return null;
    }
    List<Class<IHopMetadata>> metadataClasses = metadataProvider.getMetadataClasses();
    for (Class<IHopMetadata> metadataClass : metadataClasses) {
      if (!IGraphDatabaseMetaConvertible.class.isAssignableFrom(metadataClass)) {
        continue;
      }
      IHopMetadataSerializer<IHopMetadata> serializer =
          metadataProvider.getSerializer(metadataClass);
      if (!serializer.exists(name)) {
        continue;
      }
      IHopMetadata legacy = serializer.load(name);
      if (legacy instanceof IGraphDatabaseMetaConvertible convertible) {
        return new Found(convertible.toGraphDatabaseMeta(), keyOf(metadataClass));
      }
    }
    GraphDatabaseMeta graphDatabaseMeta = GraphDatabaseMeta.load(metadataProvider, name);
    if (graphDatabaseMeta != null) {
      return new Found(graphDatabaseMeta, keyOf(GraphDatabaseMeta.class));
    }
    return null;
  }

  /**
   * @throws HopException if there is no graph connection with that name
   */
  public static Found get(IHopMetadataProvider metadataProvider, String name) throws HopException {
    if (StringUtils.isEmpty(name)) {
      throw new HopException("Please specify the name of a graph database connection");
    }
    Found found = find(metadataProvider, name);
    if (found == null) {
      throw new HopException("Unable to find graph database connection '" + name + "'");
    }
    return found;
  }

  private static String keyOf(Class<?> metadataClass) {
    HopMetadata annotation = metadataClass.getAnnotation(HopMetadata.class);
    return annotation == null ? metadataClass.getSimpleName() : annotation.key();
  }
}
