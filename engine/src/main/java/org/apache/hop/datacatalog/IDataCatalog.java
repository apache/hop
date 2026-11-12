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

package org.apache.hop.datacatalog;

import java.util.Optional;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.datacatalog.plugin.DataCatalogPluginType;
import org.apache.hop.metadata.api.HopMetadataObject;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * A data catalog Hop reads dataset metadata from, such as DataHub. The catalog stays the owner of
 * that metadata: Hop doesn't change it. Implementations are {@link
 * org.apache.hop.datacatalog.plugin.DataCatalogPlugin} plugins and implement the {@link
 * ICatalogCapability} interfaces for what they can answer.
 *
 * <p>The instance in a {@link DataCatalog} holds the configuration. To use it, take an initialized
 * copy with {@link DataCatalog#connect(IVariables, IHopMetadataProvider)} and close it afterwards.
 * Experimental.
 */
@HopMetadataObject(objectFactory = IDataCatalog.DataCatalogObjectFactory.class)
public interface IDataCatalog extends Cloneable, AutoCloseable {

  String getPluginId();

  void setPluginId(String pluginId);

  String getPluginName();

  void setPluginName(String pluginName);

  IDataCatalog clone();

  /**
   * Resolve the configuration and prepare to answer questions. Called on a copy, once, before any
   * other question.
   */
  void initialize(IVariables variables, IHopMetadataProvider metadataProvider) throws HopException;

  /**
   * Whether the catalog can be reached with this configuration, as a line to show to the user, for
   * example the catalog version and the user Hop reads as.
   */
  CatalogAnswer<String> ping();

  /** The catalog's id of a dataset. Not found when the catalog doesn't have it. */
  CatalogAnswer<CatalogEntityId> resolve(DatasetRef dataset);

  /** One of the capabilities of this catalog, or empty when it doesn't have it. */
  default <T extends ICatalogCapability> Optional<T> capability(Class<T> type) {
    return type.isInstance(this) ? Optional.of(type.cast(this)) : Optional.empty();
  }

  /** Release what {@link #initialize} opened. Never throws. */
  @Override
  void close();

  /** Creates the plugin class stored under a plugin id in the metadata. */
  final class DataCatalogObjectFactory implements IHopMetadataObjectFactory {

    @Override
    public Object createObject(String id, Object parentObject) throws HopException {
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin plugin = registry.findPluginWithId(DataCatalogPluginType.class, id);
      if (plugin == null) {
        throw new HopException(
            "The data catalog type '" + id + "' is not available: is its plugin installed?");
      }
      IDataCatalog catalog = (IDataCatalog) registry.loadClass(plugin);
      catalog.setPluginId(plugin.getIds()[0]);
      catalog.setPluginName(plugin.getName());
      return catalog;
    }

    @Override
    public String getObjectId(Object object) throws HopException {
      if (!(object instanceof IDataCatalog catalog)) {
        throw new HopException(
            "Object is not an IDataCatalog but a " + object.getClass().getName());
      }
      return catalog.getPluginId();
    }
  }
}
