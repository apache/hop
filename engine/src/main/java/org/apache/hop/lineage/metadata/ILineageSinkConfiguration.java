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

package org.apache.hop.lineage.metadata;

import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.plugin.LineageSinkPluginType;
import org.apache.hop.lineage.spi.ILineageSink;
import org.apache.hop.metadata.api.HopMetadataObject;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * The configuration of one type of lineage sink, as a {@link LineageSink} in the project's metadata
 * holds it. Each {@link org.apache.hop.lineage.plugin.LineageSinkPlugin} that can be configured
 * this way gives one with {@link ILineageSink#newConfiguration()}; its options are annotated
 * {@code @GuiWidgetElement} fields with {@link LineageSink#GUI_PLUGIN_ELEMENT_PARENT_ID} as parent.
 */
@HopMetadataObject(objectFactory = ILineageSinkConfiguration.LineageSinkConfigurationFactory.class)
public interface ILineageSinkConfiguration extends Cloneable {

  String getPluginId();

  void setPluginId(String pluginId);

  String getPluginName();

  void setPluginName(String pluginName);

  ILineageSinkConfiguration clone();

  /**
   * The settings the sink reads in {@link ILineageSink#init}, by variable name, with the values
   * resolved against the variables of the run that uses the sink. Settings left empty are left out,
   * so the sink uses its defaults.
   *
   * @throws HopException when the configuration is incomplete, or something it refers to, such as a
   *     data catalog, can't be found
   */
  Map<String, String> toSettings(IVariables variables, IHopMetadataProvider metadataProvider)
      throws HopException;

  /** Creates the configuration of the lineage sink plugin stored under a plugin id. */
  final class LineageSinkConfigurationFactory implements IHopMetadataObjectFactory {

    @Override
    public Object createObject(String id, Object parentObject) throws HopException {
      return create(id);
    }

    /**
     * A new configuration of the lineage sink with this plugin id.
     *
     * @throws HopException when the sink isn't installed or can't be configured in metadata
     */
    public static ILineageSinkConfiguration create(String id) throws HopException {
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin plugin = registry.findPluginWithId(LineageSinkPluginType.class, id);
      if (plugin == null) {
        throw new HopException(
            "The lineage sink type '" + id + "' is not available: is its plugin installed?");
      }
      ILineageSinkConfiguration configuration =
          registry.loadClass(plugin, ILineageSink.class).newConfiguration();
      if (configuration == null) {
        throw new HopException(
            "The lineage sink type '"
                + id
                + "' can only be configured with variables, not as metadata");
      }
      configuration.setPluginId(plugin.getIds()[0]);
      configuration.setPluginName(plugin.getName());
      return configuration;
    }

    @Override
    public String getObjectId(Object object) throws HopException {
      if (!(object instanceof ILineageSinkConfiguration configuration)) {
        throw new HopException(
            "Object is not an ILineageSinkConfiguration but a " + object.getClass().getName());
      }
      return configuration.getPluginId();
    }
  }
}
