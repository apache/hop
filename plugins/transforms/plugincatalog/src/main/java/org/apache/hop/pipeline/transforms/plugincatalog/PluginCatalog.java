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
package org.apache.hop.pipeline.transforms.plugincatalog;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Plugin Catalog transform - a row-generating source that reflects the live Hop plugin registry
 * into structured rows describing every transform, action and metadata type.
 */
public class PluginCatalog extends BaseTransform<PluginCatalogMeta, PluginCatalogData> {

  public PluginCatalog(
      TransformMeta transformMeta,
      PluginCatalogMeta meta,
      PluginCatalogData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    data.outputRowMeta = new RowMeta();
    meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, getMetadataProvider());

    PluginCatalogReader reader = new PluginCatalogReader();
    List<PluginRecord> plugins =
        reader.readAll(
            meta.isIncludeTransforms(),
            meta.isIncludeActions(),
            meta.isIncludeMetadataTypes(),
            (message, error) ->
                logDetailed(error == null ? message : message + ": " + error.getMessage()));

    long rowCount = 0;
    for (PluginRecord plugin : plugins) {
      if (meta.getDetailLevel() == DetailLevel.PER_PROPERTY) {
        rowCount += emitPerProperty(plugin);
      } else {
        emitPerPlugin(plugin);
        rowCount++;
      }
    }

    logBasic("Plugin Catalog emitted " + rowCount + " row(s) for " + plugins.size() + " plugin(s)");
    setOutputDone();
    return false;
  }

  private void emitPerPlugin(PluginRecord plugin) throws HopException {
    Object[] row = baseRow(plugin);
    row[PluginCatalogMeta.BASE_FIELD_COUNT] =
        PluginCatalogReader.propertiesToJson(plugin.properties);
    putRow(data.outputRowMeta, row);
  }

  private long emitPerProperty(PluginRecord plugin) throws HopException {
    if (plugin.properties.isEmpty()) {
      // Still surface the plugin itself, with empty property columns.
      Object[] row = baseRow(plugin);
      putRow(data.outputRowMeta, row);
      return 1;
    }
    for (PropertyRecord property : plugin.properties) {
      Object[] row = baseRow(plugin);
      int i = PluginCatalogMeta.BASE_FIELD_COUNT;
      row[i] = property.field();
      row[i + 1] = property.xmlKey();
      row[i + 2] = property.javaType();
      row[i + 3] = property.password();
      row[i + 4] = property.group();
      row[i + 5] = property.groupKey();
      putRow(data.outputRowMeta, row);
    }
    return plugin.properties.size();
  }

  private Object[] baseRow(PluginRecord plugin) {
    Object[] row = new Object[data.outputRowMeta.size()];
    row[0] = plugin.pluginId;
    row[1] = plugin.pluginType;
    row[2] = plugin.name;
    row[3] = plugin.description;
    row[4] = plugin.category;
    row[5] = plugin.keywords;
    row[6] = plugin.className;
    row[7] = plugin.englishAliases;
    row[8] = plugin.locale;
    return row;
  }
}
