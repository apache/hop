/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ai.provider;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopMissingPluginsException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;

/** Instantiates {@link IAiProvider} plugins by id when deserializing AI provider metadata. */
public class AiProviderObjectFactory implements IHopMetadataObjectFactory {

  @Override
  public Object createObject(String id, Object parentObject)
      throws HopException, HopMissingPluginsException {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin plugin = registry.findPluginWithId(AiProviderPluginType.class, id);
    if (plugin == null) {
      HopMissingPluginsException missing =
          new HopMissingPluginsException("AI provider plugin not found: " + id);
      missing.addMissingPluginDetails(AiProviderPluginType.class, id);
      throw missing;
    }
    return registry.loadClass(plugin);
  }

  @Override
  public String getObjectId(Object object) throws HopException {
    if (!(object instanceof IAiProvider provider)) {
      throw new HopException("Object is not an IAiProvider but " + object.getClass().getName());
    }
    return provider.getPluginId();
  }
}
