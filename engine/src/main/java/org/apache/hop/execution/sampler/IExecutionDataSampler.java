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
 *
 */

package org.apache.hop.execution.sampler;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.metadata.api.HopMetadataObject;
import org.apache.hop.metadata.api.IHopMetadataObjectFactory;
import org.apache.hop.pipeline.transform.stream.IStream;

/** This describes methods for sampling data from */
@HopMetadataObject(objectFactory = IExecutionDataSampler.ExecutionDataSamplerObjectFactory.class)
public interface IExecutionDataSampler<Store extends IExecutionDataSamplerStore> extends Cloneable {

  String getPluginId();

  void setPluginId(String pluginId);

  String getPluginName();

  void setPluginName(String pluginName);

  IExecutionDataSampler<Store> clone();

  /**
   * Create a sampler store to match the sampler. It will allow you to store your intermediate
   * sampler results.
   *
   * @param samplerMeta Metadata about the transform we're sampling for.
   * @return A new sampler store instance.
   */
  Store createSamplerStore(ExecutionDataSamplerMeta samplerMeta);

  /**
   * Sample one row in a stream or rows. It's up to the plugin to know what to do with it.
   *
   * @param samplerStore A place to store the samples and intermediate results
   * @param streamType The type of stream we're sampling from. (INPUT, OUTPUT, READ, WRITTEN, ...)
   * @param rowMeta The row metadata
   * @param row The row data itself
   */
  void sampleRow(Store samplerStore, IStream.StreamType streamType, IRowMeta rowMeta, Object[] row)
      throws HopException;

  /**
   * This object factory is needed to instantiate the correct plugin class based on the value of the
   * Object ID which is simply the object ID.
   */
  final class ExecutionDataSamplerObjectFactory implements IHopMetadataObjectFactory {

    @Override
    public Object createObject(String id, Object parentObject) throws HopException {
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin plugin = registry.findPluginWithId(ExecutionDataSamplerPluginType.class, id);
      IExecutionDataSampler<?> sampler = registry.loadClass(plugin, IExecutionDataSampler.class);
      sampler.setPluginId(plugin.getIds()[0]);
      sampler.setPluginName(plugin.getName());
      return sampler;
    }

    @Override
    public String getObjectId(Object object) throws HopException {
      if (!(object instanceof IExecutionDataSampler)) {
        throw new HopException(
            "Object is not of class IExecutionDataSampler but of "
                + object.getClass().getName()
                + "'");
      }
      return ((IExecutionDataSampler<?>) object).getPluginId();
    }
  }
}
