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

package org.apache.hop.beam.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.IPluginType;
import org.apache.hop.core.plugins.Plugin;
import org.apache.hop.core.plugins.PluginMainClassType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.core.xml.XmlHandlerCache;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;

public class BeamHop {

  private static final Logger LOG = LoggerFactory.getLogger(BeamHop.class);

  public static final boolean isInitialized() {
    return HopEnvironment.isInitialized();
  }

  public static final void init() throws HopException {
    synchronized (PluginRegistry.getInstance()) {
      // Don't create hop config files everywhere...
      //
      System.setProperty(Const.HOP_AUTO_CREATE_CONFIG, "N");

      // Load Hop base plugins
      //
      HopEnvironment.init();

      XmlHandlerCache.getInstance();
    }
  }

  private static IPlugin findPlugin(
      PluginRegistry registry,
      Class<? extends IPluginType<?>> pluginTypeClass,
      String pluginClassName) {
    PluginMainClassType classType = pluginTypeClass.getAnnotation(PluginMainClassType.class);
    List<IPlugin> plugins = registry.getPlugins(pluginTypeClass);
    for (IPlugin plugin : plugins) {
      String mainClassName = plugin.getClassMap().get(classType.value());
      if (pluginClassName.equals(mainClassName)) {
        return plugin;
      }
    }
    return null;
  }

  public static IPlugin getTransformPluginForClass(Class<? extends ITransformMeta> metaClass) {
    Transform transformAnnotation = metaClass.getAnnotation(Transform.class);

    return new Plugin(
        new String[] {transformAnnotation.id()},
        TransformPluginType.class,
        metaClass,
        transformAnnotation.categoryDescription(),
        transformAnnotation.name(),
        transformAnnotation.description(),
        transformAnnotation.image(),
        transformAnnotation.isSeparateClassLoaderNeeded(),
        false,
        new HashMap<>(),
        new ArrayList<>(),
        transformAnnotation.documentationUrl(),
        transformAnnotation.keywords(),
        null,
        false);
  }

  public static Node getTransformXmlNode(TransformMeta transformMeta) throws HopException {
    String xml = transformMeta.getXml();
    return XmlHandler.getSubNode(XmlHandler.loadXmlString(xml), TransformMeta.XML_TAG);
  }

  public static void loadTransformMetadata(
      ITransformMeta meta,
      TransformMeta transformMeta,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta)
      throws HopException {
    meta.loadXml(getTransformXmlNode(transformMeta), metadataProvider);
    meta.searchInfoAndTargetTransforms(pipelineMeta.getTransforms());
  }
}
