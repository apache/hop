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

package org.apache.hop.core.plugins;

import org.apache.hop.core.exception.HopPluginException;

import java.lang.annotation.Annotation;
import java.net.URL;
import java.util.List;

/**
 * This interface describes a plugin type.<br>
 * It expresses the ID and the name of the plugin type.<br>
 * Then it also explains what the plugin meta class is called and classes the plugin interface itself.<br>
 * It also explains us where to load plugins of this type.<br>
 *
 * @author matt
 */
public interface IPluginType<T extends Annotation> {

  /**
   * Register an additional class type to be managed by the plugin system.
   *
   * @param clz         category class, usually an interface
   * @param xmlNodeName xml node to search for a class name
   */
  void addObjectType( Class<?> clz, String xmlNodeName );

  /**
   * @return The ID of this plugin type
   */
  String getId();

  /**
   * @return The name of this plugin
   */
  String getName();

  /**
   * @throws HopPluginException
   */
  void searchPlugins() throws HopPluginException;

  /**
   * Handle an annotated plugin
   *
   * @param clazz            The class to use
   * @param annotation       The annotation to get information from
   * @param libraries        The libraries to add
   * @param nativePluginType Is this a native plugin?
   * @param pluginFolder     The plugin folder to use
   * @throws HopPluginException
   */
  void handlePluginAnnotation( Class<?> clazz, T annotation, List<String> libraries, boolean nativePluginType, URL pluginFolder ) throws HopPluginException;

  default boolean isFragment() {
    return false;
  }
}
