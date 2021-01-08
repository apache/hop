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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * This is a holder of Plugin Class mappings which supplement those of the stock Plugin.
 * <p>
 * Created by nbaker on 3/17/17.
 */
public class SupplementalPlugin extends Plugin implements IClassLoadingPlugin {
  Map<Class, Callable> factoryMap = new HashMap<>();
  private Class<? extends IPluginType> pluginClass;
  private String id;

  public SupplementalPlugin( Class<? extends IPluginType> pluginClass, String id ) {
    super( new String[] { id }, pluginClass, null, "", id, id, "", false, false, Collections.emptyMap(), Collections.emptyList(), "", new String[] {}, null, false );
    this.pluginClass = pluginClass;
    this.id = id;
  }

  @Override public <T> T loadClass( Class<T> pluginClass ) {
    if ( !factoryMap.containsKey( pluginClass ) ) {
      return null;
    }

    try {
      return (T) factoryMap.get( pluginClass ).call();
    } catch ( Exception e ) {
      throw new RuntimeException( new HopPluginException( "Error creating plugin class", e ) );
    }
  }

  @Override public ClassLoader getClassLoader() {
    return getClass().getClassLoader();
  }

  public <T> void addFactory( Class<T> tClass, Callable<T> callable ) {
    factoryMap.put( tClass, callable );
  }

}
