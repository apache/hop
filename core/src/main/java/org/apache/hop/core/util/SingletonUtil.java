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
package org.apache.hop.core.util;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;

import java.lang.reflect.Method;
import java.util.List;

public class SingletonUtil {
  public static final List<String> getValuesList( String guiPluginId, String singletonClassName, String methodName) throws HopException {
    try {
      PluginRegistry registry = PluginRegistry.getInstance();
      IPlugin guiPlugin = registry.getPlugin( GuiPluginType.class, guiPluginId );
      ClassLoader classLoader = registry.getClassLoader( guiPlugin );

      Class<?> singletonClass = classLoader.loadClass( singletonClassName );
      Method getInstanceMethod = singletonClass.getDeclaredMethod("getInstance", new Class[] { });
      Object singleton = getInstanceMethod.invoke( null, new Object[] {} );

      Method method = singletonClass.getMethod( methodName );
      List<String> values = (List<String>) method.invoke( singleton, new Object[] {} );

      return values;
    } catch ( Exception e ) {
      throw new HopException("Unable to get list of values from class "+singletonClassName+" with method "+methodName, e);
    }
  }
}
