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
package org.apache.hop.core.gui.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

class GuiPluginTypeTest {

  @GuiPlugin(classLoaderGroup = "vfs-hdfs")
  static class Grouped {}

  @GuiPlugin
  static class Ungrouped {}

  @Test
  void extractsClassLoaderGroup() throws Exception {
    Method extract =
        GuiPluginType.class.getDeclaredMethod("extractClassLoaderGroup", GuiPlugin.class);
    extract.setAccessible(true);
    GuiPluginType type = GuiPluginType.getInstance();
    assertEquals("vfs-hdfs", extract.invoke(type, Grouped.class.getAnnotation(GuiPlugin.class)));
    assertEquals("", extract.invoke(type, Ungrouped.class.getAnnotation(GuiPlugin.class)));
  }
}
