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
package org.apache.hop.vfs.hdfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.vfs.plugin.VfsPlugin;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.vfs.hdfs.metadata.HdfsMeta;
import org.apache.hop.vfs.hdfs.metadata.HdfsMetaEditor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class HdfsVfsPluginTest {

  @Test
  void namedConnectionsOnly() {
    HdfsVfsPlugin plugin = new HdfsVfsPlugin();
    assertEquals(0, plugin.getUrlSchemes().length);
    assertNull(plugin.getProvider());
  }

  @Test
  void sharesClassLoaderGroupWithMetadata() {
    String pluginGroup = HdfsVfsPlugin.class.getAnnotation(VfsPlugin.class).classLoaderGroup();
    String metadataGroup = HdfsMeta.class.getAnnotation(HopMetadata.class).classLoaderGroup();
    String guiGroup = HdfsMeta.class.getAnnotation(GuiPlugin.class).classLoaderGroup();
    String editorGroup = HdfsMetaEditor.class.getAnnotation(GuiPlugin.class).classLoaderGroup();
    assertEquals("vfs-hdfs", pluginGroup);
    assertEquals(pluginGroup, metadataGroup);
    assertEquals(pluginGroup, guiGroup);
    assertEquals(pluginGroup, editorGroup);
  }

  @Test
  void doesNotClaimHdfsScheme() {
    VfsPlugin annotation = HdfsVfsPlugin.class.getAnnotation(VfsPlugin.class);
    assertTrue(annotation.type().startsWith("hdfs"));
    HdfsVfsPlugin plugin = new HdfsVfsPlugin();
    for (String scheme : plugin.getUrlSchemes()) {
      assertTrue(!"hdfs".equals(scheme), "must not register hdfs:// as a Hop VFS scheme");
    }
  }
}
