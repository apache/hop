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

package org.apache.hop.vfs.databricks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.databricks.metadata.DatabricksVfsConnection;
import org.junit.jupiter.api.Test;

class DatabricksVfsPluginTest {

  @Test
  void urlSchemesEmptyLikeMinio() {
    DatabricksVfsPlugin plugin = new DatabricksVfsPlugin();
    assertEquals(0, plugin.getUrlSchemes().length);
  }

  @Test
  void getProviderReturnsDatabricksProvider() {
    DatabricksVfsPlugin plugin = new DatabricksVfsPlugin();
    FileProvider provider = plugin.getProvider();
    assertNotNull(provider);
    assertTrue(provider instanceof DatabricksFileProvider);
  }

  @Test
  void getProvidersSurvivesMissingMetadata() {
    DatabricksVfsPlugin plugin = new DatabricksVfsPlugin();
    Map<String, FileProvider> providers = plugin.getProviders(new Variables());
    assertNotNull(providers);
  }

  @Test
  void fileProviderAcceptsVfsConnection() {
    DatabricksVfsConnection vfs = new DatabricksVfsConnection();
    vfs.setName("dbx-jars");
    vfs.setDatabricksConnectionName("prod-workspace");
    DatabricksFileProvider provider = new DatabricksFileProvider(new Variables(), vfs);
    assertNotNull(provider);
    assertTrue(
        provider.getCapabilities().contains(org.apache.commons.vfs2.Capability.READ_CONTENT));
  }
}
