/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.marketplace.catalog;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class OptionalPluginCatalogTest {

  @Test
  void registryLoadsWave1Plugins() {
    List<OptionalPluginInfo> plugins = OptionalPluginCatalog.listOptional();
    assertFalse(plugins.isEmpty());
    List<String> ids = OptionalPluginCatalog.artifactIds();
    assertTrue(ids.contains("hop-tech-parquet"));
    assertTrue(ids.contains("hop-engines-spark"));
    assertTrue(ids.contains("hop-engines-beam"));
    assertTrue(ids.contains("hop-transform-edi2xml"));
  }

  @Test
  void eachPluginHasInstallPath() {
    for (OptionalPluginInfo info : OptionalPluginCatalog.listOptional()) {
      assertTrue(
          info.getInstallPath() != null && info.getInstallPath().startsWith("plugins/"),
          "installPath for " + info.getArtifactId());
    }
  }
}
