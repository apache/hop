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

package org.apache.hop.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.plugins.JarCache;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.junit.jupiter.api.Test;

class HopEnvironmentInitForWorkerTest {

  @Test
  void initForWorkerRegistersThePluginsAndDropsTheScanCaches() throws Exception {
    HopEnvironment.initForWorker();

    assertTrue(HopEnvironment.isInitialized());
    // The plugins were registered from the indexes before the caches went
    assertFalse(PluginRegistry.getInstance().getPlugins(TransformPluginType.class).isEmpty());
    assertTrue(JarCache.getInstance().isEmpty(), "the plugin scan caches stay after start-up");
  }

  @Test
  void initForWorkerClearsTheCachesWhenHopIsAlreadyInitialized() throws Exception {
    HopEnvironment.init();
    // Something reads an index after start-up, as a client JVM can before it starts a run
    JarCache.getInstance().getNativeJars();

    HopEnvironment.initForWorker();

    assertTrue(JarCache.getInstance().isEmpty());
  }
}
