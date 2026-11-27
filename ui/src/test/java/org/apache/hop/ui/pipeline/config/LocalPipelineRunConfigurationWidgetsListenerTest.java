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

package org.apache.hop.ui.pipeline.config;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.junit.jupiter.api.Test;

/**
 * The run configuration editor builds an engine's widgets listener from the class name the engine
 * gives, with a constructor taking the run configuration: the local engine's class must exist and
 * be built that way.
 */
class LocalPipelineRunConfigurationWidgetsListenerTest {

  @Test
  void theEditorCanBuildTheListenerTheLocalEngineNames() throws Exception {
    LocalPipelineRunConfiguration configuration = new LocalPipelineRunConfiguration();
    String className = configuration.getGuiWidgetsListenerClassName();
    assertNotNull(className);

    Object listener =
        configuration
            .getClass()
            .getClassLoader()
            .loadClass(className)
            .getConstructor(IPipelineEngineRunConfiguration.class)
            .newInstance(configuration);
    assertTrue(listener instanceof IGuiPluginCompositeWidgetsListener);
  }
}
