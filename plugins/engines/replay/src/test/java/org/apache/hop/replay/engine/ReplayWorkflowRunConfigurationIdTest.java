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

package org.apache.hop.replay.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.workflow.engine.WorkflowEnginePlugin;
import org.junit.jupiter.api.Test;

/**
 * A run configuration created in code names its engine, so it can be saved and read back. The
 * replay configuration extends the local one, and still names the replay engine.
 */
class ReplayWorkflowRunConfigurationIdTest {

  @Test
  void theDefaultRunConfigurationNamesItsEngine() {
    WorkflowEnginePlugin engine =
        ReplayWorkflowEngine.class.getAnnotation(WorkflowEnginePlugin.class);

    ReplayWorkflowRunConfiguration configuration = new ReplayWorkflowRunConfiguration();

    assertEquals(engine.id(), configuration.getEnginePluginId());
    assertEquals(engine.name(), configuration.getEnginePluginName());
  }
}
