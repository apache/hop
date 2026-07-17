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

package org.apache.hop.workflow.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowRunConfiguration;
import org.apache.hop.workflow.engines.remote.RemoteWorkflowEngine;
import org.apache.hop.workflow.engines.remote.RemoteWorkflowRunConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class WorkflowEngineFactoryTest {

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    HopEnvironment.init();
  }

  /**
   * The parent logging object is how the log level is pushed down into the engine. A workflow that
   * runs on a server needs it as much as one that runs locally, or the server ends up logging at
   * Basic whatever level was asked for. See issue #3173.
   */
  @Test
  void remoteEngineKeepsTheParentLoggingObject() throws Exception {
    SimpleLoggingObject parent = createParent();

    RemoteWorkflowEngine engine =
        assertInstanceOf(
            RemoteWorkflowEngine.class,
            createEngine("Remote", new RemoteWorkflowRunConfiguration(), parent));

    assertSame(parent, engine.getParent());
    assertEquals(LogLevel.DEBUG, engine.getParent().getLogLevel());
  }

  /** The local engine has always been given the parent, this makes sure it keeps it. */
  @Test
  void localEngineKeepsTheParentLoggingObject() throws Exception {
    SimpleLoggingObject parent = createParent();

    LocalWorkflowEngine engine =
        assertInstanceOf(
            LocalWorkflowEngine.class,
            createEngine("Local", new LocalWorkflowRunConfiguration(), parent));

    assertSame(parent, engine.getParent());
  }

  private SimpleLoggingObject createParent() {
    SimpleLoggingObject parent =
        new SimpleLoggingObject("a-parent", LoggingObjectType.HOP_GUI, null);
    parent.setLogLevel(LogLevel.DEBUG);
    return parent;
  }

  /**
   * @param enginePluginId the id of the workflow engine plugin to run with, normally filled in when
   *     the run configuration is read from its metadata
   */
  private IWorkflowEngine<WorkflowMeta> createEngine(
      String enginePluginId,
      IWorkflowEngineRunConfiguration engineRunConfiguration,
      SimpleLoggingObject parent)
      throws HopException {
    engineRunConfiguration.setEnginePluginId(enginePluginId);

    String name = "a-" + enginePluginId + "-run-configuration";
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(WorkflowRunConfiguration.class)
        .save(new WorkflowRunConfiguration(name, "", null, engineRunConfiguration, false));

    return WorkflowEngineFactory.createWorkflowEngine(
        new Variables(), name, metadataProvider, new WorkflowMeta(), parent);
  }
}
