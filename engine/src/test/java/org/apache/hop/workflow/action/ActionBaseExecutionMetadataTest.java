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

package org.apache.hop.workflow.action;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.actions.ActionFake;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.Test;

/**
 * An action resolves variables through the variable space it delegates to, not through itself. That
 * space has to reach the running workflow, or {@link
 * org.apache.hop.core.variables.IVariables#findExecutionMetadataProvider()} finds nothing and a
 * <code>#{variable-resolver:...}</code> expression in an action falls back to the process global
 * metadata instead of the metadata of this execution - the bundled metadata of an export on a Hop
 * Server (issue #8096).
 */
class ActionBaseExecutionMetadataTest {

  @Test
  void executionMetadataProviderIsFoundFromTheActionVariableSpace() {
    IHopMetadataProvider provider = mock(IHopMetadataProvider.class);

    @SuppressWarnings("unchecked")
    IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    when(workflow.getLogLevel()).thenReturn(LogLevel.BASIC);
    when(workflow.getMetadataProvider()).thenReturn(provider);

    ActionFake action = new ActionFake();
    assertNull(
        action.getVariables().findExecutionMetadataProvider(),
        "An action without a parent workflow has no execution metadata to offer");

    action.setParentWorkflow(workflow);

    // getVariables() is what resolve() delegates to, so it is the space
    // Variables#substituteVariableResolvers walks - asserting on the action itself would pass
    // even without the parent link, through ActionBase#getParentVariables().
    assertSame(provider, action.getVariables().findExecutionMetadataProvider());
    assertSame(provider, action.findExecutionMetadataProvider());
  }
}
