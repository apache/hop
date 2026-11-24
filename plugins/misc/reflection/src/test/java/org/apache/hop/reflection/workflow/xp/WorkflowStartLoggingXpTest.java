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

package org.apache.hop.reflection.workflow.xp;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.workflow.engines.remote.RemoteWorkflowEngine;
import org.junit.jupiter.api.Test;

/**
 * Workflow Log runs where the workflow runs. For a remote run that is the Hop Server, which logs
 * the workflow itself: the client fires WorkflowStart too, and must not log the run a second time.
 */
class WorkflowStartLoggingXpTest {

  @Test
  void aRemoteRunIsNotLoggedOnTheClient() throws Exception {
    RemoteWorkflowEngine remote = mock(RemoteWorkflowEngine.class);

    new WorkflowStartLoggingXp()
        .callExtensionPoint(mock(ILogChannel.class), new Variables(), remote);

    verifyNoInteractions(remote);
  }
}
