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
package org.apache.hop.workflow.actions.workflow;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hop.core.Result;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.Test;

class ActionWorkflowRunnerTest {
  @Test
  void publishesCompletionWhenAlreadyStopped() {
    final IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    when(workflow.isStopped()).thenReturn(true);
    final var result = new Result();
    final var runner = new ActionWorkflowRunner(workflow, result, 0, mock(ILogChannel.class));

    runner.run();

    assertTrue(runner.isFinished());
    verify(workflow, never()).startExecution();
    verify(workflow).setResult(result);
    verify(workflow).setFinished(true);
  }

  @Test
  void publishesCompletionEvenWhenFinalSetterThrows() {
    final IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    final var result = new Result();
    when(workflow.startExecution()).thenReturn(result);
    final var failure = new IllegalStateException("setter failed");
    doThrow(failure).when(workflow).setFinished(true);
    final var runner = new ActionWorkflowRunner(workflow, new Result(), 0, mock(ILogChannel.class));

    assertSame(failure, assertThrows(IllegalStateException.class, runner::run));

    assertTrue(runner.isFinished());
    assertSame(result, runner.getResult());
  }

  @Test
  void publishesCompletionAndMarksWorkflowFinishedWhenResultPublicationThrows() {
    final IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    final var result = new Result();
    when(workflow.startExecution()).thenReturn(result);
    final var failure = new IllegalStateException("publication failed");
    doThrow(failure).when(workflow).setResult(result);
    final var runner = new ActionWorkflowRunner(workflow, new Result(), 0, mock(ILogChannel.class));

    assertSame(failure, assertThrows(IllegalStateException.class, runner::run));

    assertTrue(runner.isFinished());
    verify(workflow).setFinished(true);
  }
}
