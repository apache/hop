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

package org.apache.hop.lineage.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.lineage.LineageRunLifecycleEmitter;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.workflow.Workflow;

@ExtensionPoint(
    id = "LineageHubWorkflowStartXp",
    extensionPointId = "WorkflowStart",
    description = "Emits lineage RUN_LIFECYCLE STARTED for workflow executions")
public class LineageHubWorkflowStartXp implements IExtensionPoint<Workflow> {
  @Override
  public void callExtensionPoint(ILogChannel log, IVariables variables, Workflow workflow)
      throws HopException {
    LineageRunLifecycleEmitter.emitWorkflow(workflow, RunLifecyclePhase.STARTED, null);
  }
}
