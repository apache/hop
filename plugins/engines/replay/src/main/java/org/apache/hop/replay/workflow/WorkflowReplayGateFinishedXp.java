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

package org.apache.hop.replay.workflow;

import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.replay.ReplayGateUtil;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

@ExtensionPoint(
    id = "WorkflowReplayGateFinishedXp",
    description = "Drop sealed replay gates when the workflow completes successfully",
    extensionPointId = "WorkflowFinish")
public class WorkflowReplayGateFinishedXp
    implements IExtensionPoint<IWorkflowEngine<WorkflowMeta>> {

  @Override
  public void callExtensionPoint(
      ILogChannel log, IVariables variables, IWorkflowEngine<WorkflowMeta> workflow)
      throws HopException {
    if (workflow == null) {
      return;
    }
    Result result = workflow.getResult();
    boolean success =
        result != null
            && result.isResult()
            && result.getNrErrors() == 0
            && !result.isStopped()
            && !workflow.isStopped();
    if (!success) {
      return;
    }
    ReplayGateUtil.deleteExecutionSpool(log, workflow);
  }
}
