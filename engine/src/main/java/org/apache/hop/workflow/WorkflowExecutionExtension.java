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

package org.apache.hop.workflow;

import org.apache.hop.core.Result;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

public class WorkflowExecutionExtension {

  public IWorkflowEngine<WorkflowMeta> workflow;
  public Result result;
  public ActionMeta actionMeta;
  public boolean executeAction;

  public WorkflowExecutionExtension( IWorkflowEngine<WorkflowMeta> workflow, Result result, ActionMeta actionMeta, boolean executeAction ) {
    super();
    this.workflow = workflow;
    this.result = result;
    this.actionMeta = actionMeta;
    this.executeAction = executeAction;
  }
}
