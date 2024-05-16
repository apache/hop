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

package org.apache.hop.workflow.actions.movefiles;

import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.mockito.Mockito;

public class MoveFilesActionHelper {

  public static ActionMoveFiles defaultAction() {
    ActionMoveFiles action = Mockito.spy(new ActionMoveFiles());
    IWorkflowEngine<WorkflowMeta> parentWorkflow = new LocalWorkflowEngine();

    action.setLogLevel(LogLevel.BASIC);
    action.setParentWorkflow(parentWorkflow);
    action.setName("Test move same container");
    action.setIncludeSubfolders(false);
    action.setAddDate(false);
    action.setMoveEmptyFolders(false);
    action.setSimulate(false);
    action.setArgFromPrevious(false);
    action.allocate(1);
    action.setDestinationIsAFile(true);
    return action;
  }
}
