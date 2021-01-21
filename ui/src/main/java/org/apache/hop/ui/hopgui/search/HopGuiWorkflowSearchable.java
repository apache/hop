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

package org.apache.hop.ui.hopgui.search;

import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableCallback;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

public class HopGuiWorkflowSearchable implements ISearchable<WorkflowMeta> {

  private String location;
  private WorkflowMeta workflowMeta;

  public HopGuiWorkflowSearchable( String location, WorkflowMeta workflowMeta ) {
    this.location = location;
    this.workflowMeta = workflowMeta;
  }

  @Override public String getLocation() {
    return location;
  }

  @Override public String getName() {
    return workflowMeta.getName();
  }

  @Override public String getType() {
    return HopWorkflowFileType.WORKFLOW_FILE_TYPE_DESCRIPTION;
  }

  @Override public String getFilename() {
    return workflowMeta.getFilename();
  }

  @Override public WorkflowMeta getSearchableObject() {
    return workflowMeta;
  }

  @Override public ISearchableCallback getSearchCallback() {
    return ( searchable, searchResult ) -> {
      HopDataOrchestrationPerspective perspective = HopGui.getDataOrchestrationPerspective();
      perspective.activate();

      HopGuiWorkflowGraph workflowGraph;

      // See if the same workflow isn't already open.
      // Other file types we might allow to open more than once but not workflows for now.
      //
      TabItemHandler tabItemHandlerWithFilename = perspective.findTabItemHandlerWithFilename( workflowMeta.getFilename() );
      if (tabItemHandlerWithFilename!=null) {
        // Same file so we can simply switch to it.
        // This will prevent confusion.
        //
        perspective.switchToTab( tabItemHandlerWithFilename );
        workflowGraph = (HopGuiWorkflowGraph) tabItemHandlerWithFilename.getTypeHandler();
      } else {
        workflowGraph = (HopGuiWorkflowGraph) perspective.addWorkflow( HopGui.getInstance(), workflowMeta, perspective.getWorkflowFileType() );
      }

      // Select and open the found action?
      //
      if (searchResult.getComponent()!=null) {
        ActionMeta action = workflowMeta.findAction( searchResult.getComponent() );
        if (action!=null) {
          action.setSelected( true );
          workflowGraph.editAction(action);
        }
      }
    };
  }
}
