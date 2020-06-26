package org.apache.hop.ui.hopgui.search;

import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.search.ISearchableCallback;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopWorkflowFileType;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionCopy;

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
      perspective.show();

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
        workflowGraph = (HopGuiWorkflowGraph) perspective.addWorkflow( perspective.getComposite(), HopGui.getInstance(), workflowMeta, perspective.getWorkflowFileType() );
      }

      // Select and open the found action?
      //
      if (searchResult.getComponent()!=null) {
        ActionCopy actionCopy = workflowMeta.findAction( searchResult.getComponent(), 0 );
        if (actionCopy!=null) {
          actionCopy.setSelected( true );
          workflowGraph.editAction(actionCopy);
        }
      }
    };
  }
}
