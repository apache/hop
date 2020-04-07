package org.apache.hop.ui.hopgui.file.workflow;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.laf.BasePropertyHandler;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypeBase;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypePlugin;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@HopFileTypePlugin(
  id = "HopFile-Workflow-Plugin",
  description = "The workflow file information for the Hop GUI"
)
public class HopWorkflowFileType<T extends WorkflowMeta> extends HopFileTypeBase<T> implements IHopFileType<T> {

  public HopWorkflowFileType() {
  }

  @Override public String getName() {
    return "Workflow"; // TODO: i18n
  }

  @Override public String[] getFilterExtensions() {
    return new String[] { "*.hwf" };
  }

  @Override public String[] getFilterNames() {
    return new String[] { "Workflows" };
  }

  public Properties getCapabilities() {
    Properties capabilities = new Properties();
    capabilities.setProperty( IHopFileType.CAPABILITY_NEW, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_START, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_STOP, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_SAVE, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_PAUSE, "false" );
    capabilities.setProperty( IHopFileType.CAPABILITY_PREVIEW, "false" );
    capabilities.setProperty( IHopFileType.CAPABILITY_DEBUG, "false" );

    capabilities.setProperty( IHopFileType.CAPABILITY_COPY, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_PASTE, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_CUT, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_DELETE, "true" );

    capabilities.setProperty( IHopFileType.CAPABILITY_FILE_HISTORY, "true" );

    return capabilities;
  }

  @Override public IHopFileTypeHandler openFile( HopGui hopGui, String filename, IVariables parentVariableSpace ) throws HopException {
    try {
      // This file is opened in the data orchestration perspective
      //
      HopDataOrchestrationPerspective perspective = HopDataOrchestrationPerspective.getInstance();
      perspective.activate();

      // Load the workflow from file
      //
      WorkflowMeta workflowMeta = new WorkflowMeta( parentVariableSpace, filename, hopGui.getMetaStore() );

      // Pass the MetaStore for reference lookups
      //
      workflowMeta.setMetaStore( hopGui.getMetaStore() );

      // Show it in the perspective
      //
      return perspective.addWorkflow( perspective.getTabFolder(), hopGui, workflowMeta, this );
    } catch ( Exception e ) {
      throw new HopException( "Error opening workflow file '" + filename + "'", e );
    }
  }

  @Override public IHopFileTypeHandler newFile( HopGui hopGui, IVariables parentVariableSpace ) throws HopException {
    try {
      // This file is created in the data orchestration perspective
      //
      HopDataOrchestrationPerspective perspective = HopDataOrchestrationPerspective.getInstance();
      perspective.activate();

      // Create the empty pipeline
      //
      WorkflowMeta workflowMeta = new WorkflowMeta();
      workflowMeta.setParentVariableSpace( parentVariableSpace );
      workflowMeta.setName( "New workflow" );

      // Pass the MetaStore for reference lookups
      //
      workflowMeta.setMetaStore( hopGui.getMetaStore() );

      // Show it in the perspective
      //
      return perspective.addWorkflow( perspective.getTabFolder(), hopGui, workflowMeta, this );
    } catch ( Exception e ) {
      throw new HopException( "Error creating new workflow", e );
    }
  }

  @Override public boolean isHandledBy( String filename, boolean checkContent ) throws HopException {
    try {
      if ( checkContent ) {
        Document document = XMLHandler.loadXMLFile( filename );
        Node workflowNode = XMLHandler.getSubNode( document, WorkflowMeta.XML_TAG );
        return workflowNode != null;
      } else {
        return super.isHandledBy( filename, checkContent );
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to verify file handling of file '" + filename + "'", e );
    }
  }

  @Override public boolean supportsFile( IHasFilename metaObject ) {
    return metaObject instanceof WorkflowMeta;
  }

  public static final String ACTION_ID_NEW_PIPELINE = "NewWorkflow";

  @Override public List<IGuiContextHandler> getContextHandlers() {

    HopGui hopGui = HopGui.getInstance();

    List<IGuiContextHandler> handlers = new ArrayList<>();
    handlers.add( new IGuiContextHandler() {
      @Override public List<GuiAction> getSupportedActions() {
        List<GuiAction> actions = new ArrayList<>();

        GuiAction newAction = new GuiAction( ACTION_ID_NEW_PIPELINE, GuiActionType.Create, "New workflow", "Create a new workflow",
          BasePropertyHandler.getProperty( "Workflow_image" ),
          ( shiftClicked, controlClicked, parameters ) -> {
            try {
              HopWorkflowFileType.this.newFile( hopGui, hopGui.getVariables() );
            } catch ( Exception e ) {
              new ErrorDialog( hopGui.getShell(), "Error", "Error creating new workflow", e );
            }
          } );
        actions.add( newAction );

        return actions;
      }
    } );
    return handlers;
  }
}
