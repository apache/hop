package org.apache.hop.ui.hopgui.file.pipeline;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.pipeline.PipelineMeta;
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
  id = "HopFile-Pipeline-Plugin",
  description = "The pipeline file information for the Hop GUI"
)
public class HopPipelineFileType<T extends PipelineMeta> extends HopFileTypeBase<T> implements IHopFileType<T> {

  public HopPipelineFileType() {
  }

  @Override public String getName() {
    return "Pipeline"; // TODO: i18n?
  }

  @Override public String[] getFilterExtensions() {
    return new String[] { "*.hpl" };
  }

  @Override public String[] getFilterNames() {
    return new String[] { "Pipelines" };
  }

  public Properties getCapabilities() {
    Properties capabilities = new Properties();
    capabilities.setProperty( IHopFileType.CAPABILITY_NEW, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_START, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_STOP, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_SAVE, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_PAUSE, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_PREVIEW, "true" );
    capabilities.setProperty( IHopFileType.CAPABILITY_DEBUG, "true" );

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

      // Load the pipeline
      //
      PipelineMeta pipelineMeta = new PipelineMeta( filename, hopGui.getMetaStore(), true, parentVariableSpace );

      // Pass the MetaStore for reference lookups
      //
      pipelineMeta.setMetaStore( hopGui.getMetaStore() );

      // Show it in the perspective
      //
      return perspective.addPipeline( perspective.getTabFolder(), hopGui, pipelineMeta, this );
    } catch ( Exception e ) {
      throw new HopException( "Error opening pipeline file '" + filename + "'", e );
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
      PipelineMeta pipelineMeta = new PipelineMeta( parentVariableSpace );
      pipelineMeta.setName( "New pipeline" );

      // Pass the MetaStore for reference lookups
      //
      pipelineMeta.setMetaStore( hopGui.getMetaStore() );

      // Show it in the perspective
      //
      return perspective.addPipeline( perspective.getTabFolder(), hopGui, pipelineMeta, this );
    } catch ( Exception e ) {
      throw new HopException( "Error creating new pipeline", e );
    }
  }

  @Override public boolean isHandledBy( String filename, boolean checkContent ) throws HopException {
    try {
      if ( checkContent ) {
        Document document = XMLHandler.loadXMLFile( filename );
        Node pipelineNode = XMLHandler.getSubNode( document, PipelineMeta.XML_TAG );
        return pipelineNode != null;
      } else {
        return super.isHandledBy( filename, checkContent );
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to verify file handling of file '" + filename + "'", e );
    }
  }

  @Override public boolean supportsFile( IHasFilename metaObject ) {
    return metaObject instanceof PipelineMeta;
  }

  public static final String ACTION_ID_NEW_PIPELINE = "NewPipeline";

  @Override public List<IGuiContextHandler> getContextHandlers() {

    HopGui hopGui = HopGui.getInstance();

    List<IGuiContextHandler> handlers = new ArrayList<>();
    handlers.add( new IGuiContextHandler() {
      @Override public List<GuiAction> getSupportedActions() {
        List<GuiAction> actions = new ArrayList<>();

        GuiAction newAction = new GuiAction( ACTION_ID_NEW_PIPELINE, GuiActionType.Create, "New pipeline", "Create a new pipeline", "ui/images/TRN.svg",
          ( shiftClicked, controlClicked, parameters ) -> {
            try {
              HopPipelineFileType.this.newFile( hopGui, hopGui.getVariableSpace() );
            } catch ( Exception e ) {
              new ErrorDialog( hopGui.getShell(), "Error", "Error creating new pipeline", e );
            }
          } );
        actions.add( newAction );

        return actions;
      }
    } );
    return handlers;
  }
}
