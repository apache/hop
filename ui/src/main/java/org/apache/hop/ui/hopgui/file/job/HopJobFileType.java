package org.apache.hop.ui.hopgui.file.job;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.job.JobMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypeBase;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypeInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypePlugin;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@HopFileTypePlugin(
  id = "HopFile-Job-Plugin",
  description = "The job file information for the Hop GUI"
)
public class HopJobFileType<T extends JobMeta> extends HopFileTypeBase<T> implements HopFileTypeInterface<T> {

  public HopJobFileType() {
  }

  @Override public String getName() {
    return "Job"; // TODO: i18n
  }

  @Override public String[] getFilterExtensions() {
    return new String[] { "*.kjb" };
  }

  @Override public String[] getFilterNames() {
    return new String[] { "Jobs" };
  }

  public Properties getCapabilities() {
    Properties capabilities = new Properties();
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_NEW, "true" );
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_START, "true" );
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_STOP, "true" );
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_SAVE, "true" );
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_PAUSE, "false" );
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_PREVIEW, "false" );
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_DEBUG, "false" );

    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_COPY, "true" );
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_PASTE, "true" );
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_CUT, "true" );
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_DELETE, "true" );

    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_FILE_HISTORY, "true" );

    return capabilities;
  }

  @Override public HopFileTypeHandlerInterface openFile( HopGui hopGui, String filename, VariableSpace parentVariableSpace ) throws HopException {
    try {
      // This file is opened in the data orchestration perspective
      //
      HopDataOrchestrationPerspective perspective = HopDataOrchestrationPerspective.getInstance();
      perspective.activate();

      // Load the job from file
      //
      JobMeta jobMeta = new JobMeta( parentVariableSpace, filename, hopGui.getMetaStore() );

      // Pass the MetaStore for reference lookups
      //
      jobMeta.setMetaStore( hopGui.getMetaStore() );

      // Show it in the perspective
      //
      return perspective.addJob( perspective.getTabFolder(), hopGui, jobMeta, this );
    } catch ( Exception e ) {
      throw new HopException( "Error opening job file '" + filename + "'", e );
    }
  }

  @Override public HopFileTypeHandlerInterface newFile( HopGui hopGui, VariableSpace parentVariableSpace ) throws HopException {
    try {
      // This file is created in the data orchestration perspective
      //
      HopDataOrchestrationPerspective perspective = HopDataOrchestrationPerspective.getInstance();
      perspective.activate();

      // Create the empty transformation
      //
      JobMeta jobMeta = new JobMeta();
      jobMeta.setParentVariableSpace( parentVariableSpace );
      jobMeta.setName( "New job" );

      // Pass the MetaStore for reference lookups
      //
      jobMeta.setMetaStore( hopGui.getMetaStore() );

      // Show it in the perspective
      //
      return perspective.addJob( perspective.getTabFolder(), hopGui, jobMeta, this );
    } catch ( Exception e ) {
      throw new HopException( "Error creating new job", e );
    }
  }

  @Override public boolean isHandledBy( String filename, boolean checkContent ) throws HopException {
    try {
      if ( checkContent ) {
        Document document = XMLHandler.loadXMLFile( filename );
        Node jobNode = XMLHandler.getSubNode( document, JobMeta.XML_TAG );
        return jobNode != null;
      } else {
        return super.isHandledBy( filename, checkContent );
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to verify file handling of file '" + filename + "'", e );
    }
  }

  @Override public boolean supportsFile( IHasFilename metaObject ) {
    return metaObject instanceof JobMeta;
  }

  public static final String ACTION_ID_NEW_TRANSFORMATION = "NewJob";

  @Override public List<IGuiContextHandler> getContextHandlers() {

    HopGui hopGui = HopGui.getInstance();

    List<IGuiContextHandler> handlers = new ArrayList<>();
    handlers.add( new IGuiContextHandler() {
      @Override public List<GuiAction> getSupportedActions() {
        List<GuiAction> actions = new ArrayList<>();

        GuiAction newAction = new GuiAction( ACTION_ID_NEW_TRANSFORMATION, GuiActionType.Create, "New job", "Create a new job", "ui/images/JOB.svg",
          ( shiftClicked, controlClicked, parameters ) -> {
            try {
              HopJobFileType.this.newFile( hopGui, hopGui.getVariableSpace() );
            } catch ( Exception e ) {
              new ErrorDialog( hopGui.getShell(), "Error", "Error creating new job", e );
            }
          } );
        actions.add( newAction );

        return actions;
      }
    } );
    return handlers;
  }
}
