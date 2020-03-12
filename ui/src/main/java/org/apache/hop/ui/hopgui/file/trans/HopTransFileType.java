package org.apache.hop.ui.hopgui.file.trans;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.trans.TransMeta;
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
  id = "HopFile-Transformation-Plugin",
  description = "The transformation file information for the Hop GUI"
)
public class HopTransFileType<T extends TransMeta> extends HopFileTypeBase<T> implements HopFileTypeInterface<T> {

  public HopTransFileType() {
  }

  @Override public String getName() {
    return "Transformation"; // TODO: i18n
  }

  @Override public String[] getFilterExtensions() {
    return new String[] { "*.ktr" };
  }

  @Override public String[] getFilterNames() {
    return new String[] { "Transformations" };
  }

  public Properties getCapabilities() {
    Properties capabilities = new Properties();
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_NEW, "true" );
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_START, "true" );
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_STOP, "true" );
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_SAVE, "true" );
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_PAUSE, "true" );
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_PREVIEW, "true" );
    capabilities.setProperty( HopFileTypeInterface.CAPABILITY_DEBUG, "true" );

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

      // Load the transformation
      //
      TransMeta transMeta = new TransMeta( filename, parentVariableSpace );

      // Pass the MetaStore for reference lookups
      //
      transMeta.setMetaStore( hopGui.getMetaStore() );

      // Show it in the perspective
      //
      return perspective.addTransformation( perspective.getTabFolder(), hopGui, transMeta, this );
    } catch ( Exception e ) {
      throw new HopException( "Error opening transformation file '" + filename + "'", e );
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
      TransMeta transMeta = new TransMeta( parentVariableSpace );
      transMeta.setName( "New transformation" );

      // Pass the MetaStore for reference lookups
      //
      transMeta.setMetaStore( hopGui.getMetaStore() );

      // Show it in the perspective
      //
      return perspective.addTransformation( perspective.getTabFolder(), hopGui, transMeta, this );
    } catch ( Exception e ) {
      throw new HopException( "Error creating new transformation", e );
    }
  }

  @Override public boolean isHandledBy( String filename, boolean checkContent ) throws HopException {
    try {
      if ( checkContent ) {
        Document document = XMLHandler.loadXMLFile( filename );
        Node transformationNode = XMLHandler.getSubNode( document, TransMeta.XML_TAG );
        return transformationNode != null;
      } else {
        return super.isHandledBy( filename, checkContent );
      }
    } catch ( Exception e ) {
      throw new HopException( "Unable to verify file handling of file '" + filename + "'", e );
    }
  }

  public static final String ACTION_ID_NEW_TRANSFORMATION = "NewTransformation";

  @Override public List<IGuiContextHandler> getContextHandlers() {

    HopGui hopGui = HopGui.getInstance();

    List<IGuiContextHandler> handlers = new ArrayList<>();
    handlers.add( new IGuiContextHandler() {
      @Override public List<GuiAction> getSupportedActions() {
        List<GuiAction> actions = new ArrayList<>(  );

        GuiAction newAction = new GuiAction( ACTION_ID_NEW_TRANSFORMATION, GuiActionType.Create, "New transformation", "Create a new transformation", "ui/images/TRN.svg",
          (parameters) -> {
            try {
              HopTransFileType.this.newFile( hopGui, hopGui.getVariableSpace() );
            } catch ( Exception e ) {
              new ErrorDialog( hopGui.getShell(), "Error", "Error creating new transformation", e );
            }
          } );
        actions.add(newAction);

        return actions;
      }
    } );
    return handlers;
  }
}
