package org.apache.hop.ui.hopgui.file.pipeline.delegates;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.plugins.PartitionerPluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Partitioner;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.StepDialogInterface;
import org.apache.hop.pipeline.step.StepErrorMeta;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.apache.hop.pipeline.step.StepPartitioningMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ShowBrowserDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.pipeline.IPartitionSchemaSelection;
import org.apache.hop.ui.hopgui.partition.PartitionMethodSelector;
import org.apache.hop.ui.hopgui.partition.PartitionSettings;
import org.apache.hop.ui.hopgui.partition.processor.MethodProcessor;
import org.apache.hop.ui.hopgui.partition.processor.MethodProcessorFactory;
import org.apache.hop.ui.pipeline.step.StepErrorMetaDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HopGuiPipelineStepDelegate {

  // TODO: move i18n package to HopGui
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!


  private HopGui hopUi;
  private HopGuiPipelineGraph pipelineGraph;

  public HopGuiPipelineStepDelegate( HopGui hopGui, HopGuiPipelineGraph pipelineGraph ) {
    this.hopUi = hopGui;
    this.pipelineGraph = pipelineGraph;
  }

  public StepDialogInterface getStepDialog( StepMetaInterface stepMeta, PipelineMeta pipelineMeta, String stepName ) throws HopException {
    Class<?>[] paramClasses = new Class<?>[] { Shell.class, Object.class, PipelineMeta.class, String.class };
    Object[] paramArgs = new Object[] { hopUi.getShell(), stepMeta, pipelineMeta, stepName };

    PluginRegistry registry = PluginRegistry.getInstance();
    PluginInterface plugin = registry.getPlugin( StepPluginType.class, stepMeta );
    String dialogClassName = plugin.getClassMap().get( StepDialogInterface.class );
    if ( dialogClassName == null ) {
      // Calculate it from the base meta class...
      //
      dialogClassName = stepMeta.getDialogClassName();
    }

    if ( dialogClassName == null ) {
      throw new HopException( "Unable to find dialog class for plugin '" + plugin.getIds()[ 0 ] + "' : " + plugin.getName() );
    }

    try {
      Class<StepDialogInterface> dialogClass = registry.getClass( plugin, dialogClassName );
      Constructor<StepDialogInterface> dialogConstructor = dialogClass.getConstructor( paramClasses );
      return dialogConstructor.newInstance( paramArgs );
    } catch ( Exception e ) {
      // try the old way for compatibility
      try {
        Class<?>[] sig = new Class<?>[] { Shell.class, StepMetaInterface.class, PipelineMeta.class, String.class };
        Method method = stepMeta.getClass().getDeclaredMethod( "getDialog", sig );
        if ( method != null ) {
          hopUi.getLog().logDebug( "Use of StepMetaInterface#getDialog is deprecated, use PluginDialog annotation instead." );
          return (StepDialogInterface) method.invoke( stepMeta, paramArgs );
        }
      } catch ( Throwable ignored ) {
      }

      String errorTitle = BaseMessages.getString( PKG, "HopGui.Dialog.ErrorCreatingStepDialog.Title" );
      String errorMsg = BaseMessages.getString( PKG, "HopGui.Dialog.ErrorCreatingStepDialog.Message", dialogClassName );
      new ErrorDialog( hopUi.getShell(), errorTitle, errorMsg, e );
      throw new HopException( e );
    }
  }

  public String editStep( PipelineMeta pipelineMeta, StepMeta stepMeta ) {
    boolean refresh = false;
    String stepname = null;
    try {
      String name = stepMeta.getName();

      // Before we do anything, let's store the situation the way it
      // was...
      //
      StepMeta before = (StepMeta) stepMeta.clone();
      StepDialogInterface dialog = getStepDialog( stepMeta.getStepMetaInterface(), pipelineMeta, name );
      if ( dialog != null ) {
        dialog.setMetaStore( hopUi.getMetaStore() );
        stepname = dialog.open();
      }

      if ( !Utils.isEmpty( stepname ) ) {
        // Force the recreation of the step IO metadata object. (cached by default)
        //
        stepMeta.getStepMetaInterface().resetStepIoMeta();

        //
        // See if the new name the user enter, doesn't collide with
        // another step.
        // If so, change the stepname and warn the user!
        //
        String newname = stepname;
        StepMeta smeta = pipelineMeta.findStep( newname, stepMeta );
        int nr = 2;
        while ( smeta != null ) {
          newname = stepname + " " + nr;
          smeta = pipelineMeta.findStep( newname );
          nr++;
        }
        if ( nr > 2 ) {
          stepname = newname;
          MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_INFORMATION );
          mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.StepnameExists.Message", stepname ) );
          mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.StepnameExists.Title" ) );
          mb.open();
        }

        if ( !stepname.equals( name ) ) {
          refresh = true;
        }

        StepMeta newStepMeta = (StepMeta) stepMeta.clone();
        newStepMeta.setName( stepname );
        pipelineMeta.clearCaches();
        pipelineMeta.notifyAllListeners( stepMeta, newStepMeta );
        stepMeta.setName( stepname );

        //
        // OK, so the step has changed...
        // Backup the situation for undo/redo
        //
        StepMeta after = (StepMeta) stepMeta.clone();
        /**
         * TODO: Create new Undo/Redo system
         hopUi.addUndoChange( pipelineMeta, new StepMeta[] { before }, new StepMeta[] { after }, new int[] { pipelineMeta
         .indexOfStep( stepMeta ) } );
         */
      }
      pipelineGraph.redraw(); // name is displayed on the graph too.

      // TODO: verify "double pathway" steps for bug #4365
      // After the step was edited we can complain about the possible
      // deadlock here.
      //
    } catch ( Throwable e ) {
      if ( hopUi.getShell().isDisposed() ) {
        return null;
      }
      new ErrorDialog(
        hopUi.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.UnableOpenDialog.Title" ), BaseMessages
        .getString( PKG, "HopGui.Dialog.UnableOpenDialog.Message" ), e );
    }

    return stepname;
  }


  /**
   * Allocate new step, optionally open and rename it.
   *
   * @param id          Id of the new step
   * @param name        Name of the new step
   * @param description Description of the type of step
   * @param openit      Open the dialog for this step?
   * @param rename      Rename this step?
   * @return The newly created StepMeta object.
   */
  public StepMeta newStep( PipelineMeta pipelineMeta, String id, String name, String description, boolean openit, boolean rename, Point location ) {
    try {
      StepMeta stepMeta = null;

      // See if we need to rename the step to avoid doubles!
      if ( rename && pipelineMeta.findStep( name ) != null ) {
        int i = 2;
        String newName = name + " " + i;
        while ( pipelineMeta.findStep( newName ) != null ) {
          i++;
          newName = name + " " + i;
        }
        name = newName;
      }

      PluginRegistry registry = PluginRegistry.getInstance();
      PluginInterface stepPlugin = id != null ? registry.findPluginWithId( StepPluginType.class, id )
        : registry.findPluginWithName( StepPluginType.class, description );

      try {
        if ( stepPlugin != null ) {
          StepMetaInterface info = (StepMetaInterface) registry.loadClass( stepPlugin );

          info.setDefault();

          if ( openit ) {
            StepDialogInterface dialog = this.getStepDialog( info, pipelineMeta, name );
            if ( dialog != null ) {
              name = dialog.open();
            }
          }
          stepMeta = new StepMeta( stepPlugin.getIds()[ 0 ], name, info );

          if ( name != null ) {
            // OK pressed in the dialog: we have a step-name
            String newName = name;
            StepMeta candiateStepMeta = pipelineMeta.findStep( newName );
            int nr = 2;
            while ( candiateStepMeta != null ) {
              newName = name + " " + nr;
              candiateStepMeta = pipelineMeta.findStep( newName );
              nr++;
            }
            if ( nr > 2 ) {
              stepMeta.setName( newName );
              MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_INFORMATION );
              // "This stepName already exists.  HopGui changed the stepName to ["+newName+"]"
              mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.ChangeStepname.Message", newName ) );
              mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.ChangeStepname.Title" ) );
              mb.open();
            }
            stepMeta.setLocation( location.x, location.y ); // default location at (20,20)
            pipelineMeta.addStep( stepMeta );
            hopUi.undoDelegate.addUndoNew( pipelineMeta, new StepMeta[] { stepMeta }, new int[] { pipelineMeta.indexOfStep( stepMeta ) } );

            // Also store it in the pluginHistory list...
            hopUi.getProps().increasePluginHistory( stepPlugin.getIds()[ 0 ] );

          } else {
            return null; // Cancel pressed in dialog.
          }
          pipelineGraph.updateGui();
        }
      } catch ( HopException e ) {
        String filename = stepPlugin.getErrorHelpFile();
        if ( !Utils.isEmpty( filename ) ) {
          // OK, in stead of a normal error message, we give back the
          // content of the error help file... (HTML)
          FileInputStream fis = null;
          try {
            StringBuilder content = new StringBuilder();

            fis = new FileInputStream( new File( filename ) );
            int ch = fis.read();
            while ( ch >= 0 ) {
              content.append( (char) ch );
              ch = fis.read();
            }

            ShowBrowserDialog sbd = new ShowBrowserDialog( hopUi.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.ErrorHelpText.Title" ), content.toString() );
            sbd.open();
          } catch ( Exception ex ) {
            new ErrorDialog( hopUi.getShell(), BaseMessages.getString( PKG, "HopGui.Dialog.ErrorShowingHelpText.Title" ),
              BaseMessages.getString( PKG, "HopGui.Dialog.ErrorShowingHelpText.Message" ), ex );
          } finally {
            if ( fis != null ) {
              try {
                fis.close();
              } catch ( Exception ex ) {
                hopUi.getLog().logError( "Error closing plugin help file", ex );
              }
            }
          }
        } else {
          new ErrorDialog( hopUi.getShell(),
            // "Error creating step"
            // "I was unable to create a new step"
            BaseMessages.getString( PKG, "HopGui.Dialog.UnableCreateNewStep.Title" ), BaseMessages.getString(
            PKG, "HopGui.Dialog.UnableCreateNewStep.Message" ), e );
        }
        return null;
      } catch ( Throwable e ) {
        if ( !hopUi.getShell().isDisposed() ) {
          new ErrorDialog( hopUi.getShell(),
            // "Error creating step"
            BaseMessages.getString( PKG, "HopGui.Dialog.ErrorCreatingStep.Title" ), BaseMessages.getString(
            PKG, "HopGui.Dialog.UnableCreateNewStep.Message" ), e );
        }
        return null;
      }

      return stepMeta;
    } finally {
      pipelineGraph.redraw();
    }
  }

  public void editStepPartitioning( PipelineMeta pipelineMeta, StepMeta stepMeta ) {
    String[] schemaNames;
    try {
      schemaNames = hopUi.partitionManager.getNamesArray();
    } catch ( HopException e ) {
      new ErrorDialog( hopUi.getShell(),
        BaseMessages.getString( PKG, "HopGui.ErrorDialog.Title" ),
        BaseMessages.getString( PKG, "HopGui.ErrorDialog.ErrorFetchingFromRepo.PartitioningSchemas" ),
        e
      );
      return;
    }
    try {
      /*Check if Partition schema has already defined*/
      if ( isDefinedSchemaExist( schemaNames ) ) {

        /*Prepare settings for Method selection*/
        PluginRegistry registry = PluginRegistry.getInstance();
        List<PluginInterface> plugins = registry.getPlugins( PartitionerPluginType.class );
        int exactSize = StepPartitioningMeta.methodDescriptions.length + plugins.size();
        PartitionSettings partitionSettings = new PartitionSettings( exactSize, pipelineMeta, stepMeta, hopUi.partitionManager );
        partitionSettings.fillOptionsAndCodesByPlugins( plugins );

        /*Method selection*/
        PartitionMethodSelector methodSelector = new PartitionMethodSelector();
        String partitionMethodDescription = methodSelector.askForPartitionMethod( hopUi.getShell(), partitionSettings );
        if ( !StringUtil.isEmpty( partitionMethodDescription ) ) {
          String method = partitionSettings.getMethodByMethodDescription( partitionMethodDescription );
          int methodType = StepPartitioningMeta.getMethodType( method );

          partitionSettings.updateMethodType( methodType );
          partitionSettings.updateMethod( method );

          /*Schema selection*/
          MethodProcessor methodProcessor = MethodProcessorFactory.create( methodType );
          methodProcessor.schemaSelection( partitionSettings, hopUi.getShell(), new IPartitionSchemaSelection() {
            @Override public String schemaFieldSelection( Shell shell, PartitionSettings settings ) throws HopException {
              StepPartitioningMeta partitioningMeta = settings.getStepMeta().getStepPartitioningMeta();
              StepDialogInterface dialog = getPartitionerDialog( shell, settings.getStepMeta(), partitioningMeta, settings.getPipelineMeta() );
              return dialog.open();
            }
          } );
        }
        stepMeta.setChanged();
        hopUi.undoDelegate.addUndoChange( partitionSettings.getPipelineMeta(), new StepMeta[] { partitionSettings.getBefore() },
         new StepMeta[] { partitionSettings.getAfter() }, new int[] { partitionSettings.getPipelineMeta()
         .indexOfStep( partitionSettings.getStepMeta() ) }
         );
        pipelineGraph.redraw();
      }
    } catch ( Exception e ) {
      new ErrorDialog(
        hopUi.getShell(), "Error",
        "There was an unexpected error while editing the partitioning method specifics:", e );
    }
  }

  public boolean isDefinedSchemaExist( String[] schemaNames ) {
    // Before we start, check if there are any partition schemas defined...
    if ( ( schemaNames == null ) || ( schemaNames.length == 0 ) ) {
      MessageBox box = new MessageBox( hopUi.getShell(), SWT.ICON_ERROR | SWT.OK );
      box.setText( "Create a partition schema" );
      box.setMessage( "You first need to create one or more partition schemas before you can select one!" );
      box.open();
      return false;
    }
    return true;
  }

  public StepDialogInterface getPartitionerDialog( Shell shell, StepMeta stepMeta, StepPartitioningMeta partitioningMeta,
                                                   PipelineMeta pipelineMeta ) throws HopException {
    Partitioner partitioner = partitioningMeta.getPartitioner();
    String dialogClassName = partitioner.getDialogClassName();

    Class<?> dialogClass;
    Class<?>[] paramClasses = new Class<?>[] { Shell.class, StepMeta.class, StepPartitioningMeta.class, PipelineMeta.class };
    Object[] paramArgs = new Object[] { shell, stepMeta, partitioningMeta, pipelineMeta };
    Constructor<?> dialogConstructor;
    try {
      dialogClass = partitioner.getClass().getClassLoader().loadClass( dialogClassName );
      dialogConstructor = dialogClass.getConstructor( paramClasses );
      return (StepDialogInterface) dialogConstructor.newInstance( paramArgs );
    } catch ( Exception e ) {
      throw new HopException( "Unable to open dialog of partitioning method", e );
    }
  }

  public void editStepErrorHandling( PipelineMeta pipelineMeta, StepMeta stepMeta ) {
    if ( stepMeta != null && stepMeta.supportsErrorHandling() ) {
      StepErrorMeta stepErrorMeta = stepMeta.getStepErrorMeta();
      if ( stepErrorMeta == null ) {
        stepErrorMeta = new StepErrorMeta( pipelineMeta, stepMeta );
      }
      List<StepMeta> targetSteps = pipelineMeta.findNextSteps( stepMeta );

      // now edit this stepErrorMeta object:
      StepErrorMetaDialog dialog =
        new StepErrorMetaDialog( hopUi.getShell(), stepErrorMeta, pipelineMeta, targetSteps );
      if ( dialog.open() ) {
        stepMeta.setStepErrorMeta( stepErrorMeta );
        stepMeta.setChanged();
        pipelineGraph.redraw();
      }
    }
  }

  public void delSteps( PipelineMeta pipelineMeta, List<StepMeta> steps ) {
    try {
      ExtensionPointHandler.callExtensionPoint( hopUi.getLog(), HopExtensionPoint.PipelineBeforeDeleteSteps.id, steps );
    } catch ( HopException e ) {
      return;
    }

    // Hops belonging to the deleting steps are placed in a single transaction and removed.
    List<PipelineHopMeta> pipelineHops = new ArrayList<>();
    int[] hopIndexes = new int[ pipelineMeta.nrPipelineHops() ];
    int hopIndex = 0;
    for ( int i = pipelineMeta.nrPipelineHops() - 1; i >= 0; i-- ) {
      PipelineHopMeta hi = pipelineMeta.getPipelineHop( i );
      for ( int j = 0; j < steps.size() && hopIndex < hopIndexes.length; j++ ) {
        if ( hi.getFromStep().equals( steps.get( j ) ) || hi.getToStep().equals( steps.get( j ) ) ) {
          int idx = pipelineMeta.indexOfPipelineHop( hi );
          pipelineHops.add( (PipelineHopMeta) hi.clone() );
          hopIndexes[ hopIndex ] = idx;
          pipelineMeta.removePipelineHop( idx );
          hopIndex++;
          break;
        }
      }
    }
    if ( !pipelineHops.isEmpty() ) {
      PipelineHopMeta[] hops = pipelineHops.toArray( new PipelineHopMeta[ pipelineHops.size() ] );
      hopUi.undoDelegate.addUndoDelete( pipelineMeta, hops, hopIndexes );
    }

    // Deleting steps are placed all in a single transaction and removed.
    int[] positions = new int[ steps.size() ];
    for ( int i = 0; i < steps.size(); i++ ) {
      int pos = pipelineMeta.indexOfStep( steps.get( i ) );
      pipelineMeta.removeStep( pos );
      positions[ i ] = pos;
    }
    hopUi.undoDelegate.addUndoDelete( pipelineMeta, steps.toArray( new StepMeta[ 0 ] ), positions );

    pipelineGraph.redraw();
  }

  public void delStep( PipelineMeta pipelineMeta, StepMeta stepMeta ) {
    delSteps( pipelineMeta, Arrays.asList( stepMeta ) );
  }
}
