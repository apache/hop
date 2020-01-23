package org.apache.hop.ui.hopgui.file.trans;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.plugins.PartitionerPluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.Partitioner;
import org.apache.hop.trans.TransHopMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.step.StepErrorMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.trans.step.StepPartitioningMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ShowBrowserDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.hopui.partition.PartitionMethodSelector;
import org.apache.hop.ui.hopui.partition.PartitionSettings;
import org.apache.hop.ui.hopui.partition.processor.MethodProcessor;
import org.apache.hop.ui.hopui.partition.processor.MethodProcessorFactory;
import org.apache.hop.ui.trans.step.StepErrorMetaDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class HopGuiStepDelegate {

  // TODO: move i18n package to HopGui
  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!


  private HopGui hopUi;
  private HopGuiTransGraph transGraph;

  public HopGuiStepDelegate( HopGui hopGui, HopGuiTransGraph transGraph ) {
    this.hopUi = hopGui;
    this.transGraph = transGraph;
  }

  public StepDialogInterface getStepDialog( StepMetaInterface stepMeta, TransMeta transMeta, String stepName ) throws HopException {
    Class<?>[] paramClasses = new Class<?>[] { Shell.class, Object.class, TransMeta.class, String.class };
    Object[] paramArgs = new Object[] { hopUi.getShell(), stepMeta, transMeta, stepName };

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
        Class<?>[] sig = new Class<?>[] { Shell.class, StepMetaInterface.class, TransMeta.class, String.class };
        Method method = stepMeta.getClass().getDeclaredMethod( "getDialog", sig );
        if ( method != null ) {
          hopUi.getLog().logDebug( "Use of StepMetaInterface#getDialog is deprecated, use PluginDialog annotation instead." );
          return (StepDialogInterface) method.invoke( stepMeta, paramArgs );
        }
      } catch ( Throwable ignored ) {
      }

      String errorTitle = BaseMessages.getString( PKG, "Spoon.Dialog.ErrorCreatingStepDialog.Title" );
      String errorMsg = BaseMessages.getString( PKG, "Spoon.Dialog.ErrorCreatingStepDialog.Message", dialogClassName );
      new ErrorDialog( hopUi.getShell(), errorTitle, errorMsg, e );
      throw new HopException( e );
    }
  }

  public String editStep( TransMeta transMeta, StepMeta stepMeta ) {
    boolean refresh = false;
    String stepname = null;
    try {
      String name = stepMeta.getName();

      // Before we do anything, let's store the situation the way it
      // was...
      //
      StepMeta before = (StepMeta) stepMeta.clone();
      StepDialogInterface dialog = getStepDialog( stepMeta.getStepMetaInterface(), transMeta, name );
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
        StepMeta smeta = transMeta.findStep( newname, stepMeta );
        int nr = 2;
        while ( smeta != null ) {
          newname = stepname + " " + nr;
          smeta = transMeta.findStep( newname );
          nr++;
        }
        if ( nr > 2 ) {
          stepname = newname;
          MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_INFORMATION );
          mb.setMessage( BaseMessages.getString( PKG, "Spoon.Dialog.StepnameExists.Message", stepname ) );
          mb.setText( BaseMessages.getString( PKG, "Spoon.Dialog.StepnameExists.Title" ) );
          mb.open();
        }

        if ( !stepname.equals( name ) ) {
          refresh = true;
        }

        StepMeta newStepMeta = (StepMeta) stepMeta.clone();
        newStepMeta.setName( stepname );
        transMeta.clearCaches();
        transMeta.notifyAllListeners( stepMeta, newStepMeta );
        stepMeta.setName( stepname );

        //
        // OK, so the step has changed...
        // Backup the situation for undo/redo
        //
        StepMeta after = (StepMeta) stepMeta.clone();
        /**
         * TODO: Create new Undo/Redo system
         hopUi.addUndoChange( transMeta, new StepMeta[] { before }, new StepMeta[] { after }, new int[] { transMeta
         .indexOfStep( stepMeta ) } );
         */
      }
      transGraph.redraw(); // name is displayed on the graph too.

      // TODO: verify "double pathway" steps for bug #4365
      // After the step was edited we can complain about the possible
      // deadlock here.
      //
    } catch ( Throwable e ) {
      if ( hopUi.getShell().isDisposed() ) {
        return null;
      }
      new ErrorDialog(
        hopUi.getShell(), BaseMessages.getString( PKG, "Spoon.Dialog.UnableOpenDialog.Title" ), BaseMessages
        .getString( PKG, "Spoon.Dialog.UnableOpenDialog.Message" ), e );
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
  public StepMeta newStep( TransMeta transMeta, String id, String name, String description, boolean openit, boolean rename ) {
    StepMeta inf = null;

    // See if we need to rename the step to avoid doubles!
    if ( rename && transMeta.findStep( name ) != null ) {
      int i = 2;
      String newName = name + " " + i;
      while ( transMeta.findStep( newName ) != null ) {
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
          StepDialogInterface dialog = this.getStepDialog( info, transMeta, name );
          if ( dialog != null ) {
            name = dialog.open();
          }
        }
        inf = new StepMeta( stepPlugin.getIds()[ 0 ], name, info );

        if ( name != null ) {
          // OK pressed in the dialog: we have a step-name
          String newName = name;
          StepMeta stepMeta = transMeta.findStep( newName );
          int nr = 2;
          while ( stepMeta != null ) {
            newName = name + " " + nr;
            stepMeta = transMeta.findStep( newName );
            nr++;
          }
          if ( nr > 2 ) {
            inf.setName( newName );
            MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_INFORMATION );
            // "This stepName already exists.  Spoon changed the stepName to ["+newName+"]"
            mb.setMessage( BaseMessages.getString( PKG, "Spoon.Dialog.ChangeStepname.Message", newName ) );
            mb.setText( BaseMessages.getString( PKG, "Spoon.Dialog.ChangeStepname.Title" ) );
            mb.open();
          }
          inf.setLocation( 20, 20 ); // default location at (20,20)
          transMeta.addStep( inf );
          /**
           * TODO: add new Undo/Redo system
           addUndoNew( transMeta, new StepMeta[] { inf }, new int[] { transMeta.indexOfStep( inf ) } );
           */

          // Also store it in the pluginHistory list...
          hopUi.getProps().increasePluginHistory( stepPlugin.getIds()[ 0 ] );

        } else {
          return null; // Cancel pressed in dialog.
        }
        hopUi.setShellText();
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

          ShowBrowserDialog sbd =
            new ShowBrowserDialog( hopUi.getShell(), BaseMessages.getString( PKG, "Spoon.Dialog.ErrorHelpText.Title" ), content.toString() );
          sbd.open();
        } catch ( Exception ex ) {
          new ErrorDialog( hopUi.getShell(), BaseMessages.getString( PKG, "Spoon.Dialog.ErrorShowingHelpText.Title" ),
            BaseMessages.getString( PKG, "Spoon.Dialog.ErrorShowingHelpText.Message" ), ex );
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
          BaseMessages.getString( PKG, "Spoon.Dialog.UnableCreateNewStep.Title" ), BaseMessages.getString(
          PKG, "Spoon.Dialog.UnableCreateNewStep.Message" ), e );
      }
      return null;
    } catch ( Throwable e ) {
      if ( !hopUi.getShell().isDisposed() ) {
        new ErrorDialog( hopUi.getShell(),
          // "Error creating step"
          BaseMessages.getString( PKG, "Spoon.Dialog.ErrorCreatingStep.Title" ), BaseMessages.getString(
          PKG, "Spoon.Dialog.UnableCreateNewStep.Message" ), e );
      }
      return null;
    }

    return inf;
  }

  public void dupeStep( TransMeta transMeta, StepMeta stepMeta ) {
    hopUi.getLog().logDebug(
      toString(), BaseMessages.getString( PKG, "Spoon.Log.DuplicateStep" ) + stepMeta.getName() ); // Duplicate
    // step:

    StepMeta stMeta = (StepMeta) stepMeta.clone();
    if ( stMeta != null ) {
      String newname = transMeta.getAlternativeStepname( stepMeta.getName() );
      int nr = 2;
      while ( transMeta.findStep( newname ) != null ) {
        newname = stepMeta.getName() + " (copy " + nr + ")";
        nr++;
      }
      stMeta.setName( newname );
      // Don't select this new step!
      stMeta.setSelected( false );
      Point loc = stMeta.getLocation();
      stMeta.setLocation( loc.x + 20, loc.y + 20 );
      transMeta.addStep( stMeta );
      // TODO: Create new Undo/Redo system
      //
      // hopUi.addUndoNew( transMeta, new StepMeta[] { (StepMeta) stMeta.clone() }, new int[] { transMeta.indexOfStep( stMeta ) } );
      transGraph.redraw();
    }
  }

  public void editStepPartitioning( TransMeta transMeta, StepMeta stepMeta ) {
    String[] schemaNames;
    try {
      schemaNames = hopUi.partitionManager.getNamesArray();
    } catch ( HopException e ) {
      new ErrorDialog( hopUi.getShell(),
        BaseMessages.getString( PKG, "Spoon.ErrorDialog.Title" ),
        BaseMessages.getString( PKG, "Spoon.ErrorDialog.ErrorFetchingFromRepo.PartitioningSchemas" ),
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
        PartitionSettings settings = new PartitionSettings( exactSize, transMeta, stepMeta, hopUi.partitionManager );
        settings.fillOptionsAndCodesByPlugins( plugins );

        /*Method selection*/
        PartitionMethodSelector methodSelector = new PartitionMethodSelector();
        String partitionMethodDescription =
          methodSelector.askForPartitionMethod( hopUi.getShell(), settings );
        if ( !StringUtil.isEmpty( partitionMethodDescription ) ) {
          String method = settings.getMethodByMethodDescription( partitionMethodDescription );
          int methodType = StepPartitioningMeta.getMethodType( method );

          settings.updateMethodType( methodType );
          settings.updateMethod( method );

          /*Schema selection*/
          MethodProcessor methodProcessor = MethodProcessorFactory.create( methodType );
          methodProcessor.schemaSelection( settings, hopUi.getShell(), new IPartitionSchemaSelection() {
            @Override public String schemaFieldSelection( Shell shell, PartitionSettings settings ) throws HopException {
              StepPartitioningMeta partitioningMeta = settings.getStepMeta().getStepPartitioningMeta();
              StepDialogInterface dialog = getPartitionerDialog( shell, settings.getStepMeta(), partitioningMeta, settings.getTransMeta() );
              return dialog.open();
            }
          } );
        }
        /** TODO: Add new Undo/Redo system
         addUndoChange( settings.getTransMeta(), new StepMeta[] { settings.getBefore() },
         new StepMeta[] { settings.getAfter() }, new int[] { settings.getTransMeta()
         .indexOfStep( settings.getStepMeta() ) }
         );
         */
        transGraph.redraw();
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
      box.setMessage( "You first need to create one or more partition schemas in "
        + "the transformation settings dialog before you can select one!" );
      box.open();
      return false;
    }
    return true;
  }

  public StepDialogInterface getPartitionerDialog( Shell shell, StepMeta stepMeta, StepPartitioningMeta partitioningMeta,
                                                   TransMeta transMeta ) throws HopException {
    Partitioner partitioner = partitioningMeta.getPartitioner();
    String dialogClassName = partitioner.getDialogClassName();

    Class<?> dialogClass;
    Class<?>[] paramClasses = new Class<?>[] { Shell.class, StepMeta.class, StepPartitioningMeta.class, TransMeta.class };
    Object[] paramArgs = new Object[] { shell, stepMeta, partitioningMeta, transMeta };
    Constructor<?> dialogConstructor;
    try {
      dialogClass = partitioner.getClass().getClassLoader().loadClass( dialogClassName );
      dialogConstructor = dialogClass.getConstructor( paramClasses );
      return (StepDialogInterface) dialogConstructor.newInstance( paramArgs );
    } catch ( Exception e ) {
      throw new HopException( "Unable to open dialog of partitioning method", e );
    }
  }

  public void editStepErrorHandling( TransMeta transMeta, StepMeta stepMeta ) {
    if ( stepMeta != null && stepMeta.supportsErrorHandling() ) {
      StepErrorMeta stepErrorMeta = stepMeta.getStepErrorMeta();
      if ( stepErrorMeta == null ) {
        stepErrorMeta = new StepErrorMeta( transMeta, stepMeta );
      }
      List<StepMeta> targetSteps = transMeta.findNextSteps( stepMeta );

      // now edit this stepErrorMeta object:
      StepErrorMetaDialog dialog =
        new StepErrorMetaDialog( hopUi.getShell(), stepErrorMeta, transMeta, targetSteps );
      if ( dialog.open() ) {
        stepMeta.setStepErrorMeta( stepErrorMeta );
        stepMeta.setChanged();
        transGraph.redraw();
      }
    }
  }

  public void delSteps( TransMeta transformation, StepMeta[] steps ) {
    /* TODO: Put XP handling back in
    try {
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.TransBeforeDeleteSteps.id, steps );
    } catch ( HopException e ) {
      return;
    }
     */

    // Hops belonging to the deleting steps are placed in a single transaction and removed.
    List<TransHopMeta> transHops = new ArrayList<>();
    int[] hopIndexes = new int[ transformation.nrTransHops() ];
    int hopIndex = 0;
    for ( int i = transformation.nrTransHops() - 1; i >= 0; i-- ) {
      TransHopMeta hi = transformation.getTransHop( i );
      for ( int j = 0; j < steps.length && hopIndex < hopIndexes.length; j++ ) {
        if ( hi.getFromStep().equals( steps[ j ] ) || hi.getToStep().equals( steps[ j ] ) ) {
          int idx = transformation.indexOfTransHop( hi );
          transHops.add( (TransHopMeta) hi.clone() );
          hopIndexes[ hopIndex ] = idx;
          transformation.removeTransHop( idx );
          hopIndex++;
          break;
        }
      }
    }
    /* TODO: Create new Undo/Redo system

    if ( !transHops.isEmpty() ) {
      TransHopMeta[] hops = transHops.toArray( new TransHopMeta[ transHops.size() ] );
      hopUi.addUndoDelete( transformation, hops, hopIndexes );
    }
     */

    // Deleting steps are placed all in a single transaction and removed.
    int[] positions = new int[ steps.length ];
    for ( int i = 0; i < steps.length; i++ ) {
      int pos = transformation.indexOfStep( steps[ i ] );
      transformation.removeStep( pos );
      positions[ i ] = pos;
    }
    /* TODO: Create new Undo/Redo system
    hopUi.addUndoDelete( transformation, steps, positions );
    */

    transGraph.redraw();
  }

  public void delStep( TransMeta transMeta, StepMeta stepMeta ) {
    delSteps( transMeta, new StepMeta[] { stepMeta } );
  }
}
