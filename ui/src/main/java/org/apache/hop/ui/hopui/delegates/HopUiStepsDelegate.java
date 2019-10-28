/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.hopui.delegates;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.hop.ui.hopui.HopUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
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
import org.apache.hop.ui.trans.step.StepErrorMetaDialog;

public class HopUiStepsDelegate extends HopUiDelegate {
  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!

  public HopUiStepsDelegate( HopUi hopUi ) {
    super( hopUi );
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
        hopUi.refreshGraph();
      }
    }
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
      hopUi.addUndoNew( transMeta, new StepMeta[] { (StepMeta) stMeta.clone() }, new int[] { transMeta
        .indexOfStep( stMeta ) } );
      hopUi.refreshTree();
      hopUi.refreshGraph();
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
      StepDialogInterface dialog = hopUi.getStepEntryDialog( stepMeta.getStepMetaInterface(), transMeta, name );
      if ( dialog != null ) {
        dialog.setRepository( hopUi.getRepository() );
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
        hopUi.addUndoChange( transMeta, new StepMeta[] { before }, new StepMeta[] { after }, new int[] { transMeta
          .indexOfStep( stepMeta ) } );
      } else {
        // Scenario: change connections and click cancel...
        // Perhaps new connections were created in the step dialog?
        if ( transMeta.haveConnectionsChanged() ) {
          refresh = true;
        }
      }
      hopUi.refreshGraph(); // name is displayed on the graph too.

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

    if ( refresh ) {
      hopUi.refreshTree(); // Perhaps new connections were created in
      // the step
      // dialog or the step name changed.
    }

    return stepname;
  }

  public void delSteps( TransMeta transformation, StepMeta[] steps ) {
    try {
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.TransBeforeDeleteSteps.id, steps );
    } catch ( HopException e ) {
      return;
    }

    // Hops belonging to the deleting steps are placed in a single transaction and removed.
    List<TransHopMeta> transHops = new ArrayList<>();
    int[] hopIndexes = new int[transformation.nrTransHops()];
    int hopIndex = 0;
    for ( int i = transformation.nrTransHops() - 1; i >= 0; i-- ) {
      TransHopMeta hi = transformation.getTransHop( i );
      for ( int j = 0; j < steps.length && hopIndex < hopIndexes.length; j++ ) {
        if ( hi.getFromStep().equals( steps[j] ) || hi.getToStep().equals( steps[j] ) ) {
          int idx = transformation.indexOfTransHop( hi );
          transHops.add( (TransHopMeta) hi.clone() );
          hopIndexes[hopIndex] = idx;
          transformation.removeTransHop( idx );
          hopUi.refreshTree();
          hopIndex++;
          break;
        }
      }
    }
    if ( !transHops.isEmpty() ) {
      TransHopMeta[] hops = transHops.toArray( new TransHopMeta[transHops.size()] );
      hopUi.addUndoDelete( transformation, hops, hopIndexes );
    }

    // Deleting steps are placed all in a single transaction and removed.
    int[] positions = new int[steps.length];
    for ( int i = 0; i < steps.length; i++ ) {
      int pos = transformation.indexOfStep( steps[i] );
      transformation.removeStep( pos );
      positions[i] = pos;
    }
    hopUi.addUndoDelete( transformation, steps, positions );

    hopUi.refreshTree();
    hopUi.refreshGraph();
  }

  public void delStep( TransMeta transMeta, StepMeta stepMeta ) {
    delSteps( transMeta, new StepMeta[] { stepMeta } );
  }

  public StepDialogInterface getStepDialog( StepMetaInterface stepMeta, TransMeta transMeta, String stepName ) throws HopException {
    Class<?>[] paramClasses = new Class<?>[] { Shell.class, Object.class, TransMeta.class, String.class };
    Object[] paramArgs = new Object[] { hopUi.getShell(), stepMeta, transMeta, stepName };

    PluginRegistry registry = PluginRegistry.getInstance();
    PluginInterface plugin = registry.getPlugin( StepPluginType.class, stepMeta );
    String dialogClassName = plugin.getClassMap().get( StepDialogInterface.class );
    if ( dialogClassName == null ) {
      // try the deprecated way
      log.logDebug( "Use of StepMetaInterface#getDialogClassName is deprecated, use PluginDialog annotation instead." );
      dialogClassName = stepMeta.getDialogClassName();
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
          log.logDebug( "Use of StepMetaInterface#getDialog is deprecated, use PluginDialog annotation instead." );
          return (StepDialogInterface) method.invoke( stepMeta, paramArgs );
        }
      } catch ( Throwable ignored ) { }

      String errorTitle = BaseMessages.getString( PKG, "Spoon.Dialog.ErrorCreatingStepDialog.Title" );
      String errorMsg = BaseMessages.getString( PKG, "Spoon.Dialog.ErrorCreatingStepDialog.Message", dialogClassName );
      new ErrorDialog( hopUi.getShell(), errorTitle, errorMsg, e );
      throw new HopException( e );
    }
  }

  public StepDialogInterface getPartitionerDialog( StepMeta stepMeta, StepPartitioningMeta partitioningMeta,
    TransMeta transMeta ) throws HopException {
    Partitioner partitioner = partitioningMeta.getPartitioner();
    String dialogClassName = partitioner.getDialogClassName();

    Class<?> dialogClass;
    Class<?>[] paramClasses =
      new Class<?>[] { Shell.class, StepMeta.class, StepPartitioningMeta.class, TransMeta.class };
    Object[] paramArgs = new Object[] { hopUi.getShell(), stepMeta, partitioningMeta, transMeta };
    Constructor<?> dialogConstructor;
    try {
      dialogClass = partitioner.getClass().getClassLoader().loadClass( dialogClassName );
      dialogConstructor = dialogClass.getConstructor( paramClasses );
      return (StepDialogInterface) dialogConstructor.newInstance( paramArgs );
    } catch ( Exception e ) {
      // try the old way for compatibility
      Method method;
      try {
        Class<?>[] sig = new Class<?>[] { Shell.class, StepMetaInterface.class, TransMeta.class };
        method = stepMeta.getClass().getDeclaredMethod( "getDialog", sig );
        if ( method != null ) {
          return (StepDialogInterface) method.invoke( stepMeta, new Object[] {
            hopUi.getShell(), stepMeta, transMeta } );
        }
      } catch ( Throwable ignored ) { }

      throw new HopException( e );
    }

  }

}
