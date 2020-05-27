/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.ui.hopgui.file.workflow.delegates;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionCopy;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.workflow.actions.special.ActionSpecial;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

public class HopGuiWorkflowActionDelegate {
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!


  private HopGui hopGui;
  private HopGuiWorkflowGraph workflowGraph;

  public HopGuiWorkflowActionDelegate( HopGui hopGui, HopGuiWorkflowGraph workflowGraph ) {
    this.hopGui = hopGui;
    this.workflowGraph = workflowGraph;
  }

  public ActionCopy newJobEntry( WorkflowMeta workflowMeta, String pluginId, String pluginName, boolean openIt, Point location ) {
    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin actionPlugin;

    try {
      if ( pluginId == null ) {
        actionPlugin = PluginRegistry.getInstance().findPluginWithName( ActionPluginType.class, pluginName );
      } else {
        actionPlugin = PluginRegistry.getInstance().findPluginWithId( ActionPluginType.class, pluginId );
      }
      if ( actionPlugin == null ) {
        // Check if it's not START or DUMMY
        if ( WorkflowMeta.STRING_SPECIAL_START.equalsIgnoreCase( pluginName ) || WorkflowMeta.STRING_SPECIAL_DUMMY.equalsIgnoreCase( pluginName ) ) {
          actionPlugin = registry.findPluginWithId( ActionPluginType.class, WorkflowMeta.STRING_SPECIAL );
        }
      }

      if ( actionPlugin != null ) {
        // Determine name & number for this entry.

        // See if the name is already used...
        //
        String actionName = pluginName;
        int nr = 2;
        ActionCopy check = workflowMeta.findAction( actionName, 0 );
        while ( check != null ) {
          actionName = pluginName + " " + nr++;
          check = workflowMeta.findAction( actionName, 0 );
        }

        // Generate the appropriate class...
        IAction action = (IAction) registry.loadClass( actionPlugin );
        action.setPluginId( actionPlugin.getIds()[ 0 ] );
        action.setName( actionName );

        if ( action.isSpecial() ) {
          if ( WorkflowMeta.STRING_SPECIAL_START.equalsIgnoreCase( pluginName ) ) {
            // Check if start is already on the canvas...
            if ( workflowMeta.findStart() != null ) {
              HopGuiWorkflowGraph.showOnlyStartOnceMessage( hopGui.getShell() );
              return null;
            }
            ( (ActionSpecial) action ).setStart( true );
          }
          if ( WorkflowMeta.STRING_SPECIAL_DUMMY.equalsIgnoreCase( pluginName ) ) {
            ( (ActionSpecial) action ).setDummy( true );
          }
        }

        if ( openIt ) {
          IActionDialog d = getActionDialog( action, workflowMeta );
          if ( d != null && d.open() != null ) {
            ActionCopy jge = new ActionCopy();
            jge.setAction( action );
            if ( location != null ) {
              jge.setLocation( location.x, location.y );
            } else {
              jge.setLocation( 50, 50 );
            }
            jge.setNr( 0 );
            workflowMeta.addAction( jge );

            // Verify that the name is not already used in the workflow.
            //
            workflowMeta.renameActionIfNameCollides( jge );

            hopGui.undoDelegate.addUndoNew( workflowMeta, new ActionCopy[] { jge }, new int[] { workflowMeta.indexOfAction( jge ) } );
            workflowGraph.updateGui();
            return jge;
          } else {
            return null;
          }
        } else {
          ActionCopy jge = new ActionCopy();
          jge.setAction( action );
          if ( location != null ) {
            jge.setLocation( location.x, location.y );
          } else {
            jge.setLocation( 50, 50 );
          }
          jge.setNr( 0 );
          workflowMeta.addAction( jge );
          hopGui.undoDelegate.addUndoNew( workflowMeta, new ActionCopy[] { jge }, new int[] { workflowMeta.indexOfAction( jge ) } );
          workflowGraph.updateGui();
          return jge;
        }
      } else {
        return null;
      }
    } catch ( Throwable e ) {
      new ErrorDialog( hopGui.getShell(),
        BaseMessages.getString( PKG, "HopGui.ErrorDialog.UnexpectedErrorCreatingNewJobGraphEntry.Title" ),
        BaseMessages.getString( PKG, "HopGui.ErrorDialog.UnexpectedErrorCreatingNewJobGraphEntry.Message" ),
        new Exception( e ) );
      return null;
    }
  }


  public IActionDialog getActionDialog( IAction action, WorkflowMeta workflowMeta ) throws HopException{
    Class<?>[] paramClasses = new Class<?>[] { Shell.class, IAction.class, WorkflowMeta.class };
    Object[] paramArgs = new Object[] { hopGui.getShell(), action, workflowMeta };

    PluginRegistry registry = PluginRegistry.getInstance();
    IPlugin plugin = registry.getPlugin( ActionPluginType.class, action );
    String dialogClassName = plugin.getClassMap().get( IActionDialog.class );
    if ( dialogClassName == null ) {
      throw new HopException( "Unable to find dialog class for plugin '" + plugin.getIds()[ 0 ] + "' : " + plugin.getName() );
    }

    try {
      Class<IActionDialog> dialogClass = registry.getClass( plugin, dialogClassName );
      Constructor<IActionDialog> dialogConstructor = dialogClass.getConstructor( paramClasses );
      IActionDialog entryDialogInterface = dialogConstructor.newInstance( paramArgs );
      entryDialogInterface.setMetaStore( hopGui.getMetaStore() );
      return entryDialogInterface;
    } catch ( Throwable t ) {
      t.printStackTrace();
      String errorTitle = BaseMessages.getString( PKG, "HopGui.Dialog.ErrorCreatingWorkflowDialog.Title" );
      String errorMsg = BaseMessages.getString( PKG, "HopGui.Dialog.ErrorCreatingActionDialog.Message", dialogClassName );
      hopGui.getLog().logError( errorMsg );
      new ErrorDialog( hopGui.getShell(), errorTitle, errorMsg, t );
      return null;
    }
  }

  public void editAction( WorkflowMeta workflowMeta, ActionCopy action ) {
    try {
      hopGui.getLog().logBasic( BaseMessages.getString( PKG, "HopGui.Log.EditAction", action.getName() ) );

      ActionCopy before = (ActionCopy) action.cloneDeep();

      IAction jei = action.getAction();

      IActionDialog d = getActionDialog( jei, workflowMeta );
      if ( d != null ) {
        if ( d.open() != null ) {
          // First see if the name changed.
          // If so, we need to verify that the name is not already used in the workflow.
          //
          workflowMeta.renameActionIfNameCollides( action );

          ActionCopy after = (ActionCopy) action.clone();
          hopGui.undoDelegate.addUndoChange( workflowMeta, new ActionCopy[] { before }, new ActionCopy[] { after }, new int[] { workflowMeta.indexOfAction( action ) } );
          workflowGraph.updateGui();
        }
      } else {
        MessageBox mb = new MessageBox( hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION );
        mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.ActionCanNotBeChanged.Message" ) );
        mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.ActionCanNotBeChanged.Title" ) );
        mb.open();
      }

    } catch ( Exception e ) {
      if ( !hopGui.getShell().isDisposed() ) {
        new ErrorDialog( hopGui.getShell(),
          BaseMessages.getString( PKG, "HopGui.ErrorDialog.ErrorEditingAction.Title" ),
          BaseMessages.getString( PKG, "HopGui.ErrorDialog.ErrorEditingAction.Message" ), e );
      }
    }
  }

  public void deleteJobEntryCopies( WorkflowMeta workflow, List<ActionCopy> actions ) {

    // Hops belonging to the deleting actions are placed in a single transaction and removed.
    List<WorkflowHopMeta> jobHops = new ArrayList<>();
    int[] hopIndexes = new int[ workflow.nrWorkflowHops() ];
    int hopIndex = 0;
    for ( int i = workflow.nrWorkflowHops() - 1; i >= 0; i-- ) {
      WorkflowHopMeta hi = workflow.getWorkflowHop( i );
      for ( int j = 0; j < actions.size() && hopIndex < hopIndexes.length; j++ ) {
        if ( hi.getFromAction().equals( actions.get( j ) ) || hi.getToAction().equals( actions.get( j ) ) ) {
          int idx = workflow.indexOfWorkflowHop( hi );
          jobHops.add( (WorkflowHopMeta) hi.clone() );
          hopIndexes[ hopIndex ] = idx;
          workflow.removeWorkflowHop( idx );
          hopIndex++;
          break;
        }
      }
    }
    if ( !jobHops.isEmpty() ) {
      WorkflowHopMeta[] hops = jobHops.toArray( new WorkflowHopMeta[ jobHops.size() ] );
      hopGui.undoDelegate.addUndoDelete( workflow, hops, hopIndexes );
    }

    // Deleting actions are placed all in a single transaction and removed.
    int[] positions = new int[ actions.size() ];
    for ( int i = 0; i < actions.size(); i++ ) {
      int pos = workflow.indexOfAction( actions.get( i ) );
      workflow.removeAction( pos );
      positions[ i ] = pos;
    }
    hopGui.undoDelegate.addUndoDelete( workflow, actions.toArray( new ActionCopy[ 0 ] ), positions );

    workflowGraph.updateGui();
  }

  public void deleteJobEntryCopies( WorkflowMeta workflowMeta, ActionCopy action ) {
    for ( int i = workflowMeta.nrWorkflowHops() - 1; i >= 0; i-- ) {
      WorkflowHopMeta hi = workflowMeta.getWorkflowHop( i );
      if ( hi.getFromAction().equals( action ) || hi.getToAction().equals( action ) ) {
        int idx = workflowMeta.indexOfWorkflowHop( hi );
        hopGui.undoDelegate.addUndoDelete( workflowMeta, new WorkflowHopMeta[] { (WorkflowHopMeta) hi.clone() }, new int[] { idx } );
        workflowMeta.removeWorkflowHop( idx );
      }
    }

    int pos = workflowMeta.indexOfAction( action );
    workflowMeta.removeAction( pos );
    hopGui.undoDelegate.addUndoDelete( workflowMeta, new ActionCopy[] { action }, new int[] { pos } );

    workflowGraph.updateGui();
  }

  public void dupeJobEntry( WorkflowMeta workflowMeta, ActionCopy action ) {
    if ( action == null ) {
      return;
    }

    if ( action.isStart() ) {
      MessageBox mb = new MessageBox( hopGui.getShell(), SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.OnlyUseStartOnce.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.OnlyUseStartOnce.Title" ) );
      mb.open();
      return;
    }

    ActionCopy copyOfAction = action.clone();
    copyOfAction.setNr( workflowMeta.findUnusedNr( copyOfAction.getName() ) );

    Point p = action.getLocation();
    copyOfAction.setLocation( p.x + 10, p.y + 10 );

    workflowMeta.addAction( copyOfAction );

    workflowGraph.updateGui();
  }


}
