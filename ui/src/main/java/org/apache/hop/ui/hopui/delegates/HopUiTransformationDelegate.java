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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.hop.ui.hopui.HopUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.EngineMetaInterface;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.Result;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.HopUiInterface;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.TransLogTable;
import org.apache.hop.core.undo.TransAction;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransExecutionConfiguration;
import org.apache.hop.trans.TransHopMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.cluster.TransSplitter;
import org.apache.hop.trans.debug.StepDebugMeta;
import org.apache.hop.trans.debug.TransDebugMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.hopui.TabMapEntry;
import org.apache.hop.ui.hopui.TabMapEntry.ObjectType;
import org.apache.hop.ui.hopui.job.JobGraph;
import org.apache.hop.ui.hopui.trans.TransGraph;
import org.apache.hop.ui.trans.debug.TransDebugDialog;
import org.apache.hop.ui.trans.dialog.TransExecutionConfigurationDialog;
import org.apache.xul.swt.tab.TabItem;

public class HopUiTransformationDelegate extends HopUiDelegate {
  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!

  /**
   * This contains a map between the name of a transformation and the TransMeta object. If the transformation has no
   * name it will be mapped under a number [1], [2] etc.
   */
  private List<TransMeta> transformationMap;

  /**
   * Remember the debugging configuration per transformation
   */
  private Map<TransMeta, TransDebugMeta> transDebugMetaMap;

  /**
   * Remember the preview configuration per transformation
   */
  private Map<TransMeta, TransDebugMeta> transPreviewMetaMap;

  public HopUiTransformationDelegate( HopUi hopUi ) {
    super( hopUi );
    transformationMap = new ArrayList<TransMeta>();
    transDebugMetaMap = new Hashtable<TransMeta, TransDebugMeta>();
    transPreviewMetaMap = new Hashtable<TransMeta, TransDebugMeta>();
  }

  /**
   * Add a transformation to the
   *
   * @param transMeta
   *          the transformation to add to the map
   * @return true if the transformation was added, false if it couldn't be added (already loaded)
   **/
  public boolean addTransformation( TransMeta transMeta ) {
    int index = getTransformationList().indexOf( transMeta );
    if ( index < 0 ) {
      getTransformationList().add( transMeta );
      return true;
    } else {
      /*
       * ShowMessageDialog dialog = new ShowMessageDialog(spoon.getShell(), SWT.OK | SWT.ICON_INFORMATION,
       * BaseMessages.getString(PKG, "Spoon.Dialog.TransAlreadyLoaded.Title"), "'" + transMeta.toString() + "'" +
       * Const.CR + Const.CR + BaseMessages.getString(PKG, "Spoon.Dialog.TransAlreadyLoaded.Message"));
       * dialog.setTimeOut(6); dialog.open();
       */
      return false;
    }
  }

  /**
   * @param transMeta
   *          the transformation to close, make sure it's ok to dispose of it BEFORE you call this.
   */
  public synchronized void closeTransformation( TransMeta transMeta ) {
    // Close the associated tabs...
    //
    TabMapEntry entry = getSpoon().delegates.tabs.findTabMapEntry( transMeta );
    if ( entry != null ) {
      getSpoon().delegates.tabs.removeTab( entry );
    }

    // Also remove it from the item from the transformationMap
    // Otherwise it keeps showing up in the objects tree
    // Look for the transformation, not the key (name might have changed)
    //
    int index = getTransformationList().indexOf( transMeta );
    while ( index >= 0 ) {
      getTransformationList().remove( index );
      index = getTransformationList().indexOf( transMeta );
    }

    getSpoon().refreshTree();
    getSpoon().enableMenus();
  }

  public HopUi getSpoon() {
    return this.hopUi;
  }

  public void addTransGraph( TransMeta transMeta ) {
    boolean added = addTransformation( transMeta );
    if ( added ) {
      // See if there already is a tab for this graph with the short default name.
      // If there is, set that one to show the location as well.
      // If not, simply add it without
      // If no, add it
      // If yes, select that tab
      //
      boolean showLocation = false;
      boolean addTab = true;
      String tabName = hopUi.delegates.tabs.makeTabName( transMeta, showLocation );

      TabMapEntry tabEntry = hopUi.delegates.tabs.findTabMapEntry( tabName, ObjectType.TRANSFORMATION_GRAPH );
      if ( tabEntry != null ) {
        // We change the already loaded transformation to also show the location.
        //
        showLocation = true;

        // Try again, including the location of the object...
        //
        tabName = hopUi.delegates.tabs.makeTabName( transMeta, showLocation );

        TabMapEntry exactSameEntry = hopUi.delegates.tabs.findTabMapEntry( tabName, ObjectType.TRANSFORMATION_GRAPH );
        if ( exactSameEntry != null ) {
          // Already loaded, simply select the tab item in question...
          //
          addTab = false;
        } else {
          // We might need to rename the tab of the entry already loaded!
          //
          tabEntry.setShowingLocation( true );
          String newTabName = hopUi.delegates.tabs.makeTabName( tabEntry.getObject().getMeta(), showLocation );
          tabEntry.getTabItem().setText( newTabName );
        }
      }

      TransGraph transGraph = null;
      if ( addTab ) {
        transGraph = new TransGraph( hopUi.tabfolder.getSwtTabset(), hopUi, transMeta );
        PropsUI props = PropsUI.getInstance();
        if ( tabName.length() >= getMaxTabLength() ) {
          tabName = new StringBuilder().append( tabName.substring( 0, getMaxTabLength() ) ).append( "\u2026" ).toString();
        }
        TabItem tabItem = new TabItem( hopUi.tabfolder, tabName, tabName, props.getSashWeights() );
        String toolTipText =
            BaseMessages.getString( PKG, "Spoon.TabTrans.Tooltip", hopUi.delegates.tabs.makeTabName( transMeta,
                showLocation ) );
        if ( !Utils.isEmpty( transMeta.getFilename() ) ) {
          toolTipText += Const.CR + Const.CR + transMeta.getFilename();
        }
        tabItem.setToolTipText( toolTipText );
        tabItem.setImage( GUIResource.getInstance().getImageTransGraph() );
        tabItem.setControl( transGraph );
        TransLogTable logTable = transMeta.getTransLogTable();

        String versionLabel = transMeta.getObjectRevision() == null ? null : transMeta.getObjectRevision().getName();

        tabEntry =
            new TabMapEntry( tabItem, transMeta.getFilename(), transMeta.getName(), transMeta.getRepositoryDirectory(),
                versionLabel, transGraph, ObjectType.TRANSFORMATION_GRAPH );
        tabEntry.setShowingLocation( showLocation );

        hopUi.delegates.tabs.addTab( tabEntry );
      }

      int idx = hopUi.tabfolder.indexOf( tabEntry.getTabItem() );

      // keep the focus on the graph
      hopUi.tabfolder.setSelected( idx );

      if ( addTab ) {
        TransLogTable logTable = transMeta.getTransLogTable();
        // OK, also see if we need to open a new history window.
        if ( isLogTableDefined( logTable ) && !transMeta.isSlaveTransformation() ) {
          addTabsToTransGraph( transGraph );
        }
      }

      hopUi.setUndoMenu( transMeta );
      hopUi.enableMenus();
    } else {
      TabMapEntry tabEntry = hopUi.delegates.tabs.findTabMapEntry( transMeta );
      if ( tabEntry != null ) {
        int idx = hopUi.tabfolder.indexOf( tabEntry.getTabItem() );

        // keep the focus on the graph
        hopUi.tabfolder.setSelected( idx );

        hopUi.setUndoMenu( transMeta );
        hopUi.enableMenus();
      }
    }
  }

  boolean isLogTableDefined( TransLogTable logTable ) {
    return logTable.getDatabaseMeta() != null && !Utils.isEmpty( logTable.getTableName() );
  }

  void addTabsToTransGraph( TransGraph transGraph ) {
    transGraph.addAllTabs();
    transGraph.extraViewTabFolder.setSelection( transGraph.transHistoryDelegate.getTransHistoryTab() );
  }

  public void tabSelected( TabItem item ) {
    List<TabMapEntry> collection = hopUi.delegates.tabs.getTabs();

    // See which core objects to show
    //
    for ( TabMapEntry entry : collection ) {
      if ( item.equals( entry.getTabItem() ) ) {
        // TabItemInterface itemInterface = entry.getObject();

        //
        // Another way to implement this may be to keep track of the
        // state of the core object tree in method
        // addCoreObjectsToTree()
        //
        if ( entry.getObject() instanceof TransGraph || entry.getObject() instanceof JobGraph ) {
          EngineMetaInterface meta = entry.getObject().getMeta();
          if ( meta != null ) {
            meta.setInternalHopVariables();
          }
          if ( hopUi.getCoreObjectsState() != HopUiInterface.STATE_CORE_OBJECTS_SPOON ) {
            hopUi.refreshCoreObjects();
          }
        }
      }
    }

    // Also refresh the tree
    hopUi.refreshTree();
    hopUi.enableMenus();
  }

  public List<TransMeta> getTransformationList() {
    return transformationMap;
  }

  public TransMeta getTransformation( String name ) {
    TabMapEntry entry = hopUi.delegates.tabs.findTabMapEntry( name, ObjectType.TRANSFORMATION_GRAPH );
    if ( entry != null ) {
      return (TransMeta) entry.getObject().getManagedObject();
    }
    // Try again, TODO: remove part below
    //
    for ( TransMeta xform : transformationMap ) {
      if ( name != null && name.equals( xform.getName() ) ) {
        return xform;
      }
    }
    return null;
  }

  public void removeTransformation( TransMeta transMeta ) {
    transformationMap.remove( transMeta );
  }

  public TransMeta[] getLoadedTransformations() {
    return transformationMap.toArray( new TransMeta[transformationMap.size()] );
  }

  public TransGraph findTransGraphOfTransformation( TransMeta transMeta ) {
    // Now loop over the entries in the tab-map
    for ( TabMapEntry mapEntry : hopUi.delegates.tabs.getTabs() ) {
      if ( mapEntry.getObject() instanceof TransGraph ) {
        TransGraph transGraph = (TransGraph) mapEntry.getObject();
        if ( transGraph.getMeta().equals( transMeta ) ) {
          return transGraph;
        }
      }
    }
    return null;
  }

  public boolean isDefaultTransformationName( String name ) {
    if ( !name.startsWith( HopUi.STRING_TRANSFORMATION ) ) {
      return false;
    }

    // see if there are only digits behind the transformation...
    // This will detect:
    // "Transformation"
    // "Transformation "
    // "Transformation 1"
    // "Transformation 2"
    // ...
    for ( int i = HopUi.STRING_TRANSFORMATION.length() + 1; i < name.length(); i++ ) {
      if ( !Character.isDigit( name.charAt( i ) ) ) {
        return false;
      }
    }
    return true;
  }

  public void undoTransformationAction( TransMeta transMeta, TransAction transAction ) {
    switch ( transAction.getType() ) {
      // We created a new step : undo this...
      case TransAction.TYPE_ACTION_NEW_STEP:
        // Delete the step at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[i];
          transMeta.removeStep( idx );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We created a new connection : undo this...
      case TransAction.TYPE_ACTION_NEW_CONNECTION:
        // Delete the connection at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[i];
          transMeta.removeDatabase( idx );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We created a new note : undo this...
      case TransAction.TYPE_ACTION_NEW_NOTE:
        // Delete the note at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[i];
          transMeta.removeNote( idx );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We created a new hop : undo this...
      case TransAction.TYPE_ACTION_NEW_HOP:
        // Delete the hop at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[i];
          transMeta.removeTransHop( idx );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We created a new slave : undo this...
      case TransAction.TYPE_ACTION_NEW_SLAVE:
        // Delete the slave at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[i];
          transMeta.getSlaveServers().remove( idx );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We created a new slave : undo this...
      case TransAction.TYPE_ACTION_NEW_CLUSTER:
        // Delete the slave at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[i];
          transMeta.getClusterSchemas().remove( idx );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      //
      // DELETE
      //

      // We delete a step : undo this...
      case TransAction.TYPE_ACTION_DELETE_STEP:
        // un-Delete the step at correct location: re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          StepMeta stepMeta = (StepMeta) transAction.getCurrent()[i];
          int idx = transAction.getCurrentIndex()[i];
          transMeta.addStep( idx, stepMeta );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We deleted a connection : undo this...
      case TransAction.TYPE_ACTION_DELETE_CONNECTION:
        // re-insert the connection at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          DatabaseMeta ci = (DatabaseMeta) transAction.getCurrent()[i];
          int idx = transAction.getCurrentIndex()[i];
          transMeta.addDatabase( idx, ci );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We delete new note : undo this...
      case TransAction.TYPE_ACTION_DELETE_NOTE:
        // re-insert the note at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          NotePadMeta ni = (NotePadMeta) transAction.getCurrent()[i];
          int idx = transAction.getCurrentIndex()[i];
          transMeta.addNote( idx, ni );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We deleted a hop : undo this...
      case TransAction.TYPE_ACTION_DELETE_HOP:
        // re-insert the hop at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          TransHopMeta hi = (TransHopMeta) transAction.getCurrent()[i];
          int idx = transAction.getCurrentIndex()[i];
          // Build a new hop:
          StepMeta from = transMeta.findStep( hi.getFromStep().getName() );
          StepMeta to = transMeta.findStep( hi.getToStep().getName() );
          TransHopMeta hinew = new TransHopMeta( from, to );
          transMeta.addTransHop( idx, hinew );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      //
      // CHANGE
      //

      // We changed a step : undo this...
      case TransAction.TYPE_ACTION_CHANGE_STEP:
        // Delete the current step, insert previous version.
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          StepMeta prev = (StepMeta) ( (StepMeta) transAction.getPrevious()[i] ).clone();
          int idx = transAction.getCurrentIndex()[i];

          transMeta.getStep( idx ).replaceMeta( prev );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We changed a connection : undo this...
      case TransAction.TYPE_ACTION_CHANGE_CONNECTION:
        // Delete & re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          DatabaseMeta prev = (DatabaseMeta) transAction.getPrevious()[i];
          int idx = transAction.getCurrentIndex()[i];

          transMeta.getDatabase( idx ).replaceMeta( (DatabaseMeta) prev.clone() );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We changed a note : undo this...
      case TransAction.TYPE_ACTION_CHANGE_NOTE:
        // Delete & re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          int idx = transAction.getCurrentIndex()[i];
          transMeta.removeNote( idx );
          NotePadMeta prev = (NotePadMeta) transAction.getPrevious()[i];
          transMeta.addNote( idx, (NotePadMeta) prev.clone() );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We changed a hop : undo this...
      case TransAction.TYPE_ACTION_CHANGE_HOP:
        // Delete & re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          TransHopMeta prev = (TransHopMeta) transAction.getPrevious()[i];
          int idx = transAction.getCurrentIndex()[i];

          transMeta.removeTransHop( idx );
          transMeta.addTransHop( idx, (TransHopMeta) prev.clone() );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      //
      // POSITION
      //

      // The position of a step has changed: undo this...
      case TransAction.TYPE_ACTION_POSITION_STEP:
        // Find the location of the step:
        for ( int i = 0; i < transAction.getCurrentIndex().length; i++ ) {
          StepMeta stepMeta = transMeta.getStep( transAction.getCurrentIndex()[i] );
          stepMeta.setLocation( transAction.getPreviousLocation()[i] );
        }
        hopUi.refreshGraph();
        break;

      // The position of a note has changed: undo this...
      case TransAction.TYPE_ACTION_POSITION_NOTE:
        for ( int i = 0; i < transAction.getCurrentIndex().length; i++ ) {
          int idx = transAction.getCurrentIndex()[i];
          NotePadMeta npi = transMeta.getNote( idx );
          Point prev = transAction.getPreviousLocation()[i];
          npi.setLocation( prev );
        }
        hopUi.refreshGraph();
        break;
      default:
        break;
    }

    // OK, now check if we need to do this again...
    if ( transMeta.viewNextUndo() != null ) {
      if ( transMeta.viewNextUndo().getNextAlso() ) {
        hopUi.undoAction( transMeta );
      }
    }
  }

  public void redoTransformationAction( TransMeta transMeta, TransAction transAction ) {
    switch ( transAction.getType() ) {
      case TransAction.TYPE_ACTION_NEW_STEP:
        // re-delete the step at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          StepMeta stepMeta = (StepMeta) transAction.getCurrent()[i];
          int idx = transAction.getCurrentIndex()[i];
          transMeta.addStep( idx, stepMeta );

          hopUi.refreshTree();
          hopUi.refreshGraph();
        }
        break;

      case TransAction.TYPE_ACTION_NEW_CONNECTION:
        // re-insert the connection at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          DatabaseMeta ci = (DatabaseMeta) transAction.getCurrent()[i];
          int idx = transAction.getCurrentIndex()[i];
          transMeta.addDatabase( idx, ci );
          hopUi.refreshTree();
          hopUi.refreshGraph();
        }
        break;

      case TransAction.TYPE_ACTION_NEW_NOTE:
        // re-insert the note at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          NotePadMeta ni = (NotePadMeta) transAction.getCurrent()[i];
          int idx = transAction.getCurrentIndex()[i];
          transMeta.addNote( idx, ni );
          hopUi.refreshTree();
          hopUi.refreshGraph();
        }
        break;

      case TransAction.TYPE_ACTION_NEW_HOP:
        // re-insert the hop at correct location:
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          TransHopMeta hi = (TransHopMeta) transAction.getCurrent()[i];
          int idx = transAction.getCurrentIndex()[i];
          transMeta.addTransHop( idx, hi );
          hopUi.refreshTree();
          hopUi.refreshGraph();
        }
        break;

      //
      // DELETE
      //
      case TransAction.TYPE_ACTION_DELETE_STEP:
        // re-remove the step at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[i];
          transMeta.removeStep( idx );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      case TransAction.TYPE_ACTION_DELETE_CONNECTION:
        // re-remove the connection at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[i];
          transMeta.removeDatabase( idx );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      case TransAction.TYPE_ACTION_DELETE_NOTE:
        // re-remove the note at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[i];
          transMeta.removeNote( idx );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      case TransAction.TYPE_ACTION_DELETE_HOP:
        // re-remove the hop at correct location:
        for ( int i = transAction.getCurrent().length - 1; i >= 0; i-- ) {
          int idx = transAction.getCurrentIndex()[i];
          transMeta.removeTransHop( idx );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      //
      // CHANGE
      //

      // We changed a step : undo this...
      case TransAction.TYPE_ACTION_CHANGE_STEP:
        // Delete the current step, insert previous version.
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          StepMeta stepMeta = (StepMeta) ( (StepMeta) transAction.getCurrent()[i] ).clone();
          transMeta.getStep( transAction.getCurrentIndex()[i] ).replaceMeta( stepMeta );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We changed a connection : undo this...
      case TransAction.TYPE_ACTION_CHANGE_CONNECTION:
        // Delete & re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          DatabaseMeta databaseMeta = (DatabaseMeta) transAction.getCurrent()[i];
          int idx = transAction.getCurrentIndex()[i];

          transMeta.getDatabase( idx ).replaceMeta( (DatabaseMeta) databaseMeta.clone() );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We changed a note : undo this...
      case TransAction.TYPE_ACTION_CHANGE_NOTE:
        // Delete & re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          NotePadMeta ni = (NotePadMeta) transAction.getCurrent()[i];
          int idx = transAction.getCurrentIndex()[i];

          transMeta.removeNote( idx );
          transMeta.addNote( idx, (NotePadMeta) ni.clone() );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      // We changed a hop : undo this...
      case TransAction.TYPE_ACTION_CHANGE_HOP:
        // Delete & re-insert
        for ( int i = 0; i < transAction.getCurrent().length; i++ ) {
          TransHopMeta hi = (TransHopMeta) transAction.getCurrent()[i];
          int idx = transAction.getCurrentIndex()[i];

          transMeta.removeTransHop( idx );
          transMeta.addTransHop( idx, (TransHopMeta) hi.clone() );
        }
        hopUi.refreshTree();
        hopUi.refreshGraph();
        break;

      //
      // CHANGE POSITION
      //
      case TransAction.TYPE_ACTION_POSITION_STEP:
        for ( int i = 0; i < transAction.getCurrentIndex().length; i++ ) {
          // Find & change the location of the step:
          StepMeta stepMeta = transMeta.getStep( transAction.getCurrentIndex()[i] );
          stepMeta.setLocation( transAction.getCurrentLocation()[i] );
        }
        hopUi.refreshGraph();
        break;
      case TransAction.TYPE_ACTION_POSITION_NOTE:
        for ( int i = 0; i < transAction.getCurrentIndex().length; i++ ) {
          int idx = transAction.getCurrentIndex()[i];
          NotePadMeta npi = transMeta.getNote( idx );
          Point curr = transAction.getCurrentLocation()[i];
          npi.setLocation( curr );
        }
        hopUi.refreshGraph();
        break;
      default:
        break;
    }

    // OK, now check if we need to do this again...
    if ( transMeta.viewNextUndo() != null ) {
      if ( transMeta.viewNextUndo().getNextAlso() ) {
        hopUi.redoAction( transMeta );
      }
    }
  }

  public void executeTransformation( final TransMeta transMeta, final boolean local, final boolean remote,
      final boolean cluster, final boolean preview, final boolean debug, final Date replayDate, final boolean safe,
      LogLevel logLevel ) throws HopException {

    if ( transMeta == null ) {
      return;
    }

    // See if we need to ask for debugging information...
    //
    TransDebugMeta transDebugMeta = null;
    TransExecutionConfiguration executionConfiguration = null;

    if ( preview ) {
      executionConfiguration = hopUi.getTransPreviewExecutionConfiguration();
    } else if ( debug ) {
      executionConfiguration = hopUi.getTransDebugExecutionConfiguration();
    } else {
      executionConfiguration = hopUi.getTransExecutionConfiguration();
    }

    // Set defaults so the run configuration can set it up correctly
    executionConfiguration.setExecutingLocally( true );
    executionConfiguration.setExecutingRemotely( false );
    executionConfiguration.setExecutingClustered( false );

    // Set repository and safe mode information in both the exec config and the metadata
    transMeta.setRepository( hopUi.rep );
    transMeta.setMetaStore( hopUi.metaStore );

    executionConfiguration.setRepository( hopUi.rep );

    executionConfiguration.setSafeModeEnabled( safe );

    if ( debug ) {
      // See if we have debugging information stored somewhere?
      //
      transDebugMeta = transDebugMetaMap.get( transMeta );
      if ( transDebugMeta == null ) {
        transDebugMeta = new TransDebugMeta( transMeta );
        transDebugMetaMap.put( transMeta, transDebugMeta );
      }

      // Set the default number of rows to retrieve on all selected steps...
      //
      List<StepMeta> selectedSteps = transMeta.getSelectedSteps();
      if ( selectedSteps != null && selectedSteps.size() > 0 ) {
        transDebugMeta.getStepDebugMetaMap().clear();
        for ( StepMeta stepMeta : transMeta.getSelectedSteps() ) {
          StepDebugMeta stepDebugMeta = new StepDebugMeta( stepMeta );
          stepDebugMeta.setRowCount( PropsUI.getInstance().getDefaultPreviewSize() );
          stepDebugMeta.setPausingOnBreakPoint( true );
          stepDebugMeta.setReadingFirstRows( false );
          transDebugMeta.getStepDebugMetaMap().put( stepMeta, stepDebugMeta );
        }
      }

    } else if ( preview ) {
      // See if we have preview information stored somewhere?
      //
      transDebugMeta = transPreviewMetaMap.get( transMeta );
      if ( transDebugMeta == null ) {
        transDebugMeta = new TransDebugMeta( transMeta );

        transPreviewMetaMap.put( transMeta, transDebugMeta );
      }

      // Set the default number of preview rows on all selected steps...
      //
      List<StepMeta> selectedSteps = transMeta.getSelectedSteps();
      if ( selectedSteps != null && selectedSteps.size() > 0 ) {
        transDebugMeta.getStepDebugMetaMap().clear();
        for ( StepMeta stepMeta : transMeta.getSelectedSteps() ) {
          StepDebugMeta stepDebugMeta = new StepDebugMeta( stepMeta );
          stepDebugMeta.setRowCount( PropsUI.getInstance().getDefaultPreviewSize() );
          stepDebugMeta.setPausingOnBreakPoint( false );
          stepDebugMeta.setReadingFirstRows( true );
          transDebugMeta.getStepDebugMetaMap().put( stepMeta, stepDebugMeta );
        }
      }
    }

    int debugAnswer = TransDebugDialog.DEBUG_CONFIG;

    if ( debug || preview ) {
      transDebugMeta.getTransMeta().setRepository( hopUi.rep ); // pass repository for mappings
      TransDebugDialog transDebugDialog = new TransDebugDialog( hopUi.getShell(), transDebugMeta );
      debugAnswer = transDebugDialog.open();
      if ( debugAnswer != TransDebugDialog.DEBUG_CANCEL ) {
        executionConfiguration.setExecutingLocally( true );
        executionConfiguration.setExecutingRemotely( false );
        executionConfiguration.setExecutingClustered( false );
      } else {
        // If we cancel the debug dialog, we don't go further with the execution either.
        //
        return;
      }
    }

    Object[] data = hopUi.variables.getData();
    String[] fields = hopUi.variables.getRowMeta().getFieldNames();
    Map<String, String> variableMap = new HashMap<String, String>();
    variableMap.putAll( executionConfiguration.getVariables() ); // the default
    for ( int idx = 0; idx < fields.length; idx++ ) {
      String value = executionConfiguration.getVariables().get( fields[idx] );
      if ( Utils.isEmpty( value ) ) {
        value = data[idx].toString();
      }
      variableMap.put( fields[idx], value );
    }

    executionConfiguration.setVariables( variableMap );
    executionConfiguration.getUsedVariables( transMeta );
    executionConfiguration.getUsedArguments( transMeta, hopUi.getArguments() );
    executionConfiguration.setReplayDate( replayDate );

    executionConfiguration.setLogLevel( logLevel );

    boolean execConfigAnswer = true;

    if ( debugAnswer == TransDebugDialog.DEBUG_CONFIG && replayDate == null && transMeta.isShowDialog() ) {
      TransExecutionConfigurationDialog dialog =
          new TransExecutionConfigurationDialog( hopUi.getShell(), executionConfiguration, transMeta );
      execConfigAnswer = dialog.open();
    }

    if ( execConfigAnswer ) {
      TransGraph activeTransGraph = hopUi.getActiveTransGraph();
      activeTransGraph.transLogDelegate.addTransLog();

      // Set the named parameters
      Map<String, String> paramMap = executionConfiguration.getParams();
      for ( String key : paramMap.keySet() ) {
        transMeta.setParameterValue( key, Const.NVL( paramMap.get( key ), "" ) );
      }
      transMeta.activateParameters();

      // Set the log level
      //
      if ( executionConfiguration.getLogLevel() != null ) {
        transMeta.setLogLevel( executionConfiguration.getLogLevel() );
      }

      // Set the run options
      transMeta.setClearingLog( executionConfiguration.isClearingLog() );
      transMeta.setSafeModeEnabled( executionConfiguration.isSafeModeEnabled() );
      transMeta.setGatheringMetrics( executionConfiguration.isGatheringMetrics() );

      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiTransMetaExecutionStart.id, transMeta );
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiTransExecutionConfiguration.id,
          executionConfiguration );

      try {
        ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiTransBeforeStart.id, new Object[] {
          executionConfiguration, transMeta, transMeta, hopUi.getRepository()
        } );
      } catch ( HopException e ) {
        log.logError( e.getMessage(), transMeta.getFilename() );
        return;
      }

      if ( !executionConfiguration.isExecutingLocally() && !executionConfiguration.isExecutingRemotely() ) {
        if ( transMeta.hasChanged() ) {
          activeTransGraph.showSaveFileMessage();
        }
      }

      // Verify if there is at least one step specified to debug or preview...
      //
      if ( debug || preview ) {
        if ( transDebugMeta.getNrOfUsedSteps() == 0 ) {
          MessageBox box = new MessageBox( hopUi.getShell(), SWT.ICON_WARNING | SWT.YES | SWT.NO );
          box.setText( BaseMessages.getString( PKG, "Spoon.Dialog.Warning.NoPreviewOrDebugSteps.Title" ) );
          box.setMessage( BaseMessages.getString( PKG, "Spoon.Dialog.Warning.NoPreviewOrDebugSteps.Message" ) );
          int answer = box.open();
          if ( answer != SWT.YES ) {
            return;
          }
        }
      }

      // Is this a local execution?
      //
      if ( executionConfiguration.isExecutingLocally() ) {
        if ( debug || preview ) {
          activeTransGraph.debug( executionConfiguration, transDebugMeta );
        } else {
          activeTransGraph.start( executionConfiguration );
        }

        // Are we executing remotely?
        //
      } else if ( executionConfiguration.isExecutingRemotely() ) {
        activeTransGraph.handleTransMetaChanges( transMeta );
        if ( transMeta.hasChanged() ) {
          showSaveTransformationBeforeRunningDialog( hopUi.getShell() );
        } else if ( executionConfiguration.getRemoteServer() != null ) {
          String carteObjectId =
              Trans.sendToSlaveServer( transMeta, executionConfiguration, hopUi.rep, hopUi.metaStore );
          monitorRemoteTrans( transMeta, carteObjectId, executionConfiguration.getRemoteServer() );
          hopUi.delegates.slaves.addSpoonSlave( executionConfiguration.getRemoteServer() );

        } else {
          MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_INFORMATION );
          mb.setMessage( BaseMessages.getString( PKG, "Spoon.Dialog.NoRemoteServerSpecified.Message" ) );
          mb.setText( BaseMessages.getString( PKG, "Spoon.Dialog.NoRemoteServerSpecified.Title" ) );
          mb.open();
        }

        // Are we executing clustered?
        //
      } else if ( executionConfiguration.isExecutingClustered() ) {
        activeTransGraph.handleTransMetaChanges( transMeta );
        if ( transMeta.hasChanged() ) {
          showSaveTransformationBeforeRunningDialog( hopUi.getShell() );
        } else {
          splitTrans( transMeta, executionConfiguration );
        }
      }
    }
  }

  private static void showSaveTransformationBeforeRunningDialog( Shell shell ) {
    MessageBox m = new MessageBox( shell, SWT.OK | SWT.ICON_WARNING );
    m.setText( BaseMessages.getString( PKG, "TransLog.Dialog.SaveTransformationBeforeRunning.Title" ) );
    m.setMessage( BaseMessages.getString( PKG, "TransLog.Dialog.SaveTransformationBeforeRunning.Message" ) );
    m.open();
  }

  private void monitorRemoteTrans( final TransMeta transMeta, final String carteObjectId,
      final SlaveServer remoteSlaveServer ) {
    // There is a transformation running in the background. When it finishes, clean it up and log the result on the
    // console.
    // Launch in a separate thread to prevent GUI blocking...
    //
    Thread thread = new Thread( new Runnable() {
      public void run() {
        Trans.monitorRemoteTransformation( hopUi.getLog(), carteObjectId, transMeta.toString(), remoteSlaveServer );
      }
    } );

    thread.setName( "Monitor remote transformation '" + transMeta.getName() + "', carte object id=" + carteObjectId
        + ", slave server: " + remoteSlaveServer.getName() );
    thread.start();

  }

  protected void splitTrans( final TransMeta transMeta, final TransExecutionConfiguration executionConfiguration )
    throws HopException {
    try {
      final TransSplitter transSplitter = new TransSplitter( transMeta );

      transSplitter.splitOriginalTransformation();

      TransMeta master = transSplitter.getMaster();
      SlaveServer masterServer = null;
      List<StepMeta> masterSteps = master.getTransHopSteps( false );

      // add transgraph of transmetas if showing is true
      SlaveServer[] slaves = transSplitter.getSlaveTargets();

      if ( executionConfiguration.isClusterShowingTransformation() ) {
        if ( masterSteps.size() > 0 ) {
          // If there is something that needs to be done on the master...
          masterServer = transSplitter.getMasterServer();
          addTransGraph( master );
        }

        // Then the slaves...
        //
        for ( int i = 0; i < slaves.length; i++ ) {
          TransMeta slaveTrans = transSplitter.getSlaveTransMap().get( slaves[i] );
          addTransGraph( slaveTrans );
        }
      }

      // Inject certain internal variables to make it more intuitive.
      //
      for ( String var : Const.INTERNAL_TRANS_VARIABLES ) {
        executionConfiguration.getVariables().put( var, transMeta.getVariable( var ) );
      }
      for ( String var : Const.INTERNAL_JOB_VARIABLES ) {
        executionConfiguration.getVariables().put( var, transMeta.getVariable( var ) );
      }

      // Parameters override the variables.
      // For the time being we're passing the parameters over the wire as variables...
      //
      TransMeta ot = transSplitter.getOriginalTransformation();
      for ( String param : ot.listParameters() ) {
        String value =
            Const.NVL( ot.getParameterValue( param ), Const.NVL( ot.getParameterDefault( param ), ot.getVariable(
                param ) ) );
        if ( !Utils.isEmpty( value ) ) {
          executionConfiguration.getVariables().put( param, value );
        }
      }

      try {
        Trans.executeClustered( transSplitter, executionConfiguration );
      } catch ( Exception e ) {
        // Something happened posting the transformation to the cluster.
        // We need to make sure to de-allocate ports and so on for the next try...
        // We don't want to suppress original exception here.
        try {
          Trans.cleanupCluster( log, transSplitter );
        } catch ( Exception ee ) {
          throw new Exception( "Error executing transformation and error to clenaup cluster", e );
        }
        // we still have execution error but cleanup ok here...
        throw e;
      }

      if ( executionConfiguration.isClusterPosting() ) {
        // Now add monitors for the master and all the slave servers
        //
        if ( masterServer != null ) {
          hopUi.addSpoonSlave( masterServer );
          for ( int i = 0; i < slaves.length; i++ ) {
            hopUi.addSpoonSlave( slaves[i] );
          }
        }
      }

      // OK, we should also start monitoring of the cluster in the background.
      // Stop them all if one goes bad.
      // Also clean up afterwards, close sockets, etc.
      //
      // Launch in a separate thread to prevent GUI blocking...
      //
      new Thread( new Runnable() {
        public void run() {
          Trans.monitorClusteredTransformation( log, transSplitter, null );
          Result result = Trans.getClusteredTransformationResult( log, transSplitter, null );
          log.logBasic( "-----------------------------------------------------" );
          log.logBasic( "Got result back from clustered transformation:" );
          log.logBasic( transMeta.toString() + "-----------------------------------------------------" );
          log.logBasic( transMeta.toString() + " Errors : " + result.getNrErrors() );
          log.logBasic( transMeta.toString() + " Input : " + result.getNrLinesInput() );
          log.logBasic( transMeta.toString() + " Output : " + result.getNrLinesOutput() );
          log.logBasic( transMeta.toString() + " Updated : " + result.getNrLinesUpdated() );
          log.logBasic( transMeta.toString() + " Read : " + result.getNrLinesRead() );
          log.logBasic( transMeta.toString() + " Written : " + result.getNrLinesWritten() );
          log.logBasic( transMeta.toString() + " Rejected : " + result.getNrLinesRejected() );
          log.logBasic( transMeta.toString() + "-----------------------------------------------------" );
        }
      } ).start();

    } catch ( Exception e ) {
      throw new HopException( e );
    }
  }

}
