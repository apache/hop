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

import org.apache.hop.cluster.ClusterSchema;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.dnd.DragAndDropContainer;
import org.apache.hop.core.dnd.XMLTransfer;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.plugins.JobEntryPluginType;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.PluginTypeInterface;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.trans.TransHopMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.ui.core.ConstUI;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.hopui.TreeSelection;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DragSource;
import org.eclipse.swt.dnd.DragSourceEvent;
import org.eclipse.swt.dnd.DragSourceListener;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

import java.util.ArrayList;
import java.util.List;

public class HopUiTreeDelegate extends HopUiDelegate {
  public HopUiTreeDelegate( HopUi hopUi ) {
    super( hopUi );
  }

  /**
   * @return The object that is selected in the tree or null if we couldn't figure it out. (titles etc. == null)
   */
  public TreeSelection[] getTreeObjects( final Tree tree, Tree coreObjectsTree ) {
    List<TreeSelection> objects = new ArrayList<TreeSelection>();

    if ( tree != null && coreObjectsTree != null && tree.equals( coreObjectsTree ) ) {
      TreeItem[] selection = coreObjectsTree.getSelection();
      for ( int s = 0; s < selection.length; s++ ) {
        TreeItem treeItem = selection[ s ];
        String[] path = ConstUI.getTreeStrings( treeItem );

        TreeSelection object = null;

        switch ( path.length ) {
          case 0:
            break;
          case 2: // Job entries
            if ( hopUi.showJob ) {
              PluginRegistry registry = PluginRegistry.getInstance();
              Class<? extends PluginTypeInterface> pluginType = JobEntryPluginType.class;
              PluginInterface plugin = registry.findPluginWithName( pluginType, path[ 1 ] );

              // Retry for Start
              //
              if ( plugin == null ) {
                if ( path[ 1 ].equalsIgnoreCase( JobMeta.STRING_SPECIAL_START ) ) {
                  plugin = registry.findPluginWithId( pluginType, JobMeta.STRING_SPECIAL );
                }
              }
              // Retry for Dummy
              //
              if ( plugin == null ) {
                if ( path[ 1 ].equalsIgnoreCase( JobMeta.STRING_SPECIAL_DUMMY ) ) {
                  plugin = registry.findPluginWithId( pluginType, JobMeta.STRING_SPECIAL );
                }
              }

              if ( plugin != null ) {
                object = new TreeSelection( path[ 1 ], plugin );
              }
            }

            if ( hopUi.showTrans ) {
              String stepId = (String) treeItem.getData( "StepId" );

              if ( stepId != null ) {
                object = new TreeSelection( path[ 1 ], PluginRegistry.getInstance().findPluginWithId( StepPluginType.class, stepId ) );
              } else {
                object = new TreeSelection( path[ 1 ], PluginRegistry.getInstance().findPluginWithName( StepPluginType.class, path[ 1 ] ) );
              }
            }
            break;
          default:
            break;
        }

        if ( object != null ) {
          objects.add( object );
        }
      }
    }

    return objects.toArray( new TreeSelection[ objects.size() ] );
  }

  public void addDragSourceToTree( final Tree tree, final Tree coreObjectsTree ) {
    // Drag & Drop for steps
    Transfer[] ttypes = new Transfer[] { XMLTransfer.getInstance() };

    DragSource ddSource = new DragSource( tree, DND.DROP_MOVE );
    ddSource.setTransfer( ttypes );
    ddSource.addDragListener( new DragSourceListener() {
      public void dragStart( DragSourceEvent event ) {
        TreeSelection[] treeObjects = getTreeObjects( tree, coreObjectsTree );
        if ( treeObjects.length == 0 ) {
          event.doit = false;
          return;
        }

        hopUi.hideToolTips();

        TreeSelection treeObject = treeObjects[ 0 ];
        Object object = treeObject.getSelection();
        TransMeta transMeta = hopUi.getActiveTransformation();
        // JobMeta jobMeta = spoon.getActiveJob();

        if ( object instanceof StepMeta
          || object instanceof PluginInterface || ( object instanceof DatabaseMeta && transMeta != null )
          || object instanceof TransHopMeta || object instanceof JobEntryCopy ) {
          event.doit = true;
        } else {
          event.doit = false;
        }
      }

      public void dragSetData( DragSourceEvent event ) {
        TreeSelection[] treeObjects = getTreeObjects( tree, coreObjectsTree );
        if ( treeObjects.length == 0 ) {
          event.doit = false;
          return;
        }

        int type = 0;
        String id = null;
        String data = null;

        TreeSelection treeObject = treeObjects[ 0 ];
        Object object = treeObject.getSelection();

        if ( object instanceof StepMeta ) {
          StepMeta stepMeta = (StepMeta) object;
          type = DragAndDropContainer.TYPE_STEP;
          data = stepMeta.getName(); // name of the step.
        } else if ( object instanceof PluginInterface ) {
          PluginInterface plugin = (PluginInterface) object;
          Class<? extends PluginTypeInterface> pluginType = plugin.getPluginType();
          if ( Const.classIsOrExtends( pluginType, StepPluginType.class ) ) {
            type = DragAndDropContainer.TYPE_BASE_STEP_TYPE;
            id = plugin.getIds()[ 0 ];
            data = plugin.getName(); // Step type name
          } else {
            type = DragAndDropContainer.TYPE_BASE_JOB_ENTRY;
            data = plugin.getName(); // job entry type name
            if ( treeObject.getItemText().equals( JobMeta.createStartEntry().getName() ) ) {
              data = treeObject.getItemText();
            } else if ( treeObject.getItemText().equals( JobMeta.createDummyEntry().getName() ) ) {
              data = treeObject.getItemText();
            }
          }
        } else if ( object instanceof DatabaseMeta ) {
          DatabaseMeta databaseMeta = (DatabaseMeta) object;
          type = DragAndDropContainer.TYPE_DATABASE_CONNECTION;
          data = databaseMeta.getName();
        } else if ( object instanceof TransHopMeta ) {
          TransHopMeta hop = (TransHopMeta) object;
          type = DragAndDropContainer.TYPE_TRANS_HOP;
          data = hop.toString(); // nothing for really ;-)
        } else if ( object instanceof JobEntryCopy ) {
          JobEntryCopy jobEntryCopy = (JobEntryCopy) object;
          type = DragAndDropContainer.TYPE_JOB_ENTRY;
          data = jobEntryCopy.getName(); // name of the job entry.
        } else {
          event.doit = false;
          return; // ignore anything else you drag.
        }

        event.data = new DragAndDropContainer( type, data, id );
      }

      public void dragFinished( DragSourceEvent event ) {
      }
    } );

  }

  private void executeExtensionPoint( HopUiTreeDelegateExtension extension ) {
    try {
      ExtensionPointHandler
        .callExtensionPoint( log, HopExtensionPoint.HopUiTreeDelegateExtension.id, extension );
    } catch ( Exception e ) {
      log.logError( "Error handling HopUiTreeDelegate through extension point", e );
    }
  }

}
