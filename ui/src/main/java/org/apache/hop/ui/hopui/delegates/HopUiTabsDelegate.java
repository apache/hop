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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.ui.hopui.HopUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.BrowserFunction;
import org.eclipse.swt.browser.LocationListener;
import org.eclipse.swt.browser.OpenWindowListener;
import org.eclipse.swt.browser.WindowEvent;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Composite;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.Const;
import org.apache.hop.core.EngineMetaInterface;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.HopUiInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.repository.ObjectRevision;
import org.apache.hop.repository.RepositoryOperation;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.repository.RepositorySecurityUI;
import org.apache.hop.ui.hopui.AbstractGraph;
import org.apache.hop.ui.hopui.HopUiBrowser;
import org.apache.hop.ui.hopui.TabItemInterface;
import org.apache.hop.ui.hopui.TabMapEntry;
import org.apache.hop.ui.hopui.TabMapEntry.ObjectType;
import org.apache.hop.ui.hopui.job.JobGraph;
import org.apache.hop.ui.hopui.trans.TransGraph;
import org.apache.xul.swt.tab.TabItem;
import org.apache.xul.swt.tab.TabSet;

public class HopUiTabsDelegate extends HopUiDelegate {
  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!

  /**
   * This contains a list of the tab map entries
   */
  private List<TabMapEntry> tabMap;

  public HopUiTabsDelegate( HopUi hopUi ) {
    super( hopUi );
    tabMap = new ArrayList<TabMapEntry>();
  }

  public boolean tabClose( TabItem item ) throws HopException {
    return tabClose( item, false );
  }

  public boolean tabClose( TabItem item, boolean force ) throws HopException {
    // Try to find the tab-item that's being closed.

    boolean createPerms = !RepositorySecurityUI
        .verifyOperations( HopUi.getInstance().getShell(), HopUi.getInstance().getRepository(), false,
            RepositoryOperation.MODIFY_TRANSFORMATION, RepositoryOperation.MODIFY_JOB );

    boolean close = true;
    boolean canSave = true;
    for ( TabMapEntry entry : tabMap ) {
      if ( item.equals( entry.getTabItem() ) ) {
        final TabItemInterface itemInterface = entry.getObject();
        final Object managedObject = itemInterface.getManagedObject();

        if ( !force ) {
          if ( managedObject != null
            && AbstractMeta.class.isAssignableFrom( managedObject.getClass() ) ) {
            canSave = !( (AbstractMeta) managedObject ).hasMissingPlugins();
          }

          if ( canSave ) {
            // Can we close this tab? Only allow users with create content perms to save
            if ( !itemInterface.canBeClosed() && createPerms ) {
              int reply = itemInterface.showChangedWarning();
              if ( reply == SWT.YES ) {
                close = itemInterface.applyChanges();
              } else {
                if ( reply == SWT.CANCEL ) {
                  close = false;
                } else {
                  close = true;
                }
              }
            }
          }
        }

        String beforeCloseId = null;
        String afterCloseId = null;

        if ( itemInterface instanceof TransGraph ) {
          beforeCloseId = HopExtensionPoint.TransBeforeClose.id;
          afterCloseId = HopExtensionPoint.TransAfterClose.id;
        } else if ( itemInterface instanceof JobGraph ) {
          beforeCloseId = HopExtensionPoint.JobBeforeClose.id;
          afterCloseId = HopExtensionPoint.JobAfterClose.id;
        }

        if ( beforeCloseId != null ) {
          try {
            ExtensionPointHandler.callExtensionPoint( log, beforeCloseId, managedObject );
          } catch ( HopException e ) {
            // prevent tab close
            close = false;
          }
        }

        // Also clean up the log/history associated with this
        // transformation/job
        //
        if ( close ) {
          if ( itemInterface instanceof TransGraph ) {
            TransMeta transMeta = (TransMeta) managedObject;
            hopUi.delegates.trans.closeTransformation( transMeta );
            hopUi.refreshTree();
            // spoon.refreshCoreObjects();
          } else if ( itemInterface instanceof JobGraph ) {
            JobMeta jobMeta = (JobMeta) managedObject;
            hopUi.delegates.jobs.closeJob( jobMeta );
            hopUi.refreshTree();
            // spoon.refreshCoreObjects();
          } else if ( itemInterface instanceof HopUiBrowser ) {
            this.removeTab( entry );
            hopUi.refreshTree();
          } else if ( itemInterface instanceof Composite ) {
            Composite comp = (Composite) itemInterface;
            if ( comp != null && !comp.isDisposed() ) {
              comp.dispose();
            }
          }

          if ( afterCloseId != null ) {
            try {
              ExtensionPointHandler.callExtensionPoint( log, afterCloseId, managedObject );
            } catch ( HopException e ) {
              // fails gracefully... what else could we do?
            }
          }
        }

        break;
      }
    }

    return close;
  }

  public void removeTab( TabMapEntry tabMapEntry ) {
    for ( TabMapEntry entry : getTabs() ) {
      if ( tabMapEntry.equals( entry ) ) {
        tabMap.remove( tabMapEntry );
      }
    }
    if ( !tabMapEntry.getTabItem().isDisposed() ) {
      tabMapEntry.getTabItem().dispose();
    }
  }

  public List<TabMapEntry> getTabs() {
    List<TabMapEntry> list = new ArrayList<TabMapEntry>();
    list.addAll( tabMap );
    return list;
  }

  public TabMapEntry getTab( TabItem tabItem ) {
    for ( TabMapEntry tabMapEntry : tabMap ) {
      if ( tabMapEntry.getTabItem().equals( tabItem ) ) {
        return tabMapEntry;
      }
    }
    return null;
  }

  public EngineMetaInterface getActiveMeta() {
    TabSet tabfolder = hopUi.tabfolder;
    if ( tabfolder == null ) {
      return null;
    }
    TabItem tabItem = tabfolder.getSelected();
    if ( tabItem == null ) {
      return null;
    }

    // What transformation is in the active tab?
    // TransLog, TransGraph & TransHist contain the same transformation
    //
    TabMapEntry mapEntry = getTab( tabfolder.getSelected() );
    EngineMetaInterface meta = null;
    if ( mapEntry != null ) {
      if ( mapEntry.getObject() instanceof TransGraph ) {
        meta = ( mapEntry.getObject() ).getMeta();
      }
      if ( mapEntry.getObject() instanceof JobGraph ) {
        meta = ( mapEntry.getObject() ).getMeta();
      }
    }

    return meta;
  }

  public String makeSlaveTabName( SlaveServer slaveServer ) {
    return "Slave server: " + slaveServer.getName();
  }

  public boolean addSpoonBrowser( String name, String urlString ) {
    return addSpoonBrowser( name, urlString, true, null, true );
  }

  public boolean addSpoonBrowser( String name, String urlString, boolean showControls ) {
    return addSpoonBrowser( name, urlString, true, null, showControls );
  }

  public boolean addSpoonBrowser( String name, String urlString, LocationListener listener ) {
    return addSpoonBrowser( name, urlString, true, listener, true );
  }

  public boolean addSpoonBrowser( String name, String urlString, LocationListener listener, boolean showControls ) {
    return addSpoonBrowser( name, urlString, true, listener, showControls );
  }

  public boolean addSpoonBrowser( String name, String urlString, boolean isURL, LocationListener listener ) {
    return addSpoonBrowser( name, urlString, isURL, listener, true );
  }

  public boolean addSpoonBrowser( String name, String urlString, boolean isURL, LocationListener listener, boolean showControls ) {
    return addSpoonBrowser( name, urlString, isURL, listener, null, showControls );
  }

  public boolean addSpoonBrowser( String name, String urlString, boolean isURL, LocationListener listener, Map<String, Runnable> functions, boolean showControls ) {
    TabSet tabfolder = hopUi.tabfolder;

    try {
      // OK, now we have the HTML, create a new browser tab.

      // See if there already is a tab for this browser
      // If no, add it
      // If yes, select that tab
      //
      TabMapEntry tabMapEntry = findTabMapEntry( name, ObjectType.BROWSER );
      if ( tabMapEntry == null ) {
        CTabFolder cTabFolder = tabfolder.getSwtTabset();
        final HopUiBrowser browser = new HopUiBrowser( cTabFolder, hopUi, urlString, isURL, showControls, listener );

        browser.getBrowser().addOpenWindowListener( new OpenWindowListener() {

          @Override
          public void open( WindowEvent event ) {
            if ( event.required ) {
              event.browser = browser.getBrowser();
            }
          }
        } );

        if ( functions != null ) {
          for ( String functionName : functions.keySet() ) {
            new BrowserFunction( browser.getBrowser(), functionName ) {
              public Object function( Object[] arguments ) {
                functions.get( functionName ).run();
                return null;
              }
            };
          }
        }

        new BrowserFunction( browser.getBrowser(), "genericFunction" ) {
          public Object function( Object[] arguments ) {
            try {
              ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiBrowserFunction.id, arguments );
            } catch ( HopException ignored ) {
            }
            return null;
          }
        };

        new BrowserFunction( browser.getBrowser(), "openURL" ) {
          public Object function( Object[] arguments ) {
            Program.launch( arguments[0].toString() );
            return null;
          }
        };

        PropsUI props = PropsUI.getInstance();
        TabItem tabItem = new TabItem( tabfolder, name, name, props.getSashWeights() );
        tabItem.setImage( GUIResource.getInstance().getImageLogoSmall() );
        tabItem.setControl( browser.getComposite() );

        tabMapEntry =
          new TabMapEntry( tabItem, isURL ? urlString : null, name, null, null, browser, ObjectType.BROWSER );
        tabMap.add( tabMapEntry );
      }
      int idx = tabfolder.indexOf( tabMapEntry.getTabItem() );

      // keep the focus on the graph
      tabfolder.setSelected( idx );
      return true;
    } catch ( Throwable e ) {
      boolean ok = false;
      if ( !ok ) {
        // Log an error
        //
        log.logError( "Unable to open browser tab for URL: "+urlString, e );
        return false;
      } else {
        return true;
      }
    }
  }

  public TabMapEntry findTabMapEntry( String tabItemText, ObjectType objectType ) {
    return tabMap.stream()
      .filter( tabMapEntry -> !tabMapEntry.getTabItem().isDisposed() )
      .filter( tabMapEntry -> tabMapEntry.getObjectType() == objectType )
      .filter( tabMapEntry -> tabMapEntry.getTabItem().getText().equalsIgnoreCase( tabItemText ) )
      .findFirst().orElse( null );
  }

  public TabMapEntry findTabMapEntry( Object managedObject ) {
    if ( managedObject == null ) {
      return null;
    }

    return tabMap.stream()
      .filter( tabMapEntry -> !tabMapEntry.getTabItem().isDisposed() )
      .filter( tabMapEntry -> Objects.nonNull( tabMapEntry.getObject().getManagedObject() ) )
      // make sure they are the same class before comparing them
      .filter( tabMapEntry -> tabMapEntry.getObject().getManagedObject().getClass().equals( managedObject.getClass() ) )
      .filter( tabMapEntry -> tabMapEntry.getObject().getManagedObject().equals( managedObject ) )
      .findFirst().orElse( null );
  }

  /**
   * Finds the tab for the transformation that matches the metadata provided (either the file must be the same or the
   * repository id).
   *
   * @param trans
   *          Transformation metadata to look for
   * @return Tab with transformation open whose metadata matches {@code trans} or {@code null} if no tab exists.
   * @throws HopFileException
   *           If there is a problem loading the file object for an open transformation with an invalid a filename.
   */
  public TabMapEntry findTabForTransformation( TransMeta trans ) throws HopFileException {
    // File for the transformation we're looking for. It will be loaded upon first request.
    FileObject transFile = null;
    for ( TabMapEntry entry : tabMap ) {
      if ( entry != null && !entry.getTabItem().isDisposed() ) {
        if ( trans.getFilename() != null && entry.getFilename() != null ) {
          // If the entry has a file name it is the same as trans iff. they originated from the same files
          FileObject entryFile = HopVFS.getFileObject( entry.getFilename() );
          if ( transFile == null ) {
            transFile = HopVFS.getFileObject( trans.getFilename() );
          }
          if ( entryFile.equals( transFile ) ) {
            return entry;
          }
        } else if ( trans.getObjectId() != null && entry.getObject() != null ) {
          EngineMetaInterface meta = entry.getObject().getMeta();
          if ( meta != null && trans.getObjectId().equals( meta.getObjectId() ) ) {
            // If the transformation has an object id and the entry shares the same id they are the same
            return entry;
          }
        }
      }
    }
    // No tabs for the transformation exist and are not disposed
    return null;
  }

  /**
   * Rename the tabs
   */
  public void renameTabs() {
    List<TabMapEntry> list = new ArrayList<TabMapEntry>( tabMap );
    for ( TabMapEntry entry : list ) {
      if ( entry.getTabItem().isDisposed() ) {
        // this should not be in the map, get rid of it.
        tabMap.remove( entry.getObjectName() );
        continue;
      }

      // TabItem before = entry.getTabItem();
      // PDI-1683: need to get the String here, otherwise using only the "before" instance below, the reference gets
      // changed and result is always the same
      // String beforeText=before.getText();
      //
      Object managedObject = entry.getObject().getManagedObject();
      if ( managedObject != null ) {
        if ( entry.getObject() instanceof AbstractGraph ) {
          AbstractMeta meta = (AbstractMeta) managedObject;
          String tabText = makeTabName( meta, entry.isShowingLocation() );
          entry.getTabItem().setText( tabText );
          String toolTipText = BaseMessages.getString( PKG, "Spoon.TabTrans.Tooltip", tabText );
          if ( entry.getObject() instanceof JobGraph ) {
            toolTipText = BaseMessages.getString( PKG, "Spoon.TabJob.Tooltip", tabText );
          }
          if ( Const.isWindows() && !Utils.isEmpty( meta.getFilename() ) ) {
            toolTipText += Const.CR + Const.CR + meta.getFilename();
          }
          entry.getTabItem().setToolTipText( toolTipText );
        }
      }
    }
    hopUi.setShellText();
  }

  public void addTab( TabMapEntry entry ) {
    tabMap.add( entry );
  }

  public String makeTabName( EngineMetaInterface transMeta, boolean showLocation ) {
    if ( Utils.isEmpty( transMeta.getName() ) && Utils.isEmpty( transMeta.getFilename() ) ) {
      return HopUi.STRING_TRANS_NO_NAME;
    }

    if ( Utils.isEmpty( transMeta.getName() )
      || hopUi.delegates.trans.isDefaultTransformationName( transMeta.getName() ) ) {
      transMeta.nameFromFilename();
    }

    String name = "";

    if ( showLocation ) {
      if ( !Utils.isEmpty( transMeta.getFilename() ) ) {
        // Regular file...
        //
        name += transMeta.getFilename() + " : ";
      } else {
        // Repository object...
        //
        name += transMeta.getRepositoryDirectory().getPath() + " : ";
      }
    }

    name += transMeta.getName();
    if ( showLocation ) {
      ObjectRevision version = transMeta.getObjectRevision();
      if ( version != null ) {
        name += " : r" + version.getName();
      }
    }
    return name;
  }

  public void tabSelected( TabItem item ) {
    // See which core objects to show
    //
    for ( TabMapEntry entry : tabMap ) {
      boolean isAbstractGraph = ( entry.getObject() instanceof AbstractGraph );
      if ( item.equals( entry.getTabItem() ) ) {
        if ( isAbstractGraph ) {
          EngineMetaInterface meta = entry.getObject().getMeta();
          if ( meta != null ) {
            meta.setInternalHopVariables();
          }
          if ( hopUi.getCoreObjectsState() != HopUiInterface.STATE_CORE_OBJECTS_SPOON ) {
            hopUi.refreshCoreObjects();
          }
          ( (AbstractGraph) entry.getObject() ).setFocus();
        }
        break;
      }
    }

    // Also refresh the tree
    hopUi.refreshTree();
    hopUi.setShellText(); // calls also enableMenus() and markTabsChanged()
  }

}
