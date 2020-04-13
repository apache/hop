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

package org.apache.hop.ui.hopgui.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.IMetaStoreAttribute;
import org.apache.hop.metastore.api.IMetaStoreElement;
import org.apache.hop.metastore.api.IMetaStoreElementType;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.stores.delegate.DelegatingMetaStore;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.core.widget.TreeUtil;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MenuDetectEvent;
import org.eclipse.swt.events.MenuDetectListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeColumn;
import org.eclipse.swt.widgets.TreeItem;

import java.util.ArrayList;
import java.util.List;

public class MetaStoreExplorerDialog {
  private static Class<?> PKG = MetaStoreExplorerDialog.class; // for i18n purposes, needed by Translator

  private static final String META_STORE_EXPLORER_DIALOG_TREE = "MetaStore explorer dialog tree";

  private static ILogChannel log = LogChannel.GENERAL;

  private IMetaStore metaStore;

  private List<IMetaStore> metaStoreList;

  private Shell parent;

  private Shell shell;

  private Tree tree;

  private PropsUi props;

  private Button closeButton;

  public MetaStoreExplorerDialog( Shell parent, IMetaStore metaStore ) {
    this.metaStore = metaStore;
    this.parent = parent;

    metaStoreList = new ArrayList<IMetaStore>();
    if ( metaStore instanceof DelegatingMetaStore ) {
      DelegatingMetaStore delegatingMetaStore = (DelegatingMetaStore) metaStore;
      log.logBasic( "Exploring delegating meta store containing " + delegatingMetaStore.getMetaStoreList().size() );
      for ( IMetaStore delMetaStore : delegatingMetaStore.getMetaStoreList() ) {
        try {
          log.logBasic( " --> delegated meta store " + delMetaStore.getName() );
        } catch ( Exception e ) {
          log.logError( " --> Error acessing delegated meta store", e );
        }
      }
      metaStoreList.addAll( ( (DelegatingMetaStore) metaStore ).getMetaStoreList() );
    } else {
      metaStoreList.add( metaStore );
    }
    props = PropsUi.getInstance();
  }

  public void open() {
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImageHopUi() );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "MetaStoreExplorerDialog.Dialog.Title" ) );

    int margin = props.getMargin();

    closeButton = new Button( shell, SWT.PUSH );
    closeButton.setText( BaseMessages.getString( PKG, "System.Button.Close" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { closeButton, }, margin, null );

    // Add listeners
    closeButton.addListener( SWT.Selection, new Listener() {
      public void handleEvent( Event e ) {
        close();
      }
    } );

    tree = new Tree( shell, SWT.SINGLE | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    props.setLook( tree );
    tree.setHeaderVisible( true );
    FormData treeFormData = new FormData();
    treeFormData.left = new FormAttachment( 0, 0 ); // To the right of the label
    treeFormData.top = new FormAttachment( 0, 0 );
    treeFormData.right = new FormAttachment( 100, 0 );
    treeFormData.bottom = new FormAttachment( closeButton, -margin * 2 );
    tree.setLayoutData( treeFormData );

    TreeColumn keyColumn = new TreeColumn( tree, SWT.LEFT );
    keyColumn.setText( "Metastore, Namespace, Element type, element name" );
    keyColumn.setWidth( 300 );

    TreeColumn valueColumn = new TreeColumn( tree, SWT.LEFT );
    valueColumn.setText( "Description or value" );
    valueColumn.setWidth( 300 );

    TreeColumn idColumn = new TreeColumn( tree, SWT.LEFT );
    idColumn.setText( "id" );
    idColumn.setWidth( 300 );

    tree.addMenuDetectListener( new MenuDetectListener() {

      @Override
      public void menuDetected( MenuDetectEvent event ) {
        if ( tree.getSelectionCount() < 1 ) {
          return;
        }
        TreeItem treeItem = tree.getSelection()[ 0 ];
        if ( treeItem != null ) {
          String[] labels = ConstUi.getTreeStrings( treeItem );
          int depth = ConstUi.getTreeLevel( treeItem );
          if ( depth == 3 ) {
            final String metaStoreName = labels[ 0 ];
            final String namespace = labels[ 1 ];
            final String elementTypeName = labels[ 2 ];
            final String elementName = labels[ 3 ];

            Menu menu = new Menu( tree );

            // Find the class that corresponds to the selected item...
            //
            MenuItem editItem = new MenuItem( menu, SWT.POP_UP );
            editItem.setText( "Edit element" );
            editItem.addSelectionListener( new SelectionAdapter() {
              @Override
              public void widgetSelected( SelectionEvent arg0 ) {
                editElement( metaStoreName, namespace, elementTypeName, elementName );
              }
            } );

            MenuItem removeItem = new MenuItem( menu, SWT.POP_UP );
            removeItem.setText( "Remove element" );
            removeItem.addSelectionListener( new SelectionAdapter() {
              @Override
              public void widgetSelected( SelectionEvent arg0 ) {
                removeElement( metaStoreName, namespace, elementTypeName, elementName );
              }
            } );


            tree.setMenu( menu );
            menu.setVisible( true );
          }
        }
      }
    } );

    TreeMemory.addTreeListener( tree, META_STORE_EXPLORER_DIALOG_TREE );

    try {
      refreshTree();
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Unexpected error displaying metastore information", e );
    }

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        close();
      }
    } );

    BaseTransformDialog.setSize( shell );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
  }

  private void editElement( String metaStoreName, String namespace, String elementTypeName, String elementName ) {
    // TODO
  }

  private void removeElement( String metaStoreName, String namespace, String elementTypeName, String elementName ) {

    try {
      IMetaStore metaStore = findMetaStore( metaStoreName );
      if ( metaStore == null ) {
        throw new MetaStoreException( "Unable to find metastore '" + metaStoreName + "'" );
      }
      IMetaStoreElementType elementType = metaStore.getElementTypeByName( namespace, elementTypeName );
      if ( elementType == null ) {
        throw new MetaStoreException( "Unable to find element type '"
          + elementTypeName + "' from metastore '" + metaStoreName + "' in namespace '" + namespace + "'" );
      }
      IMetaStoreElement element = metaStore.getElementByName( namespace, elementType, elementName );
      if ( element == null ) {
        throw new MetaStoreException( "Unable to find element '"
          + elementName + "' of type '" + elementTypeName + "' from metastore '" + metaStoreName
          + "' in namespace '" + namespace + "'" );
      }
      metaStore.deleteElement( namespace, elementType, element.getId() );

      refreshTree();

    } catch ( MetaStoreException e ) {
      new ErrorDialog( shell, "Error removing element", "There was an error removing the element '"
        + elementName + "' of type '" + elementTypeName + "' from metastore '" + metaStoreName
        + "' in namespace '" + namespace + "'", e );
    }

  }

  private IMetaStore findMetaStore( String metaStoreName ) throws MetaStoreException {
    for ( IMetaStore metaStore : metaStoreList ) {
      if ( metaStore.getName() != null && metaStore.getName().equals( metaStoreName ) ) {
        return metaStore;
      }
    }
    return null;
  }

  private void close() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void refreshTree() throws MetaStoreException {
    tree.removeAll();

    // Top level: the MetaStore
    //
    for ( int m = 0; m < metaStoreList.size(); m++ ) {
      IMetaStore metaStore = metaStoreList.get( m );
      TreeItem metaStoreItem = new TreeItem( tree, SWT.NONE );

      metaStoreItem.setText( 0, Const.NVL( metaStore.getName(), "metastore-" + ( m + 1 ) ) );
      metaStoreItem.setText( 1, Const.NVL( metaStore.getDescription(), "" ) );

      // level: Namespace
      //
      List<String> namespaces = metaStore.getNamespaces();
      for ( String namespace : namespaces ) {
        TreeItem namespaceItem = new TreeItem( metaStoreItem, SWT.NONE );

        namespaceItem.setText( 0, Const.NVL( namespace, "" ) );

        // level: element type
        //
        List<IMetaStoreElementType> elementTypes = metaStore.getElementTypes( namespace );
        for ( IMetaStoreElementType elementType : elementTypes ) {
          TreeItem elementTypeItem = new TreeItem( namespaceItem, SWT.NONE );

          elementTypeItem.setText( 0, Const.NVL( elementType.getName(), "" ) );
          elementTypeItem.setText( 1, Const.NVL( elementType.getDescription(), "" ) );

          // level: element
          //
          List<IMetaStoreElement> elements = metaStore.getElements( namespace, elementType );
          for ( final IMetaStoreElement element : elements ) {
            TreeItem elementItem = new TreeItem( elementTypeItem, SWT.NONE );

            elementItem.setText( 0, Const.NVL( element.getName(), "" ) );
            elementItem.setText( 2, Const.NVL( element.getId(), "" ) );

            elementItem.addListener( SWT.Selection, new Listener() {

              @Override
              public void handleEvent( Event event ) {
                log.logBasic( "Selected : " + element.getName() );
              }
            } );

            addAttributesToTree( elementItem, element );
          }

        }
      }
    }
    TreeUtil.setOptimalWidthOnColumns( tree );
    TreeMemory.setExpandedFromMemory( tree, META_STORE_EXPLORER_DIALOG_TREE );
  }

  private void addAttributesToTree( TreeItem parentItem, IMetaStoreAttribute parentAttribute ) {
    for ( IMetaStoreAttribute childAttribute : parentAttribute.getChildren() ) {
      TreeItem treeItem = new TreeItem( parentItem, SWT.NONE );
      treeItem.setText( 0, Const.NVL( childAttribute.getId(), "" ) );
      treeItem.setText( 1, childAttribute.getValue() == null ? "" : childAttribute.getValue().toString() );

      // Add more child attributes below
      //
      addAttributesToTree( treeItem, childAttribute );
    }
  }

  public IMetaStore getMetaStore() {
    return metaStore;
  }

  public void setMetaStore( IMetaStore metaStore ) {
    this.metaStore = metaStore;
  }

  public List<IMetaStore> getMetaStoreList() {
    return metaStoreList;
  }

  public void setMetaStoreList( List<IMetaStore> metaStoreList ) {
    this.metaStoreList = metaStoreList;
  }

}
