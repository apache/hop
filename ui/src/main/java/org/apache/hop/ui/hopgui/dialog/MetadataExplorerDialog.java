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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.metastore.MetadataManager;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.core.widget.TreeUtil;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Image;
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

import java.util.Collections;
import java.util.List;

public class MetadataExplorerDialog {
  private static Class<?> PKG = MetadataExplorerDialog.class; // for i18n purposes, needed by Translator

  private static final String METADATA_EXPLORER_DIALOG_TREE = "Metadata explorer dialog tree";

  private static ILogChannel log = LogChannel.GENERAL;

  private IHopMetadataProvider metadataProvider;

  private Shell parent;

  private Shell shell;

  private Tree tree;

  private PropsUi props;

  private Button closeButton;

  public MetadataExplorerDialog( Shell parent, IHopMetadataProvider metadataProvider ) {
    this.metadataProvider = metadataProvider;
    this.parent = parent;
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
    shell.setText( BaseMessages.getString( PKG, "MetadataExplorerDialog.Dialog.Title" ) );

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
    keyColumn.setText( "Object type key (folder)" );
    keyColumn.setWidth( 400 );

    TreeColumn valueColumn = new TreeColumn( tree, SWT.LEFT );
    valueColumn.setText( "Description or value" );
    valueColumn.setWidth( 500 );

    tree.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetDefaultSelected( SelectionEvent e ) {
        if ( tree.getSelectionCount() < 1 ) {
          return;
        }
        TreeItem treeItem = tree.getSelection()[ 0 ];
        if ( treeItem != null ) {
          final String objectKey;
          final String objectName;
          if ( treeItem.getParentItem() == null ) {
            objectKey = treeItem.getText();
            objectName = null;
          } else {
            objectKey = treeItem.getParentItem().getText();
            objectName = treeItem.getText( 1 );
          }
          if (StringUtils.isEmpty( objectKey )){
            return;
          }
          try {
            Class<IHopMetadata> metadataClass = metadataProvider.getMetadataClassForKey( objectKey );
            MetadataManager<IHopMetadata> manager = new MetadataManager<>( HopGui.getInstance().getVariables(), metadataProvider, metadataClass );

            if ( StringUtils.isEmpty( objectName ) ) {
              if (manager.newMetadata()!=null) {
                refreshTree();
              }
            } else {
              if (manager.editMetadata(objectName)) {
                refreshTree();
              }
            }
          } catch(Exception ex) {
            new ErrorDialog( shell, "Error", "Error handling double-click selection event", ex );
          }
        }
      }
    } );

    tree.addMenuDetectListener( event -> {
      try {
        if ( tree.getSelectionCount() < 1 ) {
          return;
        }
        TreeItem treeItem = tree.getSelection()[ 0 ];
        if ( treeItem != null ) {
          final String objectKey;
          final String objectName;
          if (treeItem.getParentItem()==null) {
            objectKey = treeItem.getText();
            objectName = null;
          } else {
            objectKey = treeItem.getParentItem().getText();
            objectName = treeItem.getText(1);
          }

          if ( StringUtils.isNotEmpty(objectKey) ) {

            Class<IHopMetadata> metadataClass = metadataProvider.getMetadataClassForKey( objectKey );
            MetadataManager<IHopMetadata> manager = new MetadataManager<>( HopGui.getInstance().getVariables(), metadataProvider, metadataClass );

            // Show the menu
            //
            Menu menu = new Menu( tree );

            MenuItem newItem = new MenuItem( menu, SWT.POP_UP );
            newItem.setText( "New" );
            newItem.addSelectionListener( new SelectionAdapter() {
              @Override
              public void widgetSelected( SelectionEvent arg0 ) {
                if (manager.newMetadata()!=null) {
                  refreshTree();
                }
              }
            } );

            if ( StringUtils.isNotEmpty( objectName ) ) {

              MenuItem editItem = new MenuItem( menu, SWT.POP_UP );
              editItem.setText( "Edit" );
              editItem.addSelectionListener( new SelectionAdapter() {
                @Override
                public void widgetSelected( SelectionEvent arg0 ) {
                  if (manager.editMetadata( objectName )) {
                    refreshTree();
                  }
                }
              } );

              MenuItem removeItem = new MenuItem( menu, SWT.POP_UP );
              removeItem.setText( "Delete" );
              removeItem.addSelectionListener( new SelectionAdapter() {
                @Override
                public void widgetSelected( SelectionEvent arg0 ) {
                  if (manager.deleteMetadata( objectName )) {
                    refreshTree();
                  }
                }
              } );
            }


            tree.setMenu( menu );
            menu.setVisible( true );


          }
        }
      } catch ( Exception e ) {
        new ErrorDialog( shell, "Error", "Error handling metadata object", e );
      }
    } );

    TreeMemory.addTreeListener( tree, METADATA_EXPLORER_DIALOG_TREE );

    try {
      refreshTree();

      for (TreeItem item : tree.getItems()) {
        TreeMemory.getInstance().storeExpanded( METADATA_EXPLORER_DIALOG_TREE, item, true);
      }
      TreeMemory.setExpandedFromMemory( tree, METADATA_EXPLORER_DIALOG_TREE );
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Unexpected error displaying metadata information", e );
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

  private void close() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void refreshTree() {
    try {
      tree.removeAll();

      // top level: object key
      //
      List<Class<IHopMetadata>> metadataClasses = metadataProvider.getMetadataClasses();
      for ( Class<IHopMetadata> metadataClass : metadataClasses ) {
        HopMetadata hopMetadata = HopMetadataUtil.getHopMetadataAnnotation( metadataClass );
        Image image = SwtSvgImageUtil.getImage( shell.getDisplay(), metadataClass.getClassLoader(), hopMetadata.iconImage(), ConstUi.ICON_SIZE, ConstUi.ICON_SIZE );

        TreeItem elementTypeItem = new TreeItem( tree, SWT.NONE );
        elementTypeItem.setImage( image );

        elementTypeItem.setText( 0, Const.NVL( hopMetadata.key(), "" ) );
        elementTypeItem.setText( 1, Const.NVL( hopMetadata.name(), "" ) );

        // level 1: object names
        //
        IHopMetadataSerializer<IHopMetadata> serializer = metadataProvider.getSerializer( metadataClass );
        List<String> names = serializer.listObjectNames();
        Collections.sort( names );

        for ( final String name : names ) {
          TreeItem elementItem = new TreeItem( elementTypeItem, SWT.NONE );
          elementItem.setText( 1, Const.NVL( name, "" ) );
          elementItem.addListener( SWT.Selection, event -> log.logBasic( "Selected : " + name ) );
          elementItem.setFont( GuiResource.getInstance().getFontBold() );
        }
      }

      TreeUtil.setOptimalWidthOnColumns( tree );
      TreeMemory.setExpandedFromMemory( tree, METADATA_EXPLORER_DIALOG_TREE );
    } catch(Exception e) {
      new ErrorDialog( shell, "Error", "Error refreshing metadata tree", e );
    }
  }


  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  public void setMetadataProvider( IHopMetadataProvider metadataProvider ) {
    this.metadataProvider = metadataProvider;
  }

}
