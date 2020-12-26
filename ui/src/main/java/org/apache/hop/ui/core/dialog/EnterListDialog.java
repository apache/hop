/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DragSource;
import org.eclipse.swt.dnd.DragSourceEvent;
import org.eclipse.swt.dnd.DragSourceListener;
import org.eclipse.swt.dnd.DropTarget;
import org.eclipse.swt.dnd.DropTargetEvent;
import org.eclipse.swt.dnd.DropTargetListener;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;

import java.util.Hashtable;
import java.util.StringTokenizer;

/**
 * This dialogs allows you to select a number of items from a list of strings.
 *
 * @author Matt
 * @since 21-10-2004
 */
public class EnterListDialog extends Dialog {
  private static final Class<?> PKG = EnterListDialog.class; // Needed by Translator

  private PropsUi props;

  private String[] input;
  private String[] retval;

  private Hashtable<Integer, String> selection;

  private Shell shell;
  private List wListSource, wListDest;
  private Label wlListSource, wlListDest;
  private Button wOk;
  private Button wCancel;

  private Button wAddOne, wAddAll, wRemoveAll, wRemoveOne;

  private boolean opened;

  public EnterListDialog( Shell parent, int style, String[] input ) {
    super( parent, style );
    this.props = PropsUi.getInstance();

    this.input = input;
    this.retval = null;

    selection = new Hashtable<>();

    opened = false;
  }

  public String[] open() {
    Shell parent = getParent();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImagePipeline() );
    shell.setText( BaseMessages.getString( PKG, "EnterListDialog.Title" ) );

    shell.setLayout( new FormLayout() );

    int margin = props.getMargin();

    // //////////////////////////////////////////////////
    // Top & Bottom regions.
    // //////////////////////////////////////////////////
    Composite top = new Composite( shell, SWT.NONE );
    FormLayout topLayout = new FormLayout();
    topLayout.marginHeight = margin;
    topLayout.marginWidth = margin;
    top.setLayout( topLayout );

    FormData fdTop = new FormData();
    fdTop.left = new FormAttachment( 0, 0 );
    fdTop.top = new FormAttachment( 0, 0 );
    fdTop.right = new FormAttachment( 100, 0 );
    fdTop.bottom = new FormAttachment( 100, -50 );
    top.setLayoutData( fdTop );
    props.setLook( top );

    Composite bottom = new Composite( shell, SWT.NONE );
    bottom.setLayout( new FormLayout() );
    FormData fdBottom = new FormData();
    fdBottom.left = new FormAttachment( 0, 0 );
    fdBottom.top = new FormAttachment( top, 0 );
    fdBottom.right = new FormAttachment( 100, 0 );
    fdBottom.bottom = new FormAttachment( 100, 0 );
    bottom.setLayoutData( fdBottom );
    props.setLook( bottom );

    // //////////////////////////////////////////////////
    // Sashform
    // //////////////////////////////////////////////////

    SashForm sashform = new SashForm( top, SWT.HORIZONTAL );
    sashform.setLayout( new FormLayout() );
    FormData fdSashform = new FormData();
    fdSashform.left = new FormAttachment( 0, 0 );
    fdSashform.top = new FormAttachment( 0, 0 );
    fdSashform.right = new FormAttachment( 100, 0 );
    fdSashform.bottom = new FormAttachment( 100, 0 );
    sashform.setLayoutData( fdSashform );

    // ////////////////////////
    // / LEFT
    // ////////////////////////
    Composite leftsplit = new Composite( sashform, SWT.NONE );
    leftsplit.setLayout( new FormLayout() );
    FormData fdLeftsplit = new FormData();
    fdLeftsplit.left = new FormAttachment( 0, 0 );
    fdLeftsplit.top = new FormAttachment( 0, 0 );
    fdLeftsplit.right = new FormAttachment( 100, 0 );
    fdLeftsplit.bottom = new FormAttachment( 100, 0 );
    leftsplit.setLayoutData( fdLeftsplit );
    props.setLook( leftsplit );

    // Source list to the left...
    wlListSource = new Label( leftsplit, SWT.NONE );
    wlListSource.setText( BaseMessages.getString( PKG, "EnterListDialog.AvailableItems.Label" ) );
    props.setLook( wlListSource );
    FormData fdlListSource = new FormData();
    fdlListSource.left = new FormAttachment( 0, 0 );
    fdlListSource.top = new FormAttachment( 0, 0 );
    wlListSource.setLayoutData( fdlListSource );

    wListSource = new List( leftsplit, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL );
    props.setLook( wListSource );

    FormData fdListSource = new FormData();
    fdListSource.left = new FormAttachment( 0, 0 );
    fdListSource.top = new FormAttachment( wlListSource, 0 );
    fdListSource.right = new FormAttachment( 100, 0 );
    fdListSource.bottom = new FormAttachment( 100, 0 );
    wListSource.setLayoutData( fdListSource );

    // /////////////////////////
    // MIDDLE
    // /////////////////////////

    Composite compmiddle = new Composite( sashform, SWT.NONE );
    compmiddle.setLayout( new FormLayout() );
    FormData fdCompMiddle = new FormData();
    fdCompMiddle.left = new FormAttachment( 0, 0 );
    fdCompMiddle.top = new FormAttachment( 0, 0 );
    fdCompMiddle.right = new FormAttachment( 100, 0 );
    fdCompMiddle.bottom = new FormAttachment( 100, 0 );
    compmiddle.setLayoutData( fdCompMiddle );
    props.setLook( compmiddle );

    Composite gButtonGroup = new Composite( compmiddle, SWT.NONE );
    GridLayout gridLayout = new GridLayout( 1, false );
    gButtonGroup.setLayout( gridLayout );

    wAddOne = new Button( gButtonGroup, SWT.PUSH );
    wAddOne.setText( " > " );
    wAddOne.setToolTipText( BaseMessages.getString( PKG, "EnterListDialog.AddOne.Tooltip" ) );
    wAddAll = new Button( gButtonGroup, SWT.PUSH );
    wAddAll.setText( " >> " );
    wAddAll.setToolTipText( BaseMessages.getString( PKG, "EnterListDialog.AddAll.Tooltip" ) );
    wRemoveOne = new Button( gButtonGroup, SWT.PUSH );
    wRemoveOne.setText( " < " );
    wRemoveOne.setToolTipText( BaseMessages.getString( PKG, "EnterListDialog.RemoveOne.Tooltip" ) );
    wRemoveAll = new Button( gButtonGroup, SWT.PUSH );
    wRemoveAll.setText( " << " );
    wRemoveAll.setToolTipText( BaseMessages.getString( PKG, "EnterListDialog.RemoveAll.Tooltip" ) );

    GridData gdAddOne = new GridData( GridData.FILL_BOTH );
    wAddOne.setLayoutData( gdAddOne );

    GridData gdAddAll = new GridData( GridData.FILL_BOTH );
    wAddAll.setLayoutData( gdAddAll );

    GridData gdRemoveAll = new GridData( GridData.FILL_BOTH );
    wRemoveAll.setLayoutData( gdRemoveAll );

    GridData gdRemoveOne = new GridData( GridData.FILL_BOTH );
    wRemoveOne.setLayoutData( gdRemoveOne );

    FormData fdButtonGroup = new FormData();
    wAddAll.pack(); // get a size
    fdButtonGroup.left = new FormAttachment( 50, -( wAddAll.getSize().x / 2 ) - 5 );
    fdButtonGroup.top = new FormAttachment( 30, 0 );
    gButtonGroup.setBackground( shell.getBackground() ); // the default looks ugly
    gButtonGroup.setLayoutData( fdButtonGroup );

    // ///////////////////////////////
    // RIGHT
    // ///////////////////////////////
    Composite rightsplit = new Composite( sashform, SWT.NONE );
    rightsplit.setLayout( new FormLayout() );
    FormData fdRightsplit = new FormData();
    fdRightsplit.left = new FormAttachment( 0, 0 );
    fdRightsplit.top = new FormAttachment( 0, 0 );
    fdRightsplit.right = new FormAttachment( 100, 0 );
    fdRightsplit.bottom = new FormAttachment( 100, 0 );
    rightsplit.setLayoutData( fdRightsplit );
    props.setLook( rightsplit );

    wlListDest = new Label( rightsplit, SWT.NONE );
    wlListDest.setText( BaseMessages.getString( PKG, "EnterListDialog.Selection.Label" ) );
    props.setLook( wlListDest );
    FormData fdlListDest = new FormData();
    fdlListDest.left = new FormAttachment( 0, 0 );
    fdlListDest.top = new FormAttachment( 0, 0 );
    wlListDest.setLayoutData( fdlListDest );

    wListDest = new List( rightsplit, SWT.BORDER | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL );
    props.setLook( wListDest );

    FormData fdListDest = new FormData();
    fdListDest.left = new FormAttachment( 0, 0 );
    fdListDest.top = new FormAttachment( wlListDest, 0 );
    fdListDest.right = new FormAttachment( 100, 0 );
    fdListDest.bottom = new FormAttachment( 100, 0 );
    wListDest.setLayoutData( fdListDest );

    sashform.setWeights( new int[] { 40, 16, 40 } );

    // //////////////////////////////////////////////////////////////
    // THE BOTTOM BUTTONS...
    // //////////////////////////////////////////////////////////////

    wOk = new Button( bottom, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( bottom, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    FormData fdOk = new FormData();
    FormData fdCancel = new FormData();

    fdOk.left = new FormAttachment( 35, 0 );
    fdOk.bottom = new FormAttachment( 100, 0 );
    wOk.setLayoutData( fdOk );

    fdCancel.left = new FormAttachment( wOk, 10 );
    fdCancel.bottom = new FormAttachment( 100, 0 );
    wCancel.setLayoutData( fdCancel );

    // Add listeners
    wCancel.addListener( SWT.Selection, e -> dispose() );

    // Add listeners
    wOk.addListener( SWT.Selection, e -> handleOK() );

    // Drag & Drop for transforms
    Transfer[] ttypes = new Transfer[] { TextTransfer.getInstance() };

    DragSource ddSource = new DragSource( wListSource, DND.DROP_MOVE | DND.DROP_COPY );
    ddSource.setTransfer( ttypes );
    ddSource.addDragListener( new DragSourceListener() {
      public void dragStart( DragSourceEvent event ) {
      }

      public void dragSetData( DragSourceEvent event ) {
        String[] ti = wListSource.getSelection();
        String data = new String();
        for ( int i = 0; i < ti.length; i++ ) {
          data += ti[ i ] + Const.CR;
        }
        event.data = data;
      }

      public void dragFinished( DragSourceEvent event ) {
      }
    } );
    DropTarget ddTarget = new DropTarget( wListDest, DND.DROP_MOVE | DND.DROP_COPY );
    ddTarget.setTransfer( ttypes );
    ddTarget.addDropListener( new DropTargetListener() {
      public void dragEnter( DropTargetEvent event ) {
      }

      public void dragLeave( DropTargetEvent event ) {
      }

      public void dragOperationChanged( DropTargetEvent event ) {
      }

      public void dragOver( DropTargetEvent event ) {
      }

      public void drop( DropTargetEvent event ) {
        if ( event.data == null ) { // no data to copy, indicate failure in event.detail
          event.detail = DND.DROP_NONE;
          return;
        }
        StringTokenizer strtok = new StringTokenizer( (String) event.data, Const.CR );
        while ( strtok.hasMoreTokens() ) {
          String source = strtok.nextToken();
          addToDestination( source );
        }
      }

      public void dropAccept( DropTargetEvent event ) {
      }
    } );

    wListSource.addKeyListener( new KeyAdapter() {
      public void keyPressed( KeyEvent e ) {
        if ( e.character == SWT.CR ) {
          addToSelection( wListSource.getSelection() );
        }
      }
    } );
    wListDest.addKeyListener( new KeyAdapter() {
      public void keyPressed( KeyEvent e ) {
        if ( e.character == SWT.CR ) {
          delFromSelection( wListDest.getSelection() );
        }
      }
    } );

    // Double click adds to destination.
    wListSource.addSelectionListener( new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        addToSelection( wListSource.getSelection() );
      }
    } );
    // Double click adds to source
    wListDest.addSelectionListener( new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        delFromSelection( wListDest.getSelection() );
      }
    } );

    wAddOne.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        addToSelection( wListSource.getSelection() );
      }
    } );

    wRemoveOne.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        delFromSelection( wListDest.getSelection() );
      }
    } );

    wAddAll.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        addToSelection( wListSource.getItems() );
      }
    } );

    wRemoveAll.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        delFromSelection( wListDest.getItems() );
      }
    } );

    opened = true;
    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    Display display = parent.getDisplay();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return retval;
  }

  public void getData() {
    if ( !opened ) {
      return;
    }

    wListSource.removeAll();
    wListDest.removeAll();
    for ( int i = 0; i < input.length; i++ ) {
      Integer idx = Integer.valueOf( i );
      String str = selection.get( idx );
      if ( str == null ) {
        // Not selected: show in source!
        wListSource.add( input[ i ] );
      } else {
        // Selected, show in destination!
        wListDest.add( input[ i ] );
      }
    }
  }

  public void addToSelection( String[] string ) {
    for ( int i = 0; i < string.length; i++ ) {
      addToDestination( string[ i ] );
    }
  }

  public void delFromSelection( String[] string ) {
    for ( int i = 0; i < string.length; i++ ) {
      delFromDestination( string[ i ] );
    }
  }

  public void addToDestination( String string ) {
    int idxInput = Const.indexOfString( string, input );
    selection.put( Integer.valueOf( idxInput ), string );

    getData();
  }

  public void delFromDestination( String string ) {
    int idxInput = Const.indexOfString( string, input );
    selection.remove( Integer.valueOf( idxInput ) );

    getData();
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  public void handleOK() {
    retval = wListDest.getItems();
    dispose();
  }
}
