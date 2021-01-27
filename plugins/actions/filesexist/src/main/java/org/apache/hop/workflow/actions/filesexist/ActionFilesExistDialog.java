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

package org.apache.hop.workflow.actions.filesexist;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

/**
 * This dialog allows you to edit the Files exist action settings.
 *
 * @author Samatar
 * @since 12-10-2007
 */
public class ActionFilesExistDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionFilesExist.class; // For Translator

  private static final String[] FILETYPES = new String[] {
    BaseMessages.getString( PKG, "JobFilesExist.Filetype.Text" ),
    BaseMessages.getString( PKG, "JobFilesExist.Filetype.CSV" ),
    BaseMessages.getString( PKG, "JobFilesExist.Filetype.All" ) };

  private Text wName;

  private TextVar wFilename;

  private ActionFilesExist action;

  private Shell shell;

  private boolean changed;

  private TableView wFields;

  public ActionFilesExistDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionFilesExist) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobFilesExist.Name.Default" ) );
    }

  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobFilesExist.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "JobFilesExist.Name.Label" ) );
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData(fdlName);
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData(fdName);

    // Filename line
    Label wlFilename = new Label(shell, SWT.RIGHT);
    wlFilename.setText( BaseMessages.getString( PKG, "JobFilesExist.Filename.Label" ) );
    props.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wName, 2 * margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData(fdlFilename);

    // Browse Source folders button ...
    Button wbDirectory = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbDirectory);
    wbDirectory.setText( BaseMessages.getString( PKG, "JobFilesExist.BrowseFolders.Label" ) );
    FormData fdbDirectory = new FormData();
    fdbDirectory.right = new FormAttachment( 100, -margin );
    fdbDirectory.top = new FormAttachment( wName, margin );
    wbDirectory.setLayoutData(fdbDirectory);

    wbDirectory.addListener( SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wFilename, variables ));

    Button wbFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbFilename);
    wbFilename.setText( BaseMessages.getString( PKG, "JobFilesExist.BrowseFiles.Label" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wName, margin );
    fdbFilename.right = new FormAttachment(wbDirectory, -margin );
    wbFilename.setLayoutData(fdbFilename);

    // Add or change
    Button wbaFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbaFilename);
    wbaFilename.setText( BaseMessages.getString( PKG, "JobFilesExist.FilenameAdd.Button" ) );
    FormData fdbaFilename = new FormData();
    fdbaFilename.right = new FormAttachment(wbFilename, -margin );
    fdbaFilename.top = new FormAttachment( wName, margin );
    wbaFilename.setLayoutData(fdbaFilename);

    wFilename = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( wName, 2 * margin );
    fdFilename.right = new FormAttachment(wbFilename, -55 );
    wFilename.setLayoutData(fdFilename);


    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener( e -> wFilename.setToolTipText( variables.resolve( wFilename.getText() ) ) );

    wbFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wFilename, variables,
      new String[] { "*" }, FILETYPES, true )
    );

    // Buttons to the right of the screen...
    // Delete
    Button wbdFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbdFilename);
    wbdFilename.setText( BaseMessages.getString( PKG, "JobFilesExist.FilenameDelete.Button" ) );
    wbdFilename.setToolTipText( BaseMessages.getString( PKG, "JobFilesExist.FilenameDelete.Tooltip" ) );
    FormData fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment( 100, 0 );
    fdbdFilename.top = new FormAttachment( wFilename, 40 );
    wbdFilename.setLayoutData(fdbdFilename);

    // Edit
    Button wbeFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbeFilename);
    wbeFilename.setText( BaseMessages.getString( PKG, "JobFilesExist.FilenameEdit.Button" ) );
    wbeFilename.setToolTipText( BaseMessages.getString( PKG, "JobFilesExist.FilenameEdit.Tooltip" ) );
    FormData fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment( 100, 0 );
    fdbeFilename.left = new FormAttachment(wbdFilename, 0, SWT.LEFT );
    fdbeFilename.top = new FormAttachment(wbdFilename, margin );
    wbeFilename.setLayoutData(fdbeFilename);

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText( BaseMessages.getString( PKG, "JobFilesExist.Fields.Label" ) );
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.right = new FormAttachment( middle, -margin );
    fdlFields.top = new FormAttachment( wFilename, margin );
    wlFields.setLayoutData(fdlFields);

    int rows = action.getArguments() == null ? 1
      : ( action.getArguments().length == 0 ? 0 : action.getArguments().length );

    final int FieldsRows = rows;

    ColumnInfo[] colinf =
      new ColumnInfo[] { new ColumnInfo(
        BaseMessages.getString( PKG, "JobFilesExist.Fields.Argument.Label" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false ), };

    colinf[ 0 ].setUsingVariables( true );
    colinf[ 0 ].setToolTip( BaseMessages.getString( PKG, "JobFilesExist.Fields.Column" ) );

    wFields =
      new TableView(
    		  variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment(wlFields, margin );
    fdFields.right = new FormAttachment( 100, -75 );
    fdFields.bottom = new FormAttachment( 100, -50 );
    wFields.setLayoutData(fdFields);

    // Add the file to the list of files...
    SelectionAdapter selA = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        wFields.add( new String[] { wFilename.getText() } );
        wFilename.setText( "" );
        wFields.removeEmptyRows();
        wFields.setRowNums();
        wFields.optWidth( true );
      }
    };
    wbaFilename.addSelectionListener( selA );
    wFilename.addSelectionListener( selA );

    // Delete files from the list of files...
    wbdFilename.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        int[] idx = wFields.getSelectionIndices();
        wFields.remove( idx );
        wFields.removeEmptyRows();
        wFields.setRowNums();
      }
    } );

    // Edit the selected file & remove from the list...
    wbeFilename.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        int idx = wFields.getSelectionIndex();
        if ( idx >= 0 ) {
          String[] string = wFields.getItem( idx );
          wFilename.setText( string[ 0 ] );
          wFields.remove( idx );
        }
        wFields.removeEmptyRows();
        wFields.setRowNums();
      }
    } );

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    FormData fd = new FormData();
    fd.right = new FormAttachment( 50, -10 );
    fd.bottom = new FormAttachment( 100, 0 );
    fd.width = 100;
    wOk.setLayoutData( fd );

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 50, 10 );
    fd.bottom = new FormAttachment( 100, 0 );
    fd.width = 100;
    wCancel.setLayoutData( fd );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOk, wCancel}, margin, wFields );

    // Add listeners
    Listener lsCancel = e -> cancel();
    Listener lsOk = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel);
    wOk.addListener( SWT.Selection, lsOk);

    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };

    wName.addSelectionListener(lsDef);
    wFilename.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobFilesExistDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  public void dispose() {
    WindowProperty winprop = new WindowProperty( shell );
    props.setScreen( winprop );
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( action.getName() != null ) {
      wName.setText( action.getName() );
    }

    if ( action.getArguments() != null ) {
      for ( int i = 0; i < action.getArguments().length; i++ ) {
        TableItem ti = wFields.table.getItem( i );
        if ( action.getArguments()[ i ] != null ) {
          ti.setText( 1, action.getArguments()[ i ] );
        }
      }
      wFields.setRowNums();
      wFields.optWidth( true );
    }

    if ( action.getFilename() != null ) {
      wFilename.setText( action.getFilename() );
    }

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged( changed );
    action = null;
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wName.getText() ) ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      mb.setText( BaseMessages.getString( PKG, "System.TransformActionNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
      mb.open();
      return;
    }
    action.setName( wName.getText() );
    action.setFilename( wFilename.getText() );

    int nrItems = wFields.nrNonEmpty();
    int nr = 0;
    for ( int i = 0; i < nrItems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      if ( arg != null && arg.length() != 0 ) {
        nr++;
      }
    }
    String[] arguments = new String[ nr ];
    nr = 0;
    for ( int i = 0; i < nrItems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      if ( arg != null && arg.length() != 0 ) {
        arguments[ nr ] = arg;
        nr++;
      }
    }
    action.setArguments( arguments );

    dispose();
  }
}
