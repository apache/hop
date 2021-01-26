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

package org.apache.hop.workflow.actions.checkdbconnection;

import org.apache.hop.core.Const;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.List;

/**
 * This dialog allows you to edit the check database connection action settings.
 *
 * @author Samatar
 * @since 12-10-2007
 */
public class ActionCheckDbConnectionsDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionCheckDbConnectionsDialog.class; // For Translator

  private Shell shell;
  
  private Text wName;

  private ActionCheckDbConnections action;

  private boolean changed;

  private TableView wFields;

  public ActionCheckDbConnectionsDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionCheckDbConnections) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "ActionCheckDbConnections.Name.Default" ) );
    }
  }

  @Override
  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    ModifyListener lsMod = ( ModifyEvent e ) -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ActionCheckDbConnections.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    Label wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "ActionCheckDbConnections.Name.Label" ) );
    props.setLook( wlName );
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, -margin );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.left = new FormAttachment( middle, 0 );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData( fdName );

    Label wlFields = new Label( shell, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "ActionCheckDbConnections.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    // fdlFields.right= new FormAttachment(middle, -margin);
    fdlFields.top = new FormAttachment( wName, 2 * margin );
    wlFields.setLayoutData( fdlFields );

    // Buttons to the right of the screen...
    Button wbdSourceFileFolder = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdSourceFileFolder );
    wbdSourceFileFolder.setText( BaseMessages.getString( PKG, "ActionCheckDbConnections.DeleteEntry" ) );
    wbdSourceFileFolder.setToolTipText( BaseMessages.getString(
      PKG, "ActionCheckDbConnections.DeleteSourceFileButton.Label" ) );
    FormData fdbdSourceFileFolder = new FormData();
    fdbdSourceFileFolder.right = new FormAttachment( 100, -margin );
    fdbdSourceFileFolder.top = new FormAttachment( wlFields, 50 );
    wbdSourceFileFolder.setLayoutData(fdbdSourceFileFolder);

    // Buttons to the right of the screen...
    Button wbgetConnections = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbgetConnections );
    wbgetConnections.setText( BaseMessages.getString( PKG, "ActionCheckDbConnections.GetConnections" ) );
    wbgetConnections
      .setToolTipText( BaseMessages.getString( PKG, "ActionCheckDbConnections.GetConnections.Tooltip" ) );
    FormData fdbgetConnections = new FormData();
    fdbgetConnections.right = new FormAttachment( 100, -margin );
    fdbgetConnections.top = new FormAttachment( wlFields, 20 );
    wbgetConnections.setLayoutData(fdbgetConnections);

    int rows = action.getConnections() == null ? 1
      : ( action.getConnections().length == 0 ? 0 : action.getConnections().length );

    final int FieldsRows = rows;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "ActionCheckDbConnections.Fields.Argument.Label" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, this.getWorkflowMeta().getDatabaseNames(), false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ActionCheckDbConnections.Fields.WaitFor.Label" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "ActionCheckDbConnections.Fields.WaitForTime.Label" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ActionCheckDbConnections.unitTimeDesc, false ), };

    colinf[ 0 ].setToolTip( BaseMessages.getString( PKG, "ActionCheckDbConnections.Fields.Column" ) );
    colinf[ 1 ].setUsingVariables( true );
    colinf[ 1 ].setToolTip( BaseMessages.getString( PKG, "ActionCheckDbConnections.WaitFor.ToolTip" ) );

    wFields =
      new TableView(
    		  variables, shell, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( wbgetConnections, -margin );
    fdFields.bottom = new FormAttachment( 100, -50 );
    wFields.setLayoutData( fdFields );

    Button wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    FormData fd = new FormData();
    fd.right = new FormAttachment( 50, -10 );
    fd.bottom = new FormAttachment( 100, 0 );
    fd.width = 100;
    wOk.setLayoutData( fd );
    wOk.addListener( SWT.Selection, ( Event e ) -> ok());

    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 50, 10 );
    fd.bottom = new FormAttachment( 100, 0 );
    fd.width = 100;
    wCancel.setLayoutData( fd );
    wCancel.addListener( SWT.Selection, ( Event e ) -> cancel());

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, wFields );

    // Delete files from the list of files...
    wbdSourceFileFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        int[] idx = wFields.getSelectionIndices();
        wFields.remove( idx );
        wFields.removeEmptyRows();
        wFields.setRowNums();
      }
    } );

    // get connections...
    wbgetConnections.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        getDatabases();
      }
    } );


    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };

    wName.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "ActionCheckDbConnectionsDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  //  public void addDatabases() {
  //    connections = workflowMeta.getDatabaseNames();
  //  }

  public void getDatabases() {
    wFields.removeAll();
    List<DatabaseMeta> databases = this.getWorkflowMeta().getDatabases();
    for (DatabaseMeta ci : databases) {
      if (ci != null) {
        wFields.add(new String[]{ci.getName(), "0", ActionCheckDbConnections.unitTimeDesc[0]});
      }
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth( true );
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

    if ( action.getConnections() != null ) {
      for ( int i = 0; i < action.getConnections().length; i++ ) {
        TableItem ti = wFields.table.getItem( i );
        if ( action.getConnections()[ i ] != null ) {
          ti.setText( 1, action.getConnections()[ i ].getName() );
          ti.setText( 2, "" + Const.toInt( action.getWaitfors()[ i ], 0 ) );
          ti.setText( 3, ActionCheckDbConnections.getWaitTimeDesc( action.getWaittimes()[ i ] ) );
        }
      }
      wFields.setRowNums();
      wFields.optWidth( true );
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

    int nrItems = wFields.nrNonEmpty();

    DatabaseMeta[] connections = new DatabaseMeta[ nrItems ];
    String[] waitfors = new String[ nrItems ];
    int[] waittimes = new int[ nrItems ];

    for ( int i = 0; i < nrItems; i++ ) {
      String arg = wFields.getNonEmpty( i ).getText( 1 );
      DatabaseMeta dbMeta = this.getWorkflowMeta().findDatabase( arg );
      if ( dbMeta != null ) {
        connections[ i ] = dbMeta;
        waitfors[ i ] = "" + Const.toInt( wFields.getNonEmpty( i ).getText( 2 ), 0 );
        waittimes[ i ] =
          ActionCheckDbConnections.getWaitTimeByDesc( wFields.getNonEmpty( i ).getText( 3 ) );
      }
    }
    action.setConnections( connections );
    action.setWaitfors( waitfors );
    action.setWaittimes( waittimes );

    dispose();
  }
}
