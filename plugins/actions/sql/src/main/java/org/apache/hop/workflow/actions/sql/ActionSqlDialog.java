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

package org.apache.hop.workflow.actions.sql;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.StyledTextComp;
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
 * This dialog allows you to edit the SQL action settings. (select the connection and the sql script to be executed)
 *
 * @author Matt
 * @since 19-06-2003
 */
public class ActionSqlDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionSql.class; // For Translator

  private static final String[] FILETYPES = new String[] {
    BaseMessages.getString( PKG, "JobSQL.Filetype.Sql" ), BaseMessages.getString( PKG, "JobSQL.Filetype.Text" ),
    BaseMessages.getString( PKG, "JobSQL.Filetype.All" ) };

  private Text wName;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Button wUseSubs;

  private Button wSqlFromFile;

  private Label wlSql;

  private StyledTextComp wSql;

  private Label wlPosition;

  private ActionSql action;

  private Shell shell;

  private boolean changed;

  private Button wSendOneStatement;

  // File
  private Label wlFilename;
  private Button wbFilename;
  private TextVar wFilename;

  public ActionSqlDialog(Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionSql) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobSQL.Name.Default" ) );
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
    shell.setText( BaseMessages.getString( PKG, "JobSQL.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOk, wCancel}, margin, null );

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "JobSQL.Name.Label" ) );
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, 0 );
    fdlName.right = new FormAttachment( middle, 0 );
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

    // Connection line
    wConnection = addConnectionLine( shell, wName, action.getDatabase(), lsMod );

    // SQL from file?
    Label wlSqlFromFile = new Label(shell, SWT.RIGHT);
    wlSqlFromFile.setText( BaseMessages.getString( PKG, "JobSQL.SQLFromFile.Label" ) );
    props.setLook(wlSqlFromFile);
    FormData fdlSqlFromFile = new FormData();
    fdlSqlFromFile.left = new FormAttachment( 0, 0 );
    fdlSqlFromFile.top = new FormAttachment( wConnection, 2 * margin );
    fdlSqlFromFile.right = new FormAttachment( middle, -margin );
    wlSqlFromFile.setLayoutData(fdlSqlFromFile);
    wSqlFromFile = new Button( shell, SWT.CHECK );
    props.setLook( wSqlFromFile );
    wSqlFromFile.setToolTipText( BaseMessages.getString( PKG, "JobSQL.SQLFromFile.Tooltip" ) );
    FormData fdSqlFromFile = new FormData();
    fdSqlFromFile.left = new FormAttachment( middle, 0 );
    fdSqlFromFile.top = new FormAttachment( wConnection, 2 * margin );
    fdSqlFromFile.right = new FormAttachment( 100, 0 );
    wSqlFromFile.setLayoutData(fdSqlFromFile);
    wSqlFromFile.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeSqlFromFile();
        action.setChanged();
      }
    } );

    // Filename line
    wlFilename = new Label( shell, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "JobSQL.Filename.Label" ) );
    props.setLook( wlFilename );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wSqlFromFile, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData(fdlFilename);

    wbFilename = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbFilename );
    wbFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wSqlFromFile, margin );
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.setToolTipText( BaseMessages.getString( PKG, "JobSQL.Filename.Tooltip" ) );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, 0 );
    fdFilename.top = new FormAttachment( wSqlFromFile, margin );
    fdFilename.right = new FormAttachment( wbFilename, -margin );
    wFilename.setLayoutData(fdFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener( e -> wFilename.setToolTipText( variables.resolve( wFilename.getText() ) ) );

    wbFilename.addListener( SWT.Selection, e-> BaseDialog.presentFileDialog( shell, wFilename, variables,
      new String[] { "*.sql", "*.txt", "*" }, FILETYPES, true )
    );


    // Send one SQL Statement?
    Label wlUseOneStatement = new Label(shell, SWT.RIGHT);
    wlUseOneStatement.setText( BaseMessages.getString( PKG, "JobSQL.SendOneStatement.Label" ) );
    props.setLook(wlUseOneStatement);
    FormData fdlUseOneStatement = new FormData();
    fdlUseOneStatement.left = new FormAttachment( 0, 0 );
    fdlUseOneStatement.top = new FormAttachment( wbFilename, margin );
    fdlUseOneStatement.right = new FormAttachment( middle, -margin );
    wlUseOneStatement.setLayoutData(fdlUseOneStatement);
    wSendOneStatement = new Button( shell, SWT.CHECK );
    props.setLook( wSendOneStatement );
    wSendOneStatement.setToolTipText( BaseMessages.getString( PKG, "JobSQL.SendOneStatement.Tooltip" ) );
    FormData fdUseOneStatement = new FormData();
    fdUseOneStatement.left = new FormAttachment( middle, 0 );
    fdUseOneStatement.top = new FormAttachment( wbFilename, margin );
    fdUseOneStatement.right = new FormAttachment( 100, 0 );
    wSendOneStatement.setLayoutData(fdUseOneStatement);
    wSendOneStatement.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Use variable substitution?
    Label wlUseSubs = new Label(shell, SWT.RIGHT);
    wlUseSubs.setText( BaseMessages.getString( PKG, "JobSQL.UseVariableSubst.Label" ) );
    props.setLook(wlUseSubs);
    FormData fdlUseSubs = new FormData();
    fdlUseSubs.left = new FormAttachment( 0, 0 );
    fdlUseSubs.top = new FormAttachment( wSendOneStatement, margin );
    fdlUseSubs.right = new FormAttachment( middle, -margin );
    wlUseSubs.setLayoutData(fdlUseSubs);
    wUseSubs = new Button( shell, SWT.CHECK );
    props.setLook( wUseSubs );
    wUseSubs.setToolTipText( BaseMessages.getString( PKG, "JobSQL.UseVariableSubst.Tooltip" ) );
    FormData fdUseSubs = new FormData();
    fdUseSubs.left = new FormAttachment( middle, 0 );
    fdUseSubs.top = new FormAttachment( wSendOneStatement, margin );
    fdUseSubs.right = new FormAttachment( 100, 0 );
    wUseSubs.setLayoutData(fdUseSubs);
    wUseSubs.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setUseVariableSubstitution( !action.getUseVariableSubstitution() );
        action.setChanged();
      }
    } );

    wlPosition = new Label( shell, SWT.NONE );
    wlPosition.setText( BaseMessages.getString( PKG, "JobSQL.LineNr.Label", "0" ) );
    props.setLook( wlPosition );
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment( 0, 0 );
    fdlPosition.right = new FormAttachment( 100, 0 );
    fdlPosition.bottom = new FormAttachment(wOk, -margin );
    wlPosition.setLayoutData(fdlPosition);

    // Script line
    wlSql = new Label( shell, SWT.NONE );
    wlSql.setText( BaseMessages.getString( PKG, "JobSQL.Script.Label" ) );
    props.setLook(wlSql);
    FormData fdlSql = new FormData();
    fdlSql.left = new FormAttachment( 0, 0 );
    fdlSql.top = new FormAttachment( wUseSubs, margin );
    wlSql.setLayoutData(fdlSql);

    wSql = new StyledTextComp( action, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    props.setLook( wSql, Props.WIDGET_STYLE_FIXED );
    wSql.addModifyListener( lsMod );
    FormData fdSql = new FormData();
    fdSql.left = new FormAttachment( 0, 0 );
    fdSql.top = new FormAttachment(wlSql, margin );
    fdSql.right = new FormAttachment( 100, -10 );
    fdSql.bottom = new FormAttachment( wlPosition, -margin );
    wSql.setLayoutData(fdSql);

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

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    wSql.addModifyListener( arg0 -> setPosition() );

    wSql.addKeyListener( new KeyAdapter() {
      public void keyPressed( KeyEvent e ) {
        setPosition();
      }

      public void keyReleased( KeyEvent e ) {
        setPosition();
      }
    } );
    wSql.addFocusListener( new FocusAdapter() {
      public void focusGained( FocusEvent e ) {
        setPosition();
      }

      public void focusLost( FocusEvent e ) {
        setPosition();
      }
    } );
    wSql.addMouseListener( new MouseAdapter() {
      public void mouseDoubleClick( MouseEvent e ) {
        setPosition();
      }

      public void mouseDown( MouseEvent e ) {
        setPosition();
      }

      public void mouseUp( MouseEvent e ) {
        setPosition();
      }
    } );
    wSql.addModifyListener( lsMod );

    getData();
    activeSqlFromFile();

    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobSQLDialogSize" );
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return action;
  }

  public void setPosition() {
    int lineNumber = wSql.getLineNumber();
    int columnNumber = wSql.getColumnNumber();
    wlPosition.setText( BaseMessages.getString( PKG, "JobSQL.Position.Label", "" + lineNumber, "" + columnNumber ) );
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
    wName.setText( Const.nullToEmpty( action.getName() ) );
    wSql.setText( Const.nullToEmpty( action.getSql() ) );
    DatabaseMeta dbinfo = action.getDatabase();
    if ( dbinfo != null && dbinfo.getName() != null ) {
      wConnection.setText( dbinfo.getName() );
    } else {
      wConnection.setText( "" );
    }

    wUseSubs.setSelection( action.getUseVariableSubstitution() );
    wSqlFromFile.setSelection( action.getSqlFromFile() );
    wSendOneStatement.setSelection( action.isSendOneStatement() );

    wFilename.setText( Const.nullToEmpty( action.getSqlFilename() ) );

    wName.selectAll();
    wName.setFocus();
  }

  private void activeSqlFromFile() {
    wlFilename.setEnabled( wSqlFromFile.getSelection() );
    wFilename.setEnabled( wSqlFromFile.getSelection() );
    wbFilename.setEnabled( wSqlFromFile.getSelection() );
    wSql.setEnabled( !wSqlFromFile.getSelection() );
    wlSql.setEnabled( !wSqlFromFile.getSelection() );
    wlPosition.setEnabled( !wSqlFromFile.getSelection() );

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
    action.setSql( wSql.getText() );
    action.setUseVariableSubstitution( wUseSubs.getSelection() );
    action.setSqlFromFile( wSqlFromFile.getSelection() );
    action.setSqlFilename( wFilename.getText() );
    action.setSendOneStatement( wSendOneStatement.getSelection() );
    action.setDatabase( getWorkflowMeta().findDatabase( wConnection.getText() ) );
    dispose();
  }
}
