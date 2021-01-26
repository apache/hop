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

package org.apache.hop.workflow.actions.telnet;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.WindowProperty;
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
 * This dialog allows you to edit the Telnet action settings.
 *
 * @author Samatar
 * @since 19-06-2006
 */
public class ActionTelnetDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionTelnet.class; // For Translator

  private Text wName;

  private TextVar wHostname;

  private TextVar wTimeOut;

  private TextVar wPort;

  private ActionTelnet action;

  private Shell shell;

  private boolean changed;

  public ActionTelnetDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionTelnet) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobTelnet.Name.Default" ) );
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
    shell.setText( BaseMessages.getString( PKG, "JobTelnet.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "JobTelnet.Name.Label" ) );
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

    // hostname line
    Label wlHostname = new Label(shell, SWT.RIGHT);
    wlHostname.setText( BaseMessages.getString( PKG, "JobTelnet.Hostname.Label" ) );
    props.setLook(wlHostname);
    FormData fdlHostname = new FormData();
    fdlHostname.left = new FormAttachment( 0, -margin );
    fdlHostname.top = new FormAttachment( wName, margin );
    fdlHostname.right = new FormAttachment( middle, -margin );
    wlHostname.setLayoutData(fdlHostname);

    wHostname = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wHostname );
    wHostname.addModifyListener( lsMod );
    FormData fdHostname = new FormData();
    fdHostname.left = new FormAttachment( middle, 0 );
    fdHostname.top = new FormAttachment( wName, margin );
    fdHostname.right = new FormAttachment( 100, 0 );
    wHostname.setLayoutData(fdHostname);

    // Whenever something changes, set the tooltip to the expanded version:
    wHostname.addModifyListener( e -> wHostname.setToolTipText( variables.resolve( wHostname.getText() ) ) );

    Label wlPort = new Label(shell, SWT.RIGHT);
    wlPort.setText( BaseMessages.getString( PKG, "JobTelnet.Port.Label" ) );
    props.setLook(wlPort);
    FormData fdlPort = new FormData();
    fdlPort.left = new FormAttachment( 0, -margin );
    fdlPort.right = new FormAttachment( middle, -margin );
    fdlPort.top = new FormAttachment( wHostname, margin );
    wlPort.setLayoutData(fdlPort);

    wPort = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPort );
    wPort.addModifyListener( lsMod );
    FormData fdPort = new FormData();
    fdPort.left = new FormAttachment( middle, 0 );
    fdPort.top = new FormAttachment( wHostname, margin );
    fdPort.right = new FormAttachment( 100, 0 );
    wPort.setLayoutData(fdPort);

    Label wlTimeOut = new Label(shell, SWT.RIGHT);
    wlTimeOut.setText( BaseMessages.getString( PKG, "JobTelnet.TimeOut.Label" ) );
    props.setLook(wlTimeOut);
    FormData fdlTimeOut = new FormData();
    fdlTimeOut.left = new FormAttachment( 0, -margin );
    fdlTimeOut.right = new FormAttachment( middle, -margin );
    fdlTimeOut.top = new FormAttachment( wPort, margin );
    wlTimeOut.setLayoutData(fdlTimeOut);

    wTimeOut = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTimeOut );
    wTimeOut.addModifyListener( lsMod );
    FormData fdTimeOut = new FormData();
    fdTimeOut.left = new FormAttachment( middle, 0 );
    fdTimeOut.top = new FormAttachment( wPort, margin );
    fdTimeOut.right = new FormAttachment( 100, 0 );
    wTimeOut.setLayoutData(fdTimeOut);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOk, wCancel}, margin, wTimeOut );

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
    wHostname.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobTelnetDialogSize" );
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
    wName.setText( Const.nullToEmpty( action.getName() ) );
    if ( action.getHostname() != null ) {
      wHostname.setText( action.getHostname() );
    }

    wPort.setText( Const.NVL( action.getPort(), String.valueOf( ActionTelnet.DEFAULT_PORT ) ) );
    wTimeOut.setText( Const.NVL( action.getTimeOut(), String.valueOf( ActionTelnet.DEFAULT_TIME_OUT ) ) );

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
    action.setHostname( wHostname.getText() );
    action.setPort( wPort.getText() );
    action.setTimeOut( wTimeOut.getText() );

    dispose();
  }

}
