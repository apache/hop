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

package org.apache.hop.workflow.actions.msgboxinfo;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ControlSpaceKeyAdapter;
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
 * This dialog allows you to edit a JobEntryEval object.
 *
 * @author Matt
 * @since 19-06-2003
 */
public class ActionMsgBoxInfoDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionMsgBoxInfo.class; // For Translator

  private Text wName;

  private TextVar wBodyMessage;

  private ActionMsgBoxInfo action;

  private Shell shell;

  private boolean changed;

  private TextVar wTitleMessage;

  public ActionMsgBoxInfoDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionMsgBoxInfo) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "MsgBoxInfo.Name.Default" ) );
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
    shell.setText( BaseMessages.getString( PKG, "MsgBoxInfo.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    // at the bottom
    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOk, wCancel}, margin, null );

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "MsgBoxInfo.Label" ) );
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

    // Title Msgbox
    // TitleMessage
    Label wlTitleMessage = new Label(shell, SWT.RIGHT);
    wlTitleMessage.setText( BaseMessages.getString( PKG, "MsgBoxInfo.TitleMessage.Label" ) );
    props.setLook(wlTitleMessage);
    FormData fdlTitleMessage = new FormData();
    fdlTitleMessage.left = new FormAttachment( 0, 0 );
    fdlTitleMessage.top = new FormAttachment( wName, margin );
    fdlTitleMessage.right = new FormAttachment( middle, -margin );
    wlTitleMessage.setLayoutData(fdlTitleMessage);

    wTitleMessage = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTitleMessage );
    wTitleMessage.addModifyListener( lsMod );
    FormData fdTitleMessage = new FormData();
    fdTitleMessage.left = new FormAttachment( middle, 0 );
    fdTitleMessage.top = new FormAttachment( wName, margin );
    fdTitleMessage.right = new FormAttachment( 100, 0 );
    wTitleMessage.setLayoutData(fdTitleMessage);

    // Body Msgbox
    Label wlBodyMessage = new Label(shell, SWT.RIGHT);
    wlBodyMessage.setText( BaseMessages.getString( PKG, "MsgBoxInfo.BodyMessage.Label" ) );
    props.setLook(wlBodyMessage);
    FormData fdlBodyMessage = new FormData();
    fdlBodyMessage.left = new FormAttachment( 0, 0 );
    fdlBodyMessage.top = new FormAttachment( wTitleMessage, margin );
    fdlBodyMessage.right = new FormAttachment( middle, -margin );
    wlBodyMessage.setLayoutData(fdlBodyMessage);

    wBodyMessage = new TextVar( variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    wBodyMessage.setText( BaseMessages.getString( PKG, "MsgBoxInfo.Name.Default" ) );
    props.setLook( wBodyMessage, Props.WIDGET_STYLE_FIXED );
    wBodyMessage.addModifyListener( lsMod );
    FormData fdBodyMessage = new FormData();
    fdBodyMessage.left = new FormAttachment( middle, 0 );
    fdBodyMessage.top = new FormAttachment( wTitleMessage, margin );
    fdBodyMessage.right = new FormAttachment( 100, 0 );
    fdBodyMessage.bottom = new FormAttachment(wOk, -margin );
    wBodyMessage.setLayoutData(fdBodyMessage);

    // SelectionAdapter lsVar = VariableButtonListenerFactory.getSelectionAdapter(shell, wBodyMessage, workflowMeta);
    wBodyMessage.addKeyListener( new ControlSpaceKeyAdapter( variables, wBodyMessage ) );

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

    getData();

    BaseTransformDialog.setSize( shell, 250, 250, false );

    shell.open();
    props.setDialogSize( shell, "JobEvalDialogSize" );
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
    wName.setText( Const.NVL( action.getName(), "" ) );
    wBodyMessage.setText( Const.NVL( action.getBodyMessage(), "" ) );
    wTitleMessage.setText( Const.NVL( action.getTitleMessage(), "" ) );

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
    action.setTitleMessage( wTitleMessage.getText() );
    action.setBodyMessage( wBodyMessage.getText() );
    dispose();
  }
}
