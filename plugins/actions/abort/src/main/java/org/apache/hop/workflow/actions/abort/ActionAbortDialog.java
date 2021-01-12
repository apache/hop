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

package org.apache.hop.workflow.actions.abort;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
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
import org.eclipse.swt.widgets.Text;

/**
 * This dialog allows you to edit a Action Abort object.
 *
 * @author Samatar
 * @since 10-03-2007
 */
public class ActionAbortDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionAbortDialog.class; // For Translator

  private Shell shell;
  
  private ActionAbort action;

  private boolean changed;

  private Text wName;
  
  private TextVar wMessageAbort;

  public ActionAbortDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionAbort) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "ActionAbortDialog.Jobname.Label" ) );
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
    shell.setText( BaseMessages.getString( PKG, "ActionAbortDialog.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Filename line
    Label wlName = new Label( shell, SWT.RIGHT );
    wlName.setText( BaseMessages.getString( PKG, "ActionAbortDialog.Label" ) );
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

    // Message line
    Label wlMessageAbort = new Label( shell, SWT.RIGHT );
    wlMessageAbort.setText( BaseMessages.getString( PKG, "ActionAbortDialog.MessageAbort.Label" ) );
    props.setLook( wlMessageAbort );
    FormData fdlMessageAbort = new FormData();
    fdlMessageAbort.left = new FormAttachment( 0, 0 );
    fdlMessageAbort.right = new FormAttachment( middle, 0 );
    fdlMessageAbort.top = new FormAttachment( wName, margin );
    wlMessageAbort.setLayoutData( fdlMessageAbort );

    wMessageAbort = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMessageAbort );
    wMessageAbort.setToolTipText( BaseMessages.getString( PKG, "ActionAbortDialog.MessageAbort.Tooltip" ) );
    wMessageAbort.addModifyListener( lsMod );
    FormData fdMessageAbort = new FormData();
    fdMessageAbort.left = new FormAttachment( middle, 0 );
    fdMessageAbort.top = new FormAttachment( wName, margin );
    fdMessageAbort.right = new FormAttachment( 100, 0 );
    wMessageAbort.setLayoutData( fdMessageAbort );

    Button wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    // at the bottom
    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, wMessageAbort );

    // Add listeners
    wCancel.addListener( SWT.Selection, (Event e) -> cancel());
    wOk.addListener( SWT.Selection, (Event e) -> ok());

    SelectionAdapter lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    props.setDialogSize( shell, "JobAbortDialogSize" );
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
    if ( action.getMessageAbort() != null ) {
      wMessageAbort.setText( action.getMessageAbort() );
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
    action.setMessageAbort( wMessageAbort.getText() );
    dispose();
  }
}
