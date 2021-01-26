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

package org.apache.hop.workflow.actions.delay;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.action.IActionDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

/**
 * This dialog allows you to edit the delay action settings.
 *
 * @author Samatar Hassan
 * @since 21-02-2007
 */
public class ActionDelayDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionDelay.class; // For Translator

  private Text wName;

  private CCombo wScaleTime;

  private LabelTextVar wMaximumTimeout;

  private ActionDelay action;
  private Shell shell;

  private boolean changed;

  public ActionDelayDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionDelay) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "ActionDelay.Title" ) );
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
    shell.setText( BaseMessages.getString( PKG, "ActionDelay.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Name line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "ActionDelay.Name.Label" ) );
    props.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment( 0, -margin );
    fdlName.right = new FormAttachment( middle, 0 );
    fdlName.top = new FormAttachment( 0, margin );
    wlName.setLayoutData(fdlName);
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    wName.addModifyListener( lsMod );
    FormData fdName = new FormData();
    fdName.left = new FormAttachment( middle, margin );
    fdName.top = new FormAttachment( 0, margin );
    fdName.right = new FormAttachment( 100, 0 );
    wName.setLayoutData(fdName);

    // MaximumTimeout line
    wMaximumTimeout =
      new LabelTextVar(variables, shell, BaseMessages.getString( PKG, "ActionDelay.MaximumTimeout.Label" ), BaseMessages
        .getString( PKG, "ActionDelay.MaximumTimeout.Tooltip" ) );
    props.setLook( wMaximumTimeout );
    wMaximumTimeout.addModifyListener( lsMod );
    FormData fdMaximumTimeout = new FormData();
    fdMaximumTimeout.left = new FormAttachment( 0, -margin );
    fdMaximumTimeout.top = new FormAttachment( wName, margin );
    fdMaximumTimeout.right = new FormAttachment( 100, 0 );
    wMaximumTimeout.setLayoutData(fdMaximumTimeout);

    // Whenever something changes, set the tooltip to the expanded version:
    wMaximumTimeout.addModifyListener( e -> wMaximumTimeout.setToolTipText( variables.resolve( wMaximumTimeout.getText() ) ) );

    // Scale time

    wScaleTime = new CCombo( shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wScaleTime.add( BaseMessages.getString( PKG, "ActionDelay.SScaleTime.Label" ) );
    wScaleTime.add( BaseMessages.getString( PKG, "ActionDelay.MnScaleTime.Label" ) );
    wScaleTime.add( BaseMessages.getString( PKG, "ActionDelay.HrScaleTime.Label" ) );
    wScaleTime.select( 0 ); // +1: starts at -1

    props.setLook( wScaleTime );
    FormData fdScaleTime = new FormData();
    fdScaleTime.left = new FormAttachment( middle, 0 );
    fdScaleTime.top = new FormAttachment( wMaximumTimeout, margin );
    fdScaleTime.right = new FormAttachment( 100, 0 );
    wScaleTime.setLayoutData(fdScaleTime);

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOk, wCancel}, margin, wScaleTime );

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
    wMaximumTimeout.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
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
    if ( action.getMaximumTimeout() != null ) {
      wMaximumTimeout.setText( action.getMaximumTimeout() );
    }

    wScaleTime.select( action.scaleTime );

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
    action.setMaximumTimeout( wMaximumTimeout.getText() );
    action.scaleTime = wScaleTime.getSelectionIndex();
    dispose();
  }
}
