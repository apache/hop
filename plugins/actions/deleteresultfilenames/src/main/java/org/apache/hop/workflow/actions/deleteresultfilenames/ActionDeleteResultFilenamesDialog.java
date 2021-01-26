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

package org.apache.hop.workflow.actions.deleteresultfilenames;

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
 * This dialog allows you to edit the Create Folder action settings.
 *
 * @author Samatar
 * @since 27-10-2007
 */
public class ActionDeleteResultFilenamesDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionDeleteResultFilenames.class; // For Translator

  private Text wName;

  private Button wSpecifyWildcard;

  private Label wlWildcard;
  private TextVar wWildcard;

  private Label wlWildcardExclude;
  private TextVar wWildcardExclude;

  private ActionDeleteResultFilenames action;
  private Shell shell;

  private boolean changed;

  public ActionDeleteResultFilenamesDialog( Shell parent, IAction action,
                                            WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionDeleteResultFilenames) action;

    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "ActionDeleteResultFilenames.Name.Default" ) );
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
    shell.setText( BaseMessages.getString( PKG, "ActionDeleteResultFilenames.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Foldername line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "ActionDeleteResultFilenames.Name.Label" ) );
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

    // Specify wildcard?
    Label wlSpecifyWildcard = new Label(shell, SWT.RIGHT);
    wlSpecifyWildcard
      .setText( BaseMessages.getString( PKG, "ActionDeleteResultFilenames.SpecifyWildcard.Label" ) );
    props.setLook(wlSpecifyWildcard);
    FormData fdlSpecifyWildcard = new FormData();
    fdlSpecifyWildcard.left = new FormAttachment( 0, 0 );
    fdlSpecifyWildcard.top = new FormAttachment( wName, margin );
    fdlSpecifyWildcard.right = new FormAttachment( middle, -margin );
    wlSpecifyWildcard.setLayoutData(fdlSpecifyWildcard);
    wSpecifyWildcard = new Button( shell, SWT.CHECK );
    props.setLook( wSpecifyWildcard );
    wSpecifyWildcard.setToolTipText( BaseMessages.getString(
      PKG, "ActionDeleteResultFilenames.SpecifyWildcard.Tooltip" ) );
    FormData fdSpecifyWildcard = new FormData();
    fdSpecifyWildcard.left = new FormAttachment( middle, 0 );
    fdSpecifyWildcard.top = new FormAttachment( wName, margin );
    fdSpecifyWildcard.right = new FormAttachment( 100, 0 );
    wSpecifyWildcard.setLayoutData(fdSpecifyWildcard);
    wSpecifyWildcard.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        CheckLimit();
      }
    } );

    // Wildcard line
    wlWildcard = new Label( shell, SWT.RIGHT );
    wlWildcard.setText( BaseMessages.getString( PKG, "ActionDeleteResultFilenames.Wildcard.Label" ) );
    props.setLook( wlWildcard );
    FormData fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment( 0, 0 );
    fdlWildcard.top = new FormAttachment( wSpecifyWildcard, margin );
    fdlWildcard.right = new FormAttachment( middle, -margin );
    wlWildcard.setLayoutData(fdlWildcard);
    wWildcard = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wWildcard.setToolTipText( BaseMessages.getString( PKG, "ActionDeleteResultFilenames.Wildcard.Tooltip" ) );
    props.setLook( wWildcard );
    wWildcard.addModifyListener( lsMod );
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment( middle, 0 );
    fdWildcard.top = new FormAttachment( wSpecifyWildcard, margin );
    fdWildcard.right = new FormAttachment( 100, -margin );
    wWildcard.setLayoutData(fdWildcard);

    // Whenever something changes, set the tooltip to the expanded version:
    wWildcard.addModifyListener( e -> wWildcard.setToolTipText( variables.resolve( wWildcard.getText() ) ) );

    // wWildcardExclude
    wlWildcardExclude = new Label( shell, SWT.RIGHT );
    wlWildcardExclude
      .setText( BaseMessages.getString( PKG, "ActionDeleteResultFilenames.WildcardExclude.Label" ) );
    props.setLook( wlWildcardExclude );
    FormData fdlWildcardExclude = new FormData();
    fdlWildcardExclude.left = new FormAttachment( 0, 0 );
    fdlWildcardExclude.top = new FormAttachment( wWildcard, margin );
    fdlWildcardExclude.right = new FormAttachment( middle, -margin );
    wlWildcardExclude.setLayoutData(fdlWildcardExclude);
    wWildcardExclude = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wWildcardExclude.setToolTipText( BaseMessages.getString(
      PKG, "ActionDeleteResultFilenames.WildcardExclude.Tooltip" ) );
    props.setLook( wWildcardExclude );
    wWildcardExclude.addModifyListener( lsMod );
    FormData fdWildcardExclude = new FormData();
    fdWildcardExclude.left = new FormAttachment( middle, 0 );
    fdWildcardExclude.top = new FormAttachment( wWildcard, margin );
    fdWildcardExclude.right = new FormAttachment( 100, -margin );
    wWildcardExclude.setLayoutData(fdWildcardExclude);

    // Whenever something changes, set the tooltip to the expanded version:
    wWildcardExclude.addModifyListener( e -> wWildcardExclude.setToolTipText( variables.resolve( wWildcardExclude.getText() ) ) );

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOk, wCancel}, margin, wWildcardExclude );

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
    CheckLimit();

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

  private void CheckLimit() {
    wlWildcard.setEnabled( wSpecifyWildcard.getSelection() );
    wWildcard.setEnabled( wSpecifyWildcard.getSelection() );
    wlWildcardExclude.setEnabled( wSpecifyWildcard.getSelection() );
    wWildcardExclude.setEnabled( wSpecifyWildcard.getSelection() );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( action.getName() != null ) {
      wName.setText( action.getName() );
    }
    wSpecifyWildcard.setSelection( action.isSpecifyWildcard() );
    if ( action.getWildcard() != null ) {
      wWildcard.setText( action.getWildcard() );
    }
    if ( action.getWildcardExclude() != null ) {
      wWildcardExclude.setText( action.getWildcardExclude() );
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
    action.setSpecifyWildcard( wSpecifyWildcard.getSelection() );
    action.setWildcard( wWildcard.getText() );
    action.setWildcardExclude( wWildcardExclude.getText() );

    dispose();
  }
}
