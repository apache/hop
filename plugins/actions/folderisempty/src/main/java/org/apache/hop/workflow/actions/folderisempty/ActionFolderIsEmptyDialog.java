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

package org.apache.hop.workflow.actions.folderisempty;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
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
 * @author Sven/Samatar
 * @since 17-10-2007
 */

public class ActionFolderIsEmptyDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = ActionFolderIsEmpty.class; // For Translator

  private Text wName;

  private TextVar wFoldername;

  private Button wIncludeSubFolders;

  private Button wSpecifyWildcard;

  private Label wlWildcard;
  private TextVar wWildcard;

  private ActionFolderIsEmpty action;
  private Shell shell;

  private boolean changed;

  public ActionFolderIsEmptyDialog( Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    this.action = (ActionFolderIsEmpty) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobFolderIsEmpty.Name.Default" ) );
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
    shell.setText( BaseMessages.getString( PKG, "JobFolderIsEmpty.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Foldername line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "JobFolderIsEmpty.Name.Label" ) );
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

    // Foldername line
    Label wlFoldername = new Label(shell, SWT.RIGHT);
    wlFoldername.setText( BaseMessages.getString( PKG, "JobFolderIsEmpty.Foldername.Label" ) );
    props.setLook(wlFoldername);
    FormData fdlFoldername = new FormData();
    fdlFoldername.left = new FormAttachment( 0, 0 );
    fdlFoldername.top = new FormAttachment( wName, margin );
    fdlFoldername.right = new FormAttachment( middle, -margin );
    wlFoldername.setLayoutData(fdlFoldername);

    Button wbFoldername = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbFoldername);
    wbFoldername.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbFoldername = new FormData();
    fdbFoldername.right = new FormAttachment( 100, 0 );
    fdbFoldername.top = new FormAttachment( wName, 0 );
    wbFoldername.setLayoutData(fdbFoldername);

    wFoldername = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFoldername );
    wFoldername.addModifyListener( lsMod );
    FormData fdFoldername = new FormData();
    fdFoldername.left = new FormAttachment( middle, 0 );
    fdFoldername.top = new FormAttachment( wName, margin );
    fdFoldername.right = new FormAttachment(wbFoldername, -margin );
    wFoldername.setLayoutData(fdFoldername);

    // Include sub folders?
    Label wlIncludeSubFolders = new Label(shell, SWT.RIGHT);
    wlIncludeSubFolders.setText( BaseMessages.getString( PKG, "JobFolderIsEmpty.IncludeSubFolders.Label" ) );
    props.setLook(wlIncludeSubFolders);
    FormData fdlIncludeSubFolders = new FormData();
    fdlIncludeSubFolders.left = new FormAttachment( 0, 0 );
    fdlIncludeSubFolders.top = new FormAttachment( wFoldername, margin );
    fdlIncludeSubFolders.right = new FormAttachment( middle, -margin );
    wlIncludeSubFolders.setLayoutData(fdlIncludeSubFolders);
    wIncludeSubFolders = new Button( shell, SWT.CHECK );
    props.setLook( wIncludeSubFolders );
    wIncludeSubFolders
      .setToolTipText( BaseMessages.getString( PKG, "JobFolderIsEmpty.IncludeSubFolders.Tooltip" ) );
    FormData fdIncludeSubFolders = new FormData();
    fdIncludeSubFolders.left = new FormAttachment( middle, 0 );
    fdIncludeSubFolders.top = new FormAttachment( wFoldername, margin );
    fdIncludeSubFolders.right = new FormAttachment( 100, 0 );
    wIncludeSubFolders.setLayoutData(fdIncludeSubFolders);
    wIncludeSubFolders.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Specify wildcard?
    Label wlSpecifyWildcard = new Label(shell, SWT.RIGHT);
    wlSpecifyWildcard.setText( BaseMessages.getString( PKG, "JobFolderIsEmpty.SpecifyWildcard.Label" ) );
    props.setLook(wlSpecifyWildcard);
    FormData fdlSpecifyWildcard = new FormData();
    fdlSpecifyWildcard.left = new FormAttachment( 0, 0 );
    fdlSpecifyWildcard.top = new FormAttachment( wIncludeSubFolders, margin );
    fdlSpecifyWildcard.right = new FormAttachment( middle, -margin );
    wlSpecifyWildcard.setLayoutData(fdlSpecifyWildcard);
    wSpecifyWildcard = new Button( shell, SWT.CHECK );
    props.setLook( wSpecifyWildcard );
    wSpecifyWildcard.setToolTipText( BaseMessages.getString( PKG, "JobFolderIsEmpty.SpecifyWildcard.Tooltip" ) );
    FormData fdSpecifyWildcard = new FormData();
    fdSpecifyWildcard.left = new FormAttachment( middle, 0 );
    fdSpecifyWildcard.top = new FormAttachment( wIncludeSubFolders, margin );
    fdSpecifyWildcard.right = new FormAttachment( 100, 0 );
    wSpecifyWildcard.setLayoutData(fdSpecifyWildcard);
    wSpecifyWildcard.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
        CheckLimitSearch();
      }
    } );

    // Wildcard line
    wlWildcard = new Label( shell, SWT.RIGHT );
    wlWildcard.setText( BaseMessages.getString( PKG, "JobFolderIsEmpty.Wildcard.Label" ) );
    props.setLook( wlWildcard );
    FormData fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment( 0, 0 );
    fdlWildcard.top = new FormAttachment( wSpecifyWildcard, margin );
    fdlWildcard.right = new FormAttachment( middle, -margin );
    wlWildcard.setLayoutData(fdlWildcard);
    wWildcard = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wWildcard );
    wWildcard.addModifyListener( lsMod );
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment( middle, 0 );
    fdWildcard.top = new FormAttachment( wSpecifyWildcard, margin );
    fdWildcard.right = new FormAttachment( 100, -margin );
    wWildcard.setLayoutData(fdWildcard);

    // Whenever something changes, set the tooltip to the expanded version:
    wFoldername.addModifyListener( e -> wFoldername.setToolTipText( variables.resolve( wFoldername.getText() ) ) );
    // Whenever something changes, set the tooltip to the expanded version:
    wWildcard.addModifyListener( e -> wWildcard.setToolTipText( variables.resolve( wWildcard.getText() ) ) );

    wbFoldername.addListener(SWT.Selection, e-> BaseDialog.presentDirectoryDialog( shell, wFoldername, variables ));

    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOk, wCancel}, margin, wWildcard );

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
    wFoldername.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    CheckLimitSearch();

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

  private void CheckLimitSearch() {
    wlWildcard.setEnabled( wSpecifyWildcard.getSelection() );
    wWildcard.setEnabled( wSpecifyWildcard.getSelection() );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( action.getName() != null ) {
      wName.setText( action.getName() );
    }
    if ( action.getFoldername() != null ) {
      wFoldername.setText( action.getFoldername() );
    }
    wIncludeSubFolders.setSelection( action.isIncludeSubFolders() );
    wSpecifyWildcard.setSelection( action.isSpecifyWildcard() );
    if ( action.getWildcard() != null ) {
      wWildcard.setText( action.getWildcard() );
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
    action.setFoldername( wFoldername.getText() );
    action.setIncludeSubFolders( wIncludeSubFolders.getSelection() );
    action.setSpecifyWildcard( wSpecifyWildcard.getSelection() );
    action.setWildcard( wWildcard.getText() );

    dispose();
  }
}
