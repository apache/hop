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

package org.apache.hop.workflow.actions.xml.xsdvalidator;

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
 * This dialog allows you to edit the XSD Validator job entry settings.
 * 
 * @author Samatar Hassan
 * @since 30-04-2007
 */
public class XsdValidatorDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = XsdValidator.class; // For Translator

  private static final String[] FILETYPES_XML = new String[] {
    BaseMessages.getString( PKG, "JobEntryXSDValidator.Filetype.Xml" ),
    BaseMessages.getString( PKG, "JobEntryXSDValidator.Filetype.All" ) };

  private static final String[] FILETYPES_XSD = new String[] {
    BaseMessages.getString( PKG, "JobEntryXSDValidator.Filetype.Xsd" ),
    BaseMessages.getString( PKG, "JobEntryXSDValidator.Filetype.All" ) };

  private Text wName;

  private Button wAllowExternalEntities;

  private TextVar wxmlFilename;

  private TextVar wxsdFilename;

  private XsdValidator action;
  private Shell shell;

  private boolean changed;

  public XsdValidatorDialog(Shell parent, IAction action, WorkflowMeta workflowMeta ) {
    super( parent, workflowMeta );
    action = (XsdValidator) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobEntryXSDValidator.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE  );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    WorkflowMeta workflowMeta = getWorkflowMeta();
    
    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobEntryXSDValidator.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Name line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "JobEntryXSDValidator.Name.Label" ) );
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

    // Enable/Disable external entity for XSD validation.
    Label wlAllowExternalEntities = new Label(shell, SWT.RIGHT);
    wlAllowExternalEntities.setText( BaseMessages.getString( PKG, "JobEntryXSDValidator.AllowExternalEntities.Label" ) );
    props.setLook(wlAllowExternalEntities);
    FormData fdlAllowExternalEntities = new FormData();
    fdlAllowExternalEntities.left = new FormAttachment( 0, 0 );
    fdlAllowExternalEntities.right = new FormAttachment( middle, -margin );
    fdlAllowExternalEntities.top = new FormAttachment( wName, margin );
    wlAllowExternalEntities.setLayoutData(fdlAllowExternalEntities);
    wAllowExternalEntities = new Button( shell, SWT.CHECK );
    props.setLook( wAllowExternalEntities );
    FormData fdAllowExternalEntities = new FormData();
    fdAllowExternalEntities.left = new FormAttachment( middle, 0 );
    fdAllowExternalEntities.top = new FormAttachment( wName, margin );
    fdAllowExternalEntities.right = new FormAttachment( 100, 0 );
    wAllowExternalEntities.setLayoutData(fdAllowExternalEntities);

    wAllowExternalEntities.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        action.setChanged();
      }
    } );

    // Filename 1 line
    Label wlxmlFilename = new Label(shell, SWT.RIGHT);
    wlxmlFilename.setText( BaseMessages.getString( PKG, "JobEntryXSDValidator.xmlFilename.Label" ) );
    props.setLook(wlxmlFilename);
    FormData fdlxmlFilename = new FormData();
    fdlxmlFilename.left = new FormAttachment( 0, 0 );
    fdlxmlFilename.top = new FormAttachment( wAllowExternalEntities, margin );
    fdlxmlFilename.right = new FormAttachment( middle, -margin );
    wlxmlFilename.setLayoutData(fdlxmlFilename);
    Button wbxmlFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbxmlFilename);
    wbxmlFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbxmlFilename = new FormData();
    fdbxmlFilename.right = new FormAttachment( 100, 0 );
    fdbxmlFilename.top = new FormAttachment( wAllowExternalEntities, 0 );
    wbxmlFilename.setLayoutData(fdbxmlFilename);
    wxmlFilename = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wxmlFilename );
    wxmlFilename.addModifyListener( lsMod );
    FormData fdxmlFilename = new FormData();
    fdxmlFilename.left = new FormAttachment( middle, 0 );
    fdxmlFilename.top = new FormAttachment( wAllowExternalEntities, margin );
    fdxmlFilename.right = new FormAttachment(wbxmlFilename, -margin );
    wxmlFilename.setLayoutData(fdxmlFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wxmlFilename.addModifyListener( e -> wxmlFilename.setToolTipText( variables.resolve( wxmlFilename.getText() ) ) );

    wbxmlFilename.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        dialog.setFilterExtensions( new String[] { "*.xml;*.XML", "*" } );
        if ( wxmlFilename.getText() != null ) {
          dialog.setFileName( variables.resolve( wxmlFilename.getText() ) );
        }
        dialog.setFilterNames( FILETYPES_XML );
        if ( dialog.open() != null ) {
          wxmlFilename.setText( dialog.getFilterPath() + Const.FILE_SEPARATOR + dialog.getFileName() );
        }
      }
    } );

    // Filename 2 line
    Label wlxsdFilename = new Label(shell, SWT.RIGHT);
    wlxsdFilename.setText( BaseMessages.getString( PKG, "JobEntryXSDValidator.xsdFilename.Label" ) );
    props.setLook(wlxsdFilename);
    FormData fdlxsdFilename = new FormData();
    fdlxsdFilename.left = new FormAttachment( 0, 0 );
    fdlxsdFilename.top = new FormAttachment( wxmlFilename, margin );
    fdlxsdFilename.right = new FormAttachment( middle, -margin );
    wlxsdFilename.setLayoutData(fdlxsdFilename);
    Button wbxsdFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbxsdFilename);
    wbxsdFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbxsdFilename = new FormData();
    fdbxsdFilename.right = new FormAttachment( 100, 0 );
    fdbxsdFilename.top = new FormAttachment( wxmlFilename, 0 );
    wbxsdFilename.setLayoutData(fdbxsdFilename);
    wxsdFilename = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wxsdFilename );
    wxsdFilename.addModifyListener( lsMod );
    FormData fdxsdFilename = new FormData();
    fdxsdFilename.left = new FormAttachment( middle, 0 );
    fdxsdFilename.top = new FormAttachment( wxmlFilename, margin );
    fdxsdFilename.right = new FormAttachment(wbxsdFilename, -margin );
    wxsdFilename.setLayoutData(fdxsdFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wxsdFilename.addModifyListener( e -> wxsdFilename.setToolTipText( variables.resolve( wxsdFilename.getText() ) ) );

    wbxsdFilename.addSelectionListener(new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        dialog.setFilterExtensions( new String[] { "*.xsd;*.XSD", "*" } );
        if ( wxsdFilename.getText() != null ) {
          dialog.setFileName( variables.resolve( wxsdFilename.getText() ) );
        }
        dialog.setFilterNames( FILETYPES_XSD );
        if ( dialog.open() != null ) {
          wxsdFilename.setText( dialog.getFilterPath() + Const.FILE_SEPARATOR + dialog.getFileName() );
        }
      }
    } );

    Button wOK = new Button(shell, SWT.PUSH);
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOK, wCancel}, margin, wxsdFilename );

    // Add listeners
    Listener lsCancel = e -> cancel();
    Listener lsOK = e -> ok();

    wCancel.addListener( SWT.Selection, lsCancel);
    wOK.addListener( SWT.Selection, lsOK);

    SelectionAdapter lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected(SelectionEvent e) {
        ok();
      }
    };

    wName.addSelectionListener(lsDef);
    wAllowExternalEntities.addSelectionListener(lsDef);
    wxmlFilename.addSelectionListener(lsDef);
    wxsdFilename.addSelectionListener(lsDef);

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
    wName.setText( Const.nullToEmpty( action.getName() ) );
    wAllowExternalEntities.setSelection( action.isAllowExternalEntities() );
    wxmlFilename.setText( Const.nullToEmpty( action.getxmlFilename() ) );
    wxsdFilename.setText( Const.nullToEmpty( action.getxsdFilename() ) );

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
      mb.setText( BaseMessages.getString( PKG, "System.ActionNameMissing.Title" ) );
      mb.setMessage( BaseMessages.getString( PKG, "System.ActionNameMissing.Msg" ) );
      mb.open();
      return;
    }
    action.setName( wName.getText() );
    action.setAllowExternalEntities( wAllowExternalEntities.getSelection() );
    action.setxmlFilename( wxmlFilename.getText() );
    action.setxsdFilename( wxsdFilename.getText() );

    dispose();
  }
}
