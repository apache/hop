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

package org.apache.hop.workflow.actions.xml.dtdvalidator;

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
 * This dialog allows you to edit the DTD Validator job entry settings.
 * 
 * @author Samatar Hassan
 * @since 30-04-2007
 */

public class DtdValidatorDialog extends ActionDialog implements IActionDialog {
  private static final Class<?> PKG = DtdValidator.class; // For Translator

  private static final String[] FILETYPES_XML = new String[] {
    BaseMessages.getString( PKG, "JobEntryDTDValidator.Filetype.Xml" ),
    BaseMessages.getString( PKG, "JobEntryDTDValidator.Filetype.All" ) };

  private static final String[] FILETYPES_DTD = new String[] {
    BaseMessages.getString( PKG, "JobEntryDTDValidator.Filetype.Dtd" ),
    BaseMessages.getString( PKG, "JobEntryDTDValidator.Filetype.All" ) };

  private Text wName;

  private TextVar wxmlFilename;

  private Label wldtdFilename;
  private Button wbdtdFilename;
  private TextVar wdtdFilename;

  private Button wDTDIntern;

  private DtdValidator action;

  private Shell shell;

  private boolean changed;

  public DtdValidatorDialog(Shell parent, IAction action, WorkflowMeta jobMeta ) {
    super( parent, jobMeta );
    action = (DtdValidator) action;
    if ( this.action.getName() == null ) {
      this.action.setName( BaseMessages.getString( PKG, "JobEntryDTDValidator.Name.Default" ) );
    }
  }

  public IAction open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    WorkflowDialog.setShellImage( shell, action );

    WorkflowMeta workflowMeta = getWorkflowMeta();
    
    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "JobEntryDTDValidator.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Name line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText( BaseMessages.getString( PKG, "JobEntryDTDValidator.Name.Label" ) );
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

    // XML Filename
    Label wlxmlFilename = new Label(shell, SWT.RIGHT);
    wlxmlFilename.setText( BaseMessages.getString( PKG, "JobEntryDTDValidator.xmlFilename.Label" ) );
    props.setLook(wlxmlFilename);
    FormData fdlxmlFilename = new FormData();
    fdlxmlFilename.left = new FormAttachment( 0, 0 );
    fdlxmlFilename.top = new FormAttachment( wName, margin );
    fdlxmlFilename.right = new FormAttachment( middle, -margin );
    wlxmlFilename.setLayoutData(fdlxmlFilename);
    Button wbxmlFilename = new Button(shell, SWT.PUSH | SWT.CENTER);
    props.setLook(wbxmlFilename);
    wbxmlFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbxmlFilename = new FormData();
    fdbxmlFilename.right = new FormAttachment( 100, 0 );
    fdbxmlFilename.top = new FormAttachment( wName, 0 );
    wbxmlFilename.setLayoutData(fdbxmlFilename);
    wxmlFilename = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wxmlFilename );
    wxmlFilename.addModifyListener( lsMod );
    FormData fdxmlFilename = new FormData();
    fdxmlFilename.left = new FormAttachment( middle, 0 );
    fdxmlFilename.top = new FormAttachment( wName, margin );
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

    // DTD Intern ?
    // Intern DTD
    Label wlDTDIntern = new Label(shell, SWT.RIGHT);
    wlDTDIntern.setText( BaseMessages.getString( PKG, "JobEntryDTDValidator.DTDIntern.Label" ) );
    props.setLook(wlDTDIntern);
    FormData fdlDTDIntern = new FormData();
    fdlDTDIntern.left = new FormAttachment( 0, 0 );
    fdlDTDIntern.top = new FormAttachment( wxmlFilename, margin );
    fdlDTDIntern.right = new FormAttachment( middle, -margin );
    wlDTDIntern.setLayoutData(fdlDTDIntern);
    wDTDIntern = new Button( shell, SWT.CHECK );
    props.setLook( wDTDIntern );
    wDTDIntern.setToolTipText( BaseMessages.getString( PKG, "JobEntryDTDValidator.DTDIntern.Tooltip" ) );
    FormData fdDTDIntern = new FormData();
    fdDTDIntern.left = new FormAttachment( middle, 0 );
    fdDTDIntern.top = new FormAttachment( wxmlFilename, margin );
    fdDTDIntern.right = new FormAttachment( 100, 0 );
    wDTDIntern.setLayoutData(fdDTDIntern);
    wDTDIntern.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        ActiveDTDFilename();
        action.setChanged();
      }
    } );

    // DTD Filename
    wldtdFilename = new Label( shell, SWT.RIGHT );
    wldtdFilename.setText( BaseMessages.getString( PKG, "JobEntryDTDValidator.DTDFilename.Label" ) );
    props.setLook( wldtdFilename );
    FormData fdldtdFilename = new FormData();
    fdldtdFilename.left = new FormAttachment( 0, 0 );
    fdldtdFilename.top = new FormAttachment( wDTDIntern, margin );
    fdldtdFilename.right = new FormAttachment( middle, -margin );
    wldtdFilename.setLayoutData(fdldtdFilename);
    wbdtdFilename = new Button( shell, SWT.PUSH | SWT.CENTER );
    props.setLook( wbdtdFilename );
    wbdtdFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    FormData fdbdtdFilename = new FormData();
    fdbdtdFilename.right = new FormAttachment( 100, 0 );
    fdbdtdFilename.top = new FormAttachment( wDTDIntern, 0 );
    wbdtdFilename.setLayoutData(fdbdtdFilename);
    wdtdFilename = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wdtdFilename );
    wdtdFilename.addModifyListener( lsMod );
    FormData fddtdFilename = new FormData();
    fddtdFilename.left = new FormAttachment( middle, 0 );
    fddtdFilename.top = new FormAttachment( wDTDIntern, margin );
    fddtdFilename.right = new FormAttachment( wbdtdFilename, -margin );
    wdtdFilename.setLayoutData(fddtdFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wdtdFilename.addModifyListener( e -> wdtdFilename.setToolTipText( variables.resolve( wdtdFilename.getText() ) ) );

    wbdtdFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        dialog.setFilterExtensions( new String[] { "*.dtd;*.DTD", "*" } );
        if ( wdtdFilename.getText() != null ) {
          dialog.setFileName( variables.resolve( wdtdFilename.getText() ) );
        }
        dialog.setFilterNames( FILETYPES_DTD );
        if ( dialog.open() != null ) {
          wdtdFilename.setText( dialog.getFilterPath() + Const.FILE_SEPARATOR + dialog.getFileName() );
        }
      }
    } );

    Button wOK = new Button(shell, SWT.PUSH);
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] {wOK, wCancel}, margin, wdtdFilename );

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
    wxmlFilename.addSelectionListener(lsDef);
    wdtdFilename.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    ActiveDTDFilename();

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

  private void ActiveDTDFilename() {
    wldtdFilename.setEnabled( !wDTDIntern.getSelection() );
    wdtdFilename.setEnabled( !wDTDIntern.getSelection() );
    wbdtdFilename.setEnabled( !wDTDIntern.getSelection() );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( action.getName() != null ) {
      wName.setText( action.getName() );
    }
    if ( action.getxmlFilename() != null ) {
      wxmlFilename.setText( action.getxmlFilename() );
    }
    if ( action.getdtdFilename() != null ) {
      wdtdFilename.setText( action.getdtdFilename() );
    }
    wDTDIntern.setSelection( action.getDTDIntern() );

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
    action.setxmlFilename( wxmlFilename.getText() );
    action.setdtdFilename( wdtdFilename.getText() );

    action.setDTDIntern( wDTDIntern.getSelection() );

    dispose();
  }
}
