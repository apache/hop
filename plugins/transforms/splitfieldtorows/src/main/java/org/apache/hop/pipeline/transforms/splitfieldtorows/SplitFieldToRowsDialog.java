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

package org.apache.hop.pipeline.transforms.splitfieldtorows;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;
import org.apache.hop.core.variables.IVariables;

public class SplitFieldToRowsDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = SplitFieldToRowsMeta.class; // For Translator

  private ComboVar wSplitField;

  private TextVar wDelimiter;

  private TextVar wValName;

  private Button wInclRownum;
  
  private Label wlInclRownumField;
  private TextVar wInclRownumField;
  
  private Label wlResetRownum;
  private Button wResetRownum;

  private final SplitFieldToRowsMeta input;

  private Button wDelimiterIsRegex;

  public SplitFieldToRowsDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (SplitFieldToRowsMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Buttons at the bottom
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, null );

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    fdlTransformName.top = new FormAttachment( 0, margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.top = new FormAttachment( 0, margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );

    // Typefield line
    Label wlSplitfield = new Label(shell, SWT.RIGHT);
    wlSplitfield.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.SplitField.Label" ) );
    props.setLook(wlSplitfield);
    FormData fdlSplitfield = new FormData();
    fdlSplitfield.left = new FormAttachment( 0, 0 );
    fdlSplitfield.right = new FormAttachment( middle, -margin );
    fdlSplitfield.top = new FormAttachment( wTransformName, margin );
    wlSplitfield.setLayoutData( fdlSplitfield );

    wSplitField = new ComboVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSplitField.setToolTipText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.UrlField.Tooltip" ) );
    props.setLook( wSplitField );
    wSplitField.addModifyListener( lsMod );
    FormData fdSplitfield = new FormData();
    fdSplitfield.left = new FormAttachment( middle, 0 );
    fdSplitfield.top = new FormAttachment( wTransformName, margin );
    fdSplitfield.right = new FormAttachment( 100, 0 );
    wSplitField.setLayoutData( fdSplitfield );
    wSplitField.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        BaseTransformDialog.getFieldsFromPrevious( wSplitField, pipelineMeta, transformMeta );
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // Delimiter line
    Label wlDelimiter = new Label(shell, SWT.RIGHT);
    wlDelimiter.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.Delimiter.Label" ) );
    props.setLook(wlDelimiter);
    FormData fdlDelimiter = new FormData();
    fdlDelimiter.left = new FormAttachment( 0, 0 );
    fdlDelimiter.right = new FormAttachment( middle, -margin );
    fdlDelimiter.top = new FormAttachment( wSplitField, margin );
    wlDelimiter.setLayoutData( fdlDelimiter );
    wDelimiter = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wDelimiter.setText( "" );
    props.setLook( wDelimiter );
    wDelimiter.addModifyListener( lsMod );
    FormData fdDelimiter = new FormData();
    fdDelimiter.left = new FormAttachment( middle, 0 );
    fdDelimiter.top = new FormAttachment( wSplitField, margin );
    fdDelimiter.right = new FormAttachment( 100, 0 );
    wDelimiter.setLayoutData( fdDelimiter );

    // Add File to the result files name
    Label wlDelimiterIsRegex = new Label( shell, SWT.RIGHT );
    wlDelimiterIsRegex.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.DelimiterIsRegex.Label" ) );
    props.setLook( wlDelimiterIsRegex );
    FormData fdlDelimiterIsRegex = new FormData();
    fdlDelimiterIsRegex.left = new FormAttachment( 0, 0 );
    fdlDelimiterIsRegex.top = new FormAttachment( wDelimiter );
    fdlDelimiterIsRegex.right = new FormAttachment( middle, -margin );
    wlDelimiterIsRegex.setLayoutData( fdlDelimiterIsRegex );
    wDelimiterIsRegex = new Button( shell, SWT.CHECK );
    wDelimiterIsRegex.setToolTipText( BaseMessages.getString(
      PKG, "SplitFieldToRowsDialog.DelimiterIsRegex.Tooltip" ) );
    props.setLook( wDelimiterIsRegex );
    FormData fdDelimiterIsRegex = new FormData();
    fdDelimiterIsRegex.left = new FormAttachment( middle, 0 );
    fdDelimiterIsRegex.top = new FormAttachment( wlDelimiterIsRegex, 0, SWT.CENTER );
    fdDelimiterIsRegex.right = new FormAttachment( 100, 0 );
    wDelimiterIsRegex.setLayoutData( fdDelimiterIsRegex );
    SelectionAdapter lsSelR = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wDelimiterIsRegex.addSelectionListener( lsSelR );

    // ValName line
    Label wlValName = new Label(shell, SWT.RIGHT);
    wlValName.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.NewFieldName.Label" ) );
    props.setLook(wlValName);
    FormData fdlValName = new FormData();
    fdlValName.left = new FormAttachment( 0, 0 );
    fdlValName.right = new FormAttachment( middle, -margin );
    fdlValName.top = new FormAttachment( wDelimiterIsRegex, margin );
    wlValName.setLayoutData( fdlValName );
    wValName = new TextVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wValName.setText( "" );
    props.setLook( wValName );
    wValName.addModifyListener( lsMod );
    FormData fdValName = new FormData();
    fdValName.left = new FormAttachment( middle, 0 );
    fdValName.right = new FormAttachment( 100, 0 );
    fdValName.top = new FormAttachment( wDelimiterIsRegex, margin );
    wValName.setLayoutData( fdValName );

    // ///////////////////////////////
    // START OF Additional Fields GROUP //
    // ///////////////////////////////

    Group wAdditionalFields = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wAdditionalFields);
    wAdditionalFields.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.wAdditionalFields.Label" ) );

    FormLayout AdditionalFieldsgroupLayout = new FormLayout();
    AdditionalFieldsgroupLayout.marginWidth = 10;
    AdditionalFieldsgroupLayout.marginHeight = 10;
    wAdditionalFields.setLayout( AdditionalFieldsgroupLayout );

    Label wlInclRownum = new Label(wAdditionalFields, SWT.RIGHT);
    wlInclRownum.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.InclRownum.Label" ) );
    props.setLook(wlInclRownum);
    FormData fdlInclRownum = new FormData();
    fdlInclRownum.left = new FormAttachment( 0, 0 );
    fdlInclRownum.top = new FormAttachment( wValName, margin );
    fdlInclRownum.right = new FormAttachment( middle, -margin );
    wlInclRownum.setLayoutData( fdlInclRownum );
    wInclRownum = new Button(wAdditionalFields, SWT.CHECK );
    props.setLook( wInclRownum );
    wInclRownum.setToolTipText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.InclRownum.Tooltip" ) );
    FormData fdRownum = new FormData();
    fdRownum.left = new FormAttachment( middle, 0 );
    fdRownum.top = new FormAttachment( wlInclRownum, 0, SWT.CENTER );
    wInclRownum.setLayoutData( fdRownum );
    wInclRownum.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setIncludeRownum();
        input.setChanged();
      }
    } );

    wlInclRownumField = new Label(wAdditionalFields, SWT.RIGHT );
    wlInclRownumField.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.InclRownumField.Label" ) );
    props.setLook( wlInclRownumField );
    FormData fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment( wInclRownum, margin );
    fdlInclRownumField.top = new FormAttachment( wValName, margin );
    wlInclRownumField.setLayoutData( fdlInclRownumField );
    wInclRownumField = new TextVar( variables, wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclRownumField );
    wInclRownumField.addModifyListener( lsMod );
    FormData fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment( wlInclRownumField, margin );
    fdInclRownumField.top = new FormAttachment( wValName, margin );
    fdInclRownumField.right = new FormAttachment( 100, 0 );
    wInclRownumField.setLayoutData( fdInclRownumField );

    wlResetRownum = new Label(wAdditionalFields, SWT.RIGHT );
    wlResetRownum.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.ResetRownum.Label" ) );
    props.setLook( wlResetRownum );
    FormData fdlResetRownum = new FormData();
    fdlResetRownum.left = new FormAttachment( wInclRownum, margin );
    fdlResetRownum.top = new FormAttachment( wInclRownumField, margin );
    wlResetRownum.setLayoutData( fdlResetRownum );
    wResetRownum = new Button(wAdditionalFields, SWT.CHECK );
    props.setLook( wResetRownum );
    wResetRownum.setToolTipText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.ResetRownum.Tooltip" ) );
    fdRownum = new FormData();
    fdRownum.left = new FormAttachment( wlResetRownum, margin );
    fdRownum.top = new FormAttachment( wlResetRownum, 0, SWT.CENTER );
    wResetRownum.setLayoutData( fdRownum );
    wResetRownum.addSelectionListener( new ComponentSelectionListener( input ) );

    FormData fdAdditionalFields = new FormData();
    fdAdditionalFields.left = new FormAttachment( 0, margin );
    fdAdditionalFields.top = new FormAttachment( wValName, margin );
    fdAdditionalFields.right = new FormAttachment( 100, -margin );
    fdAdditionalFields.bottom = new FormAttachment( wOk, -2*margin);
    wAdditionalFields.setLayoutData( fdAdditionalFields );

    // ///////////////////////////////
    // END OF Additional Fields GROUP //
    // ///////////////////////////////

     // Add listeners
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );
    wValName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  public void setIncludeRownum() {
    wlInclRownumField.setEnabled( wInclRownum.getSelection() );
    wInclRownumField.setEnabled( wInclRownum.getSelection() );
    wlResetRownum.setEnabled( wInclRownum.getSelection() );
    wResetRownum.setEnabled( wInclRownum.getSelection() );
  }

  public void getData() {
    wSplitField.setText( Const.NVL( input.getSplitField(), "" ) );
    wDelimiter.setText( Const.NVL( input.getDelimiter(), "" ) );
    wValName.setText( Const.NVL( input.getNewFieldname(), "" ) );
    wInclRownum.setSelection( input.includeRowNumber() );
    wDelimiterIsRegex.setSelection( input.isDelimiterRegex() );
    if ( input.getRowNumberField() != null ) {
      wInclRownumField.setText( input.getRowNumberField() );
    }
    wResetRownum.setSelection( input.resetRowNumber() );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value
    input.setSplitField( wSplitField.getText() );
    input.setDelimiter( wDelimiter.getText() );
    input.setNewFieldname( wValName.getText() );
    input.setIncludeRowNumber( wInclRownum.getSelection() );
    input.setRowNumberField( wInclRownumField.getText() );
    input.setResetRowNumber( wResetRownum.getSelection() );
    input.setDelimiterRegex( wDelimiterIsRegex.getSelection() );
    dispose();
  }
}
