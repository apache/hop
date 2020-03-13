/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.trans.steps.splitfieldtorows;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.steps.splitfieldtorows.SplitFieldToRowsMeta;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.trans.step.BaseStepDialog;
import org.apache.hop.ui.trans.step.ComponentSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class SplitFieldToRowsDialog extends BaseStepDialog implements StepDialogInterface {
  private static Class<?> PKG = SplitFieldToRowsMeta.class; // for i18n purposes, needed by Translator2!!

  private Label wlSplitfield;
  private ComboVar wSplitfield;
  private FormData fdlSplitfield, fdSplitfield;

  private Label wlDelimiter;
  private TextVar wDelimiter;
  private FormData fdlDelimiter, fdDelimiter;

  private Label wlValName;
  private TextVar wValName;
  private FormData fdlValName, fdValName;

  private Label wlInclRownum;
  private Button wInclRownum;
  private FormData fdlInclRownum, fdRownum;

  private Label wlInclRownumField;
  private TextVar wInclRownumField;
  private FormData fdlInclRownumField, fdInclRownumField;

  private Label wlResetRownum;
  private Button wResetRownum;
  private FormData fdlResetRownum;

  private Group wAdditionalFields;
  private FormData fdAdditionalFields;

  private SplitFieldToRowsMeta input;

  private Button wDelimiterIsRegex;

  public SplitFieldToRowsDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (BaseStepMeta) in, transMeta, sname );
    input = (SplitFieldToRowsMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        input.setChanged();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.Stepname.Label" ) );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( 0, margin );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );

    // Typefield line
    wlSplitfield = new Label( shell, SWT.RIGHT );
    wlSplitfield.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.SplitField.Label" ) );
    props.setLook( wlSplitfield );
    fdlSplitfield = new FormData();
    fdlSplitfield.left = new FormAttachment( 0, 0 );
    fdlSplitfield.right = new FormAttachment( middle, -margin );
    fdlSplitfield.top = new FormAttachment( wStepname, margin );
    wlSplitfield.setLayoutData( fdlSplitfield );

    wSplitfield = new ComboVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wSplitfield.setToolTipText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.UrlField.Tooltip" ) );
    props.setLook( wSplitfield );
    wSplitfield.addModifyListener( lsMod );
    fdSplitfield = new FormData();
    fdSplitfield.left = new FormAttachment( middle, 0 );
    fdSplitfield.top = new FormAttachment( wStepname, margin );
    fdSplitfield.right = new FormAttachment( 100, 0 );
    wSplitfield.setLayoutData( fdSplitfield );
    wSplitfield.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        BaseStepDialog.getFieldsFromPrevious( wSplitfield, transMeta, stepMeta );
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // Delimiter line
    wlDelimiter = new Label( shell, SWT.RIGHT );
    wlDelimiter.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.Delimiter.Label" ) );
    props.setLook( wlDelimiter );
    fdlDelimiter = new FormData();
    fdlDelimiter.left = new FormAttachment( 0, 0 );
    fdlDelimiter.right = new FormAttachment( middle, -margin );
    fdlDelimiter.top = new FormAttachment( wSplitfield, margin );
    wlDelimiter.setLayoutData( fdlDelimiter );
    wDelimiter = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wDelimiter.setText( "" );
    props.setLook( wDelimiter );
    wDelimiter.addModifyListener( lsMod );
    fdDelimiter = new FormData();
    fdDelimiter.left = new FormAttachment( middle, 0 );
    fdDelimiter.top = new FormAttachment( wSplitfield, margin );
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
    fdDelimiterIsRegex.top = new FormAttachment( wDelimiter );
    fdDelimiterIsRegex.right = new FormAttachment( 100, 0 );
    wDelimiterIsRegex.setLayoutData( fdDelimiterIsRegex );
    SelectionAdapter lsSelR = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };
    wDelimiterIsRegex.addSelectionListener( lsSelR );

    // ValName line
    wlValName = new Label( shell, SWT.RIGHT );
    wlValName.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.NewFieldName.Label" ) );
    props.setLook( wlValName );
    fdlValName = new FormData();
    fdlValName.left = new FormAttachment( 0, 0 );
    fdlValName.right = new FormAttachment( middle, -margin );
    fdlValName.top = new FormAttachment( wDelimiterIsRegex, margin );
    wlValName.setLayoutData( fdlValName );
    wValName = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wValName.setText( "" );
    props.setLook( wValName );
    wValName.addModifyListener( lsMod );
    fdValName = new FormData();
    fdValName.left = new FormAttachment( middle, 0 );
    fdValName.right = new FormAttachment( 100, 0 );
    fdValName.top = new FormAttachment( wDelimiterIsRegex, margin );
    wValName.setLayoutData( fdValName );

    // ///////////////////////////////
    // START OF Additional Fields GROUP //
    // ///////////////////////////////

    wAdditionalFields = new Group( shell, SWT.SHADOW_NONE );
    props.setLook( wAdditionalFields );
    wAdditionalFields.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.wAdditionalFields.Label" ) );

    FormLayout AdditionalFieldsgroupLayout = new FormLayout();
    AdditionalFieldsgroupLayout.marginWidth = 10;
    AdditionalFieldsgroupLayout.marginHeight = 10;
    wAdditionalFields.setLayout( AdditionalFieldsgroupLayout );

    wlInclRownum = new Label( wAdditionalFields, SWT.RIGHT );
    wlInclRownum.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.InclRownum.Label" ) );
    props.setLook( wlInclRownum );
    fdlInclRownum = new FormData();
    fdlInclRownum.left = new FormAttachment( 0, 0 );
    fdlInclRownum.top = new FormAttachment( wValName, margin );
    fdlInclRownum.right = new FormAttachment( middle, -margin );
    wlInclRownum.setLayoutData( fdlInclRownum );
    wInclRownum = new Button( wAdditionalFields, SWT.CHECK );
    props.setLook( wInclRownum );
    wInclRownum.setToolTipText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.InclRownum.Tooltip" ) );
    fdRownum = new FormData();
    fdRownum.left = new FormAttachment( middle, 0 );
    fdRownum.top = new FormAttachment( wValName, margin );
    wInclRownum.setLayoutData( fdRownum );
    wInclRownum.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setIncludeRownum();
        input.setChanged();
      }
    } );

    wlInclRownumField = new Label( wAdditionalFields, SWT.RIGHT );
    wlInclRownumField.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.InclRownumField.Label" ) );
    props.setLook( wlInclRownumField );
    fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment( wInclRownum, margin );
    fdlInclRownumField.top = new FormAttachment( wValName, margin );
    wlInclRownumField.setLayoutData( fdlInclRownumField );
    wInclRownumField = new TextVar( transMeta, wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInclRownumField );
    wInclRownumField.addModifyListener( lsMod );
    fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment( wlInclRownumField, margin );
    fdInclRownumField.top = new FormAttachment( wValName, margin );
    fdInclRownumField.right = new FormAttachment( 100, 0 );
    wInclRownumField.setLayoutData( fdInclRownumField );

    wlResetRownum = new Label( wAdditionalFields, SWT.RIGHT );
    wlResetRownum.setText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.ResetRownum.Label" ) );
    props.setLook( wlResetRownum );
    fdlResetRownum = new FormData();
    fdlResetRownum.left = new FormAttachment( wInclRownum, margin );
    fdlResetRownum.top = new FormAttachment( wInclRownumField, margin );
    wlResetRownum.setLayoutData( fdlResetRownum );
    wResetRownum = new Button( wAdditionalFields, SWT.CHECK );
    props.setLook( wResetRownum );
    wResetRownum.setToolTipText( BaseMessages.getString( PKG, "SplitFieldToRowsDialog.ResetRownum.Tooltip" ) );
    fdRownum = new FormData();
    fdRownum.left = new FormAttachment( wlResetRownum, margin );
    fdRownum.top = new FormAttachment( wInclRownumField, margin );
    wResetRownum.setLayoutData( fdRownum );
    wResetRownum.addSelectionListener( new ComponentSelectionListener( input ) );

    fdAdditionalFields = new FormData();
    fdAdditionalFields.left = new FormAttachment( 0, margin );
    fdAdditionalFields.top = new FormAttachment( wValName, margin );
    fdAdditionalFields.right = new FormAttachment( 100, -margin );
    wAdditionalFields.setLayoutData( fdAdditionalFields );

    // ///////////////////////////////
    // END OF Additional Fields GROUP //
    // ///////////////////////////////

    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseStepDialog.positionBottomButtons( shell, new Button[] { wOK, wCancel }, margin, wAdditionalFields );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wStepname.addSelectionListener( lsDef );
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
    return stepname;
  }

  public void setIncludeRownum() {
    wlInclRownumField.setEnabled( wInclRownum.getSelection() );
    wInclRownumField.setEnabled( wInclRownum.getSelection() );
    wlResetRownum.setEnabled( wInclRownum.getSelection() );
    wResetRownum.setEnabled( wInclRownum.getSelection() );
  }

  public void getData() {
    wSplitfield.setText( Const.NVL( input.getSplitField(), "" ) );
    wDelimiter.setText( Const.NVL( input.getDelimiter(), "" ) );
    wValName.setText( Const.NVL( input.getNewFieldname(), "" ) );
    wInclRownum.setSelection( input.includeRowNumber() );
    wDelimiterIsRegex.setSelection( input.isDelimiterRegex() );
    if ( input.getRowNumberField() != null ) {
      wInclRownumField.setText( input.getRowNumberField() );
    }
    wResetRownum.setSelection( input.resetRowNumber() );

    wStepname.selectAll();
    wStepname.setFocus();
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    if ( Utils.isEmpty( wStepname.getText() ) ) {
      return;
    }

    stepname = wStepname.getText(); // return value
    input.setSplitField( wSplitfield.getText() );
    input.setDelimiter( wDelimiter.getText() );
    input.setNewFieldname( wValName.getText() );
    input.setIncludeRowNumber( wInclRownum.getSelection() );
    input.setRowNumberField( wInclRownumField.getText() );
    input.setResetRowNumber( wResetRownum.getSelection() );
    input.setDelimiterRegex( wDelimiterIsRegex.getSelection() );
    dispose();
  }
}
