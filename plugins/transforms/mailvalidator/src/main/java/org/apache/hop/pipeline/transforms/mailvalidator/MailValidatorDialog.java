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

package org.apache.hop.pipeline.transforms.mailvalidator;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class MailValidatorDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = MailValidatorDialog.class; // For Translator

  private boolean gotPreviousFields = false;

  private CCombo wEmailFieldName;

  private Label wlDefaultSMTPField;
  private CCombo wDefaultSMTPField;

  private TextVar wResult;

  private Label wleMailSender;
  private TextVar weMailSender;

  private Label wlTimeOut;
  private TextVar wTimeOut;

  private Label wlDefaultSMTP;
  private TextVar wDefaultSMTP;

  private Label wlDynamicDefaultSMTP;
  private Button wDynamicDefaultSMTP;

  private Button wResultAsString;

  private Button wSMTPCheck;

  private Label wlResultStringFalse;
  private Label wlResultStringTrue;
  private TextVar wResultStringFalse;
  private TextVar wResultStringTrue;

  private TextVar wErrorMsg;

  private final MailValidatorMeta input;

  public MailValidatorDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (MailValidatorMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "MailValidatorDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "MailValidatorDialog.TransformName.Label" ) );
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

    // emailFieldName field
    Label wlemailFieldName = new Label(shell, SWT.RIGHT);
    wlemailFieldName.setText( BaseMessages.getString( PKG, "MailValidatorDialog.emailFieldName.Label" ) );
    props.setLook(wlemailFieldName);
    FormData fdlemailFieldName = new FormData();
    fdlemailFieldName.left = new FormAttachment( 0, 0 );
    fdlemailFieldName.right = new FormAttachment( middle, -margin );
    fdlemailFieldName.top = new FormAttachment( wTransformName, margin );
    wlemailFieldName.setLayoutData(fdlemailFieldName);

    wEmailFieldName = new CCombo( shell, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wEmailFieldName );
    wEmailFieldName.addModifyListener( lsMod );
    FormData fdemailFieldName = new FormData();
    fdemailFieldName.left = new FormAttachment( middle, 0 );
    fdemailFieldName.top = new FormAttachment( wTransformName, margin );
    fdemailFieldName.right = new FormAttachment( 100, -margin );
    wEmailFieldName.setLayoutData(fdemailFieldName);
    wEmailFieldName.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        get();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // ////////////////////////
    // START OF Settings GROUP
    //

    Group wSettingsGroup = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wSettingsGroup);
    wSettingsGroup.setText( BaseMessages.getString( PKG, "MailValidatorDialog.SettingsGroup.Label" ) );

    FormLayout groupSettings = new FormLayout();
    groupSettings.marginWidth = 10;
    groupSettings.marginHeight = 10;
    wSettingsGroup.setLayout( groupSettings );

    // perform SMTP check?
    Label wlSMTPCheck = new Label(wSettingsGroup, SWT.RIGHT);
    wlSMTPCheck.setText( BaseMessages.getString( PKG, "MailValidatorDialog.SMTPCheck.Label" ) );
    props.setLook(wlSMTPCheck);
    FormData fdlSMTPCheck = new FormData();
    fdlSMTPCheck.left = new FormAttachment( 0, 0 );
    fdlSMTPCheck.top = new FormAttachment( wResult, margin );
    fdlSMTPCheck.right = new FormAttachment( middle, -2 * margin );
    wlSMTPCheck.setLayoutData(fdlSMTPCheck);
    wSMTPCheck = new Button(wSettingsGroup, SWT.CHECK );
    props.setLook( wSMTPCheck );
    wSMTPCheck.setToolTipText( BaseMessages.getString( PKG, "MailValidatorDialog.SMTPCheck.Tooltip" ) );
    FormData fdSMTPCheck = new FormData();
    fdSMTPCheck.left = new FormAttachment( middle, -margin );
    fdSMTPCheck.top = new FormAttachment( wlSMTPCheck, 0, SWT.CENTER );
    wSMTPCheck.setLayoutData(fdSMTPCheck);
    wSMTPCheck.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeSMTPCheck();
        input.setChanged();
      }
    } );

    // TimeOut fieldname ...
    wlTimeOut = new Label(wSettingsGroup, SWT.RIGHT );
    wlTimeOut.setText( BaseMessages.getString( PKG, "MailValidatorDialog.TimeOutField.Label" ) );
    props.setLook( wlTimeOut );
    FormData fdlTimeOut = new FormData();
    fdlTimeOut.left = new FormAttachment( 0, 0 );
    fdlTimeOut.right = new FormAttachment( middle, -2 * margin );
    fdlTimeOut.top = new FormAttachment( wSMTPCheck, margin );
    wlTimeOut.setLayoutData(fdlTimeOut);

    wTimeOut = new TextVar( variables, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTimeOut.setToolTipText( BaseMessages.getString( PKG, "MailValidatorDialog.TimeOutField.Tooltip" ) );
    props.setLook( wTimeOut );
    wTimeOut.addModifyListener( lsMod );
    FormData fdTimeOut = new FormData();
    fdTimeOut.left = new FormAttachment( middle, -margin );
    fdTimeOut.top = new FormAttachment( wSMTPCheck, margin );
    fdTimeOut.right = new FormAttachment( 100, 0 );
    wTimeOut.setLayoutData(fdTimeOut);

    // eMailSender fieldname ...
    wleMailSender = new Label(wSettingsGroup, SWT.RIGHT );
    wleMailSender.setText( BaseMessages.getString( PKG, "MailValidatorDialog.eMailSenderField.Label" ) );
    props.setLook( wleMailSender );
    FormData fdleMailSender = new FormData();
    fdleMailSender.left = new FormAttachment( 0, 0 );
    fdleMailSender.right = new FormAttachment( middle, -2 * margin );
    fdleMailSender.top = new FormAttachment( wTimeOut, margin );
    wleMailSender.setLayoutData(fdleMailSender);

    weMailSender = new TextVar( variables, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    weMailSender.setToolTipText( BaseMessages.getString( PKG, "MailValidatorDialog.eMailSenderField.Tooltip" ) );
    props.setLook( weMailSender );
    weMailSender.addModifyListener( lsMod );
    FormData fdeMailSender = new FormData();
    fdeMailSender.left = new FormAttachment( middle, -margin );
    fdeMailSender.top = new FormAttachment( wTimeOut, margin );
    fdeMailSender.right = new FormAttachment( 100, 0 );
    weMailSender.setLayoutData(fdeMailSender);

    // DefaultSMTP fieldname ...
    wlDefaultSMTP = new Label(wSettingsGroup, SWT.RIGHT );
    wlDefaultSMTP.setText( BaseMessages.getString( PKG, "MailValidatorDialog.DefaultSMTPField.Label" ) );
    props.setLook( wlDefaultSMTP );
    FormData fdlDefaultSMTP = new FormData();
    fdlDefaultSMTP.left = new FormAttachment( 0, 0 );
    fdlDefaultSMTP.right = new FormAttachment( middle, -2 * margin );
    fdlDefaultSMTP.top = new FormAttachment( weMailSender, margin );
    wlDefaultSMTP.setLayoutData(fdlDefaultSMTP);

    wDefaultSMTP = new TextVar( variables, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wDefaultSMTP.setToolTipText( BaseMessages.getString( PKG, "MailValidatorDialog.DefaultSMTPField.Tooltip" ) );
    props.setLook( wDefaultSMTP );
    wDefaultSMTP.addModifyListener( lsMod );
    FormData fdDefaultSMTP = new FormData();
    fdDefaultSMTP.left = new FormAttachment( middle, -margin );
    fdDefaultSMTP.top = new FormAttachment( weMailSender, margin );
    fdDefaultSMTP.right = new FormAttachment( 100, 0 );
    wDefaultSMTP.setLayoutData(fdDefaultSMTP);

    // dynamic SMTP server?
    wlDynamicDefaultSMTP = new Label(wSettingsGroup, SWT.RIGHT );
    wlDynamicDefaultSMTP.setText( BaseMessages.getString( PKG, "MailValidatorDialog.dynamicDefaultSMTP.Label" ) );
    props.setLook( wlDynamicDefaultSMTP );
    FormData fdlDynamicDefaultSMTP = new FormData();
    fdlDynamicDefaultSMTP.left = new FormAttachment( 0, 0 );
    fdlDynamicDefaultSMTP.top = new FormAttachment( wDefaultSMTP, margin );
    fdlDynamicDefaultSMTP.right = new FormAttachment( middle, -2 * margin );
    wlDynamicDefaultSMTP.setLayoutData(fdlDynamicDefaultSMTP);
    wDynamicDefaultSMTP = new Button(wSettingsGroup, SWT.CHECK );
    props.setLook( wDynamicDefaultSMTP );
    wDynamicDefaultSMTP.setToolTipText( BaseMessages.getString(
      PKG, "MailValidatorDialog.dynamicDefaultSMTP.Tooltip" ) );
    FormData fdDynamicDefaultSMTP = new FormData();
    fdDynamicDefaultSMTP.left = new FormAttachment( middle, -margin );
    fdDynamicDefaultSMTP.top = new FormAttachment( wlDynamicDefaultSMTP, 0, SWT.CENTER );
    wDynamicDefaultSMTP.setLayoutData(fdDynamicDefaultSMTP);
    wDynamicDefaultSMTP.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activedynamicDefaultSMTP();
        input.setChanged();
      }
    } );

    // defaultSMTPField field
    wlDefaultSMTPField = new Label(wSettingsGroup, SWT.RIGHT );
    wlDefaultSMTPField.setText( BaseMessages.getString( PKG, "MailValidatorDialog.defaultSMTPField.Label" ) );
    props.setLook( wlDefaultSMTPField );
    FormData fdldefaultSMTPField = new FormData();
    fdldefaultSMTPField.left = new FormAttachment( 0, 0 );
    fdldefaultSMTPField.right = new FormAttachment( middle, -2 * margin );
    fdldefaultSMTPField.top = new FormAttachment( wDynamicDefaultSMTP, margin );
    wlDefaultSMTPField.setLayoutData(fdldefaultSMTPField);

    wDefaultSMTPField = new CCombo(wSettingsGroup, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wDefaultSMTPField );
    wDefaultSMTPField.addModifyListener( lsMod );
    FormData fddefaultSMTPField = new FormData();
    fddefaultSMTPField.left = new FormAttachment( middle, -margin );
    fddefaultSMTPField.top = new FormAttachment( wDynamicDefaultSMTP, margin );
    fddefaultSMTPField.right = new FormAttachment( 100, -margin );
    wDefaultSMTPField.setLayoutData(fddefaultSMTPField);
    wDefaultSMTPField.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        get();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    FormData fdSettingsGroup = new FormData();
    fdSettingsGroup.left = new FormAttachment( 0, margin );
    fdSettingsGroup.top = new FormAttachment( wEmailFieldName, margin );
    fdSettingsGroup.right = new FormAttachment( 100, -margin );
    wSettingsGroup.setLayoutData(fdSettingsGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF Settings GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Result GROUP
    //

    Group wResultGroup = new Group(shell, SWT.SHADOW_NONE);
    props.setLook(wResultGroup);
    wResultGroup.setText( BaseMessages.getString( PKG, "MailValidatorDialog.ResultGroup.label" ) );

    FormLayout groupResult = new FormLayout();
    groupResult.marginWidth = 10;
    groupResult.marginHeight = 10;
    wResultGroup.setLayout( groupResult );

    // Result fieldname ...
    Label wlResult = new Label(wResultGroup, SWT.RIGHT);
    wlResult.setText( BaseMessages.getString( PKG, "MailValidatorDialog.ResultField.Label" ) );
    props.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment( 0, 0 );
    fdlResult.right = new FormAttachment( middle, -2 * margin );
    fdlResult.top = new FormAttachment(wSettingsGroup, margin * 2 );
    wlResult.setLayoutData(fdlResult);

    wResult = new TextVar( variables, wResultGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResult.setToolTipText( BaseMessages.getString( PKG, "MailValidatorDialog.ResultField.Tooltip" ) );
    props.setLook( wResult );
    wResult.addModifyListener( lsMod );
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment( middle, -margin );
    fdResult.top = new FormAttachment(wSettingsGroup, margin * 2 );
    fdResult.right = new FormAttachment( 100, 0 );
    wResult.setLayoutData(fdResult);

    // is Result as String
    Label wlResultAsString = new Label(wResultGroup, SWT.RIGHT);
    wlResultAsString.setText( BaseMessages.getString( PKG, "MailValidatorDialog.ResultAsString.Label" ) );
    props.setLook(wlResultAsString);
    FormData fdlResultAsString = new FormData();
    fdlResultAsString.left = new FormAttachment( 0, 0 );
    fdlResultAsString.top = new FormAttachment( wResult, margin );
    fdlResultAsString.right = new FormAttachment( middle, -2 * margin );
    wlResultAsString.setLayoutData(fdlResultAsString);
    wResultAsString = new Button(wResultGroup, SWT.CHECK );
    props.setLook( wResultAsString );
    wResultAsString.setToolTipText( BaseMessages.getString( PKG, "MailValidatorDialog.ResultAsString.Tooltip" ) );
    FormData fdResultAsString = new FormData();
    fdResultAsString.left = new FormAttachment( middle, -margin );
    fdResultAsString.top = new FormAttachment( wlResultAsString, 0, SWT.CENTER );
    wResultAsString.setLayoutData(fdResultAsString);
    wResultAsString.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeResultAsString();
        input.setChanged();
      }
    } );

    // ResultStringTrue fieldname ...
    wlResultStringTrue = new Label(wResultGroup, SWT.RIGHT );
    wlResultStringTrue.setText( BaseMessages.getString( PKG, "MailValidatorDialog.ResultStringTrueField.Label" ) );
    props.setLook( wlResultStringTrue );
    FormData fdlResultStringTrue = new FormData();
    fdlResultStringTrue.left = new FormAttachment( 0, 0 );
    fdlResultStringTrue.right = new FormAttachment( middle, -2 * margin );
    fdlResultStringTrue.top = new FormAttachment( wResultAsString, margin );
    wlResultStringTrue.setLayoutData(fdlResultStringTrue);

    wResultStringTrue = new TextVar( variables, wResultGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResultStringTrue.setToolTipText( BaseMessages.getString(
      PKG, "MailValidatorDialog.ResultStringTrueField.Tooltip" ) );
    props.setLook( wResultStringTrue );
    wResultStringTrue.addModifyListener( lsMod );
    FormData fdResultStringTrue = new FormData();
    fdResultStringTrue.left = new FormAttachment( middle, -margin );
    fdResultStringTrue.top = new FormAttachment( wResultAsString, margin );
    fdResultStringTrue.right = new FormAttachment( 100, 0 );
    wResultStringTrue.setLayoutData(fdResultStringTrue);

    // ResultStringFalse fieldname ...
    wlResultStringFalse = new Label(wResultGroup, SWT.RIGHT );
    wlResultStringFalse
      .setText( BaseMessages.getString( PKG, "MailValidatorDialog.ResultStringFalseField.Label" ) );
    props.setLook( wlResultStringFalse );
    FormData fdlResultStringFalse = new FormData();
    fdlResultStringFalse.left = new FormAttachment( 0, 0 );
    fdlResultStringFalse.right = new FormAttachment( middle, -2 * margin );
    fdlResultStringFalse.top = new FormAttachment( wResultStringTrue, margin );
    wlResultStringFalse.setLayoutData(fdlResultStringFalse);

    wResultStringFalse = new TextVar( variables, wResultGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wResultStringFalse.setToolTipText( BaseMessages.getString(
      PKG, "MailValidatorDialog.ResultStringFalseField.Tooltip" ) );
    props.setLook( wResultStringFalse );
    wResultStringFalse.addModifyListener( lsMod );
    FormData fdResultStringFalse = new FormData();
    fdResultStringFalse.left = new FormAttachment( middle, -margin );
    fdResultStringFalse.top = new FormAttachment( wResultStringTrue, margin );
    fdResultStringFalse.right = new FormAttachment( 100, 0 );
    wResultStringFalse.setLayoutData(fdResultStringFalse);

    // ErrorMsg fieldname ...
    Label wlErrorMsg = new Label(wResultGroup, SWT.RIGHT);
    wlErrorMsg.setText( BaseMessages.getString( PKG, "MailValidatorDialog.ErrorMsgField.Label" ) );
    props.setLook(wlErrorMsg);
    FormData fdlErrorMsg = new FormData();
    fdlErrorMsg.left = new FormAttachment( 0, 0 );
    fdlErrorMsg.right = new FormAttachment( middle, -2 * margin );
    fdlErrorMsg.top = new FormAttachment( wResultStringFalse, margin );
    wlErrorMsg.setLayoutData(fdlErrorMsg);

    wErrorMsg = new TextVar( variables, wResultGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wErrorMsg.setToolTipText( BaseMessages.getString( PKG, "MailValidatorDialog.ErrorMsgField.Tooltip" ) );
    props.setLook( wErrorMsg );
    wErrorMsg.addModifyListener( lsMod );
    FormData fdErrorMsg = new FormData();
    fdErrorMsg.left = new FormAttachment( middle, -margin );
    fdErrorMsg.top = new FormAttachment( wResultStringFalse, margin );
    fdErrorMsg.right = new FormAttachment( 100, 0 );
    wErrorMsg.setLayoutData(fdErrorMsg);

    FormData fdResultGroup = new FormData();
    fdResultGroup.left = new FormAttachment( 0, margin );
    fdResultGroup.top = new FormAttachment(wSettingsGroup, 2 * margin );
    fdResultGroup.right = new FormAttachment( 100, -margin );
    wResultGroup.setLayoutData(fdResultGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF Result GROUP
    // ///////////////////////////////////////////////////////////

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, wResultGroup);

    // Add listeners
    lsOk = e -> ok();

    lsCancel = e -> cancel();

    wOk.addListener( SWT.Selection, lsOk );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    activeSMTPCheck();
    activeResultAsString();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void activedynamicDefaultSMTP() {
    wlDefaultSMTPField.setEnabled( wSMTPCheck.getSelection() && wDynamicDefaultSMTP.getSelection() );
    wDefaultSMTPField.setEnabled( wSMTPCheck.getSelection() && wDynamicDefaultSMTP.getSelection() );
  }

  private void activeSMTPCheck() {
    wlTimeOut.setEnabled( wSMTPCheck.getSelection() );
    wTimeOut.setEnabled( wSMTPCheck.getSelection() );
    wlDefaultSMTP.setEnabled( wSMTPCheck.getSelection() );
    wDefaultSMTP.setEnabled( wSMTPCheck.getSelection() );
    wleMailSender.setEnabled( wSMTPCheck.getSelection() );
    weMailSender.setEnabled( wSMTPCheck.getSelection() );
    wDynamicDefaultSMTP.setEnabled( wSMTPCheck.getSelection() );
    wlDynamicDefaultSMTP.setEnabled( wSMTPCheck.getSelection() );
    activedynamicDefaultSMTP();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( input.getEmailField() != null ) {
      wEmailFieldName.setText( input.getEmailField() );
    }
    if ( input.getResultFieldName() != null ) {
      wResult.setText( input.getResultFieldName() );
    }

    wResultAsString.setSelection( input.isResultAsString() );
    if ( input.getEMailValideMsg() != null ) {
      wResultStringTrue.setText( input.getEMailValideMsg() );
    }
    if ( input.getEMailNotValideMsg() != null ) {
      wResultStringFalse.setText( input.getEMailNotValideMsg() );
    }
    if ( input.getErrorsField() != null ) {
      wErrorMsg.setText( input.getErrorsField() );
    }
    int timeout = Const.toInt( input.getTimeOut(), 0 );
    wTimeOut.setText( String.valueOf( timeout ) );
    wSMTPCheck.setSelection( input.isSMTPCheck() );
    if ( input.getDefaultSMTP() != null ) {
      wDefaultSMTP.setText( input.getDefaultSMTP() );
    }
    if ( input.geteMailSender() != null ) {
      weMailSender.setText( input.geteMailSender() );
    }
    wDynamicDefaultSMTP.setSelection( input.isdynamicDefaultSMTP() );
    if ( input.getDefaultSMTPField() != null ) {
      wDefaultSMTPField.setText( input.getDefaultSMTPField() );
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void activeResultAsString() {
    wlResultStringFalse.setEnabled( wResultAsString.getSelection() );
    wResultStringFalse.setEnabled( wResultAsString.getSelection() );
    wlResultStringTrue.setEnabled( wResultAsString.getSelection() );
    wResultStringTrue.setEnabled( wResultAsString.getSelection() );
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
    input.setEmailfield( wEmailFieldName.getText() );
    input.setResultFieldName( wResult.getText() );
    transformName = wTransformName.getText(); // return value

    input.setResultAsString( wResultAsString.getSelection() );
    input.setEmailValideMsg( wResultStringTrue.getText() );
    input.setEmailNotValideMsg( wResultStringFalse.getText() );
    input.setErrorsField( wErrorMsg.getText() );
    input.setTimeOut( wTimeOut.getText() );
    input.setDefaultSMTP( wDefaultSMTP.getText() );
    input.seteMailSender( weMailSender.getText() );
    input.setSMTPCheck( wSMTPCheck.getSelection() );
    input.setdynamicDefaultSMTP( wDynamicDefaultSMTP.getSelection() );
    input.setDefaultSMTPField( wDefaultSMTPField.getText() );

    dispose();
  }

  private void get() {
    if ( !gotPreviousFields ) {
      try {
        String emailField = null;
        String smtpdefaultField = null;
        if ( wEmailFieldName.getText() != null ) {
          emailField = wEmailFieldName.getText();
        }
        if ( wDefaultSMTPField.getText() != null ) {
          smtpdefaultField = wDefaultSMTPField.getText();
        }

        wEmailFieldName.removeAll();
        wDefaultSMTPField.removeAll();

        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          wEmailFieldName.setItems( r.getFieldNames() );
          wDefaultSMTPField.setItems( r.getFieldNames() );
        }
        if ( emailField != null ) {
          wEmailFieldName.setText( emailField );
        }
        if ( smtpdefaultField != null ) {
          wDefaultSMTPField.setText( smtpdefaultField );
        }
        gotPreviousFields = true;
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "MailValidatorDialog.FailedToGetFields.DialogTitle" ),
          BaseMessages.getString( PKG, "MailValidatorDialog.FailedToGetFields.DialogMessage" ), ke );
      }
    }
  }
}
