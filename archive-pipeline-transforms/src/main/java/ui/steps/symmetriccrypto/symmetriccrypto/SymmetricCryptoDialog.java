/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.pipeline.transforms.symmetriccrypto.symmetriccrypto;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transforms.symmetriccrypto.symmetricalgorithm.SymmetricCryptoMeta;
import org.apache.hop.pipeline.transforms.symmetriccrypto.symmetriccrypto.SymmetricCryptoPipelineMeta;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class SymmetricCryptoDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = SymmetricCryptoPipelineMeta.class; // for i18n purposes, needed by Translator!!

  private Group wCryptoSettings;
  private FormData fdCryptoSettings;

  private Group wMessageGroup;
  private FormData fdMessageGroup;

  private Label wlReadKeyAsBinary;
  private Button wReadKeyAsBinary;
  private FormData fdlReadKeyAsBinary, fdReadKeyAsBinary;

  private LabelTextVar wResultField;
  private CCombo wMessage, wSecretKeyField;
  private FormData fdlSecretKeyInField, fdSecretKeyInField;

  private Label wlMessage, wlSecretKey, wlSecretKeyField, wlSecretKeyInField;

  private Button wSecretKeyInField;

  private SymmetricCryptoPipelineMeta input;

  private Group wOutputField;
  private FormData fdOutputField;

  private TextVar wSecretKey;

  private CTabFolder wTabFolder;

  private CTabItem wGeneralTab;
  private Composite wGeneralComp;
  private FormData fdGeneralComp;

  private Label wlOperation;
  private CCombo wOperation;
  private FormData fdlOperation;
  private FormData fdOperation;

  private Label wlAlgorithm;
  private CCombo wAlgorithm;
  private FormData fdlAlgorithm;
  private FormData fdAlgorithm;

  private Label wlScheme;
  private FormData fdlScheme;
  private TextVar wScheme;
  private FormData fdScheme;

  private Label wlOutputAsBinary;
  private Button wOutputAsBinary;
  private FormData fdlOutputAsBinary, fdOutputAsBinary;

  public SymmetricCryptoDialog( Shell parent, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (SymmetricCryptoPipelineMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "SymmetricCryptoDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // SecretKey line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "SymmetricCryptoDialog.TransformName.Label" ) );
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

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, PropsUI.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
    wGeneralTab.setText( BaseMessages.getString( PKG, "SymmetricCryptoDialog.GeneralTab.TabTitle" ) );

    wGeneralComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wGeneralComp );

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // ////////////////////////
    // START OF Crypto settings GROUP
    //

    wCryptoSettings = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wCryptoSettings );
    wCryptoSettings
      .setText( BaseMessages.getString( PKG, "SymmetricCryptoDialog.CryptoSettings.Group.Label" ) );

    FormLayout CryptoSettingsgroupLayout = new FormLayout();
    CryptoSettingsgroupLayout.marginWidth = 10;
    CryptoSettingsgroupLayout.marginHeight = 10;
    wCryptoSettings.setLayout( CryptoSettingsgroupLayout );

    // Operation
    wlOperation = new Label( wCryptoSettings, SWT.RIGHT );
    wlOperation.setText( BaseMessages.getString( PKG, "SymmetricCryptoDialog.Operation.Label" ) );
    props.setLook( wlOperation );
    fdlOperation = new FormData();
    fdlOperation.left = new FormAttachment( 0, 0 );
    fdlOperation.right = new FormAttachment( middle, -margin );
    fdlOperation.top = new FormAttachment( wTransformName, margin );
    wlOperation.setLayoutData( fdlOperation );

    wOperation = new CCombo( wCryptoSettings, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wOperation );
    wOperation.addModifyListener( lsMod );
    fdOperation = new FormData();
    fdOperation.left = new FormAttachment( middle, margin );
    fdOperation.top = new FormAttachment( wTransformName, margin );
    fdOperation.right = new FormAttachment( 100, -margin );
    wOperation.setLayoutData( fdOperation );
    wOperation.setItems( SymmetricCryptoPipelineMeta.operationTypeDesc );
    wOperation.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();

      }
    } );

    // Algorithm
    wlAlgorithm = new Label( wCryptoSettings, SWT.RIGHT );
    wlAlgorithm.setText( BaseMessages.getString( PKG, "SymmetricCryptoDialog.Algorithm.Label" ) );
    props.setLook( wlAlgorithm );
    fdlAlgorithm = new FormData();
    fdlAlgorithm.left = new FormAttachment( 0, 0 );
    fdlAlgorithm.right = new FormAttachment( middle, -margin );
    fdlAlgorithm.top = new FormAttachment( wOperation, margin );
    wlAlgorithm.setLayoutData( fdlAlgorithm );

    wAlgorithm = new CCombo( wCryptoSettings, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wAlgorithm );
    wAlgorithm.addModifyListener( lsMod );
    fdAlgorithm = new FormData();
    fdAlgorithm.left = new FormAttachment( middle, margin );
    fdAlgorithm.top = new FormAttachment( wOperation, margin );
    fdAlgorithm.right = new FormAttachment( 100, -margin );
    wAlgorithm.setLayoutData( fdAlgorithm );
    wAlgorithm.setItems( SymmetricCryptoMeta.TYPE_ALGORYTHM_CODE );
    wAlgorithm.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();

      }
    } );

    // Scheme
    wlScheme = new Label( wCryptoSettings, SWT.RIGHT );
    wlScheme.setText( BaseMessages.getString( PKG, "SymmetricCryptoDialog.Scheme.Label" ) );
    props.setLook( wlScheme );
    fdlScheme = new FormData();
    fdlScheme.left = new FormAttachment( 0, 0 );
    fdlScheme.top = new FormAttachment( wAlgorithm, margin );
    fdlScheme.right = new FormAttachment( middle, -margin );
    wlScheme.setLayoutData( fdlScheme );

    wScheme = new TextVar( pipelineMeta, wCryptoSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wScheme );
    wScheme.addModifyListener( lsMod );
    fdScheme = new FormData();
    fdScheme.left = new FormAttachment( middle, margin );
    fdScheme.right = new FormAttachment( 100, -margin );
    fdScheme.top = new FormAttachment( wAlgorithm, margin );
    wScheme.setLayoutData( fdScheme );

    // SecretKey
    wlSecretKey = new Label( wCryptoSettings, SWT.RIGHT );
    wlSecretKey.setText( BaseMessages.getString( PKG, "SymmetricCryptoDialog.SecretKey.Label" ) );
    props.setLook( wlSecretKey );
    FormData fdlSecretKey = new FormData();
    fdlSecretKey.left = new FormAttachment( 0, 0 );
    fdlSecretKey.top = new FormAttachment( wScheme, 2 * margin );
    fdlSecretKey.right = new FormAttachment( middle, -margin );
    wlSecretKey.setLayoutData( fdlSecretKey );

    wSecretKey = new PasswordTextVar( pipelineMeta, wCryptoSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER,
      BaseMessages.getString( PKG, "SymmetricCryptoDialog.SecretKey.Tooltip" ) );
    props.setLook( wSecretKey );
    wSecretKey.addModifyListener( lsMod );
    FormData fdSecretKey = new FormData();
    fdSecretKey.left = new FormAttachment( middle, margin );
    fdSecretKey.right = new FormAttachment( 100, -margin );
    fdSecretKey.top = new FormAttachment( wScheme, 2 * margin );
    wSecretKey.setLayoutData( fdSecretKey );

    // Is secret key extracted from a field?
    wlSecretKeyInField = new Label( wCryptoSettings, SWT.RIGHT );
    wlSecretKeyInField.setText( BaseMessages
      .getString( PKG, "SymmetricCryptoDialog.SecretKeyFileField.Label" ) );
    props.setLook( wlSecretKeyInField );
    fdlSecretKeyInField = new FormData();
    fdlSecretKeyInField.left = new FormAttachment( 0, 0 );
    fdlSecretKeyInField.top = new FormAttachment( wSecretKey, margin );
    fdlSecretKeyInField.right = new FormAttachment( middle, -margin );
    wlSecretKeyInField.setLayoutData( fdlSecretKeyInField );
    wSecretKeyInField = new Button( wCryptoSettings, SWT.CHECK );
    props.setLook( wSecretKeyInField );
    wSecretKeyInField.setToolTipText( BaseMessages.getString(
      PKG, "SymmetricCryptoDialog.SecretKeyFileField.Tooltip" ) );
    fdSecretKeyInField = new FormData();
    fdSecretKeyInField.left = new FormAttachment( middle, margin );
    fdSecretKeyInField.top = new FormAttachment( wSecretKey, margin );
    wSecretKeyInField.setLayoutData( fdSecretKeyInField );

    SelectionAdapter lsXslFile = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        ActivewlSecretKeyField();
        input.setChanged();
      }
    };
    wSecretKeyInField.addSelectionListener( lsXslFile );

    // If secret key defined in a Field
    wlSecretKeyField = new Label( wCryptoSettings, SWT.RIGHT );
    wlSecretKeyField.setText( BaseMessages.getString( PKG, "SymmetricCryptoDialog.SecretKeyField.Label" ) );
    props.setLook( wlSecretKeyField );
    FormData fdlSecretKeyField = new FormData();
    fdlSecretKeyField.left = new FormAttachment( 0, 0 );
    fdlSecretKeyField.top = new FormAttachment( wSecretKeyInField, margin );
    fdlSecretKeyField.right = new FormAttachment( middle, -margin );
    wlSecretKeyField.setLayoutData( fdlSecretKeyField );
    wSecretKeyField = new CCombo( wCryptoSettings, SWT.BORDER | SWT.READ_ONLY );
    wSecretKeyField.setEditable( true );
    props.setLook( wSecretKeyField );
    wSecretKeyField.addModifyListener( lsMod );
    FormData fdSecretKeyField = new FormData();
    fdSecretKeyField.left = new FormAttachment( middle, margin );
    fdSecretKeyField.top = new FormAttachment( wSecretKeyInField, margin );
    fdSecretKeyField.right = new FormAttachment( 100, -margin );
    wSecretKeyField.setLayoutData( fdSecretKeyField );
    wSecretKeyField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        setSecretKeyFieldname();
      }
    } );

    wlReadKeyAsBinary = new Label( wCryptoSettings, SWT.RIGHT );
    wlReadKeyAsBinary.setText( BaseMessages.getString( PKG, "SymmetricCryptoDialog.ReadKeyAsBinary.Label" ) );
    props.setLook( wlReadKeyAsBinary );
    fdlReadKeyAsBinary = new FormData();
    fdlReadKeyAsBinary.left = new FormAttachment( 0, 0 );
    fdlReadKeyAsBinary.top = new FormAttachment( wSecretKeyField, margin );
    fdlReadKeyAsBinary.right = new FormAttachment( middle, -margin );
    wlReadKeyAsBinary.setLayoutData( fdlReadKeyAsBinary );
    wReadKeyAsBinary = new Button( wCryptoSettings, SWT.CHECK );
    wReadKeyAsBinary.setToolTipText( BaseMessages.getString(
      PKG, "SymmetricCryptoDialog.ReadKeyAsBinary.Tooltip" ) );
    props.setLook( wReadKeyAsBinary );
    fdReadKeyAsBinary = new FormData();
    fdReadKeyAsBinary.left = new FormAttachment( middle, margin );
    fdReadKeyAsBinary.top = new FormAttachment( wSecretKeyField, margin );
    fdReadKeyAsBinary.right = new FormAttachment( 100, -margin );
    wReadKeyAsBinary.setLayoutData( fdReadKeyAsBinary );
    wReadKeyAsBinary.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    fdCryptoSettings = new FormData();
    fdCryptoSettings.left = new FormAttachment( 0, margin );
    fdCryptoSettings.top = new FormAttachment( wTransformName, margin );
    fdCryptoSettings.right = new FormAttachment( 100, -margin );
    wCryptoSettings.setLayoutData( fdCryptoSettings );

    // ///////////////////////////////////////////////////////////
    // / END OF Crypto settings GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF Crypto settings GROUP
    //

    wMessageGroup = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wMessageGroup );
    wMessageGroup.setText( BaseMessages.getString( PKG, "SymmetricCryptoDialog.Message.Group.Label" ) );

    FormLayout MessageGroupgroupLayout = new FormLayout();
    MessageGroupgroupLayout.marginWidth = 10;
    MessageGroupgroupLayout.marginHeight = 10;
    wMessageGroup.setLayout( MessageGroupgroupLayout );

    // FieldName to evaluate
    wlMessage = new Label( wMessageGroup, SWT.RIGHT );
    wlMessage.setText( BaseMessages.getString( PKG, "SymmetricCryptoDialog.Field.Label" ) );
    props.setLook( wlMessage );
    FormData fdlMessage = new FormData();
    fdlMessage.left = new FormAttachment( 0, 0 );
    fdlMessage.top = new FormAttachment( wCryptoSettings, margin );
    fdlMessage.right = new FormAttachment( middle, -margin );
    wlMessage.setLayoutData( fdlMessage );
    wMessage = new CCombo( wMessageGroup, SWT.BORDER | SWT.READ_ONLY );
    wMessage.setEditable( true );
    props.setLook( wMessage );
    wMessage.addModifyListener( lsMod );
    FormData fdField = new FormData();
    fdField.left = new FormAttachment( middle, margin );
    fdField.top = new FormAttachment( wCryptoSettings, margin );
    fdField.right = new FormAttachment( 100, -margin );
    wMessage.setLayoutData( fdField );
    wMessage.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        setFieldname();
      }
    } );

    fdMessageGroup = new FormData();
    fdMessageGroup.left = new FormAttachment( 0, margin );
    fdMessageGroup.top = new FormAttachment( wCryptoSettings, margin );
    fdMessageGroup.right = new FormAttachment( 100, -margin );
    wMessageGroup.setLayoutData( fdMessageGroup );

    // ///////////////////////////////////////////////////////////
    // / END OF Crypto settings GROUP
    // ///////////////////////////////////////////////////////////

    // Transform Output field grouping?
    // ////////////////////////
    // START OF Output Field GROUP
    //

    wOutputField = new Group( wGeneralComp, SWT.SHADOW_NONE );
    props.setLook( wOutputField );
    wOutputField.setText( BaseMessages.getString( PKG, "SymmetricCryptoDialog.ResultField.Group.Label" ) );

    FormLayout outputfieldgroupLayout = new FormLayout();
    outputfieldgroupLayout.marginWidth = 10;
    outputfieldgroupLayout.marginHeight = 10;
    wOutputField.setLayout( outputfieldgroupLayout );

    // Output Fieldame
    wResultField = new LabelTextVar( pipelineMeta, wOutputField,
      BaseMessages.getString( PKG, "SymmetricCryptoDialog.ResultField.Label" ),
      BaseMessages.getString( PKG, "SymmetricCryptoDialog.ResultField.Tooltip" ) );
    props.setLook( wResultField );
    wResultField.addModifyListener( lsMod );
    FormData fdResultField = new FormData();
    fdResultField.left = new FormAttachment( 0, 0 );
    fdResultField.top = new FormAttachment( wMessageGroup, margin );
    fdResultField.right = new FormAttachment( 100, 0 );
    wResultField.setLayoutData( fdResultField );

    wlOutputAsBinary = new Label( wOutputField, SWT.RIGHT );
    wlOutputAsBinary.setText( BaseMessages.getString( PKG, "SymmetricCryptoDialog.OutputAsBinary.Label" ) );
    props.setLook( wlOutputAsBinary );
    fdlOutputAsBinary = new FormData();
    fdlOutputAsBinary.left = new FormAttachment( 0, 0 );
    fdlOutputAsBinary.top = new FormAttachment( wResultField, margin );
    fdlOutputAsBinary.right = new FormAttachment( middle, -margin );
    wlOutputAsBinary.setLayoutData( fdlOutputAsBinary );
    wOutputAsBinary = new Button( wOutputField, SWT.CHECK );
    wOutputAsBinary.setToolTipText( BaseMessages.getString(
      PKG, "SymmetricCryptoDialog.OutputAsBinary.Tooltip" ) );
    props.setLook( wOutputAsBinary );
    fdOutputAsBinary = new FormData();
    fdOutputAsBinary.left = new FormAttachment( middle, margin );
    fdOutputAsBinary.top = new FormAttachment( wResultField, margin );
    fdOutputAsBinary.right = new FormAttachment( 100, 0 );
    wOutputAsBinary.setLayoutData( fdOutputAsBinary );
    wOutputAsBinary.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment( 0, margin );
    fdOutputField.top = new FormAttachment( wMessageGroup, margin );
    fdOutputField.right = new FormAttachment( 100, -margin );
    wOutputField.setLayoutData( fdOutputField );

    // ///////////////////////////////////////////////////////////
    // / END OF Output Field GROUP
    // ///////////////////////////////////////////////////////////

    fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( wOutputField, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData( fdGeneralComp );

    wGeneralComp.layout();
    wGeneralTab.setControl( wGeneralComp );
    props.setLook( wGeneralComp );

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fdTabFolder );

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, wTabFolder );

    // Add listeners
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };

    wCancel.addListener( SWT.Selection, lsCancel );

    wOk.addListener( SWT.Selection, lsOk );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    ActivewlSecretKeyField();

    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void ActivewlSecretKeyField() {

    wSecretKeyField.setEnabled( wSecretKeyInField.getSelection() );
    wlSecretKeyField.setEnabled( wSecretKeyInField.getSelection() );
    wReadKeyAsBinary.setEnabled( wSecretKeyInField.getSelection() );
    wlReadKeyAsBinary.setEnabled( wSecretKeyInField.getSelection() );
    wSecretKey.setEnabled( !wSecretKeyInField.getSelection() );
    wlSecretKey.setEnabled( !wSecretKeyInField.getSelection() );

  }

  private void setSecretKeyFieldname() {
    try {
      String field = wSecretKeyField.getText();
      wSecretKeyField.removeAll();

      IRowMeta r = pipelineMeta.getPrevTransformFields( transformName );
      if ( r != null ) {
        wSecretKeyField.setItems( r.getFieldNames() );
      }
      if ( field != null ) {
        wSecretKeyField.setText( field );
      }

    } catch ( HopException ke ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "SymmetricCryptoDialog.FailedToGetFields.DialogTitle" ),
        BaseMessages.getString( PKG, "SymmetricCryptoDialogMod.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  private void setFieldname() {
    try {
      String field = wMessage.getText();
      wMessage.removeAll();

      IRowMeta r = pipelineMeta.getPrevTransformFields( transformName );
      wMessage.setItems( r.getFieldNames() );
      if ( field != null ) {
        wMessage.setText( field );
      }

    } catch ( HopException ke ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "SymmetricCryptoDialog.FailedToGetFields.DialogTitle" ),
        BaseMessages.getString( PKG, "SymmetricCryptoDialogMod.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wOperation.setText( SymmetricCryptoPipelineMeta.getOperationTypeDesc( input.getOperationType() ) );
    wAlgorithm.setText( Const.NVL( input.getAlgorithm(), SymmetricCryptoMeta.TYPE_ALGORYTHM_CODE[ 0 ] ) );
    if ( input.getMessageField() != null ) {
      wMessage.setText( input.getMessageField() );
    }
    if ( input.getResultfieldname() != null ) {
      wResultField.setText( input.getResultfieldname() );
    }
    if ( input.getSecretKey() != null ) {
      wSecretKey.setText( input.getSecretKey() );
    }
    if ( input.getSchema() != null ) {
      wScheme.setText( input.getSchema() );
    }

    wSecretKeyInField.setSelection( input.isSecretKeyInField() );
    wReadKeyAsBinary.setSelection( input.isReadKeyAsBinary() );
    if ( input.getSecretKeyField() != null ) {
      wSecretKeyField.setText( input.getSecretKeyField() );
    }
    wOutputAsBinary.setSelection( input.isOutputResultAsBinary() );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    transformName = wTransformName.getText(); // return value
    input.setOperationType( SymmetricCryptoPipelineMeta.getOperationTypeByDesc( wOperation.getText() ) );
    input.setAlgorithm( wAlgorithm.getText() );
    input.setMessageField( wMessage.getText() );
    input.setSchema( wScheme.getText() );
    input.setSecretKey( wSecretKey.getText() );
    input.setSecretKeyInField( wSecretKeyInField.getSelection() );
    input.setReadKeyAsBinary( wReadKeyAsBinary.getSelection() );
    input.setsecretKeyField( wSecretKeyField.getText() );
    input.setOutputResultAsBinary( wOutputAsBinary.getSelection() );
    input.setResultfieldname( wResultField.getText() );

    dispose();
  }

  public String toString() {
    return this.getClass().getName();
  }
}
