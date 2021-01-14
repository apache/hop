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

package org.apache.hop.pipeline.transforms.xml.xsdvalidator;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.LabelTextVar;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class XsdValidatorDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = XsdValidatorMeta.class; // For Translator

  private LabelTextVar wResultField, wValidationMsg, wIfXMLValid, wIfXMLUnValid;

  private CCombo wXMLStream, wXSDSource, wXSDDefinedColumn;

  private Label wlFilename;
  private Label wlXSDDefinedColumn;

  private Button wbbFilename, wAddValidationMsg, wOutputStringField, wXMLSourceFile;

  private final XsdValidatorMeta input;

  private TextVar wFilename;

  private Button wAllowExternalEntities;

  private boolean gotPrevious = false;

  public XsdValidatorDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (XsdValidatorMeta) in;
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
    shell.setText( BaseMessages.getString( PKG, "XsdValidatorDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Buttons at the bottom
    //
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null);

    // Filename line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "XsdValidatorDialog.TransformName.Label" ) );
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

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "XsdValidatorDialog.GeneralTab.TabTitle" ) );

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout( generalLayout );

    // ////////////////////////
    // START OF XML GROUP
    //

    Group wXML = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wXML);
    wXML.setText( "XML source" );

    FormLayout groupXML = new FormLayout();
    groupXML.marginWidth = 10;
    groupXML.marginHeight = 10;
    wXML.setLayout( groupXML );

    // XML Source = file ?
    Label wlXMLSourceFile = new Label(wXML, SWT.RIGHT);
    wlXMLSourceFile.setText( BaseMessages.getString( PKG, "XsdValidatorDialog.XMLSourceFile.Label" ) );
    props.setLook(wlXMLSourceFile);
    FormData fdlXMLSourceFile = new FormData();
    fdlXMLSourceFile.left = new FormAttachment( 0, 0 );
    fdlXMLSourceFile.top = new FormAttachment( wTransformName, 2 * margin );
    fdlXMLSourceFile.right = new FormAttachment( middle, -margin );
    wlXMLSourceFile.setLayoutData(fdlXMLSourceFile);
    wXMLSourceFile = new Button(wXML, SWT.CHECK );
    props.setLook( wXMLSourceFile );
    wXMLSourceFile.setToolTipText( BaseMessages.getString( PKG, "XsdValidatorDialog.XMLSourceFile.Tooltip" ) );
    FormData fdXMLSourceFile = new FormData();
    fdXMLSourceFile.left = new FormAttachment( middle, margin );
    fdXMLSourceFile.top = new FormAttachment( wlXMLSourceFile, 0, SWT.CENTER );
    wXMLSourceFile.setLayoutData(fdXMLSourceFile);
    wXMLSourceFile.addSelectionListener( new ComponentSelectionListener( input ) );

    // XML Stream Field
    Label wlXMLStream = new Label(wXML, SWT.RIGHT);
    wlXMLStream.setText( BaseMessages.getString( PKG, "XsdValidatorDialog.XMLStream.Label" ) );
    props.setLook(wlXMLStream);
    FormData fdlXMLStream = new FormData();
    fdlXMLStream.left = new FormAttachment( 0, 0 );
    fdlXMLStream.top = new FormAttachment( wXMLSourceFile, margin );
    fdlXMLStream.right = new FormAttachment( middle, -margin );
    wlXMLStream.setLayoutData(fdlXMLStream);
    wXMLStream = new CCombo(wXML, SWT.BORDER | SWT.READ_ONLY );
    wXMLStream.setEditable( true );
    props.setLook( wXMLStream );
    wXMLStream.addModifyListener( lsMod );
    FormData fdXMLStream = new FormData();
    fdXMLStream.left = new FormAttachment( middle, margin );
    fdXMLStream.top = new FormAttachment( wXMLSourceFile, margin );
    fdXMLStream.right = new FormAttachment( 100, -margin );
    wXMLStream.setLayoutData(fdXMLStream);
    wXMLStream.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        PopulateFields();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    FormData fdXML = new FormData();
    fdXML.left = new FormAttachment( 0, margin );
    fdXML.top = new FormAttachment( wTransformName, margin );
    fdXML.right = new FormAttachment( 100, -margin );
    wXML.setLayoutData(fdXML);

    // ///////////////////////////////////////////////////////////
    // / END OF XML GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF OutputFields GROUP
    //

    Group wOutputFields = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wOutputFields);
    wOutputFields.setText( "Output Fields" );

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wOutputFields.setLayout( groupLayout );

    // Output Fieldame
    wResultField =
        new LabelTextVar( variables, wOutputFields, BaseMessages
            .getString( PKG, "XsdValidatorDialog.ResultField.Label" ), BaseMessages.getString( PKG,
            "XsdValidatorDialog.ResultField.Tooltip" ) );
    props.setLook( wResultField );
    wResultField.addModifyListener( lsMod );
    FormData fdResultField = new FormData();
    fdResultField.left = new FormAttachment( 0, 0 );
    fdResultField.top = new FormAttachment(wXML, margin );
    fdResultField.right = new FormAttachment( 100, 0 );
    wResultField.setLayoutData(fdResultField);

    // Output String Field ?
    Label wlOutputStringField = new Label(wOutputFields, SWT.RIGHT);
    wlOutputStringField.setText( BaseMessages.getString( PKG, "XsdValidatorDialog.OutputStringField.Label" ) );
    props.setLook(wlOutputStringField);
    FormData fdlOutputStringField = new FormData();
    fdlOutputStringField.left = new FormAttachment( 0, 0 );
    fdlOutputStringField.top = new FormAttachment( wResultField, 2 * margin );
    fdlOutputStringField.right = new FormAttachment( middle, -margin );
    wlOutputStringField.setLayoutData(fdlOutputStringField);
    wOutputStringField = new Button(wOutputFields, SWT.CHECK );
    props.setLook( wOutputStringField );
    wOutputStringField.setToolTipText( BaseMessages.getString( PKG, "XsdValidatorDialog.OutputStringField.Tooltip" ) );
    FormData fdOutputStringField = new FormData();
    fdOutputStringField.left = new FormAttachment( middle, margin );
    fdOutputStringField.top = new FormAttachment( wlOutputStringField, 0, SWT.CENTER );
    wOutputStringField.setLayoutData(fdOutputStringField);
    wOutputStringField.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeOutputStringField();
        input.setChanged();
      }
    } );

    // Output if XML is valid field
    wIfXMLValid =
        new LabelTextVar( variables, wOutputFields,
            BaseMessages.getString( PKG, "XsdValidatorDialog.IfXMLValid.Label" ), BaseMessages.getString( PKG,
                "XsdValidatorDialog.IfXMLValid.Tooltip" ) );
    props.setLook( wIfXMLValid );
    wIfXMLValid.addModifyListener( lsMod );
    FormData fdIfXMLValid = new FormData();
    fdIfXMLValid.left = new FormAttachment( 0, 0 );
    fdIfXMLValid.top = new FormAttachment( wOutputStringField, margin );
    fdIfXMLValid.right = new FormAttachment( 100, 0 );
    wIfXMLValid.setLayoutData(fdIfXMLValid);

    // Output if XML is not valid field
    wIfXMLUnValid =
        new LabelTextVar( variables, wOutputFields, BaseMessages.getString( PKG,
            "XsdValidatorDialog.IfXMLUnValid.Label" ), BaseMessages.getString( PKG,
            "XsdValidatorDialog.IfXMLUnValid.Tooltip" ) );
    props.setLook( wIfXMLUnValid );
    wIfXMLUnValid.addModifyListener( lsMod );
    FormData fdIfXMLUnValid = new FormData();
    fdIfXMLUnValid.left = new FormAttachment( 0, 0 );
    fdIfXMLUnValid.top = new FormAttachment( wIfXMLValid, margin );
    fdIfXMLUnValid.right = new FormAttachment( 100, 0 );
    wIfXMLUnValid.setLayoutData(fdIfXMLUnValid);

    // Add validation message ?
    Label wlAddValidationMsg = new Label(wOutputFields, SWT.RIGHT);
    wlAddValidationMsg.setText( BaseMessages.getString( PKG, "XsdValidatorDialog.AddValidationMsg.Label" ) );
    props.setLook(wlAddValidationMsg);
    FormData fdlAddValidationMsg = new FormData();
    fdlAddValidationMsg.left = new FormAttachment( 0, 0 );
    fdlAddValidationMsg.top = new FormAttachment( wIfXMLUnValid, 2 * margin );
    fdlAddValidationMsg.right = new FormAttachment( middle, -margin );
    wlAddValidationMsg.setLayoutData(fdlAddValidationMsg);
    wAddValidationMsg = new Button(wOutputFields, SWT.CHECK );
    props.setLook( wAddValidationMsg );
    wAddValidationMsg.setToolTipText( BaseMessages.getString( PKG, "XsdValidatorDialog.AddValidationMsg.Tooltip" ) );
    FormData fdAddValidationMsg = new FormData();
    fdAddValidationMsg.left = new FormAttachment( middle, margin );
    fdAddValidationMsg.top = new FormAttachment( wlAddValidationMsg, 0, SWT.CENTER );
    wAddValidationMsg.setLayoutData(fdAddValidationMsg);
    wAddValidationMsg.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeValidationMsg();
        input.setChanged();
      }
    } );

    // Validation Msg Fieldame
    wValidationMsg =
        new LabelTextVar( variables, wOutputFields, BaseMessages.getString( PKG,
            "XsdValidatorDialog.ValidationMsg.Label" ), BaseMessages.getString( PKG,
            "XsdValidatorDialog.ValidationMsg.Tooltip" ) );
    props.setLook( wValidationMsg );
    wValidationMsg.addModifyListener( lsMod );
    FormData fdValidationMsg = new FormData();
    fdValidationMsg.left = new FormAttachment( 0, 0 );
    fdValidationMsg.top = new FormAttachment( wAddValidationMsg, margin );
    fdValidationMsg.right = new FormAttachment( 100, 0 );
    wValidationMsg.setLayoutData(fdValidationMsg);

    FormData fdOutputFields = new FormData();
    fdOutputFields.left = new FormAttachment( 0, margin );
    fdOutputFields.top = new FormAttachment(wXML, margin );
    fdOutputFields.right = new FormAttachment( 100, -margin );
    wOutputFields.setLayoutData(fdOutputFields);

    // ///////////////////////////////////////////////////////////
    // / END OF OUTPUT FIELDS GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF XSD GROUP
    //

    Group wXSD = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wXSD);
    wXSD.setText( "XML Schema Definition" );

    FormLayout groupXSD = new FormLayout();
    groupXSD.marginWidth = 10;
    groupXSD.marginHeight = 10;
    wXSD.setLayout( groupLayout );

    // Enable/Disable external entity for XSD validation.
    Label wlAllowExternalEntities = new Label(wXSD, SWT.RIGHT);
    wlAllowExternalEntities.setText( BaseMessages.getString( PKG, "XsdValidatorDialog.AllowExternalEntities.Label" ) );
    props.setLook(wlAllowExternalEntities);
    FormData fdlAllowExternalEntities = new FormData();
    fdlAllowExternalEntities.left = new FormAttachment( 0, 0 );
    fdlAllowExternalEntities.right = new FormAttachment( middle, -margin );
    fdlAllowExternalEntities.top = new FormAttachment( wTransformName, margin );
    wlAllowExternalEntities.setLayoutData(fdlAllowExternalEntities);
    wAllowExternalEntities = new Button(wXSD, SWT.CHECK );
    props.setLook( wAllowExternalEntities );
    FormData fdAllowExternalEntities = new FormData();
    fdAllowExternalEntities.left = new FormAttachment( middle, margin );
    fdAllowExternalEntities.top = new FormAttachment( wlAllowExternalEntities, 0, SWT.CENTER );
    fdAllowExternalEntities.right = new FormAttachment( 100, -margin );
    wAllowExternalEntities.setLayoutData(fdAllowExternalEntities);
    wAllowExternalEntities.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // XSD Source?
    Label wlXSDSource = new Label(wXSD, SWT.RIGHT);
    wlXSDSource.setText( BaseMessages.getString( PKG, "XsdValidatorDialog.XSDSource.Label" ) );
    props.setLook(wlXSDSource);
    FormData fdlXSDSource = new FormData();
    fdlXSDSource.left = new FormAttachment( 0, 0 );
    fdlXSDSource.top = new FormAttachment( wAllowExternalEntities, margin );
    fdlXSDSource.right = new FormAttachment( middle, -margin );
    wlXSDSource.setLayoutData(fdlXSDSource);
    wXSDSource = new CCombo(wXSD, SWT.BORDER | SWT.READ_ONLY );
    wXSDSource.setEditable( true );
    props.setLook( wXSDSource );
    wXSDSource.addModifyListener( lsMod );
    FormData fdXSDSource = new FormData();
    fdXSDSource.left = new FormAttachment( middle, margin );
    fdXSDSource.top = new FormAttachment( wAllowExternalEntities, margin );
    fdXSDSource.right = new FormAttachment( 100, -margin );
    wXSDSource.setLayoutData(fdXSDSource);
    wXSDSource.add( BaseMessages.getString( PKG, "XsdValidatorDialog.XSDSource.IS_A_FILE" ) );
    wXSDSource.add( BaseMessages.getString( PKG, "XsdValidatorDialog.XSDSource.IS_A_FIELD" ) );
    wXSDSource.add( BaseMessages.getString( PKG, "XsdValidatorDialog.XSDSource.NO_NEED" ) );
    wXSDSource.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        setXSDSource();
      }
    } );

    // XSD Filename
    wlFilename = new Label(wXSD, SWT.RIGHT );
    wlFilename.setText( BaseMessages.getString( PKG, "XsdValidatorDialog.XSDFilename.Label" ) );
    props.setLook( wlFilename );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wXSDSource, margin );
    fdlFilename.right = new FormAttachment( middle, -margin );
    wlFilename.setLayoutData(fdlFilename);

    wbbFilename = new Button(wXSD, SWT.PUSH | SWT.CENTER );
    props.setLook( wbbFilename );
    wbbFilename.setText( BaseMessages.getString( PKG, "XsdValidatorDialog.FilenameBrowse.Button" ) );
    wbbFilename.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.BrowseForFileOrDirAndAdd" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( 100, 0 );
    fdbFilename.top = new FormAttachment( wXSDSource, margin );
    wbbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar( variables, wXSD, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFilename );
    wFilename.addModifyListener( lsMod );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( middle, margin );
    fdFilename.right = new FormAttachment( wbbFilename, -margin );
    fdFilename.top = new FormAttachment( wXSDSource, margin );
    wFilename.setLayoutData(fdFilename);

    // XSD file defined in a column
    wlXSDDefinedColumn = new Label(wXSD, SWT.RIGHT );
    wlXSDDefinedColumn.setText( BaseMessages.getString( PKG, "XsdValidatorDialog.XSDDefinedColumn.Label" ) );
    props.setLook( wlXSDDefinedColumn );
    FormData fdlXSDDefinedColumn = new FormData();
    fdlXSDDefinedColumn.left = new FormAttachment( 0, 0 );
    fdlXSDDefinedColumn.top = new FormAttachment( wFilename, 2 * margin );
    fdlXSDDefinedColumn.right = new FormAttachment( middle, -margin );
    wlXSDDefinedColumn.setLayoutData(fdlXSDDefinedColumn);
    wXSDDefinedColumn = new CCombo(wXSD, SWT.BORDER | SWT.READ_ONLY );
    wXSDDefinedColumn.setEditable( true );
    props.setLook( wXSDDefinedColumn );
    wXSDDefinedColumn.addModifyListener( lsMod );
    FormData fdXSDDefinedColumn = new FormData();
    fdXSDDefinedColumn.left = new FormAttachment( middle, margin );
    fdXSDDefinedColumn.top = new FormAttachment( wFilename, 2 * margin );
    fdXSDDefinedColumn.right = new FormAttachment( 100, -margin );
    wXSDDefinedColumn.setLayoutData(fdXSDDefinedColumn);
    wXSDDefinedColumn.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        PopulateFields();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    FormData fdXSD = new FormData();
    fdXSD.left = new FormAttachment( 0, margin );
    fdXSD.top = new FormAttachment(wOutputFields, margin );
    fdXSD.right = new FormAttachment( 100, -margin );
    wXSD.setLayoutData(fdXSD);

    // ///////////////////////////////////////////////////////////
    // / END OF XSD GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    props.setLook(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners
    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    // Whenever something changes, set the tooltip to the expanded version
    // of the filename:
    wFilename.addModifyListener( e -> wFilename.setToolTipText( variables.resolve( wFilename.getText() ) ) );

    // Listen to the Browse... button
    wbbFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {

        FileDialog dialog = new FileDialog( shell, SWT.OPEN );
        dialog.setFilterExtensions( new String[] { "*xsd;*.XSD", "*" } );
        if ( wFilename.getText() != null ) {
          String fname = variables.resolve( wFilename.getText() );
          dialog.setFileName( fname );
        }

        dialog.setFilterNames( new String[] { BaseMessages.getString( PKG, "XsdValidatorDialog.FileType" ),
          BaseMessages.getString( PKG, "System.FileType.AllFiles" ) } );

        if ( dialog.open() != null ) {
          String str = dialog.getFilterPath() + System.getProperty( "file.separator" ) + dialog.getFileName();
          wFilename.setText( str );
        }
      }

    } );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    activeValidationMsg();
    activeOutputStringField();
    setXSDSource();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void setXSDSource() {
    if ( wXSDSource.getSelectionIndex() == 0 ) {
      // XSD source is a file, let user specify it
      wFilename.setEnabled( true );
      wlFilename.setEnabled( true );
      wbbFilename.setEnabled( true );

      wlXSDDefinedColumn.setEnabled( false );
      wXSDDefinedColumn.setEnabled( false );
    } else if ( wXSDSource.getSelectionIndex() == 1 ) {
      // XSD source is a file, let user specify field that contain it
      wFilename.setEnabled( false );
      wlFilename.setEnabled( false );
      wbbFilename.setEnabled( false );

      wlXSDDefinedColumn.setEnabled( true );
      wXSDDefinedColumn.setEnabled( true );

    } else {
      // XSD source is in the XML source
      wFilename.setEnabled( false );
      wlFilename.setEnabled( false );
      wbbFilename.setEnabled( false );

      wlXSDDefinedColumn.setEnabled( false );
      wXSDDefinedColumn.setEnabled( false );
    }
  }

  private void PopulateFields() {
    if ( !gotPrevious ) {
      gotPrevious = true;

      String fieldXML = wXMLStream.getText();
      String fieldXSD = wXSDDefinedColumn.getText();
      try {
        wXMLStream.removeAll();
        wXSDDefinedColumn.removeAll();

        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          wXMLStream.setItems( r.getFieldNames() );
          wXSDDefinedColumn.setItems( r.getFieldNames() );
        }
      } catch ( HopException ke ) {
        new ErrorDialog( shell, BaseMessages.getString( PKG, "XsdValidatorDialog.FailedToGetFields.DialogTitle" ),
            BaseMessages.getString( PKG, "XsdValidatorDialogMod.FailedToGetFields.DialogMessage" ), ke );
      }
      if ( fieldXML != null ) {
        wXMLStream.setText( fieldXML );
      }
      if ( fieldXSD != null ) {
        wXSDDefinedColumn.setText( fieldXSD );
      }
    }
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {

    if ( input.getXSDFilename() != null ) {
      wFilename.setText( input.getXSDFilename() );
    }

    // XML source
    wXMLSourceFile.setSelection( input.getXMLSourceFile() );
    if ( input.getXMLStream() != null ) {
      wXMLStream.setText( input.getXMLStream() );
    }

    if ( input.getXSDDefinedField() != null ) {
      wXSDDefinedColumn.setText( input.getXSDDefinedField() );
    }

    // Output Fields
    if ( input.getResultfieldname() != null ) {
      wResultField.setText( input.getResultfieldname() );
    }
    wAddValidationMsg.setSelection( input.useAddValidationMessage() );
    if ( input.getValidationMessageField() != null ) {
      wValidationMsg.setText( input.getValidationMessageField() );
    } else {
      wValidationMsg.setText( "ValidationMsgField" );
    }

    wOutputStringField.setSelection( input.getOutputStringField() );

    if ( input.getIfXmlValid() != null ) {
      wIfXMLValid.setText( input.getIfXmlValid() );
    }
    if ( input.getIfXmlInvalid() != null ) {
      wIfXMLUnValid.setText( input.getIfXmlInvalid() );
    }

    wAllowExternalEntities.setSelection( input.isAllowExternalEntities() );

    if ( input.getXSDSource() != null ) {
      if ( input.getXSDSource().equals( input.SPECIFY_FILENAME ) ) {
        wXSDSource.select( 0 );
      } else if ( input.getXSDSource().equals( input.SPECIFY_FIELDNAME ) ) {
        wXSDSource.select( 1 );
      } else {
        wXSDSource.select( 2 );
      }
    } else {
      wXSDSource.select( 0 );
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void activeValidationMsg() {
    wValidationMsg.setEnabled( wAddValidationMsg.getSelection() );
  }

  private void activeOutputStringField() {
    wIfXMLValid.setEnabled( wOutputStringField.getSelection() );
    wIfXMLUnValid.setEnabled( wOutputStringField.getSelection() );

  }

  private void cancel() {
    transformName = null;
    input.setChanged( changed );
    dispose();
  }

  private void ok() {
    transformName = wTransformName.getText(); // return value

    input.setXSDfilename( wFilename.getText() );
    input.setResultfieldname( wResultField.getText() );
    input.setXMLStream( wXMLStream.getText() );
    input.setXSDDefinedField( wXSDDefinedColumn.getText() );

    input.setOutputStringField( wOutputStringField.getSelection() );
    input.setAddValidationMessage( wAddValidationMsg.getSelection() );
    input.setValidationMessageField( wValidationMsg.getText() );
    input.setIfXMLValid( wIfXMLValid.getText() );
    input.setIfXmlInvalid( wIfXMLUnValid.getText() );

    input.setXMLSourceFile( wXMLSourceFile.getSelection() );

    input.setAllowExternalEntities( wAllowExternalEntities.getSelection() );

    if ( wXSDSource.getSelectionIndex() == 0 ) {
      input.setXSDSource( input.SPECIFY_FILENAME );
    } else if ( wXSDSource.getSelectionIndex() == 1 ) {
      input.setXSDSource( input.SPECIFY_FIELDNAME );
    } else {
      input.setXSDSource( input.NO_NEED );
    }

    dispose();
  }
}
