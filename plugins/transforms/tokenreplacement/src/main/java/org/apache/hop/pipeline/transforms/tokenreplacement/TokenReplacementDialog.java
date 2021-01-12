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
package org.apache.hop.pipeline.transforms.tokenreplacement;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TokenReplacementDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = TokenReplacementMeta.class; // For Translator

  private static final String[] YES_NO_COMBO = new String[] { BaseMessages.getString( PKG, "System.Combo.No" ),
    BaseMessages.getString( PKG, "System.Combo.Yes" ) };

  private CTabFolder wTabFolder;
  private FormData fdTabFolder;

  private CTabItem wInputTab, wOutputTab, wTokensTab;

  private FormData fdInputComp, fdOutputComp, fdTokensComp;

  private Group gInputText, gInputFile, gInputField;
  private FormData fdgInputText, fdgInputFile, fdgInputField;

  private Group gOutputField, gOutputFile;
  private FormData fdgOutputField, fdgOutputFile;
  
  private Label wlInputType;
  private CCombo wInputType;
  private FormData fdlInputType, fdInputType;

  private Label wlInputText;
  private Text wInputText;
  private FormData fdlInputText, fdInputText;
  
  private Label wlInputField;
  private ComboVar wInputField;
  private FormData fdlInputField, fdInputField;

  private Label wlInputFilename;
  private Button wbInputFilename;
  private TextVar wInputFilename;
  private FormData fdlInputFilename, fdbInputFilename, fdInputFilename;
  
  private Label wlInputFilenameInField;
  private Button wInputFilenameInField;
  private FormData fdlInputFilenameInField, fdInputFilenameInField;
  
  private Label wlInputFilenameField;
  private ComboVar wInputFilenameField;
  private FormData fdlInputFilenameField, fdInputFilenameField;
  
  private Label wlAddInputFilenameToResult;
  private Button wAddInputFilenameToResult;
  private FormData fdlAddInputFilenameToResult, fdAddInputFilenameToResult;
  
  private Label wlOutputType;
  private CCombo wOutputType;
  private FormData fdlOutputType, fdOutputType;

  private Label wlOutputField;
  private TextVar wOutputField;
  private FormData fdlOutputField, fdOutputField;

  private Label wlOutputFilename;
  private Button wbOutputFilename;
  private TextVar wOutputFilename;
  private FormData fdlOutputFilename, fdbOutputFilename, fdOutputFilename;

  private Label wlOutputFilenameInField;
  private Button wOutputFilenameInField;
  private FormData fdlOutputFilenameInField, fdOutputFilenameInField;

  private Label wlOutputFilenameField;
  private ComboVar wOutputFilenameField;
  private FormData fdlOutputFilenameField, fdOutputFilenameField;

  private Label wlAppendOutputFilename;
  private Button wAppendOutputFilename;
  private FormData fdlAppendOutputFilename, fdAppendOutputFilename;

  private Label wlCreateParentFolder;
  private Button wCreateParentFolder;
  private FormData fdlCreateParentFolder, fdCreateParentFolder;

  private Label wlFormat;
  private CCombo wFormat;
  private FormData fdlFormat, fdFormat;

  private Label wlOutputFileEncoding;
  private CCombo wOutputFileEncoding;
  private FormData fdlOutputFileEncoding, fdOutputFileEncoding;

  private Label wlOutputSplitEvery;
  private Text wOutputSplitEvery;
  private FormData fdlOutputSplitEvery, fdOutputSplitEvery;

  private Label wlIncludeTransformNrInFilename;
  private Button wIncludeTransformNrInFilename;
  private FormData fdlIncludeTransformNrInFilename, fdIncludeTransformNrInFilename;
  
  private Label wlIncludePartNrInFilename;
  private Button wIncludePartNrInFilename;
  private FormData fdlIncludePartNrInFilename, fdIncludePartNrInFilename;
  
  private Label wlIncludeDateInFilename;
  private Button wIncludeDateInFilename;
  private FormData fdlIncludeDateInFilename, fdIncludeDateInFilename;
  
  private Label wlIncludeTimeInFilename;
  private Button wIncludeTimeInFilename;
  private FormData fdlIncludeTimeInFilename, fdIncludeTimeInFilename;
  
  private Label wlSpecifyDateFormat;
  private Button wSpecifyDateFormat;
  private FormData fdlSpecifyDateFormat, fdSpecifyDateFormat;
  
  private Label wlDateFormat;
  private CCombo wDateFormat;
  private FormData fdlDateFormat, fdDateFormat;
  
  private Label wlAddOutputFilenameToResult;
  private Button wAddOutputFilenameToResult;
  private FormData fdlAddOutputFilenameToResult, fdAddOutputFilenameToResult;
  
  private Label wlTokenStartString;
  private TextVar wTokenStartString;
  private FormData fdlTokenStartString, fdTokenStartString;
  
  private Label wlTokenEndString;
  private TextVar wTokenEndString;
  private FormData fdlTokenEndString, fdTokenEndString;
  
  private TableView wFields;
  private FormData fdFields;

  private ColumnInfo[] colinf;

  private Button wUpdateTypes;
  private FormData fdUpdateTypes;

  private Link wDevelopedBy;
  private FormData fdDevelopedBy;

  private final TokenReplacementMeta input;

  private final Map<String, Integer> inputFields;

  private boolean gotPreviousFields = false;

  private boolean gotEncodings = false;
  
  
  public TokenReplacementDialog(Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (TokenReplacementMeta) in;
    inputFields = new HashMap<String, Integer>();
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
    shell.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Transformname line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.top = new FormAttachment( 0, margin );
    fdlTransformName.right = new FormAttachment( middle, -margin );
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
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF INPUT TAB///
    // /
    wInputTab = new CTabItem( wTabFolder, SWT.NONE );
    wInputTab.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.InputTab.TabTitle" ) );

    Composite wInputComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wInputComp );

    FormLayout inputLayout = new FormLayout();
    inputLayout.marginWidth = 3;
    inputLayout.marginHeight = 3;
    wInputComp.setLayout( inputLayout );

    //
    // Input Type
    wlInputType = new Label( wInputComp, SWT.RIGHT );
    wlInputType.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.InputType.Label" ) );
    props.setLook( wlInputType );
    fdlInputType = new FormData();
    fdlInputType.left = new FormAttachment( 0, 0 );
    fdlInputType.top = new FormAttachment( 0, margin );
    fdlInputType.right = new FormAttachment( middle, -margin );
    wlInputType.setLayoutData( fdlInputType );

    wInputType = new CCombo( wInputComp, SWT.BORDER | SWT.READ_ONLY );
    wInputType.setEditable( true );
    props.setLook( wInputType );
    wInputType.addModifyListener( lsMod );
    fdInputType = new FormData();
    fdInputType.left = new FormAttachment( middle, 0 );
    fdInputType.top = new FormAttachment( 0, margin );
    fdInputType.right = new FormAttachment( 75, 0 );
    wInputType.setLayoutData( fdInputType );
    String[] inputTypes = TokenReplacementMeta.INPUT_TYPES;
    for ( String inputType : inputTypes ) {
      wInputType.add( inputType );
    }
    wInputType.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent selectionEvent ) {
        input.setChanged();
        updateInputType();
      }
    } );

    /////////////////////
    //Input text group
    gInputText = new Group( wInputComp, SWT.SHADOW_ETCHED_IN );
    gInputText.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.InputTextGroup.Label" ) );
    FormLayout inputTextLayout = new FormLayout();
    inputTextLayout.marginWidth = 3;
    inputTextLayout.marginHeight = 3;
    gInputText.setLayout( inputTextLayout );
    props.setLook( gInputText );

    // input text
    wlInputText = new Label( gInputText, SWT.RIGHT );
    wlInputText.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.InputText.Label" ) );
    props.setLook( wlInputText );
    fdlInputText = new FormData();
    fdlInputText.left = new FormAttachment( 0, 0 );
    fdlInputText.right = new FormAttachment( middle, -margin );
    fdlInputText.top = new FormAttachment( 0, margin );
    fdlInputText.bottom = new FormAttachment( 100, -margin );
    wlInputText.setLayoutData( fdlInputText );

    wInputText = new Text( gInputText, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL );
    props.setLook( wInputText );
    wInputText.addModifyListener( lsMod );
    fdInputText = new FormData();
    fdInputText.left = new FormAttachment( middle, 0 );
    fdInputText.top = new FormAttachment( 0, margin );
    fdInputText.right = new FormAttachment( 100, 0 );
    fdInputText.bottom = new FormAttachment( 100, -margin );
    wInputText.setLayoutData( fdInputText );

    fdgInputText = new FormData();
    fdgInputText.left = new FormAttachment( 0, 0 );
    fdgInputText.right = new FormAttachment( 100, 0 );
    fdgInputText.top = new FormAttachment( wInputType, margin * 2 );
    fdgInputText.bottom = new FormAttachment( 100, -margin * 2 );
    gInputText.setLayoutData( fdgInputText );

    /////////////////////
    //Input field group
    gInputField = new Group( wInputComp, SWT.SHADOW_ETCHED_IN );
    gInputField.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.InputFieldGroup.Label" ) );
    FormLayout inputFieldLayout = new FormLayout();
    inputFieldLayout.marginWidth = 3;
    inputFieldLayout.marginHeight = 3;
    gInputField.setLayout( inputFieldLayout );
    props.setLook( gInputField );

    // input Field Line
    wlInputField = new Label( gInputField, SWT.RIGHT );
    wlInputField.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.InputField.Label" ) );
    props.setLook( wlInputField );
    fdlInputField = new FormData();
    fdlInputField.left = new FormAttachment( 0, 0 );
    fdlInputField.right = new FormAttachment( middle, -margin );
    fdlInputField.top = new FormAttachment( wInputType, margin );
    wlInputField.setLayoutData( fdlInputField );

    wInputField = new ComboVar( variables, gInputField, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInputField );
    wInputField.addModifyListener( lsMod );
    fdInputField = new FormData();
    fdInputField.left = new FormAttachment( middle, 0 );
    fdInputField.top = new FormAttachment( wInputType, margin );
    fdInputField.right = new FormAttachment( 100, 0 );
    wInputField.setLayoutData( fdInputField );
    wInputField.setEnabled( false );
    wInputField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        getFields();
        shell.setCursor( null );
        busy.dispose();
      }
    } );
    /* End */

    fdgInputField = new FormData();
    fdgInputField.left = new FormAttachment( 0, 0 );
    fdgInputField.right = new FormAttachment( 100, 0 );
    fdgInputField.top = new FormAttachment( wInputType, margin * 2 );
    gInputField.setLayoutData( fdgInputField );

    /////////////////////
    //Input file group
    gInputFile = new Group( wInputComp, SWT.SHADOW_ETCHED_IN );
    gInputFile.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.InputFileGroup.Label" ) );
    FormLayout inputFileLayout = new FormLayout();
    inputFileLayout.marginWidth = 3;
    inputFileLayout.marginHeight = 3;
    gInputFile.setLayout( inputFileLayout );
    props.setLook( gInputFile );

    // InputFilename line
    wlInputFilename = new Label( gInputFile, SWT.RIGHT );
    wlInputFilename.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.InputFilename.Label" ) );
    props.setLook( wlInputFilename );
    fdlInputFilename = new FormData();
    fdlInputFilename.left = new FormAttachment( 0, 0 );
    fdlInputFilename.top = new FormAttachment( 0, margin );
    fdlInputFilename.right = new FormAttachment( middle, -margin );
    wlInputFilename.setLayoutData( fdlInputFilename );

    wbInputFilename = new Button( gInputFile, SWT.PUSH | SWT.CENTER );
    props.setLook( wbInputFilename );
    wbInputFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbInputFilename = new FormData();
    fdbInputFilename.right = new FormAttachment( 100, 0 );
    fdbInputFilename.top = new FormAttachment( 0, 0 );
    wbInputFilename.setLayoutData( fdbInputFilename );

    wInputFilename = new TextVar( variables, gInputFile, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInputFilename );
    wInputFilename.addModifyListener( lsMod );
    fdInputFilename = new FormData();
    fdInputFilename.left = new FormAttachment( middle, 0 );
    fdInputFilename.top = new FormAttachment( 0, margin );
    fdInputFilename.right = new FormAttachment( wbInputFilename, -margin );
    wInputFilename.setLayoutData( fdInputFilename );
    

    // File name in field line
    //
    wlInputFilenameInField = new Label( gInputFile, SWT.RIGHT );
    wlInputFilenameInField.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.FilenameInField.Label" ) );
    props.setLook( wlInputFilenameInField );
    fdlInputFilenameInField = new FormData();
    fdlInputFilenameInField.left = new FormAttachment( 0, 0 );
    fdlInputFilenameInField.top = new FormAttachment( wInputFilename, margin );
    fdlInputFilenameInField.right = new FormAttachment( middle, -margin );
    wlInputFilenameInField.setLayoutData( fdlInputFilenameInField );

    wInputFilenameInField = new Button( gInputFile, SWT.CHECK );
    props.setLook( wInputFilenameInField );
    fdInputFilenameInField = new FormData();
    fdInputFilenameInField.left = new FormAttachment( middle, 0 );
    fdInputFilenameInField.top = new FormAttachment( wInputFilename, margin );
    fdInputFilenameInField.right = new FormAttachment( 100, 0 );
    wInputFilenameInField.setLayoutData( fdInputFilenameInField );
    wInputFilenameInField.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setInputFilenameInField();
      }
    } );

    // Input FileNameField Line
    wlInputFilenameField = new Label( gInputFile, SWT.RIGHT );
    wlInputFilenameField.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.InputFilenameField.Label" ) );
    props.setLook( wlInputFilenameField );
    fdlInputFilenameField = new FormData();
    fdlInputFilenameField.left = new FormAttachment( 0, 0 );
    fdlInputFilenameField.right = new FormAttachment( middle, -margin );
    fdlInputFilenameField.top = new FormAttachment( wInputFilenameInField, margin );
    wlInputFilenameField.setLayoutData( fdlInputFilenameField );

    wInputFilenameField = new ComboVar( variables, gInputFile, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wInputFilenameField );
    wInputFilenameField.addModifyListener( lsMod );
    fdInputFilenameField = new FormData();
    fdInputFilenameField.left = new FormAttachment( middle, 0 );
    fdInputFilenameField.top = new FormAttachment( wInputFilenameInField, margin );
    fdInputFilenameField.right = new FormAttachment( 100, 0 );
    wInputFilenameField.setLayoutData( fdInputFilenameField );
    wInputFilenameField.setEnabled( false );
    wInputFilenameField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        getFields();
        shell.setCursor( null );
        busy.dispose();
      }
    } );
    /* End */

    // Add Input Filename to Result Line
    //
    wlAddInputFilenameToResult = new Label( gInputFile, SWT.RIGHT );
    wlAddInputFilenameToResult.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.AddInputFilenameToResult.Label" ) );
    props.setLook( wlAddInputFilenameToResult );
    fdlAddInputFilenameToResult = new FormData();
    fdlAddInputFilenameToResult.left = new FormAttachment( 0, 0 );
    fdlAddInputFilenameToResult.top = new FormAttachment( wInputFilenameField, margin );
    fdlAddInputFilenameToResult.right = new FormAttachment( middle, -margin );
    wlAddInputFilenameToResult.setLayoutData( fdlAddInputFilenameToResult );

    wAddInputFilenameToResult = new Button( gInputFile, SWT.CHECK );
    props.setLook( wAddInputFilenameToResult );
    fdAddInputFilenameToResult = new FormData();
    fdAddInputFilenameToResult.left = new FormAttachment( middle, 0 );
    fdAddInputFilenameToResult.top = new FormAttachment( wInputFilenameField, margin );
    fdAddInputFilenameToResult.right = new FormAttachment( 100, 0 );
    wAddInputFilenameToResult.setLayoutData( fdAddInputFilenameToResult );
    wAddInputFilenameToResult.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    fdgInputFile = new FormData();
    fdgInputFile.left = new FormAttachment( 0, 0 );
    fdgInputFile.right = new FormAttachment( 100, 0 );
    fdgInputFile.top = new FormAttachment( wInputType, margin * 2 );
    gInputFile.setLayoutData( fdgInputFile );

    fdInputComp = new FormData();
    fdInputComp.left = new FormAttachment( 0, 0 );
    fdInputComp.top = new FormAttachment( 0, 0 );
    fdInputComp.right = new FormAttachment( 100, 0 );
    fdInputComp.bottom = new FormAttachment( 100, 0 );
    wInputComp.setLayoutData( fdInputComp );

    wInputComp.layout();
    wInputTab.setControl( wInputComp );

    // ////////////////////////
    // START OF OUTPUT TAB///
    // /
    wOutputTab = new CTabItem( wTabFolder, SWT.NONE );
    wOutputTab.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.OutputTab.TabTitle" ) );

    Composite wOutputComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wOutputComp );

    FormLayout outputLayout = new FormLayout();
    outputLayout.marginWidth = 3;
    outputLayout.marginHeight = 3;
    wOutputComp.setLayout( outputLayout );

    //
    // Output Type
    wlOutputType = new Label( wOutputComp, SWT.RIGHT );
    wlOutputType.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.OutputType.Label" ) );
    props.setLook( wlOutputType );
    fdlOutputType = new FormData();
    fdlOutputType.left = new FormAttachment( 0, 0 );
    fdlOutputType.top = new FormAttachment( 0, margin );
    fdlOutputType.right = new FormAttachment( middle, -margin );
    wlOutputType.setLayoutData( fdlOutputType );

    wOutputType = new CCombo( wOutputComp, SWT.BORDER | SWT.READ_ONLY );
    wOutputType.setEditable( true );
    props.setLook( wOutputType );
    wOutputType.addModifyListener( lsMod );
    fdOutputType = new FormData();
    fdOutputType.left = new FormAttachment( middle, 0 );
    fdOutputType.top = new FormAttachment( 0, margin );
    fdOutputType.right = new FormAttachment( 75, 0 );
    wOutputType.setLayoutData( fdOutputType );
    String[] outputTypes = TokenReplacementMeta.OUTPUT_TYPES;
    for ( String outputType : outputTypes ) {
      wOutputType.add( outputType );
    }
    wOutputType.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent selectionEvent ) {
        input.setChanged();
        updateOutputType();
      }
    } );

    /////////////////////
    //Output field group
    gOutputField = new Group( wOutputComp, SWT.SHADOW_ETCHED_IN );
    gOutputField.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.OutputFieldGroup.Label" ) );
    FormLayout outputFieldLayout = new FormLayout();
    outputFieldLayout.marginWidth = 3;
    outputFieldLayout.marginHeight = 3;
    gOutputField.setLayout( outputFieldLayout );
    props.setLook( gOutputField );


    // output Field Line
    wlOutputField = new Label( gOutputField, SWT.RIGHT );
    wlOutputField.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.OutputField.Label" ) );
    props.setLook( wlOutputField );
    fdlOutputField = new FormData();
    fdlOutputField.left = new FormAttachment( 0, 0 );
    fdlOutputField.right = new FormAttachment( middle, -margin );
    fdlOutputField.top = new FormAttachment( 0, margin );
    wlOutputField.setLayoutData( fdlOutputField );

    wOutputField = new TextVar( variables, gOutputField, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wOutputField );
    wOutputField.addModifyListener( lsMod );
    fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment( middle, 0 );
    fdOutputField.top = new FormAttachment( 0, margin );
    fdOutputField.right = new FormAttachment( 100, 0 );
    wOutputField.setLayoutData( fdOutputField );
    wOutputField.setEnabled( false );
    wOutputField.addModifyListener( lsMod );
    /* End */

    fdgOutputField = new FormData();
    fdgOutputField.left = new FormAttachment( 0, 0 );
    fdgOutputField.right = new FormAttachment( 100, 0 );
    fdgOutputField.top = new FormAttachment( wOutputType, margin * 2 );
    gOutputField.setLayoutData( fdgOutputField );

    /////////////////////
    //Output file group
    gOutputFile = new Group( wOutputComp, SWT.SHADOW_ETCHED_IN );
    gOutputFile.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.OutputFileGroup.Label" ) );
    FormLayout outputFileLayout = new FormLayout();
    outputFileLayout.marginWidth = 3;
    outputFileLayout.marginHeight = 3;
    gOutputFile.setLayout( outputFileLayout );
    props.setLook( gOutputFile );

    // OutputFilename line
    wlOutputFilename = new Label( gOutputFile, SWT.RIGHT );
    wlOutputFilename.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.OutputFilename.Label" ) );
    props.setLook( wlOutputFilename );
    fdlOutputFilename = new FormData();
    fdlOutputFilename.left = new FormAttachment( 0, 0 );
    fdlOutputFilename.top = new FormAttachment( 0, margin );
    fdlOutputFilename.right = new FormAttachment( middle, -margin );
    wlOutputFilename.setLayoutData( fdlOutputFilename );

    wbOutputFilename = new Button( gOutputFile, SWT.PUSH | SWT.CENTER );
    props.setLook( wbOutputFilename );
    wbOutputFilename.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    fdbOutputFilename = new FormData();
    fdbOutputFilename.right = new FormAttachment( 100, 0 );
    fdbOutputFilename.top = new FormAttachment( 0, 0 );
    wbOutputFilename.setLayoutData( fdbOutputFilename );

    wOutputFilename = new TextVar( variables, gOutputFile, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wOutputFilename );
    wOutputFilename.addModifyListener( lsMod );
    fdOutputFilename = new FormData();
    fdOutputFilename.left = new FormAttachment( middle, 0 );
    fdOutputFilename.top = new FormAttachment( 0, margin );
    fdOutputFilename.right = new FormAttachment( wbOutputFilename, -margin );
    wOutputFilename.setLayoutData( fdOutputFilename );


    // File name in field line
    //
    wlOutputFilenameInField = new Label( gOutputFile, SWT.RIGHT );
    wlOutputFilenameInField.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.FilenameInField.Label" ) );
    props.setLook( wlOutputFilenameInField );
    fdlOutputFilenameInField = new FormData();
    fdlOutputFilenameInField.left = new FormAttachment( 0, 0 );
    fdlOutputFilenameInField.top = new FormAttachment( wOutputFilename, margin );
    fdlOutputFilenameInField.right = new FormAttachment( middle, -margin );
    wlOutputFilenameInField.setLayoutData( fdlOutputFilenameInField );

    wOutputFilenameInField = new Button( gOutputFile, SWT.CHECK );
    props.setLook( wOutputFilenameInField );
    fdOutputFilenameInField = new FormData();
    fdOutputFilenameInField.left = new FormAttachment( middle, 0 );
    fdOutputFilenameInField.top = new FormAttachment( wOutputFilename, margin );
    fdOutputFilenameInField.right = new FormAttachment( 100, 0 );
    wOutputFilenameInField.setLayoutData( fdOutputFilenameInField );
    wOutputFilenameInField.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setOutputFilenameInField();
      }
    } );

    // Output FileNameField Line
    wlOutputFilenameField = new Label( gOutputFile, SWT.RIGHT );
    wlOutputFilenameField.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.OutputFilenameField.Label" ) );
    props.setLook( wlOutputFilenameField );
    fdlOutputFilenameField = new FormData();
    fdlOutputFilenameField.left = new FormAttachment( 0, 0 );
    fdlOutputFilenameField.right = new FormAttachment( middle, -margin );
    fdlOutputFilenameField.top = new FormAttachment( wOutputFilenameInField, margin );
    wlOutputFilenameField.setLayoutData( fdlOutputFilenameField );

    wOutputFilenameField = new ComboVar( variables, gOutputFile, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wOutputFilenameField );
    wOutputFilenameField.addModifyListener( lsMod );
    fdOutputFilenameField = new FormData();
    fdOutputFilenameField.left = new FormAttachment( middle, 0 );
    fdOutputFilenameField.top = new FormAttachment( wOutputFilenameInField, margin );
    fdOutputFilenameField.right = new FormAttachment( 100, 0 );
    wOutputFilenameField.setLayoutData( fdOutputFilenameField );
    wOutputFilenameField.setEnabled( false );
    wOutputFilenameField.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        getFields();
        shell.setCursor( null );
        busy.dispose();
      }
    } );
    /* End */

    // Append
    //
    wlAppendOutputFilename = new Label( gOutputFile, SWT.RIGHT );
    wlAppendOutputFilename.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.AppendOutput.Label" ) );
    props.setLook( wlAppendOutputFilename );
    fdlAppendOutputFilename = new FormData();
    fdlAppendOutputFilename.left = new FormAttachment( 0, 0 );
    fdlAppendOutputFilename.top = new FormAttachment( wOutputFilenameField, margin );
    fdlAppendOutputFilename.right = new FormAttachment( middle, -margin );
    wlAppendOutputFilename.setLayoutData( fdlAppendOutputFilename );

    wAppendOutputFilename = new Button( gOutputFile, SWT.CHECK );
    wAppendOutputFilename.setToolTipText( BaseMessages.getString( PKG, "TokenReplacementDialog.AppendOutput.Tooltip" ) );
    props.setLook( wAppendOutputFilename );
    fdAppendOutputFilename = new FormData();
    fdAppendOutputFilename.left = new FormAttachment( middle, 0 );
    fdAppendOutputFilename.top = new FormAttachment( wOutputFilenameField, margin );
    fdAppendOutputFilename.right = new FormAttachment( 100, 0 );
    wAppendOutputFilename.setLayoutData( fdAppendOutputFilename );
    wAppendOutputFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Create Parent Folder
    //
    wlCreateParentFolder = new Label( gOutputFile, SWT.RIGHT );
    wlCreateParentFolder.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.CreateParentFolder.Label" ) );
    props.setLook( wlCreateParentFolder );
    fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment( 0, 0 );
    fdlCreateParentFolder.top = new FormAttachment( wAppendOutputFilename, margin );
    fdlCreateParentFolder.right = new FormAttachment( middle, -margin );
    wlCreateParentFolder.setLayoutData( fdlCreateParentFolder );

    wCreateParentFolder = new Button( gOutputFile, SWT.CHECK );
    wCreateParentFolder.setToolTipText( BaseMessages.getString( PKG, "TokenReplacementDialog.CreateParentFolder.Tooltip" ) );
    props.setLook( wCreateParentFolder );
    fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment( middle, 0 );
    fdCreateParentFolder.top = new FormAttachment( wAppendOutputFilename, margin );
    fdCreateParentFolder.right = new FormAttachment( 100, 0 );
    wCreateParentFolder.setLayoutData( fdCreateParentFolder );
    wCreateParentFolder.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    wlFormat = new Label( gOutputFile, SWT.RIGHT );
    wlFormat.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.Format.Label" ) );
    props.setLook( wlFormat );
    fdlFormat = new FormData();
    fdlFormat.left = new FormAttachment( 0, 0 );
    fdlFormat.top = new FormAttachment( wCreateParentFolder, margin );
    fdlFormat.right = new FormAttachment( middle, -margin );
    wlFormat.setLayoutData( fdlFormat );

    wFormat = new CCombo( gOutputFile, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wFormat );

    for ( int i = 0; i < TokenReplacementMeta.formatMapperLineTerminator.length; i++ ) {
      wFormat.add( TokenReplacementMeta.formatMapperLineTerminatorDescriptions[i] );
    }
    wFormat.select( 0 );
    wFormat.addModifyListener( lsMod );
    fdFormat = new FormData();
    fdFormat.left = new FormAttachment( middle, 0 );
    fdFormat.top = new FormAttachment( wCreateParentFolder, margin );
    fdFormat.right = new FormAttachment( 100, 0 );
    wFormat.setLayoutData( fdFormat );

    //Encoding line
    wlOutputFileEncoding = new Label( gOutputFile, SWT.RIGHT );
    wlOutputFileEncoding.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.Encoding.Label" ) );
    props.setLook( wlOutputFileEncoding );
    fdlOutputFileEncoding = new FormData();
    fdlOutputFileEncoding.left = new FormAttachment( 0, 0 );
    fdlOutputFileEncoding.top = new FormAttachment( wFormat, margin );
    fdlOutputFileEncoding.right = new FormAttachment( middle, -margin );
    wlOutputFileEncoding.setLayoutData( fdlOutputFileEncoding );

    wOutputFileEncoding = new CCombo( gOutputFile, SWT.BORDER | SWT.READ_ONLY );
    wOutputFileEncoding.setEditable( true );
    props.setLook( wOutputFileEncoding );
    wOutputFileEncoding.addModifyListener( lsMod );
    fdOutputFileEncoding = new FormData();
    fdOutputFileEncoding.left = new FormAttachment( middle, 0 );
    fdOutputFileEncoding.top = new FormAttachment( wFormat, margin );
    fdOutputFileEncoding.right = new FormAttachment( 100, 0 );
    wOutputFileEncoding.setLayoutData( fdOutputFileEncoding );
    wOutputFileEncoding.addFocusListener( new FocusListener() {
      public void focusLost( org.eclipse.swt.events.FocusEvent e ) {
      }

      public void focusGained( org.eclipse.swt.events.FocusEvent e ) {
        Cursor busy = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
        shell.setCursor( busy );
        setEncodings();
        shell.setCursor( null );
        busy.dispose();
      }
    } );

    // output Split Every Line
    wlOutputSplitEvery = new Label( gOutputFile, SWT.RIGHT );
    wlOutputSplitEvery.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.OutputSplitEvery.Label" ) );
    props.setLook( wlOutputSplitEvery );
    fdlOutputSplitEvery = new FormData();
    fdlOutputSplitEvery.left = new FormAttachment( 0, 0 );
    fdlOutputSplitEvery.right = new FormAttachment( middle, -margin );
    fdlOutputSplitEvery.top = new FormAttachment( wOutputFileEncoding, margin );
    wlOutputSplitEvery.setLayoutData( fdlOutputSplitEvery );

    wOutputSplitEvery = new Text( gOutputFile, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wOutputSplitEvery );
    wOutputSplitEvery.addModifyListener( lsMod );
    fdOutputSplitEvery = new FormData();
    fdOutputSplitEvery.left = new FormAttachment( middle, 0 );
    fdOutputSplitEvery.top = new FormAttachment( wOutputFileEncoding, margin );
    fdOutputSplitEvery.right = new FormAttachment( 100, 0 );
    wOutputSplitEvery.setLayoutData( fdOutputSplitEvery );
    wOutputSplitEvery.setEnabled( false );


    // Include Transform NR in Output Filename
    //
    wlIncludeTransformNrInFilename = new Label( gOutputFile, SWT.RIGHT );
    wlIncludeTransformNrInFilename.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.IncludeTransformnr.Label" ) );
    props.setLook(wlIncludeTransformNrInFilename);
    fdlIncludeTransformNrInFilename = new FormData();
    fdlIncludeTransformNrInFilename.left = new FormAttachment( 0, 0 );
    fdlIncludeTransformNrInFilename.top = new FormAttachment( wOutputSplitEvery, margin );
    fdlIncludeTransformNrInFilename.right = new FormAttachment( middle, -margin );
    wlIncludeTransformNrInFilename.setLayoutData( fdlIncludeTransformNrInFilename );

    wIncludeTransformNrInFilename = new Button( gOutputFile, SWT.CHECK );
    props.setLook( wIncludeTransformNrInFilename );
    fdIncludeTransformNrInFilename = new FormData();
    fdIncludeTransformNrInFilename.left = new FormAttachment( middle, 0 );
    fdIncludeTransformNrInFilename.top = new FormAttachment( wOutputSplitEvery, margin );
    fdIncludeTransformNrInFilename.right = new FormAttachment( 100, 0 );
    wIncludeTransformNrInFilename.setLayoutData( fdIncludeTransformNrInFilename );
    wIncludeTransformNrInFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Include Part Nr in filename line
    //
    wlIncludePartNrInFilename = new Label( gOutputFile, SWT.RIGHT );
    wlIncludePartNrInFilename.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.IncludePartnr.Label" ) );
    props.setLook( wlIncludePartNrInFilename );
    fdlIncludePartNrInFilename = new FormData();
    fdlIncludePartNrInFilename.left = new FormAttachment( 0, 0 );
    fdlIncludePartNrInFilename.top = new FormAttachment( wIncludeTransformNrInFilename, margin );
    fdlIncludePartNrInFilename.right = new FormAttachment( middle, -margin );
    wlIncludePartNrInFilename.setLayoutData( fdlIncludePartNrInFilename );

    wIncludePartNrInFilename = new Button( gOutputFile, SWT.CHECK );
    props.setLook( wIncludePartNrInFilename );
    fdIncludePartNrInFilename = new FormData();
    fdIncludePartNrInFilename.left = new FormAttachment( middle, 0 );
    fdIncludePartNrInFilename.top = new FormAttachment( wIncludeTransformNrInFilename, margin );
    fdIncludePartNrInFilename.right = new FormAttachment( 100, 0 );
    wIncludePartNrInFilename.setLayoutData( fdIncludePartNrInFilename );
    wIncludePartNrInFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Include Date in Output Filename
    //
    wlIncludeDateInFilename = new Label( gOutputFile, SWT.RIGHT );
    wlIncludeDateInFilename.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.IncludeDate.Label" ) );
    props.setLook( wlIncludeDateInFilename );
    fdlIncludeDateInFilename = new FormData();
    fdlIncludeDateInFilename.left = new FormAttachment( 0, 0 );
    fdlIncludeDateInFilename.top = new FormAttachment( wIncludePartNrInFilename, margin );
    fdlIncludeDateInFilename.right = new FormAttachment( middle, -margin );
    wlIncludeDateInFilename.setLayoutData( fdlIncludeDateInFilename );

    wIncludeDateInFilename = new Button( gOutputFile, SWT.CHECK );
    props.setLook( wIncludeDateInFilename );
    fdIncludeDateInFilename = new FormData();
    fdIncludeDateInFilename.left = new FormAttachment( middle, 0 );
    fdIncludeDateInFilename.top = new FormAttachment( wIncludePartNrInFilename, margin );
    fdIncludeDateInFilename.right = new FormAttachment( 100, 0 );
    wIncludeDateInFilename.setLayoutData( fdIncludeDateInFilename );
    wIncludeDateInFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Include time in filename
    //
    wlIncludeTimeInFilename = new Label( gOutputFile, SWT.RIGHT );
    wlIncludeTimeInFilename.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.IncludeTime.Label" ) );
    props.setLook( wlIncludeTimeInFilename );
    fdlIncludeTimeInFilename = new FormData();
    fdlIncludeTimeInFilename.left = new FormAttachment( 0, 0 );
    fdlIncludeTimeInFilename.top = new FormAttachment( wIncludeDateInFilename, margin );
    fdlIncludeTimeInFilename.right = new FormAttachment( middle, -margin );
    wlIncludeTimeInFilename.setLayoutData( fdlIncludeTimeInFilename );

    wIncludeTimeInFilename = new Button( gOutputFile, SWT.CHECK );
    props.setLook( wIncludeTimeInFilename );
    fdIncludeTimeInFilename = new FormData();
    fdIncludeTimeInFilename.left = new FormAttachment( middle, 0 );
    fdIncludeTimeInFilename.top = new FormAttachment( wIncludeDateInFilename, margin );
    fdIncludeTimeInFilename.right = new FormAttachment( 100, 0 );
    wIncludeTimeInFilename.setLayoutData( fdIncludeTimeInFilename );
    wIncludeTimeInFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    // Specify format
    //
    wlSpecifyDateFormat = new Label( gOutputFile, SWT.RIGHT );
    wlSpecifyDateFormat.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.SpecifyFormat.Label" ) );
    props.setLook( wlSpecifyDateFormat );
    fdlSpecifyDateFormat = new FormData();
    fdlSpecifyDateFormat.left = new FormAttachment( 0, 0 );
    fdlSpecifyDateFormat.top = new FormAttachment( wIncludeTimeInFilename, margin );
    fdlSpecifyDateFormat.right = new FormAttachment( middle, -margin );
    wlSpecifyDateFormat.setLayoutData( fdlSpecifyDateFormat );

    wSpecifyDateFormat = new Button( gOutputFile, SWT.CHECK );
    props.setLook( wSpecifyDateFormat );
    fdSpecifyDateFormat = new FormData();
    fdSpecifyDateFormat.left = new FormAttachment( middle, 0 );
    fdSpecifyDateFormat.top = new FormAttachment( wIncludeTimeInFilename, margin );
    fdSpecifyDateFormat.right = new FormAttachment( 100, 0 );
    wSpecifyDateFormat.setLayoutData( fdSpecifyDateFormat );
    wSpecifyDateFormat.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setSpecifyDateFormat();
      }
    } );

    // DateTimeFormat
    wlDateFormat = new Label( gOutputFile, SWT.RIGHT );
    wlDateFormat.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.DateTimeFormat.Label" ) );
    props.setLook( wlDateFormat );
    fdlDateFormat = new FormData();
    fdlDateFormat.left = new FormAttachment( 0, 0 );
    fdlDateFormat.top = new FormAttachment( wSpecifyDateFormat, margin );
    fdlDateFormat.right = new FormAttachment( middle, -margin );
    wlDateFormat.setLayoutData( fdlDateFormat );

    wDateFormat = new CCombo( gOutputFile, SWT.BORDER | SWT.READ_ONLY );
    wDateFormat.setEditable( true );
    props.setLook( wDateFormat );
    wDateFormat.addModifyListener( lsMod );
    fdDateFormat = new FormData();
    fdDateFormat.left = new FormAttachment( middle, 0 );
    fdDateFormat.top = new FormAttachment( wSpecifyDateFormat, margin );
    fdDateFormat.right = new FormAttachment( 75, 0 );
    wDateFormat.setLayoutData( fdDateFormat );
    String[] dats = Const.getDateFormats();
    for ( String dat : dats ) {
      wDateFormat.add( dat );
    }

    // Add Output Filename to Result Line
    //
    wlAddOutputFilenameToResult = new Label( gOutputFile, SWT.RIGHT );
    wlAddOutputFilenameToResult.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.AddOutputFilenameToResult.Label" ) );
    props.setLook( wlAddOutputFilenameToResult );
    fdlAddOutputFilenameToResult = new FormData();
    fdlAddOutputFilenameToResult.left = new FormAttachment( 0, 0 );
    fdlAddOutputFilenameToResult.top = new FormAttachment( wDateFormat, margin );
    fdlAddOutputFilenameToResult.right = new FormAttachment( middle, -margin );
    wlAddOutputFilenameToResult.setLayoutData( fdlAddOutputFilenameToResult );

    wAddOutputFilenameToResult = new Button( gOutputFile, SWT.CHECK );
    props.setLook( wAddOutputFilenameToResult );
    fdAddOutputFilenameToResult = new FormData();
    fdAddOutputFilenameToResult.left = new FormAttachment( middle, 0 );
    fdAddOutputFilenameToResult.top = new FormAttachment( wDateFormat, margin );
    fdAddOutputFilenameToResult.right = new FormAttachment( 100, 0 );
    wAddOutputFilenameToResult.setLayoutData( fdAddOutputFilenameToResult );
    wAddOutputFilenameToResult.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
      }
    } );

    fdgOutputFile = new FormData();
    fdgOutputFile.left = new FormAttachment( 0, 0 );
    fdgOutputFile.right = new FormAttachment( 100, 0 );
    fdgOutputFile.top = new FormAttachment( wOutputType, margin * 2 );
    gOutputFile.setLayoutData( fdgOutputFile );

    fdOutputComp = new FormData();
    fdOutputComp.left = new FormAttachment( 0, 0 );
    fdOutputComp.top = new FormAttachment( 0, 0 );
    fdOutputComp.right = new FormAttachment( 100, 0 );
    fdOutputComp.bottom = new FormAttachment( 100, 0 );
    wOutputComp.setLayoutData( fdOutputComp );

    wOutputComp.layout();
    wOutputTab.setControl( wOutputComp );

    // ///////////////////////////////////////////////////////////
    // / END OF FILE TAB
    // ///////////////////////////////////////////////////////////

    // Token tab...
    //
    wTokensTab = new CTabItem( wTabFolder, SWT.NONE );
    wTokensTab.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.TokensTab.TabTitle" ) );

    FormLayout tokensLayout = new FormLayout();
    tokensLayout.marginWidth = Const.FORM_MARGIN;
    tokensLayout.marginHeight = Const.FORM_MARGIN;

    Composite wTokensComp = new Composite( wTabFolder, SWT.NONE );
    wTokensComp.setLayout( tokensLayout );
    props.setLook( wTokensComp );

    wGet = new Button( wTokensComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "System.Button.GetFields" ) );
    wGet.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.GetFields" ) );
    fdGet = new FormData();
    fdGet.right = new FormAttachment( 50, -margin );
    fdGet.bottom = new FormAttachment( 100, 0 );
    wGet.setLayoutData( fdGet );

    // Token Start String
    wlTokenStartString = new Label( wTokensComp, SWT.RIGHT );
    wlTokenStartString.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.TokenStartString.Label" ) );
    props.setLook( wlTokenStartString );
    fdlTokenStartString = new FormData();
    fdlTokenStartString.left = new FormAttachment( 0, 0 );
    fdlTokenStartString.top = new FormAttachment( 0, margin );
    fdlTokenStartString.right = new FormAttachment( middle, -margin );
    wlTokenStartString.setLayoutData( fdlTokenStartString );

    wTokenStartString = new TextVar( variables, wTokensComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTokenStartString );
    wTokenStartString.addModifyListener( lsMod );
    fdTokenStartString = new FormData();
    fdTokenStartString.left = new FormAttachment( middle, 0 );
    fdTokenStartString.top = new FormAttachment( 0, margin );
    fdTokenStartString.right = new FormAttachment( 100, -margin );
    wTokenStartString.setLayoutData( fdTokenStartString );

    // Token End String
    wlTokenEndString = new Label( wTokensComp, SWT.RIGHT );
    wlTokenEndString.setText( BaseMessages.getString( PKG, "TokenReplacementDialog.TokenEndString.Label" ) );
    props.setLook( wlTokenEndString );
    fdlTokenEndString = new FormData();
    fdlTokenEndString.left = new FormAttachment( 0, 0 );
    fdlTokenEndString.top = new FormAttachment( wTokenStartString, margin );
    fdlTokenEndString.right = new FormAttachment( middle, -margin );
    wlTokenEndString.setLayoutData( fdlTokenEndString );

    wTokenEndString = new TextVar( variables, wTokensComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTokenEndString );
    wTokenEndString.addModifyListener( lsMod );
    fdTokenEndString = new FormData();
    fdTokenEndString.left = new FormAttachment( middle, 0 );
    fdTokenEndString.top = new FormAttachment( wTokenStartString, margin );
    fdTokenEndString.right = new FormAttachment( 100, -margin );
    wTokenEndString.setLayoutData( fdTokenEndString );


    //Tokens table
    final int FieldsCols = 2;
    final int FieldsRows = input.getTokenReplacementFields().length;

    colinf = new ColumnInfo[FieldsCols];
    colinf[0] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "TokenReplacementDialog.TokenColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );
    colinf[1] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "TokenReplacementDialog.StreamColumn.Column" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );


    wFields =
      new TableView(
        variables, wTokensComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wTokenEndString, margin * 2 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, -margin );
    wFields.setLayoutData( fdFields );

    //
    // Search the fields in the background

    final Runnable runnable = new Runnable() {
      public void run() {
        TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
        if ( transformMeta != null ) {
          try {
            IRowMeta row = pipelineMeta.getPrevTransformFields( variables, transformMeta );

            // Remember these fields...
            for ( int i = 0; i < row.size(); i++ ) {
              inputFields.put( row.getValueMeta( i ).getName(), i );
            }
            setComboBoxes();
          } catch ( HopException e ) {
            logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
          }
        }
      }
    };
    new Thread( runnable ).start();

    fdTokensComp = new FormData();
    fdTokensComp.left = new FormAttachment( 0, 0 );
    fdTokensComp.top = new FormAttachment( 0, 0 );
    fdTokensComp.right = new FormAttachment( 100, 0 );
    fdTokensComp.bottom = new FormAttachment( 100, 0 );
    wTokensComp.setLayoutData( fdTokensComp );

    wTokensComp.layout();
    wTokensTab.setControl( wTokensComp );

    fdTabFolder = new FormData();
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
    lsOk = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        get();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOk.addListener( SWT.Selection, lsOk );
    wGet.addListener( SWT.Selection, lsGet );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    // Whenever something changes, set the tooltip to the expanded version:
    wInputFilename.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wInputFilename.setToolTipText( variables.resolve( wInputFilename.getText() ) );
      }
    } );

    // Whenever something changes, set the tooltip to the expanded version:
    wOutputFilename.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        wOutputFilename.setToolTipText( variables.resolve( wOutputFilename.getText() ) );
      }
    } );

    wbInputFilename.addSelectionListener( new SelectionAdapter() {
        public void widgetSelected( SelectionEvent e ) {
          FileDialog dialog = new FileDialog( shell, SWT.OPEN );
          dialog.setFilterExtensions( new String[] { "*" } );
          if ( wInputFilename.getText() != null ) {
            dialog.setFileName( variables.resolve( wInputFilename.getText() ) );
          }
          dialog.setFilterNames( new String[] {
            BaseMessages.getString( PKG, "System.FileType.AllFiles" ) } );
          if ( dialog.open() != null ) {
              wInputFilename.setText( dialog.getFilterPath()
                + System.getProperty( "file.separator" ) + dialog.getFileName() );
          }
        }
      } );

    wbOutputFilename.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.SAVE );
        dialog.setFilterExtensions( new String[] { "*" } );
        if ( wOutputFilename.getText() != null ) {
          dialog.setFileName( variables.resolve( wOutputFilename.getText() ) );
        }
        dialog.setFilterNames( new String[] {
          BaseMessages.getString( PKG, "System.FileType.AllFiles" ) } );
        if ( dialog.open() != null ) {
          wOutputFilename.setText( dialog.getFilterPath()
            + System.getProperty( "file.separator" ) + dialog.getFileName() );
        }
      }
    } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    lsResize = new Listener() {
      public void handleEvent( Event event ) {
        Point size = shell.getSize();
        wFields.setSize( size.x - 10, size.y - 50 );
        wFields.table.setSize( size.x - 10, size.y - 50 );
        wFields.redraw();
      }
    };
    shell.addListener( SWT.Resize, lsResize );

    wTabFolder.setSelection( 0 );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );
    updateInputType();
    updateOutputType();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void setEncodings() {
    // Encoding of the text file:
    if ( !gotEncodings ) {
      gotEncodings = true;

      wOutputFileEncoding.removeAll();
      List<Charset> values = new ArrayList<Charset>( Charset.availableCharsets().values() );
      for ( Charset charSet : values ) {
        wOutputFileEncoding.add( charSet.displayName() );
      }

      // Now select the default!
      String defEncoding = Const.getEnvironmentVariable( "file.encoding", "UTF-8" );
      int idx = Const.indexOfString( defEncoding, wOutputFileEncoding.getItems() );
      if ( idx >= 0 ) {
        wOutputFileEncoding.select( idx );
      }
    }
  }

  private void getFields() {
    if ( !gotPreviousFields ) {
      try {
        String inputFilenameField = wInputFilenameField.getText();
        String outputFilenameField = wOutputFilenameField.getText();
        String inputField = wInputField.getText();
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables,transformName );
        if ( r != null ) {
          wInputFilenameField.setItems( r.getFieldNames() );
          wOutputFilenameField.setItems( r.getFieldNames() );
          wInputField.setItems( r.getFieldNames() );
        }

        wInputFilenameField.setText( Const.NVL( inputFilenameField, "" ) );
        wOutputFilenameField.setText( Const.NVL( outputFilenameField, "" ) );
        wInputField.setText( Const.NVL( inputField, ""  ) );
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "TokenReplacementDialog.FailedToGetFields.DialogTitle" ),
          BaseMessages.getString( PKG, "TokenReplacementDialog.FailedToGetFields.DialogMessage" ), ke );
      }
      gotPreviousFields = true;
    }
  }

  protected void setSpecifyDateFormat()
  {
    if( wOutputType.getText().equalsIgnoreCase( "File" ) && !wOutputFilenameInField.getSelection() )
    {
      boolean specifyFormat = wSpecifyDateFormat.getSelection();
      wDateFormat.setEnabled( specifyFormat );
      wlDateFormat.setEnabled( specifyFormat );
      wIncludeDateInFilename.setEnabled( !specifyFormat );
      wlIncludeDateInFilename.setEnabled( !specifyFormat );
      wIncludeTimeInFilename.setEnabled( !specifyFormat );
      wlIncludeTimeInFilename.setEnabled( !specifyFormat );
    } else {
      wDateFormat.setEnabled( false );
      wlDateFormat.setEnabled( false );
      wIncludeDateInFilename.setEnabled( false );
      wlIncludeDateInFilename.setEnabled( false );
      wIncludeTimeInFilename.setEnabled( false );
      wlIncludeTimeInFilename.setEnabled( false );
    }
  }

  protected void setOutputFilenameInField() {
    if(wOutputType.getText().equalsIgnoreCase( "File" ) )
    {
      boolean outputFilenameInField = wOutputFilenameInField.getSelection();
      wOutputFilename.setEnabled( !outputFilenameInField );
      wlOutputFilename.setEnabled( !outputFilenameInField );
      wbOutputFilename.setEnabled( !outputFilenameInField );
      wOutputFilenameField.setEnabled( outputFilenameInField );
      wlOutputFilenameField.setEnabled( outputFilenameInField );
      wOutputSplitEvery.setEnabled( !outputFilenameInField );
      wlOutputSplitEvery.setEnabled( !outputFilenameInField );
      wIncludeTransformNrInFilename.setEnabled( !outputFilenameInField );
      wlIncludeTransformNrInFilename.setEnabled( !outputFilenameInField );
      wIncludePartNrInFilename.setEnabled( !outputFilenameInField );
      wlIncludePartNrInFilename.setEnabled( !outputFilenameInField );
      wSpecifyDateFormat.setEnabled( !outputFilenameInField );
      wlSpecifyDateFormat.setEnabled( !outputFilenameInField );
      setSpecifyDateFormat();
    } else {
      wOutputFilename.setEnabled( false );
      wlOutputFilename.setEnabled( false );
      wbOutputFilename.setEnabled( false );
      wOutputFilenameField.setEnabled( false );
      wlOutputFilenameField.setEnabled( false );
      wIncludeTransformNrInFilename.setEnabled( false );
      wlIncludeTransformNrInFilename.setEnabled( false );
      wIncludePartNrInFilename.setEnabled( false );
      wlIncludePartNrInFilename.setEnabled( false );
      wSpecifyDateFormat.setEnabled( false );
      wlSpecifyDateFormat.setEnabled( false );
      setSpecifyDateFormat();
    }
  }

  protected void setInputFilenameInField() {

    if( wInputType.getText().equalsIgnoreCase( "File" ) )
    {
      boolean inputFilenameInField = wInputFilenameInField.getSelection();
      wInputFilename.setEnabled( !inputFilenameInField );
      wlInputFilename.setEnabled( !inputFilenameInField );
      wbInputFilename.setEnabled( !inputFilenameInField );
      wInputFilenameField.setEnabled( inputFilenameInField );
      wlInputFilenameField.setEnabled( inputFilenameInField );
    } else {
      wInputFilename.setEnabled( false );
      wlInputFilename.setEnabled( false );
      wbInputFilename.setEnabled( false );
      wInputFilenameField.setEnabled( false );
      wlInputFilenameField.setEnabled( false );
    }
  }

  protected void updateInputType() {
    String inputType = wInputType.getText();

    boolean fieldType = inputType.equalsIgnoreCase( "Field" );
    boolean fileType = inputType.equalsIgnoreCase( "File" );
    boolean textType = inputType.equalsIgnoreCase( "Text" );

    wInputText.setEnabled( textType );
    wlInputText.setEnabled( textType );
    gInputText.setVisible( textType );

    wInputField.setEnabled( fieldType );
    wlInputField.setEnabled( fieldType );
    gInputField.setVisible( fieldType );

    wInputFilenameInField.setEnabled( fileType );
    wlInputFilenameInField.setEnabled( fileType );
    setInputFilenameInField();

    wAddInputFilenameToResult.setEnabled( fileType );
    wlAddInputFilenameToResult.setEnabled( fileType );
    gInputFile.setVisible( fileType );

  }

  protected void updateOutputType() {
    String outputType = wOutputType.getText();

    boolean fieldType = outputType.equalsIgnoreCase( "Field" );
    boolean fileType = outputType.equalsIgnoreCase( "File" );

    wOutputField.setEnabled( fieldType );
    wlOutputField.setEnabled( fieldType );
    gOutputField.setVisible( fieldType );

    wOutputFilename.setEnabled( fileType );
    wlOutputFilename.setEnabled( fileType );
    setOutputFilenameInField();
    wAppendOutputFilename.setEnabled( fileType );
    wlAppendOutputFilename.setEnabled( fileType );
    wCreateParentFolder.setEnabled( fileType );
    wlCreateParentFolder.setEnabled( fileType );
    wAddOutputFilenameToResult.setEnabled( fileType );
    wlAddOutputFilenameToResult.setEnabled( fileType );
    gOutputFile.setVisible( fileType );
  }

    protected void setComboBoxes() {
      // Something was changed in the row.
      //
      final Map<String, Integer> fields = new HashMap<String, Integer>();

      // Add the currentMeta fields...
      fields.putAll( inputFields );

      Set<String> keySet = fields.keySet();
      List<String> entries = new ArrayList<String>( keySet );

      String[] fieldNames = entries.toArray( new String[entries.size()] );

      Const.sortStrings( fieldNames );
      colinf[1].setComboValues( fieldNames );


  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wInputType.setText( Const.NVL( input.getInputType(), "" ) );
    wInputText.setText( Const.NVL( input.getInputText(), "" ) );
    wInputField.setText( Const.NVL( input.getInputFieldName(), "" ) );
    wInputFilename.setText( Const.NVL( input.getInputFileName(), "" ) );
    wInputFilenameInField.setSelection( input.isInputFileNameInField() );
    wInputFilenameField.setText( Const.NVL( input.getInputFileNameField(), "" ) );
    wAddInputFilenameToResult.setSelection( input.isAddInputFileNameToResult() );

    wOutputType.setText( Const.NVL( input.getOutputType(), "" ) );
    wOutputField.setText( Const.NVL( input.getOutputFieldName(), "" ) );
    wOutputFilename.setText( Const.NVL( input.getOutputFileName(), "" ) );
    wOutputFilenameInField.setSelection( input.isOutputFileNameInField() );
    wOutputFilenameField.setText( Const.NVL( input.getOutputFileNameField(), "" ) );
    wAppendOutputFilename.setSelection( input.isAppendOutputFileName() );
    wCreateParentFolder.setSelection( input.isCreateParentFolder() );
    wFormat.setText( TokenReplacementMeta.getOutputFileFormatDescription( input.getOutputFileFormat() ) );
    wOutputFileEncoding.setText( Const.NVL( input.getOutputFileEncoding(), "" ) );
    wOutputSplitEvery.setText( Integer.toString( input.getSplitEvery() , 10 ) );
    wIncludeTransformNrInFilename.setSelection( input.isIncludeTransformNrInOutputFileName() );
    wIncludePartNrInFilename.setSelection( input.isIncludePartNrInOutputFileName() );
    wIncludeDateInFilename.setSelection( input.isIncludeDateInOutputFileName() );
    wIncludeTimeInFilename.setSelection( input.isIncludeTimeInOutputFileName() );
    wSpecifyDateFormat.setSelection( input.isSpecifyDateFormatOutputFileName() );
    wDateFormat.setText( Const.NVL( input.getDateFormatOutputFileName(), "" ) );
    wAddOutputFilenameToResult.setSelection( input.isAddOutputFileNameToResult() );

    wTokenStartString.setText( Const.NVL( input.getTokenStartString(), "" ) );
    wTokenEndString.setText( Const.NVL( input.getTokenEndString(), ""  ) );

    logDebug( "getting fields info..." );

    for ( int i = 0; i < input.getTokenReplacementFields().length; i++ ) {
      TokenReplacementField field = input.getTokenReplacementFields()[i];

      TableItem item = wFields.table.getItem( i );
      if ( field.getName() != null ) {
        item.setText( 2, field.getName() );
      }
      if( field.getTokenName() != null ) {
    	item.setText( 1, field.getTokenName() );
      }
    }

    wFields.optWidth( true );

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;

    input.setChanged( backupChanged );

    dispose();
  }

  private void getInfo( TokenReplacementMeta tfoi ) {
    tfoi.setInputType( wInputType.getText() );
    tfoi.setInputText( wInputText.getText() );
    tfoi.setInputFieldName( wInputField.getText() );
    tfoi.setInputFileName( wInputFilename.getText() );
    tfoi.setInputFileNameInField( wInputFilenameInField.getSelection() );
    tfoi.setInputFileNameField( wInputFilenameField.getText() );
    tfoi.setAddInputFileNameToResult( wAddInputFilenameToResult.getSelection() );

    tfoi.setOutputType( wOutputType.getText() );
    tfoi.setOutputFieldName( wOutputField.getText() );
    tfoi.setOutputFileName( wOutputFilename.getText() );
    tfoi.setOutputFileNameInField( wOutputFilenameInField.getSelection() );
    tfoi.setOutputFileNameField( wOutputFilenameField.getText() );
    tfoi.setAppendOutputFileName( wAppendOutputFilename.getSelection() );
    tfoi.setCreateParentFolder( wCreateParentFolder.getSelection() );
    tfoi.setOutputFileFormat( TokenReplacementMeta.formatMapperLineTerminator[wFormat.getSelectionIndex()] );
    tfoi.setOutputFileEncoding( wOutputFileEncoding.getText() );
    tfoi.setSplitEvery( Const.toInt( wOutputSplitEvery.getText(), 0 ) );
    tfoi.setIncludeTransformNrInOutputFileName( wIncludeTransformNrInFilename.getSelection() );
    tfoi.setIncludePartNrInOutputFileName( wIncludePartNrInFilename.getSelection() );
    tfoi.setIncludeDateInOutputFileName( wIncludeDateInFilename.getSelection() );
    tfoi.setIncludeTimeInOutputFileName( wIncludeTimeInFilename.getSelection() );
    tfoi.setSpecifyDateFormatOutputFileName( wSpecifyDateFormat.getSelection() );
    tfoi.setDateFormatOutputFileName( wDateFormat.getText() );
    tfoi.setAddOutputFileNameToResult( wAddOutputFilenameToResult.getSelection() );

    tfoi.setTokenStartString( wTokenStartString.getText() );
    tfoi.setTokenEndString( wTokenEndString.getText() );

    int nrFields = wFields.nrNonEmpty();

    tfoi.allocate( nrFields );

    for ( int i = 0; i < nrFields; i++ ) {
      TokenReplacementField field = new TokenReplacementField();

      TableItem item = wFields.getNonEmpty( i );
      field.setName( item.getText( 2 ) );
      field.setTokenName( item.getText( 1 ) );
      //CHECKSTYLE:Indentation:OFF
      tfoi.getTokenReplacementFields()[i] = field;
    }
  }

  private void ok() {
    if ( Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo( input );

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName );
      if ( r != null ) {
        ITableItemInsertListener listener = new ITableItemInsertListener() {
          public boolean tableItemInserted( TableItem tableItem, IValueMeta v ) {

            return true;
          }
        };
        getFieldsFromPrevious( r, wFields, 2, new int[] { 2 }, new int[] { 1 }, listener );
      }
    } catch ( HopException ke ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Title" ), BaseMessages
        .getString( PKG, "System.Dialog.GetFieldsFailed.Message" ), ke );
    }

  }

  public void getFieldsFromPrevious( IRowMeta row, TableView tableView, int keyColumn,
		    int[] nameColumn, int[] tokenNameColumn, ITableItemInsertListener listener ) {
		    if ( row == null || row.size() == 0 ) {
		      return; // nothing to do
		    }

		    Table table = tableView.table;

		    // get a list of all the non-empty keys (names)
		    //
		    List<String> keys = new ArrayList<String>();
		    for ( int i = 0; i < table.getItemCount(); i++ ) {
		      TableItem tableItem = table.getItem( i );
		      String key = tableItem.getText( keyColumn );
		      if ( !Utils.isEmpty( key ) && keys.indexOf( key ) < 0 ) {
		        keys.add( key );
		      }
		    }

		    int choice = 0;

		    if ( keys.size() > 0 ) {
		      // Ask what we should do with the existing data in the transform.
		      //
		      MessageDialog md =
		        new MessageDialog( tableView.getShell(),
		          BaseMessages.getString( PKG, "BaseTransformDialog.GetFieldsChoice.Title" ), // "Warning!"
		          null,
		          BaseMessages.getString( PKG, "BaseTransformDialog.GetFieldsChoice.Message", "" + keys.size(), "" + row.size() ),
		          SWT.ICON_WARNING, new String[] {
		            BaseMessages.getString( PKG, "BaseTransformDialog.AddNew" ),
		            BaseMessages.getString( PKG, "BaseTransformDialog.Add" ),
		            BaseMessages.getString( PKG, "BaseTransformDialog.ClearAndAdd" ),
		            BaseMessages.getString( PKG, "BaseTransformDialog.Cancel" ), }, 0 );
		      MessageDialog.setDefaultImage( GuiResource.getInstance().getImageHop() );
		      int idx = md.open();
		      choice = idx & 0xFF;
		    }

		    if ( choice == 3 || choice == 255 ) {
		      return; // Cancel clicked
		    }

		    if ( choice == 2 ) {
		      tableView.clearAll( false );
		    }

		    for ( int i = 0; i < row.size(); i++ ) {
		      IValueMeta v = row.getValueMeta( i );

		      boolean add = true;

		      if ( choice == 0 ) { // hang on, see if it's not yet in the table view

		        if ( keys.indexOf( v.getName() ) >= 0 ) {
		          add = false;
		        }
		      }

		      if ( add ) {
		        TableItem tableItem = new TableItem( table, SWT.NONE );

            for ( int aNameColumn : nameColumn ) {
              tableItem.setText( aNameColumn, Const.NVL( v.getName(), "" ) );
            }

            for ( int aTokenNameColumn : tokenNameColumn ) {
              tableItem.setText( aTokenNameColumn, Const.NVL( v.getName(), "" ) );
            }

		        if ( listener != null ) {
		          if ( !listener.tableItemInserted( tableItem, v ) ) {
		            tableItem.dispose(); // remove it again
		          }
		        }
		      }
		    }
		    tableView.removeEmptyRows();
		    tableView.setRowNums();
		    tableView.optWidth( true );
		  }


}
