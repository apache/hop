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

package org.apache.hop.pipeline.transforms.fuzzymatch;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;

public class FuzzyMatchDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = FuzzyMatchMeta.class; // For Translator

  private CCombo wTransform;

  private CCombo wAlgorithm;

  private ComboVar wMainStreamField;

  private ComboVar wLookupField;

  private ColumnInfo[] ciReturn;
  private Label wlReturn;
  private TableView wReturn;

  private TextVar wMatchField;

  private Label wlValueField;
  private TextVar wValueField;

  private Label wlCaseSensitive;
  private Button wCaseSensitive;

  private Label wlGetCloserValue;
  private Button wGetCloserValue;

  private Label wlMinValue;
  private TextVar wMinValue;

  private Label wlMaxValue;
  private TextVar wMaxValue;

  private Label wlSeparator;
  private TextVar wSeparator;

  private Button wGetLU;

  private final FuzzyMatchMeta input;
  private boolean gotPreviousFields = false;
  private boolean gotLookupFields = false;

  public FuzzyMatchDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (FuzzyMatchMeta) in;
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    ModifyListener lsMod = e -> input.setChanged();
    SelectionListener lsSelection = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        input.setChanged();
        setComboBoxesLookup();
      }
    };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.Shell.Title" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.TransformName.Label" ) );
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
    // START OF General TAB ///
    // ////////////////////////
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.General.Tab" ) );

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralComp);

    FormLayout GeneralLayout = new FormLayout();
    GeneralLayout.marginWidth = 3;
    GeneralLayout.marginHeight = 3;
    wGeneralComp.setLayout( GeneralLayout );

    // /////////////////////////////////
    // START OF Lookup Fields GROUP
    // /////////////////////////////////

    Group wLookupGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wLookupGroup);
    wLookupGroup.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.Group.Lookup.Label" ) );

    FormLayout LookupgroupLayout = new FormLayout();
    LookupgroupLayout.marginWidth = 10;
    LookupgroupLayout.marginHeight = 10;
    wLookupGroup.setLayout( LookupgroupLayout );

    // Source transform line...
    Label wlTransform = new Label(wLookupGroup, SWT.RIGHT);
    wlTransform.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.SourceTransform.Label" ) );
    props.setLook(wlTransform);
    FormData fdlTransform = new FormData();
    fdlTransform.left = new FormAttachment( 0, 0 );
    fdlTransform.right = new FormAttachment( middle, -margin );
    fdlTransform.top = new FormAttachment( wTransformName, margin );
    wlTransform.setLayoutData(fdlTransform);
    wTransform = new CCombo(wLookupGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTransform );

    List<TransformMeta> transforms = pipelineMeta.findPreviousTransforms( pipelineMeta.findTransform( transformName ), true );
    for ( TransformMeta transformMeta : transforms ) {
      wTransform.add( transformMeta.getName() );
    }

    wTransform.addModifyListener( lsMod );
    wTransform.addSelectionListener( lsSelection );

    FormData fdTransform = new FormData();
    fdTransform.left = new FormAttachment( middle, 0 );
    fdTransform.top = new FormAttachment( wTransformName, margin );
    fdTransform.right = new FormAttachment( 100, 0 );
    wTransform.setLayoutData(fdTransform);

    // LookupField
    Label wlLookupField = new Label(wLookupGroup, SWT.RIGHT);
    wlLookupField.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.wlLookupField.Label" ) );
    props.setLook(wlLookupField);
    FormData fdlLookupField = new FormData();
    fdlLookupField.left = new FormAttachment( 0, 0 );
    fdlLookupField.top = new FormAttachment( wTransform, margin );
    fdlLookupField.right = new FormAttachment( middle, -2 * margin );
    wlLookupField.setLayoutData(fdlLookupField);

    wLookupField = new ComboVar( variables, wLookupGroup, SWT.BORDER | SWT.READ_ONLY );
    wLookupField.setEditable( true );
    props.setLook( wLookupField );
    wLookupField.addModifyListener( lsMod );
    FormData fdLookupField = new FormData();
    fdLookupField.left = new FormAttachment( middle, 0 );
    fdLookupField.top = new FormAttachment( wTransform, margin );
    fdLookupField.right = new FormAttachment( 100, -margin );
    wLookupField.setLayoutData(fdLookupField);
    wLookupField.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        setLookupField();
      }
    } );

    FormData fdLookupGroup = new FormData();
    fdLookupGroup.left = new FormAttachment( 0, margin );
    fdLookupGroup.top = new FormAttachment( wTransformName, margin );
    fdLookupGroup.right = new FormAttachment( 100, -margin );
    wLookupGroup.setLayoutData(fdLookupGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF Lookup GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF MainStream Fields GROUP
    // /////////////////////////////////

    Group wMainStreamGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wMainStreamGroup);
    wMainStreamGroup.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.Group.MainStreamGroup.Label" ) );

    FormLayout MainStreamgroupLayout = new FormLayout();
    MainStreamgroupLayout.marginWidth = 10;
    MainStreamgroupLayout.marginHeight = 10;
    wMainStreamGroup.setLayout( MainStreamgroupLayout );

    // MainStreamFieldname field
    Label wlMainStreamField = new Label(wMainStreamGroup, SWT.RIGHT);
    wlMainStreamField.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.wlMainStreamField.Label" ) );
    props.setLook(wlMainStreamField);
    FormData fdlMainStreamField = new FormData();
    fdlMainStreamField.left = new FormAttachment( 0, 0 );
    fdlMainStreamField.top = new FormAttachment(wLookupGroup, margin );
    fdlMainStreamField.right = new FormAttachment( middle, -2 * margin );
    wlMainStreamField.setLayoutData(fdlMainStreamField);

    wMainStreamField = new ComboVar( variables, wMainStreamGroup, SWT.BORDER | SWT.READ_ONLY );
    wMainStreamField.setEditable( true );
    props.setLook( wMainStreamField );
    wMainStreamField.addModifyListener( lsMod );
    FormData fdMainStreamField = new FormData();
    fdMainStreamField.left = new FormAttachment( middle, 0 );
    fdMainStreamField.top = new FormAttachment(wLookupGroup, margin );
    fdMainStreamField.right = new FormAttachment( 100, -margin );
    wMainStreamField.setLayoutData(fdMainStreamField);
    wMainStreamField.addFocusListener( new FocusListener() {
      public void focusLost( FocusEvent e ) {
      }

      public void focusGained( FocusEvent e ) {
        setMainStreamField();
      }
    } );

    FormData fdMainStreamGroup = new FormData();
    fdMainStreamGroup.left = new FormAttachment( 0, margin );
    fdMainStreamGroup.top = new FormAttachment(wLookupGroup, margin );
    fdMainStreamGroup.right = new FormAttachment( 100, -margin );
    wMainStreamGroup.setLayoutData(fdMainStreamGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF MainStream GROUP
    // ///////////////////////////////////////////////////////////

    // /////////////////////////////////
    // START OF Settings Fields GROUP
    // /////////////////////////////////

    Group wSettingsGroup = new Group(wGeneralComp, SWT.SHADOW_NONE);
    props.setLook(wSettingsGroup);
    wSettingsGroup.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.Group.SettingsGroup.Label" ) );

    FormLayout SettingsgroupLayout = new FormLayout();
    SettingsgroupLayout.marginWidth = 10;
    SettingsgroupLayout.marginHeight = 10;
    wSettingsGroup.setLayout( SettingsgroupLayout );

    // Algorithm
    Label wlAlgorithm = new Label(wSettingsGroup, SWT.RIGHT);
    wlAlgorithm.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.Algorithm.Label" ) );
    props.setLook(wlAlgorithm);
    FormData fdlAlgorithm = new FormData();
    fdlAlgorithm.left = new FormAttachment( 0, 0 );
    fdlAlgorithm.right = new FormAttachment( middle, -margin );
    fdlAlgorithm.top = new FormAttachment(wMainStreamGroup, margin );
    wlAlgorithm.setLayoutData(fdlAlgorithm);

    wAlgorithm = new CCombo(wSettingsGroup, SWT.BORDER | SWT.READ_ONLY );
    props.setLook( wAlgorithm );
    wAlgorithm.addModifyListener( lsMod );
    FormData fdAlgorithm = new FormData();
    fdAlgorithm.left = new FormAttachment( middle, 0 );
    fdAlgorithm.top = new FormAttachment(wMainStreamGroup, margin );
    fdAlgorithm.right = new FormAttachment( 100, -margin );
    wAlgorithm.setLayoutData(fdAlgorithm);
    wAlgorithm.setItems( FuzzyMatchMeta.algorithmDesc );
    wAlgorithm.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        activeAlgorithm();

      }
    } );

    // Is case sensitive
    wlCaseSensitive = new Label(wSettingsGroup, SWT.RIGHT );
    wlCaseSensitive.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.caseSensitive.Label" ) );
    props.setLook( wlCaseSensitive );
    FormData fdlcaseSensitive = new FormData();
    fdlcaseSensitive.left = new FormAttachment( 0, 0 );
    fdlcaseSensitive.top = new FormAttachment( wAlgorithm, margin );
    fdlcaseSensitive.right = new FormAttachment( middle, -2 * margin );
    wlCaseSensitive.setLayoutData(fdlcaseSensitive);

    wCaseSensitive = new Button(wSettingsGroup, SWT.CHECK );
    props.setLook( wCaseSensitive );
    wCaseSensitive.setToolTipText( BaseMessages.getString( PKG, "FuzzyMatchDialog.caseSensitive.Tooltip" ) );
    FormData fdcaseSensitive = new FormData();
    fdcaseSensitive.left = new FormAttachment( middle, 0 );
    fdcaseSensitive.top = new FormAttachment( wlCaseSensitive, 0, SWT.CENTER );
    wCaseSensitive.setLayoutData(fdcaseSensitive);
    SelectionAdapter lcaseSensitive = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };

    wCaseSensitive.addSelectionListener( lcaseSensitive );

    // Is get closer value
    wlGetCloserValue = new Label(wSettingsGroup, SWT.RIGHT );
    wlGetCloserValue.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.getCloserValue.Label" ) );
    props.setLook( wlGetCloserValue );
    FormData fdlgetCloserValue = new FormData();
    fdlgetCloserValue.left = new FormAttachment( 0, 0 );
    fdlgetCloserValue.top = new FormAttachment( wCaseSensitive, margin );
    fdlgetCloserValue.right = new FormAttachment( middle, -2 * margin );
    wlGetCloserValue.setLayoutData(fdlgetCloserValue);

    wGetCloserValue = new Button(wSettingsGroup, SWT.CHECK );
    props.setLook( wGetCloserValue );
    wGetCloserValue.setToolTipText( BaseMessages.getString( PKG, "FuzzyMatchDialog.getCloserValue.Tooltip" ) );
    FormData fdgetCloserValue = new FormData();
    fdgetCloserValue.left = new FormAttachment( middle, 0 );
    fdgetCloserValue.top = new FormAttachment( wlGetCloserValue, 0, SWT.CENTER );
    wGetCloserValue.setLayoutData(fdgetCloserValue);
    SelectionAdapter lgetCloserValue = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        activegetCloserValue();
        input.setChanged();
      }
    };
    wGetCloserValue.addSelectionListener( lgetCloserValue );

    wlMinValue = new Label(wSettingsGroup, SWT.RIGHT );
    wlMinValue.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.minValue.Label" ) );
    props.setLook( wlMinValue );
    FormData fdlminValue = new FormData();
    fdlminValue.left = new FormAttachment( 0, 0 );
    fdlminValue.top = new FormAttachment( wGetCloserValue, margin );
    fdlminValue.right = new FormAttachment( middle, -margin );
    wlMinValue.setLayoutData(fdlminValue);
    wMinValue = new TextVar( variables, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMinValue );
    wMinValue.setToolTipText( BaseMessages.getString( PKG, "FuzzyMatchDialog.minValue.Tooltip" ) );
    wMinValue.addModifyListener( lsMod );
    FormData fdminValue = new FormData();
    fdminValue.left = new FormAttachment( middle, 0 );
    fdminValue.top = new FormAttachment( wGetCloserValue, margin );
    fdminValue.right = new FormAttachment( 100, 0 );
    wMinValue.setLayoutData(fdminValue);

    wlMaxValue = new Label(wSettingsGroup, SWT.RIGHT );
    wlMaxValue.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.maxValue.Label" ) );
    props.setLook( wlMaxValue );
    FormData fdlmaxValue = new FormData();
    fdlmaxValue.left = new FormAttachment( 0, 0 );
    fdlmaxValue.top = new FormAttachment( wMinValue, margin );
    fdlmaxValue.right = new FormAttachment( middle, -margin );
    wlMaxValue.setLayoutData(fdlmaxValue);
    wMaxValue = new TextVar( variables, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaxValue );
    wMaxValue.setToolTipText( BaseMessages.getString( PKG, "FuzzyMatchDialog.maxValue.Tooltip" ) );
    wMaxValue.addModifyListener( lsMod );
    FormData fdmaxValue = new FormData();
    fdmaxValue.left = new FormAttachment( middle, 0 );
    fdmaxValue.top = new FormAttachment( wMinValue, margin );
    fdmaxValue.right = new FormAttachment( 100, 0 );
    wMaxValue.setLayoutData(fdmaxValue);

    wlSeparator = new Label(wSettingsGroup, SWT.RIGHT );
    wlSeparator.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.separator.Label" ) );
    props.setLook( wlSeparator );
    FormData fdlseparator = new FormData();
    fdlseparator.left = new FormAttachment( 0, 0 );
    fdlseparator.top = new FormAttachment( wMaxValue, margin );
    fdlseparator.right = new FormAttachment( middle, -margin );
    wlSeparator.setLayoutData(fdlseparator);
    wSeparator = new TextVar( variables, wSettingsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSeparator );
    wSeparator.addModifyListener( lsMod );
    FormData fdseparator = new FormData();
    fdseparator.left = new FormAttachment( middle, 0 );
    fdseparator.top = new FormAttachment( wMaxValue, margin );
    fdseparator.right = new FormAttachment( 100, 0 );
    wSeparator.setLayoutData(fdseparator);

    FormData fdSettingsGroup = new FormData();
    fdSettingsGroup.left = new FormAttachment( 0, margin );
    fdSettingsGroup.top = new FormAttachment(wMainStreamGroup, margin );
    fdSettingsGroup.right = new FormAttachment( 100, -margin );
    wSettingsGroup.setLayoutData(fdSettingsGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF Settings GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);

    // ///////////////////////////////////////////////////////////
    // / END OF General TAB
    // ///////////////////////////////////////////////////////////

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    setButtonPositions( new Button[] { wOk, wCancel }, margin, null );

    // ////////////////////////
    // START OF Fields TAB ///
    // ////////////////////////
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.Fields.Tab" ) );

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFieldsComp);

    FormLayout FieldsLayout = new FormLayout();
    FieldsLayout.marginWidth = 3;
    FieldsLayout.marginHeight = 3;
    wFieldsComp.setLayout( FieldsLayout );

    // /////////////////////////////////
    // START OF OutputFields Fields GROUP
    // /////////////////////////////////

    Group wOutputFieldsGroup = new Group(wFieldsComp, SWT.SHADOW_NONE);
    props.setLook(wOutputFieldsGroup);
    wOutputFieldsGroup.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.Group.OutputFieldsGroup.Label" ) );

    FormLayout OutputFieldsgroupLayout = new FormLayout();
    OutputFieldsgroupLayout.marginWidth = 10;
    OutputFieldsgroupLayout.marginHeight = 10;
    wOutputFieldsGroup.setLayout( OutputFieldsgroupLayout );

    Label wlmatchField = new Label(wOutputFieldsGroup, SWT.RIGHT);
    wlmatchField.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.MatchField.Label" ) );
    props.setLook(wlmatchField);
    FormData fdlmatchField = new FormData();
    fdlmatchField.left = new FormAttachment( 0, 0 );
    fdlmatchField.top = new FormAttachment(wSettingsGroup, margin );
    fdlmatchField.right = new FormAttachment( middle, -margin );
    wlmatchField.setLayoutData(fdlmatchField);
    wMatchField = new TextVar( variables, wOutputFieldsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMatchField );
    wMatchField.addModifyListener( lsMod );
    FormData fdmatchField = new FormData();
    fdmatchField.left = new FormAttachment( middle, 0 );
    fdmatchField.top = new FormAttachment(wSettingsGroup, margin );
    fdmatchField.right = new FormAttachment( 100, 0 );
    wMatchField.setLayoutData(fdmatchField);

    wlValueField = new Label(wOutputFieldsGroup, SWT.RIGHT );
    wlValueField.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.valueField.Label" ) );
    props.setLook( wlValueField );
    FormData fdlvalueField = new FormData();
    fdlvalueField.left = new FormAttachment( 0, 0 );
    fdlvalueField.top = new FormAttachment( wMatchField, margin );
    fdlvalueField.right = new FormAttachment( middle, -margin );
    wlValueField.setLayoutData(fdlvalueField);
    wValueField = new TextVar( variables, wOutputFieldsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wValueField );
    wValueField.setToolTipText( BaseMessages.getString( PKG, "FuzzyMatchDialog.valueField.Tooltip" ) );
    wValueField.addModifyListener( lsMod );
    FormData fdvalueField = new FormData();
    fdvalueField.left = new FormAttachment( middle, 0 );
    fdvalueField.top = new FormAttachment( wMatchField, margin );
    fdvalueField.right = new FormAttachment( 100, 0 );
    wValueField.setLayoutData(fdvalueField);

    FormData fdOutputFieldsGroup = new FormData();
    fdOutputFieldsGroup.left = new FormAttachment( 0, margin );
    fdOutputFieldsGroup.top = new FormAttachment(wSettingsGroup, margin );
    fdOutputFieldsGroup.right = new FormAttachment( 100, -margin );
    wOutputFieldsGroup.setLayoutData(fdOutputFieldsGroup);

    // ///////////////////////////////////////////////////////////
    // / END OF OutputFields GROUP
    // ///////////////////////////////////////////////////////////

    // THE UPDATE/INSERT TABLE
    wlReturn = new Label(wFieldsComp, SWT.NONE );
    wlReturn.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.ReturnFields.Label" ) );
    props.setLook( wlReturn );
    FormData fdlReturn = new FormData();
    fdlReturn.left = new FormAttachment( 0, 0 );
    fdlReturn.top = new FormAttachment(wOutputFieldsGroup, margin );
    wlReturn.setLayoutData(fdlReturn);

    wGetLU = new Button(wFieldsComp, SWT.PUSH );
    wGetLU.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.GetLookupFields.Button" ) );
    FormData fdlu = new FormData();
    fdlu.top = new FormAttachment( wlReturn, margin );
    fdlu.right = new FormAttachment( 100, 0 );
    wGetLU.setLayoutData( fdlu );

    int UpInsCols = 2;
    int UpInsRows = ( input.getValue() != null ? input.getValue().length : 1 );

    ciReturn = new ColumnInfo[ UpInsCols ];
    ciReturn[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "FuzzyMatchDialog.ColumnInfo.FieldReturn" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { "" }, false );
    ciReturn[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "FuzzyMatchDialog.ColumnInfo.NewName" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );

    wReturn =
      new TableView( variables, wFieldsComp, SWT.BORDER
        | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL, ciReturn, UpInsRows, lsMod, props );

    FormData fdReturn = new FormData();
    fdReturn.left = new FormAttachment( 0, 0 );
    fdReturn.top = new FormAttachment( wlReturn, margin );
    fdReturn.right = new FormAttachment( wGetLU, -margin );
    fdReturn.bottom = new FormAttachment( 100, -3 * margin );
    wReturn.setLayoutData(fdReturn);

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment( 0, 0 );
    fdFieldsComp.top = new FormAttachment( 0, 0 );
    fdFieldsComp.right = new FormAttachment( 100, 0 );
    fdFieldsComp.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Fields TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -margin );
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners
    lsOk = e -> ok();
    lsCancel = e -> cancel();
    Listener lsGetLU = e -> getlookup();

    wOk.addListener( SWT.Selection, lsOk );
    wCancel.addListener( SWT.Selection, lsCancel );
    wGetLU.addListener( SWT.Selection, lsGetLU);
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

    wTabFolder.setSelection( 0 );
    // Set the shell size, based upon previous time...
    setSize();

    getData();
    setComboBoxesLookup();
    activeAlgorithm();
    activegetCloserValue();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    if ( isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "FuzzyMatchDialog.Log.GettingKeyInfo" ) );
    }

    wAlgorithm.setText( FuzzyMatchMeta.getAlgorithmTypeDesc( input.getAlgorithmType() ) );

    if ( input.getMainStreamField() != null ) {
      wMainStreamField.setText( input.getMainStreamField() );
    }
    if ( input.getLookupField() != null ) {
      wLookupField.setText( input.getLookupField() );
    }
    wCaseSensitive.setSelection( input.isCaseSensitive() );
    wGetCloserValue.setSelection( input.isGetCloserValue() );
    if ( input.getMinimalValue() != null ) {
      wMinValue.setText( input.getMinimalValue() );
    }
    if ( input.getMaximalValue() != null ) {
      wMaxValue.setText( input.getMaximalValue() );
    }
    if ( input.getOutputMatchField() != null ) {
      wMatchField.setText( input.getOutputMatchField() );
    }
    if ( input.getOutputValueField() != null ) {
      wValueField.setText( input.getOutputValueField() );
    }
    if ( input.getSeparator() != null ) {
      wSeparator.setText( input.getSeparator() );
    }

    if ( input.getValue() != null ) {
      for ( int i = 0; i < input.getValue().length; i++ ) {
        TableItem item = wReturn.table.getItem( i );
        if ( input.getValue()[ i ] != null ) {
          item.setText( 1, input.getValue()[ i ] );
        }
        if ( input.getValueName()[ i ] != null && !input.getValueName()[ i ].equals( input.getValue()[ i ] ) ) {
          item.setText( 2, input.getValueName()[ i ] );
        }
      }
    }

    IStream infoStream = input.getTransformIOMeta().getInfoStreams().get( 0 );
    wTransform.setText( Const.NVL( infoStream.getTransformName(), "" ) );

    wReturn.setRowNums();
    wReturn.optWidth( true );

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

    input.setMainStreamField( wMainStreamField.getText() );
    input.setLookupField( wLookupField.getText() );

    input.setAlgorithmType( FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() ) );
    input.setCaseSensitive( wCaseSensitive.getSelection() );
    input.setGetCloserValue( wGetCloserValue.getSelection() );
    input.setMaximalValue( wMaxValue.getText() );
    input.setMinimalValue( wMinValue.getText() );

    input.setOutputMatchField( wMatchField.getText() );
    input.setOutputValueField( wValueField.getText() );
    input.setSeparator( wSeparator.getText() );

    int nrvalues = wReturn.nrNonEmpty();
    input.allocate( nrvalues );
    if ( isDebug() ) {
      logDebug( BaseMessages.getString( PKG, "FuzzyMatchDialog.Log.FoundFields", nrvalues + "" ) );
    }
    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrvalues; i++ ) {
      TableItem item = wReturn.getNonEmpty( i );
      input.getValue()[ i ] = item.getText( 1 );
      input.getValueName()[ i ] = item.getText( 2 );
      if ( input.getValueName()[ i ] == null || input.getValueName()[ i ].length() == 0 ) {
        input.getValueName()[ i ] = input.getValue()[ i ];
      }
    }

    IStream infoStream = input.getTransformIOMeta().getInfoStreams().get( 0 );
    infoStream.setTransformMeta( pipelineMeta.findTransform( wTransform.getText() ) );
    if ( infoStream.getTransformMeta() == null ) {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      if ( Utils.isEmpty( wTransform.getText() ) ) {
        mb.setMessage( BaseMessages.getString( PKG, "FuzzyMatchDialog.NotTransformSpecified.DialogMessage", wTransform
          .getText() ) );
      } else {
        mb.setMessage( BaseMessages.getString( PKG, "FuzzyMatchDialog.TransformCanNotFound.DialogMessage", wTransform
          .getText() ) );
      }

      mb.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.TransformCanNotFound.DialogTitle" ) );
      mb.open();
    }

    transformName = wTransformName.getText(); // return value

    dispose();
  }

  private void setMainStreamField() {
    if ( !gotPreviousFields ) {
      String field = wMainStreamField.getText();
      try {
        wMainStreamField.removeAll();

        IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
        if ( r != null ) {
          wMainStreamField.setItems( r.getFieldNames() );
        }
      } catch ( HopException ke ) {
        new ErrorDialog(
          shell, BaseMessages.getString( PKG, "FuzzyMatchDialog.FailedToGetFields.DialogTitle" ), BaseMessages
          .getString( PKG, "FuzzyMatchDialog.FailedToGetFields.DialogMessage" ), ke );
      }
      if ( field != null ) {
        wMainStreamField.setText( field );
      }
      gotPreviousFields = true;
    }
  }

  private void setLookupField() {
    if ( !gotLookupFields ) {
      String field = wLookupField.getText();
      try {
        wLookupField.removeAll();

        IRowMeta r = pipelineMeta.getTransformFields( variables, wTransform.getText() );
        if ( r != null ) {
          wLookupField.setItems( r.getFieldNames() );
        }
      } catch ( HopException ke ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "FuzzyMatchDialog.FailedToGetLookupFields.DialogTitle" ),
          BaseMessages.getString( PKG, "FuzzyMatchDialog.FailedToGetLookupFields.DialogMessage" ), ke );
      }
      if ( field != null ) {
        wLookupField.setText( field );
      }
      gotLookupFields = true;
    }
  }

  private void activegetCloserValue() {
    boolean enableRange =
      ( FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
        == FuzzyMatchMeta.OPERATION_TYPE_LEVENSHTEIN
        || FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
        == FuzzyMatchMeta.OPERATION_TYPE_NEEDLEMAN_WUNSH
        || FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
        == FuzzyMatchMeta.OPERATION_TYPE_DAMERAU_LEVENSHTEIN
        || FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
        == FuzzyMatchMeta.OPERATION_TYPE_JARO
        || FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
        == FuzzyMatchMeta.OPERATION_TYPE_JARO_WINKLER || FuzzyMatchMeta
        .getAlgorithmTypeByDesc( wAlgorithm.getText() ) == FuzzyMatchMeta.OPERATION_TYPE_PAIR_SIMILARITY )
        && !wGetCloserValue.getSelection();

    wlSeparator.setEnabled( enableRange );
    wSeparator.setEnabled( enableRange );
    wlValueField.setEnabled( wGetCloserValue.getSelection() );
    wValueField.setEnabled( wGetCloserValue.getSelection() );

    activeAddFields();
  }

  private void activeAddFields() {
    boolean activate =
      wGetCloserValue.getSelection()
        || ( FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
        == FuzzyMatchMeta.OPERATION_TYPE_DOUBLE_METAPHONE )
        || ( FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
        == FuzzyMatchMeta.OPERATION_TYPE_SOUNDEX )
        || ( FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
        == FuzzyMatchMeta.OPERATION_TYPE_REFINED_SOUNDEX )
        || ( FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
        == FuzzyMatchMeta.OPERATION_TYPE_METAPHONE );

    wlReturn.setEnabled( activate );
    wReturn.setEnabled( activate );
    wGetLU.setEnabled( activate );
  }

  private void activeAlgorithm() {
    boolean enable =
      ( FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
        == FuzzyMatchMeta.OPERATION_TYPE_LEVENSHTEIN
        || FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
        == FuzzyMatchMeta.OPERATION_TYPE_NEEDLEMAN_WUNSH
        || FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
        == FuzzyMatchMeta.OPERATION_TYPE_DAMERAU_LEVENSHTEIN
        || FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
        == FuzzyMatchMeta.OPERATION_TYPE_JARO
        || FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
        == FuzzyMatchMeta.OPERATION_TYPE_JARO_WINKLER || FuzzyMatchMeta
        .getAlgorithmTypeByDesc( wAlgorithm.getText() ) == FuzzyMatchMeta.OPERATION_TYPE_PAIR_SIMILARITY );

    wlGetCloserValue.setEnabled( enable );
    wGetCloserValue.setEnabled( enable );
    wlMinValue.setEnabled( enable );
    wMinValue.setEnabled( enable );
    wlMaxValue.setEnabled( enable );
    wMaxValue.setEnabled( enable );

    if ( FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
      == FuzzyMatchMeta.OPERATION_TYPE_JARO
      || FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
      == FuzzyMatchMeta.OPERATION_TYPE_JARO_WINKLER
      || FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
      == FuzzyMatchMeta.OPERATION_TYPE_PAIR_SIMILARITY ) {
      if ( Const.toDouble( variables.resolve( wMinValue.getText() ), 0 ) > 1 ) {
        wMinValue.setText( String.valueOf( 1 ) );
      }
      if ( Const.toDouble( variables.resolve( wMaxValue.getText() ), 0 ) > 1 ) {
        wMaxValue.setText( String.valueOf( 1 ) );
      }
    }

    boolean enableCaseSensitive =
      ( FuzzyMatchMeta.getAlgorithmTypeByDesc( wAlgorithm.getText() )
        == FuzzyMatchMeta.OPERATION_TYPE_LEVENSHTEIN || FuzzyMatchMeta
        .getAlgorithmTypeByDesc( wAlgorithm.getText() ) == FuzzyMatchMeta.OPERATION_TYPE_DAMERAU_LEVENSHTEIN );
    wlCaseSensitive.setEnabled( enableCaseSensitive );
    wCaseSensitive.setEnabled( enableCaseSensitive );
    activegetCloserValue();
  }

  private void getlookup() {
    try {
      String transformFrom = wTransform.getText();
      if ( !Utils.isEmpty( transformFrom ) ) {
        IRowMeta r = pipelineMeta.getTransformFields( variables, transformFrom );
        if ( r != null && !r.isEmpty() ) {
          BaseTransformDialog.getFieldsFromPrevious( r, wReturn, 1, new int[] { 1 }, new int[] { 4 }, -1, -1, null );
        } else {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "FuzzyMatchDialog.CouldNotFindFields.DialogMessage" ) );
          mb.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.CouldNotFindFields.DialogTitle" ) );
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
        mb.setMessage( BaseMessages.getString( PKG, "FuzzyMatchDialog.TransformNameRequired.DialogMessage" ) );
        mb.setText( BaseMessages.getString( PKG, "FuzzyMatchDialog.TransformNameRequired.DialogTitle" ) );
        mb.open();
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "FuzzyMatchDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "FuzzyMatchDialog.FailedToGetFields.DialogMessage" ), ke );
    }

  }

  protected void setComboBoxesLookup() {
    Runnable fieldLoader = () -> {
      TransformMeta lookupTransformMeta = pipelineMeta.findTransform( wTransform.getText() );
      if ( lookupTransformMeta != null ) {
        try {
          IRowMeta row = pipelineMeta.getTransformFields( variables, lookupTransformMeta );
          Map<String, Integer> lookupFields = new HashMap<>();
          // Remember these fields...
          for ( int i = 0; i < row.size(); i++ ) {
            lookupFields.put( row.getValueMeta( i ).getName(), i);
          }

          // Something was changed in the row.
          //
          final Map<String, Integer> fields = new HashMap<>();

          // Add the currentMeta fields...
          fields.putAll( lookupFields );

          Set<String> keySet = fields.keySet();
          List<String> entries = new ArrayList<>( keySet );

          String[] fieldNames = entries.toArray( new String[ entries.size() ] );
          Const.sortStrings( fieldNames );
          // return fields
          ciReturn[ 0 ].setComboValues( fieldNames );
        } catch ( HopException e ) {
          logError( "It was not possible to retrieve the list of fields for transform [" + wTransform.getText() + "]!" );
        }
      }
    };
    shell.getDisplay().asyncExec( fieldLoader );
  }
}
