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

package org.apache.hop.pipeline.transforms.selectvalues;


import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.nio.charset.Charset;
import java.util.List;
import java.util.*;
import org.apache.hop.core.variables.IVariables;

public class SelectValuesDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = SelectValuesMeta.class; // For Translator

  private CTabFolder wTabFolder;

  private TableView wFields;

  private Button wUnspecified;

  private TableView wRemove;

  private TableView wMeta;

  private final SelectValuesMeta input;

  private final List<ColumnInfo> fieldColumns = new ArrayList<>();

  private String[] charsets = null;

  /**
   * Fields from previous transform
   */
  private IRowMeta prevFields;

  /**
   * Previous fields are read asynchonous because this might take some time and the user is able to do other things,
   * where he will not need the previous fields
   */
  private boolean bPreviousFieldsLoaded = false;

  private final Map<String, Integer> inputFields;

  public SelectValuesDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (SelectValuesMeta) in;
    inputFields = new HashMap<>();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    SelectionListener lsSel = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
      }
    };

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    lsGet = e -> get();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "SelectValuesDialog.Shell.Label" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "SelectValuesDialog.TransformName.Label" ) );
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

    // Buttons go at the bottom.  The tabs in between
    //
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e->ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e->cancel() );

    setButtonPositions( new Button[] { wOk, wCancel }, margin, null ); // null means bottom of dialog

    // The folders!
    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // ////////////////////////
    // START OF SELECT TAB ///
    // ////////////////////////

    CTabItem wSelectTab = new CTabItem(wTabFolder, SWT.NONE);
    wSelectTab.setText( BaseMessages.getString( PKG, "SelectValuesDialog.SelectTab.TabItem" ) );

    Composite wSelectComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wSelectComp);

    FormLayout selectLayout = new FormLayout();
    selectLayout.marginWidth = margin;
    selectLayout.marginHeight = margin;
    wSelectComp.setLayout( selectLayout );

    Label wlUnspecified = new Label(wSelectComp, SWT.RIGHT);
    wlUnspecified.setText( BaseMessages.getString( PKG, "SelectValuesDialog.Unspecified.Label" ) );
    props.setLook(wlUnspecified);
    FormData fdlUnspecified = new FormData();
    fdlUnspecified.left = new FormAttachment( 0, 0 );
    fdlUnspecified.right = new FormAttachment( middle, 0 );
    fdlUnspecified.bottom = new FormAttachment( 100, 0 );
    wlUnspecified.setLayoutData(fdlUnspecified);

    wUnspecified = new Button(wSelectComp, SWT.CHECK );
    props.setLook( wUnspecified );
    FormData fdUnspecified = new FormData();
    fdUnspecified.left = new FormAttachment( middle, margin );
    fdUnspecified.right = new FormAttachment( 100, 0 );
    fdUnspecified.bottom = new FormAttachment( wlUnspecified, 0, SWT.CENTER );
    wUnspecified.setLayoutData(fdUnspecified);
    wUnspecified.addSelectionListener( lsSel );

    Label wlFields = new Label(wSelectComp, SWT.NONE);
    wlFields.setText( BaseMessages.getString( PKG, "SelectValuesDialog.Fields.Label" ) );
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( 0, 0 );
    wlFields.setLayoutData(fdlFields);

    final int fieldsCols = 4;
    final int fieldsRows = input.getSelectFields().length;

    ColumnInfo[] colinf = new ColumnInfo[ fieldsCols ];
    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Fieldname" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { BaseMessages.getString(
        PKG, "SelectValuesDialog.ColumnInfo.Loading" ) }, false );
    colinf[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.RenameTo" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    colinf[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Length" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    colinf[ 3 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Precision" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );

    fieldColumns.add( colinf[ 0 ] );
    wFields =
      new TableView(
        variables, wSelectComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, fieldsRows, lsMod, props );

    Button wGetSelect = new Button(wSelectComp, SWT.PUSH);
    wGetSelect.setText( BaseMessages.getString( PKG, "SelectValuesDialog.GetSelect.Button" ) );
    wGetSelect.addListener( SWT.Selection, lsGet );
    FormData fdGetSelect = new FormData();
    fdGetSelect.right = new FormAttachment( 100, 0 );
    fdGetSelect.top = new FormAttachment(wlFields, margin );
    wGetSelect.setLayoutData(fdGetSelect);

    Button wDoMapping = new Button(wSelectComp, SWT.PUSH);
    wDoMapping.setText( BaseMessages.getString( PKG, "SelectValuesDialog.DoMapping.Button" ) );

    wDoMapping.addListener( SWT.Selection, arg0 -> generateMappings() );

    fdGetSelect = new FormData();
    fdGetSelect.right = new FormAttachment( 100, 0 );
    fdGetSelect.top = new FormAttachment(wGetSelect, 0 );
    wDoMapping.setLayoutData(fdGetSelect);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment(wlFields, margin );
    fdFields.right = new FormAttachment(wGetSelect, -margin );
    fdFields.bottom = new FormAttachment( wlUnspecified, -2*margin );
    wFields.setLayoutData(fdFields);

    FormData fdSelectComp = new FormData();
    fdSelectComp.left = new FormAttachment( 0, 0 );
    fdSelectComp.top = new FormAttachment( 0, 0 );
    fdSelectComp.right = new FormAttachment( 100, 0 );
    fdSelectComp.bottom = new FormAttachment( 100, 0 );
    wSelectComp.setLayoutData(fdSelectComp);

    wSelectComp.layout();
    wSelectTab.setControl(wSelectComp);

    // ///////////////////////////////////////////////////////////
    // / END OF SELECT TAB
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////////////////////////////////
    // START OF REMOVE TAB
    // ///////////////////////////////////////////////////////////
    CTabItem wRemoveTab = new CTabItem(wTabFolder, SWT.NONE);
    wRemoveTab.setText( BaseMessages.getString( PKG, "SelectValuesDialog.RemoveTab.TabItem" ) );

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = margin;
    contentLayout.marginHeight = margin;

    Composite wRemoveComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wRemoveComp);
    wRemoveComp.setLayout( contentLayout );

    Label wlRemove = new Label(wRemoveComp, SWT.NONE);
    wlRemove.setText( BaseMessages.getString( PKG, "SelectValuesDialog.Remove.Label" ) );
    props.setLook(wlRemove);
    FormData fdlRemove = new FormData();
    fdlRemove.left = new FormAttachment( 0, 0 );
    fdlRemove.top = new FormAttachment( 0, 0 );
    wlRemove.setLayoutData(fdlRemove);

    final int RemoveCols = 1;
    final int RemoveRows = input.getDeleteName().length;

    ColumnInfo[] colrem = new ColumnInfo[ RemoveCols ];
    colrem[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Fieldname" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { BaseMessages.getString(
        PKG, "SelectValuesDialog.ColumnInfo.Loading" ) }, false );
    fieldColumns.add( colrem[ 0 ] );
    wRemove =
      new TableView(
        variables, wRemoveComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colrem, RemoveRows, lsMod, props );

    Button wGetRemove = new Button(wRemoveComp, SWT.PUSH);
    wGetRemove.setText( BaseMessages.getString( PKG, "SelectValuesDialog.GetRemove.Button" ) );
    wGetRemove.addListener( SWT.Selection, lsGet );
    FormData fdGetRemove = new FormData();
    fdGetRemove.right = new FormAttachment( 100, 0 );
    fdGetRemove.top = new FormAttachment( 50, 0 );
    wGetRemove.setLayoutData(fdGetRemove);

    FormData fdRemove = new FormData();
    fdRemove.left = new FormAttachment( 0, 0 );
    fdRemove.top = new FormAttachment(wlRemove, margin );
    fdRemove.right = new FormAttachment(wGetRemove, -margin );
    fdRemove.bottom = new FormAttachment( 100, 0 );
    wRemove.setLayoutData(fdRemove);

    FormData fdRemoveComp = new FormData();
    fdRemoveComp.left = new FormAttachment( 0, 0 );
    fdRemoveComp.top = new FormAttachment( 0, 0 );
    fdRemoveComp.right = new FormAttachment( 100, 0 );
    fdRemoveComp.bottom = new FormAttachment( 100, 0 );
    wRemoveComp.setLayoutData(fdRemoveComp);

    wRemoveComp.layout();
    wRemoveTab.setControl(wRemoveComp);

    // ///////////////////////////////////////////////////////////
    // / END OF REMOVE TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF META TAB ///
    // ////////////////////////

    CTabItem wMetaTab = new CTabItem(wTabFolder, SWT.NONE);
    wMetaTab.setText( BaseMessages.getString( PKG, "SelectValuesDialog.MetaTab.TabItem" ) );

    Composite wMetaComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wMetaComp);

    FormLayout metaLayout = new FormLayout();
    metaLayout.marginWidth = margin;
    metaLayout.marginHeight = margin;
    wMetaComp.setLayout( metaLayout );

    Label wlMeta = new Label(wMetaComp, SWT.NONE);
    wlMeta.setText( BaseMessages.getString( PKG, "SelectValuesDialog.Meta.Label" ) );
    props.setLook(wlMeta);
    FormData fdlMeta = new FormData();
    fdlMeta.left = new FormAttachment( 0, 0 );
    fdlMeta.top = new FormAttachment( 0, 0 );
    wlMeta.setLayoutData(fdlMeta);

    final int MetaRows = input.getMeta().length;

    ColumnInfo[] colmeta =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Fieldname" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] { BaseMessages.getString(
          PKG, "SelectValuesDialog.ColumnInfo.Loading" ) }, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Renameto" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Type" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getAllValueMetaNames(), false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Length" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Precision" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Storage.Label" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
          new String[] {
            BaseMessages.getString( PKG, "System.Combo.Yes" ), BaseMessages.getString( PKG, "System.Combo.No" ), } ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Format" ),
          ColumnInfo.COLUMN_TYPE_FORMAT, 3 ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.DateLenient" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
          new String[] {
            BaseMessages.getString( PKG, "System.Combo.Yes" ), BaseMessages.getString( PKG, "System.Combo.No" ), } ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.DateFormatLocale" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, EnvUtil.getLocaleList() ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.DateFormatTimeZone" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, EnvUtil.getTimeZones() ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.LenientStringToNumber" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {
          BaseMessages.getString( PKG, "System.Combo.Yes" ), BaseMessages.getString( PKG, "System.Combo.No" ), } ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Encoding" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, getCharsets(), false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Decimal" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Grouping" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Currency" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ), };
    colmeta[ 5 ].setToolTip( BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Storage.Tooltip" ) );
    fieldColumns.add( colmeta[ 0 ] );
    wMeta =
      new TableView(
        variables, wMetaComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colmeta, MetaRows, lsMod, props );

    Button wGetMeta = new Button(wMetaComp, SWT.PUSH);
    wGetMeta.setText( BaseMessages.getString( PKG, "SelectValuesDialog.GetMeta.Button" ) );
    wGetMeta.addListener( SWT.Selection, lsGet );
    FormData fdGetMeta = new FormData();
    fdGetMeta.right = new FormAttachment( 100, 0 );
    fdGetMeta.top = new FormAttachment( 50, 0 );
    wGetMeta.setLayoutData(fdGetMeta);

    FormData fdMeta = new FormData();
    fdMeta.left = new FormAttachment( 0, 0 );
    fdMeta.top = new FormAttachment(wlMeta, margin );
    fdMeta.right = new FormAttachment(wGetMeta, -margin );
    fdMeta.bottom = new FormAttachment( 100, 0 );
    wMeta.setLayoutData(fdMeta);

    FormData fdMetaComp = new FormData();
    fdMetaComp.left = new FormAttachment( 0, 0 );
    fdMetaComp.top = new FormAttachment( 0, 0 );
    fdMetaComp.right = new FormAttachment( 100, 0 );
    fdMetaComp.bottom = new FormAttachment( 100, 0 );
    wMetaComp.setLayoutData(fdMetaComp);

    wMetaComp.layout();
    wMetaTab.setControl(wMetaComp);

    // ///////////////////////////////////////////////////////////
    // / END OF META TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
    wTabFolder.setLayoutData(fdTabFolder);

    // ///////////////////////////////////////////////////////////
    // / END OF TAB FOLDER
    // ///////////////////////////////////////////////////////////

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

    //
    // Search the fields in the background
    //
    final Runnable runnable = () -> {
      TransformMeta transformMeta = pipelineMeta.findTransform( transformName );
      if ( transformMeta != null ) {
        try {
          IRowMeta row = pipelineMeta.getPrevTransformFields( variables, transformMeta );
          prevFields = row;
          // Remember these fields...
          for ( int i = 0; i < row.size(); i++ ) {
            inputFields.put( row.getValueMeta( i ).getName(), i);
          }
          setComboBoxes();
        } catch ( HopException e ) {
          logError( BaseMessages.getString( PKG, "System.Dialog.GetFieldsFailed.Message" ) );
        }
      }
    };
    new Thread( runnable ).start();

    // Set the shell size, based upon previous time...
    //
    setSize();

    getData();
    input.setChanged( changed );

    setComboValues();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void setComboValues() {
    Runnable fieldLoader = () -> {
      try {
        prevFields = pipelineMeta.getPrevTransformFields( variables, transformName );
      } catch ( HopException e ) {
        prevFields = new RowMeta();
        String msg = BaseMessages.getString( PKG, "SelectValuesDialog.DoMapping.UnableToFindInput" );
        logError( msg );
      }
      String[] prevTransformFieldNames = prevFields != null ? prevFields.getFieldNames() : new String[ 0 ];
      Arrays.sort( prevTransformFieldNames );
      bPreviousFieldsLoaded = true;
      for (ColumnInfo colInfo : fieldColumns) {
        colInfo.setComboValues(prevTransformFieldNames);
      }
    };
    shell.getDisplay().asyncExec( fieldLoader );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wTabFolder.setSelection( 0 ); // Default

    /*
     * Select fields
     */
    if ( input.getSelectFields() != null && input.getSelectFields().length > 0 ) {
      for ( int i = 0; i < input.getSelectFields().length; i++ ) {
        TableItem item = wFields.table.getItem( i );
        if ( input.getSelectFields()[ i ].getName() != null ) {
          item.setText( 1, input.getSelectFields()[ i ].getName() );
        }
        if ( input.getSelectFields()[ i ].getRename() != null && !input.getSelectFields()[ i ].getRename().equals( input
          .getSelectFields()[ i ].getName() ) ) {
          item.setText( 2, input.getSelectFields()[ i ].getRename() );
        }
        item.setText( 3, input.getSelectFields()[ i ].getLength() < 0 ? "" : "" + input.getSelectFields()[ i ]
          .getLength() );
        item.setText( 4, input.getSelectFields()[ i ].getPrecision() < 0 ? "" : "" + input.getSelectFields()[ i ]
          .getPrecision() );
      }
      wFields.setRowNums();
      wFields.optWidth( true );
      wTabFolder.setSelection( 0 );
    }
    wUnspecified.setSelection( input.isSelectingAndSortingUnspecifiedFields() );

    /*
     * Remove certain fields...
     */
    if ( input.getDeleteName() != null && input.getDeleteName().length > 0 ) {
      for ( int i = 0; i < input.getDeleteName().length; i++ ) {
        TableItem item = wRemove.table.getItem( i );
        if ( input.getDeleteName()[ i ] != null ) {
          item.setText( 1, input.getDeleteName()[ i ] );
        }
      }
      wRemove.setRowNums();
      wRemove.optWidth( true );
      wTabFolder.setSelection( 1 );
    }

    /*
     * Change the meta-data of certain fields
     */
    if ( !Utils.isEmpty( input.getMeta() ) ) {
      for ( int i = 0; i < input.getMeta().length; i++ ) {
        SelectMetadataChange change = input.getMeta()[ i ];

        TableItem item = wMeta.table.getItem( i );
        int index = 1;
        item.setText( index++, Const.NVL( change.getName(), "" ) );
        if ( change.getRename() != null && !change.getRename().equals( change.getName() ) ) {
          item.setText( index++, change.getRename() );
        } else {
          index++;
        }
        item.setText( index++, ValueMetaFactory.getValueMetaName( change.getType() ) );
        item.setText( index++, change.getLength() < 0 ? "" : "" + change.getLength() );
        item.setText( index++, change.getPrecision() < 0 ? "" : "" + change.getPrecision() );
        item.setText( index++, change.getStorageType() == IValueMeta.STORAGE_TYPE_NORMAL ? BaseMessages
          .getString( PKG, "System.Combo.Yes" ) : BaseMessages.getString( PKG, "System.Combo.No" ) );
        item.setText( index++, Const.NVL( change.getConversionMask(), "" ) );
        item
          .setText( index++, change.isDateFormatLenient()
            ? BaseMessages.getString( PKG, "System.Combo.Yes" ) : BaseMessages.getString(
            PKG, "System.Combo.No" ) );
        item
          .setText( index++, change.getDateFormatLocale() == null ? "" : change.getDateFormatLocale().toString() );
        item.setText( index++, change.getDateFormatTimeZone() == null ? "" : change
          .getDateFormatTimeZone().toString() );
        item
          .setText( index++, change.isLenientStringToNumber()
            ? BaseMessages.getString( PKG, "System.Combo.Yes" ) : BaseMessages.getString(
            PKG, "System.Combo.No" ) );
        item.setText( index++, Const.NVL( change.getEncoding(), "" ) );
        item.setText( index++, Const.NVL( change.getDecimalSymbol(), "" ) );
        item.setText( index++, Const.NVL( change.getGroupingSymbol(), "" ) );
        item.setText( index++, Const.NVL( change.getCurrencySymbol(), "" ) );
      }
      wMeta.setRowNums();
      wMeta.optWidth( true );
      wTabFolder.setSelection( 2 );
    }

    wTransformName.setFocus();
    wTransformName.selectAll();
  }

  private String[] getCharsets() {
    if ( charsets == null ) {
      Collection<Charset> charsetCol = Charset.availableCharsets().values();
      charsets = new String[ charsetCol.size() ];
      int i = 0;
      for ( Charset charset : charsetCol ) {
        charsets[ i++ ] = charset.displayName();
      }
    }
    return charsets;
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

    // copy info to meta class (input)

    int nrFields = wFields.nrNonEmpty();
    int nrremove = wRemove.nrNonEmpty();
    int nrmeta = wMeta.nrNonEmpty();

    input.allocate( nrFields, nrremove, nrmeta );

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      input.getSelectFields()[ i ].setName( item.getText( 1 ) );
      input.getSelectFields()[ i ].setRename( item.getText( 2 ) );
      if ( input.getSelectFields()[ i ].getRename() == null || input.getSelectFields()[ i ].getName().length() == 0 ) {
        input.getSelectFields()[ i ].setRename( input.getSelectFields()[ i ].getName() );
      }
      input.getSelectFields()[ i ].setLength( Const.toInt( item.getText( 3 ), -2 ) );
      input.getSelectFields()[ i ].setPrecision( Const.toInt( item.getText( 4 ), -2 ) );

      if ( input.getSelectFields()[ i ].getLength() < -2 ) {
        input.getSelectFields()[ i ].setLength( -2 );
      }
      if ( input.getSelectFields()[ i ].getPrecision() < -2 ) {
        input.getSelectFields()[ i ].setPrecision( -2 );
      }
    }
    input.setSelectingAndSortingUnspecifiedFields( wUnspecified.getSelection() );

    for ( int i = 0; i < nrremove; i++ ) {
      TableItem item = wRemove.getNonEmpty( i );
      input.getDeleteName()[ i ] = item.getText( 1 );
    }

    for ( int i = 0; i < nrmeta; i++ ) {
      SelectMetadataChange change = new SelectMetadataChange();
      input.getMeta()[ i ] = change;

      TableItem item = wMeta.getNonEmpty( i );

      int index = 1;
      change.setName( item.getText( index++ ) );
      change.setRename( item.getText( index++ ) );
      if ( Utils.isEmpty( change.getRename() ) ) {
        change.setRename( change.getName() );
      }
      change.setType( ValueMetaFactory.getIdForValueMeta( item.getText( index++ ) ) );

      change.setLength( Const.toInt( item.getText( index++ ), -2 ) );
      change.setPrecision( Const.toInt( item.getText( index++ ), -2 ) );

      if ( change.getLength() < -2 ) {
        change.setLength( -2 );
      }
      if ( change.getPrecision() < -2 ) {
        change.setPrecision( -2 );
      }
      if ( BaseMessages.getString( PKG, "System.Combo.Yes" ).equalsIgnoreCase( item.getText( index++ ) ) ) {
        change.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
      }

      change.setConversionMask( item.getText( index++ ) );
      // If DateFormatLenient is anything but Yes (including blank) then it is false
      change.setDateFormatLenient( item.getText( index++ ).equalsIgnoreCase(
        BaseMessages.getString( PKG, "System.Combo.Yes" ) ) ? true : false );
      change.setDateFormatLocale( item.getText( index++ ) );
      change.setDateFormatTimeZone( item.getText( index++ ) );
      change.setLenientStringToNumber( item.getText( index++ ).equalsIgnoreCase(
        BaseMessages.getString( PKG, "System.Combo.Yes" ) ) ? true : false );
      change.setEncoding( item.getText( index++ ) );
      change.setDecimalSymbol( item.getText( index++ ) );
      change.setGroupingSymbol( item.getText( index++ ) );
      change.setCurrencySymbol( item.getText( index++ ) );
    }
    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields( variables, transformName );
      if ( r != null && !r.isEmpty() ) {
        switch ( wTabFolder.getSelectionIndex() ) {
          case 0:
            BaseTransformDialog.getFieldsFromPrevious( r, wFields, 1, new int[] { 1 }, new int[] {}, -1, -1, null );
            break;
          case 1:
            BaseTransformDialog.getFieldsFromPrevious( r, wRemove, 1, new int[] { 1 }, new int[] {}, -1, -1, null );
            break;
          case 2:
            BaseTransformDialog.getFieldsFromPrevious( r, wMeta, 1, new int[] { 1 }, new int[] {}, 4, 5, null );
            break;
          default:
            break;
        }
      }
    } catch ( HopException ke ) {
      new ErrorDialog(
        shell, BaseMessages.getString( PKG, "SelectValuesDialog.FailedToGetFields.DialogTitle" ), BaseMessages
        .getString( PKG, "SelectValuesDialog.FailedToGetFields.DialogMessage" ), ke );
    }
  }

  /**
   * Reads in the fields from the previous transforms and from the ONE next transform and opens an EnterMappingDialog with this
   * information. After the user did the mapping, those information is put into the Select/Rename table.
   */
  private void generateMappings() {
    if ( !bPreviousFieldsLoaded ) {
      MessageDialog.openError(
        shell, BaseMessages.getString( PKG, "SelectValuesDialog.ColumnInfo.Loading" ), BaseMessages.getString(
          PKG, "SelectValuesDialog.ColumnInfo.Loading" ) );
      return;
    }
    if ( ( wRemove.getItemCount() > 0 ) || ( wMeta.getItemCount() > 0 ) ) {
      for ( int i = 0; i < wRemove.getItemCount(); i++ ) {
        String[] columns = wRemove.getItem( i );
        for (String column : columns) {
          if (column.length() > 0) {
            MessageDialog.openError(shell, BaseMessages.getString(
                    PKG, "SelectValuesDialog.DoMapping.NoDeletOrMetaTitle"), BaseMessages.getString(
                    PKG, "SelectValuesDialog.DoMapping.NoDeletOrMeta"));
            return;
          }
        }
      }
      for ( int i = 0; i < wMeta.getItemCount(); i++ ) {
        String[] columns = wMeta.getItem( i );
        for (String col : columns) {
          if (col.length() > 0) {
            MessageDialog.openError(shell, BaseMessages.getString(
                    PKG, "SelectValuesDialog.DoMapping.NoDeletOrMetaTitle"), BaseMessages.getString(
                    PKG, "SelectValuesDialog.DoMapping.NoDeletOrMeta"));
            return;
          }
        }
      }
    }

    IRowMeta nextTransformRequiredFields = null;

    TransformMeta transformMeta = new TransformMeta( transformName, input );
    List<TransformMeta> nextTransforms = pipelineMeta.findNextTransforms( transformMeta );
    if ( nextTransforms.size() == 0 || nextTransforms.size() > 1 ) {
      MessageDialog.openError(
        shell, BaseMessages.getString( PKG, "SelectValuesDialog.DoMapping.NoNextTransformTitle" ), BaseMessages
          .getString( PKG, "SelectValuesDialog.DoMapping.NoNextTransform" ) );
      return;
    }
    TransformMeta outputTransformMeta = nextTransforms.get( 0 );
    ITransformMeta transformMetaInterface = outputTransformMeta.getTransform();
    try {
      nextTransformRequiredFields = transformMetaInterface.getRequiredFields( variables );
    } catch ( HopException e ) {
      logError( BaseMessages.getString( PKG, "SelectValuesDialog.DoMapping.UnableToFindOutput" ) );
      nextTransformRequiredFields = new RowMeta();
    }

    String[] inputNames = new String[ prevFields.size() ];
    for ( int i = 0; i < prevFields.size(); i++ ) {
      IValueMeta value = prevFields.getValueMeta( i );
      inputNames[ i ] = value.getName();
    }

    String[] outputNames = new String[ nextTransformRequiredFields.size() ];
    for ( int i = 0; i < nextTransformRequiredFields.size(); i++ ) {
      outputNames[ i ] = nextTransformRequiredFields.getValueMeta( i ).getName();
    }

    String[] selectName = new String[ wFields.getItemCount() ];
    String[] selectRename = new String[ wFields.getItemCount() ];
    for ( int i = 0; i < wFields.getItemCount(); i++ ) {
      selectName[ i ] = wFields.getItem( i, 1 );
      selectRename[ i ] = wFields.getItem( i, 2 );
    }

    List<SourceToTargetMapping> mappings = new ArrayList<>();
    StringBuilder missingFields = new StringBuilder();
    for ( int i = 0; i < selectName.length; i++ ) {
      String valueName = selectName[ i ];
      String valueRename = selectRename[ i ];
      int inIndex = prevFields.indexOfValue( valueName );
      if ( inIndex < 0 ) {
        missingFields.append( Const.CR + "   " + valueName + " --> " + valueRename );
        continue;
      }
      if ( null == valueRename || valueRename.equals( "" ) ) {
        valueRename = valueName;
      }
      int outIndex = nextTransformRequiredFields.indexOfValue( valueRename );
      if ( outIndex < 0 ) {
        missingFields.append( Const.CR + "   " + valueName + " --> " + valueRename );
        continue;
      }
      SourceToTargetMapping mapping = new SourceToTargetMapping( inIndex, outIndex );
      mappings.add( mapping );
    }
    // show a confirm dialog if some misconfiguration was found
    if ( missingFields.length() > 0 ) {
      MessageDialog.setDefaultImage( GuiResource.getInstance().getImageHopUi() );
      boolean goOn =
        MessageDialog.openConfirm( shell,
          BaseMessages.getString( PKG, "SelectValuesDialog.DoMapping.SomeFieldsNotFoundTitle" ),
          BaseMessages.getString( PKG, "SelectValuesDialog.DoMapping.SomeFieldsNotFound", missingFields.toString() ) );
      if ( !goOn ) {
        return;
      }
    }
    EnterMappingDialog d =
      new EnterMappingDialog( SelectValuesDialog.this.shell, inputNames, outputNames, mappings );
    mappings = d.open();

    // mappings == null if the user pressed cancel
    //
    if ( mappings != null ) {
      wFields.table.removeAll();
      wFields.table.setItemCount( mappings.size() );
      for ( int i = 0; i < mappings.size(); i++ ) {
        SourceToTargetMapping mapping = mappings.get( i );
        TableItem item = wFields.table.getItem( i );
        item.setText( 1, prevFields.getValueMeta( mapping.getSourcePosition() ).getName() );
        item.setText( 2, outputNames[ mapping.getTargetPosition() ] );
      }
      wFields.setRowNums();
      wFields.optWidth( true );
      wTabFolder.setSelection( 0 );
    }
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll( inputFields );

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>( keySet );

    String[] fieldNames = entries.toArray( new String[ entries.size() ] );

    Const.sortStrings( fieldNames );

    bPreviousFieldsLoaded = true;
    for (ColumnInfo colInfo : fieldColumns) {
      colInfo.setComboValues(fieldNames);
    }
  }
}
