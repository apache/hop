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

package org.apache.hop.pipeline.transforms.datagrid;


import org.apache.commons.collections4.CollectionUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class DataGridDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = DataGridMeta.class; // For Translator

  private CTabFolder wTabFolder;
  private Composite wDataComp;

  private TableView wFields;
  private TableView wData;

  private final DataGridMeta input;
  private final DataGridMeta dataGridMeta;
  private ModifyListener lsMod;

  public DataGridDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    input = (DataGridMeta) in;

    dataGridMeta = (DataGridMeta) input.clone();
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "DataGridDialog.DialogTitle" ) );

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Filename line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "System.Label.TransformName" ) );
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


    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "System.Button.Preview" ) );
    wPreview.addListener( SWT.Selection, e -> preview() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    setButtonPositions( new Button[] { wOk, wPreview, wCancel }, margin, null ); // At the very bottom

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    // //////////////////////
    // START OF META TAB ///
    // //////////////////////

    CTabItem wMetaTab = new CTabItem(wTabFolder, SWT.NONE);
    wMetaTab.setText( BaseMessages.getString( PKG, "DataGridDialog.Meta.Label" ) );

    Composite wMetaComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wMetaComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wMetaComp.setLayout( fileLayout );

    final int FieldsRows = input.getFieldName().length;

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "DataGridDialog.Name.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "DataGridDialog.Type.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
          ValueMetaFactory.getValueMetaNames() ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "DataGridDialog.Format.Column" ), ColumnInfo.COLUMN_TYPE_CCOMBO,
          Const.getDateFormats() ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "DataGridDialog.Length.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "DataGridDialog.Precision.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "DataGridDialog.Currency.Column" ), ColumnInfo.COLUMN_TYPE_TEXT,
          false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "DataGridDialog.Decimal.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "DataGridDialog.Group.Column" ), ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "DataGridDialog.Value.SetEmptyString" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, new String[] {
          BaseMessages.getString( PKG, "System.Combo.Yes" ),
          BaseMessages.getString( PKG, "System.Combo.No" ) } ),

      };

    wFields = new TableView( variables, wMetaComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( 0, 0 );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( 100, 0 );
    wFields.setLayoutData( fdFields );

    wMetaComp.layout();
    wMetaTab.setControl(wMetaComp);

    // //////////////////////
    // START OF DATA TAB ///
    // //////////////////////

    CTabItem wDataTab = new CTabItem(wTabFolder, SWT.NONE);
    wDataTab.setText( BaseMessages.getString( PKG, "DataGridDialog.Data.Label" ) );

    wDataComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wDataComp );

    FormLayout filesettingLayout = new FormLayout();
    filesettingLayout.marginWidth = 3;
    filesettingLayout.marginHeight = 3;
    wDataComp.setLayout( fileLayout );

    addDataGrid( false );

    FormData fdDataComp = new FormData();
    fdDataComp.left = new FormAttachment( 0, 0 );
    fdDataComp.top = new FormAttachment( 0, 0 );
    fdDataComp.right = new FormAttachment( 100, 0 );
    fdDataComp.bottom = new FormAttachment( 100, 0 );
    wDataComp.setLayoutData( fdDataComp );

    wDataComp.layout();
    wDataTab.setControl( wDataComp );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wTransformName, margin );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( wOk, -2*margin );
    wTabFolder.setLayoutData( fdTabFolder );


    lsDef = new SelectionAdapter() {
      @Override
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    lsResize = event -> {
      Point size = shell.getSize();
      wFields.setSize( size.x - 10, size.y - 50 );
      wFields.table.setSize( size.x - 10, size.y - 50 );
      wFields.redraw();
    };
    shell.addListener( SWT.Resize, lsResize );

    getData();
    wTabFolder.setSelection( 0 );

    wTabFolder.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent arg0 ) {
        addDataGrid( true );
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void addDataGrid( boolean refresh ) {

    if ( refresh ) {
      // retain changes made in the dialog...
      //
      getMetaInfo( dataGridMeta );
    }

    if ( refresh ) {
      // Retain the data edited in the dialog...
      //
      getDataInfo( dataGridMeta );

      // Clear out the data composite and redraw it completely...
      //
      for ( Control control : wDataComp.getChildren() ) {
        control.dispose();
      }
    }

    ColumnInfo[] columns = new ColumnInfo[ dataGridMeta.getFieldName().length ];
    for ( int i = 0; i < columns.length; i++ ) {
      columns[ i ] = new ColumnInfo( dataGridMeta.getFieldName()[ i ], ColumnInfo.COLUMN_TYPE_TEXT, false, false );
    }
    List<List<String>> lines = dataGridMeta.getDataLines();
    wData = new TableView( variables, wDataComp, SWT.NONE, columns, lines.size(), lsMod, props );
    wData.setSortable( false );

    for ( int i = 0; i < lines.size(); i++ ) {
      List<String> line = lines.get( i );
      TableItem item = wData.table.getItem( i );

      for ( int f = 0; f < line.size(); f++ ) {
        item.setText( f + 1, Const.NVL( line.get( f ), "" ) );
      }
    }

    wData.setRowNums();
    wData.optWidth( true );

    FormData fdData = new FormData();
    fdData.left = new FormAttachment( 0, 0 );
    fdData.top = new FormAttachment( 0, 0 );
    fdData.right = new FormAttachment( 100, 0 );
    fdData.bottom = new FormAttachment( 100, 0 );
    wData.setLayoutData( fdData );

    wTabFolder.layout( true, true );

    wFields.nrNonEmpty();
    wFields.setTableViewModifyListener( new TableView.ITableViewModifyListener() {

      private Integer getIdxByValue( List<Integer> list, Integer value ) {
        for ( int i = 0; i < list.size(); i++ ) {
          if ( list.get( i ).equals( value ) ) {
            return i;
          }
        }
        return null;
      }

      @Override
      public void moveRow( int position1, int position2 ) {
        //if one of rows is empty -- don't move data
        if ( !wFields.getNonEmptyIndexes().contains( position1 )
          || !wFields.getNonEmptyIndexes().contains( position2 ) ) {
          wFields.nrNonEmpty();
          return;
        }

        Integer fieldRealPosition1 = getIdxByValue( wFields.getNonEmptyIndexes(), position1 );
        Integer fieldRealPosition2 = getIdxByValue( wFields.getNonEmptyIndexes(), position2 );
        if ( fieldRealPosition1 == null || fieldRealPosition2 == null ) {
          return; //can not happen (prevent warnings)
        }
        //data table have one technical column
        int dataPosition1 = fieldRealPosition1 + 1;
        int dataPosition2 = fieldRealPosition2 + 1;

        for ( TableItem item : wData.table.getItems() ) {
          String value1 = item.getText( dataPosition1 );
          String value2 = item.getText( dataPosition2 );
          item.setText( dataPosition2, value1 );
          item.setText( dataPosition1, value2 );
        }
        wFields.nrNonEmpty();
      }

      @Override
      public void insertRow( int rowIndex ) {
        wFields.nrNonEmpty();
      }

      @Override
      public void cellFocusLost( int rowIndex ) {
        List<Integer> nonEmptyIndexesBeforeChanges = wFields.getNonEmptyIndexes();
        wFields.nrNonEmpty();
        List<Integer> nonEmptyIndexesAfterChanges = wFields.getNonEmptyIndexes();
        if ( CollectionUtils.isEqualCollection( nonEmptyIndexesBeforeChanges, nonEmptyIndexesAfterChanges ) ) {
          //count of fields rows didn't change
          return;
        }
        Collection<Integer> disjunction = CollectionUtils.disjunction( nonEmptyIndexesBeforeChanges, nonEmptyIndexesAfterChanges );
        Integer disjunctionIdx = (Integer) disjunction.toArray()[ 0 ];
        if ( nonEmptyIndexesAfterChanges.contains( disjunctionIdx ) ) {
          // new Field was added
          Integer idxByValue = getIdxByValue( nonEmptyIndexesAfterChanges, disjunctionIdx );
          if ( idxByValue == null ) {
            return; //can not happen (preventing warnings)
          }

          idxByValue++; //data table have one technical column
          TableColumn column = new TableColumn( wData.table, SWT.NONE, idxByValue );
          column.pack();
        } else {
          // Field was deleted
          Integer removeColumn = getIdxByValue( nonEmptyIndexesBeforeChanges, disjunctionIdx );
          if ( removeColumn == null ) {
            return; //can not happen (preventing warnings)
          }
          removeColumn++;  //data table have one technical column
          wData.table.getColumn( removeColumn ).dispose();
          wFields.nrNonEmpty();
        }
      }

      @Override
      public void delete( int[] items ) {
        for ( int index : items ) {
          if ( !wFields.getNonEmptyIndexes().contains( index ) ) {
            continue;
          }
          Integer removeColumn = getIdxByValue( wFields.getNonEmptyIndexes(), index );
          if ( removeColumn == null ) {
            return; //can not happen (preventing warnings)
          }
          removeColumn++;  //data table have one technical column
          wData.table.getColumn( removeColumn ).dispose();
        }
        wFields.nrNonEmpty();
      }
    } );

    wFields.setContentListener( modifyEvent -> wFields.nrNonEmpty() );
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    getMetaData();
    addDataGrid( false );
  }

  private void getMetaData() {
    int nrFields = input.getFieldName().length;
    if ( nrFields > wFields.table.getItemCount() ) {
      nrFields = wFields.table.getItemCount();
    }
    for ( int i = 0; i < nrFields; i++ ) {
      if ( input.getFieldName()[ i ] != null ) {
        TableItem item = wFields.table.getItem( i );
        int col = 1;

        item.setText( col++, input.getFieldName()[ i ] );
        String type = input.getFieldType()[ i ];
        String format = input.getFieldFormat()[ i ];
        String length = input.getFieldLength()[ i ] < 0 ? "" : ( "" + input.getFieldLength()[ i ] );
        String prec = input.getFieldPrecision()[ i ] < 0 ? "" : ( "" + input.getFieldPrecision()[ i ] );

        String curr = input.getCurrency()[ i ];
        String group = input.getGroup()[ i ];
        String decim = input.getDecimal()[ i ];

        item.setText( col++, Const.NVL( type, "" ) );
        item.setText( col++, Const.NVL( format, "" ) );
        item.setText( col++, Const.NVL( length, "" ) );
        item.setText( col++, Const.NVL( prec, "" ) );
        item.setText( col++, Const.NVL( curr, "" ) );
        item.setText( col++, Const.NVL( decim, "" ) );
        item.setText( col++, Const.NVL( group, "" ) );
        item
          .setText( col++, input.isSetEmptyString()[ i ]
            ? BaseMessages.getString( PKG, "System.Combo.Yes" ) : BaseMessages.getString(
            PKG, "System.Combo.No" ) );

      }
    }

    wFields.setRowNums();
    wFields.optWidth( true );

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

    getInfo( input );

    dispose();
  }

  private void getInfo( DataGridMeta meta ) {
    getMetaInfo( meta );
    getDataInfo( meta );
  }

  private void getMetaInfo( DataGridMeta meta ) {
    int nrFields = wFields.nrNonEmpty();

    meta.allocate( nrFields );

    //CHECKSTYLE:Indentation:OFF
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );
      int col = 1;
      meta.getFieldName()[ i ] = item.getText( col++ );
      meta.getFieldType()[ i ] = item.getText( col++ );
      meta.getFieldFormat()[ i ] = item.getText( col++ );
      String slength = item.getText( col++ );
      String sprec = item.getText( col++ );
      meta.getCurrency()[ i ] = item.getText( col++ );
      meta.getDecimal()[ i ] = item.getText( col++ );
      meta.getGroup()[ i ] = item.getText( col++ );

      try {
        meta.getFieldLength()[ i ] = Integer.parseInt( slength );
      } catch ( Exception e ) {
        meta.getFieldLength()[ i ] = -1;
      }
      try {
        meta.getFieldPrecision()[ i ] = Integer.parseInt( sprec );
      } catch ( Exception e ) {
        meta.getFieldPrecision()[ i ] = -1;
      }
      meta.isSetEmptyString()[ i ] =
        BaseMessages.getString( PKG, "System.Combo.Yes" ).equalsIgnoreCase( item.getText( col++ ) );

      if ( meta.isSetEmptyString()[ i ] ) {
        meta.getFieldType()[ i ] = "String";
      }
    }
  }

  private void getDataInfo( DataGridMeta meta ) {
    List<List<String>> data = new ArrayList<>();

    int nrLines = wData.table.getItemCount();
    int nrFields = meta.getFieldName().length;

    for ( int i = 0; i < nrLines; i++ ) {
      List<String> line = new ArrayList<>();
      TableItem item = wData.table.getItem( i );
      for ( int f = 0; f < nrFields; f++ ) {
        line.add( item.getText( f + 1 ) );
      }
      data.add( line );
    }

    meta.setDataLines( data );
  }

  /**
   * Preview the data generated by this transform. This generates a pipeline using this transform & a dummy and previews it.
   */
  private void preview() {
    // Create the table input reader transform...
    DataGridMeta oneMeta = new DataGridMeta();
    getInfo( oneMeta );

    PipelineMeta previewMeta = PipelinePreviewFactory.generatePreviewPipeline( variables, pipelineMeta.getMetadataProvider(),
      oneMeta, wTransformName.getText() );

    EnterNumberDialog numberDialog =
      new EnterNumberDialog( shell, props.getDefaultPreviewSize(), BaseMessages.getString(
        PKG, "DataGridDialog.EnterPreviewSize.Title" ), BaseMessages.getString(
        PKG, "DataGridDialog.EnterPreviewSize.Message" ) );
    int previewSize = numberDialog.open();
    if ( previewSize > 0 ) {
      PipelinePreviewProgressDialog progressDialog =
        new PipelinePreviewProgressDialog(
          shell, variables, previewMeta, new String[] { wTransformName.getText() }, new int[] { previewSize } );
      progressDialog.open();

      Pipeline pipeline = progressDialog.getPipeline();
      String loggingText = progressDialog.getLoggingText();

      if ( !progressDialog.isCancelled() ) {
        if ( pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0 ) {
          EnterTextDialog etd =
            new EnterTextDialog(
              shell, BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title" ), BaseMessages
              .getString( PKG, "System.Dialog.PreviewError.Message" ), loggingText, true );
          etd.setReadOnly();
          etd.open();
        }
      }

      PreviewRowsDialog prd =
        new PreviewRowsDialog(
          shell, variables, SWT.NONE, wTransformName.getText(), progressDialog.getPreviewRowsMeta( wTransformName
          .getText() ), progressDialog.getPreviewRows( wTransformName.getText() ), loggingText );
      prd.open();
    }
  }

}
