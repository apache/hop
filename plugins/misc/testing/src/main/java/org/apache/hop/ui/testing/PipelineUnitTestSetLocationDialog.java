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

package org.apache.hop.ui.testing;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.testing.DataSet;
import org.apache.hop.testing.PipelineUnitTestFieldMapping;
import org.apache.hop.testing.PipelineUnitTestSetLocation;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PipelineUnitTestSetLocationDialog extends Dialog {
  private static final Class<?> PKG = PipelineUnitTestSetLocationDialog.class; // For Translator

  private PipelineUnitTestSetLocation location;
  private final List<DataSet> dataSets;
  private final Map<String, IRowMeta> transformFieldsMap;

  private String[] transformNames;
  private String[] datasetNames;

  private Shell shell;

  private Combo wTransformName;
  private Combo wDatasetName;
  private TableView wFieldMappings;
  private TableView wFieldOrder;

  private Button wOK;
  private Button wMapFields;
  private Button wGetSortFields;
  private Button wCancel;

  private PropsUi props;

  private int middle;
  private int margin;

  private boolean ok;

  public PipelineUnitTestSetLocationDialog( Shell parent, PipelineUnitTestSetLocation location, List<DataSet> dataSets, Map<String, IRowMeta> transformFieldsMap ) {
    super( parent, SWT.NONE );
    this.location = location;
    this.dataSets = dataSets;
    this.transformFieldsMap = transformFieldsMap;
    props = PropsUi.getInstance();
    ok = false;

    transformNames = transformFieldsMap.keySet().toArray( new String[ 0 ] );
    datasetNames = new String[ dataSets.size() ];
    for ( int i = 0; i < datasetNames.length; i++ ) {
      datasetNames[ i ] = dataSets.get( i ).getName();
    }
  }

  public boolean open() {
    Shell parent = getParent();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImageTable() );

    middle = props.getMiddlePct();
    margin = Const.MARGIN;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "PipelineUnitTestSetLocationDialog.Shell.Title" ) );
    shell.setLayout( formLayout );

    // Transform name
    //
    Label wlTransformName = new Label( shell, SWT.RIGHT );
    props.setLook( wlTransformName );
    wlTransformName.setText( BaseMessages.getString( PKG, "PipelineUnitTestSetLocationDialog.TransformName.Label" ) );
    FormData fdlTransformName = new FormData();
    fdlTransformName.top = new FormAttachment( 0, 0 );
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( middle, -margin );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setItems( transformNames );
    FormData fdTransformName = new FormData();
    fdTransformName.top = new FormAttachment( 0, 0 );
    fdTransformName.left = new FormAttachment( middle, 0 );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );
    Control lastControl = wTransformName;

    //
    //
    Label wlDatasetName = new Label( shell, SWT.RIGHT );
    props.setLook( wlDatasetName );
    wlDatasetName.setText( BaseMessages.getString( PKG, "PipelineUnitTestSetLocationDialog.DatasetName.Label" ) );
    FormData fdlDatasetName = new FormData();
    fdlDatasetName.top = new FormAttachment( lastControl, margin );
    fdlDatasetName.left = new FormAttachment( 0, 0 );
    fdlDatasetName.right = new FormAttachment( middle, -margin );
    wlDatasetName.setLayoutData( fdlDatasetName );
    wDatasetName = new Combo( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wDatasetName.setItems( datasetNames );
    FormData fdDatasetName = new FormData();
    fdDatasetName.top = new FormAttachment( lastControl, margin );
    fdDatasetName.left = new FormAttachment( middle, 0 );
    fdDatasetName.right = new FormAttachment( 100, 0 );
    wDatasetName.setLayoutData( fdDatasetName );
    lastControl = wDatasetName;

    // The field mapping from the transform to the data set...
    //
    Label wlFieldMapping = new Label( shell, SWT.LEFT );
    wlFieldMapping.setText( BaseMessages.getString( PKG, "PipelineUnitTestSetLocationDialog.FieldMapping.Label" ) );
    props.setLook( wlFieldMapping );
    FormData fdlFieldMapping = new FormData();
    fdlFieldMapping.left = new FormAttachment( 0, 0 );
    fdlFieldMapping.right = new FormAttachment( 60, -margin );
    fdlFieldMapping.top = new FormAttachment( lastControl, margin * 2 );
    wlFieldMapping.setLayoutData( fdlFieldMapping );

    Label wlFieldOrder = new Label( shell, SWT.LEFT );
    wlFieldOrder.setText( BaseMessages.getString( PKG, "PipelineUnitTestSetLocationDialog.FieldOrder.Label" ) );
    props.setLook( wlFieldOrder );
    FormData fdlFieldOrder = new FormData();
    fdlFieldOrder.left = new FormAttachment( 60, margin );
    fdlFieldOrder.right = new FormAttachment( 100, 0 );
    fdlFieldOrder.top = new FormAttachment( lastControl, margin * 2 );
    wlFieldOrder.setLayoutData( fdlFieldOrder );

    lastControl = wlFieldMapping;


    // Buttons at the bottom...
    //
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    wMapFields = new Button( shell, SWT.PUSH );
    wMapFields.setText( BaseMessages.getString( PKG, "PipelineUnitTestSetLocationDialog.MapFields.Button" ) );

    wGetSortFields = new Button( shell, SWT.PUSH );
    wGetSortFields.setText( BaseMessages.getString( PKG, "PipelineUnitTestSetLocationDialog.GetSortFields.Button" ) );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    Button[] buttons = new Button[] { wOK, wMapFields, wGetSortFields, wCancel };
    BaseTransformDialog.positionBottomButtons( shell, buttons, margin, null );


    // the field mapping grid in between on the left
    //
    ColumnInfo[] FieldMappingColumns = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "PipelineUnitTestSetLocationDialog.ColumnInfo.TransformField" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false ),
      new ColumnInfo( BaseMessages.getString( PKG, "PipelineUnitTestSetLocationDialog.ColumnInfo.DatasetField" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false ),
    };

    wFieldMappings = new TableView(
      new Variables(),
      shell,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
      FieldMappingColumns,
      location.getFieldMappings().size(),
      null, props );

    FormData fdFieldMapping = new FormData();
    fdFieldMapping.left = new FormAttachment( 0, 0 );
    fdFieldMapping.top = new FormAttachment( lastControl, margin );
    fdFieldMapping.right = new FormAttachment( 60, -margin );
    fdFieldMapping.bottom = new FormAttachment( wOK, -2 * margin );
    wFieldMappings.setLayoutData( fdFieldMapping );

    // the field mapping grid in between on the left
    //
    ColumnInfo[] FieldOrderColumns = new ColumnInfo[] {
      new ColumnInfo( BaseMessages.getString( PKG, "PipelineUnitTestSetLocationDialog.ColumnInfo.DatasetField" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false ),
    };

    wFieldOrder = new TableView(
      new Variables(),
      shell,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
      FieldOrderColumns,
      location.getFieldOrder().size(),
      null, props );

    FormData fdFieldOrder = new FormData();
    fdFieldOrder.left = new FormAttachment( 60, margin );
    fdFieldOrder.top = new FormAttachment( lastControl, margin );
    fdFieldOrder.right = new FormAttachment( 100, 0 );
    fdFieldOrder.bottom = new FormAttachment( wOK, -2 * margin );
    wFieldOrder.setLayoutData( fdFieldOrder );


    // Add listeners
    wOK.addListener( SWT.Selection, e -> ok() );
    wMapFields.addListener( SWT.Selection, e -> getFieldMappings() );
    wGetSortFields.addListener( SWT.Selection, e -> getSortFields() );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wTransformName.addSelectionListener( selAdapter );
    wDatasetName.addSelectionListener( selAdapter );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();
    Display display = parent.getDisplay();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return ok;
  }

  protected void getFieldMappings() {

    try {

      PipelineUnitTestSetLocation loc = new PipelineUnitTestSetLocation();
      getInfo( loc );

      String transformName = wTransformName.getText();
      String datasetName = wDatasetName.getText();
      if ( StringUtils.isEmpty( transformName ) || StringUtils.isEmpty( datasetName ) ) {
        throw new HopException( "Please select a transform and a data set to map fields between" );
      }

      IRowMeta transformRowMeta = transformFieldsMap.get( transformName );
      if ( transformRowMeta == null ) {
        throw new HopException( "Unable to find fields for transform " + transformName );
      }
      String[] transformFieldNames = transformRowMeta.getFieldNames();

      DataSet dataSet = findDataSet( datasetName );
      IRowMeta setRowMeta = dataSet.getSetRowMeta();
      String[] setFieldNames = setRowMeta.getFieldNames();

      // Get the current mappings...
      //
      List<SourceToTargetMapping> currentMappings = new ArrayList<>();
      for ( PipelineUnitTestFieldMapping mapping : loc.getFieldMappings() ) {
        int transformFieldIndex = transformRowMeta.indexOfValue( mapping.getTransformFieldName() );
        int setFieldIndex = transformRowMeta.indexOfValue( mapping.getDataSetFieldName() );
        if ( transformFieldIndex >= 0 && setFieldIndex >= 0 ) {
          currentMappings.add( new SourceToTargetMapping( transformFieldIndex, setFieldIndex ) );
        }
      }
      // Edit them
      //
      EnterMappingDialog mappingDialog = new EnterMappingDialog( shell, transformFieldNames, setFieldNames, currentMappings );
      List<SourceToTargetMapping> newMappings = mappingDialog.open();
      if ( newMappings != null ) {
        // Simply clean everything and add the new mappings
        //
        wFieldMappings.clearAll();
        for ( SourceToTargetMapping sourceToTargetMapping : newMappings ) {
          TableItem item = new TableItem( wFieldMappings.table, SWT.NONE );
          item.setText( 1, transformFieldNames[ sourceToTargetMapping.getSourcePosition() ] );
          item.setText( 2, setFieldNames[ sourceToTargetMapping.getTargetPosition() ] );
        }
        wFieldMappings.removeEmptyRows();
        wFieldMappings.setRowNums();
        wFieldMappings.optWidth( true );
      }
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error mapping fields from transform to dataset", e );
    }
  }

  private DataSet findDataSet( String datasetName ) throws HopException {
    for ( DataSet dataSet : dataSets ) {
      if ( dataSet.getName().equalsIgnoreCase( datasetName ) ) {
        return dataSet;
      }
    }
    throw new HopException( "Unable to find data set with name " + datasetName );
  }

  protected void getSortFields() {
    try {
      String datasetName = wDatasetName.getText();
      if ( StringUtils.isEmpty( datasetName ) ) {
        throw new HopException( "Please select a data set to get order fields from" );
      }

      DataSet dataSet = findDataSet( datasetName );
      IRowMeta setRowMeta = dataSet.getSetRowMeta();
      String[] setFieldNames = setRowMeta.getFieldNames();

      wFieldOrder.clearAll();
      for ( String setFieldName : setFieldNames ) {
        TableItem item = new TableItem( wFieldOrder.table, SWT.NONE );
        item.setText( 1, setFieldName );
      }
      wFieldOrder.removeEmptyRows();
      wFieldOrder.setRowNums();
      wFieldOrder.optWidth( true );

    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error getting sort fields", e );
    }
  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {

    wTransformName.setText( Const.NVL( location.getTransformName(), "" ) );
    wDatasetName.setText( Const.NVL( location.getDataSetName(), "" ) );

    for ( int i = 0; i < location.getFieldMappings().size(); i++ ) {
      PipelineUnitTestFieldMapping fieldMapping = location.getFieldMappings().get( i );
      int colnr = 1;
      wFieldMappings.setText( Const.NVL( fieldMapping.getTransformFieldName(), "" ), colnr++, i );
      wFieldMappings.setText( Const.NVL( fieldMapping.getDataSetFieldName(), "" ), colnr++, i );
    }
    wFieldMappings.removeEmptyRows();
    wFieldMappings.setRowNums();
    wFieldMappings.optWidth( true );

    for ( int i = 0; i < location.getFieldOrder().size(); i++ ) {
      String field = location.getFieldOrder().get( i );
      int colnr = 1;
      wFieldOrder.setText( Const.NVL( field, "" ), colnr++, i );
    }
    wFieldOrder.removeEmptyRows();
    wFieldOrder.setRowNums();
    wFieldOrder.optWidth( true );

    wTransformName.setFocus();
  }

  private void cancel() {
    ok = false;
    dispose();
  }

  /**
   * @param loc The data set to load the dialog information into
   */
  public void getInfo( PipelineUnitTestSetLocation loc ) {

    loc.setTransformName( wTransformName.getText() );
    loc.setDataSetName( wDatasetName.getText() );
    loc.getFieldMappings().clear();

    int nrMappings = wFieldMappings.nrNonEmpty();
    for ( int i = 0; i < nrMappings; i++ ) {
      TableItem item = wFieldMappings.getNonEmpty( i );
      int colnr = 1;
      String transformFieldName = item.getText( colnr++ );
      String dataSetFieldName = item.getText( colnr++ );
      loc.getFieldMappings().add( new PipelineUnitTestFieldMapping( transformFieldName, dataSetFieldName ) );
    }

    loc.getFieldOrder().clear();
    int nrFields = wFieldOrder.nrNonEmpty();
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wFieldOrder.getNonEmpty( i );
      int colnr = 1;
      String fieldname = item.getText( colnr++ );
      loc.getFieldOrder().add( fieldname );
    }
  }

  public void ok() {
    getInfo( location );
    ok = true;
    dispose();
  }
}
