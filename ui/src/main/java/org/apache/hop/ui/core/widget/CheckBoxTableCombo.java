/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.core.widget;

import com.google.common.collect.Lists;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Arrays.stream;
import static java.util.Collections.sort;

public class CheckBoxTableCombo {

  private final PropsUi props;
  private final ModifyListener lsMod;
  private final PipelineMeta pipelineMeta;
  private final Composite parentComposite;
  private Map<String, String> dataMap;
  private final String columnOneName;
  private final String columnTwoName;
  private final String buttonName;
  private final String tableName;

  private TableView propertiesTable;
  private Button wCheckBox;
  private boolean isEnabled;

  public CheckBoxTableCombo( Composite parentComposite, PropsUi props, ModifyListener lsMod, PipelineMeta pipelineMeta,
                             Map<String, String> dataMap, String buttonName, String tableName,
                             String columnOneName, String columnTwoName, boolean isEnabled ) {
    checkNotNull( props );
    checkNotNull( parentComposite );
    checkNotNull( lsMod );
    checkNotNull( pipelineMeta );

    this.parentComposite = parentComposite;
    this.props = props;
    this.lsMod = lsMod;
    this.pipelineMeta = pipelineMeta;
    this.dataMap = dataMap;
    this.buttonName = buttonName;
    this.tableName = tableName;
    this.columnOneName = columnOneName;
    this.columnTwoName = columnTwoName;

    this.isEnabled = isEnabled;

    buildWidget();
  }

  public Map<String, String> getPropertiesData() {
    return tableToMap( propertiesTable );
  }

  public boolean getIsEnabled() {
    return wCheckBox.getSelection();
  }

  public void updateDataMap( Map<String, String> newDataMap ) {
    this.dataMap = newDataMap;
    populateSSLData();
  }

  private void buildWidget() {

    //this.setLayout( new FormLayout() );
    wCheckBox = new Button( parentComposite, SWT.CHECK );
    wCheckBox.setText( buttonName );
    props.setLook( wCheckBox );
    FormData fdUseSSL = new FormData();
    fdUseSSL.top = new FormAttachment( parentComposite, 0 );
    fdUseSSL.left = new FormAttachment( 0, 0 );
    wCheckBox.setLayoutData( fdUseSSL );
    wCheckBox.addSelectionListener( new SelectionListener() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        boolean selection = ( (Button) selectionEvent.getSource() ).getSelection();
        propertiesTable.setEnabled( selection );
        propertiesTable.table.setEnabled( selection );
      }

      @Override public void widgetDefaultSelected( SelectionEvent selectionEvent ) {
        boolean selection = ( (Button) selectionEvent.getSource() ).getSelection();
        propertiesTable.setEnabled( selection );
        propertiesTable.table.setEnabled( selection );
      }
    } );

    Label wlSSLProperties = new Label( parentComposite, SWT.LEFT );
    wlSSLProperties.setText( tableName );
    props.setLook( wlSSLProperties );
    FormData fdlSSLProperties = new FormData();
    fdlSSLProperties.top = new FormAttachment( wCheckBox, 10 );
    fdlSSLProperties.left = new FormAttachment( 0, 0 );
    wlSSLProperties.setLayoutData( fdlSSLProperties );

    buildSSLTable( parentComposite, wlSSLProperties );

    setUIText();
  }

  private void setUIText() {
    wCheckBox.setSelection( isEnabled );
    propertiesTable.setEnabled( isEnabled );
    propertiesTable.table.setEnabled( isEnabled );

    propertiesTable.table.select( 0 );
    propertiesTable.table.showSelection();
  }

  private void buildSSLTable( Composite parentWidget, Control relativePosition ) {
    ColumnInfo[] columns = getSSLColumns();

    propertiesTable = new TableView(
      pipelineMeta,
      parentWidget,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
      columns,
      0,  // num of starting rows (will be added later)
      true,
      lsMod,
      props,
      false
    );

    propertiesTable.setSortable( false );
    propertiesTable.getTable().addListener( SWT.Resize, event -> {
      Table table = (Table) event.widget;
      table.getColumn( 1 ).setWidth( 200 );
      table.getColumn( 2 ).setWidth( 200 );
    } );

    populateSSLData();

    FormData fdData = new FormData();
    fdData.left = new FormAttachment( 0, 0 );
    fdData.top = new FormAttachment( relativePosition, 5 );
    fdData.bottom = new FormAttachment( 100, 0 );
    fdData.width = 800;

    // resize the columns to fit the data in them
    stream( propertiesTable.getTable().getColumns() ).forEach( column -> {
      if ( column.getWidth() > 0 ) {
        // don't pack anything with a 0 width, it will resize it to make it visible (like the index column)
        column.setWidth( 200 );
      }
    } );

    propertiesTable.setLayoutData( fdData );
  }

  private ColumnInfo[] getSSLColumns() {
    ColumnInfo optionName = new ColumnInfo( columnOneName, ColumnInfo.COLUMN_TYPE_TEXT, false, true );
    ColumnInfo value = new ColumnInfo( columnTwoName, ColumnInfo.COLUMN_TYPE_TEXT, false, false, 200 );

    value.setUsingVariables( true );

    return new ColumnInfo[] { optionName, value };
  }

  private void populateSSLData() {
    propertiesTable.getTable().removeAll();
    new TableItem( propertiesTable.getTable(), SWT.NONE );

    checkNotNull( propertiesTable.getItem( 0 ) );
    checkState( propertiesTable.getItem( 0 ).length == 2 );

    if ( dataMap.size() == 0 ) {
      //no data initialized
      return;
    }
    List<String> keys = Lists.newArrayList( dataMap.keySet() );
    sort( keys );
    String firstKey = keys.remove( 0 );
    propertiesTable.getTable().getItem( 0 ).setText( 1, firstKey );
    propertiesTable.getTable().getItem( 0 ).setText( 2, dataMap.get( firstKey ) );

    keys.stream()
      .forEach( key -> propertiesTable.add( key, dataMap.get( key ) ) );
  }

  private Map<String, String> tableToMap( TableView table ) {
    return IntStream.range( 0, table.getItemCount() )
      .mapToObj( table::getItem )
      .collect( Collectors.toMap( strArray -> strArray[ 0 ], strArray -> strArray[ 1 ] ) );
  }
}
