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

package org.apache.hop.pipeline.transforms.kafka.producer;

import com.google.common.collect.ImmutableMap;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transforms.kafka.shared.KafkaDialogHelper;
import org.apache.hop.pipeline.transforms.kafka.shared.KafkaFactory;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class KafkaProducerOutputDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = KafkaProducerOutputMeta.class; // For Translator

  private static final ImmutableMap<String, String> DEFAULT_OPTION_VALUES = ImmutableMap.of( ProducerConfig.COMPRESSION_TYPE_CONFIG, "none" );

  private final KafkaFactory kafkaFactory = KafkaFactory.defaultFactory();

  private static final int SHELL_MIN_WIDTH = 527;
  private static final int SHELL_MIN_HEIGHT = 569;
  private static final int INPUT_WIDTH = 350;

  private final KafkaProducerOutputMeta meta;
  private ModifyListener lsMod;

  private TextVar wClientId;
  private ComboVar wTopic;
  private ComboVar wKeyField;
  private ComboVar wMessageField;
  private TableView optionsTable;
  private CTabFolder wTabFolder;

  private Label wlBootstrapServers;
  private TextVar wBootstrapServers;

  public KafkaProducerOutputDialog( Shell parent, IVariables variables, Object in,
                                    PipelineMeta pipelineMeta, String TransformName ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, TransformName );
    meta = (KafkaProducerOutputMeta) in;
  }

  @Override public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();
    changed = meta.hasChanged();

    lsMod = e -> meta.setChanged();
    lsCancel = e -> cancel();
    lsOk = e -> ok();
    lsDef = new SelectionAdapter() {
      @Override public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    setShellImage( shell, meta );
    shell.setMinimumSize( SHELL_MIN_WIDTH, SHELL_MIN_HEIGHT );
    shell.setText( BaseMessages.getString( PKG, "KafkaProducerOutputDialog.Shell.Title" ) );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;
    shell.setLayout( formLayout );
    shell.addShellListener( new ShellAdapter() {
      @Override public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );


    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "KafkaProducerOutputDialog.TransformName.Label" ) );
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.top = new FormAttachment( 0, 0 );
    wlTransformName.setLayoutData( fdlTransformName );

    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( lsMod );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( 0, 0 );
    fdTransformName.right = new FormAttachment(100, 0);
    fdTransformName.top = new FormAttachment( wlTransformName, 5 );
    wTransformName.setLayoutData( fdTransformName );
    wTransformName.addSelectionListener( lsDef );

    Label topSeparator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fdSpacer = new FormData();
    fdSpacer.height = 2;
    fdSpacer.left = new FormAttachment( 0, 0 );
    fdSpacer.top = new FormAttachment( wTransformName, 15 );
    fdSpacer.right = new FormAttachment( 100, 0 );
    topSeparator.setLayoutData( fdSpacer );

    // Start of tabbed display
    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setUnselectedCloseVisible( true );

    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, lsOk );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, lsCancel );
    positionBottomButtons( shell, new Button[] { wOk, wCancel }, props.getMargin(), null );

    Label bottomSeparator = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    props.setLook( bottomSeparator );
    FormData fdBottomSeparator = new FormData();
    fdBottomSeparator.height = 2;
    fdBottomSeparator.left = new FormAttachment( 0, 0 );
    fdBottomSeparator.bottom = new FormAttachment( wCancel, -15 );
    fdBottomSeparator.right = new FormAttachment( 100, 0 );
    bottomSeparator.setLayoutData( fdBottomSeparator );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( topSeparator, 15 );
    fdTabFolder.bottom = new FormAttachment( bottomSeparator, -15 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    wTabFolder.setLayoutData( fdTabFolder );

    buildSetupTab();
    buildOptionsTab();

    getData();

    setSize();

    meta.setChanged( changed );

    wTabFolder.setSelection( 0 );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void buildSetupTab() {
    CTabItem wSetupTab = new CTabItem( wTabFolder, SWT.NONE );
    wSetupTab.setText( BaseMessages.getString( PKG, "KafkaProducerOutputDialog.SetupTab" ) );

    Composite wSetupComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wSetupComp );
    FormLayout setupLayout = new FormLayout();
    setupLayout.marginHeight = 15;
    setupLayout.marginWidth = 15;
    wSetupComp.setLayout( setupLayout );

    wlBootstrapServers = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlBootstrapServers );
    wlBootstrapServers.setText( BaseMessages.getString( PKG, "KafkaProducerOutputDialog.BootstrapServers" ) );
    FormData fdlBootstrapServers = new FormData();
    fdlBootstrapServers.left = new FormAttachment( 0, 0 );
    fdlBootstrapServers.top = new FormAttachment( 0, 0 );
    fdlBootstrapServers.right = new FormAttachment( 100, 0 );
    wlBootstrapServers.setLayoutData( fdlBootstrapServers );

    wBootstrapServers = new TextVar( variables, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBootstrapServers );
    wBootstrapServers.addModifyListener( lsMod );
    FormData fdBootstrapServers = new FormData();
    fdBootstrapServers.left = new FormAttachment( 0, 0 );
    fdBootstrapServers.top = new FormAttachment( wlBootstrapServers, props.getMargin() );
    fdBootstrapServers.right = new FormAttachment( 100, 0 );
    wBootstrapServers.setLayoutData( fdBootstrapServers );

    Label wlClientId = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlClientId );
    wlClientId.setText( BaseMessages.getString( PKG, "KafkaProducerOutputDialog.ClientId" ) );
    FormData fdlClientId = new FormData();
    fdlClientId.left = new FormAttachment( 0, 0 );
    fdlClientId.top = new FormAttachment( wBootstrapServers, 2 * props.getMargin() );
    fdlClientId.right = new FormAttachment( 100, 0 );
    wlClientId.setLayoutData( fdlClientId );

    wClientId = new TextVar( variables, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wClientId );
    wClientId.addModifyListener( lsMod );
    FormData fdClientId = new FormData();
    fdClientId.left = new FormAttachment( 0, 0 );
    fdClientId.top = new FormAttachment( wlClientId, props.getMargin() );
    fdClientId.right = new FormAttachment( 100, 0 );
    wClientId.setLayoutData( fdClientId );

    Label wlTopic = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlTopic );
    wlTopic.setText( BaseMessages.getString( PKG, "KafkaProducerOutputDialog.Topic" ) );
    FormData fdlTopic = new FormData();
    fdlTopic.left = new FormAttachment( 0, 0 );
    fdlTopic.top = new FormAttachment( wClientId, 2 * props.getMargin() );
    fdlTopic.right = new FormAttachment( 100, 0 );
    wlTopic.setLayoutData( fdlTopic );

    wTopic = new ComboVar( variables, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTopic );
    wTopic.addModifyListener( lsMod );
    FormData fdTopic = new FormData();
    fdTopic.left = new FormAttachment( 0, 0 );
    fdTopic.top = new FormAttachment( wlTopic, props.getMargin() );
    fdTopic.right = new FormAttachment( 100, 0 ); // 60% of dialog width
    wTopic.setLayoutData( fdTopic );
    wTopic.getCComboWidget().addListener(
      SWT.FocusIn,
      event -> {
        KafkaDialogHelper kafkaDialogHelper = new KafkaDialogHelper( variables,
          wTopic, wBootstrapServers, kafkaFactory, optionsTable, meta.getParentTransformMeta() );
        kafkaDialogHelper.clusterNameChanged( event );
      } );

    Label wlKeyField = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlKeyField );
    wlKeyField.setText( BaseMessages.getString( PKG, "KafkaProducerOutputDialog.KeyField" ) );
    FormData fdlKeyField = new FormData();
    fdlKeyField.left = new FormAttachment( 0, 0 );
    fdlKeyField.top = new FormAttachment( wTopic, 2 * props.getMargin() );
    fdlKeyField.right = new FormAttachment( 100, 0 );
    wlKeyField.setLayoutData( fdlKeyField );

    wKeyField = new ComboVar( variables, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wKeyField );
    wKeyField.addModifyListener( lsMod );
    FormData fdKeyField = new FormData();
    fdKeyField.left = new FormAttachment( 0, 0 );
    fdKeyField.top = new FormAttachment( wlKeyField, props.getMargin() );
    fdKeyField.right = new FormAttachment( 100, 0 );
    wKeyField.setLayoutData( fdKeyField );
    Listener lsKeyFocus = e -> KafkaDialogHelper.populateFieldsList( variables, pipelineMeta, wKeyField, transformName );
    wKeyField.getCComboWidget().addListener( SWT.FocusIn, lsKeyFocus );

    Label wlMessageField = new Label( wSetupComp, SWT.LEFT );
    props.setLook( wlMessageField );
    wlMessageField.setText( BaseMessages.getString( PKG, "KafkaProducerOutputDialog.MessageField" ) );
    FormData fdlMessageField = new FormData();
    fdlMessageField.left = new FormAttachment( 0, 0 );
    fdlMessageField.top = new FormAttachment( wKeyField, 2 * props.getMargin() );
    fdlMessageField.right = new FormAttachment( 100, 0 );
    wlMessageField.setLayoutData( fdlMessageField );

    wMessageField = new ComboVar( variables, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMessageField );
    wMessageField.addModifyListener( lsMod );
    FormData fdMessageField = new FormData();
    fdMessageField.left = new FormAttachment( 0, 0 );
    fdMessageField.top = new FormAttachment( wlMessageField, props.getMargin() );
    fdMessageField.right = new FormAttachment( 100, 0 );
    wMessageField.setLayoutData( fdMessageField );
    Listener lsMessageFocus = e -> KafkaDialogHelper.populateFieldsList( variables, pipelineMeta, wMessageField, transformName );
    wMessageField.getCComboWidget().addListener( SWT.FocusIn, lsMessageFocus );

    FormData fdSetupComp = new FormData();
    fdSetupComp.left = new FormAttachment( 0, 0 );
    fdSetupComp.top = new FormAttachment( 0, 0 );
    fdSetupComp.right = new FormAttachment( 100, 0 );
    fdSetupComp.bottom = new FormAttachment( 100, 0 );
    wSetupComp.setLayoutData( fdSetupComp );
    wSetupComp.layout();
    wSetupTab.setControl( wSetupComp );
  }

  private void buildOptionsTab() {
    CTabItem wOptionsTab = new CTabItem( wTabFolder, SWT.NONE );
    wOptionsTab.setText( BaseMessages.getString( PKG, "KafkaProducerOutputDialog.Options.Tab" ) );
    Composite wOptionsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wOptionsComp );
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginHeight = 15;
    fieldsLayout.marginWidth = 15;
    wOptionsComp.setLayout( fieldsLayout );

    FormData optionsFormData = new FormData();
    optionsFormData.left = new FormAttachment( 0, 0 );
    optionsFormData.top = new FormAttachment( wOptionsComp, 0 );
    optionsFormData.right = new FormAttachment( 100, 0 );
    optionsFormData.bottom = new FormAttachment( 100, 0 );
    wOptionsComp.setLayoutData( optionsFormData );

    buildOptionsTable( wOptionsComp );

    wOptionsComp.layout();
    wOptionsTab.setControl( wOptionsComp );
  }

  private void buildOptionsTable( Composite parentWidget ) {
    ColumnInfo[] columns = getOptionsColumns();

    if ( meta.getConfig().size() == 0 ) {
      // initial call
      List<String> list = KafkaDialogHelper.getProducerAdvancedConfigOptionNames();
      Map<String, String> advancedConfig = new LinkedHashMap<>();
      for ( String item : list ) {
        advancedConfig.put( item, DEFAULT_OPTION_VALUES.getOrDefault( item, "" ) );
      }
      meta.setConfig( advancedConfig );
    }
    int fieldCount = meta.getConfig().size();

    optionsTable = new TableView(
      variables,
      parentWidget,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
      columns,
      fieldCount,
      false,
      lsMod,
      props,
      false
    );

    optionsTable.setSortable( false );

    FormData fdData = new FormData();
    fdData.left = new FormAttachment( 0, 0 );
    fdData.top = new FormAttachment( 0, 0 );
    fdData.right = new FormAttachment( 100, 0 );
    fdData.bottom = new FormAttachment( 100, 0 );

    // resize the columns to fit the data in them
    Arrays.stream( optionsTable.getTable().getColumns() ).forEach( column -> {
      if ( column.getWidth() > 0 ) {
        // don't pack anything with a 0 width, it will resize it to make it visible (like the index column)
        column.setWidth( 120 );
      }
    } );

    optionsTable.setLayoutData( fdData );
  }

  private ColumnInfo[] getOptionsColumns() {

    ColumnInfo optionName =
      new ColumnInfo( BaseMessages.getString( PKG, "KafkaProducerOutputDialog.Options.Column.Name" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false, false );

    ColumnInfo value = new ColumnInfo( BaseMessages.getString( PKG, "KafkaProducerOutputDialog.Options.Column.Value" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, false );
    value.setUsingVariables( true );

    return new ColumnInfo[] { optionName, value };
  }

  private void populateOptionsData() {
    int rowIndex = 0;
    for ( Map.Entry<String, String> entry : meta.getConfig().entrySet() ) {
      TableItem key = optionsTable.getTable().getItem( rowIndex++ );
      key.setText( 1, entry.getKey() );
      key.setText( 2, entry.getValue() );
    }
    optionsTable.optimizeTableView();
  }

  private void getData() {
    wBootstrapServers.setText( Const.NVL( meta.getDirectBootstrapServers(), "") );
    wClientId.setText( Const.NVL( meta.getClientId(), "") );
    wTopic.setText( Const.NVL( meta.getTopic(), "") );
    wKeyField.setText( Const.NVL( meta.getKeyField(), "") );
    wMessageField.setText( Const.NVL( meta.getMessageField(), "") );

    populateOptionsData();
  }


  private void cancel() {
    meta.setChanged( false );
    dispose();
  }

  private void ok() {
    transformName = wTransformName.getText();
    meta.setDirectBootstrapServers( wBootstrapServers.getText() );
    meta.setClientId( wClientId.getText() );
    meta.setTopic( wTopic.getText() );
    meta.setKeyField( wKeyField.getText() );
    meta.setMessageField( wMessageField.getText() );
    meta.setConfig( KafkaDialogHelper.getConfig( optionsTable ) );
    dispose();
  }
}
