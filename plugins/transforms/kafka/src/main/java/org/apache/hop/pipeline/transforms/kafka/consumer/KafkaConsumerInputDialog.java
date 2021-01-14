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

package org.apache.hop.pipeline.transforms.kafka.consumer;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.pipeline.transforms.kafka.shared.KafkaDialogHelper;
import org.apache.hop.pipeline.transforms.kafka.shared.KafkaFactory;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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

import static java.util.Optional.ofNullable;

public class KafkaConsumerInputDialog extends BaseTransformDialog implements ITransformDialog {

  private static final Class<?> PKG = KafkaConsumerInputDialog.class; // For Translator

  private static final Map<String, String> DEFAULT_OPTION_VALUES = ImmutableMap.of( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest" );

  private final KafkaFactory kafkaFactory = KafkaFactory.defaultFactory();

  protected KafkaConsumerInputMeta meta;

  protected Label wlFilename;
  protected TextVar wFilename;
  protected Button wbFilename;
  protected Button wbCreatePipeline;

  protected Label wlSubTransform;
  protected ComboVar wSubTransform;

  protected ModifyListener lsMod;
  protected Label wlBatchSize;
  protected TextVar wBatchSize;
  protected Label wlBatchDuration;
  protected TextVar wBatchDuration;

  protected CTabFolder wTabFolder;
  protected CTabItem wSetupTab;
  protected CTabItem wBatchTab;
  protected CTabItem wResultsTab;

  protected Composite wSetupComp;
  protected Composite wBatchComp;
  protected Composite wResultsComp;

  private final HopGui hopGui;

  private TextVar wConsumerGroup;
  private Button wbAutoCommit;
  private Button wbManualCommit;

  private TableView fieldsTable;
  private TableView topicsTable;
  private TableView optionsTable;

  private TextVar wBootstrapServers;

  public KafkaConsumerInputDialog( Shell parent, IVariables variables, Object meta, PipelineMeta pipelineMeta, String name ) {
    super( parent, variables, (BaseTransformMeta) meta, pipelineMeta, name );
    this.meta = (KafkaConsumerInputMeta) meta;
    hopGui = HopGui.getInstance();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE );
    props.setLook( shell );
    setShellImage( shell, meta );
    shell.setMinimumSize( 527, 622 );

    lsMod = e -> meta.setChanged();
    changed = meta.hasChanged();
    int margin = props.getMargin();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = props.getMargin();
    formLayout.marginHeight = props.getMargin();

    shell.setLayout( formLayout );
    shell.setText( getDialogTitle() );

    // Some buttons at the bottom...
    //
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, null );

    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.TransformName.Label" ) );
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
    fdTransformName.left = new FormAttachment( wlTransformName, 2*margin );
    fdTransformName.right = new FormAttachment( 100, 0 );
    fdTransformName.top = new FormAttachment( 0, 0 );
    wTransformName.setLayoutData( fdTransformName );

    // The filename
    //
    wlFilename = new Label( shell, SWT.LEFT );
    props.setLook( wlFilename );
    wlFilename.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Pipeline" ) );
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment( 0, 0 );
    fdlFilename.top = new FormAttachment( wTransformName, margin );
    wlFilename.setLayoutData( fdlFilename );

    wbCreatePipeline = new Button( shell, SWT.PUSH );
    props.setLook( wbCreatePipeline );
    wbCreatePipeline.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Pipeline.CreatePipeline" ) );
    FormData fdCreatePipeline = new FormData();
    fdCreatePipeline.right = new FormAttachment( 100, 0 );
    fdCreatePipeline.top = new FormAttachment( wlFilename, 0, SWT.CENTER );
    wbCreatePipeline.setLayoutData( fdCreatePipeline );
    wbCreatePipeline.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        createNewKafkaPipeline();
      }
    } );

    wbFilename = new Button( shell, SWT.PUSH );
    props.setLook( wbFilename );
    wbFilename.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Pipeline.Browse" ) );
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment( wbCreatePipeline, -margin );
    fdbFilename.top = new FormAttachment( wlFilename, 0, SWT.CENTER );
    wbFilename.setLayoutData( fdbFilename );
    wbFilename.addListener( SWT.Selection, e -> {
      HopPipelineFileType pipelineFileType = new HopPipelineFileType();
      BaseDialog.presentFileDialog(
        shell,
        wFilename, variables,
        pipelineFileType.getFilterExtensions(), pipelineFileType.getFilterNames(),
        true
      );
    } );

    wFilename = new TextVar( variables, shell, SWT.SINGLE | SWT.BORDER | SWT.LEFT );
    props.setLook( wFilename );
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment( wlFilename, margin );
    fdFilename.right = new FormAttachment( wbFilename, -Const.MARGIN );
    fdFilename.top = new FormAttachment( wlFilename, 0, SWT.CENTER );
    wFilename.setLayoutData( fdFilename );


    // Start of tabbed display
    //
    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setUnselectedCloseVisible( true );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( wlFilename, 15 );
    fdTabFolder.bottom = new FormAttachment( wOk, -15 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    wTabFolder.setLayoutData( fdTabFolder );

    buildSetupTab();
    buildBatchTab();
    buildResultsTab();
    createAdditionalTabs();


    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wTransformName.addSelectionListener( lsDef );

    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();
    setSize();

    wTabFolder.setSelection( 0 );

    wTransformName.selectAll();
    wTransformName.setFocus();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void ok() {
    transformName = wTransformName.getText();
    updateMeta( meta );
    dispose();
  }

  @Override public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  private void updateMeta( KafkaConsumerInputMeta m ) {
    m.setFilename( wFilename.getText() );
    m.setBatchSize( wBatchSize.getText() );
    m.setBatchDuration( wBatchDuration.getText() );
    m.setSubTransform( wSubTransform.getText() );
    setTopicsFromTable();

    m.setConsumerGroup( wConsumerGroup.getText() );
    m.setDirectBootstrapServers( wBootstrapServers.getText() );
    m.setAutoCommit( wbAutoCommit.getSelection() );

    setFieldsFromTable();
    setOptionsFromTable();
  }

  protected String getDialogTitle() {
    return BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Shell.Title" );
  }

  protected void createAdditionalTabs() {
    buildFieldsTab();
    buildOptionsTab();
    buildOffsetManagement();
  }

  private void buildOffsetManagement() {
    Group wOffsetGroup = new Group( wBatchComp, SWT.SHADOW_ETCHED_IN );
    wOffsetGroup.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.OffsetManagement" ) );
    FormLayout flOffsetGroup = new FormLayout();
    flOffsetGroup.marginHeight = 15;
    flOffsetGroup.marginWidth = 15;
    wOffsetGroup.setLayout( flOffsetGroup );

    FormData fdOffsetGroup = new FormData();
    fdOffsetGroup.top = new FormAttachment( wBatchSize, 15 );
    fdOffsetGroup.left = new FormAttachment( 0, 0 );
    fdOffsetGroup.right = new FormAttachment( 100, 0 );
    wOffsetGroup.setLayoutData( fdOffsetGroup );
    props.setLook( wOffsetGroup );

    wbAutoCommit = new Button( wOffsetGroup, SWT.RADIO );
    wbAutoCommit.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.AutoOffset" ) );
    FormData fdbAutoCommit = new FormData();
    fdbAutoCommit.top = new FormAttachment( 0, 0 );
    fdbAutoCommit.left = new FormAttachment( 0, 0 );
    wbAutoCommit.setLayoutData( fdbAutoCommit );
    props.setLook( wbAutoCommit );

    wbManualCommit = new Button( wOffsetGroup, SWT.RADIO );
    wbManualCommit.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.ManualOffset" ) );
    FormData fdbManualCommit = new FormData();
    fdbManualCommit.left = new FormAttachment( 0, 0 );
    fdbManualCommit.top = new FormAttachment( wbAutoCommit, 10, SWT.BOTTOM );
    wbManualCommit.setLayoutData( fdbManualCommit );
    props.setLook( wbManualCommit );
  }

  protected void buildSetup( Composite wSetupComp ) {
    props.setLook( wSetupComp );
    FormLayout setupLayout = new FormLayout();
    setupLayout.marginHeight = 15;
    setupLayout.marginWidth = 15;
    wSetupComp.setLayout( setupLayout );


    Label wlBootstrapServers = new Label(wSetupComp, SWT.LEFT);
    props.setLook(wlBootstrapServers);
    wlBootstrapServers.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.BootstrapServers" ) );
    FormData fdlBootstrapServers = new FormData();
    fdlBootstrapServers.left = new FormAttachment( 0, 0 );
    fdlBootstrapServers.top = new FormAttachment( 0, 0 );
    wlBootstrapServers.setLayoutData( fdlBootstrapServers );

    wBootstrapServers = new TextVar( variables, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBootstrapServers );
    wBootstrapServers.addModifyListener( lsMod );
    FormData fdBootstrapServers = new FormData();
    fdBootstrapServers.left = new FormAttachment( 0, 0 );
    fdBootstrapServers.top = new FormAttachment(wlBootstrapServers, Const.MARGIN );
    fdBootstrapServers.right = new FormAttachment( 100, 0 );
    wBootstrapServers.setLayoutData( fdBootstrapServers );

    Label wlTopic = new Label(wSetupComp, SWT.LEFT);
    props.setLook(wlTopic);
    wlTopic.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Topics" ) );
    FormData fdlTopic = new FormData();
    fdlTopic.left = new FormAttachment( 0, 0 );
    fdlTopic.top = new FormAttachment( wBootstrapServers, 3 * Const.MARGIN );
    fdlTopic.right = new FormAttachment( Const.MIDDLE_PCT, 0 );
    wlTopic.setLayoutData( fdlTopic );

    wConsumerGroup = new TextVar( variables, wSetupComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConsumerGroup );
    wConsumerGroup.addModifyListener( lsMod );
    FormData fdConsumerGroup = new FormData();
    fdConsumerGroup.left = new FormAttachment( 0, 0 );
    fdConsumerGroup.right = new FormAttachment( 100, 0 );
    fdConsumerGroup.bottom = new FormAttachment( 100, 0 );
    wConsumerGroup.setLayoutData( fdConsumerGroup );

    Label wlConsumerGroup = new Label(wSetupComp, SWT.LEFT);
    props.setLook(wlConsumerGroup);
    wlConsumerGroup.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.ConsumerGroup" ) );
    FormData fdlConsumerGroup = new FormData();
    fdlConsumerGroup.left = new FormAttachment( 0, 0 );
    fdlConsumerGroup.bottom = new FormAttachment( wConsumerGroup, -5, SWT.TOP );
    fdlConsumerGroup.right = new FormAttachment( 50, 0 );
    wlConsumerGroup.setLayoutData( fdlConsumerGroup );

    buildTopicsTable( wSetupComp, wlTopic, wlConsumerGroup);

    FormData fdSetupComp = new FormData();
    fdSetupComp.left = new FormAttachment( 0, 0 );
    fdSetupComp.top = new FormAttachment( 0, 0 );
    fdSetupComp.right = new FormAttachment( 100, 0 );
    fdSetupComp.bottom = new FormAttachment( 100, 0 );
    wSetupComp.setLayoutData( fdSetupComp );
    wSetupComp.layout();
    wSetupTab.setControl( wSetupComp );
  }

  private void buildFieldsTab() {
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE, 2);
    wFieldsTab.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.FieldsTab" ) );

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wFieldsComp);
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginHeight = 15;
    fieldsLayout.marginWidth = 15;
    wFieldsComp.setLayout( fieldsLayout );

    FormData fieldsFormData = new FormData();
    fieldsFormData.left = new FormAttachment( 0, 0 );
    fieldsFormData.top = new FormAttachment(wFieldsComp, 0 );
    fieldsFormData.right = new FormAttachment( 100, 0 );
    fieldsFormData.bottom = new FormAttachment( 100, 0 );
    wFieldsComp.setLayoutData( fieldsFormData );

    buildFieldTable(wFieldsComp, wFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);
  }

  private void buildOptionsTab() {
    CTabItem wOptionsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOptionsTab.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.OptionsTab" ) );

    Composite wOptionsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wOptionsComp);
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginHeight = 15;
    fieldsLayout.marginWidth = 15;
    wOptionsComp.setLayout( fieldsLayout );

    FormData optionsFormData = new FormData();
    optionsFormData.left = new FormAttachment( 0, 0 );
    optionsFormData.top = new FormAttachment(wOptionsComp, 0 );
    optionsFormData.right = new FormAttachment( 100, 0 );
    optionsFormData.bottom = new FormAttachment( 100, 0 );
    wOptionsComp.setLayoutData( optionsFormData );

    buildOptionsTable(wOptionsComp);

    wOptionsComp.layout();
    wOptionsTab.setControl(wOptionsComp);
  }

  private void buildFieldTable( Composite parentWidget, Control relativePosition ) {
    ColumnInfo[] columns = getFieldColumns();

    int fieldCount = KafkaConsumerField.Name.values().length;

    fieldsTable = new TableView(
      variables,
      parentWidget,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
      columns,
      fieldCount,
      true,
      lsMod,
      props,
      false
    );

    fieldsTable.setSortable( false );

    populateFieldData();

    FormData fdData = new FormData();
    fdData.left = new FormAttachment( 0, 0 );
    fdData.top = new FormAttachment( relativePosition, 5 );
    fdData.right = new FormAttachment( 100, 0 );
    fieldsTable.setLayoutData( fdData );

    // don't let any rows get deleted or added (this does not affect the read-only state of the cells)
    fieldsTable.setReadonly( true );
  }

  private void buildOptionsTable( Composite parentWidget ) {
    ColumnInfo[] columns = getOptionsColumns();

    if ( meta.getConfig().size() == 0 ) {
      // inital call
      List<String> list = KafkaDialogHelper.getConsumerAdvancedConfigOptionNames();
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

    populateOptionsData();

    FormData fdData = new FormData();
    fdData.left = new FormAttachment( 0, 0 );
    fdData.top = new FormAttachment( 0, 0 );
    fdData.right = new FormAttachment( 100, 0 );
    fdData.bottom = new FormAttachment( 100, 0 );
    optionsTable.setLayoutData( fdData );
  }

  private void buildSetupTab() {
    wSetupTab = new CTabItem( wTabFolder, SWT.NONE );
    wSetupTab.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.SetupTab" ) );

    wSetupComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wSetupComp );
    FormLayout setupLayout = new FormLayout();
    setupLayout.marginHeight = 15;
    setupLayout.marginWidth = 15;
    wSetupComp.setLayout( setupLayout );

    buildSetup( wSetupComp );

    FormData fdSetupComp = new FormData();
    fdSetupComp.left = new FormAttachment( 0, 0 );
    fdSetupComp.top = new FormAttachment( 0, 0 );
    fdSetupComp.right = new FormAttachment( 100, 0 );
    fdSetupComp.bottom = new FormAttachment( 100, 0 );
    wSetupComp.setLayoutData( fdSetupComp );
    wSetupComp.layout();
    wSetupTab.setControl( wSetupComp );
  }

  private void buildBatchTab() {
    wBatchTab = new CTabItem( wTabFolder, SWT.NONE );
    wBatchTab.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.BatchTab" ) );

    wBatchComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wBatchComp );
    FormLayout batchLayout = new FormLayout();
    batchLayout.marginHeight = 15;
    batchLayout.marginWidth = 15;
    wBatchComp.setLayout( batchLayout );

    FormData fdBatchComp = new FormData();
    fdBatchComp.left = new FormAttachment( 0, 0 );
    fdBatchComp.top = new FormAttachment( 0, 0 );
    fdBatchComp.right = new FormAttachment( 100, 0 );
    fdBatchComp.bottom = new FormAttachment( 100, 0 );
    wBatchComp.setLayoutData( fdBatchComp );

    wlBatchDuration = new Label( wBatchComp, SWT.LEFT );
    props.setLook( wlBatchDuration );
    wlBatchDuration.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.BatchDuration" ) );
    FormData fdlBatchDuration = new FormData();
    fdlBatchDuration.left = new FormAttachment( 0, 0 );
    fdlBatchDuration.top = new FormAttachment( 0, 0 );
    fdlBatchDuration.right = new FormAttachment( 50, 0 );
    wlBatchDuration.setLayoutData( fdlBatchDuration );

    wBatchDuration = new TextVar( variables, wBatchComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBatchDuration );
    wBatchDuration.addModifyListener( lsMod );
    FormData fdBatchDuration = new FormData();
    fdBatchDuration.left = new FormAttachment( 0, 0 );
    fdBatchDuration.right = new FormAttachment( 100, 0 );
    fdBatchDuration.top = new FormAttachment( wlBatchDuration, 5 );
    wBatchDuration.setLayoutData( fdBatchDuration );

    wlBatchSize = new Label( wBatchComp, SWT.LEFT );
    props.setLook( wlBatchSize );
    wlBatchSize.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.BatchSize" ) );
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment( 0, 0 );
    fdlBatchSize.top = new FormAttachment( wBatchDuration, 10 );
    fdlBatchSize.right = new FormAttachment( 50, 0 );
    wlBatchSize.setLayoutData( fdlBatchSize );

    wBatchSize = new TextVar( variables, wBatchComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wBatchSize );
    wBatchSize.addModifyListener( lsMod );
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment( 0, 0 );
    fdBatchSize.right = new FormAttachment( 100, 0 );
    fdBatchSize.top = new FormAttachment( wlBatchSize, 5 );
    wBatchSize.setLayoutData( fdBatchSize );

    wBatchComp.layout();
    wBatchTab.setControl( wBatchComp );
  }

  private void buildResultsTab() {
    wResultsTab = new CTabItem( wTabFolder, SWT.NONE );
    wResultsTab.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.ResultsTab" ) );

    wResultsComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wResultsComp );
    FormLayout resultsLayout = new FormLayout();
    resultsLayout.marginHeight = 15;
    resultsLayout.marginWidth = 15;
    wResultsComp.setLayout( resultsLayout );

    FormData fdResultsComp = new FormData();
    fdResultsComp.left = new FormAttachment( 0, 0 );
    fdResultsComp.top = new FormAttachment( 0, 0 );
    fdResultsComp.right = new FormAttachment( 100, 0 );
    fdResultsComp.bottom = new FormAttachment( 100, 0 );
    wResultsComp.setLayoutData( fdResultsComp );

    wlSubTransform = new Label( wResultsComp, SWT.LEFT );
    props.setLook( wlSubTransform );
    FormData fdlSubTrans = new FormData();
    fdlSubTrans.left = new FormAttachment( 0, 0 );
    fdlSubTrans.top = new FormAttachment( 0, 0 );
    wlSubTransform.setLayoutData( fdlSubTrans );
    wlSubTransform.setText( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Pipeline.SubPipelineTransform" ) );

    wSubTransform = new ComboVar( variables, wResultsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSubTransform );
    FormData fdSubTransform = new FormData();
    fdSubTransform.left = new FormAttachment( 0, 0 );
    fdSubTransform.right = new FormAttachment( 60, 0 ); // 60% of dialog width
    fdSubTransform.top = new FormAttachment( wlSubTransform, 5 );
    wSubTransform.setLayoutData( fdSubTransform );
    wSubTransform.getCComboWidget().addListener( SWT.FocusIn, this::populateSubTransforms );

    wResultsComp.layout();
    wResultsTab.setControl( wResultsComp );
  }

  private ColumnInfo[] getFieldColumns() {
    KafkaConsumerField.Type[] values = KafkaConsumerField.Type.values();
    String[] supportedTypes = Arrays.stream( values ).map( KafkaConsumerField.Type::toString ).toArray( String[]::new );

    ColumnInfo referenceName = new ColumnInfo( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Column.Ref" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, true );

    ColumnInfo name = new ColumnInfo( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Column.Name" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, false );

    ColumnInfo type = new ColumnInfo( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Column.Type" ),
      ColumnInfo.COLUMN_TYPE_CCOMBO, supportedTypes, false );

    // don't let the user edit the type for anything other than key & msg fields
    type.setDisabledListener( rowNumber -> {
      String ref = fieldsTable.getTable().getItem( rowNumber ).getText( 1 );
      KafkaConsumerField.Name refName = KafkaConsumerField.Name.valueOf( ref.toUpperCase() );

      return !( refName == KafkaConsumerField.Name.KEY || refName == KafkaConsumerField.Name.MESSAGE );
    } );

    return new ColumnInfo[] { referenceName, name, type };
  }

  private ColumnInfo[] getOptionsColumns() {

    ColumnInfo optionName = new ColumnInfo( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.NameField" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, false );

    ColumnInfo value = new ColumnInfo( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.Column.Value" ),
      ColumnInfo.COLUMN_TYPE_TEXT, false, false );
    value.setUsingVariables( true );

    return new ColumnInfo[] { optionName, value };
  }

  private void populateFieldData() {
    List<KafkaConsumerField> fieldDefinitions = meta.getFieldDefinitions();
    int rowIndex = 0;
    for ( KafkaConsumerField field : fieldDefinitions ) {
      TableItem key = fieldsTable.getTable().getItem( rowIndex++ );

      key.setText( 1, Const.NVL(field.getKafkaName().toString(), "") );
      key.setText( 2, Const.NVL(field.getOutputName(), "") );
      key.setText( 3, Const.NVL(field.getOutputType().toString(), "") );
    }
  }

  private void populateOptionsData() {
    int rowIndex = 0;
    for ( Map.Entry<String, String> entry : meta.getConfig().entrySet() ) {
      TableItem key = optionsTable.getTable().getItem( rowIndex++ );
      key.setText( 1, entry.getKey() );
      key.setText( 2, entry.getValue() );
    }
  }

  private void populateTopicsData() {
    List<String> topics = meta.getTopics();
    int rowIndex = 0;
    for ( String topic : topics ) {
      TableItem key = topicsTable.getTable().getItem( rowIndex++ );
      if ( topic != null ) {
        key.setText( 1, topic );
      }
    }
  }

  private void buildTopicsTable( Composite parentWidget, Control controlAbove, Control controlBelow ) {
    ColumnInfo[] columns = new ColumnInfo[] { new ColumnInfo( BaseMessages.getString( PKG, "KafkaConsumerInputDialog.NameField" ),
      ColumnInfo.COLUMN_TYPE_CCOMBO, new String[ 1 ], false ) };

    columns[ 0 ].setUsingVariables( true );

    int topicsCount = meta.getTopics().size();

    Listener lsFocusInTopic = e -> {
      CCombo comboWidget = (CCombo) e.widget;
      ComboVar topicsCombo = (ComboVar) comboWidget.getParent();

      KafkaDialogHelper kdh = new KafkaDialogHelper( variables, topicsCombo, wBootstrapServers, kafkaFactory, optionsTable, meta.getParentTransformMeta() );
      kdh.clusterNameChanged( e );
    };

    topicsTable = new TableView(
      variables,
      parentWidget,
      SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
      columns,
      topicsCount,
      false,
      lsMod,
      props,
      false,
      lsFocusInTopic
    );

    topicsTable.setSortable( false );

    populateTopicsData();

    FormData fdData = new FormData();
    fdData.left = new FormAttachment( 0, 0 );
    fdData.top = new FormAttachment( controlAbove, 5 );
    fdData.right = new FormAttachment( 60, 0 ); // 60% of dialog width
    fdData.bottom = new FormAttachment( controlBelow, -10, SWT.TOP );
    topicsTable.setLayoutData( fdData );

    topicsTable.optimizeTableView();
  }

  protected void getData() {
    wFilename.setText( Const.NVL( meta.getFilename(), "" ) );
    wBootstrapServers.setText( Const.NVL( meta.getDirectBootstrapServers(), "" ) );

    populateTopicsData();

    wSubTransform.setText( Const.NVL( meta.getSubTransform(), "" ) );
    wConsumerGroup.setText( Const.NVL( meta.getConsumerGroup(), "" ) );
    wBatchSize.setText( Const.NVL(meta.getBatchSize(), "") );
    wBatchDuration.setText( Const.NVL(meta.getBatchDuration(), "") );

    wbAutoCommit.setSelection( meta.isAutoCommit() );
    wbManualCommit.setSelection( !meta.isAutoCommit() );

    populateFieldData();

    fieldsTable.optimizeTableView();
    topicsTable.optimizeTableView();
    optionsTable.optimizeTableView();
  }

  private void cancel() {
    meta.setChanged( false );
    dispose();
  }

  private void setFieldsFromTable() {
    int itemCount = fieldsTable.getItemCount();
    for ( int rowIndex = 0; rowIndex < itemCount; rowIndex++ ) {
      TableItem row = fieldsTable.getTable().getItem( rowIndex );
      String kafkaName = row.getText( 1 );
      String outputName = row.getText( 2 );
      String outputType = row.getText( 3 );
      try {
        KafkaConsumerField.Name ref = KafkaConsumerField.Name.valueOf( kafkaName.toUpperCase() );
        KafkaConsumerField field = new KafkaConsumerField(
          ref,
          outputName,
          KafkaConsumerField.Type.valueOf( outputType )
        );
        meta.setField( field );
      } catch ( IllegalArgumentException e ) {
        if ( isDebug() ) {
          logDebug( e.getMessage(), e );
        }
      }
    }
  }

  private void setTopicsFromTable() {
    int itemCount = topicsTable.getItemCount();
    ArrayList<String> tableTopics = new ArrayList<>();
    for ( int rowIndex = 0; rowIndex < itemCount; rowIndex++ ) {
      TableItem row = topicsTable.getTable().getItem( rowIndex );
      String topic = row.getText( 1 );
      if ( !"".equals( topic ) && tableTopics.indexOf( topic ) == -1 ) {
        tableTopics.add( topic );
      }
    }
    meta.setTopics( tableTopics );
  }

  private void setOptionsFromTable() {
    meta.setConfig( KafkaDialogHelper.getConfig( optionsTable ) );
  }

  protected String[] getFieldNames() {
    return Arrays.stream( fieldsTable.getTable().getItems() ).map( row -> row.getText( 2 ) ).toArray( String[]::new );
  }

  protected int[] getFieldTypes() {
    return Arrays.stream( fieldsTable.getTable().getItems() )
      .mapToInt( row -> ValueMetaFactory.getIdForValueMeta( row.getText( 3 ) ) ).toArray();
  }


  protected void createNewKafkaPipeline() {
    PipelineMeta kafkaPipelineMeta = createSubPipelineMeta();

    HopDataOrchestrationPerspective doPerspective = HopGui.getDataOrchestrationPerspective();
    if (doPerspective==null) {
      return;
    }

    try {
      // Add a new tab with a new pipeline in the background
      //
      doPerspective.addPipeline( hopGui, kafkaPipelineMeta, new HopPipelineFileType() );

      // Ask the user to save the new pipeline
      //
      String filename = hopGui.fileDelegate.fileSaveAs();

      if ( StringUtils.isNotEmpty(filename)) {
        // It's hidden in another tab so to make sure, do it asynchronous
        //
        Display.getDefault().asyncExec( () -> wFilename.setText( filename ) );
      }
    } catch(Exception e) {
      new ErrorDialog( shell, "Error", "Error adding new Kafka pipeline", e );
    }
  }

  protected PipelineMeta createSubPipelineMeta() {
    InjectorMeta rm = new InjectorMeta();
    String[] fieldNames = getFieldNames();
    int[] empty = new int[ fieldNames.length ];
    Arrays.fill( empty, -1 );
    rm.setFieldname( fieldNames );
    rm.setType( getFieldTypes() );
    rm.setLength( empty );
    rm.setPrecision( empty );

    TransformMeta recsFromStream = new TransformMeta( "RecordsFromStream", "Get messages from Kafka", rm );
    recsFromStream.setLocation( new Point( 100, 100 ) );

    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform( recsFromStream );
    pipelineMeta.setFilename( "" );

    return pipelineMeta;
  }

  private PipelineMeta loadKafkaPipelineMeta() throws HopException {
    KafkaConsumerInputMeta copyMeta = meta.clone();
    updateMeta( copyMeta );
    return TransformWithMappingMeta.loadMappingMeta( copyMeta, getMetadataProvider(), variables );
  }

  protected void populateSubTransforms( Event event ) {
    try {
      String current = wSubTransform.getText();
      wSubTransform.removeAll();

      ofNullable( loadKafkaPipelineMeta() )
        .ifPresent( pipelineMeta ->
          pipelineMeta
            .getTransforms()
            .stream()
            .map( TransformMeta::getName )
            .sorted()
            .forEach( wSubTransform::add ) );

      //I don't know why but just calling setText does not work when the text is not one of the items in the list.
      //Instead the first item in the list is selected.  asyncExec solves it.  If you have a better solution, by all
      //means go ahead and implement
      //
      Display.getDefault().asyncExec( () -> wSubTransform.setText( current ) );
    } catch ( HopException e ) {
      log.logError("Error getting transform names from Kafka pipeline", e );
    }
  }
}
