// CHECKSTYLE:FileLength:OFF
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

package org.apache.hop.ui.pipeline.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;

//import org.apache.hop.pipeline.transforms.databaselookup.DatabaseLookupMeta;


public class PipelineDialog extends Dialog {

  private static final Class<?> PKG = PipelineDialog.class; // For Translator

  public enum Tabs {
    PIPELINE_TAB, PARAM_TAB, MISC_TAB, MONITOR_TAB, EXTRA_TAB,
  }

  private CTabFolder wTabFolder;
  private FormData fdTabFolder;

  private CTabItem wPipelineTab, wParamTab, wMiscTab, wMonitorTab;

  private Text wPipelineName;
  private Button wNameFilenameSync;
  private Text wPipelineFilename;

  // Pipeline description
  private Text wPipelineDescription;

  private Label wlExtendedDescription;
  private Text wExtendedDescription;
  private FormData fdlExtendedDescription, fdExtendedDescription;

  // Pipeline Status
  private Label wlPipelineStatus;
  private CCombo wPipelineStatus;
  private FormData fdlPipelineStatus, fdPipelineStatus;

  // Pipeline version
  private Text wPipelineVersion;

  private Text wCreateUser;
  private Text wCreateDate;

  private Text wModUser;
  private Text wModDate;

  private TableView wParamFields;

  private Button wOk, wCancel;

  private IVariables variables;
  private PipelineMeta pipelineMeta;
  private Shell shell;

  private SelectionAdapter lsDef;

  private ModifyListener lsMod;
  private PropsUi props;

  private int middle;

  private int margin;

  private Button wEnableTransformPerfMonitor;

  private Text wTransformPerfInterval;

  private Tabs currentTab = null;

  protected boolean changed;

  //private DatabaseDialog databaseDialog;
  private SelectionAdapter lsModSel;
  private TextVar wTransformPerfMaxSize;

  private ArrayList<IPipelineDialogPlugin> extraTabs;

  public PipelineDialog( Shell parent, int style, IVariables variables, PipelineMeta pipelineMeta, Tabs currentTab ) {
    this( parent, style, variables, pipelineMeta );
    this.currentTab = currentTab;
  }

  public PipelineDialog( Shell parent, int style, IVariables variables, PipelineMeta pipelineMeta ) {
    super( parent, style );
    this.props = PropsUi.getInstance();
    this.variables = variables;
    this.pipelineMeta = pipelineMeta;

    changed = false;
  }

  public PipelineMeta open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImagePipeline() );

    lsMod = e -> changed = true;
    lsModSel = new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        changed = true;
      }
    };

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "PipelineDialog.Shell.Title" ) );

    middle = props.getMiddlePct();
    margin = props.getMargin();

    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );

    addPipelineTab();
    addParamTab();
    addMonitoringTab();

    // See if there are any other tabs to be added...
    extraTabs = new ArrayList<>();
    java.util.List<IPlugin> pipelineDialogPlugins =
      PluginRegistry.getInstance().getPlugins( PipelineDialogPluginType.class );
    for ( IPlugin pipelineDialogPlugin : pipelineDialogPlugins ) {
      try {
        IPipelineDialogPlugin extraTab =
          (IPipelineDialogPlugin) PluginRegistry.getInstance().loadClass( pipelineDialogPlugin );
        extraTab.addTab( pipelineMeta, parent, wTabFolder );
        extraTabs.add( extraTab );
      } catch ( Exception e ) {
        new ErrorDialog( shell, "Error", "Error loading pipeline dialog plugin with id "
          + pipelineDialogPlugin.getIds()[ 0 ], e );
      }
    }

    fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.top = new FormAttachment( 0, 0 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.bottom = new FormAttachment( 100, -50 );
    wTabFolder.setLayoutData( fdTabFolder );

    // THE BUTTONS
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, props.getMargin(), null );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wPipelineName.addSelectionListener( lsDef );
    wPipelineDescription.addSelectionListener( lsDef );
    wPipelineVersion.addSelectionListener( lsDef );
    wTransformPerfInterval.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    if ( currentTab != null ) {
      setCurrentTab( currentTab );
    } else {
      wTabFolder.setSelection( 0 );
    }

    getData();

    BaseTransformDialog.setSize( shell );

    changed = false;

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return pipelineMeta;
  }

  private void addPipelineTab() {
    // ////////////////////////
    // START OF PIPELINE TAB///
    // /
    wPipelineTab = new CTabItem( wTabFolder, SWT.NONE );
    wPipelineTab.setText( BaseMessages.getString( PKG, "PipelineDialog.PipelineTab.Label" ) );

    Composite wPipelineComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wPipelineComp );

    FormLayout workflowLayout = new FormLayout();
    workflowLayout.marginWidth = Const.FORM_MARGIN;
    workflowLayout.marginHeight = Const.FORM_MARGIN;
    wPipelineComp.setLayout( workflowLayout );

    // Pipeline name:
    //
    Label wlPipelineName = new Label( wPipelineComp, SWT.RIGHT );
    wlPipelineName.setText( BaseMessages.getString( PKG, "PipelineDialog.PipelineName.Label" ) );
    props.setLook( wlPipelineName );
    FormData fdlPipelineName = new FormData();
    fdlPipelineName.left = new FormAttachment( 0, 0 );
    fdlPipelineName.right = new FormAttachment( middle, -margin );
    fdlPipelineName.top = new FormAttachment( 0, margin );
    wlPipelineName.setLayoutData( fdlPipelineName );
    wPipelineName = new Text( wPipelineComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPipelineName );
    wPipelineName.addModifyListener( lsMod );
    FormData fdPipelineName = new FormData();
    fdPipelineName.left = new FormAttachment( middle, 0 );
    fdPipelineName.top = new FormAttachment( 0, margin );
    fdPipelineName.right = new FormAttachment( 100, 0 );
    wPipelineName.setLayoutData( fdPipelineName );
    Control lastControl = wPipelineName;

    // Synchronize name with filename?
    //
    Label wlNameFilenameSync = new Label( wPipelineComp, SWT.RIGHT );
    wlNameFilenameSync.setText( BaseMessages.getString( PKG, "PipelineDialog.NameFilenameSync.Label" ) );
    props.setLook( wlNameFilenameSync );
    FormData fdlNameFilenameSync = new FormData();
    fdlNameFilenameSync.left = new FormAttachment( 0, 0 );
    fdlNameFilenameSync.right = new FormAttachment( middle, -margin );
    fdlNameFilenameSync.top = new FormAttachment( lastControl, margin );
    wlNameFilenameSync.setLayoutData( fdlNameFilenameSync );
    wNameFilenameSync = new Button( wPipelineComp, SWT.CHECK );
    props.setLook( wNameFilenameSync );
    FormData fdNameFilenameSync = new FormData();
    fdNameFilenameSync.left = new FormAttachment( middle, 0 );
    fdNameFilenameSync.top = new FormAttachment( wlNameFilenameSync, 0, SWT.CENTER );
    fdNameFilenameSync.right = new FormAttachment( 100, 0 );
    wNameFilenameSync.setLayoutData( fdNameFilenameSync );
    wNameFilenameSync.addListener( SWT.Selection, this::updateNameFilenameSync);
    lastControl = wNameFilenameSync;

    // Pipeline name:
    Label wlPipelineFilename = new Label( wPipelineComp, SWT.RIGHT );
    wlPipelineFilename.setText( BaseMessages.getString( PKG, "PipelineDialog.PipelineFilename.Label" ) );
    props.setLook( wlPipelineFilename );
    FormData fdlPipelineFilename = new FormData();
    fdlPipelineFilename.left = new FormAttachment( 0, 0 );
    fdlPipelineFilename.right = new FormAttachment( middle, -margin );
    fdlPipelineFilename.top = new FormAttachment( lastControl, margin );
    wlPipelineFilename.setLayoutData( fdlPipelineFilename );
    wPipelineFilename = new Text( wPipelineComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPipelineFilename );
    wPipelineFilename.addModifyListener( lsMod );
    FormData fdPipelineFilename = new FormData();
    fdPipelineFilename.left = new FormAttachment( middle, 0 );
    fdPipelineFilename.top = new FormAttachment( wlPipelineFilename, 0, SWT.CENTER );
    fdPipelineFilename.right = new FormAttachment( 100, 0 );
    wPipelineFilename.setLayoutData( fdPipelineFilename );
    wPipelineFilename.setEditable( false );
    wPipelineFilename.setBackground( GuiResource.getInstance().getColorLightGray() );
    lastControl = wPipelineFilename;

    // Pipeline description:
    //
    Label wlPipelineDescription = new Label( wPipelineComp, SWT.RIGHT );
    wlPipelineDescription.setText( BaseMessages.getString( PKG, "PipelineDialog.PipelineDescription.Label" ) );
    props.setLook( wlPipelineDescription );
    FormData fdlPipelineDescription = new FormData();
    fdlPipelineDescription.left = new FormAttachment( 0, 0 );
    fdlPipelineDescription.right = new FormAttachment( middle, -margin );
    fdlPipelineDescription.top = new FormAttachment( lastControl, margin );
    wlPipelineDescription.setLayoutData( fdlPipelineDescription );
    wPipelineDescription = new Text( wPipelineComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPipelineDescription );
    wPipelineDescription.addModifyListener( lsMod );
    FormData fdPipelineDescription = new FormData();
    fdPipelineDescription.left = new FormAttachment( middle, 0 );
    fdPipelineDescription.top = new FormAttachment( wlPipelineDescription, 0, SWT.CENTER );
    fdPipelineDescription.right = new FormAttachment( 100, 0 );
    wPipelineDescription.setLayoutData( fdPipelineDescription );

    // Pipeline Extended description
    wlExtendedDescription = new Label( wPipelineComp, SWT.RIGHT );
    wlExtendedDescription.setText( BaseMessages.getString( PKG, "PipelineDialog.Extendeddescription.Label" ) );
    props.setLook( wlExtendedDescription );
    fdlExtendedDescription = new FormData();
    fdlExtendedDescription.left = new FormAttachment( 0, 0 );
    fdlExtendedDescription.top = new FormAttachment( wPipelineDescription, margin );
    fdlExtendedDescription.right = new FormAttachment( middle, -margin );
    wlExtendedDescription.setLayoutData( fdlExtendedDescription );

    wExtendedDescription = new Text( wPipelineComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    props.setLook( wExtendedDescription, Props.WIDGET_STYLE_FIXED );
    wExtendedDescription.addModifyListener( lsMod );
    fdExtendedDescription = new FormData();
    fdExtendedDescription.left = new FormAttachment( middle, 0 );
    fdExtendedDescription.top = new FormAttachment( wPipelineDescription, margin );
    fdExtendedDescription.right = new FormAttachment( 100, 0 );
    fdExtendedDescription.bottom = new FormAttachment( 50, -margin );
    wExtendedDescription.setLayoutData( fdExtendedDescription );

    // Pipeline Status
    wlPipelineStatus = new Label( wPipelineComp, SWT.RIGHT );
    wlPipelineStatus.setText( BaseMessages.getString( PKG, "PipelineDialog.PipelineStatus.Label" ) );
    props.setLook( wlPipelineStatus );
    fdlPipelineStatus = new FormData();
    fdlPipelineStatus.left = new FormAttachment( 0, 0 );
    fdlPipelineStatus.right = new FormAttachment( middle, 0 );
    fdlPipelineStatus.top = new FormAttachment( wExtendedDescription, margin * 2 );
    wlPipelineStatus.setLayoutData( fdlPipelineStatus );
    wPipelineStatus = new CCombo( wPipelineComp, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER );
    wPipelineStatus.add( BaseMessages.getString( PKG, "PipelineDialog.Draft_PipelineStatus.Label" ) );
    wPipelineStatus.add( BaseMessages.getString( PKG, "PipelineDialog.Production_PipelineStatus.Label" ) );
    wPipelineStatus.add( "" );
    wPipelineStatus.select( -1 ); // +1: starts at -1
    wPipelineStatus.addSelectionListener( lsModSel );

    props.setLook( wPipelineStatus );
    fdPipelineStatus = new FormData();
    fdPipelineStatus.left = new FormAttachment( middle, 0 );
    fdPipelineStatus.top = new FormAttachment( wExtendedDescription, margin * 2 );
    fdPipelineStatus.right = new FormAttachment( 100, 0 );
    wPipelineStatus.setLayoutData( fdPipelineStatus );

    // Pipeline PipelineVersion:
    Label wlPipelineVersion = new Label( wPipelineComp, SWT.RIGHT );
    wlPipelineVersion.setText( BaseMessages.getString( PKG, "PipelineDialog.PipelineVersion.Label" ) );
    props.setLook( wlPipelineVersion );
    FormData fdlPipelineVersion = new FormData();
    fdlPipelineVersion.left = new FormAttachment( 0, 0 );
    fdlPipelineVersion.right = new FormAttachment( middle, -margin );
    fdlPipelineVersion.top = new FormAttachment( wPipelineStatus, margin );
    wlPipelineVersion.setLayoutData( fdlPipelineVersion );
    wPipelineVersion = new Text( wPipelineComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPipelineVersion );
    wPipelineVersion.addModifyListener( lsMod );
    FormData fdPipelineVersion = new FormData();
    fdPipelineVersion.left = new FormAttachment( middle, 0 );
    fdPipelineVersion.top = new FormAttachment( wPipelineStatus, margin );
    fdPipelineVersion.right = new FormAttachment( 100, 0 );
    wPipelineVersion.setLayoutData( fdPipelineVersion );

    // Create User:
    Label wlCreateUser = new Label( wPipelineComp, SWT.RIGHT );
    wlCreateUser.setText( BaseMessages.getString( PKG, "PipelineDialog.CreateUser.Label" ) );
    props.setLook( wlCreateUser );
    FormData fdlCreateUser = new FormData();
    fdlCreateUser.left = new FormAttachment( 0, 0 );
    fdlCreateUser.right = new FormAttachment( middle, -margin );
    fdlCreateUser.top = new FormAttachment( wPipelineVersion, margin );
    wlCreateUser.setLayoutData( fdlCreateUser );
    wCreateUser = new Text( wPipelineComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateUser );
    wCreateUser.setEditable( false );
    wCreateUser.addModifyListener( lsMod );
    FormData fdCreateUser = new FormData();
    fdCreateUser.left = new FormAttachment( middle, 0 );
    fdCreateUser.top = new FormAttachment( wPipelineVersion, margin );
    fdCreateUser.right = new FormAttachment( 100, 0 );
    wCreateUser.setLayoutData( fdCreateUser );

    // Created Date:
    Label wlCreateDate = new Label( wPipelineComp, SWT.RIGHT );
    wlCreateDate.setText( BaseMessages.getString( PKG, "PipelineDialog.CreateDate.Label" ) );
    props.setLook( wlCreateDate );
    FormData fdlCreateDate = new FormData();
    fdlCreateDate.left = new FormAttachment( 0, 0 );
    fdlCreateDate.right = new FormAttachment( middle, -margin );
    fdlCreateDate.top = new FormAttachment( wCreateUser, margin );
    wlCreateDate.setLayoutData( fdlCreateDate );
    wCreateDate = new Text( wPipelineComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wCreateDate );
    wCreateDate.setEditable( false );
    wCreateDate.addModifyListener( lsMod );
    FormData fdCreateDate = new FormData();
    fdCreateDate.left = new FormAttachment( middle, 0 );
    fdCreateDate.top = new FormAttachment( wCreateUser, margin );
    fdCreateDate.right = new FormAttachment( 100, 0 );
    wCreateDate.setLayoutData( fdCreateDate );

    // Modified User:
    Label wlModUser = new Label( wPipelineComp, SWT.RIGHT );
    wlModUser.setText( BaseMessages.getString( PKG, "PipelineDialog.LastModifiedUser.Label" ) );
    props.setLook( wlModUser );
    FormData fdlModUser = new FormData();
    fdlModUser.left = new FormAttachment( 0, 0 );
    fdlModUser.right = new FormAttachment( middle, -margin );
    fdlModUser.top = new FormAttachment( wCreateDate, margin );
    wlModUser.setLayoutData( fdlModUser );
    wModUser = new Text( wPipelineComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wModUser );
    wModUser.setEditable( false );
    wModUser.addModifyListener( lsMod );
    FormData fdModUser = new FormData();
    fdModUser.left = new FormAttachment( middle, 0 );
    fdModUser.top = new FormAttachment( wCreateDate, margin );
    fdModUser.right = new FormAttachment( 100, 0 );
    wModUser.setLayoutData( fdModUser );

    // Modified Date:
    Label wlModDate = new Label( wPipelineComp, SWT.RIGHT );
    wlModDate.setText( BaseMessages.getString( PKG, "PipelineDialog.LastModifiedDate.Label" ) );
    props.setLook( wlModDate );
    FormData fdlModDate = new FormData();
    fdlModDate.left = new FormAttachment( 0, 0 );
    fdlModDate.right = new FormAttachment( middle, -margin );
    fdlModDate.top = new FormAttachment( wModUser, margin );
    wlModDate.setLayoutData( fdlModDate );
    wModDate = new Text( wPipelineComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wModDate );
    wModDate.setEditable( false );
    wModDate.addModifyListener( lsMod );
    FormData fdModDate = new FormData();
    fdModDate.left = new FormAttachment( middle, 0 );
    fdModDate.top = new FormAttachment( wModUser, margin );
    fdModDate.right = new FormAttachment( 100, 0 );
    wModDate.setLayoutData( fdModDate );

    FormData fdPipelineComp = new FormData();
    fdPipelineComp.left = new FormAttachment( 0, 0 );
    fdPipelineComp.top = new FormAttachment( 0, 0 );
    fdPipelineComp.right = new FormAttachment( 100, 0 );
    fdPipelineComp.bottom = new FormAttachment( 100, 0 );
    wPipelineComp.setLayoutData( fdPipelineComp );

    wPipelineComp.layout();
    wPipelineTab.setControl( wPipelineComp );

    // ///////////////////////////////////////////////////////////
    // / END OF PIPELINE TAB
    // ///////////////////////////////////////////////////////////
  }

  private void updateNameFilenameSync( Event event ) {
    String name = wPipelineName.getText();
    String filename = wPipelineFilename.getText();
    boolean sync = wNameFilenameSync.getSelection();

    String actualName = PipelineMeta.extractNameFromFilename( sync, name, filename, PipelineMeta.PIPELINE_EXTENSION );
    wPipelineName.setEnabled( !sync );
    wPipelineName.setEditable( !sync );

    wPipelineName.setText( Const.NVL(actualName, "") );
  }

  private void addParamTab() {
    // ////////////////////////
    // START OF PARAM TAB
    // /
    wParamTab = new CTabItem( wTabFolder, SWT.NONE );
    wParamTab.setText( BaseMessages.getString( PKG, "PipelineDialog.ParamTab.Label" ) );

    FormLayout paramLayout = new FormLayout();
    paramLayout.marginWidth = props.getMargin();
    paramLayout.marginHeight = props.getMargin();

    Composite wParamComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wParamComp );
    wParamComp.setLayout( paramLayout );

    Label wlFields = new Label( wParamComp, SWT.RIGHT );
    wlFields.setText( BaseMessages.getString( PKG, "PipelineDialog.Parameters.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( 0, 0 );
    wlFields.setLayoutData( fdlFields );

    final int FieldsCols = 3;
    final int FieldsRows = pipelineMeta.listParameters().length;

    ColumnInfo[] colinf = new ColumnInfo[ FieldsCols ];
    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "PipelineDialog.ColumnInfo.Parameter.Label" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    colinf[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "PipelineDialog.ColumnInfo.Default.Label" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    colinf[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "PipelineDialog.ColumnInfo.Description.Label" ),
        ColumnInfo.COLUMN_TYPE_TEXT, false );

    wParamFields =
      new TableView(
        variables, wParamComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( 100, 0 );
    wParamFields.setLayoutData( fdFields );

    FormData fdDepComp = new FormData();
    fdDepComp.left = new FormAttachment( 0, 0 );
    fdDepComp.top = new FormAttachment( 0, 0 );
    fdDepComp.right = new FormAttachment( 100, 0 );
    fdDepComp.bottom = new FormAttachment( 100, 0 );
    wParamComp.setLayoutData( fdDepComp );

    wParamComp.layout();
    wParamTab.setControl( wParamComp );

    // ///////////////////////////////////////////////////////////
    // / END OF PARAM TAB
    // ///////////////////////////////////////////////////////////
  }

  private void addMonitoringTab() {
    // ////////////////////////
    // START OF MONITORING TAB///
    // /
    wMonitorTab = new CTabItem( wTabFolder, SWT.NONE );
    wMonitorTab.setText( BaseMessages.getString( PKG, "PipelineDialog.MonitorTab.Label" ) );

    Composite wMonitorComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wMonitorComp );

    FormLayout monitorLayout = new FormLayout();
    monitorLayout.marginWidth = Const.FORM_MARGIN;
    monitorLayout.marginHeight = Const.FORM_MARGIN;
    wMonitorComp.setLayout( monitorLayout );

    //
    // Enable transform performance monitoring?
    //
    Label wlEnableTransformPerfMonitor = new Label( wMonitorComp, SWT.LEFT );
    wlEnableTransformPerfMonitor.setText( BaseMessages.getString( PKG, "PipelineDialog.TransformPerformanceMonitoring.Label" ) );
    props.setLook( wlEnableTransformPerfMonitor );
    FormData fdlSchemaName = new FormData();
    fdlSchemaName.left = new FormAttachment( 0, 0 );
    fdlSchemaName.right = new FormAttachment( middle, -margin );
    fdlSchemaName.top = new FormAttachment( 0, 0 );
    wlEnableTransformPerfMonitor.setLayoutData( fdlSchemaName );
    wEnableTransformPerfMonitor = new Button( wMonitorComp, SWT.CHECK );
    props.setLook( wEnableTransformPerfMonitor );
    FormData fdEnableTransformPerfMonitor = new FormData();
    fdEnableTransformPerfMonitor.left = new FormAttachment( middle, 0 );
    fdEnableTransformPerfMonitor.right = new FormAttachment( 100, 0 );
    fdEnableTransformPerfMonitor.top = new FormAttachment( 0, 0 );
    wEnableTransformPerfMonitor.setLayoutData( fdEnableTransformPerfMonitor );
    wEnableTransformPerfMonitor.addSelectionListener( lsModSel );

    //
    // Transform performance interval
    //
    Label wlTransformPerfInterval = new Label( wMonitorComp, SWT.LEFT );
    wlTransformPerfInterval.setText( BaseMessages.getString( PKG, "PipelineDialog.TransformPerformanceInterval.Label" ) );
    props.setLook( wlTransformPerfInterval );
    FormData fdlTransformPerfInterval = new FormData();
    fdlTransformPerfInterval.left = new FormAttachment( 0, 0 );
    fdlTransformPerfInterval.right = new FormAttachment( middle, -margin );
    fdlTransformPerfInterval.top = new FormAttachment( wEnableTransformPerfMonitor, margin );
    wlTransformPerfInterval.setLayoutData( fdlTransformPerfInterval );
    wTransformPerfInterval = new Text( wMonitorComp, SWT.LEFT | SWT.BORDER | SWT.SINGLE );
    props.setLook( wTransformPerfInterval );
    FormData fdTransformPerfInterval = new FormData();
    fdTransformPerfInterval.left = new FormAttachment( middle, 0 );
    fdTransformPerfInterval.right = new FormAttachment( 100, 0 );
    fdTransformPerfInterval.top = new FormAttachment( wEnableTransformPerfMonitor, margin );
    wTransformPerfInterval.setLayoutData( fdTransformPerfInterval );
    wTransformPerfInterval.addModifyListener( lsMod );

    //
    // Transform performance interval
    //
    Label wlTransformPerfMaxSize = new Label( wMonitorComp, SWT.LEFT );
    wlTransformPerfMaxSize.setText( BaseMessages.getString( PKG, "PipelineDialog.TransformPerformanceMaxSize.Label" ) );
    wlTransformPerfMaxSize.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.TransformPerformanceMaxSize.Tooltip" ) );
    props.setLook( wlTransformPerfMaxSize );
    FormData fdlTransformPerfMaxSize = new FormData();
    fdlTransformPerfMaxSize.left = new FormAttachment( 0, 0 );
    fdlTransformPerfMaxSize.right = new FormAttachment( middle, -margin );
    fdlTransformPerfMaxSize.top = new FormAttachment( wTransformPerfInterval, margin );
    wlTransformPerfMaxSize.setLayoutData( fdlTransformPerfMaxSize );
    wTransformPerfMaxSize = new TextVar( variables, wMonitorComp, SWT.LEFT | SWT.BORDER | SWT.SINGLE );
    wTransformPerfMaxSize.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.TransformPerformanceMaxSize.Tooltip" ) );
    props.setLook( wTransformPerfMaxSize );
    FormData fdTransformPerfMaxSize = new FormData();
    fdTransformPerfMaxSize.left = new FormAttachment( middle, 0 );
    fdTransformPerfMaxSize.right = new FormAttachment( 100, 0 );
    fdTransformPerfMaxSize.top = new FormAttachment( wTransformPerfInterval, margin );
    wTransformPerfMaxSize.setLayoutData( fdTransformPerfMaxSize );
    wTransformPerfMaxSize.addModifyListener( lsMod );

    FormData fdMonitorComp = new FormData();
    fdMonitorComp.left = new FormAttachment( 0, 0 );
    fdMonitorComp.top = new FormAttachment( 0, 0 );
    fdMonitorComp.right = new FormAttachment( 100, 0 );
    fdMonitorComp.bottom = new FormAttachment( 100, 0 );
    wMonitorComp.setLayoutData( fdMonitorComp );

    wMonitorComp.layout();
    wMonitorTab.setControl( wMonitorComp );

    // ///////////////////////////////////////////////////////////
    // / END OF MONITORING TAB
    // ///////////////////////////////////////////////////////////

  }

  public void dispose() {
    shell.dispose();
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   */
  public void getData() {
    wPipelineName.setText( Const.NVL( pipelineMeta.getName(), "" ) );
    wNameFilenameSync.setSelection( pipelineMeta.isNameSynchronizedWithFilename() );
    wPipelineFilename.setText( Const.NVL( pipelineMeta.getFilename(), "" ) );
    updateNameFilenameSync( null );
    wPipelineDescription.setText( Const.NVL( pipelineMeta.getDescription(), "" ) );
    wExtendedDescription.setText( Const.NVL( pipelineMeta.getExtendedDescription(), "" ) );
    wPipelineVersion.setText( Const.NVL( pipelineMeta.getPipelineVersion(), "" ) );
    wPipelineStatus.select( pipelineMeta.getPipelineStatus() - 1 );

    if ( pipelineMeta.getCreatedUser() != null ) {
      wCreateUser.setText( pipelineMeta.getCreatedUser() );
    }
    if ( pipelineMeta.getCreatedDate() != null ) {
      wCreateDate.setText( pipelineMeta.getCreatedDate().toString() );
    }

    if ( pipelineMeta.getModifiedUser() != null ) {
      wModUser.setText( pipelineMeta.getModifiedUser() );
    }
    if ( pipelineMeta.getModifiedDate() != null ) {
      wModDate.setText( pipelineMeta.getModifiedDate().toString() );
    }

    // The named parameters
    String[] parameters = pipelineMeta.listParameters();
    for ( int idx = 0; idx < parameters.length; idx++ ) {
      TableItem item = wParamFields.table.getItem( idx );

      String defValue;
      try {
        defValue = pipelineMeta.getParameterDefault( parameters[ idx ] );
      } catch ( UnknownParamException e ) {
        defValue = "";
      }
      String description;
      try {
        description = pipelineMeta.getParameterDescription( parameters[ idx ] );
      } catch ( UnknownParamException e ) {
        description = "";
      }

      item.setText( 1, parameters[ idx ] );
      item.setText( 2, Const.NVL( defValue, "" ) );
      item.setText( 3, Const.NVL( description, "" ) );
    }

    wParamFields.setRowNums();
    wParamFields.optWidth( true );

    // Performance monitoring tab:
    //
    wEnableTransformPerfMonitor.setSelection( pipelineMeta.isCapturingTransformPerformanceSnapShots() );
    wTransformPerfInterval.setText( Long.toString( pipelineMeta.getTransformPerformanceCapturingDelay() ) );
    wTransformPerfMaxSize.setText( Const.NVL( pipelineMeta.getTransformPerformanceCapturingSizeLimit(), "" ) );

    wPipelineName.selectAll();
    wPipelineName.setFocus();

    for ( IPipelineDialogPlugin extraTab : extraTabs ) {
      try {
        extraTab.getData( pipelineMeta );
      } catch ( Exception e ) {
        new ErrorDialog( shell, "Error", "Error adding extra plugin tab", e );
      }
    }
  }

  private void cancel() {
    props.setScreen( new WindowProperty( shell ) );
    pipelineMeta = null;
    dispose();
  }

  private void ok() {
    boolean OK = true;

    pipelineMeta.setName( wPipelineName.getText() );
    pipelineMeta.setNameSynchronizedWithFilename( wNameFilenameSync.getSelection() );
    pipelineMeta.setDescription( wPipelineDescription.getText() );
    pipelineMeta.setExtendedDescription( wExtendedDescription.getText() );
    pipelineMeta.setPipelineVersion( wPipelineVersion.getText() );

    if ( wPipelineStatus.getSelectionIndex() != 2 ) {
      pipelineMeta.setPipelineStatus( wPipelineStatus.getSelectionIndex() + 1 );
    } else {
      pipelineMeta.setPipelineStatus( -1 );
    }

    // Clear and add parameters
    pipelineMeta.removeAllParameters();
    int nrNonEmptyFields = wParamFields.nrNonEmpty();
    for ( int i = 0; i < nrNonEmptyFields; i++ ) {
      TableItem item = wParamFields.getNonEmpty( i );

      try {
        pipelineMeta.addParameterDefinition( item.getText( 1 ), item.getText( 2 ), item.getText( 3 ) );
      } catch ( DuplicateParamException e ) {
        // Ignore the duplicate parameter.
      }
    }

    // Performance monitoring tab:
    //
    pipelineMeta.setCapturingTransformPerformanceSnapShots( wEnableTransformPerfMonitor.getSelection() );
    pipelineMeta.setTransformPerformanceCapturingSizeLimit( wTransformPerfMaxSize.getText() );

    try {
      long transformPerformanceCapturingDelay = Long.parseLong( wTransformPerfInterval.getText() );
      // values equal or less than zero cause problems during monitoring
      if ( transformPerformanceCapturingDelay <= 0 && pipelineMeta.isCapturingTransformPerformanceSnapShots() ) {
        throw new HopException();
      } else {
        if ( transformPerformanceCapturingDelay <= 0 ) {
          // PDI-4848: Default to 1 second if transform performance monitoring is disabled
          transformPerformanceCapturingDelay = 1000;
        }
        pipelineMeta.setTransformPerformanceCapturingDelay( transformPerformanceCapturingDelay );
      }
    } catch ( Exception e ) {
      MessageBox mb = new MessageBox( shell, SWT.ICON_ERROR | SWT.OK );
      mb.setText( BaseMessages.getString( PKG, "PipelineDialog.InvalidTransformPerfIntervalNumber.DialogTitle" ) );
      mb.setMessage( BaseMessages.getString( PKG, "PipelineDialog.InvalidTransformPerfIntervalNumber.DialogMessage" ) );
      mb.open();
      wTransformPerfInterval.setFocus();
      wTransformPerfInterval.selectAll();
      OK = false;
    }

    for ( IPipelineDialogPlugin extraTab : extraTabs ) {
      try {
        extraTab.ok( pipelineMeta );
      } catch ( Exception e ) {
        new ErrorDialog( shell, "Error", "Error getting information from extra plugin tab", e );
      }
    }

    if ( OK ) {
      pipelineMeta.setChanged( changed || pipelineMeta.hasChanged() );
      dispose();
    }
  }


  private void setCurrentTab( Tabs currentTab ) {

    switch ( currentTab ) {
      case PARAM_TAB:
        wTabFolder.setSelection( wParamTab );
        break;
      case MISC_TAB:
        wTabFolder.setSelection( wMiscTab );
        break;
      case MONITOR_TAB:
        wTabFolder.setSelection( wMonitorTab );
        break;
      case EXTRA_TAB:
        if ( extraTabs.size() > 0 ) {
          wTabFolder.setSelection( extraTabs.get( 0 ).getTab() );
        }
        break;
      case PIPELINE_TAB:
      default:
        wTabFolder.setSelection( wPipelineTab );
        break;
    }
  }
}
