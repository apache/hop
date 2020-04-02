// CHECKSTYLE:FileLength:OFF
/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.pipeline.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ChannelLogTable;
import org.apache.hop.core.logging.LogStatus;
import org.apache.hop.core.logging.LogTableField;
import org.apache.hop.core.logging.LogTableInterface;
import org.apache.hop.core.logging.MetricsLogTable;
import org.apache.hop.core.logging.PerformanceLogTable;
import org.apache.hop.core.logging.TransformLogTable;
import org.apache.hop.core.logging.PipelineLogTable;
import org.apache.hop.core.parameters.DuplicateParamException;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineDependency;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transforms.tableinput.TableInputMeta;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.database.dialog.DatabaseDialog;
import org.apache.hop.ui.core.database.dialog.SQLEditor;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.FieldDisabledListener;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
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
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;

//import org.apache.hop.pipeline.transforms.databaselookup.DatabaseLookupMeta;


public class PipelineDialog extends Dialog {
  public static final int LOG_INDEX_PIPELINE = 0;
  public static final int LOG_INDEX_TRANSFORM = 1;
  public static final int LOG_INDEX_PERFORMANCE = 2;
  public static final int LOG_INDEX_CHANNEL = 3;
  public static final int LOG_INDEX_METRICS = 4;

  private static Class<?> PKG = PipelineDialog.class; // for i18n purposes, needed by Translator!!

  public enum Tabs {
    PIPELINE_TAB, PARAM_TAB, LOG_TAB, DATE_TAB, DEP_TAB, MISC_TAB, MONITOR_TAB, EXTRA_TAB,
  }

  private CTabFolder wTabFolder;
  private FormData fdTabFolder;

  private CTabItem wPipelineTab, wParamTab, wLogTab, wDateTab, wDepTab, wMiscTab, wMonitorTab;

  private Text wPipelineName;

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

  private MetaSelectionLine<DatabaseMeta> wLogConnection;

  private TextVar wLogSizeLimit;

  private TextVar wLogTimeout;

  private int previousLogTableIndex = -1;
  private TableView wOptionFields;
  private TextVar wLogTable;
  private TextVar wLogSchema;
  private TextVar wLogInterval;

  private CCombo wMaxdateconnection;
  private Text wMaxdatetable;
  private Text wMaxdatefield;
  private Text wMaxdateoffset;
  private Text wMaxdatediff;

  private TableView wFields;

  private TableView wParamFields;

  private Button wUniqueConnections;

  private Button wOK, wGet, wSQL, wCancel;
  private FormData fdGet;
  private Listener lsOK, lsGet, lsSQL, lsCancel;

  private PipelineMeta pipelineMeta;
  private Shell shell;

  private SelectionAdapter lsDef;

  private ModifyListener lsMod;
  private PropsUI props;

  private int middle;

  private int margin;

  private Button wManageThreads;

  private boolean directoryChangeAllowed;

  private Label wlDirectory;

  private Button wEnableTransformPerfMonitor;

  private Text wTransformPerfInterval;

  private CCombo wPipelineType;

  private Tabs currentTab = null;

  private String[] connectionNames;

  protected boolean changed;
  private List wLogTypeList;
  private Composite wLogOptionsComposite;
  private Composite wLogComp;
  private PipelineLogTable pipelineLogTable;
  private PerformanceLogTable performanceLogTable;
  private ChannelLogTable channelLogTable;
  private TransformLogTable transformLogTable;
  private MetricsLogTable metricsLogTable;

  private DatabaseDialog databaseDialog;
  private SelectionAdapter lsModSel;
  private TextVar wTransformPerfMaxSize;

  private ArrayList<PipelineDialogPluginInterface> extraTabs;

  public PipelineDialog( Shell parent, int style, PipelineMeta pipelineMeta, Tabs currentTab ) {
    this( parent, style, pipelineMeta );
    this.currentTab = currentTab;
  }

  public PipelineDialog( Shell parent, int style, PipelineMeta pipelineMeta ) {
    super( parent, style );
    this.props = PropsUI.getInstance();
    this.pipelineMeta = pipelineMeta;

    directoryChangeAllowed = true;
    changed = false;

    // Create a copy of the pipeline log table object
    //
    pipelineLogTable = (PipelineLogTable) pipelineMeta.getPipelineLogTable().clone();
    performanceLogTable = (PerformanceLogTable) pipelineMeta.getPerformanceLogTable().clone();
    channelLogTable = (ChannelLogTable) pipelineMeta.getChannelLogTable().clone();
    transformLogTable = (TransformLogTable) pipelineMeta.getTransformLogTable().clone();
    metricsLogTable = (MetricsLogTable) pipelineMeta.getMetricsLogTable().clone();
  }

  public PipelineMeta open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImagePipelineGraph() );

    lsMod = new ModifyListener() {
      public void modifyText( ModifyEvent e ) {
        changed = true;
      }
    };
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
    wTabFolder.setSimple( false );

    addPipelineTab();
    addParamTab();
    addLogTab();
    addDateTab();
    addDepTab();
    addMiscTab();
    addMonitoringTab();

    // See if there are any other tabs to be added...
    extraTabs = new ArrayList<PipelineDialogPluginInterface>();
    java.util.List<PluginInterface> pipelineDialogPlugins =
      PluginRegistry.getInstance().getPlugins( PipelineDialogPluginType.class );
    for ( PluginInterface pipelineDialogPlugin : pipelineDialogPlugins ) {
      try {
        PipelineDialogPluginInterface extraTab =
          (PipelineDialogPluginInterface) PluginRegistry.getInstance().loadClass( pipelineDialogPlugin );
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
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wSQL = new Button( shell, SWT.PUSH );
    wSQL.setText( BaseMessages.getString( PKG, "System.Button.SQL" ) );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOK, wSQL, wCancel }, props.getMargin(), null );

    // Add listeners
    lsOK = new Listener() {
      public void handleEvent( Event e ) {
        ok();
      }
    };
    lsGet = new Listener() {
      public void handleEvent( Event e ) {
        get();
      }
    };
    lsSQL = new Listener() {
      public void handleEvent( Event e ) {
        sql();
      }
    };
    lsCancel = new Listener() {
      public void handleEvent( Event e ) {
        cancel();
      }
    };

    wOK.addListener( SWT.Selection, lsOK );
    wGet.addListener( SWT.Selection, lsGet );
    wSQL.addListener( SWT.Selection, lsSQL );
    wCancel.addListener( SWT.Selection, lsCancel );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wPipelineName.addSelectionListener( lsDef );
    wPipelineDescription.addSelectionListener( lsDef );
    wPipelineVersion.addSelectionListener( lsDef );
    wMaxdatetable.addSelectionListener( lsDef );
    wMaxdatefield.addSelectionListener( lsDef );
    wMaxdateoffset.addSelectionListener( lsDef );
    wMaxdatediff.addSelectionListener( lsDef );
    wUniqueConnections.addSelectionListener( lsDef );
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

  private DatabaseDialog getDatabaseDialog() {
    if ( databaseDialog != null ) {
      return databaseDialog;
    }
    databaseDialog = new DatabaseDialog( shell );
    return databaseDialog;
  }

  private void addPipelineTab() {
    // ////////////////////////
    // START OF PIPELINE TAB///
    // /
    wPipelineTab = new CTabItem( wTabFolder, SWT.NONE );
    wPipelineTab.setText( BaseMessages.getString( PKG, "PipelineDialog.PipelineTab.Label" ) );

    Composite wPipelineComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wPipelineComp );

    FormLayout transLayout = new FormLayout();
    transLayout.marginWidth = Const.FORM_MARGIN;
    transLayout.marginHeight = Const.FORM_MARGIN;
    wPipelineComp.setLayout( transLayout );

    // Pipeline name:
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

    // Pipeline name:
    Label wlPipelineFilename = new Label( wPipelineComp, SWT.RIGHT );
    wlPipelineFilename.setText( BaseMessages.getString( PKG, "PipelineDialog.PipelineFilename.Label" ) );
    props.setLook( wlPipelineFilename );
    FormData fdlPipelineFilename = new FormData();
    fdlPipelineFilename.left = new FormAttachment( 0, 0 );
    fdlPipelineFilename.right = new FormAttachment( middle, -margin );
    fdlPipelineFilename.top = new FormAttachment( wPipelineName, margin );
    wlPipelineFilename.setLayoutData( fdlPipelineFilename );
    wPipelineFilename = new Text( wPipelineComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPipelineFilename );
    wPipelineFilename.addModifyListener( lsMod );
    FormData fdPipelineFilename = new FormData();
    fdPipelineFilename.left = new FormAttachment( middle, 0 );
    fdPipelineFilename.top = new FormAttachment( wPipelineName, margin );
    fdPipelineFilename.right = new FormAttachment( 100, 0 );
    wPipelineFilename.setLayoutData( fdPipelineFilename );
    wPipelineFilename.setEditable( false );
    wPipelineFilename.setBackground( GUIResource.getInstance().getColorLightGray() );

    // Pipeline description:
    Label wlPipelineDescription = new Label( wPipelineComp, SWT.RIGHT );
    wlPipelineDescription.setText( BaseMessages.getString( PKG, "PipelineDialog.PipelineDescription.Label" ) );
    props.setLook( wlPipelineDescription );
    FormData fdlPipelineDescription = new FormData();
    fdlPipelineDescription.left = new FormAttachment( 0, 0 );
    fdlPipelineDescription.right = new FormAttachment( middle, -margin );
    fdlPipelineDescription.top = new FormAttachment( wPipelineFilename, margin );
    wlPipelineDescription.setLayoutData( fdlPipelineDescription );
    wPipelineDescription = new Text( wPipelineComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPipelineDescription );
    wPipelineDescription.addModifyListener( lsMod );
    FormData fdPipelineDescription = new FormData();
    fdPipelineDescription.left = new FormAttachment( middle, 0 );
    fdPipelineDescription.top = new FormAttachment( wPipelineFilename, margin );
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
        pipelineMeta, wParamComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

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

  private void addLogTab() {
    // ////////////////////////
    // START OF LOG TAB///
    // /
    wLogTab = new CTabItem( wTabFolder, SWT.NONE );
    wLogTab.setText( BaseMessages.getString( PKG, "PipelineDialog.LogTab.Label" ) );

    FormLayout LogLayout = new FormLayout();
    LogLayout.marginWidth = props.getMargin();
    LogLayout.marginHeight = props.getMargin();

    wLogComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wLogComp );
    wLogComp.setLayout( LogLayout );

    // Add a log type List on the left hand side...
    //
    wLogTypeList = new List( wLogComp, SWT.SINGLE | SWT.V_SCROLL | SWT.H_SCROLL | SWT.BORDER );
    props.setLook( wLogTypeList );

    wLogTypeList.add( BaseMessages.getString( PKG, "PipelineDialog.LogTableType.Pipeline" ) ); // Index 0
    wLogTypeList.add( BaseMessages.getString( PKG, "PipelineDialog.LogTableType.Transform" ) ); // Index 1
    wLogTypeList.add( BaseMessages.getString( PKG, "PipelineDialog.LogTableType.Performance" ) ); // Index 2
    wLogTypeList.add( BaseMessages.getString( PKG, "PipelineDialog.LogTableType.LoggingChannels" ) ); // Index 3
    wLogTypeList.add( BaseMessages.getString( PKG, "PipelineDialog.LogTableType.Metrics" ) ); // Index 3

    FormData fdLogTypeList = new FormData();
    fdLogTypeList.left = new FormAttachment( 0, 0 );
    fdLogTypeList.top = new FormAttachment( 0, 0 );
    fdLogTypeList.right = new FormAttachment( middle / 2, 0 );
    fdLogTypeList.bottom = new FormAttachment( 100, 0 );
    wLogTypeList.setLayoutData( fdLogTypeList );

    wLogTypeList.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        showLogTypeOptions( wLogTypeList.getSelectionIndex() );
      }
    } );

    // On the right side we see a dynamic area : a composite...
    //
    wLogOptionsComposite = new Composite( wLogComp, SWT.BORDER );

    FormLayout logOptionsLayout = new FormLayout();
    logOptionsLayout.marginWidth = props.getMargin();
    logOptionsLayout.marginHeight = props.getMargin();
    wLogOptionsComposite.setLayout( logOptionsLayout );

    props.setLook( wLogOptionsComposite );
    FormData fdLogOptionsComposite = new FormData();
    fdLogOptionsComposite.left = new FormAttachment( wLogTypeList, margin );
    fdLogOptionsComposite.top = new FormAttachment( 0, 0 );
    fdLogOptionsComposite.right = new FormAttachment( 100, 0 );
    fdLogOptionsComposite.bottom = new FormAttachment( 100, 0 );
    wLogOptionsComposite.setLayoutData( fdLogOptionsComposite );

    FormData fdLogComp = new FormData();
    fdLogComp.left = new FormAttachment( 0, 0 );
    fdLogComp.top = new FormAttachment( 0, 0 );
    fdLogComp.right = new FormAttachment( 100, 0 );
    fdLogComp.bottom = new FormAttachment( 100, 0 );
    wLogComp.setLayoutData( fdLogComp );

    wLogComp.layout();
    wLogTab.setControl( wLogComp );

    // ///////////////////////////////////////////////////////////
    // / END OF LOG TAB
    // ///////////////////////////////////////////////////////////
  }

  private void showLogTypeOptions( int index ) {

    if ( index != previousLogTableIndex ) {

      // Remember the that was entered data...
      //
      switch ( previousLogTableIndex ) {
        case LOG_INDEX_PIPELINE:
          getPipelineLogTableOptions();
          break;
        case LOG_INDEX_PERFORMANCE:
          getPerformanceLogTableOptions();
          break;
        case LOG_INDEX_CHANNEL:
          getChannelLogTableOptions();
          break;
        case LOG_INDEX_TRANSFORM:
          getTransformLogTableOptions();
          break;
        case LOG_INDEX_METRICS:
          getMetricsLogTableOptions();
          break;
        default:
          break;
      }

      // clean the log options composite...
      //
      for ( Control control : wLogOptionsComposite.getChildren() ) {
        control.dispose();
      }

      switch ( index ) {
        case LOG_INDEX_PIPELINE:
          showPipelineLogTableOptions();
          break;
        case LOG_INDEX_PERFORMANCE:
          showPerformanceLogTableOptions();
          break;
        case LOG_INDEX_CHANNEL:
          showChannelLogTableOptions();
          break;
        case LOG_INDEX_TRANSFORM:
          showTransformLogTableOptions();
          break;
        case LOG_INDEX_METRICS:
          showMetricsLogTableOptions();
          break;
        default:
          break;
      }
    }
  }

  private void getPipelineLogTableOptions() {

    if ( previousLogTableIndex == LOG_INDEX_PIPELINE ) {
      // The connection...
      //
      pipelineLogTable.setConnectionName( wLogConnection.getText() );
      pipelineLogTable.setSchemaName( wLogSchema.getText() );
      pipelineLogTable.setTableName( wLogTable.getText() );
      pipelineLogTable.setLogInterval( wLogInterval.getText() );
      pipelineLogTable.setLogSizeLimit( wLogSizeLimit.getText() );
      pipelineLogTable.setTimeoutInDays( wLogTimeout.getText() );

      for ( int i = 0; i < pipelineLogTable.getFields().size(); i++ ) {
        TableItem item = wOptionFields.table.getItem( i );

        LogTableField field = pipelineLogTable.getFields().get( i );
        field.setEnabled( item.getChecked() );
        field.setFieldName( item.getText( 1 ) );
        if ( field.isSubjectAllowed() ) {
          field.setSubject( pipelineMeta.findTransform( item.getText( 2 ) ) );
        }
      }
    }
  }

  private Control addDBSchemaTableLogOptions( LogTableInterface logTable ) {

    // Log table connection...
    //
    wLogConnection = new MetaSelectionLine<DatabaseMeta>( pipelineMeta, pipelineMeta.getMetaStore(), DatabaseMeta.class,
      wLogOptionsComposite, SWT.NONE,
      BaseMessages.getString( PKG, "PipelineDialog.LogConnection.Label" ),
      BaseMessages.getString( PKG, "PipelineDialog.LogConnection.Tooltip", logTable.getConnectionNameVariable() )
    );
    wLogConnection.addToConnectionLine( wLogOptionsComposite, null, logTable.getDatabaseMeta(), lsMod );
    connectionNames = wLogConnection.getItems();

    // Log schema ...
    //
    Label wlLogSchema = new Label( wLogOptionsComposite, SWT.RIGHT );
    wlLogSchema.setText( BaseMessages.getString( PKG, "PipelineDialog.LogSchema.Label" ) );
    props.setLook( wlLogSchema );
    FormData fdlLogSchema = new FormData();
    fdlLogSchema.left = new FormAttachment( 0, 0 );
    fdlLogSchema.right = new FormAttachment( middle, -margin );
    fdlLogSchema.top = new FormAttachment( wLogConnection, margin );
    wlLogSchema.setLayoutData( fdlLogSchema );
    wLogSchema = new TextVar( pipelineMeta, wLogOptionsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLogSchema );
    wLogSchema.addModifyListener( lsMod );
    FormData fdLogSchema = new FormData();
    fdLogSchema.left = new FormAttachment( middle, 0 );
    fdLogSchema.top = new FormAttachment( wLogConnection, margin );
    fdLogSchema.right = new FormAttachment( 100, 0 );
    wLogSchema.setLayoutData( fdLogSchema );
    wLogSchema.setText( Const.NVL( logTable.getSchemaName(), "" ) );
    wLogSchema.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.LogSchema.Tooltip", logTable
      .getSchemaNameVariable() ) );

    // Log table...
    //
    Label wlLogtable = new Label( wLogOptionsComposite, SWT.RIGHT );
    wlLogtable.setText( BaseMessages.getString( PKG, "PipelineDialog.Logtable.Label" ) );
    props.setLook( wlLogtable );
    FormData fdlLogtable = new FormData();
    fdlLogtable.left = new FormAttachment( 0, 0 );
    fdlLogtable.right = new FormAttachment( middle, -margin );
    fdlLogtable.top = new FormAttachment( wLogSchema, margin );
    wlLogtable.setLayoutData( fdlLogtable );
    wLogTable = new TextVar( pipelineMeta, wLogOptionsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLogTable );
    wLogTable.addModifyListener( lsMod );
    FormData fdLogtable = new FormData();
    fdLogtable.left = new FormAttachment( middle, 0 );
    fdLogtable.top = new FormAttachment( wLogSchema, margin );
    fdLogtable.right = new FormAttachment( 100, 0 );
    wLogTable.setLayoutData( fdLogtable );
    wLogTable.setText( Const.NVL( logTable.getTableName(), "" ) );
    wLogTable.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.LogTable.Tooltip", logTable
      .getTableNameVariable() ) );

    return wLogTable;

  }

  private void showPipelineLogTableOptions() {

    previousLogTableIndex = LOG_INDEX_PIPELINE;

    addDBSchemaTableLogOptions( pipelineLogTable );

    // Log interval...
    //
    Label wlLogInterval = new Label( wLogOptionsComposite, SWT.RIGHT );
    wlLogInterval.setText( BaseMessages.getString( PKG, "PipelineDialog.LogInterval.Label" ) );
    props.setLook( wlLogInterval );
    FormData fdlLogInterval = new FormData();
    fdlLogInterval.left = new FormAttachment( 0, 0 );
    fdlLogInterval.right = new FormAttachment( middle, -margin );
    fdlLogInterval.top = new FormAttachment( wLogTable, margin );
    wlLogInterval.setLayoutData( fdlLogInterval );
    wLogInterval = new TextVar( pipelineMeta, wLogOptionsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLogInterval );
    wLogInterval.addModifyListener( lsMod );
    FormData fdLogInterval = new FormData();
    fdLogInterval.left = new FormAttachment( middle, 0 );
    fdLogInterval.top = new FormAttachment( wLogTable, margin );
    fdLogInterval.right = new FormAttachment( 100, 0 );
    wLogInterval.setLayoutData( fdLogInterval );
    wLogInterval.setText( Const.NVL( pipelineLogTable.getLogInterval(), "" ) );

    // The log timeout in days
    //
    Label wlLogTimeout = new Label( wLogOptionsComposite, SWT.RIGHT );
    wlLogTimeout.setText( BaseMessages.getString( PKG, "PipelineDialog.LogTimeout.Label" ) );
    wlLogTimeout.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.LogTimeout.Tooltip" ) );
    props.setLook( wlLogTimeout );
    FormData fdlLogTimeout = new FormData();
    fdlLogTimeout.left = new FormAttachment( 0, 0 );
    fdlLogTimeout.right = new FormAttachment( middle, -margin );
    fdlLogTimeout.top = new FormAttachment( wLogInterval, margin );
    wlLogTimeout.setLayoutData( fdlLogTimeout );
    wLogTimeout = new TextVar( pipelineMeta, wLogOptionsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLogTimeout.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.LogTimeout.Tooltip" ) );
    props.setLook( wLogTimeout );
    wLogTimeout.addModifyListener( lsMod );
    FormData fdLogTimeout = new FormData();
    fdLogTimeout.left = new FormAttachment( middle, 0 );
    fdLogTimeout.top = new FormAttachment( wLogInterval, margin );
    fdLogTimeout.right = new FormAttachment( 100, 0 );
    wLogTimeout.setLayoutData( fdLogTimeout );
    wLogTimeout.setText( Const.NVL( pipelineLogTable.getTimeoutInDays(), "" ) );

    // The log size limit
    //
    Label wlLogSizeLimit = new Label( wLogOptionsComposite, SWT.RIGHT );
    wlLogSizeLimit.setText( BaseMessages.getString( PKG, "PipelineDialog.LogSizeLimit.Label" ) );
    wlLogSizeLimit.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.LogSizeLimit.Tooltip" ) );
    props.setLook( wlLogSizeLimit );
    FormData fdlLogSizeLimit = new FormData();
    fdlLogSizeLimit.left = new FormAttachment( 0, 0 );
    fdlLogSizeLimit.right = new FormAttachment( middle, -margin );
    fdlLogSizeLimit.top = new FormAttachment( wLogTimeout, margin );
    wlLogSizeLimit.setLayoutData( fdlLogSizeLimit );
    wLogSizeLimit = new TextVar( pipelineMeta, wLogOptionsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLogSizeLimit.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.LogSizeLimit.Tooltip" ) );
    props.setLook( wLogSizeLimit );
    wLogSizeLimit.addModifyListener( lsMod );
    FormData fdLogSizeLimit = new FormData();
    fdLogSizeLimit.left = new FormAttachment( middle, 0 );
    fdLogSizeLimit.top = new FormAttachment( wLogTimeout, margin );
    fdLogSizeLimit.right = new FormAttachment( 100, 0 );
    wLogSizeLimit.setLayoutData( fdLogSizeLimit );
    wLogSizeLimit.setText( Const.NVL( pipelineLogTable.getLogSizeLimit(), "" ) );

    // Add the fields grid...
    //
    Label wlFields = new Label( wLogOptionsComposite, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wLogSizeLimit, margin * 2 );
    wlFields.setLayoutData( fdlFields );

    final java.util.List<LogTableField> fields = pipelineLogTable.getFields();
    final int nrRows = fields.size();

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.FieldName" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.TransformName" ),
          ColumnInfo.COLUMN_TYPE_CCOMBO, pipelineMeta.getTransformNames() ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.Description" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ), };

    FieldDisabledListener disabledListener = new FieldDisabledListener() {

      public boolean isFieldDisabled( int rowNr ) {
        if ( rowNr >= 0 && rowNr < fields.size() ) {
          LogTableField field = fields.get( rowNr );
          return !field.isSubjectAllowed();
        } else {
          return true;
        }
      }
    };

    colinf[ 1 ].setDisabledListener( disabledListener );

    wOptionFields =
      new TableView( pipelineMeta, wLogOptionsComposite, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.CHECK,
        colinf, nrRows, true, lsMod, props );

    wOptionFields.setSortable( false );

    for ( int i = 0; i < fields.size(); i++ ) {
      LogTableField field = fields.get( i );
      TableItem item = wOptionFields.table.getItem( i );
      item.setChecked( field.isEnabled() );
      item
        .setText( new String[] {
          "", Const.NVL( field.getFieldName(), "" ),
          field.getSubject() == null ? "" : field.getSubject().toString(),
          Const.NVL( field.getDescription(), "" ) } );

      // Exceptions!!!
      //
      if ( disabledListener.isFieldDisabled( i ) ) {
        item.setBackground( 2, GUIResource.getInstance().getColorLightGray() );
      }
    }

    wOptionFields.table.getColumn( 0 ).setText(
      BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.Enabled" ) );

    FormData fdOptionFields = new FormData();
    fdOptionFields.left = new FormAttachment( 0, 0 );
    fdOptionFields.top = new FormAttachment( wlFields, margin );
    fdOptionFields.right = new FormAttachment( 100, 0 );
    fdOptionFields.bottom = new FormAttachment( 100, 0 );
    wOptionFields.setLayoutData( fdOptionFields );

    wOptionFields.optWidth( true );

    wOptionFields.layout();
    wLogOptionsComposite.layout( true, true );
    wLogComp.layout( true, true );
  }

  private void getPerformanceLogTableOptions() {

    if ( previousLogTableIndex == LOG_INDEX_PERFORMANCE ) {
      // The connection...
      //
      performanceLogTable.setConnectionName( wLogConnection.getText() );
      performanceLogTable.setSchemaName( wLogSchema.getText() );
      performanceLogTable.setTableName( wLogTable.getText() );
      performanceLogTable.setLogInterval( wLogInterval.getText() );
      performanceLogTable.setTimeoutInDays( wLogTimeout.getText() );

      for ( int i = 0; i < performanceLogTable.getFields().size(); i++ ) {
        TableItem item = wOptionFields.table.getItem( i );

        LogTableField field = performanceLogTable.getFields().get( i );
        field.setEnabled( item.getChecked() );
        field.setFieldName( item.getText( 1 ) );
      }
    }
  }

  private void showPerformanceLogTableOptions() {

    previousLogTableIndex = LOG_INDEX_PERFORMANCE;

    addDBSchemaTableLogOptions( performanceLogTable );

    // Log interval...
    //
    Label wlLogInterval = new Label( wLogOptionsComposite, SWT.RIGHT );
    wlLogInterval.setText( BaseMessages.getString( PKG, "PipelineDialog.LogInterval.Label" ) );
    props.setLook( wlLogInterval );
    FormData fdlLogInterval = new FormData();
    fdlLogInterval.left = new FormAttachment( 0, 0 );
    fdlLogInterval.right = new FormAttachment( middle, -margin );
    fdlLogInterval.top = new FormAttachment( wLogTable, margin );
    wlLogInterval.setLayoutData( fdlLogInterval );
    wLogInterval = new TextVar( pipelineMeta, wLogOptionsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wLogInterval );
    wLogInterval.addModifyListener( lsMod );
    FormData fdLogInterval = new FormData();
    fdLogInterval.left = new FormAttachment( middle, 0 );
    fdLogInterval.top = new FormAttachment( wLogTable, margin );
    fdLogInterval.right = new FormAttachment( 100, 0 );
    wLogInterval.setLayoutData( fdLogInterval );
    wLogInterval.setText( Const.NVL( performanceLogTable.getLogInterval(), "" ) );

    // The log timeout in days
    //
    Label wlLogTimeout = new Label( wLogOptionsComposite, SWT.RIGHT );
    wlLogTimeout.setText( BaseMessages.getString( PKG, "PipelineDialog.LogTimeout.Label" ) );
    wlLogTimeout.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.LogTimeout.Tooltip" ) );
    props.setLook( wlLogTimeout );
    FormData fdlLogTimeout = new FormData();
    fdlLogTimeout.left = new FormAttachment( 0, 0 );
    fdlLogTimeout.right = new FormAttachment( middle, -margin );
    fdlLogTimeout.top = new FormAttachment( wLogInterval, margin );
    wlLogTimeout.setLayoutData( fdlLogTimeout );
    wLogTimeout = new TextVar( pipelineMeta, wLogOptionsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLogTimeout.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.LogTimeout.Tooltip" ) );
    props.setLook( wLogTimeout );
    wLogTimeout.addModifyListener( lsMod );
    FormData fdLogTimeout = new FormData();
    fdLogTimeout.left = new FormAttachment( middle, 0 );
    fdLogTimeout.top = new FormAttachment( wLogInterval, margin );
    fdLogTimeout.right = new FormAttachment( 100, 0 );
    wLogTimeout.setLayoutData( fdLogTimeout );
    wLogTimeout.setText( Const.NVL( performanceLogTable.getTimeoutInDays(), "" ) );

    // Add the fields grid...
    //
    Label wlFields = new Label( wLogOptionsComposite, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wLogTimeout, margin * 2 );
    wlFields.setLayoutData( fdlFields );

    final java.util.List<LogTableField> fields = performanceLogTable.getFields();
    final int nrRows = fields.size();

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.FieldName" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.Description" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ), };

    FieldDisabledListener disabledListener = new FieldDisabledListener() {

      public boolean isFieldDisabled( int rowNr ) {
        if ( rowNr >= 0 && rowNr < fields.size() ) {
          LogTableField field = fields.get( rowNr );
          return field.isSubjectAllowed();
        } else {
          return true;
        }
      }
    };

    colinf[ 1 ].setDisabledListener( disabledListener );

    wOptionFields =
      new TableView( pipelineMeta, wLogOptionsComposite, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.CHECK,
        colinf, nrRows, true, lsMod, props );

    wOptionFields.setSortable( false );

    for ( int i = 0; i < fields.size(); i++ ) {
      LogTableField field = fields.get( i );
      TableItem item = wOptionFields.table.getItem( i );
      item.setChecked( field.isEnabled() );
      item.setText( new String[] {
        "", Const.NVL( field.getFieldName(), "" ), Const.NVL( field.getDescription(), "" ) } );
    }

    wOptionFields.table.getColumn( 0 ).setText(
      BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.Enabled" ) );

    FormData fdOptionFields = new FormData();
    fdOptionFields.left = new FormAttachment( 0, 0 );
    fdOptionFields.top = new FormAttachment( wlFields, margin );
    fdOptionFields.right = new FormAttachment( 100, 0 );
    fdOptionFields.bottom = new FormAttachment( 100, 0 );
    wOptionFields.setLayoutData( fdOptionFields );

    wOptionFields.optWidth( true );

    wOptionFields.layout();
    wLogOptionsComposite.layout( true, true );
    wLogComp.layout( true, true );
  }

  private void getChannelLogTableOptions() {

    if ( previousLogTableIndex == LOG_INDEX_CHANNEL ) {
      // The connection...
      //
      channelLogTable.setConnectionName( wLogConnection.getText() );
      channelLogTable.setSchemaName( wLogSchema.getText() );
      channelLogTable.setTableName( wLogTable.getText() );
      channelLogTable.setTimeoutInDays( wLogTimeout.getText() );

      for ( int i = 0; i < channelLogTable.getFields().size(); i++ ) {
        TableItem item = wOptionFields.table.getItem( i );

        LogTableField field = channelLogTable.getFields().get( i );
        field.setEnabled( item.getChecked() );
        field.setFieldName( item.getText( 1 ) );
      }
    }
  }

  private void getMetricsLogTableOptions() {

    if ( previousLogTableIndex == LOG_INDEX_METRICS ) {

      // The connection...
      //
      metricsLogTable.setConnectionName( wLogConnection.getText() );
      metricsLogTable.setSchemaName( wLogSchema.getText() );
      metricsLogTable.setTableName( wLogTable.getText() );
      metricsLogTable.setTimeoutInDays( wLogTimeout.getText() );

      for ( int i = 0; i < metricsLogTable.getFields().size(); i++ ) {
        TableItem item = wOptionFields.table.getItem( i );

        LogTableField field = metricsLogTable.getFields().get( i );
        field.setEnabled( item.getChecked() );
        field.setFieldName( item.getText( 1 ) );
      }
    }
  }

  private void showChannelLogTableOptions() {

    previousLogTableIndex = LOG_INDEX_CHANNEL;

    addDBSchemaTableLogOptions( channelLogTable );

    // The log timeout in days
    //
    Label wlLogTimeout = new Label( wLogOptionsComposite, SWT.RIGHT );
    wlLogTimeout.setText( BaseMessages.getString( PKG, "PipelineDialog.LogTimeout.Label" ) );
    wlLogTimeout.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.LogTimeout.Tooltip" ) );
    props.setLook( wlLogTimeout );
    FormData fdlLogTimeout = new FormData();
    fdlLogTimeout.left = new FormAttachment( 0, 0 );
    fdlLogTimeout.right = new FormAttachment( middle, -margin );
    fdlLogTimeout.top = new FormAttachment( wLogTable, margin );
    wlLogTimeout.setLayoutData( fdlLogTimeout );
    wLogTimeout = new TextVar( pipelineMeta, wLogOptionsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLogTimeout.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.LogTimeout.Tooltip" ) );
    props.setLook( wLogTimeout );
    wLogTimeout.addModifyListener( lsMod );
    FormData fdLogTimeout = new FormData();
    fdLogTimeout.left = new FormAttachment( middle, 0 );
    fdLogTimeout.top = new FormAttachment( wLogTable, margin );
    fdLogTimeout.right = new FormAttachment( 100, 0 );
    wLogTimeout.setLayoutData( fdLogTimeout );
    wLogTimeout.setText( Const.NVL( channelLogTable.getTimeoutInDays(), "" ) );

    // Add the fields grid...
    //
    Label wlFields = new Label( wLogOptionsComposite, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wLogTimeout, margin * 2 );
    wlFields.setLayoutData( fdlFields );

    final java.util.List<LogTableField> fields = channelLogTable.getFields();
    final int nrRows = fields.size();

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.FieldName" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.Description" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ), };

    FieldDisabledListener disabledListener = new FieldDisabledListener() {

      public boolean isFieldDisabled( int rowNr ) {
        if ( rowNr >= 0 && rowNr < fields.size() ) {
          LogTableField field = fields.get( rowNr );
          return field.isSubjectAllowed();
        } else {
          return true;
        }
      }
    };

    colinf[ 1 ].setDisabledListener( disabledListener );

    wOptionFields =
      new TableView( pipelineMeta, wLogOptionsComposite, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.CHECK,
        colinf, nrRows, true, lsMod, props );

    wOptionFields.setSortable( false );

    for ( int i = 0; i < fields.size(); i++ ) {
      LogTableField field = fields.get( i );
      TableItem item = wOptionFields.table.getItem( i );
      item.setChecked( field.isEnabled() );
      item.setText( new String[] {
        "", Const.NVL( field.getFieldName(), "" ), Const.NVL( field.getDescription(), "" ) } );
    }

    wOptionFields.table.getColumn( 0 ).setText(
      BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.Enabled" ) );

    FormData fdOptionFields = new FormData();
    fdOptionFields.left = new FormAttachment( 0, 0 );
    fdOptionFields.top = new FormAttachment( wlFields, margin );
    fdOptionFields.right = new FormAttachment( 100, 0 );
    fdOptionFields.bottom = new FormAttachment( 100, 0 );
    wOptionFields.setLayoutData( fdOptionFields );

    wOptionFields.optWidth( true );

    wOptionFields.layout();
    wLogOptionsComposite.layout( true, true );
    wLogComp.layout( true, true );
  }

  private void showMetricsLogTableOptions() {

    previousLogTableIndex = LOG_INDEX_METRICS;

    addDBSchemaTableLogOptions( metricsLogTable );

    // The log timeout in days
    //
    Label wlLogTimeout = new Label( wLogOptionsComposite, SWT.RIGHT );
    wlLogTimeout.setText( BaseMessages.getString( PKG, "PipelineDialog.LogTimeout.Label" ) );
    wlLogTimeout.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.LogTimeout.Tooltip" ) );
    props.setLook( wlLogTimeout );
    FormData fdlLogTimeout = new FormData();
    fdlLogTimeout.left = new FormAttachment( 0, 0 );
    fdlLogTimeout.right = new FormAttachment( middle, -margin );
    fdlLogTimeout.top = new FormAttachment( wLogTable, margin );
    wlLogTimeout.setLayoutData( fdlLogTimeout );
    wLogTimeout = new TextVar( pipelineMeta, wLogOptionsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLogTimeout.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.LogTimeout.Tooltip" ) );
    props.setLook( wLogTimeout );
    wLogTimeout.addModifyListener( lsMod );
    FormData fdLogTimeout = new FormData();
    fdLogTimeout.left = new FormAttachment( middle, 0 );
    fdLogTimeout.top = new FormAttachment( wLogTable, margin );
    fdLogTimeout.right = new FormAttachment( 100, 0 );
    wLogTimeout.setLayoutData( fdLogTimeout );
    wLogTimeout.setText( Const.NVL( metricsLogTable.getTimeoutInDays(), "" ) );

    // Add the fields grid...
    //
    Label wlFields = new Label( wLogOptionsComposite, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wLogTimeout, margin * 2 );
    wlFields.setLayoutData( fdlFields );

    final java.util.List<LogTableField> fields = metricsLogTable.getFields();
    final int nrRows = fields.size();

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.FieldName" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.Description" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ), };

    FieldDisabledListener disabledListener = new FieldDisabledListener() {

      public boolean isFieldDisabled( int rowNr ) {
        if ( rowNr >= 0 && rowNr < fields.size() ) {
          LogTableField field = fields.get( rowNr );
          return field.isSubjectAllowed();
        } else {
          return true;
        }
      }
    };

    colinf[ 1 ].setDisabledListener( disabledListener );

    wOptionFields =
      new TableView( pipelineMeta, wLogOptionsComposite, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.CHECK,
        colinf, nrRows, true, lsMod, props );

    wOptionFields.setSortable( false );

    for ( int i = 0; i < fields.size(); i++ ) {
      LogTableField field = fields.get( i );
      TableItem item = wOptionFields.table.getItem( i );
      item.setChecked( field.isEnabled() );
      item.setText( new String[] {
        "", Const.NVL( field.getFieldName(), "" ), Const.NVL( field.getDescription(), "" ) } );
    }

    wOptionFields.table.getColumn( 0 ).setText(
      BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.Enabled" ) );

    FormData fdOptionFields = new FormData();
    fdOptionFields.left = new FormAttachment( 0, 0 );
    fdOptionFields.top = new FormAttachment( wlFields, margin );
    fdOptionFields.right = new FormAttachment( 100, 0 );
    fdOptionFields.bottom = new FormAttachment( 100, 0 );
    wOptionFields.setLayoutData( fdOptionFields );

    wOptionFields.optWidth( true );

    wOptionFields.layout();
    wLogOptionsComposite.layout( true, true );
    wLogComp.layout( true, true );
  }

  private void getTransformLogTableOptions() {

    if ( previousLogTableIndex == LOG_INDEX_TRANSFORM ) {
      // The connection...
      //
      transformLogTable.setConnectionName( wLogConnection.getText() );
      transformLogTable.setSchemaName( wLogSchema.getText() );
      transformLogTable.setTableName( wLogTable.getText() );
      transformLogTable.setTimeoutInDays( wLogTimeout.getText() );

      for ( int i = 0; i < transformLogTable.getFields().size(); i++ ) {
        TableItem item = wOptionFields.table.getItem( i );

        LogTableField field = transformLogTable.getFields().get( i );
        field.setEnabled( item.getChecked() );
        field.setFieldName( item.getText( 1 ) );
      }
    }
  }

  private void showTransformLogTableOptions() {

    previousLogTableIndex = LOG_INDEX_TRANSFORM;

    addDBSchemaTableLogOptions( transformLogTable );

    // The log timeout in days
    //
    Label wlLogTimeout = new Label( wLogOptionsComposite, SWT.RIGHT );
    wlLogTimeout.setText( BaseMessages.getString( PKG, "PipelineDialog.LogTimeout.Label" ) );
    wlLogTimeout.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.LogTimeout.Tooltip" ) );
    props.setLook( wlLogTimeout );
    FormData fdlLogTimeout = new FormData();
    fdlLogTimeout.left = new FormAttachment( 0, 0 );
    fdlLogTimeout.right = new FormAttachment( middle, -margin );
    fdlLogTimeout.top = new FormAttachment( wLogTable, margin );
    wlLogTimeout.setLayoutData( fdlLogTimeout );
    wLogTimeout = new TextVar( pipelineMeta, wLogOptionsComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wLogTimeout.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.LogTimeout.Tooltip" ) );
    props.setLook( wLogTimeout );
    wLogTimeout.addModifyListener( lsMod );
    FormData fdLogTimeout = new FormData();
    fdLogTimeout.left = new FormAttachment( middle, 0 );
    fdLogTimeout.top = new FormAttachment( wLogTable, margin );
    fdLogTimeout.right = new FormAttachment( 100, 0 );
    wLogTimeout.setLayoutData( fdLogTimeout );
    wLogTimeout.setText( Const.NVL( transformLogTable.getTimeoutInDays(), "" ) );

    // Add the fields grid...
    //
    Label wlFields = new Label( wLogOptionsComposite, SWT.NONE );
    wlFields.setText( BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( wLogTimeout, margin * 2 );
    wlFields.setLayoutData( fdlFields );

    final java.util.List<LogTableField> fields = transformLogTable.getFields();
    final int nrRows = fields.size();

    ColumnInfo[] colinf =
      new ColumnInfo[] {
        new ColumnInfo(
          BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.FieldName" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo(
          BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.Description" ),
          ColumnInfo.COLUMN_TYPE_TEXT, false, true ), };

    FieldDisabledListener disabledListener = new FieldDisabledListener() {

      public boolean isFieldDisabled( int rowNr ) {
        if ( rowNr >= 0 && rowNr < fields.size() ) {
          LogTableField field = fields.get( rowNr );
          return field.isSubjectAllowed();
        } else {
          return true;
        }
      }
    };

    colinf[ 1 ].setDisabledListener( disabledListener );

    wOptionFields =
      new TableView( pipelineMeta, wLogOptionsComposite, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.CHECK,
        colinf, nrRows, true, lsMod, props );

    wOptionFields.setSortable( false );

    for ( int i = 0; i < fields.size(); i++ ) {
      LogTableField field = fields.get( i );
      TableItem item = wOptionFields.table.getItem( i );
      item.setChecked( field.isEnabled() );
      item.setText( new String[] {
        "", Const.NVL( field.getFieldName(), "" ), Const.NVL( field.getDescription(), "" ) } );
    }

    wOptionFields.table.getColumn( 0 ).setText(
      BaseMessages.getString( PKG, "PipelineDialog.PipelineLogTable.Fields.Enabled" ) );

    FormData fdOptionFields = new FormData();
    fdOptionFields.left = new FormAttachment( 0, 0 );
    fdOptionFields.top = new FormAttachment( wlFields, margin );
    fdOptionFields.right = new FormAttachment( 100, 0 );
    fdOptionFields.bottom = new FormAttachment( 100, 0 );
    wOptionFields.setLayoutData( fdOptionFields );

    wOptionFields.optWidth( true );

    wOptionFields.layout();
    wLogOptionsComposite.layout( true, true );
    wLogComp.layout( true, true );
  }

  private void addDateTab() {
    // ////////////////////////
    // START OF DATE TAB///
    // /
    wDateTab = new CTabItem( wTabFolder, SWT.NONE );
    wDateTab.setText( BaseMessages.getString( PKG, "PipelineDialog.DateTab.Label" ) );

    FormLayout DateLayout = new FormLayout();
    DateLayout.marginWidth = props.getMargin();
    DateLayout.marginHeight = props.getMargin();

    Composite wDateComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wDateComp );
    wDateComp.setLayout( DateLayout );

    // Max date table connection...
    Label wlMaxdateconnection = new Label( wDateComp, SWT.RIGHT );
    wlMaxdateconnection.setText( BaseMessages.getString( PKG, "PipelineDialog.MaxdateConnection.Label" ) );
    props.setLook( wlMaxdateconnection );
    FormData fdlMaxdateconnection = new FormData();
    fdlMaxdateconnection.left = new FormAttachment( 0, 0 );
    fdlMaxdateconnection.right = new FormAttachment( middle, -margin );
    fdlMaxdateconnection.top = new FormAttachment( 0, 0 );
    wlMaxdateconnection.setLayoutData( fdlMaxdateconnection );
    wMaxdateconnection = new CCombo( wDateComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaxdateconnection );
    wMaxdateconnection.addModifyListener( lsMod );
    FormData fdMaxdateconnection = new FormData();
    fdMaxdateconnection.left = new FormAttachment( middle, 0 );
    fdMaxdateconnection.top = new FormAttachment( 0, 0 );
    fdMaxdateconnection.right = new FormAttachment( 100, 0 );
    wMaxdateconnection.setLayoutData( fdMaxdateconnection );

    // Maxdate table...:
    Label wlMaxdatetable = new Label( wDateComp, SWT.RIGHT );
    wlMaxdatetable.setText( BaseMessages.getString( PKG, "PipelineDialog.MaxdateTable.Label" ) );
    props.setLook( wlMaxdatetable );
    FormData fdlMaxdatetable = new FormData();
    fdlMaxdatetable.left = new FormAttachment( 0, 0 );
    fdlMaxdatetable.right = new FormAttachment( middle, -margin );
    fdlMaxdatetable.top = new FormAttachment( wMaxdateconnection, margin );
    wlMaxdatetable.setLayoutData( fdlMaxdatetable );
    wMaxdatetable = new Text( wDateComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaxdatetable );
    wMaxdatetable.addModifyListener( lsMod );
    FormData fdMaxdatetable = new FormData();
    fdMaxdatetable.left = new FormAttachment( middle, 0 );
    fdMaxdatetable.top = new FormAttachment( wMaxdateconnection, margin );
    fdMaxdatetable.right = new FormAttachment( 100, 0 );
    wMaxdatetable.setLayoutData( fdMaxdatetable );

    // Maxdate field...:
    Label wlMaxdatefield = new Label( wDateComp, SWT.RIGHT );
    wlMaxdatefield.setText( BaseMessages.getString( PKG, "PipelineDialog.MaxdateField.Label" ) );
    props.setLook( wlMaxdatefield );
    FormData fdlMaxdatefield = new FormData();
    fdlMaxdatefield.left = new FormAttachment( 0, 0 );
    fdlMaxdatefield.right = new FormAttachment( middle, -margin );
    fdlMaxdatefield.top = new FormAttachment( wMaxdatetable, margin );
    wlMaxdatefield.setLayoutData( fdlMaxdatefield );
    wMaxdatefield = new Text( wDateComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaxdatefield );
    wMaxdatefield.addModifyListener( lsMod );
    FormData fdMaxdatefield = new FormData();
    fdMaxdatefield.left = new FormAttachment( middle, 0 );
    fdMaxdatefield.top = new FormAttachment( wMaxdatetable, margin );
    fdMaxdatefield.right = new FormAttachment( 100, 0 );
    wMaxdatefield.setLayoutData( fdMaxdatefield );

    // Maxdate offset...:
    Label wlMaxdateoffset = new Label( wDateComp, SWT.RIGHT );
    wlMaxdateoffset.setText( BaseMessages.getString( PKG, "PipelineDialog.MaxdateOffset.Label" ) );
    props.setLook( wlMaxdateoffset );
    FormData fdlMaxdateoffset = new FormData();
    fdlMaxdateoffset.left = new FormAttachment( 0, 0 );
    fdlMaxdateoffset.right = new FormAttachment( middle, -margin );
    fdlMaxdateoffset.top = new FormAttachment( wMaxdatefield, margin );
    wlMaxdateoffset.setLayoutData( fdlMaxdateoffset );
    wMaxdateoffset = new Text( wDateComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaxdateoffset );
    wMaxdateoffset.addModifyListener( lsMod );
    FormData fdMaxdateoffset = new FormData();
    fdMaxdateoffset.left = new FormAttachment( middle, 0 );
    fdMaxdateoffset.top = new FormAttachment( wMaxdatefield, margin );
    fdMaxdateoffset.right = new FormAttachment( 100, 0 );
    wMaxdateoffset.setLayoutData( fdMaxdateoffset );

    // Maxdate diff...:
    Label wlMaxdatediff = new Label( wDateComp, SWT.RIGHT );
    wlMaxdatediff.setText( BaseMessages.getString( PKG, "PipelineDialog.Maxdatediff.Label" ) );
    props.setLook( wlMaxdatediff );
    FormData fdlMaxdatediff = new FormData();
    fdlMaxdatediff.left = new FormAttachment( 0, 0 );
    fdlMaxdatediff.right = new FormAttachment( middle, -margin );
    fdlMaxdatediff.top = new FormAttachment( wMaxdateoffset, margin );
    wlMaxdatediff.setLayoutData( fdlMaxdatediff );
    wMaxdatediff = new Text( wDateComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wMaxdatediff );
    wMaxdatediff.addModifyListener( lsMod );
    FormData fdMaxdatediff = new FormData();
    fdMaxdatediff.left = new FormAttachment( middle, 0 );
    fdMaxdatediff.top = new FormAttachment( wMaxdateoffset, margin );
    fdMaxdatediff.right = new FormAttachment( 100, 0 );
    wMaxdatediff.setLayoutData( fdMaxdatediff );

    FormData fdDateComp = new FormData();
    fdDateComp.left = new FormAttachment( 0, 0 );
    fdDateComp.top = new FormAttachment( 0, 0 );
    fdDateComp.right = new FormAttachment( 100, 0 );
    fdDateComp.bottom = new FormAttachment( 100, 0 );
    wDateComp.setLayoutData( fdDateComp );

    wDateComp.layout();
    wDateTab.setControl( wDateComp );

    // ///////////////////////////////////////////////////////////
    // / END OF DATE TAB
    // ///////////////////////////////////////////////////////////
  }

  private void addDepTab() {
    // ////////////////////////
    // START OF Dep TAB///
    // /
    wDepTab = new CTabItem( wTabFolder, SWT.NONE );
    wDepTab.setText( BaseMessages.getString( PKG, "PipelineDialog.DepTab.Label" ) );

    FormLayout DepLayout = new FormLayout();
    DepLayout.marginWidth = props.getMargin();
    DepLayout.marginHeight = props.getMargin();

    Composite wDepComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wDepComp );
    wDepComp.setLayout( DepLayout );

    Label wlFields = new Label( wDepComp, SWT.RIGHT );
    wlFields.setText( BaseMessages.getString( PKG, "PipelineDialog.Fields.Label" ) );
    props.setLook( wlFields );
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment( 0, 0 );
    fdlFields.top = new FormAttachment( 0, 0 );
    wlFields.setLayoutData( fdlFields );

    final int FieldsCols = 3;
    final int FieldsRows = pipelineMeta.nrDependencies();

    ColumnInfo[] colinf = new ColumnInfo[ FieldsCols ];
    colinf[ 0 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "PipelineDialog.ColumnInfo.Connection.Label" ),
        ColumnInfo.COLUMN_TYPE_CCOMBO, connectionNames );
    colinf[ 1 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "PipelineDialog.ColumnInfo.Table.Label" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );
    colinf[ 2 ] =
      new ColumnInfo(
        BaseMessages.getString( PKG, "PipelineDialog.ColumnInfo.Field.Label" ), ColumnInfo.COLUMN_TYPE_TEXT,
        false );

    wFields =
      new TableView(
        pipelineMeta, wDepComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, FieldsRows, lsMod, props );

    wGet = new Button( wDepComp, SWT.PUSH );
    wGet.setText( BaseMessages.getString( PKG, "PipelineDialog.GetDependenciesButton.Label" ) );

    fdGet = new FormData();
    fdGet.bottom = new FormAttachment( 100, 0 );
    fdGet.left = new FormAttachment( 50, 0 );
    wGet.setLayoutData( fdGet );

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment( 0, 0 );
    fdFields.top = new FormAttachment( wlFields, margin );
    fdFields.right = new FormAttachment( 100, 0 );
    fdFields.bottom = new FormAttachment( wGet, 0 );
    wFields.setLayoutData( fdFields );

    FormData fdDepComp = new FormData();
    fdDepComp.left = new FormAttachment( 0, 0 );
    fdDepComp.top = new FormAttachment( 0, 0 );
    fdDepComp.right = new FormAttachment( 100, 0 );
    fdDepComp.bottom = new FormAttachment( 100, 0 );
    wDepComp.setLayoutData( fdDepComp );

    wDepComp.layout();
    wDepTab.setControl( wDepComp );

    // ///////////////////////////////////////////////////////////
    // / END OF DEP TAB
    // ///////////////////////////////////////////////////////////
  }

  private void addMiscTab() {
    // ////////////////////////
    // START OF PERFORMANCE TAB///
    // /
    wMiscTab = new CTabItem( wTabFolder, SWT.NONE );
    wMiscTab.setText( BaseMessages.getString( PKG, "PipelineDialog.MiscTab.Label" ) );

    Composite wMiscComp = new Composite( wTabFolder, SWT.NONE );
    props.setLook( wMiscComp );

    FormLayout perfLayout = new FormLayout();
    perfLayout.marginWidth = Const.FORM_MARGIN;
    perfLayout.marginHeight = Const.FORM_MARGIN;
    wMiscComp.setLayout( perfLayout );

    // Unique connections
    Label wlUniqueConnections = new Label( wMiscComp, SWT.RIGHT );
    wlUniqueConnections.setText( BaseMessages.getString( PKG, "PipelineDialog.UniqueConnections.Label" ) );
    props.setLook( wlUniqueConnections );
    FormData fdlUniqueConnections = new FormData();
    fdlUniqueConnections.left = new FormAttachment( 0, 0 );
    fdlUniqueConnections.right = new FormAttachment( middle, -margin );
    fdlUniqueConnections.top = new FormAttachment( 0, 0 );
    wlUniqueConnections.setLayoutData( fdlUniqueConnections );
    wUniqueConnections = new Button( wMiscComp, SWT.CHECK );
    props.setLook( wUniqueConnections );
    wUniqueConnections.addSelectionListener( lsModSel );
    FormData fdUniqueConnections = new FormData();
    fdUniqueConnections.left = new FormAttachment( middle, 0 );
    fdUniqueConnections.top = new FormAttachment( 0, 0 );
    fdUniqueConnections.right = new FormAttachment( 100, 0 );
    wUniqueConnections.setLayoutData( fdUniqueConnections );

    // Show feedback in pipelines transforms?
    Label wlManageThreads = new Label( wMiscComp, SWT.RIGHT );
    wlManageThreads.setText( BaseMessages.getString( PKG, "PipelineDialog.ManageThreadPriorities.Label" ) );
    props.setLook( wlManageThreads );
    FormData fdlManageThreads = new FormData();
    fdlManageThreads.left = new FormAttachment( 0, 0 );
    fdlManageThreads.top = new FormAttachment( wUniqueConnections, margin );
    fdlManageThreads.right = new FormAttachment( middle, -margin );
    wlManageThreads.setLayoutData( fdlManageThreads );
    wManageThreads = new Button( wMiscComp, SWT.CHECK );
    wManageThreads.addSelectionListener( lsModSel );
    props.setLook( wManageThreads );
    FormData fdManageThreads = new FormData();
    fdManageThreads.left = new FormAttachment( middle, 0 );
    fdManageThreads.top = new FormAttachment( wUniqueConnections, margin );
    fdManageThreads.right = new FormAttachment( 100, 0 );
    wManageThreads.setLayoutData( fdManageThreads );

    // Single threaded option ...
    Label wlPipelineType = new Label( wMiscComp, SWT.RIGHT );
    wlPipelineType.setText( BaseMessages.getString( PKG, "PipelineDialog.PipelineType.Label" ) );
    wlPipelineType.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.PipelineType.Tooltip", Const.CR ) );
    props.setLook( wlPipelineType );
    FormData fdlPipelineType = new FormData();
    fdlPipelineType.left = new FormAttachment( 0, 0 );
    fdlPipelineType.right = new FormAttachment( middle, -margin );
    fdlPipelineType.top = new FormAttachment( wManageThreads, margin );
    wlPipelineType.setLayoutData( fdlPipelineType );
    wPipelineType = new CCombo( wMiscComp, SWT.NORMAL );
    wPipelineType.setToolTipText( BaseMessages.getString( PKG, "PipelineDialog.PipelineType.Tooltip", Const.CR ) );
    wPipelineType.addSelectionListener( lsModSel );
    props.setLook( wPipelineType );
    FormData fdPipelineType = new FormData();
    fdPipelineType.left = new FormAttachment( middle, 0 );
    fdPipelineType.top = new FormAttachment( wManageThreads, margin );
    fdPipelineType.right = new FormAttachment( 100, 0 );
    wPipelineType.setLayoutData( fdPipelineType );
    wPipelineType.setItems( PipelineType.getPipelineTypesDescriptions() );

    FormData fdMiscComp = new FormData();
    fdMiscComp.left = new FormAttachment( 0, 0 );
    fdMiscComp.top = new FormAttachment( 0, 0 );
    fdMiscComp.right = new FormAttachment( 100, 0 );
    fdMiscComp.bottom = new FormAttachment( 100, 0 );
    wMiscComp.setLayoutData( fdMiscComp );

    wMiscComp.layout();
    wMiscTab.setControl( wMiscComp );

    // ///////////////////////////////////////////////////////////
    // / END OF PERF TAB
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
    wTransformPerfMaxSize = new TextVar( pipelineMeta, wMonitorComp, SWT.LEFT | SWT.BORDER | SWT.SINGLE );
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
    wPipelineFilename.setText( Const.NVL( pipelineMeta.getFilename(), "" ) );
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

    if ( pipelineMeta.getMaxDateConnection() != null ) {
      wMaxdateconnection.setText( pipelineMeta.getMaxDateConnection().getName() );
    }
    if ( pipelineMeta.getMaxDateTable() != null ) {
      wMaxdatetable.setText( pipelineMeta.getMaxDateTable() );
    }
    if ( pipelineMeta.getMaxDateField() != null ) {
      wMaxdatefield.setText( pipelineMeta.getMaxDateField() );
    }
    wMaxdateoffset.setText( Double.toString( pipelineMeta.getMaxDateOffset() ) );
    wMaxdatediff.setText( Double.toString( pipelineMeta.getMaxDateDifference() ) );

    // The dependencies
    for ( int i = 0; i < pipelineMeta.nrDependencies(); i++ ) {
      TableItem item = wFields.table.getItem( i );
      PipelineDependency td = pipelineMeta.getDependency( i );

      DatabaseMeta conn = td.getDatabase();
      String table = td.getTablename();
      String field = td.getFieldname();
      if ( conn != null ) {
        item.setText( 1, conn.getName() );
      }
      if ( table != null ) {
        item.setText( 2, table );
      }
      if ( field != null ) {
        item.setText( 3, field );
      }
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

    wUniqueConnections.setSelection( pipelineMeta.isUsingUniqueConnections() );
    wManageThreads.setSelection( pipelineMeta.isUsingThreadPriorityManagment() );
    wPipelineType.setText( pipelineMeta.getPipelineType().getDescription() );

    wFields.setRowNums();
    wFields.optWidth( true );

    wParamFields.setRowNums();
    wParamFields.optWidth( true );

    // Performance monitoring tab:
    //
    wEnableTransformPerfMonitor.setSelection( pipelineMeta.isCapturingTransformPerformanceSnapShots() );
    wTransformPerfInterval.setText( Long.toString( pipelineMeta.getTransformPerformanceCapturingDelay() ) );
    wTransformPerfMaxSize.setText( Const.NVL( pipelineMeta.getTransformPerformanceCapturingSizeLimit(), "" ) );

    wPipelineName.selectAll();
    wPipelineName.setFocus();

    for ( PipelineDialogPluginInterface extraTab : extraTabs ) {
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

    getLogInfo();

    pipelineMeta.setPipelineLogTable( pipelineLogTable );
    pipelineMeta.setPerformanceLogTable( performanceLogTable );
    pipelineMeta.setChannelLogTable( channelLogTable );
    pipelineMeta.setTransformLogTable( transformLogTable );
    pipelineMeta.setMetricsLogTable( metricsLogTable );

    // pipelineMeta.setTransformPerformanceLogTable(wTransformLogtable.getText());
    pipelineMeta.setMaxDateConnection( pipelineMeta.findDatabase( wMaxdateconnection.getText() ) );
    pipelineMeta.setMaxDateTable( wMaxdatetable.getText() );
    pipelineMeta.setMaxDateField( wMaxdatefield.getText() );
    pipelineMeta.setName( wPipelineName.getText() );

    pipelineMeta.setDescription( wPipelineDescription.getText() );
    pipelineMeta.setExtendedDescription( wExtendedDescription.getText() );
    pipelineMeta.setPipelineVersion( wPipelineVersion.getText() );

    if ( wPipelineStatus.getSelectionIndex() != 2 ) {
      pipelineMeta.setPipelineStatus( wPipelineStatus.getSelectionIndex() + 1 );
    } else {
      pipelineMeta.setPipelineStatus( -1 );
    }

    try {
      pipelineMeta.setMaxDateOffset( Double.parseDouble( wMaxdateoffset.getText() ) );
    } catch ( Exception e ) {
      MessageBox mb = new MessageBox( shell, SWT.ICON_ERROR | SWT.OK );
      mb.setText( BaseMessages.getString( PKG, "PipelineDialog.InvalidOffsetNumber.DialogTitle" ) );
      mb.setMessage( BaseMessages.getString( PKG, "PipelineDialog.InvalidOffsetNumber.DialogMessage" ) );
      mb.open();
      wMaxdateoffset.setFocus();
      wMaxdateoffset.selectAll();
      OK = false;
    }

    try {
      pipelineMeta.setMaxDateDifference( Double.parseDouble( wMaxdatediff.getText() ) );
    } catch ( Exception e ) {
      MessageBox mb = new MessageBox( shell, SWT.ICON_ERROR | SWT.OK );
      mb.setText( BaseMessages.getString( PKG, "PipelineDialog.InvalidDateDifferenceNumber.DialogTitle" ) );
      mb.setMessage( BaseMessages.getString( PKG, "PipelineDialog.InvalidDateDifferenceNumber.DialogMessage" ) );
      mb.open();
      wMaxdatediff.setFocus();
      wMaxdatediff.selectAll();
      OK = false;
    }

    // Clear and add current dependencies
    pipelineMeta.removeAllDependencies();
    int nrNonEmptyFields = wFields.nrNonEmpty();
    for ( int i = 0; i < nrNonEmptyFields; i++ ) {
      TableItem item = wFields.getNonEmpty( i );

      DatabaseMeta db = pipelineMeta.findDatabase( item.getText( 1 ) );
      String tablename = item.getText( 2 );
      String fieldname = item.getText( 3 );
      PipelineDependency td = new PipelineDependency( db, tablename, fieldname );
      pipelineMeta.addDependency( td );
    }

    // Clear and add parameters
    pipelineMeta.eraseParameters();
    nrNonEmptyFields = wParamFields.nrNonEmpty();
    for ( int i = 0; i < nrNonEmptyFields; i++ ) {
      TableItem item = wParamFields.getNonEmpty( i );

      try {
        pipelineMeta.addParameterDefinition( item.getText( 1 ), item.getText( 2 ), item.getText( 3 ) );
      } catch ( DuplicateParamException e ) {
        // Ignore the duplicate parameter.
      }
    }
    pipelineMeta.activateParameters();

    pipelineMeta.setUsingUniqueConnections( wUniqueConnections.getSelection() );

    pipelineMeta.setUsingThreadPriorityManagment( wManageThreads.getSelection() );
    pipelineMeta.setPipelineType( PipelineType.values()[ Const.indexOfString( wPipelineType
      .getText(), PipelineType.getPipelineTypesDescriptions() ) ] );

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

    for ( PipelineDialogPluginInterface extraTab : extraTabs ) {
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

  // Get the dependencies
  private void get() {
    Table table = wFields.table;
    for ( int i = 0; i < pipelineMeta.nrTransforms(); i++ ) {
      TransformMeta transformMeta = pipelineMeta.getTransform( i );
      String con = null;
      String tab = null;
      TableItem item = null;
      TransformMetaInterface sii = transformMeta.getTransformMetaInterface();
      if ( sii instanceof TableInputMeta ) {
        TableInputMeta tii = (TableInputMeta) transformMeta.getTransformMetaInterface();
        if ( tii.getDatabaseMeta() == null ) {
          MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "PipelineDialog.DatabaseMetaNotSet.Text" ) );
          mb.open();

          return;
        }
        con = tii.getDatabaseMeta().getName();
        tab = getTableFromSQL( tii.getSQL() );
        if ( tab == null ) {
          tab = transformMeta.getName();
        }
      }
      //TODO: refactor
/*      if ( sii instanceof DatabaseLookupMeta ) {
        DatabaseLookupMeta dvli = (DatabaseLookupMeta) transformMeta.getTransformMetaInterface();
        con = dvli.getDatabaseMeta().getName();
        tab = dvli.getTablename();
        if ( tab == null ) {
          tab = transformMeta.getName();
        }
        break;
      }*/

      if ( tab != null || con != null ) {
        item = new TableItem( table, SWT.NONE );
        if ( con != null ) {
          item.setText( 1, con );
        }
        if ( tab != null ) {
          item.setText( 2, tab );
        }
      }
    }
    wFields.setRowNums();
  }

  private String getTableFromSQL( String sql ) {
    if ( sql == null ) {
      return null;
    }

    int idxfrom = sql.toUpperCase().indexOf( "FROM" );
    int idxto = sql.toUpperCase().indexOf( "WHERE" );
    if ( idxfrom == -1 ) {
      return null;
    }
    if ( idxto == -1 ) {
      idxto = sql.length();
    }
    return sql.substring( idxfrom + 5, idxto );
  }

  /**
   * Generates code for create table... Conversions done by Database
   */
  private void sql() {
    getLogInfo();

    try {

      boolean allOK = true;

      for ( LogTableInterface logTable : new LogTableInterface[] {
        pipelineLogTable, performanceLogTable, channelLogTable, transformLogTable, metricsLogTable, } ) {
        if ( logTable.getDatabaseMeta() != null && !Utils.isEmpty( logTable.getTableName() ) ) {
          // OK, we have something to work with!
          //
          Database db = null;
          try {
            db = new Database( pipelineMeta, logTable.getDatabaseMeta() );
            db.shareVariablesWith( pipelineMeta );
            db.connect();

            StringBuilder ddl = new StringBuilder();

            RowMetaInterface fields = logTable.getLogRecord( LogStatus.START, null, null ).getRowMeta();
            String tableName = db.environmentSubstitute( logTable.getTableName() );
            String schemaTable =
              logTable.getDatabaseMeta().getQuotedSchemaTableCombination(
                db.environmentSubstitute( logTable.getSchemaName() ), tableName );
            String createTable = db.getDDL( schemaTable, fields );

            if ( !Utils.isEmpty( createTable ) ) {
              ddl.append( "-- " ).append( logTable.getLogTableType() ).append( Const.CR );
              ddl.append( "--" ).append( Const.CR ).append( Const.CR );
              ddl.append( createTable ).append( Const.CR );
            }

            java.util.List<RowMetaInterface> indexes = logTable.getRecommendedIndexes();
            for ( int i = 0; i < indexes.size(); i++ ) {
              RowMetaInterface index = indexes.get( i );
              if ( !index.isEmpty() ) {
                String createIndex =
                  db.getCreateIndexStatement( schemaTable, "IDX_" + tableName + "_" + ( i + 1 ), index
                    .getFieldNames(), false, false, false, true );
                if ( !Utils.isEmpty( createIndex ) ) {
                  ddl.append( createIndex );
                }
              }
            }

            if ( ddl.length() > 0 ) {
              allOK = false;
              SQLEditor sqledit =
                new SQLEditor(
                  pipelineMeta, shell, SWT.NONE, logTable.getDatabaseMeta(), pipelineMeta.getDbCache(), ddl
                  .toString() );
              sqledit.open();
            }
          } finally {
            if ( db != null ) {
              db.disconnect();
            }
          }
        }
      }

      if ( allOK ) {
        MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
        mb.setText( BaseMessages.getString( PKG, "PipelineDialog.NoSqlNedds.DialogTitle" ) );
        mb.setMessage( BaseMessages.getString( PKG, "PipelineDialog.NoSqlNedds.DialogMessage" ) );
        mb.open();
      }

    } catch ( Exception e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "PipelineDialog.ErrorOccurred.DialogTitle" ), BaseMessages
        .getString( PKG, "PipelineDialog.ErrorOccurred.DialogMessage" ), e );
    }
  }

  private void getLogInfo() {
    getPipelineLogTableOptions();
    getPerformanceLogTableOptions();
    getChannelLogTableOptions();
    getTransformLogTableOptions();
    getMetricsLogTableOptions();
  }

  public void setDirectoryChangeAllowed( boolean directoryChangeAllowed ) {
    this.directoryChangeAllowed = directoryChangeAllowed;
  }

  private void setCurrentTab( Tabs currentTab ) {

    switch ( currentTab ) {
      case PARAM_TAB:
        wTabFolder.setSelection( wParamTab );
        break;
      case MISC_TAB:
        wTabFolder.setSelection( wMiscTab );
        break;
      case DATE_TAB:
        wTabFolder.setSelection( wDateTab );
        break;
      case LOG_TAB:
        wTabFolder.setSelection( wLogTab );
        break;
      case DEP_TAB:
        wTabFolder.setSelection( wDepTab );
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
