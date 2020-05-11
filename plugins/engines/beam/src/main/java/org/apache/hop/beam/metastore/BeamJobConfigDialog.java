package org.apache.hop.beam.metastore;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.pipeline.fatjar.FatJarBuilder;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.List;

public class BeamJobConfigDialog {

  private static Class<?> PKG = BeamJobConfigDialog.class; // for i18n purposes, needed by Translator2!!

  private BeamJobConfig config;

  private Shell parent;
  private Shell shell;

  private CTabFolder wTabFolder;

  private CTabItem wGeneralTab;
  private CTabItem wParametersTab;
  private CTabItem wDataflowTab;
  private CTabItem wSparkTab;
  private CTabItem wFlinkTab;

  private ScrolledComposite wGeneralSComp;
  private ScrolledComposite wDataflowSComp;
  private ScrolledComposite wSparkSComp;
  private ScrolledComposite wFlinkSComp;

  private Composite wGeneralComp;
  private Composite wParametersComp;
  private Composite wDataflowComp;
  private Composite wSparkComp;
  private Composite wFlinkComp;

  // Connection properties
  //
  private Text wName;
  private Text wDescription;
  private ComboVar wRunner;
  private TextVar wUserAgent;
  private TextVar wTempLocation;
  private TextVar wStreamingKettleStepsFlushInterval;
  private TextVar wPluginsToStage;
  private TextVar wStepPluginClasses;
  private TextVar wXpPluginClasses;
  private TextVar wFatJar;

  // Parameters

  private TableView wParameters;

  // GCP settings

  private TextVar wGcpProjectId;
  private TextVar wGcpAppName;
  private TextVar wGcpStagingLocation;
  private TextVar wGcpInitialNumberOfWorkers;
  private TextVar wGcpMaximumNumberOfWorkers;
  private TextVar wGcpAutoScalingAlgorithm;
  private ComboVar wGcpWorkerMachineType;
  private TextVar wGcpWorkerDiskType;
  private TextVar wGcpDiskSizeGb;
  private ComboVar wGcpRegion;
  private TextVar wGcpZone;
  private Button wGcpStreaming;

  // Spark settings

  private Button wSparkLocal;
  private TextVar wSparkMaster;
  private TextVar wSparkDeployFolder;
  private TextVar wSparkBatchIntervalMillis;
  private TextVar wSparkCheckpointDir;
  private TextVar wSparkCheckpointDurationMillis;
  private Button wsparkEnableSparkMetricSinks;
  private TextVar wSparkMaxRecordsPerBatch;
  private TextVar wSparkMinReadTimeMillis;
  private TextVar wSparkReadTimePercentage;
  private TextVar wSparkBundleSize;
  private ComboVar wSparkStorageLevel;

  // Flink settings

  private Button wFlinkLocal;
  private TextVar wFlinkMaster;
  private TextVar wFlinkParallelism;
  private TextVar wFlinkCheckpointingInterval;
  private ComboVar wFlinkCheckpointingMode;
  private TextVar wFlinkCheckpointTimeoutMillis;
  private TextVar wFlinkMinPauseBetweenCheckpoints;
  private TextVar wFlinkNumberOfExecutionRetries;
  private TextVar wFlinkExecutionRetryDelay;
  private TextVar wFlinkObjectReuse;
  private TextVar wFlinkStateBackend;
  private TextVar wFlinkDisbaleMetrics;
  private TextVar wFlinkExternalizedCheckpointsEnabled;
  private TextVar wFlinkRetainExternalizedCheckpointsOnCancellation;
  private TextVar wFlinkMaxBundleSize;
  private TextVar wFlinkMaxBundleTimeMills;
  private TextVar wFlinkShutdownSourcesOnFinalWatermark;
  private TextVar wFlinkLatencyTrackingInterval;
  private TextVar wFlinkAutoWatermarkInterval;
  private ComboVar wFlinkExecutionModeForBatch;

  private Button wOk;
  private Button wCancel;

  private IVariables variables;

  private PropsUi props;

  private int margin;

  private boolean ok;
  private Label wlFlinkMaster;
  private Label wlSparkMaster;


  public BeamJobConfigDialog( Shell parent, BeamJobConfig config ) {
    this.parent = parent;
    this.config = config;
    props = PropsUi.getInstance();
    ok = false;

    // Just environment variables right now.
    //
    variables = Variables.getADefaultVariableSpace();
  }

  public boolean open() {
    Display display = parent.getDisplay();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GuiResource.getInstance().getImageSlave() );

    int middle = props.getMiddlePct();
    margin = Const.MARGIN + 2;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.Shell.Title" ) );
    shell.setLayout( formLayout );

    // Buttons at the bottom of the dialo...
    //
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOk.addListener( SWT.Selection, e -> ok() );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, e -> cancel() );

    BaseTransformDialog.positionBottomButtons( shell, new Button[] { wOk, wCancel }, margin, null );

    // The rest of the dialog is for the widgets...
    //
    addFormWidgets();

    // Add listeners

    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wName.addSelectionListener( selAdapter );
    wDescription.addSelectionListener( selAdapter );
    wRunner.addSelectionListener( selAdapter );
    wUserAgent.addSelectionListener( selAdapter );
    wTempLocation.addSelectionListener( selAdapter );
    wPluginsToStage.addSelectionListener( selAdapter );
    wStepPluginClasses.addSelectionListener( selAdapter );
    wXpPluginClasses.addSelectionListener( selAdapter );
    wFatJar.addSelectionListener( selAdapter );
    wStreamingKettleStepsFlushInterval.addSelectionListener( selAdapter );
    wGcpProjectId.addSelectionListener( selAdapter );
    wGcpAppName.addSelectionListener( selAdapter );
    wGcpStagingLocation.addSelectionListener( selAdapter );
    wGcpInitialNumberOfWorkers.addSelectionListener( selAdapter );
    wGcpMaximumNumberOfWorkers.addSelectionListener( selAdapter );
    wGcpStreaming.addSelectionListener( selAdapter );
    wGcpAutoScalingAlgorithm.addSelectionListener( selAdapter );
    wGcpWorkerMachineType.addSelectionListener( selAdapter );
    wGcpWorkerDiskType.addSelectionListener( selAdapter );
    wGcpDiskSizeGb.addSelectionListener( selAdapter );
    wGcpRegion.addSelectionListener( selAdapter );
    wGcpZone.addSelectionListener( selAdapter );

    wSparkLocal.addSelectionListener( selAdapter );
    wSparkMaster.addSelectionListener( selAdapter );
    wSparkDeployFolder.addSelectionListener( selAdapter );
    wSparkBatchIntervalMillis.addSelectionListener( selAdapter );
    wSparkCheckpointDir.addSelectionListener( selAdapter );
    wSparkCheckpointDurationMillis.addSelectionListener( selAdapter );
    wSparkMaxRecordsPerBatch.addSelectionListener( selAdapter );
    wSparkMinReadTimeMillis.addSelectionListener( selAdapter );
    wSparkReadTimePercentage.addSelectionListener( selAdapter );
    wSparkBundleSize.addSelectionListener( selAdapter );
    wSparkStorageLevel.addSelectionListener( selAdapter );

    wFlinkLocal.addSelectionListener( selAdapter );
    wFlinkMaster.addSelectionListener( selAdapter );
    wFlinkParallelism.addSelectionListener( selAdapter );
    wFlinkCheckpointingInterval.addSelectionListener( selAdapter );
    wFlinkCheckpointingMode.addSelectionListener( selAdapter );
    wFlinkCheckpointTimeoutMillis.addSelectionListener( selAdapter );
    wFlinkMinPauseBetweenCheckpoints.addSelectionListener( selAdapter );
    wFlinkNumberOfExecutionRetries.addSelectionListener( selAdapter );
    wFlinkExecutionRetryDelay.addSelectionListener( selAdapter );
    wFlinkObjectReuse.addSelectionListener( selAdapter );
    wFlinkStateBackend.addSelectionListener( selAdapter );
    wFlinkDisbaleMetrics.addSelectionListener( selAdapter );
    wFlinkExternalizedCheckpointsEnabled.addSelectionListener( selAdapter );
    wFlinkRetainExternalizedCheckpointsOnCancellation.addSelectionListener( selAdapter );
    wFlinkMaxBundleSize.addSelectionListener( selAdapter );
    wFlinkMaxBundleTimeMills.addSelectionListener( selAdapter );
    wFlinkShutdownSourcesOnFinalWatermark.addSelectionListener( selAdapter );
    wFlinkLatencyTrackingInterval.addSelectionListener( selAdapter );
    wFlinkAutoWatermarkInterval.addSelectionListener( selAdapter );
    wFlinkExecutionModeForBatch.addSelectionListener( selAdapter );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseTransformDialog.setSize( shell );

    shell.open();

    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return ok;
  }

  private void addFormWidgets() {

    int middle = Const.MIDDLE_PCT;

    // The name of the Beam Job Configuration
    //
    Label wlName = new Label( shell, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.Name.Label" ) );
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment( 0, margin );
    fdlName.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlName.right = new FormAttachment( middle, -margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( wlName, 0, SWT.CENTER );
    fdName.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdName.right = new FormAttachment( 95, 0 );
    wName.setLayoutData( fdName );
    Control lastControl = wName;

    // The description
    //
    Label wlDescription = new Label( shell, SWT.RIGHT );
    props.setLook( wlDescription );
    wlDescription.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.Description.Label" ) );
    FormData fdlDescription = new FormData();
    fdlDescription.top = new FormAttachment( lastControl, margin );
    fdlDescription.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlDescription.right = new FormAttachment( middle, -margin );
    wlDescription.setLayoutData( fdlDescription );
    wDescription = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDescription );
    FormData fdDescription = new FormData();
    fdDescription.top = new FormAttachment( wlDescription, 0, SWT.CENTER );
    fdDescription.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdDescription.right = new FormAttachment( 95, 0 );
    wDescription.setLayoutData( fdDescription );
    lastControl = wDescription;

    // Runner
    //
    Label wlRunner = new Label( shell, SWT.RIGHT );
    props.setLook( wlRunner );
    wlRunner.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.Runner.Label" ) );
    FormData fdlRunner = new FormData();
    fdlRunner.top = new FormAttachment( lastControl, margin );
    fdlRunner.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlRunner.right = new FormAttachment( middle, -margin );
    wlRunner.setLayoutData( fdlRunner );
    wRunner = new ComboVar( variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wRunner.setItems( RunnerType.getNames() );
    props.setLook( wRunner );
    FormData fdRunner = new FormData();
    fdRunner.top = new FormAttachment( wlRunner, 0, SWT.CENTER );
    fdRunner.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdRunner.right = new FormAttachment( 95, 0 );
    wRunner.setLayoutData( fdRunner );
    lastControl = wRunner;


    wTabFolder = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
    wTabFolder.setSimple( false );
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.top = new FormAttachment( lastControl, margin * 2 );
    fdTabFolder.bottom = new FormAttachment( wOk, -margin * 2 );
    wTabFolder.setLayoutData( fdTabFolder );

    addGeneralTab();
    addParametersTab();
    addDataflowTab();
    addSparkTab();
    addFlinkTab();

    wTabFolder.setSelection( 0 );

  }

  private void addGeneralTab() {

    int middle = Const.MIDDLE_PCT;

    wGeneralTab = new CTabItem( wTabFolder, SWT.NONE );
    wGeneralTab.setText( "  General  " );

    wGeneralSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wGeneralSComp.setLayout( new FillLayout() );

    wGeneralComp = new Composite( wGeneralSComp, SWT.NO_BACKGROUND );
    props.setLook( wGeneralComp );

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 10;
    generalLayout.marginHeight = 10;
    wGeneralComp.setLayout( generalLayout );

    // UserAgent
    //
    Label wlUserAgent = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlUserAgent );
    wlUserAgent.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.UserAgent.Label" ) );
    FormData fdlUserAgent = new FormData();
    fdlUserAgent.top = new FormAttachment( 0, 0 );
    fdlUserAgent.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlUserAgent.right = new FormAttachment( middle, -margin );
    wlUserAgent.setLayoutData( fdlUserAgent );
    wUserAgent = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUserAgent );
    FormData fdUserAgent = new FormData();
    fdUserAgent.top = new FormAttachment( wlUserAgent, 0, SWT.CENTER );
    fdUserAgent.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdUserAgent.right = new FormAttachment( 95, 0 );
    wUserAgent.setLayoutData( fdUserAgent );
    Control lastControl = wUserAgent;

    // TempLocation
    //
    Label wlTempLocation = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlTempLocation );
    wlTempLocation.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.TempLocation.Label" ) );
    FormData fdlTempLocation = new FormData();
    fdlTempLocation.top = new FormAttachment( lastControl, margin );
    fdlTempLocation.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlTempLocation.right = new FormAttachment( middle, -margin );
    wlTempLocation.setLayoutData( fdlTempLocation );
    wTempLocation = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wTempLocation );
    FormData fdTempLocation = new FormData();
    fdTempLocation.top = new FormAttachment( wlTempLocation, 0, SWT.CENTER );
    fdTempLocation.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdTempLocation.right = new FormAttachment( 95, 0 );
    wTempLocation.setLayoutData( fdTempLocation );
    lastControl = wTempLocation;

    // PluginsToStage
    //
    Label wlPluginsToStage = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlPluginsToStage );
    wlPluginsToStage.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.PluginsToStage.Label" ) );
    FormData fdlPluginsToStage = new FormData();
    fdlPluginsToStage.top = new FormAttachment( lastControl, margin );
    fdlPluginsToStage.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlPluginsToStage.right = new FormAttachment( middle, -margin );
    wlPluginsToStage.setLayoutData( fdlPluginsToStage );
    wPluginsToStage = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPluginsToStage );
    FormData fdPluginsToStage = new FormData();
    fdPluginsToStage.top = new FormAttachment( wlPluginsToStage, 0, SWT.CENTER );
    fdPluginsToStage.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdPluginsToStage.right = new FormAttachment( 95, 0 );
    wPluginsToStage.setLayoutData( fdPluginsToStage );
    lastControl = wPluginsToStage;

    // StepPluginClasses
    //
    Label wlStepPluginClasses = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlStepPluginClasses );
    wlStepPluginClasses.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.TransformPluginClasses.Label" ) );
    FormData fdlStepPluginClasses = new FormData();
    fdlStepPluginClasses.top = new FormAttachment( lastControl, margin );
    fdlStepPluginClasses.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlStepPluginClasses.right = new FormAttachment( middle, -margin );
    wlStepPluginClasses.setLayoutData( fdlStepPluginClasses );
    Button wbStepPluginClasses = new Button( wGeneralComp, SWT.PUSH );
    wStepPluginClasses = new TextVar( variables, wGeneralComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.WRAP );
    props.setLook( wStepPluginClasses );
    FormData fdStepPluginClasses = new FormData();
    fdStepPluginClasses.top = new FormAttachment( lastControl, margin );
    fdStepPluginClasses.bottom = new FormAttachment( lastControl, 200 );
    fdStepPluginClasses.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdStepPluginClasses.right = new FormAttachment( 95, 0 );
    wStepPluginClasses.setLayoutData( fdStepPluginClasses );
    wbStepPluginClasses.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.TransformPluginClasses.Button" ) );
    FormData fdbStepPluginClasses = new FormData();
    fdbStepPluginClasses.top = new FormAttachment( lastControl, margin );
    fdbStepPluginClasses.left = new FormAttachment( wStepPluginClasses, margin );
    fdbStepPluginClasses.right = new FormAttachment( 100, 0 );
    wbStepPluginClasses.setLayoutData( fdbStepPluginClasses );
    wbStepPluginClasses.addListener( SWT.Selection, this::findStepClasses );
    lastControl = wStepPluginClasses;

    // XpPluginClasses
    //
    Label wlXpPluginClasses = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlXpPluginClasses );
    wlXpPluginClasses.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.XpPluginClasses.Label" ) );
    FormData fdlXpPluginClasses = new FormData();
    fdlXpPluginClasses.top = new FormAttachment( lastControl, margin );
    fdlXpPluginClasses.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlXpPluginClasses.right = new FormAttachment( middle, -margin );
    wlXpPluginClasses.setLayoutData( fdlXpPluginClasses );
    Button wbXpPluginClasses = new Button( wGeneralComp, SWT.PUSH );
    wXpPluginClasses = new TextVar( variables, wGeneralComp, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL | SWT.WRAP );
    props.setLook( wXpPluginClasses );
    FormData fdXpPluginClasses = new FormData();
    fdXpPluginClasses.top = new FormAttachment( lastControl, margin );
    fdXpPluginClasses.bottom = new FormAttachment( lastControl, 300 );
    fdXpPluginClasses.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdXpPluginClasses.right = new FormAttachment( 95, 0 );
    wXpPluginClasses.setLayoutData( fdXpPluginClasses );
    wbXpPluginClasses.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.XpPluginClasses.Button" ) );
    FormData fdbXpPluginClasses = new FormData();
    fdbXpPluginClasses.top = new FormAttachment( lastControl, margin );
    fdbXpPluginClasses.left = new FormAttachment( wXpPluginClasses, margin );
    fdbXpPluginClasses.right = new FormAttachment( 100, 0 );
    wbXpPluginClasses.setLayoutData( fdbXpPluginClasses );
    wbXpPluginClasses.addListener( SWT.Selection, this::findXpClasses );
    lastControl = wXpPluginClasses;

    // FatJar
    //
    Label wlFatJar = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlFatJar );
    wlFatJar.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FatJar.Label" ) );
    FormData fdlFatJar = new FormData();
    fdlFatJar.top = new FormAttachment( lastControl, margin );
    fdlFatJar.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFatJar.right = new FormAttachment( middle, -margin );
    wlFatJar.setLayoutData( fdlFatJar );
    wFatJar = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFatJar );
    FormData fdFatJar = new FormData();
    fdFatJar.top = new FormAttachment( wlFatJar, 0, SWT.CENTER );
    fdFatJar.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFatJar.right = new FormAttachment( 95, 0 );
    wFatJar.setLayoutData( fdFatJar );
    Button wbFatJar = new Button( wGeneralComp, SWT.PUSH );
    wbFatJar.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FatJar.Button" ) );
    FormData fdbFatJar = new FormData();
    fdbFatJar.top = new FormAttachment( lastControl, margin );
    fdbFatJar.left = new FormAttachment( wFatJar, margin );
    fdbFatJar.right = new FormAttachment( 100, 0 );
    wbFatJar.setLayoutData( fdbFatJar );
    wbFatJar.addListener( SWT.Selection, this::buildFatJar );
    lastControl = wFatJar;

    // Streaming Kettle Steps Flush Interval
    //
    Label wlStreamingKettleStepsFlushInterval = new Label( wGeneralComp, SWT.RIGHT );
    props.setLook( wlStreamingKettleStepsFlushInterval );
    wlStreamingKettleStepsFlushInterval.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.StreamingKettleTransformsFlushInterval.Label" ) );
    FormData fdlStreamingKettleStepsFlushInterval = new FormData();
    fdlStreamingKettleStepsFlushInterval.top = new FormAttachment( lastControl, margin );
    fdlStreamingKettleStepsFlushInterval.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlStreamingKettleStepsFlushInterval.right = new FormAttachment( middle, -margin );
    wlStreamingKettleStepsFlushInterval.setLayoutData( fdlStreamingKettleStepsFlushInterval );
    wStreamingKettleStepsFlushInterval = new TextVar( variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStreamingKettleStepsFlushInterval );
    FormData fdStreamingKettleStepsFlushInterval = new FormData();
    fdStreamingKettleStepsFlushInterval.top = new FormAttachment( wlStreamingKettleStepsFlushInterval, 0, SWT.CENTER );
    fdStreamingKettleStepsFlushInterval.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdStreamingKettleStepsFlushInterval.right = new FormAttachment( 95, 0 );
    wStreamingKettleStepsFlushInterval.setLayoutData( fdStreamingKettleStepsFlushInterval );
    lastControl = wStreamingKettleStepsFlushInterval;

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment( 0, 0 );
    fdGeneralComp.top = new FormAttachment( 0, 0 );
    fdGeneralComp.right = new FormAttachment( 100, 0 );
    fdGeneralComp.bottom = new FormAttachment( 100, 0 );
    wGeneralComp.setLayoutData( fdGeneralComp );

    wGeneralComp.pack();
    Rectangle bounds = wGeneralComp.getBounds();

    wGeneralSComp.setContent( wGeneralComp );
    wGeneralSComp.setExpandHorizontal( true );
    wGeneralSComp.setExpandVertical( true );
    wGeneralSComp.setMinWidth( bounds.width );
    wGeneralSComp.setMinHeight( bounds.height );

    wGeneralTab.setControl( wGeneralSComp );
  }

  private void buildFatJar( Event event ) {
    try {
      IVariables variables = Variables.getADefaultVariableSpace();

      BeamJobConfig jobConfig = new BeamJobConfig();
      getInfo( jobConfig );

      FileDialog dialog = new FileDialog( shell, SWT.SAVE );
      dialog.setText( "Select the location of the Kettle+Beam+Plugins fat jar" );
      dialog.setFilterNames( new String[] { "Jar files (*.jar)", "All Files (*.*)" } );
      dialog.setFilterExtensions( new String[] { "*.jar", "*.*" } ); // Windows
      if ( StringUtils.isNotEmpty( jobConfig.getFatJar() ) ) {
        dialog.setFileName( variables.environmentSubstitute( jobConfig.getFatJar() ) );
      }
      String filename = dialog.open();
      if ( StringUtils.isEmpty( filename ) ) {
        return;
      }

      List<String> files = BeamConst.findLibraryFilesToStage( null, jobConfig.getPluginsToStage(), true, true );
      files.removeIf( s -> s.contains( "commons-logging" ) || s.startsWith( "log4j" ) || s.contains( "xml-apis" ) );

      // Find the plugin classes for the specified plugins...
      //
      String stepPluginClasses = findPluginClasses( Transform.class.getName() );
      if (StringUtils.isNotEmpty(jobConfig.getStepPluginClasses())) {
        if (StringUtils.isEmpty( stepPluginClasses )) {
          stepPluginClasses="";
        } else {
          stepPluginClasses+=",";
        }
        stepPluginClasses+=jobConfig.getStepPluginClasses();
      }
      String xpPluginClasses = findPluginClasses( ExtensionPoint.class.getName() );
      if (StringUtils.isNotEmpty(jobConfig.getXpPluginClasses())) {
        if (StringUtils.isEmpty( xpPluginClasses )) {
          xpPluginClasses="";
        } else {
          xpPluginClasses+=",";
        }
        xpPluginClasses+=jobConfig.getStepPluginClasses();
      }


      FatJarBuilder fatJarBuilder = new FatJarBuilder( filename, files );
      fatJarBuilder.setExtraStepPluginClasses( stepPluginClasses );
      fatJarBuilder.setExtraXpPluginClasses( xpPluginClasses );
      Cursor waitCursor = new Cursor( shell.getDisplay(), SWT.CURSOR_WAIT );
      Cursor regularCursor = shell.getCursor();

      try {
        shell.setCursor( waitCursor );
        fatJarBuilder.buildTargetJar();
      } finally {
        shell.setCursor( regularCursor );
        waitCursor.dispose();
      }

      // All went well, insert the filename...
      //
      wFatJar.setText( filename );

    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error building fat jar: " + e.getMessage(), e );
    }
  }

  private void findStepClasses( Event event ) {
    String stepPluginClasses = findPluginClasses( Transform.class.getName() );
    if ( stepPluginClasses != null ) {
      wStepPluginClasses.setText( stepPluginClasses );
    }
  }

  private void findXpClasses( Event event ) {
    String xpPluginClasses = findPluginClasses( ExtensionPoint.class.getName() );
    if ( xpPluginClasses != null ) {
      wXpPluginClasses.setText( xpPluginClasses );
    }
  }

  private String findPluginClasses( String pluginClassName ) {
    BeamJobConfig jobConfig = new BeamJobConfig();
    getInfo( jobConfig );

    try {
      return FatJarBuilder.findPluginClasses( pluginClassName, jobConfig.getPluginsToStage() );
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error find plugin classes of annotation type '" + pluginClassName, e );
      return null;
    }
  }

  private void addParametersTab() {
    wParametersTab = new CTabItem( wTabFolder, SWT.NONE );
    wParametersTab.setText( "Parameters" );

    wParametersComp = new Composite( wTabFolder, SWT.NO_BACKGROUND );
    props.setLook( wParametersComp );
    wParametersComp.setLayout( new FormLayout() );

    ColumnInfo[] columnInfos = new ColumnInfo[] {
      new ColumnInfo( "Name", ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
      new ColumnInfo( "Value", ColumnInfo.COLUMN_TYPE_TEXT, false, false ),
    };

    wParameters = new TableView( variables, wParametersComp, SWT.BORDER, columnInfos, config.getParameters().size(), null, props );
    props.setLook( wParameters );
    FormData fdParameters = new FormData();
    fdParameters.left = new FormAttachment( 0, 0 );
    fdParameters.right = new FormAttachment( 100, 0 );
    fdParameters.top = new FormAttachment( 0, 0 );
    fdParameters.bottom = new FormAttachment( 100, 0 );
    wParameters.setLayoutData( fdParameters );

    FormData fdParametersComp = new FormData();
    fdParametersComp.left = new FormAttachment( 0, 0 );
    fdParametersComp.right = new FormAttachment( 100, 0 );
    fdParametersComp.top = new FormAttachment( 0, 0 );
    fdParametersComp.bottom = new FormAttachment( 100, 0 );
    wParametersComp.setLayoutData( fdParametersComp );

    wParametersTab.setControl( wParametersComp );
  }

  private void addDataflowTab() {

    int middle = Const.MIDDLE_PCT;

    wDataflowTab = new CTabItem( wTabFolder, SWT.NONE );
    wDataflowTab.setText( "  Dataflow  " );

    wDataflowSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wDataflowSComp.setLayout( new FillLayout() );

    wDataflowComp = new Composite( wDataflowSComp, SWT.NONE );
    props.setLook( wDataflowComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wDataflowComp.setLayout( fileLayout );

    // Project ID
    //
    Label wlGcpProjectId = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpProjectId );
    wlGcpProjectId.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpProjectId.Label" ) );
    FormData fdlGcpProjectId = new FormData();
    fdlGcpProjectId.top = new FormAttachment( 0, 0 );
    fdlGcpProjectId.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpProjectId.right = new FormAttachment( middle, -margin );
    wlGcpProjectId.setLayoutData( fdlGcpProjectId );
    wGcpProjectId = new TextVar( variables, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpProjectId );
    FormData fdGcpProjectId = new FormData();
    fdGcpProjectId.top = new FormAttachment( wlGcpProjectId, 0, SWT.CENTER );
    fdGcpProjectId.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpProjectId.right = new FormAttachment( 95, 0 );
    wGcpProjectId.setLayoutData( fdGcpProjectId );
    Control lastControl = wGcpProjectId;

    // App name
    //
    Label wlGcpAppName = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpAppName );
    wlGcpAppName.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpAppName.Label" ) );
    FormData fdlGcpAppName = new FormData();
    fdlGcpAppName.top = new FormAttachment( lastControl, margin );
    fdlGcpAppName.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpAppName.right = new FormAttachment( middle, -margin );
    wlGcpAppName.setLayoutData( fdlGcpAppName );
    wGcpAppName = new TextVar( variables, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpAppName );
    FormData fdGcpAppName = new FormData();
    fdGcpAppName.top = new FormAttachment( wlGcpAppName, 0, SWT.CENTER );
    fdGcpAppName.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpAppName.right = new FormAttachment( 95, 0 );
    wGcpAppName.setLayoutData( fdGcpAppName );
    lastControl = wGcpAppName;

    // Staging location
    //
    Label wlGcpStagingLocation = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpStagingLocation );
    wlGcpStagingLocation.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpStagingLocation.Label" ) );
    FormData fdlGcpStagingLocation = new FormData();
    fdlGcpStagingLocation.top = new FormAttachment( lastControl, margin );
    fdlGcpStagingLocation.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpStagingLocation.right = new FormAttachment( middle, -margin );
    wlGcpStagingLocation.setLayoutData( fdlGcpStagingLocation );
    wGcpStagingLocation = new TextVar( variables, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpStagingLocation );
    FormData fdGcpStagingLocation = new FormData();
    fdGcpStagingLocation.top = new FormAttachment( wlGcpStagingLocation, 0, SWT.CENTER );
    fdGcpStagingLocation.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpStagingLocation.right = new FormAttachment( 95, 0 );
    wGcpStagingLocation.setLayoutData( fdGcpStagingLocation );
    lastControl = wGcpStagingLocation;

    // Initial number of workers
    //
    Label wlInitialNumberOfWorkers = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlInitialNumberOfWorkers );
    wlInitialNumberOfWorkers.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpInitialNumberOfWorkers.Label" ) );
    FormData fdlInitialNumberOfWorkers = new FormData();
    fdlInitialNumberOfWorkers.top = new FormAttachment( lastControl, margin );
    fdlInitialNumberOfWorkers.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlInitialNumberOfWorkers.right = new FormAttachment( middle, -margin );
    wlInitialNumberOfWorkers.setLayoutData( fdlInitialNumberOfWorkers );
    wGcpInitialNumberOfWorkers = new TextVar( variables, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpInitialNumberOfWorkers );
    FormData fdInitialNumberOfWorkers = new FormData();
    fdInitialNumberOfWorkers.top = new FormAttachment( wlInitialNumberOfWorkers, 0, SWT.CENTER );
    fdInitialNumberOfWorkers.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdInitialNumberOfWorkers.right = new FormAttachment( 95, 0 );
    wGcpInitialNumberOfWorkers.setLayoutData( fdInitialNumberOfWorkers );
    lastControl = wGcpInitialNumberOfWorkers;

    // Maximum number of workers
    //
    Label wlMaximumNumberOfWorkers = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlMaximumNumberOfWorkers );
    wlMaximumNumberOfWorkers.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpMaximumNumberOfWorkers.Label" ) );
    FormData fdlMaximumNumberOfWorkers = new FormData();
    fdlMaximumNumberOfWorkers.top = new FormAttachment( lastControl, margin );
    fdlMaximumNumberOfWorkers.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlMaximumNumberOfWorkers.right = new FormAttachment( middle, -margin );
    wlMaximumNumberOfWorkers.setLayoutData( fdlMaximumNumberOfWorkers );
    wGcpMaximumNumberOfWorkers = new TextVar( variables, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpMaximumNumberOfWorkers );
    FormData fdMaximumNumberOfWorkers = new FormData();
    fdMaximumNumberOfWorkers.top = new FormAttachment( wlMaximumNumberOfWorkers, 0, SWT.CENTER );
    fdMaximumNumberOfWorkers.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdMaximumNumberOfWorkers.right = new FormAttachment( 95, 0 );
    wGcpMaximumNumberOfWorkers.setLayoutData( fdMaximumNumberOfWorkers );
    lastControl = wGcpMaximumNumberOfWorkers;

    // Auto Scaling Algorithm
    //
    Label wlAutoScalingAlgorithm = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlAutoScalingAlgorithm );
    wlAutoScalingAlgorithm.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpAutoScalingAlgorithm.Label" ) );
    FormData fdlAutoScalingAlgorithm = new FormData();
    fdlAutoScalingAlgorithm.top = new FormAttachment( lastControl, margin );
    fdlAutoScalingAlgorithm.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlAutoScalingAlgorithm.right = new FormAttachment( middle, -margin );
    wlAutoScalingAlgorithm.setLayoutData( fdlAutoScalingAlgorithm );
    wGcpAutoScalingAlgorithm = new TextVar( variables, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpAutoScalingAlgorithm );
    FormData fdAutoScalingAlgorithm = new FormData();
    fdAutoScalingAlgorithm.top = new FormAttachment( wlAutoScalingAlgorithm, 0, SWT.CENTER );
    fdAutoScalingAlgorithm.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdAutoScalingAlgorithm.right = new FormAttachment( 95, 0 );
    wGcpAutoScalingAlgorithm.setLayoutData( fdAutoScalingAlgorithm );
    lastControl = wGcpAutoScalingAlgorithm;


    // Worker machine type
    //
    Label wlGcpWorkerMachineType = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpWorkerMachineType );
    wlGcpWorkerMachineType.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpWorkerMachineType.Label" ) );
    FormData fdlGcpWorkerMachineType = new FormData();
    fdlGcpWorkerMachineType.top = new FormAttachment( lastControl, margin );
    fdlGcpWorkerMachineType.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpWorkerMachineType.right = new FormAttachment( middle, -margin );
    wlGcpWorkerMachineType.setLayoutData( fdlGcpWorkerMachineType );
    wGcpWorkerMachineType = new ComboVar( variables, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpWorkerMachineType );
    wGcpWorkerMachineType.setItems( BeamConst.getGcpWorkerMachineTypeDescriptions() );
    FormData fdGcpWorkerMachineType = new FormData();
    fdGcpWorkerMachineType.top = new FormAttachment( wlGcpWorkerMachineType, 0, SWT.CENTER );
    fdGcpWorkerMachineType.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpWorkerMachineType.right = new FormAttachment( 95, 0 );
    wGcpWorkerMachineType.setLayoutData( fdGcpWorkerMachineType );
    lastControl = wGcpWorkerMachineType;

    // Worker disk type
    //
    Label wlGcpWorkerDiskType = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpWorkerDiskType );
    wlGcpWorkerDiskType.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpWorkerDiskType.Label" ) );
    FormData fdlGcpWorkerDiskType = new FormData();
    fdlGcpWorkerDiskType.top = new FormAttachment( lastControl, margin );
    fdlGcpWorkerDiskType.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpWorkerDiskType.right = new FormAttachment( middle, -margin );
    wlGcpWorkerDiskType.setLayoutData( fdlGcpWorkerDiskType );
    wGcpWorkerDiskType = new TextVar( variables, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpWorkerDiskType );
    FormData fdGcpWorkerDiskType = new FormData();
    fdGcpWorkerDiskType.top = new FormAttachment( wlGcpWorkerDiskType, 0, SWT.CENTER );
    fdGcpWorkerDiskType.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpWorkerDiskType.right = new FormAttachment( 95, 0 );
    wGcpWorkerDiskType.setLayoutData( fdGcpWorkerDiskType );
    lastControl = wGcpWorkerDiskType;

    // Disk Size in GB
    //
    Label wlGcpDiskSizeGb = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpDiskSizeGb );
    wlGcpDiskSizeGb.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpDiskSizeGb.Label" ) );
    FormData fdlGcpDiskSizeGb = new FormData();
    fdlGcpDiskSizeGb.top = new FormAttachment( lastControl, margin );
    fdlGcpDiskSizeGb.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpDiskSizeGb.right = new FormAttachment( middle, -margin );
    wlGcpDiskSizeGb.setLayoutData( fdlGcpDiskSizeGb );
    wGcpDiskSizeGb = new TextVar( variables, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpDiskSizeGb );
    FormData fdGcpDiskSizeGb = new FormData();
    fdGcpDiskSizeGb.top = new FormAttachment( wlGcpDiskSizeGb, 0, SWT.CENTER );
    fdGcpDiskSizeGb.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpDiskSizeGb.right = new FormAttachment( 95, 0 );
    wGcpDiskSizeGb.setLayoutData( fdGcpDiskSizeGb );
    lastControl = wGcpDiskSizeGb;

    // Region
    //
    Label wlGcpRegion = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpRegion );
    wlGcpRegion.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpRegion.Label" ) );
    FormData fdlGcpRegion = new FormData();
    fdlGcpRegion.top = new FormAttachment( lastControl, margin );
    fdlGcpRegion.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpRegion.right = new FormAttachment( middle, -margin );
    wlGcpRegion.setLayoutData( fdlGcpRegion );
    wGcpRegion = new ComboVar( variables, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpRegion );
    wGcpRegion.setItems( BeamConst.getGcpRegionDescriptions() );
    FormData fdGcpRegion = new FormData();
    fdGcpRegion.top = new FormAttachment( wlGcpRegion, 0, SWT.CENTER );
    fdGcpRegion.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpRegion.right = new FormAttachment( 95, 0 );
    wGcpRegion.setLayoutData( fdGcpRegion );
    lastControl = wGcpRegion;

    // Zone
    //
    Label wlGcpZone = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpZone );
    wlGcpZone.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpZone.Label" ) );
    FormData fdlGcpZone = new FormData();
    fdlGcpZone.top = new FormAttachment( lastControl, margin );
    fdlGcpZone.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpZone.right = new FormAttachment( middle, -margin );
    wlGcpZone.setLayoutData( fdlGcpZone );
    wGcpZone = new TextVar( variables, wDataflowComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wGcpZone );
    FormData fdGcpZone = new FormData();
    fdGcpZone.top = new FormAttachment( wlGcpZone, 0, SWT.CENTER );
    fdGcpZone.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpZone.right = new FormAttachment( 95, 0 );
    wGcpZone.setLayoutData( fdGcpZone );
    lastControl = wGcpZone;

    // Streaming?
    //
    Label wlGcpStreaming = new Label( wDataflowComp, SWT.RIGHT );
    props.setLook( wlGcpStreaming );
    wlGcpStreaming.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.GcpStreaming.Label" ) );
    FormData fdlGcpStreaming = new FormData();
    fdlGcpStreaming.top = new FormAttachment( lastControl, margin );
    fdlGcpStreaming.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlGcpStreaming.right = new FormAttachment( middle, -margin );
    wlGcpStreaming.setLayoutData( fdlGcpStreaming );
    wGcpStreaming = new Button( wDataflowComp, SWT.CHECK | SWT.LEFT );
    props.setLook( wGcpStreaming );
    FormData fdGcpStreaming = new FormData();
    fdGcpStreaming.top = new FormAttachment( wlGcpStreaming, 0, SWT.CENTER );
    fdGcpStreaming.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdGcpStreaming.right = new FormAttachment( 95, 0 );
    wGcpStreaming.setLayoutData( fdGcpStreaming );
    lastControl = wGcpStreaming;

    FormData fdDataflowComp = new FormData();
    fdDataflowComp.left = new FormAttachment( 0, 0 );
    fdDataflowComp.top = new FormAttachment( 0, 0 );
    fdDataflowComp.right = new FormAttachment( 100, 0 );
    fdDataflowComp.bottom = new FormAttachment( 100, 0 );
    wDataflowComp.setLayoutData( fdDataflowComp );

    wDataflowComp.pack();
    Rectangle bounds = wDataflowComp.getBounds();

    wDataflowSComp.setContent( wDataflowComp );
    wDataflowSComp.setExpandHorizontal( true );
    wDataflowSComp.setExpandVertical( true );
    wDataflowSComp.setMinWidth( bounds.width );
    wDataflowSComp.setMinHeight( bounds.height );

    wDataflowTab.setControl( wDataflowSComp );
  }

  private void addSparkTab() {

    int middle = 65;

    wSparkTab = new CTabItem( wTabFolder, SWT.NONE );
    wSparkTab.setText( "  Spark  " );

    wSparkSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wSparkSComp.setLayout( new FillLayout() );

    wSparkComp = new Composite( wSparkSComp, SWT.NONE );
    props.setLook( wSparkComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wSparkComp.setLayout( fileLayout );

    // Spark local processing?
    //
    Label wlSparkLocal = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkLocal );
    wlSparkLocal.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkLocal.Label" ) );
    FormData fdlSparkLocal = new FormData();
    fdlSparkLocal.top = new FormAttachment( 0, 0 );
    fdlSparkLocal.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkLocal.right = new FormAttachment( middle, -margin );
    wlSparkLocal.setLayoutData( fdlSparkLocal );
    wSparkLocal = new Button( wSparkComp, SWT.CHECK | SWT.LEFT );
    props.setLook( wSparkLocal );
    FormData fdSparkLocal = new FormData();
    fdSparkLocal.top = new FormAttachment( wlSparkLocal, 0, SWT.CENTER );
    fdSparkLocal.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkLocal.right = new FormAttachment( 95, 0 );
    wSparkLocal.setLayoutData( fdSparkLocal );
    Control lastControl = wSparkLocal;

    wSparkLocal.addListener( SWT.Selection, e -> {
      enableFields();
    } );

    // Spark master
    //
    wlSparkMaster = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkMaster );
    wlSparkMaster.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkMaster.Label" ) );
    FormData fdlSparkMaster = new FormData();
    fdlSparkMaster.top = new FormAttachment( lastControl, margin );
    fdlSparkMaster.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkMaster.right = new FormAttachment( middle, -margin );
    wlSparkMaster.setLayoutData( fdlSparkMaster );
    wSparkMaster = new TextVar( variables, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkMaster );
    FormData fdSparkMaster = new FormData();
    fdSparkMaster.top = new FormAttachment( wlSparkMaster, 0, SWT.CENTER );
    fdSparkMaster.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkMaster.right = new FormAttachment( 95, 0 );
    wSparkMaster.setLayoutData( fdSparkMaster );
    lastControl = wSparkMaster;

    // Folder to deploy Spark submit artifacts in
    //
    Label wlSparkDeployFolder = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkDeployFolder );
    wlSparkDeployFolder.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkDeployFolder.Label" ) );
    FormData fdlSparkDeployFolder = new FormData();
    fdlSparkDeployFolder.top = new FormAttachment( lastControl, margin );
    fdlSparkDeployFolder.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkDeployFolder.right = new FormAttachment( middle, -margin );
    wlSparkDeployFolder.setLayoutData( fdlSparkDeployFolder );
    wSparkDeployFolder = new TextVar( variables, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkDeployFolder );
    FormData fdSparkDeployFolder = new FormData();
    fdSparkDeployFolder.top = new FormAttachment( wlSparkDeployFolder, 0, SWT.CENTER );
    fdSparkDeployFolder.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkDeployFolder.right = new FormAttachment( 95, 0 );
    wSparkDeployFolder.setLayoutData( fdSparkDeployFolder );
    lastControl = wSparkDeployFolder;

    // Spark batch interval in ms
    //
    Label wlSparkBatchIntervalMillis = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkBatchIntervalMillis );
    wlSparkBatchIntervalMillis.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkBatchIntervalMillis.Label" ) );
    FormData fdlSparkBatchIntervalMillis = new FormData();
    fdlSparkBatchIntervalMillis.top = new FormAttachment( lastControl, margin );
    fdlSparkBatchIntervalMillis.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkBatchIntervalMillis.right = new FormAttachment( middle, -margin );
    wlSparkBatchIntervalMillis.setLayoutData( fdlSparkBatchIntervalMillis );
    wSparkBatchIntervalMillis = new TextVar( variables, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkBatchIntervalMillis );
    FormData fdSparkBatchIntervalMillis = new FormData();
    fdSparkBatchIntervalMillis.top = new FormAttachment( wlSparkBatchIntervalMillis, 0, SWT.CENTER );
    fdSparkBatchIntervalMillis.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkBatchIntervalMillis.right = new FormAttachment( 95, 0 );
    wSparkBatchIntervalMillis.setLayoutData( fdSparkBatchIntervalMillis );
    lastControl = wSparkBatchIntervalMillis;

    // Spark checkpoint directory
    //
    Label wlSparkCheckpointDir = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkCheckpointDir );
    wlSparkCheckpointDir.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkCheckpointDir.Label" ) );
    FormData fdlSparkCheckpointDir = new FormData();
    fdlSparkCheckpointDir.top = new FormAttachment( lastControl, margin );
    fdlSparkCheckpointDir.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkCheckpointDir.right = new FormAttachment( middle, -margin );
    wlSparkCheckpointDir.setLayoutData( fdlSparkCheckpointDir );
    wSparkCheckpointDir = new TextVar( variables, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkCheckpointDir );
    FormData fdSparkCheckpointDir = new FormData();
    fdSparkCheckpointDir.top = new FormAttachment( wlSparkCheckpointDir, 0, SWT.CENTER );
    fdSparkCheckpointDir.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkCheckpointDir.right = new FormAttachment( 95, 0 );
    wSparkCheckpointDir.setLayoutData( fdSparkCheckpointDir );
    lastControl = wSparkCheckpointDir;

    // Spark checkpoint duration ms
    //
    Label wlSparkCheckpointDurationMillis = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkCheckpointDurationMillis );
    wlSparkCheckpointDurationMillis.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkCheckpointDurationMillis.Label" ) );
    FormData fdlSparkCheckpointDurationMillis = new FormData();
    fdlSparkCheckpointDurationMillis.top = new FormAttachment( lastControl, margin );
    fdlSparkCheckpointDurationMillis.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkCheckpointDurationMillis.right = new FormAttachment( middle, -margin );
    wlSparkCheckpointDurationMillis.setLayoutData( fdlSparkCheckpointDurationMillis );
    wSparkCheckpointDurationMillis = new TextVar( variables, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkCheckpointDurationMillis );
    FormData fdSparkCheckpointDurationMillis = new FormData();
    fdSparkCheckpointDurationMillis.top = new FormAttachment( wlSparkCheckpointDurationMillis, 0, SWT.CENTER );
    fdSparkCheckpointDurationMillis.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkCheckpointDurationMillis.right = new FormAttachment( 95, 0 );
    wSparkCheckpointDurationMillis.setLayoutData( fdSparkCheckpointDurationMillis );
    lastControl = wSparkCheckpointDurationMillis;

    // Spark : enable spark metrics sink
    //
    Label wlsparkEnableSparkMetricSinks = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlsparkEnableSparkMetricSinks );
    wlsparkEnableSparkMetricSinks.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.sparkEnableSparkMetricSinks.Label" ) );
    FormData fdlsparkEnableSparkMetricSinks = new FormData();
    fdlsparkEnableSparkMetricSinks.top = new FormAttachment( lastControl, margin );
    fdlsparkEnableSparkMetricSinks.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlsparkEnableSparkMetricSinks.right = new FormAttachment( middle, -margin );
    wlsparkEnableSparkMetricSinks.setLayoutData( fdlsparkEnableSparkMetricSinks );
    wsparkEnableSparkMetricSinks = new Button( wSparkComp, SWT.CHECK | SWT.LEFT | SWT.BORDER );
    props.setLook( wsparkEnableSparkMetricSinks );
    FormData fdsparkEnableSparkMetricSinks = new FormData();
    fdsparkEnableSparkMetricSinks.top = new FormAttachment( wlsparkEnableSparkMetricSinks, 0, SWT.CENTER );
    fdsparkEnableSparkMetricSinks.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdsparkEnableSparkMetricSinks.right = new FormAttachment( 95, 0 );
    wsparkEnableSparkMetricSinks.setLayoutData( fdsparkEnableSparkMetricSinks );
    lastControl = wsparkEnableSparkMetricSinks;

    // Spark: Max records per batch
    //
    Label wlSparkMaxRecordsPerBatch = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkMaxRecordsPerBatch );
    wlSparkMaxRecordsPerBatch.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkMaxRecordsPerBatch.Label" ) );
    FormData fdlSparkMaxRecordsPerBatch = new FormData();
    fdlSparkMaxRecordsPerBatch.top = new FormAttachment( lastControl, margin );
    fdlSparkMaxRecordsPerBatch.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkMaxRecordsPerBatch.right = new FormAttachment( middle, -margin );
    wlSparkMaxRecordsPerBatch.setLayoutData( fdlSparkMaxRecordsPerBatch );
    wSparkMaxRecordsPerBatch = new TextVar( variables, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkMaxRecordsPerBatch );
    FormData fdSparkMaxRecordsPerBatch = new FormData();
    fdSparkMaxRecordsPerBatch.top = new FormAttachment( wlSparkMaxRecordsPerBatch, 0, SWT.CENTER );
    fdSparkMaxRecordsPerBatch.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkMaxRecordsPerBatch.right = new FormAttachment( 95, 0 );
    wSparkMaxRecordsPerBatch.setLayoutData( fdSparkMaxRecordsPerBatch );
    lastControl = wSparkMaxRecordsPerBatch;


    // Spark: minimum read time in ms
    //
    Label wlSparkMinReadTimeMillis = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkMinReadTimeMillis );
    wlSparkMinReadTimeMillis.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkMinReadTimeMillis.Label" ) );
    FormData fdlSparkMinReadTimeMillis = new FormData();
    fdlSparkMinReadTimeMillis.top = new FormAttachment( lastControl, margin );
    fdlSparkMinReadTimeMillis.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkMinReadTimeMillis.right = new FormAttachment( middle, -margin );
    wlSparkMinReadTimeMillis.setLayoutData( fdlSparkMinReadTimeMillis );
    wSparkMinReadTimeMillis = new TextVar( variables, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkMinReadTimeMillis );
    FormData fdSparkMinReadTimeMillis = new FormData();
    fdSparkMinReadTimeMillis.top = new FormAttachment( wlSparkMinReadTimeMillis, 0, SWT.CENTER );
    fdSparkMinReadTimeMillis.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkMinReadTimeMillis.right = new FormAttachment( 95, 0 );
    wSparkMinReadTimeMillis.setLayoutData( fdSparkMinReadTimeMillis );
    lastControl = wSparkMinReadTimeMillis;

    // Spark read time %
    //
    Label wlSparkReadTimePercentage = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkReadTimePercentage );
    wlSparkReadTimePercentage.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkReadTimePercentage.Label" ) );
    FormData fdlSparkReadTimePercentage = new FormData();
    fdlSparkReadTimePercentage.top = new FormAttachment( lastControl, margin );
    fdlSparkReadTimePercentage.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkReadTimePercentage.right = new FormAttachment( middle, -margin );
    wlSparkReadTimePercentage.setLayoutData( fdlSparkReadTimePercentage );
    wSparkReadTimePercentage = new TextVar( variables, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkReadTimePercentage );
    FormData fdSparkReadTimePercentage = new FormData();
    fdSparkReadTimePercentage.top = new FormAttachment( wlSparkReadTimePercentage, 0, SWT.CENTER );
    fdSparkReadTimePercentage.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkReadTimePercentage.right = new FormAttachment( 95, 0 );
    wSparkReadTimePercentage.setLayoutData( fdSparkReadTimePercentage );
    lastControl = wSparkReadTimePercentage;

    // Bundle size
    //
    Label wlSparkBundleSize = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkBundleSize );
    wlSparkBundleSize.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkBundleSize.Label" ) );
    FormData fdlSparkBundleSize = new FormData();
    fdlSparkBundleSize.top = new FormAttachment( lastControl, margin );
    fdlSparkBundleSize.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkBundleSize.right = new FormAttachment( middle, -margin );
    wlSparkBundleSize.setLayoutData( fdlSparkBundleSize );
    wSparkBundleSize = new TextVar( variables, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkBundleSize );
    FormData fdSparkBundleSize = new FormData();
    fdSparkBundleSize.top = new FormAttachment( wlSparkBundleSize, 0, SWT.CENTER );
    fdSparkBundleSize.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkBundleSize.right = new FormAttachment( 95, 0 );
    wSparkBundleSize.setLayoutData( fdSparkBundleSize );
    lastControl = wSparkBundleSize;

    // Storage level
    //
    Label wlSparkStorageLevel = new Label( wSparkComp, SWT.RIGHT );
    props.setLook( wlSparkStorageLevel );
    wlSparkStorageLevel.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.SparkStorageLevel.Label" ) );
    FormData fdlSparkStorageLevel = new FormData();
    fdlSparkStorageLevel.top = new FormAttachment( lastControl, margin );
    fdlSparkStorageLevel.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlSparkStorageLevel.right = new FormAttachment( middle, -margin );
    wlSparkStorageLevel.setLayoutData( fdlSparkStorageLevel );
    wSparkStorageLevel = new ComboVar( variables, wSparkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wSparkStorageLevel );
    wSparkStorageLevel.setItems( new String[] { "MEMORY_ONLY", "MEMORY_ONLY_SER", "MEMORY_AND_DISK" } );
    FormData fdSparkStorageLevel = new FormData();
    fdSparkStorageLevel.top = new FormAttachment( wlSparkStorageLevel, 0, SWT.CENTER );
    fdSparkStorageLevel.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdSparkStorageLevel.right = new FormAttachment( 95, 0 );
    wSparkStorageLevel.setLayoutData( fdSparkStorageLevel );
    lastControl = wSparkStorageLevel;


    FormData fdSparkComp = new FormData();
    fdSparkComp.left = new FormAttachment( 0, 0 );
    fdSparkComp.top = new FormAttachment( 0, 0 );
    fdSparkComp.right = new FormAttachment( 100, 0 );
    fdSparkComp.bottom = new FormAttachment( 100, 0 );
    wSparkComp.setLayoutData( fdSparkComp );

    wSparkComp.pack();
    Rectangle bounds = wSparkComp.getBounds();

    wSparkSComp.setContent( wSparkComp );
    wSparkSComp.setExpandHorizontal( true );
    wSparkSComp.setExpandVertical( true );
    wSparkSComp.setMinWidth( bounds.width );
    wSparkSComp.setMinHeight( bounds.height );

    wSparkTab.setControl( wSparkSComp );
  }

  private void addFlinkTab() {

    int middle = 65;

    wFlinkTab = new CTabItem( wTabFolder, SWT.NONE );
    wFlinkTab.setText( "  Flink  " );

    wFlinkSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL );
    wFlinkSComp.setLayout( new FillLayout() );

    wFlinkComp = new Composite( wFlinkSComp, SWT.NONE );
    props.setLook( wFlinkComp );

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFlinkComp.setLayout( fileLayout );

    // Flink local processing?
    //
    Label wlFlinkLocal = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkLocal );
    wlFlinkLocal.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkLocal.Label" ) );
    FormData fdlFlinkLocal = new FormData();
    fdlFlinkLocal.top = new FormAttachment( 0, 0 );
    fdlFlinkLocal.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkLocal.right = new FormAttachment( middle, -margin );
    wlFlinkLocal.setLayoutData( fdlFlinkLocal );
    wFlinkLocal = new Button( wFlinkComp, SWT.CHECK | SWT.LEFT );
    props.setLook( wFlinkLocal );
    FormData fdFlinkLocal = new FormData();
    fdFlinkLocal.top = new FormAttachment( wlFlinkLocal, 0, SWT.CENTER );
    fdFlinkLocal.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkLocal.right = new FormAttachment( 95, 0 );
    wFlinkLocal.setLayoutData( fdFlinkLocal );
    Control lastControl = wFlinkLocal;

    wFlinkLocal.addListener( SWT.Selection, e -> {
      enableFields();
    } );

    // Flink master
    //
    wlFlinkMaster = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkMaster );
    wlFlinkMaster.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkMaster.Label" ) );
    FormData fdlFlinkMaster = new FormData();
    fdlFlinkMaster.top = new FormAttachment( lastControl, margin );
    fdlFlinkMaster.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkMaster.right = new FormAttachment( middle, -margin );
    wlFlinkMaster.setLayoutData( fdlFlinkMaster );
    wFlinkMaster = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkMaster );
    FormData fdFlinkMaster = new FormData();
    fdFlinkMaster.top = new FormAttachment( wlFlinkMaster, 0, SWT.CENTER );
    fdFlinkMaster.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkMaster.right = new FormAttachment( 95, 0 );
    wFlinkMaster.setLayoutData( fdFlinkMaster );
    lastControl = wFlinkMaster;

    Label wlFlinkParallelism = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkParallelism );
    wlFlinkParallelism.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkParallelism.Label" ) );
    FormData fdlFlinkParallelism = new FormData();
    fdlFlinkParallelism.top = new FormAttachment( lastControl, margin );
    fdlFlinkParallelism.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkParallelism.right = new FormAttachment( middle, -margin );
    wlFlinkParallelism.setLayoutData( fdlFlinkParallelism );
    wFlinkParallelism = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkParallelism );
    FormData fdFlinkParallelism = new FormData();
    fdFlinkParallelism.top = new FormAttachment( wlFlinkParallelism, 0, SWT.CENTER );
    fdFlinkParallelism.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkParallelism.right = new FormAttachment( 95, 0 );
    wFlinkParallelism.setLayoutData( fdFlinkParallelism );
    lastControl = wFlinkParallelism;

    Label wlFlinkCheckpointingInterval = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkCheckpointingInterval );
    wlFlinkCheckpointingInterval.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkCheckpointingInterval.Label" ) );
    FormData fdlFlinkCheckpointingInterval = new FormData();
    fdlFlinkCheckpointingInterval.top = new FormAttachment( lastControl, margin );
    fdlFlinkCheckpointingInterval.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkCheckpointingInterval.right = new FormAttachment( middle, -margin );
    wlFlinkCheckpointingInterval.setLayoutData( fdlFlinkCheckpointingInterval );
    wFlinkCheckpointingInterval = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkCheckpointingInterval );
    FormData fdFlinkCheckpointingInterval = new FormData();
    fdFlinkCheckpointingInterval.top = new FormAttachment( wlFlinkCheckpointingInterval, 0, SWT.CENTER );
    fdFlinkCheckpointingInterval.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkCheckpointingInterval.right = new FormAttachment( 95, 0 );
    wFlinkCheckpointingInterval.setLayoutData( fdFlinkCheckpointingInterval );
    lastControl = wFlinkCheckpointingInterval;

    Label wlFlinkCheckpointingMode = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkCheckpointingMode );
    wlFlinkCheckpointingMode.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkCheckpointingMode.Label" ) );
    FormData fdlFlinkCheckpointingMode = new FormData();
    fdlFlinkCheckpointingMode.top = new FormAttachment( lastControl, margin );
    fdlFlinkCheckpointingMode.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkCheckpointingMode.right = new FormAttachment( middle, -margin );
    wlFlinkCheckpointingMode.setLayoutData( fdlFlinkCheckpointingMode );
    wFlinkCheckpointingMode = new ComboVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wFlinkCheckpointingMode.setItems( new String[] { "EXACTLY_ONCE", "AT_LEAST_ONCE" } );
    props.setLook( wFlinkCheckpointingMode );
    FormData fdFlinkCheckpointingMode = new FormData();
    fdFlinkCheckpointingMode.top = new FormAttachment( wlFlinkCheckpointingMode, 0, SWT.CENTER );
    fdFlinkCheckpointingMode.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkCheckpointingMode.right = new FormAttachment( 95, 0 );
    wFlinkCheckpointingMode.setLayoutData( fdFlinkCheckpointingMode );
    lastControl = wFlinkCheckpointingMode;

    Label wlFlinkCheckpointTimeoutMillis = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkCheckpointTimeoutMillis );
    wlFlinkCheckpointTimeoutMillis.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkCheckpointTimeoutMillis.Label" ) );
    FormData fdlFlinkCheckpointTimeoutMillis = new FormData();
    fdlFlinkCheckpointTimeoutMillis.top = new FormAttachment( lastControl, margin );
    fdlFlinkCheckpointTimeoutMillis.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkCheckpointTimeoutMillis.right = new FormAttachment( middle, -margin );
    wlFlinkCheckpointTimeoutMillis.setLayoutData( fdlFlinkCheckpointTimeoutMillis );
    wFlinkCheckpointTimeoutMillis = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkCheckpointTimeoutMillis );
    FormData fdFlinkCheckpointTimeoutMillis = new FormData();
    fdFlinkCheckpointTimeoutMillis.top = new FormAttachment( wlFlinkCheckpointTimeoutMillis, 0, SWT.CENTER );
    fdFlinkCheckpointTimeoutMillis.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkCheckpointTimeoutMillis.right = new FormAttachment( 95, 0 );
    wFlinkCheckpointTimeoutMillis.setLayoutData( fdFlinkCheckpointTimeoutMillis );
    lastControl = wFlinkCheckpointTimeoutMillis;

    Label wlFlinkMinPauseBetweenCheckpoints = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkMinPauseBetweenCheckpoints );
    wlFlinkMinPauseBetweenCheckpoints.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkMinPauseBetweenCheckpoints.Label" ) );
    FormData fdlFlinkMinPauseBetweenCheckpoints = new FormData();
    fdlFlinkMinPauseBetweenCheckpoints.top = new FormAttachment( lastControl, margin );
    fdlFlinkMinPauseBetweenCheckpoints.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkMinPauseBetweenCheckpoints.right = new FormAttachment( middle, -margin );
    wlFlinkMinPauseBetweenCheckpoints.setLayoutData( fdlFlinkMinPauseBetweenCheckpoints );
    wFlinkMinPauseBetweenCheckpoints = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkMinPauseBetweenCheckpoints );
    FormData fdFlinkMinPauseBetweenCheckpoints = new FormData();
    fdFlinkMinPauseBetweenCheckpoints.top = new FormAttachment( wlFlinkMinPauseBetweenCheckpoints, 0, SWT.CENTER );
    fdFlinkMinPauseBetweenCheckpoints.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkMinPauseBetweenCheckpoints.right = new FormAttachment( 95, 0 );
    wFlinkMinPauseBetweenCheckpoints.setLayoutData( fdFlinkMinPauseBetweenCheckpoints );
    lastControl = wFlinkMinPauseBetweenCheckpoints;

    Label wlFlinkNumberOfExecutionRetries = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkNumberOfExecutionRetries );
    wlFlinkNumberOfExecutionRetries.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkNumberOfExecutionRetries.Label" ) );
    FormData fdlFlinkNumberOfExecutionRetries = new FormData();
    fdlFlinkNumberOfExecutionRetries.top = new FormAttachment( lastControl, margin );
    fdlFlinkNumberOfExecutionRetries.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkNumberOfExecutionRetries.right = new FormAttachment( middle, -margin );
    wlFlinkNumberOfExecutionRetries.setLayoutData( fdlFlinkNumberOfExecutionRetries );
    wFlinkNumberOfExecutionRetries = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkNumberOfExecutionRetries );
    FormData fdFlinkNumberOfExecutionRetries = new FormData();
    fdFlinkNumberOfExecutionRetries.top = new FormAttachment( wlFlinkNumberOfExecutionRetries, 0, SWT.CENTER );
    fdFlinkNumberOfExecutionRetries.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkNumberOfExecutionRetries.right = new FormAttachment( 95, 0 );
    wFlinkNumberOfExecutionRetries.setLayoutData( fdFlinkNumberOfExecutionRetries );
    lastControl = wFlinkNumberOfExecutionRetries;

    Label wlFlinkExecutionRetryDelay = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkExecutionRetryDelay );
    wlFlinkExecutionRetryDelay.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkExecutionRetryDelay.Label" ) );
    FormData fdlFlinkExecutionRetryDelay = new FormData();
    fdlFlinkExecutionRetryDelay.top = new FormAttachment( lastControl, margin );
    fdlFlinkExecutionRetryDelay.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkExecutionRetryDelay.right = new FormAttachment( middle, -margin );
    wlFlinkExecutionRetryDelay.setLayoutData( fdlFlinkExecutionRetryDelay );
    wFlinkExecutionRetryDelay = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkExecutionRetryDelay );
    FormData fdFlinkExecutionRetryDelay = new FormData();
    fdFlinkExecutionRetryDelay.top = new FormAttachment( wlFlinkExecutionRetryDelay, 0, SWT.CENTER );
    fdFlinkExecutionRetryDelay.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkExecutionRetryDelay.right = new FormAttachment( 95, 0 );
    wFlinkExecutionRetryDelay.setLayoutData( fdFlinkExecutionRetryDelay );
    lastControl = wFlinkExecutionRetryDelay;

    Label wlFlinkObjectReuse = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkObjectReuse );
    wlFlinkObjectReuse.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkObjectReuse.Label" ) );
    FormData fdlFlinkObjectReuse = new FormData();
    fdlFlinkObjectReuse.top = new FormAttachment( lastControl, margin );
    fdlFlinkObjectReuse.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkObjectReuse.right = new FormAttachment( middle, -margin );
    wlFlinkObjectReuse.setLayoutData( fdlFlinkObjectReuse );
    wFlinkObjectReuse = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkObjectReuse );
    FormData fdFlinkObjectReuse = new FormData();
    fdFlinkObjectReuse.top = new FormAttachment( wlFlinkObjectReuse, 0, SWT.CENTER );
    fdFlinkObjectReuse.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkObjectReuse.right = new FormAttachment( 95, 0 );
    wFlinkObjectReuse.setLayoutData( fdFlinkObjectReuse );
    lastControl = wFlinkObjectReuse;


    Label wlFlinkStateBackend = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkStateBackend );
    wlFlinkStateBackend.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkStateBackend.Label" ) );
    FormData fdlFlinkStateBackend = new FormData();
    fdlFlinkStateBackend.top = new FormAttachment( lastControl, margin );
    fdlFlinkStateBackend.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkStateBackend.right = new FormAttachment( middle, -margin );
    wlFlinkStateBackend.setLayoutData( fdlFlinkStateBackend );
    wFlinkStateBackend = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkStateBackend );
    FormData fdFlinkStateBackend = new FormData();
    fdFlinkStateBackend.top = new FormAttachment( wlFlinkStateBackend, 0, SWT.CENTER );
    fdFlinkStateBackend.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkStateBackend.right = new FormAttachment( 95, 0 );
    wFlinkStateBackend.setLayoutData( fdFlinkStateBackend );
    lastControl = wFlinkStateBackend;
    // TODO: figure out what to do here.
    wlFlinkStateBackend.setEnabled( false );
    wFlinkStateBackend.setEnabled( false );

    Label wlFlinkDisbaleMetrics = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkDisbaleMetrics );
    wlFlinkDisbaleMetrics.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkDisbaleMetrics.Label" ) );
    FormData fdlFlinkDisbaleMetrics = new FormData();
    fdlFlinkDisbaleMetrics.top = new FormAttachment( lastControl, margin );
    fdlFlinkDisbaleMetrics.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkDisbaleMetrics.right = new FormAttachment( middle, -margin );
    wlFlinkDisbaleMetrics.setLayoutData( fdlFlinkDisbaleMetrics );
    wFlinkDisbaleMetrics = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkDisbaleMetrics );
    FormData fdFlinkDisbaleMetrics = new FormData();
    fdFlinkDisbaleMetrics.top = new FormAttachment( wlFlinkDisbaleMetrics, 0, SWT.CENTER );
    fdFlinkDisbaleMetrics.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkDisbaleMetrics.right = new FormAttachment( 95, 0 );
    wFlinkDisbaleMetrics.setLayoutData( fdFlinkDisbaleMetrics );
    lastControl = wFlinkDisbaleMetrics;

    Label wlFlinkExternalizedCheckpointsEnabled = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkExternalizedCheckpointsEnabled );
    wlFlinkExternalizedCheckpointsEnabled.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkExternalizedCheckpointsEnabled.Label" ) );
    FormData fdlFlinkExternalizedCheckpointsEnabled = new FormData();
    fdlFlinkExternalizedCheckpointsEnabled.top = new FormAttachment( lastControl, margin );
    fdlFlinkExternalizedCheckpointsEnabled.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkExternalizedCheckpointsEnabled.right = new FormAttachment( middle, -margin );
    wlFlinkExternalizedCheckpointsEnabled.setLayoutData( fdlFlinkExternalizedCheckpointsEnabled );
    wFlinkExternalizedCheckpointsEnabled = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkExternalizedCheckpointsEnabled );
    FormData fdFlinkExternalizedCheckpointsEnabled = new FormData();
    fdFlinkExternalizedCheckpointsEnabled.top = new FormAttachment( wlFlinkExternalizedCheckpointsEnabled, 0, SWT.CENTER );
    fdFlinkExternalizedCheckpointsEnabled.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkExternalizedCheckpointsEnabled.right = new FormAttachment( 95, 0 );
    wFlinkExternalizedCheckpointsEnabled.setLayoutData( fdFlinkExternalizedCheckpointsEnabled );
    lastControl = wFlinkExternalizedCheckpointsEnabled;

    Label wlFlinkRetainExternalizedCheckpointsOnCancellation = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkRetainExternalizedCheckpointsOnCancellation );
    wlFlinkRetainExternalizedCheckpointsOnCancellation.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkRetainExternalizedCheckpointsOnCancellation.Label" ) );
    FormData fdlFlinkRetainExternalizedCheckpointsOnCancellation = new FormData();
    fdlFlinkRetainExternalizedCheckpointsOnCancellation.top = new FormAttachment( lastControl, margin );
    fdlFlinkRetainExternalizedCheckpointsOnCancellation.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkRetainExternalizedCheckpointsOnCancellation.right = new FormAttachment( middle, -margin );
    wlFlinkRetainExternalizedCheckpointsOnCancellation.setLayoutData( fdlFlinkRetainExternalizedCheckpointsOnCancellation );
    wFlinkRetainExternalizedCheckpointsOnCancellation = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkRetainExternalizedCheckpointsOnCancellation );
    FormData fdFlinkRetainExternalizedCheckpointsOnCancellation = new FormData();
    fdFlinkRetainExternalizedCheckpointsOnCancellation.top = new FormAttachment( wlFlinkRetainExternalizedCheckpointsOnCancellation, 0, SWT.CENTER );
    fdFlinkRetainExternalizedCheckpointsOnCancellation.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkRetainExternalizedCheckpointsOnCancellation.right = new FormAttachment( 95, 0 );
    wFlinkRetainExternalizedCheckpointsOnCancellation.setLayoutData( fdFlinkRetainExternalizedCheckpointsOnCancellation );
    lastControl = wFlinkRetainExternalizedCheckpointsOnCancellation;

    Label wlFlinkMaxBundleSize = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkMaxBundleSize );
    wlFlinkMaxBundleSize.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkMaxBundleSize.Label" ) );
    FormData fdlFlinkMaxBundleSize = new FormData();
    fdlFlinkMaxBundleSize.top = new FormAttachment( lastControl, margin );
    fdlFlinkMaxBundleSize.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkMaxBundleSize.right = new FormAttachment( middle, -margin );
    wlFlinkMaxBundleSize.setLayoutData( fdlFlinkMaxBundleSize );
    wFlinkMaxBundleSize = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkMaxBundleSize );
    FormData fdFlinkMaxBundleSize = new FormData();
    fdFlinkMaxBundleSize.top = new FormAttachment( wlFlinkMaxBundleSize, 0, SWT.CENTER );
    fdFlinkMaxBundleSize.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkMaxBundleSize.right = new FormAttachment( 95, 0 );
    wFlinkMaxBundleSize.setLayoutData( fdFlinkMaxBundleSize );
    lastControl = wFlinkMaxBundleSize;

    Label wlFlinkMaxBundleTimeMills = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkMaxBundleTimeMills );
    wlFlinkMaxBundleTimeMills.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkMaxBundleTimeMills.Label" ) );
    FormData fdlFlinkMaxBundleTimeMills = new FormData();
    fdlFlinkMaxBundleTimeMills.top = new FormAttachment( lastControl, margin );
    fdlFlinkMaxBundleTimeMills.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkMaxBundleTimeMills.right = new FormAttachment( middle, -margin );
    wlFlinkMaxBundleTimeMills.setLayoutData( fdlFlinkMaxBundleTimeMills );
    wFlinkMaxBundleTimeMills = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkMaxBundleTimeMills );
    FormData fdFlinkMaxBundleTimeMills = new FormData();
    fdFlinkMaxBundleTimeMills.top = new FormAttachment( wlFlinkMaxBundleTimeMills, 0, SWT.CENTER );
    fdFlinkMaxBundleTimeMills.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkMaxBundleTimeMills.right = new FormAttachment( 95, 0 );
    wFlinkMaxBundleTimeMills.setLayoutData( fdFlinkMaxBundleTimeMills );
    lastControl = wFlinkMaxBundleTimeMills;

    Label wlFlinkShutdownSourcesOnFinalWatermark = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkShutdownSourcesOnFinalWatermark );
    wlFlinkShutdownSourcesOnFinalWatermark.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkShutdownSourcesOnFinalWatermark.Label" ) );
    FormData fdlFlinkShutdownSourcesOnFinalWatermark = new FormData();
    fdlFlinkShutdownSourcesOnFinalWatermark.top = new FormAttachment( lastControl, margin );
    fdlFlinkShutdownSourcesOnFinalWatermark.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkShutdownSourcesOnFinalWatermark.right = new FormAttachment( middle, -margin );
    wlFlinkShutdownSourcesOnFinalWatermark.setLayoutData( fdlFlinkShutdownSourcesOnFinalWatermark );
    wFlinkShutdownSourcesOnFinalWatermark = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkShutdownSourcesOnFinalWatermark );
    FormData fdFlinkShutdownSourcesOnFinalWatermark = new FormData();
    fdFlinkShutdownSourcesOnFinalWatermark.top = new FormAttachment( wlFlinkShutdownSourcesOnFinalWatermark, 0, SWT.CENTER );
    fdFlinkShutdownSourcesOnFinalWatermark.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkShutdownSourcesOnFinalWatermark.right = new FormAttachment( 95, 0 );
    wFlinkShutdownSourcesOnFinalWatermark.setLayoutData( fdFlinkShutdownSourcesOnFinalWatermark );
    lastControl = wFlinkShutdownSourcesOnFinalWatermark;

    Label wlFlinkLatencyTrackingInterval = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkLatencyTrackingInterval );
    wlFlinkLatencyTrackingInterval.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkLatencyTrackingInterval.Label" ) );
    FormData fdlFlinkLatencyTrackingInterval = new FormData();
    fdlFlinkLatencyTrackingInterval.top = new FormAttachment( lastControl, margin );
    fdlFlinkLatencyTrackingInterval.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkLatencyTrackingInterval.right = new FormAttachment( middle, -margin );
    wlFlinkLatencyTrackingInterval.setLayoutData( fdlFlinkLatencyTrackingInterval );
    wFlinkLatencyTrackingInterval = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkLatencyTrackingInterval );
    FormData fdFlinkLatencyTrackingInterval = new FormData();
    fdFlinkLatencyTrackingInterval.top = new FormAttachment( wlFlinkLatencyTrackingInterval, 0, SWT.CENTER );
    fdFlinkLatencyTrackingInterval.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkLatencyTrackingInterval.right = new FormAttachment( 95, 0 );
    wFlinkLatencyTrackingInterval.setLayoutData( fdFlinkLatencyTrackingInterval );
    lastControl = wFlinkLatencyTrackingInterval;

    Label wlFlinkAutoWatermarkInterval = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkAutoWatermarkInterval );
    wlFlinkAutoWatermarkInterval.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkAutoWatermarkInterval.Label" ) );
    FormData fdlFlinkAutoWatermarkInterval = new FormData();
    fdlFlinkAutoWatermarkInterval.top = new FormAttachment( lastControl, margin );
    fdlFlinkAutoWatermarkInterval.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkAutoWatermarkInterval.right = new FormAttachment( middle, -margin );
    wlFlinkAutoWatermarkInterval.setLayoutData( fdlFlinkAutoWatermarkInterval );
    wFlinkAutoWatermarkInterval = new TextVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkAutoWatermarkInterval );
    FormData fdFlinkAutoWatermarkInterval = new FormData();
    fdFlinkAutoWatermarkInterval.top = new FormAttachment( wlFlinkAutoWatermarkInterval, 0, SWT.CENTER );
    fdFlinkAutoWatermarkInterval.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkAutoWatermarkInterval.right = new FormAttachment( 95, 0 );
    wFlinkAutoWatermarkInterval.setLayoutData( fdFlinkAutoWatermarkInterval );
    lastControl = wFlinkAutoWatermarkInterval;

    Label wlFlinkExecutionModeForBatch = new Label( wFlinkComp, SWT.RIGHT );
    props.setLook( wlFlinkExecutionModeForBatch );
    wlFlinkExecutionModeForBatch.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.FlinkExecutionModeForBatch.Label" ) );
    FormData fdlFlinkExecutionModeForBatch = new FormData();
    fdlFlinkExecutionModeForBatch.top = new FormAttachment( lastControl, margin );
    fdlFlinkExecutionModeForBatch.left = new FormAttachment( 0, -margin ); // First one in the left top corner
    fdlFlinkExecutionModeForBatch.right = new FormAttachment( middle, -margin );
    wlFlinkExecutionModeForBatch.setLayoutData( fdlFlinkExecutionModeForBatch );
    wFlinkExecutionModeForBatch = new ComboVar( variables, wFlinkComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wFlinkExecutionModeForBatch );
    wFlinkExecutionModeForBatch.setItems( new String[] { "PIPELINED", "PIPELINED_FORCED", "BATCH", "BATCH_FORCED" } );
    FormData fdFlinkExecutionModeForBatch = new FormData();
    fdFlinkExecutionModeForBatch.top = new FormAttachment( wlFlinkExecutionModeForBatch, 0, SWT.CENTER );
    fdFlinkExecutionModeForBatch.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdFlinkExecutionModeForBatch.right = new FormAttachment( 95, 0 );
    wFlinkExecutionModeForBatch.setLayoutData( fdFlinkExecutionModeForBatch );
    lastControl = wFlinkExecutionModeForBatch;


    FormData fdFlinkComp = new FormData();
    fdFlinkComp.left = new FormAttachment( 0, 0 );
    fdFlinkComp.top = new FormAttachment( 0, 0 );
    fdFlinkComp.right = new FormAttachment( 100, 0 );
    fdFlinkComp.bottom = new FormAttachment( 100, 0 );
    wFlinkComp.setLayoutData( fdFlinkComp );

    wFlinkComp.pack();
    Rectangle bounds = wFlinkComp.getBounds();

    wFlinkSComp.setContent( wFlinkComp );
    wFlinkSComp.setExpandHorizontal( true );
    wFlinkSComp.setExpandVertical( true );
    wFlinkSComp.setMinWidth( bounds.width );
    wFlinkSComp.setMinHeight( bounds.height );

    wFlinkTab.setControl( wFlinkSComp );
  }

  private void enableFields() {

    wlSparkMaster.setEnabled( !wSparkLocal.getSelection() );
    wSparkMaster.setEnabled( !wSparkLocal.getSelection() );


    wlFlinkMaster.setEnabled( !wFlinkLocal.getSelection() );
    wFlinkMaster.setEnabled( !wFlinkLocal.getSelection() );
  }


  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {
    wName.setText( Const.NVL( config.getName(), "" ) );
    wDescription.setText( Const.NVL( config.getDescription(), "" ) );

    // General
    wRunner.setText( Const.NVL( config.getRunnerTypeName(), "" ) );
    wUserAgent.setText( Const.NVL( config.getUserAgent(), "" ) );
    wTempLocation.setText( Const.NVL( config.getTempLocation(), "" ) );
    wPluginsToStage.setText( Const.NVL( config.getPluginsToStage(), "" ) );
    wStepPluginClasses.setText( Const.NVL( config.getStepPluginClasses(), "" ) );
    wXpPluginClasses.setText( Const.NVL( config.getXpPluginClasses(), "" ) );
    wFatJar.setText( Const.NVL( config.getFatJar(), "" ) );
    wStreamingKettleStepsFlushInterval.setText( Const.NVL( config.getStreamingKettleStepsFlushInterval(), "" ) );

    // GCP
    /*
     */
    wGcpProjectId.setText( Const.NVL( config.getGcpProjectId(), "" ) );
    wGcpAppName.setText( Const.NVL( config.getGcpAppName(), "" ) );
    wGcpStagingLocation.setText( Const.NVL( config.getGcpStagingLocation(), "" ) );
    String workerCode = config.getGcpWorkerMachineType();
    String workerDescription = "";
    if ( StringUtils.isNotEmpty( workerCode ) ) {
      int index = Const.indexOfString( workerCode, BeamConst.getGcpWorkerMachineTypeCodes() );
      if ( index < 0 ) {
        workerDescription = workerCode; // variable, manually entered in general
      } else {
        workerDescription = BeamConst.getGcpWorkerMachineTypeDescriptions()[ index ];
      }
    }
    wGcpWorkerMachineType.setText( workerDescription );
    wGcpWorkerDiskType.setText( Const.NVL( config.getGcpWorkerDiskType(), "" ) );
    wGcpDiskSizeGb.setText( Const.NVL( config.getGcpDiskSizeGb(), "" ) );
    wGcpInitialNumberOfWorkers.setText( Const.NVL( config.getGcpInitialNumberOfWorkers(), "" ) );
    wGcpMaximumNumberOfWorkers.setText( Const.NVL( config.getGcpMaximumNumberOfWokers(), "" ) );
    wGcpAutoScalingAlgorithm.setText( Const.NVL( config.getGcpAutoScalingAlgorithm(), "" ) );
    wGcpStreaming.setSelection( config.isGcpStreaming() );
    String regionCode = config.getGcpRegion();
    String regionDescription = "";
    if ( StringUtils.isNotEmpty( regionCode ) ) {
      int index = Const.indexOfString( regionCode, BeamConst.getGcpRegionCodes() );
      if ( index < 0 ) {
        regionDescription = regionCode; // variable, manually entered in general
      } else {
        regionDescription = BeamConst.getGcpRegionDescriptions()[ index ];
      }
    }
    wGcpRegion.setText( regionDescription );
    wGcpZone.setText( Const.NVL( config.getGcpZone(), "" ) );

    // Spark
    wSparkLocal.setSelection( config.isSparkLocal() );
    wSparkMaster.setText( Const.NVL( config.getSparkMaster(), "" ) );
    wSparkDeployFolder.setText( Const.NVL( config.getSparkDeployFolder(), "" ) );
    wSparkBatchIntervalMillis.setText( Const.NVL( config.getSparkBatchIntervalMillis(), "" ) );
    wSparkCheckpointDir.setText( Const.NVL( config.getSparkCheckpointDir(), "" ) );
    wSparkCheckpointDurationMillis.setText( Const.NVL( config.getSparkCheckpointDurationMillis(), "" ) );
    wsparkEnableSparkMetricSinks.setSelection( config.isSparkEnableSparkMetricSinks() );
    wSparkMaxRecordsPerBatch.setText( Const.NVL( config.getSparkMaxRecordsPerBatch(), "" ) );
    wSparkMinReadTimeMillis.setText( Const.NVL( config.getSparkMinReadTimeMillis(), "" ) );
    wSparkReadTimePercentage.setText( Const.NVL( config.getSparkReadTimePercentage(), "" ) );
    wSparkBundleSize.setText( Const.NVL( config.getSparkBundleSize(), "" ) );
    wSparkStorageLevel.setText( Const.NVL( config.getSparkStorageLevel(), "" ) );

    // Flink
    wFlinkLocal.setSelection( config.isFlinkLocal() );
    wFlinkMaster.setText( Const.NVL( config.getFlinkMaster(), "" ) );
    wFlinkParallelism.setText( Const.NVL( config.getFlinkParallelism(), "" ) );
    wFlinkCheckpointingInterval.setText( Const.NVL( config.getFlinkCheckpointingInterval(), "" ) );
    wFlinkCheckpointingMode.setText( Const.NVL( config.getFlinkCheckpointingMode(), "" ) );
    wFlinkCheckpointTimeoutMillis.setText( Const.NVL( config.getFlinkCheckpointTimeoutMillis(), "" ) );
    wFlinkMinPauseBetweenCheckpoints.setText( Const.NVL( config.getFlinkMinPauseBetweenCheckpoints(), "" ) );
    wFlinkNumberOfExecutionRetries.setText( Const.NVL( config.getFlinkNumberOfExecutionRetries(), "" ) );
    wFlinkExecutionRetryDelay.setText( Const.NVL( config.getFlinkExecutionRetryDelay(), "" ) );
    wFlinkObjectReuse.setText( Const.NVL( config.getFlinkObjectReuse(), "" ) );
    wFlinkStateBackend.setText( Const.NVL( config.getFlinkStateBackend(), "" ) );
    wFlinkDisbaleMetrics.setText( Const.NVL( config.getFlinkDisableMetrics(), "" ) );
    wFlinkExternalizedCheckpointsEnabled.setText( Const.NVL( config.getFlinkExternalizedCheckpointsEnabled(), "" ) );
    wFlinkRetainExternalizedCheckpointsOnCancellation.setText( Const.NVL( config.getFlinkRetainExternalizedCheckpointsOnCancellation(), "" ) );
    wFlinkMaxBundleSize.setText( Const.NVL( config.getFlinkMaxBundleSize(), "" ) );
    wFlinkMaxBundleTimeMills.setText( Const.NVL( config.getFlinkMaxBundleTimeMills(), "" ) );
    wFlinkShutdownSourcesOnFinalWatermark.setText( Const.NVL( config.getFlinkShutdownSourcesOnFinalWatermark(), "" ) );
    wFlinkLatencyTrackingInterval.setText( Const.NVL( config.getFlinkLatencyTrackingInterval(), "" ) );
    wFlinkAutoWatermarkInterval.setText( Const.NVL( config.getFlinkAutoWatermarkInterval(), "" ) );
    wFlinkExecutionModeForBatch.setText( Const.NVL( config.getFlinkExecutionModeForBatch(), "" ) );

    // Parameters
    //
    int rowNr = 0;
    for ( JobParameter parameter : config.getParameters() ) {
      TableItem item = wParameters.table.getItem( rowNr++ );
      item.setText( 1, Const.NVL( parameter.getVariable(), "" ) );
      item.setText( 2, Const.NVL( parameter.getValue(), "" ) );
    }
    wParameters.setRowNums();
    wParameters.optWidth( true );

    enableFields();

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    ok = false;
    dispose();
  }

  public void ok() {
    if ( StringUtils.isEmpty( wName.getText() ) ) {
      MessageBox box = new MessageBox( shell, SWT.ICON_ERROR | SWT.OK );
      box.setText( BaseMessages.getString( PKG, "BeamJobConfigDialog.NoNameDialog.Title" ) );
      box.setMessage( BaseMessages.getString( PKG, "BeamJobConfigDialog.NoNameDialog.Message" ) );
      box.open();
      return;
    }
    getInfo( config );
    ok = true;
    dispose();
  }

  // Get dialog info in securityService
  private void getInfo( BeamJobConfig cfg ) {
    cfg.setName( wName.getText() );
    cfg.setDescription( wDescription.getText() );
    cfg.setRunnerTypeName( wRunner.getText() );
    cfg.setUserAgent( wUserAgent.getText() );
    cfg.setTempLocation( wTempLocation.getText() );
    cfg.setPluginsToStage( wPluginsToStage.getText() );
    cfg.setStepPluginClasses( ( wStepPluginClasses.getText() ) );
    cfg.setXpPluginClasses( ( wXpPluginClasses.getText() ) );
    cfg.setStreamingKettleStepsFlushInterval( wStreamingKettleStepsFlushInterval.getText() );
    cfg.setFatJar( wFatJar.getText() );
    cfg.setGcpProjectId( wGcpProjectId.getText() );
    cfg.setGcpAppName( wGcpAppName.getText() );
    cfg.setGcpStagingLocation( wGcpStagingLocation.getText() );
    cfg.setGcpInitialNumberOfWorkers( wGcpInitialNumberOfWorkers.getText() );
    cfg.setGcpMaximumNumberOfWokers( wGcpMaximumNumberOfWorkers.getText() );
    cfg.setGcpStreaming( wGcpStreaming.getSelection() );
    cfg.setGcpAutoScalingAlgorithm( wGcpAutoScalingAlgorithm.getText() );

    String workerMachineDesciption = wGcpWorkerMachineType.getText();
    String workerMachineCode = null;
    if ( StringUtils.isNotEmpty( workerMachineDesciption ) ) {
      int index = Const.indexOfString( workerMachineDesciption, BeamConst.getGcpWorkerMachineTypeDescriptions() );
      if ( index < 0 ) {
        workerMachineCode = workerMachineDesciption; // Variable or manually entered
      } else {
        workerMachineCode = BeamConst.getGcpWorkerMachineTypeCodes()[ index ];
      }
    }
    cfg.setGcpWorkerMachineType( workerMachineCode );

    cfg.setGcpWorkerDiskType( wGcpWorkerDiskType.getText() );
    cfg.setGcpDiskSizeGb( wGcpDiskSizeGb.getText() );
    cfg.setGcpZone( wGcpZone.getText() );

    String regionDesciption = wGcpRegion.getText();
    String regionCode = null;
    if ( StringUtils.isNotEmpty( regionDesciption ) ) {
      int index = Const.indexOfString( regionDesciption, BeamConst.getGcpRegionDescriptions() );
      if ( index < 0 ) {
        regionCode = regionDesciption; // Variable or manually entered
      } else {
        regionCode = BeamConst.getGcpRegionCodes()[ index ];
      }
    }
    cfg.setGcpRegion( regionCode );

    cfg.setSparkLocal( wSparkLocal.getSelection() );
    cfg.setSparkMaster( wSparkMaster.getText() );
    cfg.setSparkDeployFolder( wSparkDeployFolder.getText() );
    cfg.setSparkBatchIntervalMillis( wSparkBatchIntervalMillis.getText() );
    cfg.setSparkCheckpointDir( wSparkCheckpointDir.getText() );
    cfg.setSparkCheckpointDurationMillis( wSparkCheckpointDurationMillis.getText() );
    cfg.setSparkEnableSparkMetricSinks( wsparkEnableSparkMetricSinks.getSelection() );
    cfg.setSparkMaxRecordsPerBatch( wSparkMaxRecordsPerBatch.getText() );
    cfg.setSparkMinReadTimeMillis( wSparkMinReadTimeMillis.getText() );
    cfg.setSparkReadTimePercentage( wSparkReadTimePercentage.getText() );
    cfg.setSparkBundleSize( wSparkBundleSize.getText() );
    cfg.setSparkStorageLevel( wSparkStorageLevel.getText() );

    cfg.setFlinkLocal( wFlinkLocal.getSelection() );
    cfg.setFlinkMaster( wFlinkMaster.getText() );
    cfg.setFlinkParallelism( wFlinkParallelism.getText() );
    cfg.setFlinkCheckpointingInterval( wFlinkCheckpointingInterval.getText() );
    cfg.setFlinkCheckpointingMode( wFlinkCheckpointingMode.getText() );
    cfg.setFlinkCheckpointTimeoutMillis( wFlinkCheckpointTimeoutMillis.getText() );
    cfg.setFlinkMinPauseBetweenCheckpoints( wFlinkMinPauseBetweenCheckpoints.getText() );
    cfg.setFlinkNumberOfExecutionRetries( wFlinkNumberOfExecutionRetries.getText() );
    cfg.setFlinkExecutionRetryDelay( wFlinkExecutionRetryDelay.getText() );
    cfg.setFlinkObjectReuse( wFlinkObjectReuse.getText() );
    cfg.setFlinkStateBackend( wFlinkStateBackend.getText() );
    cfg.setFlinkDisableMetrics( wFlinkDisbaleMetrics.getText() );
    cfg.setFlinkExternalizedCheckpointsEnabled( wFlinkExternalizedCheckpointsEnabled.getText() );
    cfg.setFlinkRetainExternalizedCheckpointsOnCancellation( wFlinkRetainExternalizedCheckpointsOnCancellation.getText() );
    cfg.setFlinkMaxBundleSize( wFlinkMaxBundleSize.getText() );
    cfg.setFlinkMaxBundleTimeMills( wFlinkMaxBundleTimeMills.getText() );
    cfg.setFlinkShutdownSourcesOnFinalWatermark( wFlinkShutdownSourcesOnFinalWatermark.getText() ); ;
    cfg.setFlinkLatencyTrackingInterval( wFlinkLatencyTrackingInterval.getText() );
    cfg.setFlinkAutoWatermarkInterval( wFlinkAutoWatermarkInterval.getText() );
    cfg.setFlinkExecutionModeForBatch( wFlinkExecutionModeForBatch.getText() );

    cfg.getParameters().clear();
    for ( int i = 0; i < wParameters.nrNonEmpty(); i++ ) {
      TableItem item = wParameters.getNonEmpty( i );
      cfg.getParameters().add( new JobParameter( item.getText( 1 ), item.getText( 2 ) ) );
    }

  }
}
