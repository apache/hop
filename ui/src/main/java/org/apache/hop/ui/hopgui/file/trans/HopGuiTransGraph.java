//CHECKSTYLE:FileLength:OFF
/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.hopgui.file.trans;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.EngineMetaInterface;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.Props;
import org.apache.hop.core.SwtUniversalImage;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.dnd.DragAndDropContainer;
import org.apache.hop.core.dnd.XMLTransfer;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.AreaOwner.AreaType;
import org.apache.hop.core.gui.BasePainter;
import org.apache.hop.core.gui.GCInterface;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Redrawable;
import org.apache.hop.core.gui.SnapAllignDistribute;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiInterface;
import org.apache.hop.core.gui.plugin.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.GuiOSXKeyboardShortcut;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiToolbarElement;
import org.apache.hop.core.logging.DefaultLogLevel;
import org.apache.hop.core.logging.HasLogChannelInterface;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LogMessage;
import org.apache.hop.core.logging.LogParentProvidedInterface;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.Job;
import org.apache.hop.lineage.TransDataLineage;
import org.apache.hop.trans.DatabaseImpact;
import org.apache.hop.trans.ExecutionAdapter;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransExecutionConfiguration;
import org.apache.hop.trans.TransHopMeta;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.TransPainter;
import org.apache.hop.trans.debug.BreakPointListener;
import org.apache.hop.trans.debug.StepDebugMeta;
import org.apache.hop.trans.debug.TransDebugMeta;
import org.apache.hop.trans.debug.TransDebugMetaWrapper;
import org.apache.hop.trans.engine.IEngine;
import org.apache.hop.trans.step.RemoteStep;
import org.apache.hop.trans.step.RowDistributionInterface;
import org.apache.hop.trans.step.RowDistributionPluginType;
import org.apache.hop.trans.step.RowListener;
import org.apache.hop.trans.step.StepErrorMeta;
import org.apache.hop.trans.step.StepIOMetaInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaDataCombi;
import org.apache.hop.trans.step.errorhandling.Stream;
import org.apache.hop.trans.step.errorhandling.StreamIcon;
import org.apache.hop.trans.step.errorhandling.StreamInterface;
import org.apache.hop.trans.step.errorhandling.StreamInterface.StreamType;
import org.apache.hop.trans.steps.tableinput.TableInputMeta;
import org.apache.hop.ui.core.ConstUI;
import org.apache.hop.ui.core.PrintSpool;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.dialog.DialogClosedListener;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.dialog.StepFieldsDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.widget.CheckBoxToolTip;
import org.apache.hop.ui.core.widget.CheckBoxToolTipListener;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.GuiContextUtil;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopGuiAbstractGraph;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.hopui.HopUiExtenderPluginInterface;
import org.apache.hop.ui.hopui.HopUiExtenderPluginType;
import org.apache.hop.ui.hopui.SWTGC;
import org.apache.hop.ui.hopui.SwtScrollBar;
import org.apache.hop.ui.hopui.dialog.EnterPreviewRowsDialog;
import org.apache.hop.ui.hopui.dialog.NotePadDialog;
import org.apache.hop.ui.hopui.dialog.SearchFieldsProgressDialog;
import org.apache.hop.ui.hopui.job.JobGraph;
import org.apache.hop.ui.trans.dialog.TransDialog;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.MessageDialogWithToggle;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.window.DefaultToolTip;
import org.eclipse.jface.window.ToolTip;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DropTarget;
import org.eclipse.swt.dnd.DropTargetEvent;
import org.eclipse.swt.dnd.DropTargetListener;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.KeyListener;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseListener;
import org.eclipse.swt.events.MouseMoveListener;
import org.eclipse.swt.events.MouseTrackListener;
import org.eclipse.swt.events.MouseWheelListener;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.printing.Printer;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Widget;
import org.pentaho.ui.xul.components.XulToolbarbutton;

import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;

/**
 * This class handles the display of the transformations in a graphical way using icons, arrows, etc. One transformation
 * is handled per HopGuiTransGraph
 *
 * @author Matt
 * @since 17-mei-2003
 */
@GuiPlugin(
  id = "HopGuiTransGraph",
  description = "The transformation graph GUI plugin"
)
public class HopGuiTransGraph extends HopGuiAbstractGraph
  implements Redrawable, MouseListener, MouseMoveListener, MouseTrackListener, MouseWheelListener, KeyListener,
  HasLogChannelInterface, LogParentProvidedInterface,  // TODO: Aren't these the same?
  HopFileTypeHandlerInterface,
  GuiInterface {

  private static Class<?> PKG = HopUi.class; // for i18n purposes, needed by Translator2!!

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiTransGraph-Toolbar";
  public static final String TOOLBAR_ITEM_START = "HopGuiTransGraph-ToolBar-10010-Run";
  public static final String TOOLBAR_ITEM_STOP = "HopGuiTransGraph-ToolBar-10030-Stop";
  public static final String TOOLBAR_ITEM_PAUSE = "HopGuiTransGraph-ToolBar-10020-Pause";
  public static final String TOOLBAR_ITEM_PREVIEW = "HopGuiTransGraph-ToolBar-10040-Preview";
  public static final String TOOLBAR_ITEM_DEBUG = "HopGuiTransGraph-ToolBar-10045-Debug";

  public static final String TOOLBAR_ITEM_UNDO_ID = "HopGuiTransGraph-ToolBar-10100-Undo";
  public static final String TOOLBAR_ITEM_REDO_ID = "HopGuiTransGraph-ToolBar-10110-Redo";

  public static final String TOOLBAR_ITEM_ALIGN_LEFT = "HopGuiTransGraph-ToolBar-10200-Align-Left";
  public static final String TOOLBAR_ITEM_ALIGN_RIGHT = "HopGuiTransGraph-ToolBar-10210-Align-Right";
  public static final String TOOLBAR_ITEM_ALIGN_TOP = "HopGuiTransGraph-ToolBar-10250-Align-Ttop";
  public static final String TOOLBAR_ITEM_ALIGN_BOTTOM = "HopGuiTransGraph-ToolBar-10260-Align-Bottom";
  public static final String TOOLBAR_ITEM_DISTRIBUTE_HORIZONTALLY = "HopGuiTransGraph-ToolBar-10300-Distribute-Horizontally";
  public static final String TOOLBAR_ITEM_DISTRIBUTE_VERTICALLY = "HopGuiTransGraph-ToolBar-10310-Distribute-Vertically";

  public static final String TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS = "HopGuiTransGraph-ToolBar-10400-Execution-Results";

  public static final String LOAD_TAB = "loadTab";
  public static final String PREVIEW_TRANS = "previewTrans";

  private LogChannelInterface log;

  private static final int HOP_SEL_MARGIN = 9;

  public static final String START_TEXT = BaseMessages.getString( PKG, "TransLog.Button.StartTransformation" );

  public static final String PAUSE_TEXT = BaseMessages.getString( PKG, "TransLog.Button.PauseTransformation" );

  public static final String RESUME_TEXT = BaseMessages.getString( PKG, "TransLog.Button.ResumeTransformation" );

  public static final String STOP_TEXT = BaseMessages.getString( PKG, "TransLog.Button.StopTransformation" );

  public static final String TRANS_GRAPH_ENTRY_SNIFF = "trans-graph-entry-sniff";

  public static final String TRANS_GRAPH_ENTRY_AGAIN = "trans-graph-entry-align";

  private static final int TOOLTIP_HIDE_DELAY_SHORT = 5000;

  private static final int TOOLTIP_HIDE_DELAY_LONG = 10000;

  private TransMeta transMeta;
  public Trans trans;

  private Shell shell;

  private final HopDataOrchestrationPerspective perspective;

  private Composite mainComposite;

  private DefaultToolTip toolTip;

  private CheckBoxToolTip helpTip;

  private ToolBar toolBar;
  private GuiCompositeWidgets toolBarWidgets;

  private int iconsize;

  private Point lastclick;

  private Point lastMove;

  private Point[] previous_step_locations;

  private Point[] previous_note_locations;

  private List<StepMeta> selectedSteps;

  private StepMeta selectedStep;

  private List<NotePadMeta> selectedNotes;

  private NotePadMeta selectedNote;

  private TransHopMeta candidate;

  private Point drop_candidate;

  private HopGui hopUi;

  // public boolean shift, control;

  private boolean split_hop;

  private int lastButton;

  private TransHopMeta last_hop_split;

  private org.apache.hop.core.gui.Rectangle selectionRegion;

  /**
   * A list of remarks on the current Transformation...
   */
  private List<CheckResultInterface> remarks;

  /**
   * A list of impacts of the current transformation on the used databases.
   */
  private List<DatabaseImpact> impact;

  /**
   * Indicates whether or not an impact analysis has already run.
   */
  private boolean impactFinished;

  private TransDebugMeta lastTransDebugMeta;

  protected int currentMouseX = 0;

  protected int currentMouseY = 0;

  protected NotePadMeta ni = null;

  protected TransHopMeta currentHop;

  protected StepMeta currentStep;

  private List<AreaOwner> areaOwners;

  // private Text filenameLabel;
  private SashForm sashForm;

  public Composite extraViewComposite;

  public CTabFolder extraViewTabFolder;

  private boolean initialized;

  private boolean running;

  private boolean halted;

  private boolean halting;

  private boolean safeStopping;

  private boolean debug;

  private boolean pausing;

  public HopGuiTransLogDelegate transLogDelegate;
  public HopGuiTransGridDelegate transGridDelegate;
  public HopGuiTransPerfDelegate transPerfDelegate;
  public HopGuiTransMetricsDelegate transMetricsDelegate;
  public HopGuiTransPreviewDelegate transPreviewDelegate;
  public HopGuiTransRunDelegate transRunDelegate;
  public HopGuiTransStepDelegate transStepDelegate;
  public HopGuiTransClipboardDelegate transClipboardDelegate;
  public HopGuiTransHopDelegate transHopDelegate;
  public HopGuiTransUndoDelegate undoTransDelegate;

  public HopGuiSlaveDelegate slaveDelegate;
  public HopGuiNotePadDelegate notePadDelegate;

  public List<SelectedStepListener> stepListeners;
  public List<StepSelectionListener> currentStepListeners = new ArrayList<>();

  /**
   * A map that keeps track of which log line was written by which step
   */
  private Map<StepMeta, String> stepLogMap;

  private StepMeta startHopStep;
  private Point endHopLocation;
  private boolean startErrorHopStep;

  private StepMeta noInputStep;

  private StepMeta endHopStep;

  private StreamType candidateHopType;

  private Map<StepMeta, DelayTimer> delayTimers;

  private StepMeta showTargetStreamsStep;

  Timer redrawTimer;
  private ToolItem stopItem;
  private Combo zoomLabel;

  private HopTransFileType fileType;

  private String id;

  public void setCurrentNote( NotePadMeta ni ) {
    this.ni = ni;
  }

  public NotePadMeta getCurrentNote() {
    return ni;
  }

  public TransHopMeta getCurrentHop() {
    return currentHop;
  }

  public void setCurrentHop( TransHopMeta currentHop ) {
    this.currentHop = currentHop;
  }

  public StepMeta getCurrentStep() {
    return currentStep;
  }

  public void setCurrentStep( StepMeta currentStep ) {
    this.currentStep = currentStep;
  }

  public void addSelectedStepListener( SelectedStepListener selectedStepListener ) {
    stepListeners.add( selectedStepListener );
  }

  public void addCurrentStepListener( StepSelectionListener stepSelectionListener ) {
    currentStepListeners.add( stepSelectionListener );
  }

  public HopGuiTransGraph( Composite parent, final HopGui hopUi, final CTabItem parentTabItem,
                           final HopDataOrchestrationPerspective perspective, final TransMeta transMeta, final HopTransFileType fileType ) {
    super( parent, SWT.NONE, parentTabItem );
    this.shell = parent.getShell();
    this.hopUi = hopUi;
    this.parentTabItem = parentTabItem;
    this.perspective = perspective;
    this.transMeta = transMeta;
    this.fileType = fileType;
    this.id = UUID.randomUUID().toString();
    this.areaOwners = new ArrayList<>();

    this.log = hopUi.getLog();

    this.delayTimers = new HashMap<>();

    transLogDelegate = new HopGuiTransLogDelegate( hopUi, this );
    transGridDelegate = new HopGuiTransGridDelegate( hopUi, this );
    transPerfDelegate = new HopGuiTransPerfDelegate( hopUi, this );
    transMetricsDelegate = new HopGuiTransMetricsDelegate( hopUi, this );
    transPreviewDelegate = new HopGuiTransPreviewDelegate( hopUi, this );
    transClipboardDelegate = new HopGuiTransClipboardDelegate( hopUi, this );
    transStepDelegate = new HopGuiTransStepDelegate( hopUi, this );
    transHopDelegate = new HopGuiTransHopDelegate( hopUi, this );
    undoTransDelegate = new HopGuiTransUndoDelegate( hopUi, this );
    transRunDelegate = new HopGuiTransRunDelegate( hopUi, this );

    slaveDelegate = new HopGuiSlaveDelegate( hopUi, this );
    notePadDelegate = new HopGuiNotePadDelegate( hopUi, this );

    stepListeners = new ArrayList<>();

    // This composite takes up all the space in the parent
    //
    FormData formData = new FormData();
    formData.left = new FormAttachment( 0, 0 );
    formData.top = new FormAttachment( 0, 0 );
    formData.right = new FormAttachment( 100, 0 );
    formData.bottom = new FormAttachment( 100, 0 );
    setLayoutData( formData );

    // The layout in the widget is done using a FormLayout
    //
    setLayout( new FormLayout() );

    // Add a tool-bar at the top of the tab
    // The form-data is set on the native widget automatically
    //
    addToolBar();

    // The main composite contains the graph view, but if needed also
    // a view with an extra tab containing log, etc.
    //
    mainComposite = new Composite( this, SWT.NONE );
    mainComposite.setBackground( GUIResource.getInstance().getColorOrange() );
    mainComposite.setLayout( new FormLayout() );
    FormData fdMainComposite = new FormData();
    fdMainComposite.left = new FormAttachment( 0, 0 );
    fdMainComposite.top = new FormAttachment( 0, toolBar.getBounds().height ); // Position below toolbar
    fdMainComposite.right = new FormAttachment( 100, 0 );
    fdMainComposite.bottom = new FormAttachment( 100, 0 );
    mainComposite.setLayoutData( fdMainComposite );


    // To allow for a splitter later on, we will add the splitter here...
    //
    sashForm = new SashForm( mainComposite, SWT.VERTICAL );
    FormData fdSashForm = new FormData();
    fdSashForm.left = new FormAttachment( 0, 0 );
    fdSashForm.top = new FormAttachment( 0, 0 );
    fdSashForm.right = new FormAttachment( 100, 0 );
    fdSashForm.bottom = new FormAttachment( 100, 0 );
    sashForm.setLayoutData( fdSashForm );

    // Add a canvas below it, use up all space initially
    //
    canvas = new Canvas( sashForm, SWT.V_SCROLL | SWT.H_SCROLL | SWT.NO_BACKGROUND | SWT.BORDER );
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment( 0, 0 );
    fdCanvas.top = new FormAttachment( 0, 0 );
    fdCanvas.right = new FormAttachment( 100, 0 );
    fdCanvas.bottom = new FormAttachment( 100, 0 );
    canvas.setLayoutData( fdCanvas );

    sashForm.setWeights( new int[] { 100, } );


    toolTip = new DefaultToolTip( canvas, ToolTip.NO_RECREATE, true );
    toolTip.setRespectMonitorBounds( true );
    toolTip.setRespectDisplayBounds( true );
    toolTip.setPopupDelay( 350 );
    toolTip.setHideDelay( TOOLTIP_HIDE_DELAY_SHORT );
    toolTip.setShift( new org.eclipse.swt.graphics.Point( ConstUI.TOOLTIP_OFFSET, ConstUI.TOOLTIP_OFFSET ) );

    helpTip = new CheckBoxToolTip( canvas );
    helpTip.addCheckBoxToolTipListener( new CheckBoxToolTipListener() {

      @Override
      public void checkBoxSelected( boolean enabled ) {
        hopUi.getProps().setShowingHelpToolTips( enabled );
      }
    } );

    iconsize = hopUi.getProps().getIconSize();

    clearSettings();

    remarks = new ArrayList<>();
    impact = new ArrayList<>();
    impactFinished = false;

    hori = canvas.getHorizontalBar();
    vert = canvas.getVerticalBar();

    hori.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        redraw();
      }
    } );
    vert.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        redraw();
      }
    } );
    hori.setThumb( 100 );
    vert.setThumb( 100 );

    hori.setVisible( true );
    vert.setVisible( true );

    setVisible( true );
    newProps();

    canvas.setBackground( GUIResource.getInstance().getColorBlueCustomGrid() );

    canvas.addPaintListener( new PaintListener() {
      @Override
      public void paintControl( PaintEvent e ) {
        // if ( !hopUi.isStopped() ) {
        HopGuiTransGraph.this.paintControl( e );
        // }
      }
    } );

    selectedSteps = null;
    lastclick = null;

    /*
     * Handle the mouse...
     */

    canvas.addMouseListener( this );
    canvas.addMouseMoveListener( this );
    canvas.addMouseTrackListener( this );
    canvas.addMouseWheelListener( this );
    canvas.addKeyListener( this );

    // Drag & Drop for steps
    Transfer[] ttypes = new Transfer[] { XMLTransfer.getInstance() };
    DropTarget ddTarget = new DropTarget( canvas, DND.DROP_MOVE );
    ddTarget.setTransfer( ttypes );
    ddTarget.addDropListener( new DropTargetListener() {
      @Override
      public void dragEnter( DropTargetEvent event ) {
        clearSettings();

        drop_candidate = PropsUI.calculateGridPosition( getRealPosition( canvas, event.x, event.y ) );

        redraw();
      }

      @Override
      public void dragLeave( DropTargetEvent event ) {
        drop_candidate = null;
        redraw();
      }

      @Override
      public void dragOperationChanged( DropTargetEvent event ) {
      }

      @Override
      public void dragOver( DropTargetEvent event ) {
        drop_candidate = PropsUI.calculateGridPosition( getRealPosition( canvas, event.x, event.y ) );

        redraw();
      }

      @Override
      public void drop( DropTargetEvent event ) {
        // no data to copy, indicate failure in event.detail
        if ( event.data == null ) {
          event.detail = DND.DROP_NONE;
          return;
        }

        // What's the real drop position?
        Point p = getRealPosition( canvas, event.x, event.y );

        //
        // We expect a Drag and Drop container... (encased in XML)
        try {
          DragAndDropContainer container = (DragAndDropContainer) event.data;

          StepMeta stepMeta = null;
          boolean newstep = false;

          switch ( container.getType() ) {
            // Put an existing one on the canvas.
            case DragAndDropContainer.TYPE_STEP:
              // Drop hidden step onto canvas....
              stepMeta = transMeta.findStep( container.getData() );
              if ( stepMeta != null ) {
                if ( stepMeta.isDrawn() || transMeta.isStepUsedInTransHops( stepMeta ) ) {
                  modalMessageDialog( getString( "TransGraph.Dialog.StepIsAlreadyOnCanvas.Title" ),
                    getString( "TransGraph.Dialog.StepIsAlreadyOnCanvas.Message" ), SWT.OK );
                  return;
                }
                // This step gets the drawn attribute and position set below.
              } else {
                // Unknown step dropped: ignore this to be safe!
                return;
              }
              break;

            // Create a new step
            case DragAndDropContainer.TYPE_BASE_STEP_TYPE:
              // Not an existing step: data refers to the type of step to create
              String id = container.getId();
              String name = container.getData();
              stepMeta = transStepDelegate.newStep( transMeta, id, name, name, false, true, p );
              if ( stepMeta != null ) {
                newstep = true;
              } else {
                return; // Cancelled pressed in dialog or unable to create step.
              }
              break;

            // Create a new TableInput step using the selected connection...
            case DragAndDropContainer.TYPE_DATABASE_CONNECTION:
              newstep = true;
              String connectionName = container.getData();
              TableInputMeta tii = new TableInputMeta();
              tii.setDatabaseMeta( transMeta.findDatabase( connectionName ) );
              PluginRegistry registry = PluginRegistry.getInstance();
              String stepID = registry.getPluginId( StepPluginType.class, tii );
              PluginInterface stepPlugin = registry.findPluginWithId( StepPluginType.class, stepID );
              String stepName = transMeta.getAlternativeStepname( stepPlugin.getName() );
              stepMeta = new StepMeta( stepID, stepName, tii );
              if ( transStepDelegate.editStep( transMeta, stepMeta ) != null ) {
                transMeta.addStep( stepMeta );
                redraw();
              } else {
                return;
              }
              break;

            // Drag hop on the canvas: create a new Hop...
            case DragAndDropContainer.TYPE_TRANS_HOP:
              newHop();
              return;

            default:
              // Nothing we can use: give an error!
              modalMessageDialog( getString( "TransGraph.Dialog.ItemCanNotBePlacedOnCanvas.Title" ),
                getString( "TransGraph.Dialog.ItemCanNotBePlacedOnCanvas.Message" ), SWT.OK );
              return;
          }

          transMeta.unselectAll();

          StepMeta before = null;
          if ( !newstep ) {
            before = (StepMeta) stepMeta.clone();
          }

          stepMeta.drawStep();
          stepMeta.setSelected( true );
          PropsUI.setLocation( stepMeta, p.x, p.y );

          if ( newstep ) {
            hopUi.undoDelegate.addUndoNew( transMeta, new StepMeta[] { stepMeta }, new int[] { transMeta.indexOfStep( stepMeta ) } );
          } else {
            hopUi.undoDelegate.addUndoChange( transMeta, new StepMeta[] { before }, new StepMeta[] { (StepMeta) stepMeta.clone() },
              new int[] { transMeta.indexOfStep( stepMeta ) } );
          }

          forceFocus();
          redraw();

          // See if we want to draw a tool tip explaining how to create new hops...
          //
          if ( newstep && transMeta.nrSteps() > 1 && transMeta.nrSteps() < 5 && hopUi.getProps().isShowingHelpToolTips() ) {
            showHelpTip( p.x, p.y, BaseMessages.getString( PKG, "TransGraph.HelpToolTip.CreatingHops.Title" ),
              BaseMessages.getString( PKG, "TransGraph.HelpToolTip.CreatingHops.Message" ) );
          }
        } catch ( Exception e ) {
          new ErrorDialog( shell, BaseMessages.getString( PKG, "TransGraph.Dialog.ErrorDroppingObject.Message" ),
            BaseMessages.getString( PKG, "TransGraph.Dialog.ErrorDroppingObject.Title" ), e );
        }
      }

      @Override
      public void dropAccept( DropTargetEvent event ) {
      }
    } );

    setBackground( GUIResource.getInstance().getColorBackground() );

    // Add a timer to set correct the state of the run/stop buttons every 2 seconds...
    //
    final Timer timer = new Timer( "HopGuiTransGraph.setControlStates Timer: " + getMeta().getName() );
    TimerTask timerTask = new TimerTask() {

      @Override
      public void run() {
        setControlStates();
      }
    };
    timer.schedule( timerTask, 2000, 1000 );

    // Make sure the timer stops when we close the tab...
    //
    addDisposeListener( new DisposeListener() {
      @Override
      public void widgetDisposed( DisposeEvent arg0 ) {
        timer.cancel();
      }
    } );

    // Add keyboard listeners from the main GUI and this class (toolbar etc) to the canvas. That's where the focus should be
    //
    hopUi.replaceKeyboardShortcutListeners( this );

    // Update menu, toolbar, force redraw canvas
    //
    updateGui();
  }


  @Override
  public void mouseDoubleClick( MouseEvent e ) {
    clearSettings();

    Point real = screen2real( e.x, e.y );

    // Hide the tooltip!
    hideToolTips();

    /** TODO: Add back in
     try {
     ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.TransGraphMouseDoubleClick.id,
     new HopGUiTransGraphExtension( this, e, real ) );
     } catch ( Exception ex ) {
     LogChannel.GENERAL.logError( "Error calling TransGraphMouseDoubleClick extension point", ex );
     }
     **/

    StepMeta stepMeta = transMeta.getStep( real.x, real.y, iconsize );
    if ( stepMeta != null ) {
      if ( e.button == 1 ) {
        editStep( stepMeta );
      } else {
        editDescription( stepMeta );
      }
    } else {
      // Check if point lies on one of the many hop-lines...
      TransHopMeta online = findHop( real.x, real.y );
      if ( online != null ) {
        editHop( online );
      } else {
        NotePadMeta ni = transMeta.getNote( real.x, real.y );
        if ( ni != null ) {
          selectedNote = null;
          editNote( ni );
        } else {
          // See if the double click was in one of the area's...
          //
          boolean hit = false;
          for ( AreaOwner areaOwner : areaOwners ) {
            if ( areaOwner.contains( real.x, real.y ) ) {
              if ( areaOwner.getParent() instanceof StepMeta
                && areaOwner.getOwner().equals( TransPainter.STRING_PARTITIONING_CURRENT_STEP ) ) {
                StepMeta step = (StepMeta) areaOwner.getParent();
                transStepDelegate.editStepPartitioning( transMeta, step );
                hit = true;
                break;
              }
            }
          }

          if ( !hit ) {
            settings( new HopGuiTransContext( transMeta, this, real ) );
          }

        }
      }
    }
  }

  @Override
  public void mouseDown( MouseEvent e ) {

    boolean alt = ( e.stateMask & SWT.ALT ) != 0;
    boolean control = ( e.stateMask & SWT.MOD1 ) != 0;
    boolean shift = ( e.stateMask & SWT.SHIFT ) != 0;

    lastButton = e.button;
    Point real = screen2real( e.x, e.y );
    lastclick = new Point( real.x, real.y );

    // Hide the tooltip!
    hideToolTips();

    // Set the pop-up menu
    if ( e.button == 3 ) {
      setMenu( real.x, real.y );
      return;
    }

    try {
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.TransGraphMouseDown.id, new HopGUiTransGraphExtension( this, e, real ) );
    } catch ( Exception ex ) {
      LogChannel.GENERAL.logError( "Error calling TransGraphMouseDown extension point", ex );
    }

    // A single left or middle click on one of the area owners...
    //
    if ( e.button == 1 || e.button == 2 ) {
      AreaOwner areaOwner = getVisibleAreaOwner( real.x, real.y );
      if ( areaOwner != null && areaOwner.getAreaType() != null ) {
        switch ( areaOwner.getAreaType() ) {
          case STEP_OUTPUT_HOP_ICON:
            // Click on the output icon means: start of drag
            // Action: We show the input icons on the other steps...
            //
            selectedStep = null;
            startHopStep = (StepMeta) areaOwner.getParent();
            candidateHopType = null;
            startErrorHopStep = false;
            break;

          case STEP_INPUT_HOP_ICON:
            // Click on the input icon means: start to a new hop
            // In this case, we set the end hop step...
            //
            selectedStep = null;
            startHopStep = null;
            endHopStep = (StepMeta) areaOwner.getParent();
            candidateHopType = null;
            startErrorHopStep = false;
            break;

          case HOP_ERROR_ICON:
            // Click on the error icon means: Edit error handling
            //
            StepMeta stepMeta = (StepMeta) areaOwner.getParent();
            transStepDelegate.editStepErrorHandling( transMeta, stepMeta );
            break;

          case STEP_TARGET_HOP_ICON_OPTION:
            // Below, see showStepTargetOptions()
            break;

          case STEP_EDIT_ICON:
            clearSettings();
            currentStep = (StepMeta) areaOwner.getParent();
            editStep();
            break;

          case STEP_INJECT_ICON:
            modalMessageDialog( getString( "TransGraph.StepInjectionSupported.Title" ),
              getString( "TransGraph.StepInjectionSupported.Tooltip" ), SWT.OK | SWT.ICON_INFORMATION );
            break;

          case STEP_MENU_ICON:
            clearSettings();
            stepMeta = (StepMeta) areaOwner.getParent();
            setMenu( stepMeta.getLocation().x, stepMeta.getLocation().y );
            break;

          case STEP_ICON:
            stepMeta = (StepMeta) areaOwner.getOwner();
            currentStep = stepMeta;

            for ( StepSelectionListener listener : currentStepListeners ) {
              listener.onUpdateSelection( currentStep );
            }

            if ( candidate != null ) {
              addCandidateAsHop( e.x, e.y );
            }
            // ALT-Click: edit error handling
            //
            if ( e.button == 1 && alt && stepMeta.supportsErrorHandling() ) {
              transStepDelegate.editStepErrorHandling( transMeta, stepMeta );
              return;
            } else if ( e.button == 2 || ( e.button == 1 && shift ) ) {
              // SHIFT CLICK is start of drag to create a new hop
              //
              startHopStep = stepMeta;
            } else {
              selectedSteps = transMeta.getSelectedSteps();
              selectedStep = stepMeta;
              //
              // When an icon is moved that is not selected, it gets
              // selected too late.
              // It is not captured here, but in the mouseMoveListener...
              //
              previous_step_locations = transMeta.getSelectedStepLocations();

              Point p = stepMeta.getLocation();
              iconoffset = new Point( real.x - p.x, real.y - p.y );
            }
            redraw();
            break;

          case NOTE:
            ni = (NotePadMeta) areaOwner.getOwner();
            selectedNotes = transMeta.getSelectedNotes();
            selectedNote = ni;
            Point loc = ni.getLocation();

            previous_note_locations = transMeta.getSelectedNoteLocations();

            noteoffset = new Point( real.x - loc.x, real.y - loc.y );

            redraw();
            break;

          case STEP_COPIES_TEXT:
            copies( (StepMeta) areaOwner.getOwner() );
            break;

          case STEP_DATA_SERVICE:
            editProperties( transMeta, hopUi, true, TransDialog.Tabs.EXTRA_TAB );
            break;
          default:
            break;
        }
      } else {
        // A hop? --> enable/disable
        //
        TransHopMeta hop = findHop( real.x, real.y );
        if ( hop != null ) {
          TransHopMeta before = (TransHopMeta) hop.clone();
          setHopEnabled( hop, !hop.isEnabled() );
          if ( hop.isEnabled() && transMeta.hasLoop( hop.getToStep() ) ) {
            setHopEnabled( hop, false );
            modalMessageDialog( getString( "TransGraph.Dialog.HopCausesLoop.Title" ),
              getString( "TransGraph.Dialog.HopCausesLoop.Message" ), SWT.OK | SWT.ICON_ERROR );
          }
          TransHopMeta after = (TransHopMeta) hop.clone();
          hopUi.undoDelegate.addUndoChange( transMeta, new TransHopMeta[] { before }, new TransHopMeta[] { after }, new int[] { transMeta.indexOfTransHop( hop ) } );

          redraw();
          hopUi.setShellText();
        } else {
          // No area-owner & no hop means : background click:
          //
          startHopStep = null;
          if ( !control ) {
            selectionRegion = new org.apache.hop.core.gui.Rectangle( real.x, real.y, 0, 0 );
          }
          redraw();
        }
      }
    }
  }

  private enum SingleClickType {
    Trans,
    Step,
    Note
  }

  @Override
  public void mouseUp( MouseEvent e ) {
    try {
      HopGUiTransGraphExtension ext = new HopGUiTransGraphExtension( null, e, getArea() );
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.TransGraphMouseUp.id, ext );
      if ( ext.isPreventDefault() ) {
        redraw();
        clearSettings();
        return;
      }
    } catch ( Exception ex ) {
      LogChannel.GENERAL.logError( "Error calling TransGraphMouseUp extension point", ex );
    }

    boolean control = ( e.stateMask & SWT.MOD1 ) != 0;
    TransHopMeta selectedHop = findHop( e.x, e.y );
    updateErrorMetaForHop( selectedHop );
    boolean singleClick = false;
    SingleClickType singleClickType = null;
    StepMeta singleClickStep = null;
    NotePadMeta singleClickNote = null;

    if ( iconoffset == null ) {
      iconoffset = new Point( 0, 0 );
    }
    Point real = screen2real( e.x, e.y );
    Point icon = new Point( real.x - iconoffset.x, real.y - iconoffset.y );
    AreaOwner areaOwner = getVisibleAreaOwner( real.x, real.y );

    try {
      HopGUiTransGraphExtension ext = new HopGUiTransGraphExtension( this, e, real );
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.TransGraphMouseUp.id, ext );
      if ( ext.isPreventDefault() ) {
        redraw();
        clearSettings();
        return;
      }
    } catch ( Exception ex ) {
      LogChannel.GENERAL.logError( "Error calling TransGraphMouseUp extension point", ex );
    }

    // Quick new hop option? (drag from one step to another)
    //
    if ( candidate != null && areaOwner != null && areaOwner.getAreaType() != null ) {
      switch ( areaOwner.getAreaType() ) {
        case STEP_ICON:
          currentStep = (StepMeta) areaOwner.getOwner();
          break;
        case STEP_INPUT_HOP_ICON:
          currentStep = (StepMeta) areaOwner.getParent();
          break;
        default:
          break;
      }
      addCandidateAsHop( e.x, e.y );
      redraw();
    } else {
      // Did we select a region on the screen? Mark steps in region as
      // selected
      //
      if ( selectionRegion != null ) {
        selectionRegion.width = real.x - selectionRegion.x;
        selectionRegion.height = real.y - selectionRegion.y;
        if ( selectionRegion.width == 0 && selectionRegion.height == 0 ) {
          singleClick = true;
          singleClickType = SingleClickType.Trans;
        }
        transMeta.unselectAll();
        selectInRect( transMeta, selectionRegion );
        selectionRegion = null;
        redraw();
      } else {
        // Clicked on an icon?
        //
        if ( selectedStep != null && startHopStep == null ) {
          if ( e.button == 1 ) {
            Point realclick = screen2real( e.x, e.y );
            if ( lastclick.x == realclick.x && lastclick.y == realclick.y ) {
              // Flip selection when control is pressed!
              if ( control ) {
                selectedStep.flipSelected();
              } else {
                singleClick = true;
                singleClickType = SingleClickType.Step;
                singleClickStep = selectedStep;
              }
            } else {
              // Find out which Steps & Notes are selected
              selectedSteps = transMeta.getSelectedSteps();
              selectedNotes = transMeta.getSelectedNotes();

              // We moved around some items: store undo info...
              //
              boolean also = false;
              if ( selectedNotes != null && selectedNotes.size() > 0 && previous_note_locations != null ) {
                int[] indexes = transMeta.getNoteIndexes( selectedNotes );

                also = selectedSteps != null && selectedSteps.size() > 0;
                hopUi.undoDelegate.addUndoPosition( transMeta, selectedNotes.toArray( new NotePadMeta[ selectedNotes.size() ] ),
                  indexes, previous_note_locations, transMeta.getSelectedNoteLocations(), also );
              }
              if ( selectedSteps != null && previous_step_locations != null ) {
                int[] indexes = transMeta.getStepIndexes( selectedSteps );
                hopUi.undoDelegate.addUndoPosition( transMeta, selectedSteps.toArray( new StepMeta[ selectedSteps.size() ] ), indexes,
                  previous_step_locations, transMeta.getSelectedStepLocations(), also );
              }
            }
          }

          // OK, we moved the step, did we move it across a hop?
          // If so, ask to split the hop!
          if ( split_hop ) {
            TransHopMeta hi = findHop( icon.x + iconsize / 2, icon.y + iconsize / 2, selectedStep );
            if ( hi != null ) {
              splitHop( hi );
            }
            split_hop = false;
          }

          selectedSteps = null;
          selectedNotes = null;
          selectedStep = null;
          selectedNote = null;
          startHopStep = null;
          endHopLocation = null;
          redraw();
          hopUi.setShellText();
        } else {
          // Notes?
          //

          if ( selectedNote != null ) {
            if ( e.button == 1 ) {
              if ( lastclick.x == real.x && lastclick.y == real.y ) {
                // Flip selection when control is pressed!
                if ( control ) {
                  selectedNote.flipSelected();
                } else {
                  // single click on a note: ask what needs to happen...
                  //
                  singleClick = true;
                  singleClickType = SingleClickType.Note;
                  singleClickNote = selectedNote;
                }
              } else {
                // Find out which Steps & Notes are selected
                selectedSteps = transMeta.getSelectedSteps();
                selectedNotes = transMeta.getSelectedNotes();

                // We moved around some items: store undo info...

                boolean also = false;
                if ( selectedNotes != null && selectedNotes.size() > 0 && previous_note_locations != null ) {
                  int[] indexes = transMeta.getNoteIndexes( selectedNotes );
                  hopUi.undoDelegate.addUndoPosition( transMeta, selectedNotes.toArray( new NotePadMeta[ selectedNotes.size() ] ), indexes,
                    previous_note_locations, transMeta.getSelectedNoteLocations(), also );
                  also = selectedSteps != null && selectedSteps.size() > 0;
                }
                if ( selectedSteps != null && selectedSteps.size() > 0 && previous_step_locations != null ) {
                  int[] indexes = transMeta.getStepIndexes( selectedSteps );
                  hopUi.undoDelegate.addUndoPosition( transMeta, selectedSteps.toArray( new StepMeta[ selectedSteps.size() ] ), indexes,
                    previous_step_locations, transMeta.getSelectedStepLocations(), also );
                }
              }
            }

            selectedNotes = null;
            selectedSteps = null;
            selectedStep = null;
            selectedNote = null;
            startHopStep = null;
            endHopLocation = null;
          }
        }
      }
    }

    // Just a single click on the background:
    // We have a bunch of possible actions for you...
    //
    if ( singleClick && singleClickType != null ) {
      IGuiContextHandler contextHandler = null;
      String message = null;
      switch ( singleClickType ) {
        case Trans:
          message = "Select the action to execute or the step to create:";
          contextHandler = new HopGuiTransContext( transMeta, this, real );
          break;
        case Step:
          message = "Select the action to take on step '"+singleClickStep.getName()+"':";
          contextHandler = new HopGuiTransStepContext( transMeta, singleClickStep, this, real ); break;
        case Note:
          message = "Select the note action to take:";
          contextHandler = new HopGuiTransNoteContext( transMeta, singleClickNote, this, real ); break;
        default:
          break;
      }
      if ( contextHandler != null ) {
        GuiContextUtil.handleActionSelection( shell, message, new Point(e.x, e.y), contextHandler.getSupportedActions() );
      }
    }

    lastButton = 0;
  }

  private void splitHop( TransHopMeta hi ) {
    int id = 0;
    if ( !hopUi.getProps().getAutoSplit() ) {
      MessageDialogWithToggle md =
        new MessageDialogWithToggle( shell, BaseMessages.getString( PKG, "TransGraph.Dialog.SplitHop.Title" ), null,
          BaseMessages.getString( PKG, "TransGraph.Dialog.SplitHop.Message" ) + Const.CR + hi.toString(),
          MessageDialog.QUESTION, new String[] { BaseMessages.getString( PKG, "System.Button.Yes" ),
          BaseMessages.getString( PKG, "System.Button.No" ) }, 0, BaseMessages.getString( PKG,
          "TransGraph.Dialog.Option.SplitHop.DoNotAskAgain" ), hopUi.getProps().getAutoSplit() );
      MessageDialogWithToggle.setDefaultImage( GUIResource.getInstance().getImageHopUi() );
      id = md.open();
      hopUi.getProps().setAutoSplit( md.getToggleState() );
    }

    if ( ( id & 0xFF ) == 0 ) { // Means: "Yes" button clicked!

      // Only split A-->--B by putting C in between IF...
      // C-->--A or B-->--C don't exists...
      // A ==> hi.getFromStep()
      // B ==> hi.getToStep();
      // C ==> selected_step
      //
      boolean caExists = transMeta.findTransHop( selectedStep, hi.getFromStep() ) != null;
      boolean bcExists = transMeta.findTransHop( hi.getToStep(), selectedStep ) != null;
      if ( !caExists && !bcExists ) {

        StepMeta fromStep = hi.getFromStep();
        StepMeta toStep = hi.getToStep();

        // In case step A targets B then we now need to target C
        //
        StepIOMetaInterface fromIo = fromStep.getStepMetaInterface().getStepIOMeta();
        for ( StreamInterface stream : fromIo.getTargetStreams() ) {
          if ( stream.getStepMeta() != null && stream.getStepMeta().equals( toStep ) ) {
            // This target stream was directed to B, now we need to direct it to C
            stream.setStepMeta( selectedStep );
            fromStep.getStepMetaInterface().handleStreamSelection( stream );
          }
        }

        // In case step B sources from A then we now need to source from C
        //
        StepIOMetaInterface toIo = toStep.getStepMetaInterface().getStepIOMeta();
        for ( StreamInterface stream : toIo.getInfoStreams() ) {
          if ( stream.getStepMeta() != null && stream.getStepMeta().equals( fromStep ) ) {
            // This info stream was reading from B, now we need to direct it to C
            stream.setStepMeta( selectedStep );
            toStep.getStepMetaInterface().handleStreamSelection( stream );
          }
        }

        // In case there is error handling on A, we want to make it point to C now
        //
        StepErrorMeta errorMeta = fromStep.getStepErrorMeta();
        if ( fromStep.isDoingErrorHandling() && toStep.equals( errorMeta.getTargetStep() ) ) {
          errorMeta.setTargetStep( selectedStep );
        }

        TransHopMeta newhop1 = new TransHopMeta( hi.getFromStep(), selectedStep );
        if ( transMeta.findTransHop( newhop1 ) == null ) {
          transMeta.addTransHop( newhop1 );
          hopUi.undoDelegate.addUndoNew( transMeta, new TransHopMeta[] { newhop1, }, new int[] { transMeta.indexOfTransHop( newhop1 ), }, true );
        }
        TransHopMeta newhop2 = new TransHopMeta( selectedStep, hi.getToStep() );
        if ( transMeta.findTransHop( newhop2 ) == null ) {
          transMeta.addTransHop( newhop2 );
          hopUi.undoDelegate.addUndoNew( transMeta, new TransHopMeta[] { newhop2 }, new int[] { transMeta.indexOfTransHop( newhop2 ) }, true );
        }
        int idx = transMeta.indexOfTransHop( hi );

        hopUi.undoDelegate.addUndoDelete( transMeta, new TransHopMeta[] { hi }, new int[] { idx }, true );
        transMeta.removeTransHop( idx );

        redraw();
      }

      // else: Silently discard this hop-split attempt.
    }
  }

  @Override
  public void mouseMove( MouseEvent e ) {
    boolean shift = ( e.stateMask & SWT.SHIFT ) != 0;
    noInputStep = null;

    // disable the tooltip
    //
    toolTip.hide();
    toolTip.setHideDelay( TOOLTIP_HIDE_DELAY_SHORT );

    Point real = screen2real( e.x, e.y );

    currentMouseX = real.x;
    currentMouseY = real.y;

    // Remember the last position of the mouse for paste with keyboard
    //
    lastMove = real;

    if ( iconoffset == null ) {
      iconoffset = new Point( 0, 0 );
    }
    Point icon = new Point( real.x - iconoffset.x, real.y - iconoffset.y );

    if ( noteoffset == null ) {
      noteoffset = new Point( 0, 0 );
    }
    Point note = new Point( real.x - noteoffset.x, real.y - noteoffset.y );

    // Moved over an area?
    //
    AreaOwner areaOwner = getVisibleAreaOwner( real.x, real.y );
    if ( areaOwner != null && areaOwner.getAreaType() != null ) {
      switch ( areaOwner.getAreaType() ) {
        case STEP_ICON:
          StepMeta stepMeta = (StepMeta) areaOwner.getOwner();
          resetDelayTimer( stepMeta );
          break;

        case MINI_ICONS_BALLOON: // Give the timer a bit more time
          stepMeta = (StepMeta) areaOwner.getParent();
          resetDelayTimer( stepMeta );
          break;

        default:
          break;
      }
    }

    try {
      HopGUiTransGraphExtension ext = new HopGUiTransGraphExtension( this, e, real );
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.TransGraphMouseMoved.id, ext );
    } catch ( Exception ex ) {
      LogChannel.GENERAL.logError( "Error calling TransGraphMouseMoved extension point", ex );
    }

    //
    // First see if the icon we clicked on was selected.
    // If the icon was not selected, we should un-select all other
    // icons, selected and move only the one icon
    //
    if ( selectedStep != null && !selectedStep.isSelected() ) {
      transMeta.unselectAll();
      selectedStep.setSelected( true );
      selectedSteps = new ArrayList<>();
      selectedSteps.add( selectedStep );
      previous_step_locations = new Point[] { selectedStep.getLocation() };
      redraw();
    } else if ( selectedNote != null && !selectedNote.isSelected() ) {
      transMeta.unselectAll();
      selectedNote.setSelected( true );
      selectedNotes = new ArrayList<>();
      selectedNotes.add( selectedNote );
      previous_note_locations = new Point[] { selectedNote.getLocation() };
      redraw();
    } else if ( selectionRegion != null && startHopStep == null ) {
      // Did we select a region...?
      //

      selectionRegion.width = real.x - selectionRegion.x;
      selectionRegion.height = real.y - selectionRegion.y;
      redraw();
    } else if ( selectedStep != null && lastButton == 1 && !shift && startHopStep == null ) {
      //
      // One or more icons are selected and moved around...
      //
      // new : new position of the ICON (not the mouse pointer) dx : difference with previous position
      //
      int dx = icon.x - selectedStep.getLocation().x;
      int dy = icon.y - selectedStep.getLocation().y;

      // See if we have a hop-split candidate
      //
      TransHopMeta hi = findHop( icon.x + iconsize / 2, icon.y + iconsize / 2, selectedStep );
      if ( hi != null ) {
        // OK, we want to split the hop in 2
        //
        if ( !hi.getFromStep().equals( selectedStep ) && !hi.getToStep().equals( selectedStep ) ) {
          split_hop = true;
          last_hop_split = hi;
          hi.split = true;
        }
      } else {
        if ( last_hop_split != null ) {
          last_hop_split.split = false;
          last_hop_split = null;
          split_hop = false;
        }
      }

      selectedNotes = transMeta.getSelectedNotes();
      selectedSteps = transMeta.getSelectedSteps();

      // Adjust location of selected steps...
      if ( selectedSteps != null ) {
        for ( int i = 0; i < selectedSteps.size(); i++ ) {
          StepMeta stepMeta = selectedSteps.get( i );
          PropsUI.setLocation( stepMeta, stepMeta.getLocation().x + dx, stepMeta.getLocation().y + dy );
        }
      }
      // Adjust location of selected hops...
      if ( selectedNotes != null ) {
        for ( int i = 0; i < selectedNotes.size(); i++ ) {
          NotePadMeta ni = selectedNotes.get( i );
          PropsUI.setLocation( ni, ni.getLocation().x + dx, ni.getLocation().y + dy );
        }
      }

      redraw();
    } else if ( ( startHopStep != null && endHopStep == null ) || ( endHopStep != null && startHopStep == null ) ) {
      // Are we creating a new hop with the middle button or pressing SHIFT?
      //

      StepMeta stepMeta = transMeta.getStep( real.x, real.y, iconsize );
      endHopLocation = new Point( real.x, real.y );
      if ( stepMeta != null
        && ( ( startHopStep != null && !startHopStep.equals( stepMeta ) ) || ( endHopStep != null && !endHopStep
        .equals( stepMeta ) ) ) ) {
        StepIOMetaInterface ioMeta = stepMeta.getStepMetaInterface().getStepIOMeta();
        if ( candidate == null ) {
          // See if the step accepts input. If not, we can't create a new hop...
          //
          if ( startHopStep != null ) {
            if ( ioMeta.isInputAcceptor() ) {
              candidate = new TransHopMeta( startHopStep, stepMeta );
              endHopLocation = null;
            } else {
              noInputStep = stepMeta;
              toolTip.setImage( null );
              toolTip.setText( "This step does not accept any input from other steps" );
              toolTip.show( new org.eclipse.swt.graphics.Point( real.x, real.y ) );
            }
          } else if ( endHopStep != null ) {
            if ( ioMeta.isOutputProducer() ) {
              candidate = new TransHopMeta( stepMeta, endHopStep );
              endHopLocation = null;
            } else {
              noInputStep = stepMeta;
              toolTip.setImage( null );
              toolTip
                .setText( "This step doesn't pass any output to other steps. (except perhaps for targetted output)" );
              toolTip.show( new org.eclipse.swt.graphics.Point( real.x, real.y ) );
            }
          }
        }
      } else {
        if ( candidate != null ) {
          candidate = null;
          redraw();
        }
      }

      redraw();
    }

    // Move around notes & steps
    //
    if ( selectedNote != null ) {
      if ( lastButton == 1 && !shift ) {
        /*
         * One or more notes are selected and moved around...
         *
         * new : new position of the note (not the mouse pointer) dx : difference with previous position
         */
        int dx = note.x - selectedNote.getLocation().x;
        int dy = note.y - selectedNote.getLocation().y;

        selectedNotes = transMeta.getSelectedNotes();
        selectedSteps = transMeta.getSelectedSteps();

        // Adjust location of selected steps...
        if ( selectedSteps != null ) {
          for ( int i = 0; i < selectedSteps.size(); i++ ) {
            StepMeta stepMeta = selectedSteps.get( i );
            PropsUI.setLocation( stepMeta, stepMeta.getLocation().x + dx, stepMeta.getLocation().y + dy );
          }
        }
        // Adjust location of selected hops...
        if ( selectedNotes != null ) {
          for ( int i = 0; i < selectedNotes.size(); i++ ) {
            NotePadMeta ni = selectedNotes.get( i );
            PropsUI.setLocation( ni, ni.getLocation().x + dx, ni.getLocation().y + dy );
          }
        }

        redraw();
      }
    }
  }

  @Override
  public void mouseHover( MouseEvent e ) {

    boolean tip = true;
    boolean isDeprecated = false;

    toolTip.hide();
    toolTip.setHideDelay( TOOLTIP_HIDE_DELAY_SHORT );
    Point real = screen2real( e.x, e.y );

    AreaOwner areaOwner = getVisibleAreaOwner( real.x, real.y );
    if ( areaOwner != null && areaOwner.getAreaType() != null ) {
      switch ( areaOwner.getAreaType() ) {
        default:
          break;
      }
    }

    // Show a tool tip upon mouse-over of an object on the canvas
    if ( ( tip && !helpTip.isVisible() ) || isDeprecated ) {
      setToolTip( real.x, real.y, e.x, e.y );
    }
  }

  @Override
  public void mouseScrolled( MouseEvent e ) {
    /*
     * if (e.count == 3) { // scroll up zoomIn(); } else if (e.count == -3) { // scroll down zoomOut(); } }
     */
  }

  private void addCandidateAsHop( int mouseX, int mouseY ) {

    boolean forward = startHopStep != null;

    StepMeta fromStep = candidate.getFromStep();
    StepMeta toStep = candidate.getToStep();

    // See what the options are.
    // - Does the source step has multiple stream options?
    // - Does the target step have multiple input stream options?
    //
    List<StreamInterface> streams = new ArrayList<>();

    StepIOMetaInterface fromIoMeta = fromStep.getStepMetaInterface().getStepIOMeta();
    List<StreamInterface> targetStreams = fromIoMeta.getTargetStreams();
    if ( forward ) {
      streams.addAll( targetStreams );
    }

    StepIOMetaInterface toIoMeta = toStep.getStepMetaInterface().getStepIOMeta();
    List<StreamInterface> infoStreams = toIoMeta.getInfoStreams();
    if ( !forward ) {
      streams.addAll( infoStreams );
    }

    if ( forward ) {
      if ( fromIoMeta.isOutputProducer() && toStep.equals( currentStep ) ) {
        streams.add( new Stream( StreamType.OUTPUT, fromStep, BaseMessages
          .getString( PKG, "Spoon.Hop.MainOutputOfStep" ), StreamIcon.OUTPUT, null ) );
      }

      if ( fromStep.supportsErrorHandling() && toStep.equals( currentStep ) ) {
        streams.add( new Stream( StreamType.ERROR, fromStep, BaseMessages.getString( PKG,
          "Spoon.Hop.ErrorHandlingOfStep" ), StreamIcon.ERROR, null ) );
      }
    } else {
      if ( toIoMeta.isInputAcceptor() && fromStep.equals( currentStep ) ) {
        streams.add( new Stream( StreamType.INPUT, toStep, BaseMessages.getString( PKG, "Spoon.Hop.MainInputOfStep" ),
          StreamIcon.INPUT, null ) );
      }

      if ( fromStep.supportsErrorHandling() && fromStep.equals( currentStep ) ) {
        streams.add( new Stream( StreamType.ERROR, fromStep, BaseMessages.getString( PKG,
          "Spoon.Hop.ErrorHandlingOfStep" ), StreamIcon.ERROR, null ) );
      }
    }

    // Targets can be dynamically added to this step...
    //
    if ( forward ) {
      streams.addAll( fromStep.getStepMetaInterface().getOptionalStreams() );
    } else {
      streams.addAll( toStep.getStepMetaInterface().getOptionalStreams() );
    }

    // Show a list of options on the canvas...
    //
    if ( streams.size() > 1 ) {
      // Show a pop-up menu with all the possible options...
      //
      Menu menu = new Menu( canvas );
      for ( final StreamInterface stream : streams ) {
        MenuItem item = new MenuItem( menu, SWT.NONE );
        item.setText( Const.NVL( stream.getDescription(), "" ) );
        item.setImage( getImageFor( stream ) );
        item.addSelectionListener( new SelectionAdapter() {
          @Override
          public void widgetSelected( SelectionEvent e ) {
            addHop( stream );
          }
        } );
      }
      menu.setLocation( canvas.toDisplay( mouseX, mouseY ) );
      menu.setVisible( true );

      return;
    }
    if ( streams.size() == 1 ) {
      addHop( streams.get( 0 ) );
    } else {
      return;
    }

    /*
     *
     * if (transMeta.findTransHop(candidate) == null) { spoon.newHop(transMeta, candidate); } if (startErrorHopStep) {
     * addErrorHop(); } if (startTargetHopStream != null) { // Auto-configure the target in the source step... //
     * startTargetHopStream.setStepMeta(candidate.getToStep());
     * startTargetHopStream.setStepname(candidate.getToStep().getName()); startTargetHopStream = null; }
     */
    candidate = null;
    selectedSteps = null;
    startHopStep = null;
    endHopLocation = null;
    startErrorHopStep = false;

    // redraw();
  }

  private Image getImageFor( StreamInterface stream ) {
    Display disp = shell.getDisplay();
    SwtUniversalImage swtImage = SWTGC.getNativeImage( BasePainter.getStreamIconImage( stream.getStreamIcon() ) );
    return swtImage.getAsBitmapForSize( disp, ConstUI.SMALL_ICON_SIZE, ConstUI.SMALL_ICON_SIZE );
  }

  protected void addHop( StreamInterface stream ) {
    switch ( stream.getStreamType() ) {
      case ERROR:
        addErrorHop();
        candidate.setErrorHop( true );
        transHopDelegate.newHop( transMeta, candidate );
        break;
      case INPUT:
        transHopDelegate.newHop( transMeta, candidate );
        break;
      case OUTPUT:
        StepErrorMeta stepErrorMeta = candidate.getFromStep().getStepErrorMeta();
        if ( stepErrorMeta != null && stepErrorMeta.getTargetStep() != null ) {
          if ( stepErrorMeta.getTargetStep().equals( candidate.getToStep() ) ) {
            candidate.getFromStep().setStepErrorMeta( null );
          }
        }
        transHopDelegate.newHop( transMeta, candidate );
        break;
      case INFO:
        stream.setStepMeta( candidate.getFromStep() );
        candidate.getToStep().getStepMetaInterface().handleStreamSelection( stream );
        transHopDelegate.newHop( transMeta, candidate );
        break;
      case TARGET:
        // We connect a target of the source step to an output step...
        //
        stream.setStepMeta( candidate.getToStep() );
        candidate.getFromStep().getStepMetaInterface().handleStreamSelection( stream );
        transHopDelegate.newHop( transMeta, candidate );
        break;
      default:
        break;

    }
    clearSettings();
  }

  private void addErrorHop() {
    // Automatically configure the step error handling too!
    //
    if ( candidate == null || candidate.getFromStep() == null ) {
      return;
    }
    StepErrorMeta errorMeta = candidate.getFromStep().getStepErrorMeta();
    if ( errorMeta == null ) {
      errorMeta = new StepErrorMeta( transMeta, candidate.getFromStep() );
    }
    errorMeta.setEnabled( true );
    errorMeta.setTargetStep( candidate.getToStep() );
    candidate.getFromStep().setStepErrorMeta( errorMeta );
  }

  private void resetDelayTimer( StepMeta stepMeta ) {
    DelayTimer delayTimer = delayTimers.get( stepMeta );
    if ( delayTimer != null ) {
      delayTimer.reset();
    }
  }

  @Override
  public void mouseEnter( MouseEvent arg0 ) {
  }

  @Override
  public void mouseExit( MouseEvent arg0 ) {
  }


  protected void asyncRedraw() {
    hopUi.getDisplay().asyncExec( new Runnable() {
      @Override
      public void run() {
        if ( !HopGuiTransGraph.this.isDisposed() ) {
          HopGuiTransGraph.this.redraw();
        }
      }
    } );
  }

  private void addToolBar() {

    try {
      // Create a new toolbar at the top of the main composite...
      //
      toolBar = new ToolBar( this, SWT.BORDER | SWT.WRAP | SWT.SHADOW_OUT | SWT.LEFT | SWT.HORIZONTAL );
      toolBarWidgets = new GuiCompositeWidgets( HopGui.getInstance().getVariableSpace() );
      toolBarWidgets.createCompositeWidgets( this, null, toolBar, GUI_PLUGIN_TOOLBAR_PARENT_ID, null );
      toolBar.pack();

      // Add a zoom label widget: TODO: move to GuiElement
      //
      new ToolItem( toolBar, SWT.SEPARATOR );
      ToolItem sep = new ToolItem( toolBar, SWT.SEPARATOR );

      zoomLabel = new Combo( toolBar, SWT.DROP_DOWN );
      zoomLabel.setItems( TransPainter.magnificationDescriptions );
      zoomLabel.addSelectionListener( new SelectionAdapter() {
        @Override
        public void widgetSelected( SelectionEvent arg0 ) {
          readMagnification();
        }
      } );

      zoomLabel.addKeyListener( new KeyAdapter() {
        @Override
        public void keyPressed( KeyEvent event ) {
          if ( event.character == SWT.CR ) {
            readMagnification();
          }
        }
      } );

      setZoomLabel();
      zoomLabel.pack();
      zoomLabel.layout( true, true );
      sep.setWidth( zoomLabel.getBounds().width );
      sep.setControl( zoomLabel );
      toolBar.pack();

      // enable / disable the icons in the toolbar too.
      //
      setControlStates();

    } catch ( Throwable t ) {
      log.logError( "Error setting up the navigation toolbar for HopUI", t );
      new ErrorDialog( shell, "Error", "Error setting up the navigation toolbar for HopGUI", new Exception( t ) );
    }
  }

  public void setZoomLabel() {
    zoomLabel.setText( Integer.toString( Math.round( magnification * 100 ) ) + "%" );
  }

  /**
   * Allows for magnifying to any percentage entered by the user...
   */
  private void readMagnification() {
    String possibleText = zoomLabel.getText();
    possibleText = possibleText.replace( "%", "" );

    float possibleFloatMagnification;
    try {
      possibleFloatMagnification = Float.parseFloat( possibleText ) / 100;
      magnification = possibleFloatMagnification;
      if ( zoomLabel.getText().indexOf( '%' ) < 0 ) {
        zoomLabel.setText( zoomLabel.getText().concat( "%" ) );
      }
    } catch ( Exception e ) {
      modalMessageDialog( getString( "TransGraph.Dialog.InvalidZoomMeasurement.Title" ),
        getString( "TransGraph.Dialog.InvalidZoomMeasurement.Message", zoomLabel.getText() ),
        SWT.YES | SWT.ICON_ERROR );
    }
    redraw();
  }

  protected void hideToolTips() {
    toolTip.hide();
    helpTip.hide();
    toolTip.setHideDelay( TOOLTIP_HIDE_DELAY_SHORT );
  }

  private void showHelpTip( int x, int y, String tipTitle, String tipMessage ) {

    helpTip.setTitle( tipTitle );
    helpTip.setMessage( tipMessage.replaceAll( "\n", Const.CR ) );
    helpTip
      .setCheckBoxMessage( BaseMessages.getString( PKG, "TransGraph.HelpToolTip.DoNotShowAnyMoreCheckBox.Message" ) );

    // helpTip.hide();
    // int iconSize = spoon.props.getIconSize();
    org.eclipse.swt.graphics.Point location = new org.eclipse.swt.graphics.Point( x - 5, y - 5 );

    helpTip.show( location );
  }

  /**
   * Select all the steps in a certain (screen) rectangle
   *
   * @param rect The selection area as a rectangle
   */
  public void selectInRect( TransMeta transMeta, org.apache.hop.core.gui.Rectangle rect ) {
    if ( rect.height < 0 || rect.width < 0 ) {
      org.apache.hop.core.gui.Rectangle rectified =
        new org.apache.hop.core.gui.Rectangle( rect.x, rect.y, rect.width, rect.height );

      // Only for people not dragging from left top to right bottom
      if ( rectified.height < 0 ) {
        rectified.y = rectified.y + rectified.height;
        rectified.height = -rectified.height;
      }
      if ( rectified.width < 0 ) {
        rectified.x = rectified.x + rectified.width;
        rectified.width = -rectified.width;
      }
      rect = rectified;
    }

    for ( int i = 0; i < transMeta.nrSteps(); i++ ) {
      StepMeta stepMeta = transMeta.getStep( i );
      Point a = stepMeta.getLocation();
      if ( rect.contains( a.x, a.y ) ) {
        stepMeta.setSelected( true );
      }
    }

    for ( int i = 0; i < transMeta.nrNotes(); i++ ) {
      NotePadMeta ni = transMeta.getNote( i );
      Point a = ni.getLocation();
      Point b = new Point( a.x + ni.width, a.y + ni.height );
      if ( rect.contains( a.x, a.y ) && rect.contains( b.x, b.y ) ) {
        ni.setSelected( true );
      }
    }
  }

  @Override
  public void keyPressed( KeyEvent e ) {

    // CTRL-UP : allignTop();
    if ( e.keyCode == SWT.ARROW_UP && ( e.stateMask & SWT.MOD1 ) != 0 ) {
      alignTop();
    }
    // CTRL-DOWN : allignBottom();
    if ( e.keyCode == SWT.ARROW_DOWN && ( e.stateMask & SWT.MOD1 ) != 0 ) {
      alignBottom();
    }
    // CTRL-LEFT : allignleft();
    if ( e.keyCode == SWT.ARROW_LEFT && ( e.stateMask & SWT.MOD1 ) != 0 ) {
      alignLeft();
    }
    // CTRL-RIGHT : allignRight();
    if ( e.keyCode == SWT.ARROW_RIGHT && ( e.stateMask & SWT.MOD1 ) != 0 ) {
      alignRight();
    }
    // ALT-RIGHT : distributeHorizontal();
    if ( e.keyCode == SWT.ARROW_RIGHT && ( e.stateMask & SWT.ALT ) != 0 ) {
      distributeHorizontal();
    }
    // ALT-UP : distributeVertical();
    if ( e.keyCode == SWT.ARROW_UP && ( e.stateMask & SWT.ALT ) != 0 ) {
      distributeVertical();
    }
    // ALT-HOME : snap to grid
    if ( e.keyCode == SWT.HOME && ( e.stateMask & SWT.ALT ) != 0 ) {
      snaptogrid( ConstUI.GRID_SIZE );
    }

    if ( e.character == 'E' && ( e.stateMask & SWT.CTRL ) != 0 ) {
      checkErrorVisuals();
    }

    // SPACE : over a step: show output fields...
    if ( e.character == ' ' && lastMove != null ) {

      Point real = lastMove;

      // Hide the tooltip!
      hideToolTips();

      // Set the pop-up menu
      StepMeta stepMeta = transMeta.getStep( real.x, real.y, iconsize );
      if ( stepMeta != null ) {
        // OK, we found a step, show the output fields...
        inputOutputFields( stepMeta, false );
      }
    }
  }

  @Override
  public void keyReleased( KeyEvent e ) {
  }

  @Override
  public boolean setFocus() {
    return ( canvas != null && !canvas.isDisposed() ) ? canvas.setFocus() : false;
  }

  public void renameStep( StepMeta stepMeta, String stepname ) {
    String newname = stepname;

    StepMeta smeta = transMeta.findStep( newname, stepMeta );
    int nr = 2;
    while ( smeta != null ) {
      newname = stepname + " " + nr;
      smeta = transMeta.findStep( newname );
      nr++;
    }
    if ( nr > 2 ) {
      stepname = newname;
      modalMessageDialog( getString( "Spoon.Dialog.StepnameExists.Title" ),
        getString( "Spoon.Dialog.StepnameExists.Message", stepname ), SWT.OK | SWT.ICON_INFORMATION );
    }
    stepMeta.setName( stepname );
    stepMeta.setChanged();
    redraw();
  }

  public void clearSettings() {
    selectedStep = null;
    noInputStep = null;
    selectedNote = null;
    selectedSteps = null;
    selectionRegion = null;
    candidate = null;
    last_hop_split = null;
    lastButton = 0;
    iconoffset = null;
    startHopStep = null;
    endHopStep = null;
    endHopLocation = null;
    for ( int i = 0; i < transMeta.nrTransHops(); i++ ) {
      // CHECKSTYLE:Indentation:OFF
      transMeta.getTransHop( i ).split = false;
    }
  }

  public String[] getDropStrings( String str, String sep ) {
    StringTokenizer strtok = new StringTokenizer( str, sep );
    String[] retval = new String[ strtok.countTokens() ];
    int i = 0;
    while ( strtok.hasMoreElements() ) {
      retval[ i ] = strtok.nextToken();
      i++;
    }
    return retval;
  }

  public Point getRealPosition( Composite canvas, int x, int y ) {
    Point p = new Point( 0, 0 );
    Composite follow = canvas;
    while ( follow != null ) {
      org.eclipse.swt.graphics.Point loc = follow.getLocation();
      Point xy = new Point( loc.x, loc.y );
      p.x += xy.x;
      p.y += xy.y;
      follow = follow.getParent();
    }

    int offsetX = -16;
    int offsetY = -64;
    if ( Const.isOSX() ) {
      offsetX = -2;
      offsetY = -24;
    }
    p.x = x - p.x + offsetX;
    p.y = y - p.y + offsetY;

    return screen2real( p.x, p.y );
  }

  /**
   * See if location (x,y) is on a line between two steps: the hop!
   *
   * @param x
   * @param y
   * @return the transformation hop on the specified location, otherwise: null
   */
  protected TransHopMeta findHop( int x, int y ) {
    return findHop( x, y, null );
  }

  /**
   * See if location (x,y) is on a line between two steps: the hop!
   *
   * @param x
   * @param y
   * @param exclude the step to exclude from the hops (from or to location). Specify null if no step is to be excluded.
   * @return the transformation hop on the specified location, otherwise: null
   */
  private TransHopMeta findHop( int x, int y, StepMeta exclude ) {
    int i;
    TransHopMeta online = null;
    for ( i = 0; i < transMeta.nrTransHops(); i++ ) {
      TransHopMeta hi = transMeta.getTransHop( i );
      StepMeta fs = hi.getFromStep();
      StepMeta ts = hi.getToStep();

      if ( fs == null || ts == null ) {
        return null;
      }

      // If either the "from" or "to" step is excluded, skip this hop.
      //
      if ( exclude != null && ( exclude.equals( fs ) || exclude.equals( ts ) ) ) {
        continue;
      }

      int[] line = getLine( fs, ts );

      if ( pointOnLine( x, y, line ) ) {
        online = hi;
      }
    }
    return online;
  }

  private int[] getLine( StepMeta fs, StepMeta ts ) {
    Point from = fs.getLocation();
    Point to = ts.getLocation();
    offset = getOffset();

    int x1 = from.x + iconsize / 2;
    int y1 = from.y + iconsize / 2;

    int x2 = to.x + iconsize / 2;
    int y2 = to.y + iconsize / 2;

    return new int[] { x1, y1, x2, y2 };
  }

  public void hideStep() {
    for ( int i = 0; i < transMeta.nrSteps(); i++ ) {
      StepMeta sti = transMeta.getStep( i );
      if ( sti.isDrawn() && sti.isSelected() ) {
        sti.hideStep();
      }
    }
    getCurrentStep().hideStep();
    redraw();
  }

  public void detachStep() {
    detach( getCurrentStep() );
    selectedSteps = null;
  }

  public void partitioning() {
    transStepDelegate.editStepPartitioning( transMeta, getCurrentStep() );
  }

  public void errorHandling() {
    transStepDelegate.editStepErrorHandling( transMeta, getCurrentStep() );
  }

  public void newHopChoice() {
    selectedSteps = null;
    newHop();
  }

  @GuiContextAction(
    id = "transgraph-edit-step",
    parentId = HopGuiTransStepContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Edit the step",
    tooltip = "Edit the step properties",
    image = "ui/images/Edit.svg"
  )
  public void editStep( HopGuiTransStepContext context ) {
    editStep( context.getStepMeta() );
  }

  public void editStep() {
    selectedSteps = null;
    editStep( getCurrentStep() );
  }

  @GuiContextAction(
    id = "transgraph-edit-step-description",
    parentId = HopGuiTransStepContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Edit step description",
    tooltip = "Modify the step description",
    image = "ui/images/Edit.svg"
  )
  public void editDescription( HopGuiTransStepContext context ) {
    editDescription( context.getStepMeta() );
  }

  @GuiContextAction(
    id = "transgraph-edit-step-rows-distrubute",
    parentId = HopGuiTransStepContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Distribute rows",
    tooltip = "Make the step distribute rows to next steps",
    image = "ui/images/Edit.svg"
  )
  public void setDistributes( HopGuiTransStepContext context ) {
    context.getStepMeta().setDistributes( true );
    context.getStepMeta().setRowDistribution( null );
    redraw();
  }

  @GuiContextAction(
    id = "transgraph-edit-step-custom-row-distribution",
    parentId = HopGuiTransStepContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Specify row distribution",
    tooltip = "Specify how the step should distribute rows to next steps",
    image = "ui/images/Edit.svg"
  )
  public void setCustomRowDistribution( HopGuiTransStepContext context ) {
    // ask user which row distribution is needed...
    //
    RowDistributionInterface rowDistribution = askUserForCustomDistributionMethod();
    context.getStepMeta().setDistributes( true );
    context.getStepMeta().setRowDistribution( rowDistribution );
    redraw();
  }

  public RowDistributionInterface askUserForCustomDistributionMethod() {
    List<PluginInterface> plugins = PluginRegistry.getInstance().getPlugins( RowDistributionPluginType.class );
    if ( Utils.isEmpty( plugins ) ) {
      return null;
    }
    List<String> choices = new ArrayList<>();
    for ( PluginInterface plugin : plugins ) {
      choices.add( plugin.getName() + " : " + plugin.getDescription() );
    }
    EnterSelectionDialog dialog =
      new EnterSelectionDialog( shell, choices.toArray( new String[ choices.size() ] ), "Select distribution method",
        "Please select the row distribution method:" );
    if ( dialog.open() != null ) {
      PluginInterface plugin = plugins.get( dialog.getSelectionNr() );
      try {
        return (RowDistributionInterface) PluginRegistry.getInstance().loadClass( plugin );
      } catch ( Exception e ) {
        new ErrorDialog( shell, "Error", "Error loading row distribution plugin class", e );
        return null;
      }
    } else {
      return null;
    }
  }

  @GuiContextAction(
    id = "transgraph-edit-step-copies",
    parentId = HopGuiTransStepContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Set the number of step copies",
    tooltip = "Set the number of step copies to use during execution",
    image = "ui/images/parallel-hop.svg"
  )
  public void copies( HopGuiTransStepContext context ) {
    StepMeta stepMeta = context.getStepMeta();
    copies( stepMeta );
  }

  public void copies(StepMeta stepMeta) {
    final boolean multipleOK = checkNumberOfCopies( transMeta, stepMeta );
    selectedSteps = null;
    String tt = BaseMessages.getString( PKG, "TransGraph.Dialog.NrOfCopiesOfStep.Title" );
    String mt = BaseMessages.getString( PKG, "TransGraph.Dialog.NrOfCopiesOfStep.Message" );
    EnterStringDialog nd = new EnterStringDialog( shell, stepMeta.getCopiesString(), tt, mt, true, transMeta );
    String cop = nd.open();
    if ( !Utils.isEmpty( cop ) ) {

      int copies = Const.toInt( transMeta.environmentSubstitute( cop ), -1 );
      if ( copies > 1 && !multipleOK ) {
        cop = "1";

        modalMessageDialog( getString( "TransGraph.Dialog.MultipleCopiesAreNotAllowedHere.Title" ),
          getString( "TransGraph.Dialog.MultipleCopiesAreNotAllowedHere.Message" ), SWT.YES | SWT.ICON_WARNING );
      }
      String cps = stepMeta.getCopiesString();
      if ( ( cps != null && !cps.equals( cop ) ) || ( cps == null && cop != null ) ) {
        stepMeta.setChanged();
      }
      stepMeta.setCopiesString( cop );
      redraw();
    }
  }

  @GuiContextAction(
    id = "transgraph-edit-step-duplicate",
    parentId = HopGuiTransStepContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Duplicate this step",
    tooltip = "Creates a duplicate of the selected step",
    image = "ui/images/CPY.svg"
  )
  public void dupeStep( HopGuiTransStepContext context ) {
    try {
      transStepDelegate.dupeStep( transMeta, context.getStepMeta() );
    } catch ( Exception ex ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "TransGraph.Dialog.ErrorDuplicatingStep.Title" ),
        BaseMessages.getString( PKG, "TransGraph.Dialog.ErrorDuplicatingStep.Message" ), ex );
    }
  }

  @GuiContextAction(
    id = "transgraph-delete-step",
    parentId = HopGuiTransStepContext.CONTEXT_ID,
    type = GuiActionType.Delete,
    name = "Delete this step",
    tooltip = "Delete the selected step from the transformation",
    image = "ui/images/generic-delete.svg"
  )
  public void delStep(HopGuiTransStepContext context) {
    delSelected( context.getStepMeta() );
  }

  @GuiContextAction(
    id = "transgraph-step-fields-before",
    parentId = HopGuiTransStepContext.CONTEXT_ID,
    type = GuiActionType.Info,
    name = "Show the fields entering this step",
    tooltip = "Show all the fields entering this step",
    image = "ui/images/info-hop.svg"
  )
  public void fieldsBefore(HopGuiTransStepContext context) {
    selectedSteps = null;
    inputOutputFields( context.getStepMeta(), true );
  }

  @GuiContextAction(
    id = "transgraph-step-fields-after",
    parentId = HopGuiTransStepContext.CONTEXT_ID,
    type = GuiActionType.Info,
    name = "Show the fields exiting this step",
    tooltip = "Show all the fields resulting from this step",
    image = "ui/images/info-hop.svg"
  )
  public void fieldsAfter(HopGuiTransStepContext context) {
    selectedSteps = null;
    inputOutputFields( context.getStepMeta(), false );
  }

  public void fieldsLineage() {
    TransDataLineage tdl = new TransDataLineage( transMeta );
    try {
      tdl.calculateLineage();
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Lineage error", "Unexpected lineage calculation error", e );
    }
  }

  public void editHop() {
    selectionRegion = null;
    editHop( getCurrentHop() );
  }

  public void flipHopDirection() {
    selectionRegion = null;
    TransHopMeta hi = getCurrentHop();

    hi.flip();
    if ( transMeta.hasLoop( hi.getToStep() ) ) {
      redraw();
      modalMessageDialog( getString( "TransGraph.Dialog.LoopsAreNotAllowed.Title" ),
        getString( "TransGraph.Dialog.LoopsAreNotAllowed.Message" ), SWT.OK | SWT.ICON_ERROR );
      hi.flip();
      redraw();
    } else {
      hi.setChanged();
      redraw();
      hopUi.setShellText();
    }
  }

  public void enableHop() {
    selectionRegion = null;
    TransHopMeta hi = getCurrentHop();
    TransHopMeta before = (TransHopMeta) hi.clone();
    setHopEnabled( hi, !hi.isEnabled() );
    if ( hi.isEnabled() && transMeta.hasLoop( hi.getToStep() ) ) {
      setHopEnabled( hi, false );
      modalMessageDialog( getString( "TransGraph.Dialog.LoopAfterHopEnabled.Title" ),
        getString( "TransGraph.Dialog.LoopAfterHopEnabled.Message" ), SWT.OK | SWT.ICON_ERROR );
    } else {
      TransHopMeta after = (TransHopMeta) hi.clone();
      hopUi.undoDelegate.addUndoChange( transMeta, new TransHopMeta[] { before }, new TransHopMeta[] { after }, new int[] { transMeta.indexOfTransHop( hi ) } );
      redraw();
    }
    updateErrorMetaForHop( hi );
  }

  public void deleteHop() {
    selectionRegion = null;
    TransHopMeta hi = getCurrentHop();
    transHopDelegate.delHop( transMeta, hi );
  }

  private void updateErrorMetaForHop( TransHopMeta hop ) {
    if ( hop != null && hop.isErrorHop() ) {
      StepErrorMeta errorMeta = hop.getFromStep().getStepErrorMeta();
      if ( errorMeta != null ) {
        errorMeta.setEnabled( hop.isEnabled() );
      }
    }
  }

  public void enableHopsBetweenSelectedSteps() {
    enableHopsBetweenSelectedSteps( true );
  }

  public void disableHopsBetweenSelectedSteps() {
    enableHopsBetweenSelectedSteps( false );
  }

  /**
   * This method enables or disables all the hops between the selected steps.
   **/
  public void enableHopsBetweenSelectedSteps( boolean enabled ) {
    List<StepMeta> list = transMeta.getSelectedSteps();

    boolean hasLoop = false;

    for ( int i = 0; i < transMeta.nrTransHops(); i++ ) {
      TransHopMeta hop = transMeta.getTransHop( i );
      if ( list.contains( hop.getFromStep() ) && list.contains( hop.getToStep() ) ) {

        TransHopMeta before = (TransHopMeta) hop.clone();
        setHopEnabled( hop, enabled );
        TransHopMeta after = (TransHopMeta) hop.clone();
        hopUi.undoDelegate.addUndoChange( transMeta, new TransHopMeta[] { before }, new TransHopMeta[] { after }, new int[] { transMeta.indexOfTransHop( hop ) } );

        if ( transMeta.hasLoop( hop.getToStep() ) ) {
          hasLoop = true;
          setHopEnabled( hop, false );
        }
      }
    }

    if ( enabled && hasLoop ) {
      modalMessageDialog( getString( "TransGraph.Dialog.HopCausesLoop.Title" ),
        getString( "TransGraph.Dialog.HopCausesLoop.Message" ), SWT.OK | SWT.ICON_ERROR );
    }

    redraw();
  }

  public void enableHopsDownstream() {
    enableDisableHopsDownstream( true );
  }

  public void disableHopsDownstream() {
    enableDisableHopsDownstream( false );
  }

  public void enableDisableHopsDownstream( boolean enabled ) {
    if ( currentHop == null ) {
      return;
    }

    TransHopMeta before = (TransHopMeta) currentHop.clone();
    setHopEnabled( currentHop, enabled );
    TransHopMeta after = (TransHopMeta) currentHop.clone();
    hopUi.undoDelegate.addUndoChange( transMeta, new TransHopMeta[] { before }, new TransHopMeta[] { after }, new int[] { transMeta.indexOfTransHop( currentHop ) } );

    Set<StepMeta> checkedEntries = enableDisableNextHops( currentHop.getToStep(), enabled, new HashSet<>() );

    if ( checkedEntries.stream().anyMatch( entry -> transMeta.hasLoop( entry ) ) ) {
      modalMessageDialog( getString( "TransGraph.Dialog.HopCausesLoop.Title" ),
        getString( "TransGraph.Dialog.HopCausesLoop.Message" ), SWT.OK | SWT.ICON_ERROR );
    }

    redraw();
  }

  private Set<StepMeta> enableDisableNextHops( StepMeta from, boolean enabled, Set<StepMeta> checkedEntries ) {
    checkedEntries.add( from );
    transMeta.getTransHops().stream()
      .filter( hop -> from.equals( hop.getFromStep() ) )
      .forEach( hop -> {
        if ( hop.isEnabled() != enabled ) {
          TransHopMeta before = (TransHopMeta) hop.clone();
          setHopEnabled( hop, enabled );
          TransHopMeta after = (TransHopMeta) hop.clone();
          hopUi.undoDelegate.addUndoChange( transMeta, new TransHopMeta[] { before }, new TransHopMeta[] { after }, new int[] { transMeta.indexOfTransHop( hop ) } );
        }
        if ( !checkedEntries.contains( hop.getToStep() ) ) {
          enableDisableNextHops( hop.getToStep(), enabled, checkedEntries );
        }
      } );
    return checkedEntries;
  }

  @GuiContextAction(
    id = "transgraph-edit-note",
    parentId = HopGuiTransNoteContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Edit the note",
    tooltip = "Edit the note",
    image = "ui/images/Edit.svg"
  )
  public void editNote( HopGuiTransNoteContext context ) {
    selectionRegion = null;
    editNote( context.getNotePadMeta() );
  }

  @GuiContextAction(
    id = "transgraph-delete-note",
    parentId = HopGuiTransNoteContext.CONTEXT_ID,
    type = GuiActionType.Delete,
    name = "Delete the note",
    tooltip = "Delete the note",
    image = "ui/images/generic-delete.svg"
  )
  public void deleteNote( HopGuiTransNoteContext context ) {
    selectionRegion = null;
    int idx = transMeta.indexOfNote( context.getNotePadMeta() );
    if ( idx >= 0 ) {
      transMeta.removeNote( idx );
      hopUi.undoDelegate.addUndoDelete( transMeta, new NotePadMeta[] { context.getNotePadMeta().clone() }, new int[] { idx } );
      redraw();
    }
  }

  @GuiContextAction(
    id = "transgraph-new-note",
    parentId = HopGuiTransContext.CONTEXT_ID,
    type = GuiActionType.Create,
    name = "Create a note",
    tooltip = "Create a new note",
    image = "ui/images/new.svg"
  )
  public void newNote( HopGuiTransContext context ) {
    selectionRegion = null;
    String title = BaseMessages.getString( PKG, "TransGraph.Dialog.NoteEditor.Title" );
    NotePadDialog dd = new NotePadDialog( transMeta, shell, title );
    NotePadMeta n = dd.open();
    if ( n != null ) {
      NotePadMeta npi =
        new NotePadMeta( n.getNote(), context.getClick().x, context.getClick().y, ConstUI.NOTE_MIN_SIZE, ConstUI.NOTE_MIN_SIZE, n
          .getFontName(), n.getFontSize(), n.isFontBold(), n.isFontItalic(), n.getFontColorRed(), n
          .getFontColorGreen(), n.getFontColorBlue(), n.getBackGroundColorRed(), n.getBackGroundColorGreen(), n
          .getBackGroundColorBlue(), n.getBorderColorRed(), n.getBorderColorGreen(), n.getBorderColorBlue(), n
          .isDrawShadow() );
      transMeta.addNote( npi );
      hopUi.undoDelegate.addUndoNew( transMeta, new NotePadMeta[] { npi }, new int[] { transMeta.indexOfNote( npi ) } );
      redraw();
    }
  }

  @GuiContextAction(
    id = "transgraph-edit-transformation",
    parentId = HopGuiTransContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Edit transformation",
    tooltip = "Edit transformation properties",
    image = "ui/images/TRN.svg"
  )
  public void settings( HopGuiTransContext context ) {
    editProperties( transMeta, hopUi, true );
  }

  public void newStep( String description ) {
    StepMeta stepMeta = transStepDelegate.newStep( transMeta, null, description, description, false, true, new Point( currentMouseX, currentMouseY ) );
    PropsUI.setLocation( stepMeta, currentMouseX, currentMouseY );
    stepMeta.setDraw( true );
    redraw();
  }

  /**
   * This sets the popup-menu on the background of the canvas based on the xy coordinate of the mouse. This method is
   * called after a mouse-click.
   *
   * @param x X-coordinate on screen
   * @param y Y-coordinate on screen
   */
  private synchronized void setMenu( int x, int y ) {
    try {
      currentMouseX = x;
      currentMouseY = y;

      /** TODO: Rewrite using a new set of GuiElements

       final StepMeta stepMeta = transMeta.getStep( x, y, iconsize );
       if ( stepMeta != null ) { // We clicked on a Step!

       setCurrentStep( stepMeta );


       XulMenupopup menu = menuMap.get( "trans-graph-entry" );

       try {
       ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.TransStepRightClick.id,
       new StepMenuExtension( this, menu ) );
       } catch ( Exception ex ) {
       LogChannel.GENERAL.logError( "Error calling TransStepRightClick extension point", ex );
       }

       if ( menu != null ) {
       List<StepMeta> selection = transMeta.getSelectedSteps();
       doRightClickSelection( stepMeta, selection );
       int sels = selection.size();

       Document doc = getXulDomContainer().getDocumentRoot();

       // TODO: cache the next line (seems fast enough)?
       //
       List<PluginInterface> rowDistributionPlugins =
       PluginRegistry.getInstance().getPlugins( RowDistributionPluginType.class );

       JfaceMenupopup customRowDistMenu =
       (JfaceMenupopup) doc.getElementById( "trans-graph-entry-data-movement-popup" );
       customRowDistMenu.setDisabled( false );
       customRowDistMenu.removeChildren();

       // Add the default round robin plugin...
       //
       Action action = new Action( "RoundRobinRowDistribution", Action.AS_CHECK_BOX ) {
      @Override public void run() {
      stepMeta.setRowDistribution( null ); // default
      stepMeta.setDistributes( true );
      }
      };
       boolean selected = stepMeta.isDistributes() && stepMeta.getRowDistribution() == null;
       action.setChecked( selected );
       JfaceMenuitem child =
       new JfaceMenuitem( null, customRowDistMenu, xulDomContainer, "Round Robin row distribution", 0, action );
       child.setLabel( BaseMessages.getString( PKG, "TransGraph.PopupMenu.RoundRobin" ) );
       child.setDisabled( false );
       child.setSelected( selected );

       for ( int p = 0; p < rowDistributionPlugins.size(); p++ ) {
       final PluginInterface rowDistributionPlugin = rowDistributionPlugins.get( p );
       selected =
       stepMeta.isDistributes() && stepMeta.getRowDistribution() != null
       && stepMeta.getRowDistribution().getCode().equals( rowDistributionPlugin.getIds()[ 0 ] );

       action = new Action( rowDistributionPlugin.getIds()[ 0 ], Action.AS_CHECK_BOX ) {
      @Override public void run() {
      try {
      stepMeta.setRowDistribution( (RowDistributionInterface) PluginRegistry.getInstance().loadClass(
      rowDistributionPlugin ) );
      } catch ( Exception e ) {
      LogChannel.GENERAL.logError( "Error loading row distribution plugin class: ", e );
      }
      }
      };
       action.setChecked( selected );
       child =
       new JfaceMenuitem( null, customRowDistMenu, xulDomContainer, rowDistributionPlugin.getName(), p + 1,
       action );
       child.setLabel( rowDistributionPlugin.getName() );
       child.setDisabled( false );
       child.setSelected( selected );
       }

       // Add the default copy rows plugin...
       //

       action = new Action( "CopyRowsDistribution", Action.AS_CHECK_BOX ) {
      @Override public void run() {
      stepMeta.setDistributes( false );
      }
      };
       selected = !stepMeta.isDistributes();
       action.setChecked( selected );
       child = new JfaceMenuitem( null, customRowDistMenu, xulDomContainer, "Copy rows distribution", 0, action );
       child.setLabel( BaseMessages.getString( PKG, "TransGraph.PopupMenu.CopyData" ) );
       child.setDisabled( false );
       child.setSelected( selected );
       JfaceMenupopup launchMenu = (JfaceMenupopup) doc.getElementById( "trans-graph-entry-launch-popup" );
       String[] referencedObjects = stepMeta.getStepMetaInterface().getReferencedObjectDescriptions();
       boolean[] enabledObjects = stepMeta.getStepMetaInterface().isReferencedObjectEnabled();
       launchMenu.setDisabled( Utils.isEmpty( referencedObjects ) );

       launchMenu.removeChildren();
       int childIndex = 0;

       // First see if we need to add a special "active" entry (running transformation)
       //
       StepMetaInterface stepMetaInterface = stepMeta.getStepMetaInterface();
       String activeReferencedObjectDescription = stepMetaInterface.getActiveReferencedObjectDescription();
       if ( getActiveSubtransformation( this, stepMeta ) != null && activeReferencedObjectDescription != null ) {
       action = new Action( activeReferencedObjectDescription, Action.AS_DROP_DOWN_MENU ) {
      @Override public void run() {
      openMapping( stepMeta, -1 ); // negative by convention
      }
      };
       child =
       new JfaceMenuitem( null, launchMenu, xulDomContainer, activeReferencedObjectDescription, childIndex++,
       action );
       child.setLabel( activeReferencedObjectDescription );
       child.setDisabled( false );
       }

       if ( !Utils.isEmpty( referencedObjects ) ) {
       for ( int i = 0; i < referencedObjects.length; i++ ) {
       final int index = i;
       String referencedObject = referencedObjects[ i ];
       action = new Action( referencedObject, Action.AS_DROP_DOWN_MENU ) {
      @Override public void run() {
      openMapping( stepMeta, index );
      }
      };
       child = new JfaceMenuitem( null, launchMenu, xulDomContainer, referencedObject, childIndex++, action );
       child.setLabel( referencedObject );
       child.setDisabled( !enabledObjects[ i ] );
       }
       }

       initializeXulMenu( doc, selection, stepMeta );
       ConstUI.displayMenu( menu, canvas );
       }
       } else {
       final TransHopMeta hi = findHop( x, y );
       if ( hi != null ) { // We clicked on a HOP!

       XulMenupopup menu = menuMap.get( "trans-graph-hop" );
       if ( menu != null ) {
       setCurrentHop( hi );
       XulMenuitem item =
       (XulMenuitem) getXulDomContainer().getDocumentRoot().getElementById( "trans-graph-hop-enabled" );
       if ( item != null ) {
       if ( hi.isEnabled() ) {
       item.setLabel( BaseMessages.getString( PKG, "TransGraph.PopupMenu.DisableHop" ) );
       } else {
       item.setLabel( BaseMessages.getString( PKG, "TransGraph.PopupMenu.EnableHop" ) );
       }
       }

       ConstUI.displayMenu( menu, canvas );
       }
       } else {
       // Clicked on the background: maybe we hit a note?
       final NotePadMeta ni = transMeta.getNote( x, y );
       setCurrentNote( ni );
       if ( ni != null ) {

       XulMenupopup menu = menuMap.get( "trans-graph-note" );
       if ( menu != null ) {
       ConstUI.displayMenu( menu, canvas );
       }
       } else {
       XulMenupopup menu = menuMap.get( "trans-graph-background" );
       if ( menu != null ) {
       final String clipcontent = hopUi.fromClipboard();
       XulMenuitem item =
       (XulMenuitem) getXulDomContainer().getDocumentRoot().getElementById( "trans-graph-background-paste" );
       if ( item != null ) {
       item.setDisabled( clipcontent == null );
       }
       ConstUI.displayMenu( menu, canvas );
       }

       }
       }
       }
       */
    } catch ( Throwable t ) {
      new ErrorDialog( shell, "Error", "Error showing context menu", t );
    }
  }


  private boolean checkNumberOfCopies( TransMeta transMeta, StepMeta stepMeta ) {
    boolean enabled = true;
    List<StepMeta> prevSteps = transMeta.findPreviousSteps( stepMeta );
    for ( StepMeta prevStep : prevSteps ) {
      // See what the target steps are.
      // If one of the target steps is our original step, we can't start multiple copies
      //
      String[] targetSteps = prevStep.getStepMetaInterface().getStepIOMeta().getTargetStepnames();
      if ( targetSteps != null ) {
        for ( int t = 0; t < targetSteps.length && enabled; t++ ) {
          if ( !Utils.isEmpty( targetSteps[ t ] ) && targetSteps[ t ].equalsIgnoreCase( stepMeta.getName() ) ) {
            enabled = false;
          }
        }
      }
    }
    return enabled;
  }

  private AreaOwner setToolTip( int x, int y, int screenX, int screenY ) {
    AreaOwner subject = null;

    if ( !hopUi.getProps().showToolTips() ) {
      return subject;
    }

    canvas.setToolTipText( null );

    String newTip = null;
    Image tipImage = null;

    final TransHopMeta hi = findHop( x, y );
    // check the area owner list...
    //
    StringBuilder tip = new StringBuilder();
    AreaOwner areaOwner = getVisibleAreaOwner( x, y );
    AreaType areaType = null;
    if ( areaOwner != null && areaOwner.getAreaType() != null ) {
      areaType = areaOwner.getAreaType();
      switch ( areaType ) {
        case REMOTE_INPUT_STEP:
          StepMeta step = (StepMeta) areaOwner.getParent();
          tip.append( "Remote input steps:" ).append( Const.CR ).append( "-----------------------" ).append( Const.CR );
          for ( RemoteStep remoteStep : step.getRemoteInputSteps() ) {
            tip.append( remoteStep.toString() ).append( Const.CR );
          }
          break;
        case REMOTE_OUTPUT_STEP:
          step = (StepMeta) areaOwner.getParent();
          tip.append( "Remote output steps:" ).append( Const.CR ).append( "-----------------------" )
            .append( Const.CR );
          for ( RemoteStep remoteStep : step.getRemoteOutputSteps() ) {
            tip.append( remoteStep.toString() ).append( Const.CR );
          }
          break;
        case STEP_PARTITIONING:
          step = (StepMeta) areaOwner.getParent();
          tip.append( "Step partitioning:" ).append( Const.CR ).append( "-----------------------" ).append( Const.CR );
          tip.append( step.getStepPartitioningMeta().toString() ).append( Const.CR );
          if ( step.getTargetStepPartitioningMeta() != null ) {
            tip.append( Const.CR ).append( Const.CR ).append(
              "TARGET: " + step.getTargetStepPartitioningMeta().toString() ).append( Const.CR );
          }
          break;
        case STEP_ERROR_ICON:
          String log = (String) areaOwner.getParent();
          tip.append( log );
          tipImage = GUIResource.getInstance().getImageStepError();
          break;
        case STEP_ERROR_RED_ICON:
          String redLog = (String) areaOwner.getParent();
          tip.append( redLog );
          tipImage = GUIResource.getInstance().getImageRedStepError();
          break;
        case HOP_COPY_ICON:
          step = (StepMeta) areaOwner.getParent();
          tip.append( BaseMessages.getString( PKG, "TransGraph.Hop.Tooltip.HopTypeCopy", step.getName(), Const.CR ) );
          tipImage = GUIResource.getInstance().getImageCopyHop();
          break;
        case ROW_DISTRIBUTION_ICON:
          step = (StepMeta) areaOwner.getParent();
          tip.append( BaseMessages.getString( PKG, "TransGraph.Hop.Tooltip.RowDistribution", step.getName(), step
            .getRowDistribution() == null ? "" : step.getRowDistribution().getDescription() ) );
          tip.append( Const.CR );
          tipImage = GUIResource.getInstance().getImageBalance();
          break;
        case HOP_INFO_ICON:
          StepMeta from = (StepMeta) areaOwner.getParent();
          StepMeta to = (StepMeta) areaOwner.getOwner();
          tip.append( BaseMessages.getString( PKG, "TransGraph.Hop.Tooltip.HopTypeInfo", to.getName(), from.getName(),
            Const.CR ) );
          tipImage = GUIResource.getInstance().getImageInfoHop();
          break;
        case HOP_ERROR_ICON:
          from = (StepMeta) areaOwner.getParent();
          to = (StepMeta) areaOwner.getOwner();
          areaOwner.getOwner();
          tip.append( BaseMessages.getString( PKG, "TransGraph.Hop.Tooltip.HopTypeError", from.getName(), to.getName(),
            Const.CR ) );
          tipImage = GUIResource.getInstance().getImageErrorHop();
          break;
        case HOP_INFO_STEP_COPIES_ERROR:
          from = (StepMeta) areaOwner.getParent();
          to = (StepMeta) areaOwner.getOwner();
          tip.append( BaseMessages.getString( PKG, "TransGraph.Hop.Tooltip.InfoStepCopies", from.getName(), to
            .getName(), Const.CR ) );
          tipImage = GUIResource.getInstance().getImageStepError();
          break;
        case STEP_INPUT_HOP_ICON:
          // StepMeta subjectStep = (StepMeta) (areaOwner.getParent());
          tip.append( BaseMessages.getString( PKG, "TransGraph.StepInputConnector.Tooltip" ) );
          tipImage = GUIResource.getInstance().getImageHopInput();
          break;
        case STEP_OUTPUT_HOP_ICON:
          // subjectStep = (StepMeta) (areaOwner.getParent());
          tip.append( BaseMessages.getString( PKG, "TransGraph.StepOutputConnector.Tooltip" ) );
          tipImage = GUIResource.getInstance().getImageHopOutput();
          break;
        case STEP_INFO_HOP_ICON:
          // subjectStep = (StepMeta) (areaOwner.getParent());
          // StreamInterface stream = (StreamInterface) areaOwner.getOwner();
          StepIOMetaInterface ioMeta = (StepIOMetaInterface) areaOwner.getOwner();
          tip.append( BaseMessages.getString( PKG, "TransGraph.StepInfoConnector.Tooltip" ) + Const.CR
            + ioMeta.toString() );
          tipImage = GUIResource.getInstance().getImageHopOutput();
          break;
        case STEP_TARGET_HOP_ICON:
          StreamInterface stream = (StreamInterface) areaOwner.getOwner();
          tip.append( stream.getDescription() );
          tipImage = GUIResource.getInstance().getImageHopOutput();
          break;
        case STEP_ERROR_HOP_ICON:
          StepMeta stepMeta = (StepMeta) areaOwner.getParent();
          if ( stepMeta.supportsErrorHandling() ) {
            tip.append( BaseMessages.getString( PKG, "TransGraph.StepSupportsErrorHandling.Tooltip" ) );
          } else {
            tip.append( BaseMessages.getString( PKG, "TransGraph.StepDoesNotSupportsErrorHandling.Tooltip" ) );
          }
          tipImage = GUIResource.getInstance().getImageHopOutput();
          break;
        case STEP_EDIT_ICON:
          tip.append( BaseMessages.getString( PKG, "TransGraph.EditStep.Tooltip" ) );
          tipImage = GUIResource.getInstance().getImageEdit();
          break;
        case STEP_INJECT_ICON:
          Object injection = areaOwner.getOwner();
          if ( injection != null ) {
            tip.append( BaseMessages.getString( PKG, "TransGraph.StepInjectionSupported.Tooltip" ) );
          } else {
            tip.append( BaseMessages.getString( PKG, "TransGraph.StepInjectionNotSupported.Tooltip" ) );
          }
          tipImage = GUIResource.getInstance().getImageInject();
          break;
        case STEP_MENU_ICON:
          tip.append( BaseMessages.getString( PKG, "TransGraph.ShowMenu.Tooltip" ) );
          tipImage = GUIResource.getInstance().getImageContextMenu();
          break;
        case STEP_ICON:
          StepMeta iconStepMeta = (StepMeta) areaOwner.getOwner();
          if ( iconStepMeta.isDeprecated() ) { // only need tooltip if step is deprecated
            tip.append( BaseMessages.getString( PKG, "TransGraph.DeprecatedStep.Tooltip.Title" ) ).append( Const.CR );
            String tipNext = BaseMessages.getString( PKG, "TransGraph.DeprecatedStep.Tooltip.Message1",
              iconStepMeta.getName() );
            int length = tipNext.length() + 5;
            for ( int i = 0; i < length; i++ ) {
              tip.append( "-" );
            }
            tip.append( Const.CR ).append( tipNext ).append( Const.CR );
            tip.append( BaseMessages.getString( PKG, "TransGraph.DeprecatedStep.Tooltip.Message2" ) );
            if ( !Utils.isEmpty( iconStepMeta.getSuggestion() )
              && !( iconStepMeta.getSuggestion().startsWith( "!" ) && iconStepMeta.getSuggestion().endsWith( "!" ) ) ) {
              tip.append( " " );
              tip.append( BaseMessages.getString( PKG, "TransGraph.DeprecatedStep.Tooltip.Message3",
                iconStepMeta.getSuggestion() ) );
            }
            tipImage = GUIResource.getInstance().getImageDeprecated();
            toolTip.setHideDelay( TOOLTIP_HIDE_DELAY_LONG );
          }
          break;
        default:
          break;
      }
    }

    if ( hi != null && tip.length() == 0 ) { // We clicked on a HOP!
      // Set the tooltip for the hop:
      tip.append( Const.CR ).append( BaseMessages.getString( PKG, "TransGraph.Dialog.HopInfo" ) ).append(
        newTip = hi.toString() ).append( Const.CR );
    }

    if ( tip.length() == 0 ) {
      newTip = null;
    } else {
      newTip = tip.toString();
    }

    if ( newTip == null ) {
      toolTip.hide();
      if ( hi != null ) { // We clicked on a HOP!

        // Set the tooltip for the hop:
        newTip =
          BaseMessages.getString( PKG, "TransGraph.Dialog.HopInfo" )
            + Const.CR
            + BaseMessages.getString( PKG, "TransGraph.Dialog.HopInfo.SourceStep" )
            + " "
            + hi.getFromStep().getName()
            + Const.CR
            + BaseMessages.getString( PKG, "TransGraph.Dialog.HopInfo.TargetStep" )
            + " "
            + hi.getToStep().getName()
            + Const.CR
            + BaseMessages.getString( PKG, "TransGraph.Dialog.HopInfo.Status" )
            + " "
            + ( hi.isEnabled() ? BaseMessages.getString( PKG, "TransGraph.Dialog.HopInfo.Enable" ) : BaseMessages
            .getString( PKG, "TransGraph.Dialog.HopInfo.Disable" ) );
        toolTip.setText( newTip );
        if ( hi.isEnabled() ) {
          toolTip.setImage( GUIResource.getInstance().getImageHop() );
        } else {
          toolTip.setImage( GUIResource.getInstance().getImageDisabledHop() );
        }
        toolTip.show( new org.eclipse.swt.graphics.Point( screenX, screenY ) );
      } else {
        newTip = null;
      }

    } else if ( !newTip.equalsIgnoreCase( getToolTipText() ) ) {
      Image tooltipImage = null;
      if ( tipImage != null ) {
        tooltipImage = tipImage;
      } else {
        tooltipImage = GUIResource.getInstance().getImageHopUi();
      }
      showTooltip( newTip, tooltipImage, screenX, screenY );
    }

    if ( areaOwner != null && areaOwner.getExtensionAreaType() != null ) {
      try {
        HopGuiTransPainterFlyoutTooltipExtension extension =
          new HopGuiTransPainterFlyoutTooltipExtension( areaOwner, this, new Point( screenX, screenY ) );

        ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, HopExtensionPoint.TransPainterFlyoutTooltip.id, extension );

      } catch ( Exception e ) {
        LogChannel.GENERAL.logError( "Error calling extension point(s) for the transformation painter step", e );
      }
    }

    return subject;
  }

  public void showTooltip( String label, Image image, int screenX, int screenY ) {
    toolTip.setImage( image );
    toolTip.setText( label );
    toolTip.hide();
    toolTip.show( new org.eclipse.swt.graphics.Point( screenX, screenY ) );
  }

  public synchronized AreaOwner getVisibleAreaOwner( int x, int y ) {
    for ( int i = areaOwners.size() - 1; i >= 0; i-- ) {
      AreaOwner areaOwner = areaOwners.get( i );
      if ( areaOwner.contains( x, y ) ) {
        return areaOwner;
      }
    }
    return null;
  }

  public void delSelected( StepMeta stepMeta ) {
    List<StepMeta> selection = transMeta.getSelectedSteps();
    if ( stepMeta != null && selection.size() == 0 ) {
      transStepDelegate.delStep( transMeta, stepMeta );
      return;
    }

    if ( currentStep != null && selection.contains( currentStep ) ) {
      currentStep = null;
      for ( StepSelectionListener listener : currentStepListeners ) {
        listener.onUpdateSelection( currentStep );
      }
    }

    transStepDelegate.delSteps( transMeta, selection );
    notePadDelegate.deleteNotes( transMeta, transMeta.getSelectedNotes() );
  }

  public void editDescription( StepMeta stepMeta ) {
    String title = BaseMessages.getString( PKG, "TransGraph.Dialog.StepDescription.Title" );
    String message = BaseMessages.getString( PKG, "TransGraph.Dialog.StepDescription.Message" );
    EnterTextDialog dd = new EnterTextDialog( shell, title, message, stepMeta.getDescription() );
    String d = dd.open();
    if ( d != null ) {
      stepMeta.setDescription( d );
      stepMeta.setChanged();
      hopUi.setShellText();
    }
  }

  /**
   * Display the input- or outputfields for a step.
   *
   * @param stepMeta The step (it's metadata) to query
   * @param before   set to true if you want to have the fields going INTO the step, false if you want to see all the
   *                 fields that exit the step.
   */
  private void inputOutputFields( StepMeta stepMeta, boolean before ) {
    redraw();

    SearchFieldsProgressDialog op = new SearchFieldsProgressDialog( transMeta, stepMeta, before );
    boolean alreadyThrownError = false;
    try {
      final ProgressMonitorDialog pmd = new ProgressMonitorDialog( shell );

      // Run something in the background to cancel active database queries, forecably if needed!
      Runnable run = new Runnable() {
        @Override
        public void run() {
          IProgressMonitor monitor = pmd.getProgressMonitor();
          while ( pmd.getShell() == null || ( !pmd.getShell().isDisposed() && !monitor.isCanceled() ) ) {
            try {
              Thread.sleep( 250 );
            } catch ( InterruptedException e ) {
              // Ignore
            }
          }

          if ( monitor.isCanceled() ) { // Disconnect and see what happens!

            try {
              transMeta.cancelQueries();
            } catch ( Exception e ) {
              // Ignore
            }
          }
        }
      };
      // Dump the cancel looker in the background!
      new Thread( run ).start();

      pmd.run( true, true, op );
    } catch ( InvocationTargetException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "TransGraph.Dialog.GettingFields.Title" ), BaseMessages
        .getString( PKG, "TransGraph.Dialog.GettingFields.Message" ), e );
      alreadyThrownError = true;
    } catch ( InterruptedException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "TransGraph.Dialog.GettingFields.Title" ), BaseMessages
        .getString( PKG, "TransGraph.Dialog.GettingFields.Message" ), e );
      alreadyThrownError = true;
    }

    RowMetaInterface fields = op.getFields();

    if ( fields != null && fields.size() > 0 ) {
      StepFieldsDialog sfd = new StepFieldsDialog( shell, transMeta, SWT.NONE, stepMeta.getName(), fields );
      String sn = (String) sfd.open();
      if ( sn != null ) {
        StepMeta esi = transMeta.findStep( sn );
        if ( esi != null ) {
          editStep( esi );
        }
      }
    } else {
      if ( !alreadyThrownError ) {
        modalMessageDialog( getString( "TransGraph.Dialog.CouldntFindFields.Title" ),
          getString( "TransGraph.Dialog.CouldntFindFields.Message" ), SWT.OK | SWT.ICON_INFORMATION );
      }
    }

  }

  public void paintControl( PaintEvent e ) {
    Point area = getArea();
    if ( area.x == 0 || area.y == 0 ) {
      return; // nothing to do!
    }

    Display disp = shell.getDisplay();

    Image img = getTransformationImage( disp, area.x, area.y, magnification );
    e.gc.drawImage( img, 0, 0 );
    if ( transMeta.nrSteps() == 0 ) {
      e.gc.setForeground( GUIResource.getInstance().getColorCrystalText() );
      e.gc.setFont( GUIResource.getInstance().getFontMedium() );

      Image emptyCanvasImage = GUIResource.getInstance().getImageTransCanvas();
      int leftPosition = ( area.x - emptyCanvasImage.getBounds().width ) / 2;
      int topPosition = ( area.y - emptyCanvasImage.getBounds().height ) / 2;
      e.gc.drawImage( emptyCanvasImage, leftPosition, topPosition );
    }
    img.dispose();
  }

  public Image getTransformationImage( Device device, int x, int y, float magnificationFactor ) {

    GCInterface gc = new SWTGC( device, new Point( x, y ), iconsize );

    int gridSize =
      PropsUI.getInstance().isShowCanvasGridEnabled() ? PropsUI.getInstance().getCanvasGridSize() : 1;

    TransPainter transPainter = new TransPainter( gc, transMeta, new Point( x, y ), new SwtScrollBar( hori ), new SwtScrollBar( vert ),
      candidate, drop_candidate, selectionRegion, areaOwners,
      PropsUI.getInstance().getIconSize(), PropsUI.getInstance().getLineWidth(), gridSize,
      PropsUI.getInstance().getShadowSize(), PropsUI.getInstance()
      .isAntiAliasingEnabled(), PropsUI.getInstance().getNoteFont().getName(), PropsUI.getInstance()
      .getNoteFont().getHeight(), trans, PropsUI.getInstance().isIndicateSlowTransStepsEnabled(), PropsUI.getInstance().getZoomFactor() );

    // correct the magnifacation with the overall zoom factor
    //
    float correctedMagnification = (float) ( magnificationFactor * PropsUI.getInstance().getZoomFactor() );

    transPainter.setMagnification( correctedMagnification );
    transPainter.setStepLogMap( stepLogMap );
    transPainter.setStartHopStep( startHopStep );
    transPainter.setEndHopLocation( endHopLocation );
    transPainter.setNoInputStep( noInputStep );
    transPainter.setEndHopStep( endHopStep );
    transPainter.setCandidateHopType( candidateHopType );
    transPainter.setStartErrorHopStep( startErrorHopStep );
    transPainter.setShowTargetStreamsStep( showTargetStreamsStep );

    transPainter.buildTransformationImage();

    Image img = (Image) gc.getImage();

    gc.dispose();
    return img;
  }

  @Override
  protected Point getOffset() {
    Point area = getArea();
    Point max = transMeta.getMaximum();
    Point thumb = getThumb( area, max );
    return getOffset( thumb, area );
  }

  private void editStep( StepMeta stepMeta ) {
    transStepDelegate.editStep( transMeta, stepMeta );
  }

  private void editNote( NotePadMeta ni ) {
    NotePadMeta before = (NotePadMeta) ni.clone();

    String title = BaseMessages.getString( PKG, "TransGraph.Dialog.EditNote.Title" );
    NotePadDialog dd = new NotePadDialog( transMeta, shell, title, ni );
    NotePadMeta n = dd.open();

    if ( n != null ) {
      ni.setChanged();
      ni.setNote( n.getNote() );
      ni.setFontName( n.getFontName() );
      ni.setFontSize( n.getFontSize() );
      ni.setFontBold( n.isFontBold() );
      ni.setFontItalic( n.isFontItalic() );
      // font color
      ni.setFontColorRed( n.getFontColorRed() );
      ni.setFontColorGreen( n.getFontColorGreen() );
      ni.setFontColorBlue( n.getFontColorBlue() );
      // background color
      ni.setBackGroundColorRed( n.getBackGroundColorRed() );
      ni.setBackGroundColorGreen( n.getBackGroundColorGreen() );
      ni.setBackGroundColorBlue( n.getBackGroundColorBlue() );
      // border color
      ni.setBorderColorRed( n.getBorderColorRed() );
      ni.setBorderColorGreen( n.getBorderColorGreen() );
      ni.setBorderColorBlue( n.getBorderColorBlue() );
      ni.setDrawShadow( n.isDrawShadow() );
      ni.width = ConstUI.NOTE_MIN_SIZE;
      ni.height = ConstUI.NOTE_MIN_SIZE;

      NotePadMeta after = (NotePadMeta) ni.clone();
      hopUi.undoDelegate.addUndoChange( transMeta, new NotePadMeta[] { before }, new NotePadMeta[] { after }, new int[] { transMeta.indexOfNote( ni ) } );
      redraw();
    }
  }

  private void editHop( TransHopMeta transHopMeta ) {
    String name = transHopMeta.toString();
    if ( log.isDebug() ) {
      log.logDebug( BaseMessages.getString( PKG, "TransGraph.Logging.EditingHop" ) + name );
    }
    transHopDelegate.editHop( transMeta, transHopMeta );
  }

  private void newHop() {
    List<StepMeta> selection = transMeta.getSelectedSteps();
    if ( selection.size() == 2 ) {
      StepMeta fr = selection.get( 0 );
      StepMeta to = selection.get( 1 );
      transHopDelegate.newHop( transMeta, fr, to );
    }
  }

  private boolean pointOnLine( int x, int y, int[] line ) {
    int dx, dy;
    int pm = HOP_SEL_MARGIN / 2;
    boolean retval = false;

    for ( dx = -pm; dx <= pm && !retval; dx++ ) {
      for ( dy = -pm; dy <= pm && !retval; dy++ ) {
        retval = pointOnThinLine( x + dx, y + dy, line );
      }
    }

    return retval;
  }

  private boolean pointOnThinLine( int x, int y, int[] line ) {
    int x1 = line[ 0 ];
    int y1 = line[ 1 ];
    int x2 = line[ 2 ];
    int y2 = line[ 3 ];

    // Not in the square formed by these 2 points: ignore!
    // CHECKSTYLE:LineLength:OFF
    if ( !( ( ( x >= x1 && x <= x2 ) || ( x >= x2 && x <= x1 ) ) && ( ( y >= y1 && y <= y2 ) || ( y >= y2
      && y <= y1 ) ) ) ) {
      return false;
    }

    double angle_line = Math.atan2( y2 - y1, x2 - x1 ) + Math.PI;
    double angle_point = Math.atan2( y - y1, x - x1 ) + Math.PI;

    // Same angle, or close enough?
    if ( angle_point >= angle_line - 0.01 && angle_point <= angle_line + 0.01 ) {
      return true;
    }

    return false;
  }

  private SnapAllignDistribute createSnapAllignDistribute() {
    List<StepMeta> selection = transMeta.getSelectedSteps();
    int[] indices = transMeta.getStepIndexes( selection );

    return new SnapAllignDistribute( transMeta, selection, indices, hopUi.undoDelegate, this );
  }

  public void snaptogrid() {
    snaptogrid( ConstUI.GRID_SIZE );
  }

  private void snaptogrid( int size ) {
    createSnapAllignDistribute().snaptogrid( size );
  }

  @GuiToolbarElement(
    id = TOOLBAR_ITEM_ALIGN_LEFT,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "Left-align selected steps",
    toolTip = "Align the steps with the left-most step in your selection",
    image = "ui/images/toolbar/align-left.svg",
    disabledImage = "ui/images/toolbar/align-left-disabled.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  public void alignLeft() {
    createSnapAllignDistribute().allignleft();
  }

  @GuiToolbarElement(
    id = TOOLBAR_ITEM_ALIGN_RIGHT,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "Right-align selected steps",
    toolTip = "Align the steps with the right-most step in your selection",
    image = "ui/images/toolbar/align-right.svg",
    disabledImage = "ui/images/toolbar/align-right-disabled.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  public void alignRight() {
    createSnapAllignDistribute().allignright();
  }

  @GuiToolbarElement(
    id = TOOLBAR_ITEM_ALIGN_TOP,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "Top-align selected steps",
    toolTip = "Align the steps with the top-most step in your selection",
    image = "ui/images/toolbar/align-top.svg",
    disabledImage = "ui/images/toolbar/align-top-disabled.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  public void alignTop() {
    createSnapAllignDistribute().alligntop();
  }

  @GuiToolbarElement(
    id = TOOLBAR_ITEM_ALIGN_BOTTOM,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "Bottom-align selected steps",
    toolTip = "Align the steps with the bottom-most step in your selection",
    image = "ui/images/toolbar/align-bottom.svg",
    disabledImage = "ui/images/toolbar/align-bottom-disabled.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  public void alignBottom() {
    createSnapAllignDistribute().allignbottom();
  }

  @GuiToolbarElement(
    id = TOOLBAR_ITEM_DISTRIBUTE_HORIZONTALLY,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "Horizontally distribute selected steps",
    toolTip = "Distribute the selected steps evenly between the left-most and right-most step in your selection",
    image = "ui/images/toolbar/distribute-horizontally.svg",
    disabledImage = "ui/images/toolbar/distribute-horizontally-disabled.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  public void distributeHorizontal() {
    createSnapAllignDistribute().distributehorizontal();
  }

  @GuiToolbarElement(
    id = TOOLBAR_ITEM_DISTRIBUTE_VERTICALLY,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "Vertically distribute selected steps",
    toolTip = "Distribute the selected steps evenly between the top-most and bottom-most step in your selection",
    image = "ui/images/toolbar/distribute-vertically.svg",
    disabledImage = "ui/images/toolbar/distribute-vertically-disabled.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  ) public void distributeVertical() {
    createSnapAllignDistribute().distributevertical();
  }

  private void detach( StepMeta stepMeta ) {

    for ( int i = transMeta.nrTransHops() - 1; i >= 0; i-- ) {
      TransHopMeta hop = transMeta.getTransHop( i );
      if ( stepMeta.equals( hop.getFromStep() ) || stepMeta.equals( hop.getToStep() ) ) {
        // Step is connected with a hop, remove this hop.
        //
        hopUi.undoDelegate.addUndoNew( transMeta, new TransHopMeta[] { hop }, new int[] { i } );
        transMeta.removeTransHop( i );
      }
    }

    redraw();
  }

  @GuiToolbarElement(
    type = GuiElementType.TOOLBAR_BUTTON,
    id = TOOLBAR_ITEM_PREVIEW,
    label = "Preview",
    toolTip = "Preview the transformation",
    image = "ui/images/preview.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  @Override
  public void preview() {
    try {
      transRunDelegate.executeTransformation( hopUi.getLog(), transMeta, true, false, false, true, false, true,
        transRunDelegate.getTransPreviewExecutionConfiguration().getLogLevel() );
    } catch ( Exception e ) {
      new ErrorDialog( hopUi.getShell(), "Error", "Error previewing transformation", e );
    }
  }

  @GuiToolbarElement(
    type = GuiElementType.TOOLBAR_BUTTON,
    id = TOOLBAR_ITEM_DEBUG,
    label = "Debug",
    toolTip = "Debug the transformation",
    image = "ui/images/debug.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  @Override
  public void debug() {
    try {
      transRunDelegate.executeTransformation( hopUi.getLog(), transMeta, true, false, false, false, true, true,
        transRunDelegate.getTransDebugExecutionConfiguration().getLogLevel() );
    } catch ( Exception e ) {
      new ErrorDialog( hopUi.getShell(), "Error", "Error debugging transformation", e );
    }
  }

  public void newProps() {
    iconsize = hopUi.getProps().getIconSize();
  }

  public EngineMetaInterface getMeta() {
    return transMeta;
  }

  /**
   * @param transMeta the transMeta to set
   * @return the transMeta / public TransMeta getTransMeta() { return transMeta; }
   * <p/>
   * /**
   */
  public void setTransMeta( TransMeta transMeta ) {
    this.transMeta = transMeta;
  }


  @Override public void setFilename( String filename ) {
    transMeta.setFilename( filename );
  }

  @Override public String getFilename() {
    return transMeta.getFilename();
  }

  public boolean applyChanges() throws HopException {
    // return hopUi.saveToFile( transMeta ); TODO: Make file saving generic
    return false;
  }

  public boolean canBeClosed() {
    return !transMeta.hasChanged();
  }

  public TransMeta getManagedObject() {
    return transMeta;
  }

  public boolean hasContentChanged() {
    return transMeta.hasChanged();
  }

  public List<CheckResultInterface> getRemarks() {
    return remarks;
  }

  public void setRemarks( List<CheckResultInterface> remarks ) {
    this.remarks = remarks;
  }

  public List<DatabaseImpact> getImpact() {
    return impact;
  }

  public void setImpact( List<DatabaseImpact> impact ) {
    this.impact = impact;
  }

  public boolean isImpactFinished() {
    return impactFinished;
  }

  public void setImpactFinished( boolean impactHasRun ) {
    this.impactFinished = impactHasRun;
  }

  /**
   * @return the lastMove
   */
  public Point getLastMove() {
    return lastMove;
  }

  public static boolean editProperties( TransMeta transMeta, HopGui hopUi, boolean allowDirectoryChange ) {
    return editProperties( transMeta, hopUi, allowDirectoryChange, null );

  }

  public static boolean editProperties( TransMeta transMeta, HopGui hopUi, boolean allowDirectoryChange,
                                        TransDialog.Tabs currentTab ) {
    if ( transMeta == null ) {
      return false;
    }

    TransDialog tid = new TransDialog( hopUi.getShell(), SWT.NONE, transMeta, currentTab );
    tid.setDirectoryChangeAllowed( allowDirectoryChange );
    TransMeta ti = tid.open();

    hopUi.setShellText();
    return ti != null;
  }

  @Override public boolean hasChanged() {
    return transMeta.hasChanged();
  }

  @Override
  public void save() throws HopException {
    String filename = transMeta.getFilename();
    try {
      if ( StringUtils.isEmpty( filename ) ) {
        throw new HopException( "Please give the transformation a filename" );
      }
      String xml = transMeta.getXML();
      OutputStream out = HopVFS.getOutputStream( transMeta.getFilename(), false );
      try {
        out.write( XMLHandler.getXMLHeader( Const.XML_ENCODING ).getBytes( Const.XML_ENCODING ) );
        out.write( xml.getBytes( Const.XML_ENCODING ) );
        transMeta.clearChanged();
        redraw();
      } finally {
        out.flush();
        out.close();
      }
    } catch ( Exception e ) {
      throw new HopException( "Error saving transformation to file '" + filename + "'", e );
    }
  }

  @Override
  public void saveAs( String filename ) throws HopException {
    transMeta.setFilename( filename );
    save();
  }

  @GuiToolbarElement(
    type = GuiElementType.TOOLBAR_BUTTON,
    id = "HopGuiTransGraph-ToolBar-10060-Print",
    label = "Print",
    toolTip = "Print this transformation",
    image = "ui/images/print.svg",
    separator = true,
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  @Override
  public void print() {
    PrintSpool ps = new PrintSpool();
    Printer printer = ps.getPrinter( shell );

    // Create an image of the screen
    Point max = transMeta.getMaximum();
    Image img = getTransformationImage( printer, max.x, max.y, 1.0f );
    ps.printImage( shell, img );

    img.dispose();
    ps.dispose();
  }

  /**
   * This is an alias for the File/Close menu item.
   *
   * @GuiToolbarElement( type = GuiElementType.TOOLBAR_BUTTON,
   * id = "HopGuiTransGraph-ToolBar-10080-Close",
   * label = "Close",
   * toolTip = "Close this transformation",
   * image = "ui/images/toolbar/close.svg",
   * separator = true,
   * parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
   * )
   */
  public void close() {
    hopUi.menuFileClose();
  }

  @Override public boolean isCloseable() {
    try {
      // Check if the file is saved. If not, ask for it to be saved.
      //
      if ( transMeta.hasChanged() ) {

        MessageBox messageDialog = new MessageBox( shell, SWT.ICON_QUESTION | SWT.YES | SWT.NO | SWT.CANCEL );
        messageDialog.setText( "Save file?" );
        messageDialog.setMessage( "Do you want to save file '" + buildTabName() + "' before closing?" );
        int answer = messageDialog.open();
        if ( ( answer & SWT.YES ) != 0 ) {
          save();
          return true;
        }
        if ( ( answer & SWT.NO ) != 0 ) {
          // User doesn't want to save but close
          return true;
        }
        return false;
      } else {
        return true;
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopUi.getShell(), "Error", "Error preparing file close", e );
    }
    return false;
  }

  @GuiToolbarElement(
    type = GuiElementType.TOOLBAR_BUTTON,
    id = TOOLBAR_ITEM_START,
    label = "Start",
    toolTip = "Start the execution of the transformation",
    image = "ui/images/toolbar/run.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  @Override
  public void start() {
    transMeta.setShowDialog( transMeta.isAlwaysShowRunOptions() );
    Thread thread = new Thread() {
      @Override
      public void run() {
        getDisplay().asyncExec( new Runnable() {
          @Override
          public void run() {
            try {
              transRunDelegate.executeTransformation( hopUi.getLog(), transMeta, true, false, false, false, debug, false, LogLevel.BASIC );
            } catch ( Exception e ) {
              new ErrorDialog(
                shell, "Execute transformation", "There was an error during transformation execution", e );
            }
          }
        } );
      }
    };
    thread.start();
  }

  @Override
  @GuiToolbarElement(
    id = TOOLBAR_ITEM_PAUSE,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "Pause",
    toolTip = "Pause the execution of the transformation",
    image = "ui/images/toolbar/pause.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  public void pause() {
    pauseResume();
  }

  /* TODO: re-introduce
  public void checkTrans() {
    hopUi.checkTrans();
  }
  */

  /** TODO: re-introduce
   public void analyseImpact() {
   hopUi.analyseImpact();
   }
   */

  /**
   * TODO: re-introduce
   * public void getSQL() {
   * hopUi.getSQL();
   * }
   */

   /* TODO: re-introduce
  public void exploreDatabase() {
    hopUi.exploreDatabase();
  }
   */
  public boolean isExecutionResultsPaneVisible() {
    return extraViewComposite != null && !extraViewComposite.isDisposed();
  }

  @GuiToolbarElement(
    id = TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "Spoon.Menu.ShowExecutionResults",
    toolTip = "Spoon.Tooltip.ShowExecutionResults",
    i18nPackageClass = HopUi.class,
    image = "ui/images/show-results.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    separator = true
  ) public void showExecutionResults() {
    ToolItem item = toolBarWidgets.findToolItem( TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS );
    if ( isExecutionResultsPaneVisible() ) {
      disposeExtraView();
      item.setImage( GUIResource.getInstance().getImageShowResults() );
      item.setToolTipText( BaseMessages.getString( PKG, "Spoon.Tooltip.ShowExecutionResults" ) );
    } else {
      addAllTabs();
      item.setImage( GUIResource.getInstance().getImageHideResults() );
      item.setToolTipText( BaseMessages.getString( PKG, "Spoon.Tooltip.HideExecutionResults" ) );
    }
  }

  /**
   * If the extra tab view at the bottom is empty, we close it.
   */
  public void checkEmptyExtraView() {
    if ( extraViewTabFolder.getItemCount() == 0 ) {
      disposeExtraView();
    }
  }

  private void disposeExtraView() {

    extraViewComposite.dispose();
    sashForm.layout();
    sashForm.setWeights( new int[] { 100, } );

    // TODO: implement enable/disable toolbar icons using GuiWidgetElements
    //
    // XulToolbarbutton button = (XulToolbarbutton) toolBar.getElementById( "trans-show-results" );
    // button.setTooltiptext( BaseMessages.getString( PKG, "Spoon.Tooltip.ShowExecutionResults" ) );
    // ToolItem toolItem = (ToolItem) button.getManagedObject();
    //toolItem.setImage( GUIResource.getInstance().getImageShowResults() );
  }

  private void minMaxExtraView() {
    // What is the state?
    //
    boolean maximized = sashForm.getMaximizedControl() != null;
    if ( maximized ) {
      // Minimize
      //
      sashForm.setMaximizedControl( null );
      minMaxButton.setImage( GUIResource.getInstance().getImageMaximizePanel() );
      minMaxButton.setToolTipText( BaseMessages.getString( PKG, "TransGraph.ExecutionResultsPanel.MaxButton.Tooltip" ) );
    } else {
      // Maximize
      //
      sashForm.setMaximizedControl( extraViewComposite );
      minMaxButton.setImage( GUIResource.getInstance().getImageMinimizePanel() );
      minMaxButton.setToolTipText( BaseMessages.getString( PKG, "TransGraph.ExecutionResultsPanel.MinButton.Tooltip" ) );
    }
  }

  /**
   * @return the toolbar
   */
  public ToolBar getToolBar() {
    return toolBar;
  }

  /**
   * @param toolBar the toolbar to set
   */
  public void setToolBar( ToolBar toolBar ) {
    this.toolBar = toolBar;
  }

  private Label closeButton;

  private Label minMaxButton;

  /**
   * Add an extra view to the main composite SashForm
   */
  public void addExtraView() {
    PropsUI props = PropsUI.getInstance();

    extraViewComposite = new Composite( sashForm, SWT.NONE );
    FormLayout extraCompositeFormLayout = new FormLayout();
    extraCompositeFormLayout.marginWidth = 2;
    extraCompositeFormLayout.marginHeight = 2;
    extraViewComposite.setLayout( extraCompositeFormLayout );

    // Put a close and max button to the upper right corner...
    //
    closeButton = new Label( extraViewComposite, SWT.NONE );
    closeButton.setImage( GUIResource.getInstance().getImageClosePanel() );
    closeButton.setToolTipText( BaseMessages.getString( PKG, "TransGraph.ExecutionResultsPanel.CloseButton.Tooltip" ) );
    FormData fdClose = new FormData();
    fdClose.right = new FormAttachment( 100, 0 );
    fdClose.top = new FormAttachment( 0, 0 );
    closeButton.setLayoutData( fdClose );
    closeButton.addMouseListener( new MouseAdapter() {
      @Override
      public void mouseDown( MouseEvent e ) {
        disposeExtraView();
      }
    } );

    minMaxButton = new Label( extraViewComposite, SWT.NONE );
    minMaxButton.setImage( GUIResource.getInstance().getImageMaximizePanel() );
    minMaxButton.setToolTipText( BaseMessages.getString( PKG, "TransGraph.ExecutionResultsPanel.MaxButton.Tooltip" ) );
    FormData fdMinMax = new FormData();
    fdMinMax.right = new FormAttachment( closeButton, -props.getMargin() );
    fdMinMax.top = new FormAttachment( 0, 0 );
    minMaxButton.setLayoutData( fdMinMax );
    minMaxButton.addMouseListener( new MouseAdapter() {
      @Override
      public void mouseDown( MouseEvent e ) {
        minMaxExtraView();
      }
    } );

    // Add a label at the top: Results
    //
    Label wResultsLabel = new Label( extraViewComposite, SWT.LEFT );
    wResultsLabel.setFont( GUIResource.getInstance().getFontLarge() );
    wResultsLabel.setBackground( GUIResource.getInstance().getColorWhite() );
    wResultsLabel.setText( BaseMessages.getString( PKG, "TransLog.ResultsPanel.NameLabel" ) );
    FormData fdResultsLabel = new FormData();
    fdResultsLabel.left = new FormAttachment( 0, 0 );
    fdResultsLabel.right = new FormAttachment( minMaxButton, -props.getMargin() );
    fdResultsLabel.top = new FormAttachment( 0, 0 );
    wResultsLabel.setLayoutData( fdResultsLabel );

    // Add a tab folder ...
    //
    extraViewTabFolder = new CTabFolder( extraViewComposite, SWT.MULTI );
    hopUi.getProps().setLook( extraViewTabFolder, Props.WIDGET_STYLE_TAB );

    extraViewTabFolder.addMouseListener( new MouseAdapter() {

      @Override
      public void mouseDoubleClick( MouseEvent arg0 ) {
        if ( sashForm.getMaximizedControl() == null ) {
          sashForm.setMaximizedControl( extraViewComposite );
        } else {
          sashForm.setMaximizedControl( null );
        }
      }

    } );

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment( 0, 0 );
    fdTabFolder.right = new FormAttachment( 100, 0 );
    fdTabFolder.top = new FormAttachment( wResultsLabel, props.getMargin() );
    fdTabFolder.bottom = new FormAttachment( 100, 0 );
    extraViewTabFolder.setLayoutData( fdTabFolder );

    sashForm.setWeights( new int[] { 60, 40, } );
  }

  public synchronized void start( TransExecutionConfiguration executionConfiguration ) throws HopException {
    // Auto save feature...
    handleTransMetaChanges( transMeta );

    // filename set & not changed?
    //
    if ( StringUtils.isNotEmpty( transMeta.getFilename() ) && !transMeta.hasChanged() ) {
      if ( trans == null || !running ) {
        try {
          // Set the requested logging level..
          //
          DefaultLogLevel.setLogLevel( executionConfiguration.getLogLevel() );

          transMeta.injectVariables( executionConfiguration.getVariables() );

          // Set the named parameters
          Map<String, String> paramMap = executionConfiguration.getParams();
          Set<String> keys = paramMap.keySet();
          for ( String key : keys ) {
            transMeta.setParameterValue( key, Const.NVL( paramMap.get( key ), "" ) );
          }

          transMeta.activateParameters();

          // Do we need to clear the log before running?
          //
          // if ( executionConfiguration.isClearingLog() ) {
          //  transLogDelegate.clearLog();
          // }

          // Also make sure to clear the log entries in the central log store & registry
          //
          if ( trans != null ) {
            HopLogStore.discardLines( trans.getLogChannelId(), true );
          }

          // Important: even though transMeta is passed to the Trans constructor, it is not the same object as is in
          // memory
          // To be able to completely test this, we need to run it as we would normally do in pan
          //
          trans = new Trans( transMeta );

          trans.setMetaStore( hopUi.getMetaStore() );

          String spoonLogObjectId = UUID.randomUUID().toString();
          SimpleLoggingObject spoonLoggingObject = new SimpleLoggingObject( "HOPUI", LoggingObjectType.HOPUI, null );
          spoonLoggingObject.setContainerObjectId( spoonLogObjectId );
          spoonLoggingObject.setLogLevel( executionConfiguration.getLogLevel() );
          trans.setParent( spoonLoggingObject );

          trans.setLogLevel( executionConfiguration.getLogLevel() );
          trans.setMonitored( true );
          log.logBasic( BaseMessages.getString( PKG, "TransLog.Log.TransformationOpened" ) );
        } catch ( HopException e ) {
          trans = null;
          new ErrorDialog( shell, BaseMessages.getString( PKG, "TransLog.Dialog.ErrorOpeningTransformation.Title" ),
            BaseMessages.getString( PKG, "TransLog.Dialog.ErrorOpeningTransformation.Message" ), e );
        }
        if ( trans != null ) {
          log.logMinimal( BaseMessages.getString( PKG, "TransLog.Log.LaunchingTransformation" ) + trans.getTransMeta().getName() + "]..." );

          trans.setSafeModeEnabled( executionConfiguration.isSafeModeEnabled() );
          trans.setGatheringMetrics( executionConfiguration.isGatheringMetrics() );

          // Launch the step preparation in a different thread.
          // That way Spoon doesn't block anymore and that way we can follow the progress of the initialization
          //
          final Thread parentThread = Thread.currentThread();

          shell.getDisplay().asyncExec( new Runnable() {
            @Override
            public void run() {
              addAllTabs();
              prepareTrans( parentThread );
            }
          } );

          log.logMinimal( BaseMessages.getString( PKG, "TransLog.Log.StartedExecutionOfTransformation" ) );

          setControlStates();
        }
      } else {
        modalMessageDialog( getString( "TransLog.Dialog.DoNoStartTransformationTwice.Title" ),
          getString( "TransLog.Dialog.DoNoStartTransformationTwice.Message" ), SWT.OK | SWT.ICON_WARNING );
      }
    } else {
      if ( transMeta.hasChanged() ) {
        showSaveFileMessage();
      }

/*       else if ( transMeta.getName() == null ) {
        modalMessageDialog( getString( "TransLog.Dialog.GiveTransformationANameBeforeRunning.Title" ),
          getString( "TransLog.Dialog.GiveTransformationANameBeforeRunning.Message" ), SWT.OK | SWT.ICON_WARNING );
      } else {
        modalMessageDialog( getString( "TransLog.Dialog.SaveTransformationBeforeRunning2.Title" ),
          getString( "TransLog.Dialog.SaveTransformationBeforeRunning2.Message" ), SWT.OK | SWT.ICON_WARNING );
      }
 */

    }
  }

  public void showSaveFileMessage() {
    modalMessageDialog( getString( "TransLog.Dialog.SaveTransformationBeforeRunning.Title" ),
      getString( "TransLog.Dialog.SaveTransformationBeforeRunning.Message" ), SWT.OK | SWT.ICON_WARNING );
  }

  public void addAllTabs() {

    CTabItem tabItemSelection = null;
    if ( extraViewTabFolder != null && !extraViewTabFolder.isDisposed() ) {
      tabItemSelection = extraViewTabFolder.getSelection();
    }

    transLogDelegate.addTransLog();
    transGridDelegate.addTransGrid();
    transPerfDelegate.addTransPerf();
    transMetricsDelegate.addTransMetrics();
    transPreviewDelegate.addTransPreview();

    List<HopUiExtenderPluginInterface> relevantExtenders = HopUiExtenderPluginType.getInstance().getRelevantExtenders( HopGuiTransGraph.class, LOAD_TAB );
    for ( HopUiExtenderPluginInterface relevantExtender : relevantExtenders ) {
      relevantExtender.uiEvent( this, LOAD_TAB );
    }

    if ( tabItemSelection != null ) {
      extraViewTabFolder.setSelection( tabItemSelection );
    } else {
      extraViewTabFolder.setSelection( transGridDelegate.getTransGridTab() );
    }
  }

  public synchronized void debug( TransExecutionConfiguration executionConfiguration, TransDebugMeta transDebugMeta ) {
    if ( !running ) {
      try {
        this.lastTransDebugMeta = transDebugMeta;

        log.setLogLevel( executionConfiguration.getLogLevel() );
        if ( log.isDetailed() ) {
          log.logDetailed( BaseMessages.getString( PKG, "TransLog.Log.DoPreview" ) );
        }
        transMeta.injectVariables( executionConfiguration.getVariables() );

        // Set the named parameters
        Map<String, String> paramMap = executionConfiguration.getParams();
        Set<String> keys = paramMap.keySet();
        for ( String key : keys ) {
          transMeta.setParameterValue( key, Const.NVL( paramMap.get( key ), "" ) );
        }

        transMeta.activateParameters();

        // Do we need to clear the log before running?
        //
        if ( executionConfiguration.isClearingLog() ) {
          transLogDelegate.clearLog();
        }

        // Do we have a previous execution to clean up in the logging registry?
        //
        if ( trans != null ) {
          HopLogStore.discardLines( trans.getLogChannelId(), false );
          LoggingRegistry.getInstance().removeIncludingChildren( trans.getLogChannelId() );
        }

        // Create a new transformation to execution
        //
        trans = new Trans( transMeta );
        trans.setSafeModeEnabled( executionConfiguration.isSafeModeEnabled() );
        trans.setPreview( true );
        trans.setGatheringMetrics( executionConfiguration.isGatheringMetrics() );
        trans.setMetaStore( hopUi.getMetaStore() );
        trans.prepareExecution();

        List<HopUiExtenderPluginInterface> relevantExtenders =
          HopUiExtenderPluginType.getInstance().getRelevantExtenders( TransDebugMetaWrapper.class, PREVIEW_TRANS );
        TransDebugMetaWrapper transDebugMetaWrapper = new TransDebugMetaWrapper( trans, transDebugMeta );
        for ( HopUiExtenderPluginInterface relevantExtender : relevantExtenders ) {
          relevantExtender.uiEvent( transDebugMetaWrapper, PREVIEW_TRANS );
        }
        // Add the row listeners to the allocated threads
        //
        transDebugMeta.addRowListenersToTransformation( trans );

        // What method should we call back when a break-point is hit?

        transDebugMeta.addBreakPointListers( new BreakPointListener() {
          @Override
          public void breakPointHit( TransDebugMeta transDebugMeta, StepDebugMeta stepDebugMeta,
                                     RowMetaInterface rowBufferMeta, List<Object[]> rowBuffer ) {
            showPreview( transDebugMeta, stepDebugMeta, rowBufferMeta, rowBuffer );
          }
        } );

        // Do we capture data?
        //
        if ( transPreviewDelegate.isActive() ) {
          transPreviewDelegate.capturePreviewData( trans, transMeta.getSteps() );
        }

        // Start the threads for the steps...
        //
        startThreads();

        debug = true;

        // Show the execution results view...
        //
        shell.getDisplay().asyncExec( new Runnable() {
          @Override
          public void run() {
            addAllTabs();
          }
        } );
      } catch ( Exception e ) {
        new ErrorDialog( shell, BaseMessages.getString( PKG, "TransLog.Dialog.UnexpectedErrorDuringPreview.Title" ),
          BaseMessages.getString( PKG, "TransLog.Dialog.UnexpectedErrorDuringPreview.Message" ), e );
      }
    } else {
      modalMessageDialog( getString( "TransLog.Dialog.DoNoPreviewWhileRunning.Title" ),
        getString( "TransLog.Dialog.DoNoPreviewWhileRunning.Message" ), SWT.OK | SWT.ICON_WARNING );
    }
    checkErrorVisuals();
  }

  public synchronized void showPreview( final TransDebugMeta transDebugMeta, final StepDebugMeta stepDebugMeta,
                                        final RowMetaInterface rowBufferMeta, final List<Object[]> rowBuffer ) {
    shell.getDisplay().asyncExec( new Runnable() {

      @Override
      public void run() {

        if ( isDisposed() ) {
          return;
        }

        // hopUi.enableMenus(); TODO Make this automatic

        // The transformation is now paused, indicate this in the log dialog...
        //
        pausing = true;

        setControlStates();
        checkErrorVisuals();

        PreviewRowsDialog previewRowsDialog =
          new PreviewRowsDialog(
            shell, transMeta, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.APPLICATION_MODAL | SWT.SHEET,
            stepDebugMeta.getStepMeta().getName(), rowBufferMeta, rowBuffer );
        previewRowsDialog.setProposingToGetMoreRows( true );
        previewRowsDialog.setProposingToStop( true );
        previewRowsDialog.open();

        if ( previewRowsDialog.isAskingForMoreRows() ) {
          // clear the row buffer.
          // That way if you click resume, you get the next N rows for the step :-)
          //
          rowBuffer.clear();

          // Resume running: find more rows...
          //
          pauseResume();
        }

        if ( previewRowsDialog.isAskingToStop() ) {
          // Stop running
          //
          stop();
        }
      }
    } );
  }

  private String[] convertArguments( Map<String, String> arguments ) {
    String[] argumentNames = arguments.keySet().toArray( new String[ arguments.size() ] );
    Arrays.sort( argumentNames );

    String[] args = new String[ argumentNames.length ];
    for ( int i = 0; i < args.length; i++ ) {
      String argumentName = argumentNames[ i ];
      args[ i ] = arguments.get( argumentName );
    }
    return args;
  }

  @GuiToolbarElement(
    id = TOOLBAR_ITEM_STOP,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "Stop",
    toolTip = "Stop the execution of the transformation",
    image = "ui/images/toolbar/stop.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  @Override
  public void stop() {
    if ( safeStopping ) {
      modalMessageDialog( getString( "TransLog.Log.SafeStopAlreadyStarted.Title" ),
        getString( "TransLog.Log.SafeStopAlreadyStarted" ), SWT.ICON_ERROR | SWT.OK );
      return;
    }
    if ( ( running && !halting ) ) {
      halting = true;
      trans.stopAll();
      log.logMinimal( BaseMessages.getString( PKG, "TransLog.Log.ProcessingOfTransformationStopped" ) );

      running = false;
      initialized = false;
      halted = false;
      halting = false;

      setControlStates();

      transMeta.setInternalHopVariables(); // set the original vars back as they may be changed by a mapping
    }
  }

  public void safeStop() {
    if ( running && !halting ) {
      halting = true;
      safeStopping = true;
      trans.safeStop();
      log.logMinimal( BaseMessages.getString( PKG, "TransLog.Log.TransformationSafeStopped" ) );

      initialized = false;
      halted = false;

      setControlStates();

      transMeta.setInternalHopVariables(); // set the original vars back as they may be changed by a mapping
    }
  }

  public synchronized void pauseResume() {
    if ( running ) {
      // Get the pause toolbar item
      //
      if ( !pausing ) {
        pausing = true;
        trans.pauseRunning();
        setControlStates();
      } else {
        pausing = false;
        trans.resumeRunning();
        setControlStates();
      }
    }
  }

  private boolean controlDisposed( XulToolbarbutton button ) {
    if ( button.getManagedObject() instanceof Widget ) {
      Widget widget = (Widget) button.getManagedObject();
      return widget.isDisposed();
    }
    return false;
  }

  public synchronized void setControlStates() {
    /** TODO: implement through GuiElement
     *
     if ( isDisposed() || getDisplay().isDisposed() ) {
     return;
     }
     if ( ( (Control) toolBar.getManagedObject() ).isDisposed() ) {
     return;
     }

     getDisplay().asyncExec( new Runnable() {

    @Override public void run() {

    // Start/Run button...
    //
    XulToolbarbutton runButton = (XulToolbarbutton) toolBar.getElementById( "trans-run" );
    if ( runButton != null && !controlDisposed( runButton ) ) {
    if ( runButton.isDisabled() ^ running ) {
    runButton.setDisabled( running );
    }
    }

    // Pause button...
    //
    XulToolbarbutton pauseButton = (XulToolbarbutton) toolBar.getElementById( "trans-pause" );
    if ( pauseButton != null && !controlDisposed( pauseButton ) ) {
    if ( pauseButton.isDisabled() ^ !running ) {
    pauseButton.setDisabled( !running );
    pauseButton.setLabel( pausing ? RESUME_TEXT : PAUSE_TEXT );
    pauseButton.setTooltiptext( pausing ? BaseMessages.getString( PKG, "Spoon.Tooltip.ResumeTranformation" )
    : BaseMessages.getString( PKG, "Spoon.Tooltip.PauseTranformation" ) );
    }
    }

    // Stop button...
    //
    if ( !stopItem.isDisposed() && !stopItem.isEnabled() ^ !running ) {
    stopItem.setEnabled( running );
    }

    // Debug button...
    //
    XulToolbarbutton debugButton = (XulToolbarbutton) toolBar.getElementById( "trans-debug" );

    if ( debugButton != null && !controlDisposed( debugButton ) ) {
    if ( debugButton.isDisabled() ^ running ) {
    debugButton.setDisabled( running );
    }
    }

    // Preview button...
    //
    XulToolbarbutton previewButton = (XulToolbarbutton) toolBar.getElementById( "trans-preview" );
    if ( previewButton != null && !controlDisposed( previewButton ) ) {
    if ( previewButton.isDisabled() ^ running ) {
    previewButton.setDisabled( running );
    }
    }
    }

    } );

     */
  }

  private synchronized void prepareTrans( final Thread parentThread ) {
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        try {
          trans.prepareExecution();

          // Do we capture data?
          //
          if ( transPreviewDelegate.isActive() ) {
            transPreviewDelegate.capturePreviewData( trans, transMeta.getSteps() );
          }

          initialized = true;
        } catch ( HopException e ) {
          log.logError( trans.getName() + ": preparing transformation execution failed", e );
          checkErrorVisuals();
        }
        halted = trans.hasHaltedSteps();
        if ( trans.isReadyToStart() ) {
          checkStartThreads(); // After init, launch the threads.
        } else {
          initialized = false;
          running = false;
          checkErrorVisuals();
        }
      }
    };
    Thread thread = new Thread( runnable );
    thread.start();
  }

  private void checkStartThreads() {
    if ( initialized && !running && trans != null ) {
      startThreads();
    }
  }

  private synchronized void startThreads() {
    running = true;
    try {
      // Add a listener to the transformation.
      // If the transformation is done, we want to do the end processing, etc.
      //
      trans.addTransListener( new ExecutionAdapter<TransMeta>() {

        @Override
        public void finished( IEngine<TransMeta> trans ) {
          checkTransEnded();
          checkErrorVisuals();
          stopRedrawTimer();

          transMetricsDelegate.resetLastRefreshTime();
          transMetricsDelegate.updateGraph();
        }
      } );

      trans.startThreads();
      startRedrawTimer();

      setControlStates();
    } catch ( HopException e ) {
      log.logError( "Error starting step threads", e );
      checkErrorVisuals();
      stopRedrawTimer();
    }

    // See if we have to fire off the performance graph updater etc.
    //
    getDisplay().asyncExec( new Runnable() {
      @Override
      public void run() {
        if ( transPerfDelegate.getTransPerfTab() != null ) {
          // If there is a tab open, try to the correct content on there now
          //
          transPerfDelegate.setupContent();
          transPerfDelegate.layoutPerfComposite();
        }
      }
    } );
  }

  private void startRedrawTimer() {

    redrawTimer = new Timer( "HopGuiTransGraph: redraw timer" );
    TimerTask timtask = new TimerTask() {
      @Override
      public void run() {
        if ( !hopUi.getDisplay().isDisposed() ) {
          hopUi.getDisplay().asyncExec( new Runnable() {
            @Override
            public void run() {
              if ( !HopGuiTransGraph.this.canvas.isDisposed() ) {
                HopGuiTransGraph.this.canvas.redraw();
              }
            }
          } );
        }
      }
    };

    redrawTimer.schedule( timtask, 0L, ConstUI.INTERVAL_MS_TRANS_CANVAS_REFRESH );

  }

  protected void stopRedrawTimer() {
    if ( redrawTimer != null ) {
      redrawTimer.cancel();
      redrawTimer.purge();
      redrawTimer = null;
    }

  }

  private void checkTransEnded() {
    if ( trans != null ) {
      if ( trans.isFinished() && ( running || halted ) ) {
        log.logMinimal( BaseMessages.getString( PKG, "TransLog.Log.TransformationHasFinished" ) );

        running = false;
        initialized = false;
        halted = false;
        halting = false;
        safeStopping = false;

        setControlStates();

        // OK, also see if we had a debugging session going on.
        // If so and we didn't hit a breakpoint yet, display the show
        // preview dialog...
        //
        if ( debug && lastTransDebugMeta != null && lastTransDebugMeta.getTotalNumberOfHits() == 0 ) {
          debug = false;
          showLastPreviewResults();
        }
        debug = false;

        checkErrorVisuals();

        shell.getDisplay().asyncExec( new Runnable() {
          @Override
          public void run() {
            // hopUi.fireMenuControlers();
            redraw();
          }
        } );
      }
    }
  }

  private void checkErrorVisuals() {
    if ( trans.getErrors() > 0 ) {
      // Get the logging text and filter it out. Store it in the stepLogMap...
      //
      stepLogMap = new HashMap<>();
      shell.getDisplay().syncExec( new Runnable() {

        @Override
        public void run() {

          for ( StepMetaDataCombi combi : trans.getSteps() ) {
            if ( combi.step.getErrors() > 0 ) {
              String channelId = combi.step.getLogChannel().getLogChannelId();
              List<HopLoggingEvent> eventList =
                HopLogStore.getLogBufferFromTo( channelId, false, 0, HopLogStore.getLastBufferLineNr() );
              StringBuilder logText = new StringBuilder();
              for ( HopLoggingEvent event : eventList ) {
                Object message = event.getMessage();
                if ( message instanceof LogMessage ) {
                  LogMessage logMessage = (LogMessage) message;
                  if ( logMessage.isError() ) {
                    logText.append( logMessage.getMessage() ).append( Const.CR );
                  }
                }
              }
              stepLogMap.put( combi.stepMeta, logText.toString() );
            }
          }
        }
      } );

    } else {
      stepLogMap = null;
    }
    // Redraw the canvas to show the error icons etc.
    //
    shell.getDisplay().asyncExec( new Runnable() {
      @Override
      public void run() {
        redraw();
      }
    } );
  }

  public synchronized void showLastPreviewResults() {
    if ( lastTransDebugMeta == null || lastTransDebugMeta.getStepDebugMetaMap().isEmpty() ) {
      return;
    }

    final List<String> stepnames = new ArrayList<>();
    final List<RowMetaInterface> rowMetas = new ArrayList<>();
    final List<List<Object[]>> rowBuffers = new ArrayList<>();

    // Assemble the buffers etc in the old style...
    //
    for ( StepMeta stepMeta : lastTransDebugMeta.getStepDebugMetaMap().keySet() ) {
      StepDebugMeta stepDebugMeta = lastTransDebugMeta.getStepDebugMetaMap().get( stepMeta );

      stepnames.add( stepMeta.getName() );
      rowMetas.add( stepDebugMeta.getRowBufferMeta() );
      rowBuffers.add( stepDebugMeta.getRowBuffer() );
    }

    getDisplay().asyncExec( new Runnable() {
      @Override
      public void run() {
        EnterPreviewRowsDialog dialog = new EnterPreviewRowsDialog( shell, SWT.NONE, stepnames, rowMetas, rowBuffers );
        dialog.open();
      }
    } );
  }

  /**
   * Finds the last active transformation in the running job to the opened transMeta
   *
   * @param transGraph
   * @param stepMeta
   */
  private void attachActiveTrans( HopGuiTransGraph transGraph, StepMeta stepMeta ) {
    if ( trans != null && transGraph != null ) {
      Trans subTransformation = trans.getActiveSubTransformation( stepMeta.getName() );
      transGraph.setTrans( subTransformation );
      if ( !transGraph.isExecutionResultsPaneVisible() ) {
        transGraph.showExecutionResults();
      }
      transGraph.setControlStates();
    }
  }

  /**
   * Finds the last active transformation in the running job to the opened transMeta
   *
   * @param transGraph
   * @param stepMeta
   */
  private Trans getActiveSubtransformation( HopGuiTransGraph transGraph, StepMeta stepMeta ) {
    if ( trans != null && transGraph != null ) {
      return trans.getActiveSubTransformation( stepMeta.getName() );
    }
    return null;
  }

  /**
   * Finds the last active job in the running transformation to the opened jobMeta
   *
   * @param jobGraph
   * @param stepMeta
   */
  private void attachActiveJob( JobGraph jobGraph, StepMeta stepMeta ) {
    if ( trans != null && jobGraph != null ) {
      Job subJob = trans.getActiveSubjobs().get( stepMeta.getName() );
      jobGraph.setJob( subJob );
      if ( !jobGraph.isExecutionResultsPaneVisible() ) {
        jobGraph.showExecutionResults();
      }
      jobGraph.setControlStates();
    }
  }

  /**
   * @return the running
   */
  public boolean isRunning() {
    return running;
  }

  /**
   * @param running the running to set
   */
  public void setRunning( boolean running ) {
    this.running = running;
  }

  /**
   * @return the lastTransDebugMeta
   */
  public TransDebugMeta getLastTransDebugMeta() {
    return lastTransDebugMeta;
  }

  /**
   * @return the halting
   */
  public boolean isHalting() {
    return halting;
  }

  /**
   * @param halting the halting to set
   */
  public void setHalting( boolean halting ) {
    this.halting = halting;
  }

  /**
   * @return the stepLogMap
   */
  public Map<StepMeta, String> getStepLogMap() {
    return stepLogMap;
  }

  /**
   * @param stepLogMap the stepLogMap to set
   */
  public void setStepLogMap( Map<StepMeta, String> stepLogMap ) {
    this.stepLogMap = stepLogMap;
  }

  @Override
  public HasLogChannelInterface getLogChannelProvider() {
    return new HasLogChannelInterface() {
      @Override
      public LogChannelInterface getLogChannel() {
        return getTrans() != null ? getTrans().getLogChannel() : getTransMeta().getLogChannel();
      }
    };
  }

  public synchronized void setTrans( Trans trans ) {
    this.trans = trans;
    if ( trans != null ) {
      pausing = trans.isPaused();
      initialized = trans.isInitializing();
      running = trans.isRunning();
      halted = trans.isStopped();

      if ( running ) {
        trans.addTransListener( new ExecutionAdapter<TransMeta>() {

          @Override
          public void finished( IEngine<TransMeta> trans ) {
            checkTransEnded();
            checkErrorVisuals();
          }
        } );
      }
    }
  }

  public void sniffInput() {
    sniff( true, false, false );
  }

  public void sniffOutput() {
    sniff( false, true, false );
  }

  public void sniffError() {
    sniff( false, false, true );
  }

  public void sniff( final boolean input, final boolean output, final boolean error ) {
    StepMeta stepMeta = getCurrentStep();
    if ( stepMeta == null || trans == null ) {
      return;
    }
    final StepInterface runThread = trans.findRunThread( stepMeta.getName() );
    if ( runThread != null ) {

      List<Object[]> rows = new ArrayList<>();

      final PreviewRowsDialog dialog = new PreviewRowsDialog( shell, trans, SWT.NONE, stepMeta.getName(), null, rows );
      dialog.setDynamic( true );

      // Add a row listener that sends the rows over to the dialog...
      //
      final RowListener rowListener = new RowListener() {

        @Override
        public void rowReadEvent( RowMetaInterface rowMeta, Object[] row ) throws HopStepException {
          if ( input ) {
            try {
              dialog.addDataRow( rowMeta, rowMeta.cloneRow( row ) );
            } catch ( HopValueException e ) {
              throw new HopStepException( e );
            }
          }
        }

        @Override
        public void rowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws HopStepException {
          if ( output ) {
            try {
              dialog.addDataRow( rowMeta, rowMeta.cloneRow( row ) );
            } catch ( HopValueException e ) {
              throw new HopStepException( e );
            }
          }
        }

        @Override
        public void errorRowWrittenEvent( RowMetaInterface rowMeta, Object[] row ) throws HopStepException {
          if ( error ) {
            try {
              dialog.addDataRow( rowMeta, rowMeta.cloneRow( row ) );
            } catch ( HopValueException e ) {
              throw new HopStepException( e );
            }
          }
        }
      };

      // When the dialog is closed, make sure to remove the listener!
      //
      dialog.addDialogClosedListener( new DialogClosedListener() {
        @Override
        public void dialogClosed() {
          runThread.removeRowListener( rowListener );
        }
      } );

      // Open the dialog in a separate thread to make sure it doesn't block
      //
      getDisplay().asyncExec( new Runnable() {

        @Override
        public void run() {
          dialog.open();
        }
      } );

      runThread.addRowListener( rowListener );
    }
  }

  @Override
  public int showChangedWarning() throws HopException {
    return showChangedWarning( transMeta.getName() );
  }

  @Override public LogChannelInterface getLogChannel() {
    return log;
  }

  /**
   * Edit the step of the given transformation
   *
   * @param transMeta
   * @param stepMeta
   */
  public void editStep( TransMeta transMeta, StepMeta stepMeta ) {
    // TODO: implement this
  }

  public String buildTabName() throws HopException {
    String tabName = null;
    String realFilename = transMeta.environmentSubstitute( transMeta.getFilename() );
    if ( StringUtils.isEmpty( realFilename ) ) {
      tabName = transMeta.getName();
    } else {
      try {
        FileObject fileObject = HopVFS.getFileObject( transMeta.getFilename() );
        FileName fileName = fileObject.getName();
        tabName = fileName.getBaseName();
      } catch ( Exception e ) {
        throw new HopException( "Unable to get information from file '" + transMeta.getFilename() + "'", e );
      }
    }
    return tabName;
  }

  public void handleTransMetaChanges( TransMeta transMeta ) throws HopException {
    if ( transMeta.hasChanged() ) {
      if ( hopUi.getProps().getAutoSave() ) {
        save();
      } else {
        MessageDialogWithToggle md =
          new MessageDialogWithToggle( shell, BaseMessages.getString( PKG, "TransLog.Dialog.FileHasChanged.Title" ),
            null, BaseMessages.getString( PKG, "TransLog.Dialog.FileHasChanged1.Message" ) + Const.CR
            + BaseMessages.getString( PKG, "TransLog.Dialog.FileHasChanged2.Message" ) + Const.CR,
            MessageDialog.QUESTION, new String[] { BaseMessages.getString( PKG, "System.Button.Yes" ),
            BaseMessages.getString( PKG, "System.Button.No" ) }, 0, BaseMessages.getString( PKG,
            "TransLog.Dialog.Option.AutoSaveTransformation" ), hopUi.getProps().getAutoSave() );
        MessageDialogWithToggle.setDefaultImage( GUIResource.getInstance().getImageHopUi() );
        int answer = md.open();
        if ( ( answer & 0xFF ) == 0 ) {
          save();
        }
        hopUi.getProps().setAutoSave( md.getToggleState() );
      }
    }
  }

  private StepMeta lastChained = null;

  public void addStepToChain( PluginInterface stepPlugin, boolean shift ) {
    // Is the lastChained entry still valid?
    //
    if ( lastChained != null && transMeta.findStep( lastChained.getName() ) == null ) {
      lastChained = null;
    }

    // If there is exactly one selected step, pick that one as last chained.
    //
    List<StepMeta> sel = transMeta.getSelectedSteps();
    if ( sel.size() == 1 ) {
      lastChained = sel.get( 0 );
    }

    // Where do we add this?

    Point p = null;
    if ( lastChained == null ) {
      p = transMeta.getMaximum();
      p.x -= 100;
    } else {
      p = new Point( lastChained.getLocation().x, lastChained.getLocation().y );
    }

    p.x += 200;

    // Which is the new step?

    StepMeta newStep = transStepDelegate.newStep( transMeta, stepPlugin.getIds()[ 0 ], stepPlugin.getName(), stepPlugin.getName(), false, true, p );
    if ( newStep == null ) {
      return;
    }
    newStep.setLocation( p.x, p.y );
    newStep.setDraw( true );

    if ( lastChained != null ) {
      TransHopMeta hop = new TransHopMeta( lastChained, newStep );
      transHopDelegate.newHop( transMeta, hop );
    }

    lastChained = newStep;
    redraw();

    if ( shift ) {
      editStep( newStep );
    }

    transMeta.unselectAll();
    newStep.setSelected( true );

  }

  public HopGui getHopUi() {
    return hopUi;
  }

  public void setHopUi( HopGui hopUi ) {
    this.hopUi = hopUi;
  }

  public TransMeta getTransMeta() {
    return transMeta;
  }

  public Trans getTrans() {
    return trans;
  }

  private void setHopEnabled( TransHopMeta hop, boolean enabled ) {
    hop.setEnabled( enabled );
    transMeta.clearCaches();
  }

  private void modalMessageDialog( String title, String message, int swtFlags ) {
    MessageBox messageBox = new MessageBox( shell, swtFlags );
    messageBox.setMessage( message );
    messageBox.setText( title );
    messageBox.open();
  }

  private String getString( String key ) {
    return BaseMessages.getString( PKG, key );
  }

  private String getString( String key, String... params ) {
    return BaseMessages.getString( PKG, key, params );
  }

  /**
   * Gets fileType
   *
   * @return value of fileType
   */
  public HopTransFileType getFileType() {
    return fileType;
  }

  /**
   * @param fileType The fileType to set
   */
  public void setFileType( HopTransFileType fileType ) {
    this.fileType = fileType;
  }

  /**
   * Gets perspective
   *
   * @return value of perspective
   */
  public HopDataOrchestrationPerspective getPerspective() {
    return perspective;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    HopGuiTransGraph that = (HopGuiTransGraph) o;
    return Objects.equals( transMeta, that.transMeta ) &&
      Objects.equals( id, that.id );
  }

  @Override public int hashCode() {
    return Objects.hash( transMeta, id );
  }

  @GuiToolbarElement(
    type = GuiElementType.TOOLBAR_BUTTON,
    id = TOOLBAR_ITEM_UNDO_ID,
    label = "Undo",
    toolTip = "Undo an operation",
    image = "ui/images/toolbar/Antu_edit-undo.svg",
    disabledImage = "ui/images/toolbar/Antu_edit-undo-disabled.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    separator = true
  )
  @GuiKeyboardShortcut( control = true, key = 'z' )
  @Override public void undo() {
    undoTransDelegate.undoTransformationAction( this, transMeta );
    forceFocus();
  }

  @GuiToolbarElement(
    type = GuiElementType.TOOLBAR_BUTTON,
    id = TOOLBAR_ITEM_REDO_ID,
    label = "Redo",
    toolTip = "Redo an operation",
    image = "ui/images/toolbar/Antu_edit-redo.svg",
    disabledImage = "ui/images/toolbar/Antu_edit-redo-disabled.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  @GuiKeyboardShortcut( control = true, shift = true, key = 'z' )
  @Override public void redo() {
    undoTransDelegate.redoTransformationAction( this, transMeta );
    forceFocus();
  }

  /**
   * Update the representation, toolbar, menus and so on. This is needed after a file, context or capabilities changes
   */
  @Override public void updateGui() {

    setZoomLabel();

    // Enable/disable the undo/redo toolbar buttons...
    //
    toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_UNDO_ID, transMeta.viewThisUndo() != null );
    toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_REDO_ID, transMeta.viewNextUndo() != null );

    // Enable/disable the align/distribute toolbar buttons
    //
    boolean selectedSteps = !transMeta.getSelectedSteps().isEmpty();
    toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_ALIGN_LEFT, selectedSteps );
    toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_ALIGN_RIGHT, selectedSteps );
    toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_ALIGN_TOP, selectedSteps );
    toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_ALIGN_BOTTOM, selectedSteps );
    toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_DISTRIBUTE_HORIZONTALLY, selectedSteps );
    toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_DISTRIBUTE_VERTICALLY, selectedSteps );

    hopUi.setUndoMenu( transMeta );
    hopUi.handleFileCapabilities( fileType );
    super.redraw();
    canvas.setFocus();
  }

  public void redraw() {
    updateGui();
  }

  public boolean forceFocus() {
    return canvas.forceFocus();
  }

  @GuiKeyboardShortcut( control = true, key = 'a' )
  @GuiOSXKeyboardShortcut( command = true, key = 'a' )
  @Override public void selectAll() {
    transMeta.selectAll();
    updateGui();
  }

  @GuiKeyboardShortcut( key = SWT.ESC )
  @Override public void unselectAll() {
    clearSettings();
    updateGui();
  }

  @GuiKeyboardShortcut( control = true, key = 'c' )
  @GuiOSXKeyboardShortcut( command = true, key = 'c' )
  @Override public void copySelectedToClipboard() {
    if ( transLogDelegate.hasSelectedText() ) {
      transLogDelegate.copySelected();
    } else {
      transClipboardDelegate.copySelected( transMeta, transMeta.getSelectedSteps(), transMeta.getSelectedNotes() );
    }
  }

  @GuiKeyboardShortcut( control = true, key = 'x' )
  @GuiOSXKeyboardShortcut( command = true, key = 'x' )
  @Override public void cutSelectedToClipboard() {
    transClipboardDelegate.copySelected( transMeta, transMeta.getSelectedSteps(), transMeta.getSelectedNotes() );
    transStepDelegate.delSteps( transMeta, transMeta.getSelectedSteps() );
    notePadDelegate.deleteNotes( transMeta, transMeta.getSelectedNotes() );
  }

  @GuiKeyboardShortcut( key = SWT.DEL )
  @Override public void deleteSelected() {
    delSelected(null);
    updateGui();
  }

  @GuiKeyboardShortcut( control = true, key = 'v' )
  @GuiOSXKeyboardShortcut( command = true, key = 'v' )

  @Override public void pasteFromClipboard() {
    pasteFromClipboard(  new Point( currentMouseX, currentMouseY ) );
  }

  public void pasteFromClipboard(Point location) {
    final String clipboard = transClipboardDelegate.fromClipboard();
    transClipboardDelegate.pasteXML( transMeta, clipboard, location );
  }

  @GuiContextAction(
    id = "transgraph-trans-paste",
    parentId = HopGuiTransContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Paste from the clipboard",
    tooltip = "Paste steps, notes or a whole transformation from the clipboard",
    image = "ui/images/CPY.svg"
  )
  public void pasteFromClipboard(HopGuiTransContext context) {
    pasteFromClipboard(context.getClick());
  }

  @Override public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>();
    return handlers;
  }
}
