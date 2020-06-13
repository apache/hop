//CHECKSTYLE:FileLength:OFF
/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.ui.hopgui.file.pipeline;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.IEngineMeta;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.Props;
import org.apache.hop.core.SwtUniversalImage;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.dnd.DragAndDropContainer;
import org.apache.hop.core.dnd.XMLTransfer;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.AreaOwner.AreaType;
import org.apache.hop.core.gui.BasePainter;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.IRedrawable;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.SnapAllignDistribute;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.IGuiRefresher;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.logging.DefaultLogLevel;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.IHasLogChannel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILogParentProvided;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.PipelineDataLineage;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineExecutionConfiguration;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePainter;
import org.apache.hop.pipeline.debug.PipelineDebugMeta;
import org.apache.hop.pipeline.debug.TransformDebugMeta;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.IRowDistribution;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.RowDistributionPluginType;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.IStream.StreamType;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.apache.hop.pipeline.transforms.tableinput.TableInputMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.dialog.TransformFieldsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.widget.CheckBoxToolTip;
import org.apache.hop.ui.core.widget.ICheckBoxToolTipListener;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.GuiContextUtil;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.delegates.HopGuiSlaveDelegate;
import org.apache.hop.ui.hopgui.dialog.EnterPreviewRowsDialog;
import org.apache.hop.ui.hopgui.dialog.NotePadDialog;
import org.apache.hop.ui.hopgui.dialog.SearchFieldsProgressDialog;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.delegates.HopGuiNotePadDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineContext;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineHopContext;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineNoteContext;
import org.apache.hop.ui.hopgui.file.pipeline.context.HopGuiPipelineTransformContext;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineClipboardDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineGridDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineHopDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineLogDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineMetricsDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelinePerfDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelinePreviewDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineRunDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineTransformDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.delegates.HopGuiPipelineUndoDelegate;
import org.apache.hop.ui.hopgui.file.pipeline.extension.HopGuiPipelineGraphExtension;
import org.apache.hop.ui.hopgui.file.shared.DelayTimer;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopGuiAbstractGraph;
import org.apache.hop.ui.hopgui.shared.SwtGc;
import org.apache.hop.ui.hopgui.shared.SwtScrollBar;
import org.apache.hop.ui.pipeline.dialog.PipelineDialog;
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
 * This class handles the display of the pipelines in a graphical way using icons, arrows, etc. One pipeline
 * is handled per HopGuiPipelineGraph
 *
 * @author Matt
 * @since 17-mei-2003
 */
@GuiPlugin(
  description = "The pipeline graph GUI plugin"
)
public class HopGuiPipelineGraph extends HopGuiAbstractGraph
  implements IRedrawable, MouseListener, MouseMoveListener, MouseTrackListener, MouseWheelListener, KeyListener,
  IHasLogChannel, ILogParentProvided,  // TODO: Aren't these the same?
  IHopFileTypeHandler,
  IGuiRefresher {

  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiPipelineGraph-Toolbar";
  public static final String TOOLBAR_ITEM_START = "HopGuiPipelineGraph-ToolBar-10010-Run";
  public static final String TOOLBAR_ITEM_STOP = "HopGuiPipelineGraph-ToolBar-10030-Stop";
  public static final String TOOLBAR_ITEM_PAUSE = "HopGuiPipelineGraph-ToolBar-10020-Pause";
  public static final String TOOLBAR_ITEM_PREVIEW = "HopGuiPipelineGraph-ToolBar-10040-Preview";
  public static final String TOOLBAR_ITEM_DEBUG = "HopGuiPipelineGraph-ToolBar-10045-Debug";

  public static final String TOOLBAR_ITEM_UNDO_ID = "HopGuiPipelineGraph-ToolBar-10100-Undo";
  public static final String TOOLBAR_ITEM_REDO_ID = "HopGuiPipelineGraph-ToolBar-10110-Redo";

  public static final String TOOLBAR_ITEM_SNAP_TO_GRID = "HopGuiPipelineGraph-ToolBar-10190-Snap-To-Grid";
  public static final String TOOLBAR_ITEM_ALIGN_LEFT = "HopGuiPipelineGraph-ToolBar-10200-Align-Left";
  public static final String TOOLBAR_ITEM_ALIGN_RIGHT = "HopGuiPipelineGraph-ToolBar-10210-Align-Right";
  public static final String TOOLBAR_ITEM_ALIGN_TOP = "HopGuiPipelineGraph-ToolBar-10250-Align-Ttop";
  public static final String TOOLBAR_ITEM_ALIGN_BOTTOM = "HopGuiPipelineGraph-ToolBar-10260-Align-Bottom";
  public static final String TOOLBAR_ITEM_DISTRIBUTE_HORIZONTALLY = "HopGuiPipelineGraph-ToolBar-10300-Distribute-Horizontally";
  public static final String TOOLBAR_ITEM_DISTRIBUTE_VERTICALLY = "HopGuiPipelineGraph-ToolBar-10310-Distribute-Vertically";

  public static final String TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS = "HopGuiPipelineGraph-ToolBar-10400-Execution-Results";

  public static final String TOOLBAR_ITEM_ZOOM_LEVEL = "HopGuiPipelineGraph-ToolBar-10500-Zoom-Level";

  private ILogChannel log;

  private static final int HOP_SEL_MARGIN = 9;

  private static final int TOOLTIP_HIDE_DELAY_SHORT = 5000;

  private static final int TOOLTIP_HIDE_DELAY_LONG = 10000;

  private PipelineMeta pipelineMeta;
  public IPipelineEngine<PipelineMeta> pipeline;

  private final HopDataOrchestrationPerspective perspective;

  private Composite mainComposite;

  private DefaultToolTip toolTip;

  private CheckBoxToolTip helpTip;

  private ToolBar toolBar;
  private GuiToolbarWidgets toolBarWidgets;

  private int iconsize;

  private Point lastclick;

  private Point lastMove;

  private Point[] previous_transform_locations;

  private Point[] previous_note_locations;

  private List<TransformMeta> selectedTransforms;

  private TransformMeta selectedTransform;

  private List<NotePadMeta> selectedNotes;

  private NotePadMeta selectedNote;

  private PipelineHopMeta candidate;

  private Point drop_candidate;

  private boolean split_hop;

  private int lastButton;

  private PipelineHopMeta last_hop_split;

  private org.apache.hop.core.gui.Rectangle selectionRegion;

  /**
   * A list of remarks on the current Pipeline...
   */
  private List<ICheckResult> remarks;

  /**
   * A list of impacts of the current pipeline on the used databases.
   */
  private List<DatabaseImpact> impact;

  /**
   * Indicates whether or not an impact analysis has already run.
   */
  private boolean impactFinished;

  private PipelineDebugMeta lastPipelineDebugMeta;

  protected int currentMouseX = 0;

  protected int currentMouseY = 0;

  protected NotePadMeta ni = null;

  protected TransformMeta currentTransform;

  private List<AreaOwner> areaOwners;

  // private Text filenameLabel;
  private SashForm sashForm;

  public Composite extraViewComposite;

  public CTabFolder extraViewTabFolder;

  private boolean initialized;

  private boolean halted;

  private boolean halting;

  private boolean safeStopping;

  private boolean debug;

  public HopGuiPipelineLogDelegate pipelineLogDelegate;
  public HopGuiPipelineGridDelegate pipelineGridDelegate;
  public HopGuiPipelineMetricsDelegate pipelineMetricsDelegate;
  public HopGuiPipelinePreviewDelegate pipelinePreviewDelegate;
  public HopGuiPipelineRunDelegate pipelineRunDelegate;
  public HopGuiPipelineTransformDelegate pipelineTransformDelegate;
  public HopGuiPipelineClipboardDelegate pipelineClipboardDelegate;
  public HopGuiPipelineHopDelegate pipelineHopDelegate;
  public HopGuiPipelineUndoDelegate pipelineUndoDelegate;
  public HopGuiPipelinePerfDelegate pipelinePerfDelegate;

  public HopGuiSlaveDelegate slaveDelegate;
  public HopGuiNotePadDelegate notePadDelegate;

  public List<ISelectedTransformListener> transformListeners;
  public List<ITransformSelectionListener> currentTransformListeners = new ArrayList<>();

  /**
   * A map that keeps track of which log line was written by which transform
   */
  private Map<String, String> transformLogMap;

  private TransformMeta startHopTransform;
  private Point endHopLocation;
  private boolean startErrorHopTransform;

  private TransformMeta noInputTransform;

  private TransformMeta endHopTransform;

  private StreamType candidateHopType;

  private Map<TransformMeta, DelayTimer> delayTimers;

  Timer redrawTimer;

  private HopPipelineFileType fileType;
  private boolean ignoreNextClick;
  private boolean doubleClick;
  private PipelineHopMeta clickedPipelineHop;

  public void setCurrentNote( NotePadMeta ni ) {
    this.ni = ni;
  }

  public NotePadMeta getCurrentNote() {
    return ni;
  }

  public TransformMeta getCurrentTransform() {
    return currentTransform;
  }

  public void setCurrentTransform( TransformMeta currentTransform ) {
    this.currentTransform = currentTransform;
  }

  public void addSelectedTransformListener( ISelectedTransformListener selectedTransformListener ) {
    transformListeners.add( selectedTransformListener );
  }

  public void addCurrentTransformListener( ITransformSelectionListener transformSelectionListener ) {
    currentTransformListeners.add( transformSelectionListener );
  }

  public HopGuiPipelineGraph( Composite parent, final HopGui hopGui, final CTabItem parentTabItem,
                              final HopDataOrchestrationPerspective perspective, final PipelineMeta pipelineMeta, final HopPipelineFileType fileType ) {
    super( hopGui, parent, SWT.NONE, parentTabItem );
    activePipelineGraph = this; // We're working on this one
    this.hopGui = hopGui;
    this.parentTabItem = parentTabItem;
    this.perspective = perspective;
    this.pipelineMeta = pipelineMeta;
    this.fileType = fileType;
    this.areaOwners = new ArrayList<>();

    this.log = hopGui.getLog();

    this.delayTimers = new HashMap<>();

    pipelineLogDelegate = new HopGuiPipelineLogDelegate( hopGui, this );
    pipelineGridDelegate = new HopGuiPipelineGridDelegate( hopGui, this );
    pipelinePerfDelegate = new HopGuiPipelinePerfDelegate( hopGui, this );
    pipelineMetricsDelegate = new HopGuiPipelineMetricsDelegate( hopGui, this );
    pipelinePreviewDelegate = new HopGuiPipelinePreviewDelegate( hopGui, this );
    pipelineClipboardDelegate = new HopGuiPipelineClipboardDelegate( hopGui, this );
    pipelineTransformDelegate = new HopGuiPipelineTransformDelegate( hopGui, this );
    pipelineHopDelegate = new HopGuiPipelineHopDelegate( hopGui, this );
    pipelineUndoDelegate = new HopGuiPipelineUndoDelegate( hopGui, this );
    pipelineRunDelegate = new HopGuiPipelineRunDelegate( hopGui, this );
    pipelinePerfDelegate = new HopGuiPipelinePerfDelegate( hopGui, this );

    slaveDelegate = new HopGuiSlaveDelegate( hopGui, this );
    notePadDelegate = new HopGuiNotePadDelegate( hopGui, this );

    transformListeners = new ArrayList<>();

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

    activePipelineGraph = null; // No longer needed

    // The main composite contains the graph view, but if needed also
    // a view with an extra tab containing log, etc.
    //
    mainComposite = new Composite( this, SWT.NONE );
    mainComposite.setBackground( GuiResource.getInstance().getColorOrange() );
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
    toolTip.setShift( new org.eclipse.swt.graphics.Point( ConstUi.TOOLTIP_OFFSET, ConstUi.TOOLTIP_OFFSET ) );

    helpTip = new CheckBoxToolTip( canvas );
    helpTip.addCheckBoxToolTipListener( new ICheckBoxToolTipListener() {

      @Override
      public void checkBoxSelected( boolean enabled ) {
        hopGui.getProps().setShowingHelpToolTips( enabled );
      }
    } );

    iconsize = hopGui.getProps().getIconSize();

    clearSettings();

    remarks = new ArrayList<>();
    impact = new ArrayList<>();
    impactFinished = false;

    horizontalScrollBar = canvas.getHorizontalBar();
    verticalScrollBar = canvas.getVerticalBar();

    horizontalScrollBar.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        redraw();
      }
    } );
    verticalScrollBar.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent e ) {
        redraw();
      }
    } );
    horizontalScrollBar.setThumb( 100 );
    verticalScrollBar.setThumb( 100 );

    horizontalScrollBar.setVisible( true );
    verticalScrollBar.setVisible( true );

    setVisible( true );
    newProps();

    canvas.setBackground( GuiResource.getInstance().getColorBlueCustomGrid() );

    canvas.addPaintListener( new PaintListener() {
      @Override
      public void paintControl( PaintEvent e ) {
        // if ( !hopGui.isStopped() ) {
        HopGuiPipelineGraph.this.paintControl( e );
        // }
      }
    } );

    selectedTransforms = null;
    lastclick = null;

    /*
     * Handle the mouse...
     */

    canvas.addMouseListener( this );
    canvas.addMouseMoveListener( this );
    canvas.addMouseTrackListener( this );
    canvas.addMouseWheelListener( this );
    // canvas.addKeyListener( this );

    // Drag & Drop for transforms
    Transfer[] ttypes = new Transfer[] { XMLTransfer.getInstance() };
    DropTarget ddTarget = new DropTarget( canvas, DND.DROP_MOVE );
    ddTarget.setTransfer( ttypes );
    ddTarget.addDropListener( new DropTargetListener() {
      @Override
      public void dragEnter( DropTargetEvent event ) {
        clearSettings();

        drop_candidate = PropsUi.calculateGridPosition( getRealPosition( canvas, event.x, event.y ) );

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
        drop_candidate = PropsUi.calculateGridPosition( getRealPosition( canvas, event.x, event.y ) );

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

          TransformMeta transformMeta = null;
          boolean newTransform = false;

          switch ( container.getType() ) {
            // Put an existing one on the canvas.
            case DragAndDropContainer.TYPE_TRANSFORM:
              // Drop hidden transform onto canvas....
              transformMeta = pipelineMeta.findTransform( container.getData() );
              if ( transformMeta != null ) {
                if ( pipelineMeta.isTransformUsedInPipelineHops( transformMeta ) ) {
                  modalMessageDialog( BaseMessages.getString( PKG, "PipelineGraph.Dialog.TransformIsAlreadyOnCanvas.Title" ),
                    BaseMessages.getString( PKG, "PipelineGraph.Dialog.TransformIsAlreadyOnCanvas.Message" ), SWT.OK );
                  return;
                }
                // This transform gets the drawn attribute and position set below.
              } else {
                // Unknown transform dropped: ignore this to be safe!
                return;
              }
              break;

            // Create a new transform
            case DragAndDropContainer.TYPE_BASE_TRANSFORM_TYPE:
              // Not an existing transform: data refers to the type of transform to create
              String id = container.getId();
              String name = container.getData();
              transformMeta = pipelineTransformDelegate.newTransform( pipelineMeta, id, name, name, false, true, p );
              if ( transformMeta != null ) {
                newTransform = true;
              } else {
                return; // Cancelled pressed in dialog or unable to create transform.
              }
              break;

            // Create a new TableInput transform using the selected connection...
            case DragAndDropContainer.TYPE_DATABASE_CONNECTION:
              newTransform = true;
              String connectionName = container.getData();
              TableInputMeta tii = new TableInputMeta();
              tii.setDatabaseMeta( pipelineMeta.findDatabase( connectionName ) );
              PluginRegistry registry = PluginRegistry.getInstance();
              String transformID = registry.getPluginId( TransformPluginType.class, tii );
              IPlugin transformPlugin = registry.findPluginWithId( TransformPluginType.class, transformID );
              String transformName = pipelineMeta.getAlternativeTransformName( transformPlugin.getName() );
              transformMeta = new TransformMeta( transformID, transformName, tii );
              if ( pipelineTransformDelegate.editTransform( pipelineMeta, transformMeta ) != null ) {
                pipelineMeta.addTransform( transformMeta );
                redraw();
              } else {
                return;
              }
              break;

            // Drag hop on the canvas: create a new Hop...
            case DragAndDropContainer.TYPE_PIPELINE_HOP:
              newHop();
              return;

            default:
              // Nothing we can use: give an error!
              modalMessageDialog( BaseMessages.getString( PKG, "PipelineGraph.Dialog.ItemCanNotBePlacedOnCanvas.Title" ),
                BaseMessages.getString( PKG, "PipelineGraph.Dialog.ItemCanNotBePlacedOnCanvas.Message" ), SWT.OK );
              return;
          }

          pipelineMeta.unselectAll();

          TransformMeta before = null;
          if ( !newTransform ) {
            before = (TransformMeta) transformMeta.clone();
          }


          transformMeta.setSelected( true );
          PropsUi.setLocation( transformMeta, p.x, p.y );

          if ( newTransform ) {
            hopGui.undoDelegate.addUndoNew( pipelineMeta, new TransformMeta[] { transformMeta }, new int[] { pipelineMeta.indexOfTransform( transformMeta ) } );
          } else {
            hopGui.undoDelegate.addUndoChange( pipelineMeta, new TransformMeta[] { before }, new TransformMeta[] { (TransformMeta) transformMeta.clone() },
              new int[] { pipelineMeta.indexOfTransform( transformMeta ) } );
          }

          forceFocus();
          redraw();

          // See if we want to draw a tool tip explaining how to create new hops...
          //
          if ( newTransform && pipelineMeta.nrTransforms() > 1 && pipelineMeta.nrTransforms() < 5 && hopGui.getProps().isShowingHelpToolTips() ) {
            showHelpTip( p.x, p.y, BaseMessages.getString( PKG, "PipelineGraph.HelpToolTip.CreatingHops.Title" ),
              BaseMessages.getString( PKG, "PipelineGraph.HelpToolTip.CreatingHops.Message" ) );
          }
        } catch ( Exception e ) {
          new ErrorDialog( hopShell(), BaseMessages.getString( PKG, "PipelineGraph.Dialog.ErrorDroppingObject.Message" ),
            BaseMessages.getString( PKG, "PipelineGraph.Dialog.ErrorDroppingObject.Title" ), e );
        }
      }

      @Override
      public void dropAccept( DropTargetEvent event ) {
      }
    } );

    setBackground( GuiResource.getInstance().getColorBackground() );

    // Add keyboard listeners from the main GUI and this class (toolbar etc) to the canvas. That's where the focus should be
    //
    hopGui.replaceKeyboardShortcutListeners( this );

    // Update menu, toolbar, force redraw canvas
    //
    updateGui();
  }

  private static HopGuiPipelineGraph activePipelineGraph;

  // In case anyone asks...
  //
  public static HopGuiPipelineGraph getInstance() {
    if ( activePipelineGraph != null ) {
      return activePipelineGraph;
    }
    IHopFileTypeHandler fileTypeHandler = HopGui.getInstance().getActiveFileTypeHandler();
    if ( fileTypeHandler instanceof HopGuiPipelineGraph ) {
      return (HopGuiPipelineGraph) fileTypeHandler;
    }
    return null;
  }

  @Override
  public void mouseDoubleClick( MouseEvent e ) {
    doubleClick = true;
    clearSettings();

    Point real = screen2real( e.x, e.y );

    // Hide the tooltip!
    hideToolTips();

    /** TODO: Add back in
     try {
     ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.PipelineGraphMouseDoubleClick.id,
     new HopGuiPipelineGraphExtension( this, e, real ) );
     } catch ( Exception ex ) {
     LogChannel.GENERAL.logError( "Error calling PipelineGraphMouseDoubleClick extension point", ex );
     }
     **/

    TransformMeta transformMeta = pipelineMeta.getTransform( real.x, real.y, iconsize );
    if ( transformMeta != null ) {
      if ( e.button == 1 ) {
        editTransform( transformMeta );
      } else {
        editDescription( transformMeta );
      }
    } else {
      // Check if point lies on one of the many hop-lines...
      PipelineHopMeta online = findPipelineHop( real.x, real.y );
      if ( online != null ) {
        editHop( online );
      } else {
        NotePadMeta ni = pipelineMeta.getNote( real.x, real.y );
        if ( ni != null ) {
          selectedNote = null;
          editNote( ni );
        } else {
          // See if the double click was in one of the area's...
          //
          boolean hit = false;
          for ( AreaOwner areaOwner : areaOwners ) {
            if ( areaOwner.contains( real.x, real.y ) ) {
              if ( areaOwner.getParent() instanceof TransformMeta
                && areaOwner.getOwner().equals( PipelinePainter.STRING_PARTITIONING_CURRENT_TRANSFORM ) ) {
                TransformMeta transform = (TransformMeta) areaOwner.getParent();
                pipelineTransformDelegate.editTransformPartitioning( pipelineMeta, transform );
                hit = true;
                break;
              }
            }
          }

          if ( !hit ) {
            editPipelineProperties( new HopGuiPipelineContext( pipelineMeta, this, real ) );
          }

        }
      }
    }
  }

  @Override
  public void mouseDown( MouseEvent e ) {
    doubleClick = false;

    if ( ignoreNextClick ) {
      ignoreNextClick = false;
      return;
    }

    boolean alt = ( e.stateMask & SWT.ALT ) != 0;
    boolean control = ( e.stateMask & SWT.MOD1 ) != 0;
    boolean shift = ( e.stateMask & SWT.SHIFT ) != 0;

    lastButton = e.button;
    Point real = screen2real( e.x, e.y );
    lastclick = new Point( real.x, real.y );

    // Hide the tooltip!
    hideToolTips();

    try {
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.PipelineGraphMouseDown.id, new HopGuiPipelineGraphExtension( this, e, real ) );
    } catch ( Exception ex ) {
      LogChannel.GENERAL.logError( "Error calling PipelineGraphMouseDown extension point", ex );
    }

    // A single left or middle click on one of the area owners...
    //
    if ( e.button == 1 || e.button == 2 ) {
      AreaOwner areaOwner = getVisibleAreaOwner( real.x, real.y );
      if ( areaOwner != null && areaOwner.getAreaType() != null ) {
        switch ( areaOwner.getAreaType() ) {
          case TRANSFORM_OUTPUT_HOP_ICON:
            // Click on the output icon means: start of drag
            // Action: We show the input icons on the other transforms...
            //
            selectedTransform = null;
            startHopTransform = (TransformMeta) areaOwner.getParent();
            candidateHopType = null;
            startErrorHopTransform = false;
            break;

          case TRANSFORM_INPUT_HOP_ICON:
            // Click on the input icon means: start to a new hop
            // In this case, we set the end hop transform...
            //
            selectedTransform = null;
            startHopTransform = null;
            endHopTransform = (TransformMeta) areaOwner.getParent();
            candidateHopType = null;
            startErrorHopTransform = false;
            break;

          case HOP_ERROR_ICON:
            // Click on the error icon means: Edit error handling
            //
            TransformMeta transformMeta = (TransformMeta) areaOwner.getParent();
            pipelineTransformDelegate.editTransformErrorHandling( pipelineMeta, transformMeta );
            break;

          case TRANSFORM_TARGET_HOP_ICON_OPTION:
            // Below, see showTransformTargetOptions()
            break;

          case TRANSFORM_EDIT_ICON:
            clearSettings();
            currentTransform = (TransformMeta) areaOwner.getParent();
            editTransform();
            break;

          case TRANSFORM_INJECT_ICON:
            modalMessageDialog( BaseMessages.getString( PKG, "PipelineGraph.TransformInjectionSupported.Title" ),
              BaseMessages.getString( PKG, "PipelineGraph.TransformInjectionSupported.Tooltip" ), SWT.OK | SWT.ICON_INFORMATION );
            break;

          case TRANSFORM_ICON:
            transformMeta = (TransformMeta) areaOwner.getOwner();
            currentTransform = transformMeta;

            for ( ITransformSelectionListener listener : currentTransformListeners ) {
              listener.onUpdateSelection( currentTransform );
            }

            if ( candidate != null ) {
              addCandidateAsHop( e.x, e.y );
            }
            // ALT-Click: edit error handling
            //
            if ( e.button == 1 && alt && transformMeta.supportsErrorHandling() ) {
              pipelineTransformDelegate.editTransformErrorHandling( pipelineMeta, transformMeta );
              return;
            } else if ( e.button == 1 && startHopTransform != null && endHopTransform == null ) {
              candidate = new PipelineHopMeta( startHopTransform, currentTransform );
              addCandidateAsHop( e.x, e.y );
            } else if ( e.button == 2 || ( e.button == 1 && shift ) ) {
              // SHIFT CLICK is start of drag to create a new hop
              //
              startHopTransform = transformMeta;
            } else {
              selectedTransforms = pipelineMeta.getSelectedTransforms();
              selectedTransform = transformMeta;
              //
              // When an icon is moved that is not selected, it gets
              // selected too late.
              // It is not captured here, but in the mouseMoveListener...
              //
              previous_transform_locations = pipelineMeta.getSelectedTransformLocations();

              Point p = transformMeta.getLocation();
              iconOffset = new Point( real.x - p.x, real.y - p.y );
            }
            redraw();
            break;

          case NOTE:
            ni = (NotePadMeta) areaOwner.getOwner();
            selectedNotes = pipelineMeta.getSelectedNotes();
            selectedNote = ni;
            Point loc = ni.getLocation();

            previous_note_locations = pipelineMeta.getSelectedNoteLocations();

            noteOffset = new Point( real.x - loc.x, real.y - loc.y );

            redraw();
            break;

          case TRANSFORM_COPIES_TEXT:
            copies( (TransformMeta) areaOwner.getOwner() );
            break;

          case TRANSFORM_DATA_SERVICE:
            editProperties( pipelineMeta, hopGui, true, PipelineDialog.Tabs.EXTRA_TAB );
            break;
          default:
            break;
        }
      } else {
        PipelineHopMeta hop = findPipelineHop( real.x, real.y );
        if ( hop != null ) {
          // A hop: show context dialog in mouseUp()
          //
          clickedPipelineHop = hop;
        } else {
          // No area-owner & no hop means : background click:
          //
          startHopTransform = null;
          if ( !control ) {
            selectionRegion = new org.apache.hop.core.gui.Rectangle( real.x, real.y, 0, 0 );
          }
          updateGui();
        }
      }
    }
  }

  private enum SingleClickType {
    Pipeline,
    Transform,
    Note,
    Hop,
  }

  @Override
  public void mouseUp( MouseEvent e ) {

    try {
      HopGuiPipelineGraphExtension ext = new HopGuiPipelineGraphExtension( null, e, getArea() );
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.PipelineGraphMouseUp.id, ext );
      if ( ext.isPreventDefault() ) {
        redraw();
        clearSettings();
        return;
      }
    } catch ( Exception ex ) {
      LogChannel.GENERAL.logError( "Error calling PipelineGraphMouseUp extension point", ex );
    }

    boolean control = ( e.stateMask & SWT.MOD1 ) != 0;
    PipelineHopMeta selectedHop = findPipelineHop( e.x, e.y );
    updateErrorMetaForHop( selectedHop );
    boolean singleClick = false;
    SingleClickType singleClickType = null;
    TransformMeta singleClickTransform = null;
    NotePadMeta singleClickNote = null;
    PipelineHopMeta singleClickHop = null;

    if ( iconOffset == null ) {
      iconOffset = new Point( 0, 0 );
    }
    Point real = screen2real( e.x, e.y );
    Point icon = new Point( real.x - iconOffset.x, real.y - iconOffset.y );
    AreaOwner areaOwner = getVisibleAreaOwner( real.x, real.y );

    try {
      HopGuiPipelineGraphExtension ext = new HopGuiPipelineGraphExtension( this, e, real );
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.PipelineGraphMouseUp.id, ext );
      if ( ext.isPreventDefault() ) {
        redraw();
        clearSettings();
        return;
      }
    } catch ( Exception ex ) {
      LogChannel.GENERAL.logError( "Error calling PipelineGraphMouseUp extension point", ex );
    }

    // Quick new hop option? (drag from one transform to another)
    //
    if ( candidate != null && areaOwner != null && areaOwner.getAreaType() != null ) {
      switch ( areaOwner.getAreaType() ) {
        case TRANSFORM_ICON:
          currentTransform = (TransformMeta) areaOwner.getOwner();
          break;
        case TRANSFORM_INPUT_HOP_ICON:
          currentTransform = (TransformMeta) areaOwner.getParent();
          break;
        default:
          break;
      }
      addCandidateAsHop( e.x, e.y );
      redraw();
    } else {
      // Did we select a region on the screen? Mark transforms in region as
      // selected
      //
      if ( selectionRegion != null ) {
        selectionRegion.width = real.x - selectionRegion.x;
        selectionRegion.height = real.y - selectionRegion.y;
        if ( selectionRegion.width == 0 && selectionRegion.height == 0 ) {
          singleClick = true;
          singleClickType = SingleClickType.Pipeline;
        }
        pipelineMeta.unselectAll();
        selectInRect( pipelineMeta, selectionRegion );
        selectionRegion = null;
        updateGui();
      } else {
        // Clicked on an icon?
        //
        if ( selectedTransform != null && startHopTransform == null ) {
          if ( e.button == 1 ) {
            Point realclick = screen2real( e.x, e.y );
            if ( lastclick.x == realclick.x && lastclick.y == realclick.y ) {
              // Flip selection when control is pressed!
              if ( control ) {
                selectedTransform.flipSelected();
              } else {
                singleClick = true;
                singleClickType = SingleClickType.Transform;
                singleClickTransform = selectedTransform;
              }
            } else {
              // Find out which Transforms & Notes are selected
              selectedTransforms = pipelineMeta.getSelectedTransforms();
              selectedNotes = pipelineMeta.getSelectedNotes();

              // We moved around some items: store undo info...
              //
              boolean also = false;
              if ( selectedNotes != null && selectedNotes.size() > 0 && previous_note_locations != null ) {
                int[] indexes = pipelineMeta.getNoteIndexes( selectedNotes );

                also = selectedTransforms != null && selectedTransforms.size() > 0;
                hopGui.undoDelegate.addUndoPosition( pipelineMeta, selectedNotes.toArray( new NotePadMeta[ selectedNotes.size() ] ),
                  indexes, previous_note_locations, pipelineMeta.getSelectedNoteLocations(), also );
              }
              if ( selectedTransforms != null && previous_transform_locations != null ) {
                int[] indexes = pipelineMeta.getTransformIndexes( selectedTransforms );
                hopGui.undoDelegate.addUndoPosition( pipelineMeta, selectedTransforms.toArray( new TransformMeta[ selectedTransforms.size() ] ), indexes,
                  previous_transform_locations, pipelineMeta.getSelectedTransformLocations(), also );
              }
            }
          }

          // OK, we moved the transform, did we move it across a hop?
          // If so, ask to split the hop!
          if ( split_hop ) {
            PipelineHopMeta hi = findPipelineHop( icon.x + iconsize / 2, icon.y + iconsize / 2, selectedTransform );
            if ( hi != null ) {
              splitHop( hi );
            }
            split_hop = false;
          }

          selectedTransforms = null;
          selectedNotes = null;
          selectedTransform = null;
          selectedNote = null;
          startHopTransform = null;
          endHopLocation = null;

          updateGui();
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
                // Find out which Transforms & Notes are selected
                selectedTransforms = pipelineMeta.getSelectedTransforms();
                selectedNotes = pipelineMeta.getSelectedNotes();

                // We moved around some items: store undo info...

                boolean also = false;
                if ( selectedNotes != null && selectedNotes.size() > 0 && previous_note_locations != null ) {
                  int[] indexes = pipelineMeta.getNoteIndexes( selectedNotes );
                  hopGui.undoDelegate.addUndoPosition( pipelineMeta, selectedNotes.toArray( new NotePadMeta[ selectedNotes.size() ] ), indexes,
                    previous_note_locations, pipelineMeta.getSelectedNoteLocations(), also );
                  also = selectedTransforms != null && selectedTransforms.size() > 0;
                }
                if ( selectedTransforms != null && selectedTransforms.size() > 0 && previous_transform_locations != null ) {
                  int[] indexes = pipelineMeta.getTransformIndexes( selectedTransforms );
                  hopGui.undoDelegate.addUndoPosition( pipelineMeta, selectedTransforms.toArray( new TransformMeta[ selectedTransforms.size() ] ), indexes,
                    previous_transform_locations, pipelineMeta.getSelectedTransformLocations(), also );
                }
              }
            }

            selectedNotes = null;
            selectedTransforms = null;
            selectedTransform = null;
            selectedNote = null;
            startHopTransform = null;
            endHopLocation = null;
          }
        }
      }
    }
    if ( clickedPipelineHop != null ) {
      // Clicked on a hop
      //
      singleClick = true;
      singleClickType = HopGuiPipelineGraph.SingleClickType.Hop;
      singleClickHop = clickedPipelineHop;
    }
    clickedPipelineHop = null;

    // Only do this "mouseUp()" if this is not part of a double click...
    //
    final boolean fSingleClick = singleClick;
    final SingleClickType fSingleClickType = singleClickType;
    final TransformMeta fSingleClickTransform = singleClickTransform;
    final NotePadMeta fSingleClickNote = singleClickNote;
    final PipelineHopMeta fSingleClickHop = singleClickHop;

    Display.getDefault().timerExec( Display.getDefault().getDoubleClickTime(),
      () -> {
        if ( !doubleClick ) {
          // Just a single click on the background:
          // We have a bunch of possible actions for you...
          //
          if ( fSingleClick && fSingleClickType != null ) {
            IGuiContextHandler contextHandler = null;
            String message = null;
            switch ( fSingleClickType ) {
              case Pipeline:
                message = "Select the action to execute or the transform to create:";
                contextHandler = new HopGuiPipelineContext( pipelineMeta, this, real );
                break;
              case Transform:
                message = "Select the action to take on transform '" + fSingleClickTransform.getName() + "':";
                contextHandler = new HopGuiPipelineTransformContext( pipelineMeta, fSingleClickTransform, this, real );
                break;
              case Note:
                message = "Select the note action to take:";
                contextHandler = new HopGuiPipelineNoteContext( pipelineMeta, fSingleClickNote, this, real );
                break;
              case Hop:
                message = "Select the hop action to take:";
                contextHandler = new HopGuiPipelineHopContext( pipelineMeta, fSingleClickHop, this, real );
                break;
              default:
                break;
            }
            if ( contextHandler != null ) {
              Shell parent = hopShell();
              org.eclipse.swt.graphics.Point p = parent.getDisplay().map( canvas, null, e.x, e.y );

              // If we lost focus ignore the next left click
              //
              ignoreNextClick = GuiContextUtil.handleActionSelection( parent, message, new Point( p.x, p.y ), contextHandler.getSupportedActions() );
            }

          }
        }
      } );

    lastButton = 0;
  }

  private void splitHop( PipelineHopMeta hi ) {
    int id = 0;
    if ( !hopGui.getProps().getAutoSplit() ) {
      MessageDialogWithToggle md =
        new MessageDialogWithToggle( hopShell(), BaseMessages.getString( PKG, "PipelineGraph.Dialog.SplitHop.Title" ), null,
          BaseMessages.getString( PKG, "PipelineGraph.Dialog.SplitHop.Message" ) + Const.CR + hi.toString(),
          MessageDialog.QUESTION, new String[] { BaseMessages.getString( PKG, "System.Button.Yes" ),
          BaseMessages.getString( PKG, "System.Button.No" ) }, 0, BaseMessages.getString( PKG,
          "PipelineGraph.Dialog.Option.SplitHop.DoNotAskAgain" ), hopGui.getProps().getAutoSplit() );
      MessageDialogWithToggle.setDefaultImage( GuiResource.getInstance().getImageHopUi() );
      id = md.open();
      hopGui.getProps().setAutoSplit( md.getToggleState() );
    }

    if ( ( id & 0xFF ) == 0 ) { // Means: "Yes" button clicked!

      // Only split A-->--B by putting C in between IF...
      // C-->--A or B-->--C don't exists...
      // A ==> hi.getFromTransform()
      // B ==> hi.getToTransform();
      // C ==> selectedTransform
      //
      boolean caExists = pipelineMeta.findPipelineHop( selectedTransform, hi.getFromTransform() ) != null;
      boolean bcExists = pipelineMeta.findPipelineHop( hi.getToTransform(), selectedTransform ) != null;
      if ( !caExists && !bcExists ) {

        TransformMeta fromTransform = hi.getFromTransform();
        TransformMeta toTransform = hi.getToTransform();

        // In case transform A targets B then we now need to target C
        //
        ITransformIOMeta fromIo = fromTransform.getTransform().getTransformIOMeta();
        for ( IStream stream : fromIo.getTargetStreams() ) {
          if ( stream.getTransformMeta() != null && stream.getTransformMeta().equals( toTransform ) ) {
            // This target stream was directed to B, now we need to direct it to C
            stream.setTransformMeta( selectedTransform );
            fromTransform.getTransform().handleStreamSelection( stream );
          }
        }

        // In case transform B sources from A then we now need to source from C
        //
        ITransformIOMeta toIo = toTransform.getTransform().getTransformIOMeta();
        for ( IStream stream : toIo.getInfoStreams() ) {
          if ( stream.getTransformMeta() != null && stream.getTransformMeta().equals( fromTransform ) ) {
            // This info stream was reading from B, now we need to direct it to C
            stream.setTransformMeta( selectedTransform );
            toTransform.getTransform().handleStreamSelection( stream );
          }
        }

        // In case there is error handling on A, we want to make it point to C now
        //
        TransformErrorMeta errorMeta = fromTransform.getTransformErrorMeta();
        if ( fromTransform.isDoingErrorHandling() && toTransform.equals( errorMeta.getTargetTransform() ) ) {
          errorMeta.setTargetTransform( selectedTransform );
        }

        PipelineHopMeta newhop1 = new PipelineHopMeta( hi.getFromTransform(), selectedTransform );
        if ( pipelineMeta.findPipelineHop( newhop1 ) == null ) {
          pipelineMeta.addPipelineHop( newhop1 );
          hopGui.undoDelegate.addUndoNew( pipelineMeta, new PipelineHopMeta[] { newhop1, }, new int[] { pipelineMeta.indexOfPipelineHop( newhop1 ), }, true );
        }
        PipelineHopMeta newhop2 = new PipelineHopMeta( selectedTransform, hi.getToTransform() );
        if ( pipelineMeta.findPipelineHop( newhop2 ) == null ) {
          pipelineMeta.addPipelineHop( newhop2 );
          hopGui.undoDelegate.addUndoNew( pipelineMeta, new PipelineHopMeta[] { newhop2 }, new int[] { pipelineMeta.indexOfPipelineHop( newhop2 ) }, true );
        }
        int idx = pipelineMeta.indexOfPipelineHop( hi );

        hopGui.undoDelegate.addUndoDelete( pipelineMeta, new PipelineHopMeta[] { hi }, new int[] { idx }, true );
        pipelineMeta.removePipelineHop( idx );

        redraw();
      }

      // else: Silently discard this hop-split attempt.
    }
  }

  @Override
  public void mouseMove( MouseEvent e ) {
    boolean shift = ( e.stateMask & SWT.SHIFT ) != 0;
    noInputTransform = null;

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

    if ( iconOffset == null ) {
      iconOffset = new Point( 0, 0 );
    }
    Point icon = new Point( real.x - iconOffset.x, real.y - iconOffset.y );

    if ( noteOffset == null ) {
      noteOffset = new Point( 0, 0 );
    }
    Point note = new Point( real.x - noteOffset.x, real.y - noteOffset.y );

    // Moved over an area?
    //
    AreaOwner areaOwner = getVisibleAreaOwner( real.x, real.y );
    if ( areaOwner != null && areaOwner.getAreaType() != null ) {
      switch ( areaOwner.getAreaType() ) {
        case TRANSFORM_ICON:
          TransformMeta transformMeta = (TransformMeta) areaOwner.getOwner();
          resetDelayTimer( transformMeta );
          break;

        case MINI_ICONS_BALLOON: // Give the timer a bit more time
          transformMeta = (TransformMeta) areaOwner.getParent();
          resetDelayTimer( transformMeta );
          break;

        default:
          break;
      }
    }

    try {
      HopGuiPipelineGraphExtension ext = new HopGuiPipelineGraphExtension( this, e, real );
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.PipelineGraphMouseMoved.id, ext );
    } catch ( Exception ex ) {
      LogChannel.GENERAL.logError( "Error calling PipelineGraphMouseMoved extension point", ex );
    }

    //
    // First see if the icon we clicked on was selected.
    // If the icon was not selected, we should un-select all other
    // icons, selected and move only the one icon
    //
    if ( selectedTransform != null && !selectedTransform.isSelected() ) {
      pipelineMeta.unselectAll();
      selectedTransform.setSelected( true );
      selectedTransforms = new ArrayList<>();
      selectedTransforms.add( selectedTransform );
      previous_transform_locations = new Point[] { selectedTransform.getLocation() };
      redraw();
    } else if ( selectedNote != null && !selectedNote.isSelected() ) {
      pipelineMeta.unselectAll();
      selectedNote.setSelected( true );
      selectedNotes = new ArrayList<>();
      selectedNotes.add( selectedNote );
      previous_note_locations = new Point[] { selectedNote.getLocation() };
      redraw();
    } else if ( selectionRegion != null && startHopTransform == null ) {
      // Did we select a region...?
      //

      selectionRegion.width = real.x - selectionRegion.x;
      selectionRegion.height = real.y - selectionRegion.y;
      redraw();
    } else if ( selectedTransform != null && lastButton == 1 && !shift && startHopTransform == null ) {
      //
      // One or more icons are selected and moved around...
      //
      // new : new position of the ICON (not the mouse pointer) dx : difference with previous position
      //
      int dx = icon.x - selectedTransform.getLocation().x;
      int dy = icon.y - selectedTransform.getLocation().y;

      // See if we have a hop-split candidate
      //
      PipelineHopMeta hi = findPipelineHop( icon.x + iconsize / 2, icon.y + iconsize / 2, selectedTransform );
      if ( hi != null ) {
        // OK, we want to split the hop in 2
        //
        if ( !hi.getFromTransform().equals( selectedTransform ) && !hi.getToTransform().equals( selectedTransform ) ) {
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

      selectedNotes = pipelineMeta.getSelectedNotes();
      selectedTransforms = pipelineMeta.getSelectedTransforms();

      // Adjust location of selected transforms...
      if ( selectedTransforms != null ) {
        for ( int i = 0; i < selectedTransforms.size(); i++ ) {
          TransformMeta transformMeta = selectedTransforms.get( i );
          PropsUi.setLocation( transformMeta, transformMeta.getLocation().x + dx, transformMeta.getLocation().y + dy );
        }
      }
      // Adjust location of selected hops...
      if ( selectedNotes != null ) {
        for ( int i = 0; i < selectedNotes.size(); i++ ) {
          NotePadMeta ni = selectedNotes.get( i );
          PropsUi.setLocation( ni, ni.getLocation().x + dx, ni.getLocation().y + dy );
        }
      }

      redraw();
    } else if ( ( startHopTransform != null && endHopTransform == null ) || ( endHopTransform != null && startHopTransform == null ) ) {
      // Are we creating a new hop with the middle button or pressing SHIFT?
      //

      TransformMeta transformMeta = pipelineMeta.getTransform( real.x, real.y, iconsize );
      endHopLocation = new Point( real.x, real.y );
      if ( transformMeta != null
        && ( ( startHopTransform != null && !startHopTransform.equals( transformMeta ) ) || ( endHopTransform != null && !endHopTransform
        .equals( transformMeta ) ) ) ) {
        ITransformIOMeta ioMeta = transformMeta.getTransform().getTransformIOMeta();
        if ( candidate == null ) {
          // See if the transform accepts input. If not, we can't create a new hop...
          //
          if ( startHopTransform != null ) {
            if ( ioMeta.isInputAcceptor() ) {
              candidate = new PipelineHopMeta( startHopTransform, transformMeta );
              endHopLocation = null;
            } else {
              noInputTransform = transformMeta;
              toolTip.setImage( null );
              toolTip.setText( "This transform does not accept any input from other transforms" );
              toolTip.show( new org.eclipse.swt.graphics.Point( real.x, real.y ) );
            }
          } else if ( endHopTransform != null ) {
            if ( ioMeta.isOutputProducer() ) {
              candidate = new PipelineHopMeta( transformMeta, endHopTransform );
              endHopLocation = null;
            } else {
              noInputTransform = transformMeta;
              toolTip.setImage( null );
              toolTip
                .setText( "This transform doesn't pass any output to other transforms. (except perhaps for targetted output)" );
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

    // Move around notes & transforms
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

        selectedNotes = pipelineMeta.getSelectedNotes();
        selectedTransforms = pipelineMeta.getSelectedTransforms();

        // Adjust location of selected transforms...
        if ( selectedTransforms != null ) {
          for ( int i = 0; i < selectedTransforms.size(); i++ ) {
            TransformMeta transformMeta = selectedTransforms.get( i );
            PropsUi.setLocation( transformMeta, transformMeta.getLocation().x + dx, transformMeta.getLocation().y + dy );
          }
        }
        // Adjust location of selected hops...
        if ( selectedNotes != null ) {
          for ( int i = 0; i < selectedNotes.size(); i++ ) {
            NotePadMeta ni = selectedNotes.get( i );
            PropsUi.setLocation( ni, ni.getLocation().x + dx, ni.getLocation().y + dy );
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

    boolean forward = startHopTransform != null;

    TransformMeta fromTransform = candidate.getFromTransform();
    TransformMeta toTransform = candidate.getToTransform();
    if ( fromTransform.equals( toTransform ) ) {
      return; // Don't add
    }

    // See what the options are.
    // - Does the source transform has multiple stream options?
    // - Does the target transform have multiple input stream options?
    //
    List<IStream> streams = new ArrayList<>();

    ITransformIOMeta fromIoMeta = fromTransform.getTransform().getTransformIOMeta();
    List<IStream> targetStreams = fromIoMeta.getTargetStreams();
    if ( forward ) {
      streams.addAll( targetStreams );
    }

    ITransformIOMeta toIoMeta = toTransform.getTransform().getTransformIOMeta();
    List<IStream> infoStreams = toIoMeta.getInfoStreams();
    if ( !forward ) {
      streams.addAll( infoStreams );
    }

    if ( forward ) {
      if ( fromIoMeta.isOutputProducer() && toTransform.equals( currentTransform ) ) {
        streams.add( new Stream( StreamType.OUTPUT, fromTransform, BaseMessages
          .getString( PKG, "HopGui.Hop.MainOutputOfTransform" ), StreamIcon.OUTPUT, null ) );
      }

      if ( fromTransform.supportsErrorHandling() && toTransform.equals( currentTransform ) ) {
        streams.add( new Stream( StreamType.ERROR, fromTransform, BaseMessages.getString( PKG,
          "HopGui.Hop.ErrorHandlingOfTransform" ), StreamIcon.ERROR, null ) );
      }
    } else {
      if ( toIoMeta.isInputAcceptor() && fromTransform.equals( currentTransform ) ) {
        streams.add( new Stream( StreamType.INPUT, toTransform, BaseMessages.getString( PKG, "HopGui.Hop.MainInputOfTransform" ),
          StreamIcon.INPUT, null ) );
      }

      if ( fromTransform.supportsErrorHandling() && fromTransform.equals( currentTransform ) ) {
        streams.add( new Stream( StreamType.ERROR, fromTransform, BaseMessages.getString( PKG,
          "HopGui.Hop.ErrorHandlingOfTransform" ), StreamIcon.ERROR, null ) );
      }
    }

    // Targets can be dynamically added to this transform...
    //
    if ( forward ) {
      streams.addAll( fromTransform.getTransform().getOptionalStreams() );
    } else {
      streams.addAll( toTransform.getTransform().getOptionalStreams() );
    }

    // Show a list of options on the canvas...
    //
    if ( streams.size() > 1 ) {
      // Show a pop-up menu with all the possible options...
      //
      Menu menu = new Menu( canvas );
      for ( final IStream stream : streams ) {
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
     * if (pipelineMeta.findPipelineHop(candidate) == null) { spoon.newHop(pipelineMeta, candidate); } if (startErrorHopTransform) {
     * addErrorHop(); } if (startTargetHopStream != null) { // Auto-configure the target in the source transform... //
     * startTargetHopStream.setTransformMeta(candidate.getToTransform());
     * startTargetHopStream.setTransformName(candidate.getToTransform().getName()); startTargetHopStream = null; }
     */
    candidate = null;
    selectedTransforms = null;
    startHopTransform = null;
    endHopLocation = null;
    startErrorHopTransform = false;

    // redraw();
  }

  private Image getImageFor( IStream stream ) {
    Display disp = hopDisplay();
    SwtUniversalImage swtImage = SwtGc.getNativeImage( BasePainter.getStreamIconImage( stream.getStreamIcon() ) );
    return swtImage.getAsBitmapForSize( disp, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE );
  }

  protected void addHop( IStream stream ) {
    switch ( stream.getStreamType() ) {
      case ERROR:
        addErrorHop();
        candidate.setErrorHop( true );
        pipelineHopDelegate.newHop( pipelineMeta, candidate );
        break;
      case INPUT:
        pipelineHopDelegate.newHop( pipelineMeta, candidate );
        break;
      case OUTPUT:
        TransformErrorMeta transformErrorMeta = candidate.getFromTransform().getTransformErrorMeta();
        if ( transformErrorMeta != null && transformErrorMeta.getTargetTransform() != null ) {
          if ( transformErrorMeta.getTargetTransform().equals( candidate.getToTransform() ) ) {
            candidate.getFromTransform().setTransformErrorMeta( null );
          }
        }
        pipelineHopDelegate.newHop( pipelineMeta, candidate );
        break;
      case INFO:
        stream.setTransformMeta( candidate.getFromTransform() );
        candidate.getToTransform().getTransform().handleStreamSelection( stream );
        pipelineHopDelegate.newHop( pipelineMeta, candidate );
        break;
      case TARGET:
        // We connect a target of the source transform to an output transform...
        //
        stream.setTransformMeta( candidate.getToTransform() );
        candidate.getFromTransform().getTransform().handleStreamSelection( stream );
        pipelineHopDelegate.newHop( pipelineMeta, candidate );
        break;
      default:
        break;

    }
    clearSettings();
  }

  private void addErrorHop() {
    // Automatically configure the transform error handling too!
    //
    if ( candidate == null || candidate.getFromTransform() == null ) {
      return;
    }
    TransformErrorMeta errorMeta = candidate.getFromTransform().getTransformErrorMeta();
    if ( errorMeta == null ) {
      errorMeta = new TransformErrorMeta( pipelineMeta, candidate.getFromTransform() );
    }
    errorMeta.setEnabled( true );
    errorMeta.setTargetTransform( candidate.getToTransform() );
    candidate.getFromTransform().setTransformErrorMeta( errorMeta );
  }

  private void resetDelayTimer( TransformMeta transformMeta ) {
    DelayTimer delayTimer = delayTimers.get( transformMeta );
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
    hopDisplay().asyncExec( new Runnable() {
      @Override
      public void run() {
        if ( !HopGuiPipelineGraph.this.isDisposed() ) {
          HopGuiPipelineGraph.this.redraw();
        }
      }
    } );
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_ZOOM_LEVEL,
    label = "  Zoom: ",
    toolTip = "Zoom in our out",
    type = GuiToolbarElementType.COMBO,
    alignRight = true,
    comboValuesMethod = "getZoomLevels"
  )
  public void zoomLevel() {
    readMagnification();
    redraw();
  }

  public List<String> getZoomLevels() {
    return Arrays.asList( PipelinePainter.magnificationDescriptions );
  }

  private void addToolBar() {

    try {
      // Create a new toolbar at the top of the main composite...
      //
      toolBar = new ToolBar( this, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL );
      toolBarWidgets = new GuiToolbarWidgets();
      toolBarWidgets.createToolbarWidgets( toolBar, GUI_PLUGIN_TOOLBAR_PARENT_ID );
      FormData layoutData = new FormData();
      layoutData.left = new FormAttachment( 0, 0 );
      layoutData.top = new FormAttachment( 0, 0 );
      layoutData.right = new FormAttachment( 100, 0 );
      toolBar.setLayoutData( layoutData );
      toolBar.pack();

      // enable / disable the icons in the toolbar too.
      //
      updateGui();

    } catch ( Throwable t ) {
      log.logError( "Error setting up the navigation toolbar for HopUI", t );
      new ErrorDialog( hopShell(), "Error", "Error setting up the navigation toolbar for HopGUI", new Exception( t ) );
    }
  }

  public void setZoomLabel() {
    Combo combo = (Combo) toolBarWidgets.getWidgetsMap().get( TOOLBAR_ITEM_ZOOM_LEVEL );
    if ( combo == null ) {
      return;
    }
    String newString = Math.round( magnification * 100 ) + "%";
    String oldString = combo.getText();
    if ( !newString.equals( oldString ) ) {
      combo.setText( Math.round( magnification * 100 ) + "%" );
    }
  }

  /**
   * Allows for magnifying to any percentage entered by the user...
   */
  private void readMagnification() {
    float oldMagnification = magnification;
    Combo zoomLabel = (Combo) toolBarWidgets.getWidgetsMap().get( TOOLBAR_ITEM_ZOOM_LEVEL );
    if ( zoomLabel == null ) {
      return;
    }
    String possibleText = zoomLabel.getText().replace( "%", "" );

    float possibleFloatMagnification;
    try {
      possibleFloatMagnification = Float.parseFloat( possibleText ) / 100;
      magnification = possibleFloatMagnification;
      if ( zoomLabel.getText().indexOf( '%' ) < 0 ) {
        zoomLabel.setText( zoomLabel.getText().concat( "%" ) );
      }
    } catch ( Exception e ) {
      modalMessageDialog( BaseMessages.getString( PKG, "PipelineGraph.Dialog.InvalidZoomMeasurement.Title" ),
        BaseMessages.getString( PKG, "PipelineGraph.Dialog.InvalidZoomMeasurement.Message", zoomLabel.getText() ),
        SWT.YES | SWT.ICON_ERROR );
    }

    // When zooming out we want to correct the scroll bars.
    //
    float factor = magnification / oldMagnification;
    int newHThumb = Math.min((int)( horizontalScrollBar.getThumb() / factor), 100);
    horizontalScrollBar.setThumb( newHThumb );
    horizontalScrollBar.setSelection( (int)( horizontalScrollBar.getSelection()*factor ));
    int newVThumb = Math.min((int)( verticalScrollBar.getThumb() / factor), 100);
    verticalScrollBar.setThumb( newVThumb );
    verticalScrollBar.setSelection( (int)( verticalScrollBar.getSelection()*factor ));

    canvas.setFocus();
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
    helpTip.setCheckBoxMessage( BaseMessages.getString( PKG, "PipelineGraph.HelpToolTip.DoNotShowAnyMoreCheckBox.Message" ) );

    org.eclipse.swt.graphics.Point location = new org.eclipse.swt.graphics.Point( x - 5, y - 5 );

    helpTip.show( location );
  }

  /**
   * Select all the transforms in a certain (screen) rectangle
   *
   * @param rect The selection area as a rectangle
   */
  public void selectInRect( PipelineMeta pipelineMeta, org.apache.hop.core.gui.Rectangle rect ) {
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

    for ( int i = 0; i < pipelineMeta.nrTransforms(); i++ ) {
      TransformMeta transformMeta = pipelineMeta.getTransform( i );
      Point a = transformMeta.getLocation();
      if ( rect.contains( a.x, a.y ) ) {
        transformMeta.setSelected( true );
      }
    }

    for ( int i = 0; i < pipelineMeta.nrNotes(); i++ ) {
      NotePadMeta ni = pipelineMeta.getNote( i );
      Point a = ni.getLocation();
      Point b = new Point( a.x + ni.width, a.y + ni.height );
      if ( rect.contains( a.x, a.y ) && rect.contains( b.x, b.y ) ) {
        ni.setSelected( true );
      }
    }
  }

  @Override
  public void keyPressed( KeyEvent e ) {

    if ( e.character == 'E' && ( e.stateMask & SWT.CTRL ) != 0 ) {
      checkErrorVisuals();
    }

    // SPACE : over a transform: show output fields...
    if ( e.character == ' ' && lastMove != null ) {

      Point real = lastMove;

      // Hide the tooltip!
      hideToolTips();

      // Set the pop-up menu
      TransformMeta transformMeta = pipelineMeta.getTransform( real.x, real.y, iconsize );
      if ( transformMeta != null ) {
        // OK, we found a transform, show the output fields...
        inputOutputFields( transformMeta, false );
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

  public void renameTransform( TransformMeta transformMeta, String transformName ) {
    String newname = transformName;

    TransformMeta smeta = pipelineMeta.findTransform( newname, transformMeta );
    int nr = 2;
    while ( smeta != null ) {
      newname = transformName + " " + nr;
      smeta = pipelineMeta.findTransform( newname );
      nr++;
    }
    if ( nr > 2 ) {
      transformName = newname;
      modalMessageDialog( BaseMessages.getString( PKG, "HopGui.Dialog.TransformnameExists.Title" ),
        BaseMessages.getString( PKG, "HopGui.Dialog.TransformnameExists.Message", transformName ), SWT.OK | SWT.ICON_INFORMATION );
    }
    transformMeta.setName( transformName );
    transformMeta.setChanged();
    redraw();
  }

  public void clearSettings() {
    selectedTransform = null;
    noInputTransform = null;
    selectedNote = null;
    selectedTransforms = null;
    selectionRegion = null;
    candidate = null;
    last_hop_split = null;
    lastButton = 0;
    iconOffset = null;
    startHopTransform = null;
    endHopTransform = null;
    endHopLocation = null;
    pipelineMeta.unselectAll();
    for ( int i = 0; i < pipelineMeta.nrPipelineHops(); i++ ) {
      pipelineMeta.getPipelineHop( i ).split = false;
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
   * See if location (x,y) is on a line between two transforms: the hop!
   *
   * @param x
   * @param y
   * @return the pipeline hop on the specified location, otherwise: null
   */
  protected PipelineHopMeta findPipelineHop( int x, int y ) {
    return findPipelineHop( x, y, null );
  }

  /**
   * See if location (x,y) is on a line between two transforms: the hop!
   *
   * @param x
   * @param y
   * @param exclude the transform to exclude from the hops (from or to location). Specify null if no transform is to be excluded.
   * @return the pipeline hop on the specified location, otherwise: null
   */
  private PipelineHopMeta findPipelineHop( int x, int y, TransformMeta exclude ) {
    int i;
    PipelineHopMeta online = null;
    for ( i = 0; i < pipelineMeta.nrPipelineHops(); i++ ) {
      PipelineHopMeta hi = pipelineMeta.getPipelineHop( i );
      TransformMeta fs = hi.getFromTransform();
      TransformMeta ts = hi.getToTransform();

      if ( fs == null || ts == null ) {
        return null;
      }

      // If either the "from" or "to" transform is excluded, skip this hop.
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

  private int[] getLine( TransformMeta fs, TransformMeta ts ) {
    Point from = fs.getLocation();
    Point to = ts.getLocation();
    offset = getOffset();

    int x1 = from.x + iconsize / 2;
    int y1 = from.y + iconsize / 2;

    int x2 = to.x + iconsize / 2;
    int y2 = to.y + iconsize / 2;

    return new int[] { x1, y1, x2, y2 };
  }

  @GuiContextAction(
    id = "pipeline-graph-action-10100-transform-detach",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Detach transform",
    tooltip = "Remove hops to and from this action",
    image = "ui/images/HOP_delete.svg"
  )
  public void detachTransform( HopGuiPipelineTransformContext context ) {
    TransformMeta transformMeta = context.getTransformMeta();
    PipelineHopMeta fromHop = pipelineMeta.findPipelineHopTo( transformMeta );
    PipelineHopMeta toHop = pipelineMeta.findPipelineHopFrom( transformMeta );

    for ( int i = pipelineMeta.nrPipelineHops() - 1; i >= 0; i-- ) {
      PipelineHopMeta hop = pipelineMeta.getPipelineHop( i );
      if ( transformMeta.equals( hop.getFromTransform() ) || transformMeta.equals( hop.getToTransform() ) ) {
        // Transform is connected with a hop, remove this hop.
        //
        hopGui.undoDelegate.addUndoNew( pipelineMeta, new PipelineHopMeta[] { hop }, new int[] { i } );
        pipelineMeta.removePipelineHop( i );
      }
    }

    // If the transform was part of a chain, re-connect it.
    //
    if ( fromHop != null && toHop != null ) {
      pipelineHopDelegate.newHop( pipelineMeta, new PipelineHopMeta( fromHop.getFromTransform(), toHop.getToTransform() ) );
    }

    updateGui();
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-10700-partitioning",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Specify transform partitioning",
    tooltip = "Specify how rows of data need to be grouped into partitions allowing parallel execution where similar rows need to end up on the same transform copy",
    image = "ui/images/partition_schema.svg"
  )
  public void partitioning( HopGuiPipelineTransformContext context ) {
    pipelineTransformDelegate.editTransformPartitioning( pipelineMeta, context.getTransformMeta() );
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-10800-error-handling",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Transform error handling",
    tooltip = "Specify how error handling is behaving for this transform",
    image = "ui/images/transform_error.svg"
  )
  public void errorHandling( HopGuiPipelineTransformContext context ) {
    pipelineTransformDelegate.editTransformErrorHandling( pipelineMeta, context.getTransformMeta() );
  }

  public void newHopChoice() {
    selectedTransforms = null;
    newHop();
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-10000-edit",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Edit the transform",
    tooltip = "Edit the transform properties",
    image = "ui/images/Edit.svg"
  )
  public void editTransform( HopGuiPipelineTransformContext context ) {
    editTransform( context.getTransformMeta() );
  }

  public void editTransform() {
    selectedTransforms = null;
    editTransform( getCurrentTransform() );
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-10800-edit-description",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Edit transform description",
    tooltip = "Modify the transform description",
    image = "ui/images/Edit.svg"
  )
  public void editDescription( HopGuiPipelineTransformContext context ) {
    editDescription( context.getTransformMeta() );
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-10600-rows-distrubute",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Distribute rows",
    tooltip = "Make the transform distribute rows to next transforms",
    image = "ui/images/Edit.svg"
  )
  public void setDistributes( HopGuiPipelineTransformContext context ) {
    context.getTransformMeta().setDistributes( true );
    context.getTransformMeta().setRowDistribution( null );
    redraw();
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-10500-custom-row-distribution",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Specify row distribution",
    tooltip = "Specify how the transform should distribute rows to next transforms",
    image = "ui/images/Edit.svg"
  )
  public void setCustomRowDistribution( HopGuiPipelineTransformContext context ) {
    // ask user which row distribution is needed...
    //
    IRowDistribution rowDistribution = askUserForCustomDistributionMethod();
    context.getTransformMeta().setDistributes( true );
    context.getTransformMeta().setRowDistribution( rowDistribution );
    redraw();
  }

  public IRowDistribution askUserForCustomDistributionMethod() {
    List<IPlugin> plugins = PluginRegistry.getInstance().getPlugins( RowDistributionPluginType.class );
    if ( Utils.isEmpty( plugins ) ) {
      return null;
    }
    List<String> choices = new ArrayList<>();
    for ( IPlugin plugin : plugins ) {
      choices.add( plugin.getName() + " : " + plugin.getDescription() );
    }
    EnterSelectionDialog dialog =
      new EnterSelectionDialog( hopShell(), choices.toArray( new String[ choices.size() ] ), "Select distribution method",
        "Please select the row distribution method:" );
    if ( dialog.open() != null ) {
      IPlugin plugin = plugins.get( dialog.getSelectionNr() );
      try {
        return (IRowDistribution) PluginRegistry.getInstance().loadClass( plugin );
      } catch ( Exception e ) {
        new ErrorDialog( hopShell(), "Error", "Error loading row distribution plugin class", e );
        return null;
      }
    } else {
      return null;
    }
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-10100-copies",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Set the number of transform copies",
    tooltip = "Set the number of transform copies to use during execution",
    image = "ui/images/parallel-hop.svg"
  )
  public void copies( HopGuiPipelineTransformContext context ) {
    TransformMeta transformMeta = context.getTransformMeta();
    copies( transformMeta );
  }

  public void copies( TransformMeta transformMeta ) {
    final boolean multipleOK = checkNumberOfCopies( pipelineMeta, transformMeta );
    selectedTransforms = null;
    String tt = BaseMessages.getString( PKG, "PipelineGraph.Dialog.NrOfCopiesOfTransform.Title" );
    String mt = BaseMessages.getString( PKG, "PipelineGraph.Dialog.NrOfCopiesOfTransform.Message" );
    EnterStringDialog nd = new EnterStringDialog( hopShell(), transformMeta.getCopiesString(), tt, mt, true, pipelineMeta );
    String cop = nd.open();
    if ( !Utils.isEmpty( cop ) ) {

      int copies = Const.toInt( pipelineMeta.environmentSubstitute( cop ), -1 );
      if ( copies > 1 && !multipleOK ) {
        cop = "1";

        modalMessageDialog( BaseMessages.getString( PKG, "PipelineGraph.Dialog.MultipleCopiesAreNotAllowedHere.Title" ),
          BaseMessages.getString( PKG, "PipelineGraph.Dialog.MultipleCopiesAreNotAllowedHere.Message" ), SWT.YES | SWT.ICON_WARNING );
      }
      String cps = transformMeta.getCopiesString();
      if ( ( cps != null && !cps.equals( cop ) ) || ( cps == null && cop != null ) ) {
        transformMeta.setChanged();
      }
      transformMeta.setCopiesString( cop );
      redraw();
    }
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-10900-delete",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Delete,
    name = "Delete this transform",
    tooltip = "Delete the selected transform from the pipeline",
    image = "ui/images/generic-delete.svg"
  )
  public void delTransform( HopGuiPipelineTransformContext context ) {
    delSelected( context.getTransformMeta() );
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-10200-fields-before",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Info,
    name = "Show the fields entering this transform",
    tooltip = "Show all the fields entering this transform",
    image = "ui/images/info-hop.svg"
  )
  public void fieldsBefore( HopGuiPipelineTransformContext context ) {
    selectedTransforms = null;
    inputOutputFields( context.getTransformMeta(), true );
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-10300-fields-after",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Info,
    name = "Show the fields exiting this transform",
    tooltip = "Show all the fields resulting from this transform",
    image = "ui/images/info-hop.svg"
  )
  public void fieldsAfter( HopGuiPipelineTransformContext context ) {
    selectedTransforms = null;
    inputOutputFields( context.getTransformMeta(), false );
  }

  public void fieldsLineage() {
    PipelineDataLineage tdl = new PipelineDataLineage( pipelineMeta );
    try {
      tdl.calculateLineage();
    } catch ( Exception e ) {
      new ErrorDialog( hopShell(), "Lineage error", "Unexpected lineage calculation error", e );
    }
  }

  @GuiContextAction(
    id = "pipeline-graph-hop-10010-hop-enable",
    parentId = HopGuiPipelineHopContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Enable hop",
    tooltip = "Enable the hop",
    image = "ui/images/HOP.svg"
  )
  public void enableHop( HopGuiPipelineHopContext context ) {
    PipelineHopMeta hop = context.getHopMeta();
    if ( !hop.isEnabled() ) {
      PipelineHopMeta before = hop.clone();
      setHopEnabled( hop, true );
      if ( pipelineMeta.hasLoop( hop.getToTransform() ) ) {
        setHopEnabled( hop, false );
        modalMessageDialog( BaseMessages.getString( PKG, "PipelineGraph.Dialog.LoopAfterHopEnabled.Title" ),
          BaseMessages.getString( PKG, "PipelineGraph.Dialog.LoopAfterHopEnabled.Message" ), SWT.OK | SWT.ICON_ERROR );
      } else {
        PipelineHopMeta after = hop.clone();
        hopGui.undoDelegate.addUndoChange( pipelineMeta, new PipelineHopMeta[] { before }, new PipelineHopMeta[] { after }, new int[] { pipelineMeta.indexOfPipelineHop( hop ) } );
        redraw();
      }
    }
    updateErrorMetaForHop( hop );
  }

  @GuiContextAction(
    id = "pipeline-graph-hop-10010-hop-disable",
    parentId = HopGuiPipelineHopContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Disable hop",
    tooltip = "Disable the hop",
    image = "ui/images/HOP_disable.svg"
  )
  public void disableHop( HopGuiPipelineHopContext context ) {
    PipelineHopMeta hopMeta = context.getHopMeta();
    if ( hopMeta.isEnabled() ) {
      PipelineHopMeta before = hopMeta.clone();
      setHopEnabled( hopMeta, false );

      PipelineHopMeta after = hopMeta.clone();
      hopGui.undoDelegate.addUndoChange( pipelineMeta, new PipelineHopMeta[] { before }, new PipelineHopMeta[] { after }, new int[] { pipelineMeta.indexOfPipelineHop( hopMeta ) } );
      redraw();
    }
    updateErrorMetaForHop( hopMeta );
  }

  @GuiContextAction(
    id = "pipeline-graph-hop-10020-hop-delete",
    parentId = HopGuiPipelineHopContext.CONTEXT_ID,
    type = GuiActionType.Delete,
    name = "Delete hop",
    tooltip = "Delete the hop between 2 actions",
    image = "ui/images/HOP_delete.svg"
  )
  public void deleteHop( HopGuiPipelineHopContext context ) {
    pipelineHopDelegate.delHop( pipelineMeta, context.getHopMeta() );
  }

  private void updateErrorMetaForHop( PipelineHopMeta hop ) {
    if ( hop != null && hop.isErrorHop() ) {
      TransformErrorMeta errorMeta = hop.getFromTransform().getTransformErrorMeta();
      if ( errorMeta != null ) {
        errorMeta.setEnabled( hop.isEnabled() );
      }
    }
  }

  // TODO
  public void enableHopsBetweenSelectedTransforms() {
    enableHopsBetweenSelectedTransforms( true );
  }

  // TODO
  public void disableHopsBetweenSelectedTransforms() {
    enableHopsBetweenSelectedTransforms( false );
  }

  /**
   * This method enables or disables all the hops between the selected transforms.
   **/
  public void enableHopsBetweenSelectedTransforms( boolean enabled ) {
    List<TransformMeta> list = pipelineMeta.getSelectedTransforms();

    boolean hasLoop = false;

    for ( int i = 0; i < pipelineMeta.nrPipelineHops(); i++ ) {
      PipelineHopMeta hop = pipelineMeta.getPipelineHop( i );
      if ( list.contains( hop.getFromTransform() ) && list.contains( hop.getToTransform() ) ) {

        PipelineHopMeta before = hop.clone();
        setHopEnabled( hop, enabled );
        PipelineHopMeta after = hop.clone();
        hopGui.undoDelegate.addUndoChange( pipelineMeta, new PipelineHopMeta[] { before }, new PipelineHopMeta[] { after }, new int[] { pipelineMeta.indexOfPipelineHop( hop ) } );

        if ( pipelineMeta.hasLoop( hop.getToTransform() ) ) {
          hasLoop = true;
          setHopEnabled( hop, false );
        }
      }
    }

    if ( enabled && hasLoop ) {
      modalMessageDialog( BaseMessages.getString( PKG, "PipelineGraph.Dialog.HopCausesLoop.Title" ),
        BaseMessages.getString( PKG, "PipelineGraph.Dialog.HopCausesLoop.Message" ), SWT.OK | SWT.ICON_ERROR );
    }

    updateGui();
  }

  @GuiContextAction(
    id = "pipeline-graph-hop-10060-hop-enable-downstream",
    parentId = HopGuiPipelineHopContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Enable downstream hops",
    tooltip = "Enable all disabled downstream hops",
    image = "ui/images/HOP_enable_downstream.svg"
  )
  public void enableHopsDownstream( HopGuiPipelineHopContext context ) {
    enableDisableHopsDownstream( context.getHopMeta(), true );
  }

  @GuiContextAction(
    id = "pipeline-graph-hop-10070-hop-disable-downstream",
    parentId = HopGuiPipelineHopContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Disable downstream hops",
    tooltip = "Disable all enabled downstream hops",
    image = "ui/images/HOP_disable_downstream.svg"
  )
  public void disableHopsDownstream( HopGuiPipelineHopContext context ) {
    enableDisableHopsDownstream( context.getHopMeta(), false );
  }

  public void enableDisableHopsDownstream( PipelineHopMeta hop, boolean enabled ) {
    PipelineHopMeta before = hop.clone();
    setHopEnabled( hop, enabled );
    PipelineHopMeta after = hop.clone();
    hopGui.undoDelegate.addUndoChange( pipelineMeta, new PipelineHopMeta[] { before }, new PipelineHopMeta[] { after }, new int[] { pipelineMeta.indexOfPipelineHop( hop ) } );

    Set<TransformMeta> checkedTransforms = enableDisableNextHops( hop.getToTransform(), enabled, new HashSet<>() );

    if ( checkedTransforms.stream().anyMatch( entry -> pipelineMeta.hasLoop( entry ) ) ) {
      modalMessageDialog( BaseMessages.getString( PKG, "PipelineGraph.Dialog.HopCausesLoop.Title" ),
        BaseMessages.getString( PKG, "PipelineGraph.Dialog.HopCausesLoop.Message" ), SWT.OK | SWT.ICON_ERROR );
    }

    updateGui();
  }

  private Set<TransformMeta> enableDisableNextHops( TransformMeta from, boolean enabled, Set<TransformMeta> checkedEntries ) {
    checkedEntries.add( from );
    pipelineMeta.getPipelineHops().stream()
      .filter( hop -> from.equals( hop.getFromTransform() ) )
      .forEach( hop -> {
        if ( hop.isEnabled() != enabled ) {
          PipelineHopMeta before = hop.clone();
          setHopEnabled( hop, enabled );
          PipelineHopMeta after = hop.clone();
          hopGui.undoDelegate.addUndoChange( pipelineMeta, new PipelineHopMeta[] { before }, new PipelineHopMeta[] { after }, new int[] { pipelineMeta.indexOfPipelineHop( hop ) } );
        }
        if ( !checkedEntries.contains( hop.getToTransform() ) ) {
          enableDisableNextHops( hop.getToTransform(), enabled, checkedEntries );
        }
      } );
    return checkedEntries;
  }

  @GuiContextAction(
    id = "pipeline-graph-edit-note",
    parentId = HopGuiPipelineNoteContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Edit the note",
    tooltip = "Edit the note",
    image = "ui/images/Edit.svg"
  )
  public void editNote( HopGuiPipelineNoteContext context ) {
    selectionRegion = null;
    editNote( context.getNotePadMeta() );
  }

  @GuiContextAction(
    id = "pipeline-graph-delete-note",
    parentId = HopGuiPipelineNoteContext.CONTEXT_ID,
    type = GuiActionType.Delete,
    name = "Delete the note",
    tooltip = "Delete the note",
    image = "ui/images/generic-delete.svg"
  )
  public void deleteNote( HopGuiPipelineNoteContext context ) {
    selectionRegion = null;
    int idx = pipelineMeta.indexOfNote( context.getNotePadMeta() );
    if ( idx >= 0 ) {
      pipelineMeta.removeNote( idx );
      hopGui.undoDelegate.addUndoDelete( pipelineMeta, new NotePadMeta[] { context.getNotePadMeta().clone() }, new int[] { idx } );
      updateGui();
    }
  }

  @GuiContextAction(
    id = "pipeline-graph-new-note",
    parentId = HopGuiPipelineContext.CONTEXT_ID,
    type = GuiActionType.Create,
    name = "Create a note",
    tooltip = "Create a new note",
    image = "ui/images/new.svg"
  )
  public void newNote( HopGuiPipelineContext context ) {
    selectionRegion = null;
    String title = BaseMessages.getString( PKG, "PipelineGraph.Dialog.NoteEditor.Title" );
    NotePadDialog dd = new NotePadDialog( pipelineMeta, hopShell(), title );
    NotePadMeta n = dd.open();
    if ( n != null ) {
      NotePadMeta npi =
        new NotePadMeta( n.getNote(), context.getClick().x, context.getClick().y, ConstUi.NOTE_MIN_SIZE, ConstUi.NOTE_MIN_SIZE, n
          .getFontName(), n.getFontSize(), n.isFontBold(), n.isFontItalic(), n.getFontColorRed(), n
          .getFontColorGreen(), n.getFontColorBlue(), n.getBackGroundColorRed(), n.getBackGroundColorGreen(), n
          .getBackGroundColorBlue(), n.getBorderColorRed(), n.getBorderColorGreen(), n.getBorderColorBlue()
        );
      pipelineMeta.addNote( npi );
      hopGui.undoDelegate.addUndoNew( pipelineMeta, new NotePadMeta[] { npi }, new int[] { pipelineMeta.indexOfNote( npi ) } );
      updateGui();
    }
  }

  @GuiContextAction(
    id = "pipeline-graph-edit-pipeline",
    parentId = HopGuiPipelineContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Edit pipeline",
    tooltip = "Edit pipeline properties",
    image = "ui/images/toolbar/pipeline.svg"
  )
  public void editPipelineProperties( HopGuiPipelineContext context ) {
    editProperties( pipelineMeta, hopGui, true );
  }

  public void newTransform( String description ) {
    TransformMeta transformMeta = pipelineTransformDelegate.newTransform( pipelineMeta, null, description, description, false, true, new Point( currentMouseX, currentMouseY ) );
    PropsUi.setLocation( transformMeta, currentMouseX, currentMouseY );
    updateGui();
  }

  private boolean checkNumberOfCopies( PipelineMeta pipelineMeta, TransformMeta transformMeta ) {
    boolean enabled = true;
    List<TransformMeta> prevTransforms = pipelineMeta.findPreviousTransforms( transformMeta );
    for ( TransformMeta prevTransform : prevTransforms ) {
      // See what the target transforms are.
      // If one of the target transforms is our original transform, we can't start multiple copies
      //
      String[] targetTransforms = prevTransform.getTransform().getTransformIOMeta().getTargetTransformNames();
      if ( targetTransforms != null ) {
        for ( int t = 0; t < targetTransforms.length && enabled; t++ ) {
          if ( !Utils.isEmpty( targetTransforms[ t ] ) && targetTransforms[ t ].equalsIgnoreCase( transformMeta.getName() ) ) {
            enabled = false;
          }
        }
      }
    }
    return enabled;
  }

  private AreaOwner setToolTip( int x, int y, int screenX, int screenY ) {
    AreaOwner subject = null;

    if ( !hopGui.getProps().showToolTips() ) {
      return subject;
    }

    canvas.setToolTipText( null );

    String newTip = null;
    Image tipImage = null;

    final PipelineHopMeta hi = findPipelineHop( x, y );
    // check the area owner list...
    //
    StringBuilder tip = new StringBuilder();
    AreaOwner areaOwner = getVisibleAreaOwner( x, y );
    AreaType areaType = null;
    if ( areaOwner != null && areaOwner.getAreaType() != null ) {
      areaType = areaOwner.getAreaType();
      switch ( areaType ) {
        case TRANSFORM_PARTITIONING:
          TransformMeta transform = (TransformMeta) areaOwner.getParent();
          tip.append( "Transform partitioning:" ).append( Const.CR ).append( "-----------------------" ).append( Const.CR );
          tip.append( transform.getTransformPartitioningMeta().toString() ).append( Const.CR );
          if ( transform.getTargetTransformPartitioningMeta() != null ) {
            tip.append( Const.CR ).append( Const.CR ).append(
              "TARGET: " + transform.getTargetTransformPartitioningMeta().toString() ).append( Const.CR );
          }
          break;
        case TRANSFORM_ERROR_ICON:
          String log = (String) areaOwner.getParent();
          tip.append( log );
          tipImage = GuiResource.getInstance().getImageTransformError();
          break;
        case TRANSFORM_ERROR_RED_ICON:
          String redLog = (String) areaOwner.getParent();
          tip.append( redLog );
          tipImage = GuiResource.getInstance().getImageRedTransformError();
          break;
        case HOP_COPY_ICON:
          transform = (TransformMeta) areaOwner.getParent();
          tip.append( BaseMessages.getString( PKG, "PipelineGraph.Hop.Tooltip.HopTypeCopy", transform.getName(), Const.CR ) );
          tipImage = GuiResource.getInstance().getImageCopyHop();
          break;
        case ROW_DISTRIBUTION_ICON:
          transform = (TransformMeta) areaOwner.getParent();
          tip.append( BaseMessages.getString( PKG, "PipelineGraph.Hop.Tooltip.RowDistribution", transform.getName(), transform
            .getRowDistribution() == null ? "" : transform.getRowDistribution().getDescription() ) );
          tip.append( Const.CR );
          tipImage = GuiResource.getInstance().getImageBalance();
          break;
        case HOP_INFO_ICON:
          TransformMeta from = (TransformMeta) areaOwner.getParent();
          TransformMeta to = (TransformMeta) areaOwner.getOwner();
          tip.append( BaseMessages.getString( PKG, "PipelineGraph.Hop.Tooltip.HopTypeInfo", to.getName(), from.getName(),
            Const.CR ) );
          tipImage = GuiResource.getInstance().getImageInfoHop();
          break;
        case HOP_ERROR_ICON:
          from = (TransformMeta) areaOwner.getParent();
          to = (TransformMeta) areaOwner.getOwner();
          areaOwner.getOwner();
          tip.append( BaseMessages.getString( PKG, "PipelineGraph.Hop.Tooltip.HopTypeError", from.getName(), to.getName(),
            Const.CR ) );
          tipImage = GuiResource.getInstance().getImageErrorHop();
          break;
        case HOP_INFO_TRANSFORM_COPIES_ERROR:
          from = (TransformMeta) areaOwner.getParent();
          to = (TransformMeta) areaOwner.getOwner();
          tip.append( BaseMessages.getString( PKG, "PipelineGraph.Hop.Tooltip.InfoTransformCopies", from.getName(), to
            .getName(), Const.CR ) );
          tipImage = GuiResource.getInstance().getImageTransformError();
          break;
        case TRANSFORM_INPUT_HOP_ICON:
          // TransformMeta subjectTransform = (TransformMeta) (areaOwner.getParent());
          tip.append( BaseMessages.getString( PKG, "PipelineGraph.TransformInputConnector.Tooltip" ) );
          tipImage = GuiResource.getInstance().getImageHopInput();
          break;
        case TRANSFORM_OUTPUT_HOP_ICON:
          // subjectTransform = (TransformMeta) (areaOwner.getParent());
          tip.append( BaseMessages.getString( PKG, "PipelineGraph.TransformOutputConnector.Tooltip" ) );
          tipImage = GuiResource.getInstance().getImageHopOutput();
          break;
        case TRANSFORM_INFO_HOP_ICON:
          // subjectTransform = (TransformMeta) (areaOwner.getParent());
          // IStream stream = (IStream) areaOwner.getOwner();
          ITransformIOMeta ioMeta = (ITransformIOMeta) areaOwner.getOwner();
          tip.append( BaseMessages.getString( PKG, "PipelineGraph.TransformMetaConnector.Tooltip" ) + Const.CR
            + ioMeta.toString() );
          tipImage = GuiResource.getInstance().getImageHopOutput();
          break;
        case TRANSFORM_TARGET_HOP_ICON:
          IStream stream = (IStream) areaOwner.getOwner();
          tip.append( stream.getDescription() );
          tipImage = GuiResource.getInstance().getImageHopOutput();
          break;
        case TRANSFORM_ERROR_HOP_ICON:
          TransformMeta transformMeta = (TransformMeta) areaOwner.getParent();
          if ( transformMeta.supportsErrorHandling() ) {
            tip.append( BaseMessages.getString( PKG, "PipelineGraph.TransformSupportsErrorHandling.Tooltip" ) );
          } else {
            tip.append( BaseMessages.getString( PKG, "PipelineGraph.TransformDoesNotSupportsErrorHandling.Tooltip" ) );
          }
          tipImage = GuiResource.getInstance().getImageHopOutput();
          break;
        case TRANSFORM_EDIT_ICON:
          tip.append( BaseMessages.getString( PKG, "PipelineGraph.EditTransform.Tooltip" ) );
          tipImage = GuiResource.getInstance().getImageEdit();
          break;
        case TRANSFORM_INJECT_ICON:
          Object injection = areaOwner.getOwner();
          if ( injection != null ) {
            tip.append( BaseMessages.getString( PKG, "PipelineGraph.TransformInjectionSupported.Tooltip" ) );
          } else {
            tip.append( BaseMessages.getString( PKG, "PipelineGraph.TransformInjectionNotSupported.Tooltip" ) );
          }
          tipImage = GuiResource.getInstance().getImageInject();
          break;
        case TRANSFORM_MENU_ICON:
          tip.append( BaseMessages.getString( PKG, "PipelineGraph.ShowMenu.Tooltip" ) );
          tipImage = GuiResource.getInstance().getImageContextMenu();
          break;
        case TRANSFORM_ICON:
          TransformMeta iconTransformMeta = (TransformMeta) areaOwner.getOwner();
          if ( iconTransformMeta.isDeprecated() ) { // only need tooltip if transform is deprecated
            tip.append( BaseMessages.getString( PKG, "PipelineGraph.DeprecatedTransform.Tooltip.Title" ) ).append( Const.CR );
            String tipNext = BaseMessages.getString( PKG, "PipelineGraph.DeprecatedTransform.Tooltip.Message1",
              iconTransformMeta.getName() );
            int length = tipNext.length() + 5;
            for ( int i = 0; i < length; i++ ) {
              tip.append( "-" );
            }
            tip.append( Const.CR ).append( tipNext ).append( Const.CR );
            tip.append( BaseMessages.getString( PKG, "PipelineGraph.DeprecatedTransform.Tooltip.Message2" ) );
            if ( !Utils.isEmpty( iconTransformMeta.getSuggestion() )
              && !( iconTransformMeta.getSuggestion().startsWith( "!" ) && iconTransformMeta.getSuggestion().endsWith( "!" ) ) ) {
              tip.append( " " );
              tip.append( BaseMessages.getString( PKG, "PipelineGraph.DeprecatedTransform.Tooltip.Message3",
                iconTransformMeta.getSuggestion() ) );
            }
            tipImage = GuiResource.getInstance().getImageDeprecated();
            toolTip.setHideDelay( TOOLTIP_HIDE_DELAY_LONG );
          }
          break;
        default:
          break;
      }
    }

    if ( hi != null && tip.length() == 0 ) { // We clicked on a HOP!
      // Set the tooltip for the hop:
      tip.append( Const.CR ).append( BaseMessages.getString( PKG, "PipelineGraph.Dialog.HopInfo" ) ).append(
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
          BaseMessages.getString( PKG, "PipelineGraph.Dialog.HopInfo" )
            + Const.CR
            + BaseMessages.getString( PKG, "PipelineGraph.Dialog.HopInfo.SourceTransform" )
            + " "
            + hi.getFromTransform().getName()
            + Const.CR
            + BaseMessages.getString( PKG, "PipelineGraph.Dialog.HopInfo.TargetTransform" )
            + " "
            + hi.getToTransform().getName()
            + Const.CR
            + BaseMessages.getString( PKG, "PipelineGraph.Dialog.HopInfo.Status" )
            + " "
            + ( hi.isEnabled() ? BaseMessages.getString( PKG, "PipelineGraph.Dialog.HopInfo.Enable" ) : BaseMessages
            .getString( PKG, "PipelineGraph.Dialog.HopInfo.Disable" ) );
        toolTip.setText( newTip );
        if ( hi.isEnabled() ) {
          toolTip.setImage( GuiResource.getInstance().getImageHop() );
        } else {
          toolTip.setImage( GuiResource.getInstance().getImageDisabledHop() );
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
        tooltipImage = GuiResource.getInstance().getImageHopUi();
      }
      showTooltip( newTip, tooltipImage, screenX, screenY );
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

  public void delSelected( TransformMeta transformMeta ) {
    List<TransformMeta> selection = pipelineMeta.getSelectedTransforms();
    if ( currentTransform == null && transformMeta == null && selection.isEmpty() ) {
      return; // nothing to do
    }
    if ( transformMeta != null && selection.size() == 0 ) {
      pipelineTransformDelegate.delTransform( pipelineMeta, transformMeta );
      return;
    }

    if ( currentTransform != null && selection.contains( currentTransform ) ) {
      currentTransform = null;
      for ( ITransformSelectionListener listener : currentTransformListeners ) {
        listener.onUpdateSelection( currentTransform );
      }
    }

    pipelineTransformDelegate.delTransforms( pipelineMeta, selection );
    notePadDelegate.deleteNotes( pipelineMeta, pipelineMeta.getSelectedNotes() );
  }

  public void editDescription( TransformMeta transformMeta ) {
    String title = BaseMessages.getString( PKG, "PipelineGraph.Dialog.TransformDescription.Title" );
    String message = BaseMessages.getString( PKG, "PipelineGraph.Dialog.TransformDescription.Message" );
    EnterTextDialog dd = new EnterTextDialog( hopShell(), title, message, transformMeta.getDescription() );
    String d = dd.open();
    if ( d != null ) {
      transformMeta.setDescription( d );
      transformMeta.setChanged();
      updateGui();
    }
  }

  /**
   * Display the input- or outputfields for a transform.
   *
   * @param transformMeta The transform (it's metadata) to query
   * @param before        set to true if you want to have the fields going INTO the transform, false if you want to see all the
   *                      fields that exit the transform.
   */
  private void inputOutputFields( TransformMeta transformMeta, boolean before ) {
    redraw();

    SearchFieldsProgressDialog op = new SearchFieldsProgressDialog( pipelineMeta, transformMeta, before );
    boolean alreadyThrownError = false;
    try {
      final ProgressMonitorDialog pmd = new ProgressMonitorDialog( hopShell() );

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
              pipelineMeta.cancelQueries();
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
      new ErrorDialog( hopShell(), BaseMessages.getString( PKG, "PipelineGraph.Dialog.GettingFields.Title" ), BaseMessages
        .getString( PKG, "PipelineGraph.Dialog.GettingFields.Message" ), e );
      alreadyThrownError = true;
    } catch ( InterruptedException e ) {
      new ErrorDialog( hopShell(), BaseMessages.getString( PKG, "PipelineGraph.Dialog.GettingFields.Title" ), BaseMessages
        .getString( PKG, "PipelineGraph.Dialog.GettingFields.Message" ), e );
      alreadyThrownError = true;
    }

    IRowMeta fields = op.getFields();

    if ( fields != null && fields.size() > 0 ) {
      TransformFieldsDialog sfd = new TransformFieldsDialog( hopShell(), pipelineMeta, SWT.NONE, transformMeta.getName(), fields );
      String sn = (String) sfd.open();
      if ( sn != null ) {
        TransformMeta esi = pipelineMeta.findTransform( sn );
        if ( esi != null ) {
          editTransform( esi );
        }
      }
    } else {
      if ( !alreadyThrownError ) {
        modalMessageDialog( BaseMessages.getString( PKG, "PipelineGraph.Dialog.CouldntFindFields.Title" ),
          BaseMessages.getString( PKG, "PipelineGraph.Dialog.CouldntFindFields.Message" ), SWT.OK | SWT.ICON_INFORMATION );
      }
    }

  }

  public void paintControl( PaintEvent e ) {
    Point area = getArea();
    if ( area.x == 0 || area.y == 0 ) {
      return; // nothing to do!
    }

    Display display = hopDisplay();

    Image img = getPipelineImage( display, area.x, area.y, magnification );
    e.gc.drawImage( img, 0, 0 );
    if ( pipelineMeta.nrTransforms() == 0 ) {
      e.gc.setForeground( GuiResource.getInstance().getColorCrystalText() );
      e.gc.setFont( GuiResource.getInstance().getFontMedium() );

      Image welcomeImage = GuiResource.getInstance().getImagePipelineCanvas();
      int leftPosition = ( area.x - welcomeImage.getBounds().width ) / 2;
      int topPosition = ( area.y - welcomeImage.getBounds().height ) / 2;
      e.gc.drawImage( welcomeImage, leftPosition, topPosition );
    }
    img.dispose();
  }

  public Image getPipelineImage( Device device, int x, int y, float magnificationFactor ) {

    IGc gc = new SwtGc( device, new Point( x, y ), iconsize );

    int gridSize =
      PropsUi.getInstance().isShowCanvasGridEnabled() ? PropsUi.getInstance().getCanvasGridSize() : 1;

    PipelinePainter pipelinePainter = new PipelinePainter( gc, pipelineMeta, new Point( x, y ), new SwtScrollBar( horizontalScrollBar ), new SwtScrollBar( verticalScrollBar ),
      candidate, drop_candidate, selectionRegion, areaOwners,
      PropsUi.getInstance().getIconSize(), PropsUi.getInstance().getLineWidth(), gridSize,
      PropsUi.getInstance().getNoteFont().getName(), PropsUi.getInstance()
      .getNoteFont().getHeight(), pipeline, PropsUi.getInstance().isIndicateSlowPipelineTransformsEnabled(), PropsUi.getInstance().getZoomFactor() );

    // correct the magnification with the overall zoom factor
    //
    float correctedMagnification = (float) ( magnificationFactor * PropsUi.getInstance().getZoomFactor() );

    pipelinePainter.setMagnification( correctedMagnification );
    pipelinePainter.setTransformLogMap( transformLogMap );
    pipelinePainter.setStartHopTransform( startHopTransform );
    pipelinePainter.setEndHopLocation( endHopLocation );
    pipelinePainter.setNoInputTransform( noInputTransform );
    pipelinePainter.setEndHopTransform( endHopTransform );
    pipelinePainter.setCandidateHopType( candidateHopType );
    pipelinePainter.setStartErrorHopTransform( startErrorHopTransform );

    pipelinePainter.buildPipelineImage();

    Image img = (Image) gc.getImage();

    gc.dispose();
    return img;
  }

  @Override
  protected Point getOffset() {
    Point area = getArea();
    Point max = pipelineMeta.getMaximum();
    Point thumb = getThumb( area, max );
    return getOffset( thumb, area );
  }

  private void editTransform( TransformMeta transformMeta ) {
    pipelineTransformDelegate.editTransform( pipelineMeta, transformMeta );
  }

  private void editNote( NotePadMeta ni ) {
    NotePadMeta before = ni.clone();

    String title = BaseMessages.getString( PKG, "PipelineGraph.Dialog.EditNote.Title" );
    NotePadDialog dd = new NotePadDialog( pipelineMeta, hopShell(), title, ni );
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
      ni.width = ConstUi.NOTE_MIN_SIZE;
      ni.height = ConstUi.NOTE_MIN_SIZE;

      NotePadMeta after = (NotePadMeta) ni.clone();
      hopGui.undoDelegate.addUndoChange( pipelineMeta, new NotePadMeta[] { before }, new NotePadMeta[] { after }, new int[] { pipelineMeta.indexOfNote( ni ) } );
      updateGui();
    }
  }

  private void editHop( PipelineHopMeta pipelineHopMeta ) {
    String name = pipelineHopMeta.toString();
    if ( log.isDebug() ) {
      log.logDebug( BaseMessages.getString( PKG, "PipelineGraph.Logging.EditingHop" ) + name );
    }
    pipelineHopDelegate.editHop( pipelineMeta, pipelineHopMeta );
  }

  private void newHop() {
    List<TransformMeta> selection = pipelineMeta.getSelectedTransforms();
    if ( selection.size() == 2 ) {
      TransformMeta fr = selection.get( 0 );
      TransformMeta to = selection.get( 1 );
      pipelineHopDelegate.newHop( pipelineMeta, fr, to );
    }
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-10050-create-hop",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Create,
    name = "Create hop",
    tooltip = "Create a new hop between 2 transforms",
    image = "ui/images/HOP.svg"
  )
  public void newHopCandidate( HopGuiPipelineTransformContext context ) {
    startHopTransform = context.getTransformMeta();
    endHopTransform = null;
    redraw();
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
    List<TransformMeta> selection = pipelineMeta.getSelectedTransforms();
    int[] indices = pipelineMeta.getTransformIndexes( selection );

    return new SnapAllignDistribute( pipelineMeta, selection, indices, hopGui.undoDelegate, this );
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_SNAP_TO_GRID,
    // label = "Snap to grid",
    toolTip = "Align the selected transforms to the specified grid size",
    image = "ui/images/toolbar/snap-to-grid.svg",
    disabledImage = "ui/images/toolbar/snap-to-grid-disabled.svg"
  )
  @GuiKeyboardShortcut( control=true, key=SWT.HOME )
  @GuiOsxKeyboardShortcut( command=true, key=SWT.HOME )
  public void snapToGrid() {
    snapToGrid( ConstUi.GRID_SIZE );
  }

  private void snapToGrid( int size ) {
    createSnapAllignDistribute().snapToGrid( size );
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_ALIGN_LEFT,
    toolTip = "Align the transforms with the left-most transform in your selection",
    image = "ui/images/toolbar/align-left.svg",
    disabledImage = "ui/images/toolbar/align-left-disabled.svg"
  )
  @GuiKeyboardShortcut( control=true, key=SWT.ARROW_LEFT )
  @GuiOsxKeyboardShortcut( command=true, key=SWT.ARROW_LEFT )
  public void alignLeft() {
    createSnapAllignDistribute().allignleft();
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_ALIGN_RIGHT,
    toolTip = "Align the transforms with the right-most transform in your selection",
    image = "ui/images/toolbar/align-right.svg",
    disabledImage = "ui/images/toolbar/align-right-disabled.svg"
  )
  @GuiKeyboardShortcut( control=true, key=SWT.ARROW_RIGHT )
  @GuiOsxKeyboardShortcut( command=true, key=SWT.ARROW_RIGHT )
  public void alignRight() {
    createSnapAllignDistribute().allignright();
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_ALIGN_TOP,
    toolTip = "Align the transforms with the top-most transform in your selection",
    image = "ui/images/toolbar/align-top.svg",
    disabledImage = "ui/images/toolbar/align-top-disabled.svg"
  )
  @GuiKeyboardShortcut( control=true, key=SWT.ARROW_UP )
  @GuiOsxKeyboardShortcut( command=true, key=SWT.ARROW_UP )
  public void alignTop() {
    createSnapAllignDistribute().alligntop();
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_ALIGN_BOTTOM,
    // label = "Bottom-align selected transforms",
    toolTip = "Align the transforms with the bottom-most transform in your selection",
    image = "ui/images/toolbar/align-bottom.svg",
    disabledImage = "ui/images/toolbar/align-bottom-disabled.svg"
  )
  @GuiKeyboardShortcut( control=true, key=SWT.ARROW_DOWN )
  @GuiOsxKeyboardShortcut( command=true, key=SWT.ARROW_DOWN )
  public void alignBottom() {
    createSnapAllignDistribute().allignbottom();
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_DISTRIBUTE_HORIZONTALLY,
    // label = "Horizontally distribute selected transforms",
    toolTip = "Distribute the selected transforms evenly between the left-most and right-most transform in your selection",
    image = "ui/images/toolbar/distribute-horizontally.svg",
    disabledImage = "ui/images/toolbar/distribute-horizontally-disabled.svg"
  )
  @GuiKeyboardShortcut( alt=true, key=SWT.ARROW_RIGHT )
  @GuiOsxKeyboardShortcut( alt=true, key=SWT.ARROW_RIGHT )
  public void distributeHorizontal() {
    createSnapAllignDistribute().distributehorizontal();
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_DISTRIBUTE_VERTICALLY,
    // label = "Vertically distribute selected transforms",
    toolTip = "Distribute the selected transforms evenly between the top-most and bottom-most transform in your selection",
    image = "ui/images/toolbar/distribute-vertically.svg",
    disabledImage = "ui/images/toolbar/distribute-vertically-disabled.svg"
  )
  @GuiKeyboardShortcut( alt=true, key=SWT.ARROW_UP )
  @GuiOsxKeyboardShortcut( alt=true, key=SWT.ARROW_UP )
  public void distributeVertical() {
    createSnapAllignDistribute().distributevertical();
  }


  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_PREVIEW,
    // label = "Preview",
    toolTip = "Preview the pipeline",
    image = "ui/images/preview.svg"
  )
  @Override
  public void preview() {
    try {
      pipelineRunDelegate.executePipeline( hopGui.getLog(), pipelineMeta, true, false, pipelineRunDelegate.getPipelinePreviewExecutionConfiguration().getLogLevel() );
    } catch ( Exception e ) {
      new ErrorDialog( hopShell(), "Error", "Error previewing pipeline", e );
    }
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-10100-preview-output",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Info,
    name = "Preview output",
    tooltip = "Execute the pipeline and see the output of the transform",
    image = "ui/images/preview.svg"
  )
  /**
   *
   * Preview a single step
   */
  public void preview( HopGuiPipelineTransformContext context ) {
    try {
      context.getPipelineMeta().unselectAll();
      context.getTransformMeta().setSelected( true );
      pipelineRunDelegate.executePipeline( hopGui.getLog(), pipelineMeta, true, false, pipelineRunDelegate.getPipelinePreviewExecutionConfiguration().getLogLevel() );
    } catch ( Exception e ) {
      new ErrorDialog( hopShell(), "Error", "Error previewing pipeline", e );
    }
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_DEBUG,
    // label = "Debug",
    toolTip = "Debug the pipeline",
    image = "ui/images/debug.svg"
  )
  @Override
  public void debug() {
    try {
      pipelineRunDelegate.executePipeline( hopGui.getLog(), pipelineMeta, false, true, pipelineRunDelegate.getPipelineDebugExecutionConfiguration().getLogLevel() );
    } catch ( Exception e ) {
      new ErrorDialog( hopShell(), "Error", "Error debugging pipeline", e );
    }
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-10150-debug-output",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Info,
    name = "Debug output",
    tooltip = "Execute the pipeline and debug output of the transform. Pause the pipeline when the condition is met.",
    image = "ui/images/debug.svg"
  )
  /**
   *
   * Debug a single step
   */
  public void debug( HopGuiPipelineTransformContext context ) {
    try {
      context.getPipelineMeta().unselectAll();
      context.getTransformMeta().setSelected( true );
      pipelineRunDelegate.executePipeline( hopGui.getLog(), pipelineMeta, false, debug, pipelineRunDelegate.getPipelinePreviewExecutionConfiguration().getLogLevel() );
    } catch ( Exception e ) {
      new ErrorDialog( hopShell(), "Error", "Error previewing pipeline", e );
    }
  }


  public void newProps() {
    iconsize = hopGui.getProps().getIconSize();
  }

  public IEngineMeta getMeta() {
    return pipelineMeta;
  }

  /**
   * @param pipelineMeta the pipelineMeta to set
   * @return the pipelineMeta / public PipelineMeta getPipelineMeta() { return pipelineMeta; }
   * <p/>
   * /**
   */
  public void setPipelineMeta( PipelineMeta pipelineMeta ) {
    this.pipelineMeta = pipelineMeta;
  }

  @Override public String getName() {
    return pipelineMeta.getName();
  }

  @Override public void setName( String name ) {
    pipelineMeta.setName( name );
  }

  @Override public void setFilename( String filename ) {
    pipelineMeta.setFilename( filename );
  }

  @Override public String getFilename() {
    return pipelineMeta.getFilename();
  }

  public boolean canBeClosed() {
    return !pipelineMeta.hasChanged();
  }

  public PipelineMeta getManagedObject() {
    return pipelineMeta;
  }

  public boolean hasContentChanged() {
    return pipelineMeta.hasChanged();
  }

  public List<ICheckResult> getRemarks() {
    return remarks;
  }

  public void setRemarks( List<ICheckResult> remarks ) {
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

  public boolean editProperties( PipelineMeta pipelineMeta, HopGui hopGui, boolean allowDirectoryChange ) {
    return editProperties( pipelineMeta, hopGui, allowDirectoryChange, null );

  }

  public boolean editProperties( PipelineMeta pipelineMeta, HopGui hopGui, boolean allowDirectoryChange, PipelineDialog.Tabs currentTab ) {
    if ( pipelineMeta == null ) {
      return false;
    }

    PipelineDialog tid = new PipelineDialog( hopGui.getShell(), SWT.NONE, pipelineMeta, currentTab );
    if ( tid.open() != null ) {
      hopGui.setParametersAsVariablesInUI( pipelineMeta, pipelineMeta );
      updateGui();
      perspective.updateTabs();
      return true;
    }
    return false;
  }

  @Override public boolean hasChanged() {
    return pipelineMeta.hasChanged();
  }

  @Override
  public void save() throws HopException {
    try {
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.PipelineBeforeSave.id, pipelineMeta );

      if ( StringUtils.isEmpty( pipelineMeta.getFilename() ) ) {
        throw new HopException( "No filename: please specify a filename for this pipeline" );
      }

      String xml = pipelineMeta.getXml();
      OutputStream out = HopVfs.getOutputStream( pipelineMeta.getFilename(), false );
      try {
        out.write( XmlHandler.getXmlHeader( Const.XML_ENCODING ).getBytes( Const.XML_ENCODING ) );
        out.write( xml.getBytes( Const.XML_ENCODING ) );
        pipelineMeta.clearChanged();
        updateGui();
        HopDataOrchestrationPerspective.getInstance().updateTabs();
      } finally {
        out.flush();
        out.close();

        ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.PipelineAfterSave.id, pipelineMeta );
      }
    } catch ( Exception e ) {
      throw new HopException( "Error saving pipeline to file '" + pipelineMeta.getFilename() + "'", e );
    }
  }

  @Override
  public void saveAs( String filename ) throws HopException {

    try {
      FileObject fileObject = HopVfs.getFileObject( filename );
      if ( fileObject.exists() ) {
        MessageBox box = new MessageBox( hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION );
        box.setText( "Overwrite?" );
        box.setMessage( "Are you sure you want to overwrite file '" + filename + "'?" );
        int answer = box.open();
        if ( ( answer & SWT.YES ) == 0 ) {
          return;
        }
      }

      pipelineMeta.setFilename( filename );
      save();
    } catch ( Exception e ) {
      new HopException( "Error validating file existence for '" + filename + "'", e );
    }

  }

  public void close() {
    hopGui.menuFileClose();
  }

  @Override public boolean isCloseable() {
    try {
      // Check if the file is saved. If not, ask for it to be saved.
      //
      if ( pipelineMeta.hasChanged() ) {

        MessageBox messageDialog = new MessageBox( hopShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO | SWT.CANCEL );
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
      new ErrorDialog( hopShell(), "Error", "Error preparing file close", e );
    }
    return false;
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_START,
    toolTip = "Start the execution of the pipeline",
    image = "ui/images/toolbar/run.svg"
  )
  @Override
  public void start() {
    try {
      pipelineMeta.setShowDialog( pipelineMeta.isAlwaysShowRunOptions() );
      Thread thread = new Thread( () -> getDisplay().asyncExec( () -> {
        try {
          if ( isRunning() && pipeline.isPaused() ) {
            pauseResume();
          } else {
            pipelineRunDelegate.executePipeline( hopGui.getLog(), pipelineMeta, false, false, LogLevel.BASIC );
          }
        } catch ( Throwable e ) {
          new ErrorDialog( getShell(), "Execute pipeline", "There was an error during pipeline execution", e );
        }
      } ) );
      thread.start();
    } catch ( Throwable e ) {
      log.logError( "Severe error in pipeline execution detected", e );
    }
  }

  @Override
  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_PAUSE,
    // label = "Pause",
    toolTip = "Pause the execution of the pipeline",
    image = "ui/images/toolbar/pause.svg"
  )
  public void pause() {
    pauseResume();
  }

  @Override public void resume() {
    pauseResume();
  }

  /* TODO: re-introduce
  public void checkPipeline() {
    hopGui.checkPipeline();
  }
  */

  /** TODO: re-introduce
   public void analyseImpact() {
   hopGui.analyseImpact();
   }
   */

  /**
   * TODO: re-introduce
   * public void getSql() {
   * hopGui.getSql();
   * }
   */

   /* TODO: re-introduce
  public void exploreDatabase() {
    hopGui.exploreDatabase();
  }
   */
  public boolean isExecutionResultsPaneVisible() {
    return extraViewComposite != null && !extraViewComposite.isDisposed();
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS,
    // label = "HopGui.Menu.ShowExecutionResults",
    toolTip = "HopGui.Tooltip.ShowExecutionResults",
    i18nPackageClass = HopGui.class,
    image = "ui/images/show-results.svg",
    separator = true
  )
  public void showExecutionResults() {
    ToolItem item = toolBarWidgets.findToolItem( TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS );
    if ( isExecutionResultsPaneVisible() ) {
      disposeExtraView();
    } else {
      addAllTabs();
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

    ToolItem item = toolBarWidgets.findToolItem( TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS );
    item.setToolTipText( BaseMessages.getString( PKG, "HopGui.Tooltip.ShowExecutionResults" ) );
    item.setImage( GuiResource.getInstance().getImageShowResults() );
  }

  private void minMaxExtraView() {
    // What is the state?
    //
    boolean maximized = sashForm.getMaximizedControl() != null;
    if ( maximized ) {
      // Minimize
      //
      sashForm.setMaximizedControl( null );
      minMaxButton.setImage( GuiResource.getInstance().getImageMaximizePanel() );
      minMaxButton.setToolTipText( BaseMessages.getString( PKG, "PipelineGraph.ExecutionResultsPanel.MaxButton.Tooltip" ) );
    } else {
      // Maximize
      //
      sashForm.setMaximizedControl( extraViewComposite );
      minMaxButton.setImage( GuiResource.getInstance().getImageMinimizePanel() );
      minMaxButton.setToolTipText( BaseMessages.getString( PKG, "PipelineGraph.ExecutionResultsPanel.MinButton.Tooltip" ) );
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
    PropsUi props = PropsUi.getInstance();

    extraViewComposite = new Composite( sashForm, SWT.NONE );
    FormLayout extraCompositeFormLayout = new FormLayout();
    extraCompositeFormLayout.marginWidth = 2;
    extraCompositeFormLayout.marginHeight = 2;
    extraViewComposite.setLayout( extraCompositeFormLayout );

    // Put a close and max button to the upper right corner...
    //
    closeButton = new Label( extraViewComposite, SWT.NONE );
    closeButton.setImage( GuiResource.getInstance().getImageClosePanel() );
    closeButton.setToolTipText( BaseMessages.getString( PKG, "PipelineGraph.ExecutionResultsPanel.CloseButton.Tooltip" ) );
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
    minMaxButton.setImage( GuiResource.getInstance().getImageMaximizePanel() );
    minMaxButton.setToolTipText( BaseMessages.getString( PKG, "PipelineGraph.ExecutionResultsPanel.MaxButton.Tooltip" ) );
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
    wResultsLabel.setFont( GuiResource.getInstance().getFontLarge() );
    wResultsLabel.setBackground( GuiResource.getInstance().getColorWhite() );
    wResultsLabel.setText( BaseMessages.getString( PKG, "PipelineLog.ResultsPanel.NameLabel" ) );
    FormData fdResultsLabel = new FormData();
    fdResultsLabel.left = new FormAttachment( 0, 0 );
    fdResultsLabel.right = new FormAttachment( minMaxButton, -props.getMargin() );
    fdResultsLabel.top = new FormAttachment( 0, 0 );
    wResultsLabel.setLayoutData( fdResultsLabel );

    // Add a tab folder ...
    //
    extraViewTabFolder = new CTabFolder( extraViewComposite, SWT.MULTI );
    hopGui.getProps().setLook( extraViewTabFolder, Props.WIDGET_STYLE_TAB );

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

  public synchronized void start( PipelineExecutionConfiguration executionConfiguration ) throws HopException {
    // Auto save feature...
    handlePipelineMetaChanges( pipelineMeta );

    // filename set & not changed?
    //
    if ( StringUtils.isNotEmpty( pipelineMeta.getFilename() ) && !pipelineMeta.hasChanged() ) {
      if ( !isRunning() ) {
        try {
          // Set the requested logging level..
          //
          DefaultLogLevel.setLogLevel( executionConfiguration.getLogLevel() );

          pipelineMeta.injectVariables( executionConfiguration.getVariablesMap() );

          // Set the named parameters
          Map<String, String> paramMap = executionConfiguration.getParametersMap();
          Set<String> keys = paramMap.keySet();
          for ( String key : keys ) {
            pipelineMeta.setParameterValue( key, Const.NVL( paramMap.get( key ), "" ) );
          }

          pipelineMeta.activateParameters();

          // Do we need to clear the log before running?
          //
          if ( executionConfiguration.isClearingLog() ) {
            pipelineLogDelegate.clearLog();
          }

          // Also make sure to clear the log entries in the central log store & registry
          //
          if ( pipeline != null ) {
            HopLogStore.discardLines( pipeline.getLogChannelId(), true );
          }

          // Important: even though pipelineMeta is passed to the Pipeline constructor, it is not the same object as is in
          // memory. To be able to completely test this, we need to run it as we would normally do in hop-run
          //
          String pipelineRunConfigurationName = executionConfiguration.getRunConfiguration();
          pipeline = PipelineEngineFactory.createPipelineEngine( pipelineRunConfigurationName, hopGui.getMetadataProvider(), pipelineMeta );
          pipeline.setMetadataProvider( hopGui.getMetadataProvider() );

          String guiLogObjectId = UUID.randomUUID().toString();
          SimpleLoggingObject guiLoggingObject = new SimpleLoggingObject( "HOP GUI", LoggingObjectType.HOP_GUI, null );
          guiLoggingObject.setContainerObjectId( guiLogObjectId );
          guiLoggingObject.setLogLevel( executionConfiguration.getLogLevel() );
          pipeline.setParent( guiLoggingObject );

          pipeline.setLogLevel( executionConfiguration.getLogLevel() );
          log.logBasic( BaseMessages.getString( PKG, "PipelineLog.Log.PipelineOpened" ) );
        } catch ( HopException e ) {
          pipeline = null;
          new ErrorDialog( hopShell(), BaseMessages.getString( PKG, "PipelineLog.Dialog.ErrorOpeningPipeline.Title" ),
            BaseMessages.getString( PKG, "PipelineLog.Dialog.ErrorOpeningPipeline.Message" ), e );
        }
        if ( pipeline != null ) {
          log.logMinimal( BaseMessages.getString( PKG, "PipelineLog.Log.LaunchingPipeline" ) + pipeline.getPipelineMeta().getName() + "]..." );

          // Launch the transform preparation in a different thread.
          // That way HopGui doesn't block anymore and that way we can follow the progress of the initialization
          //
          final Thread parentThread = Thread.currentThread();

          getDisplay().asyncExec( () -> {
            addAllTabs();
            preparePipeline( parentThread );
          } );

          log.logMinimal( BaseMessages.getString( PKG, "PipelineLog.Log.StartedExecutionOfPipeline" ) );

          updateGui();

          // Update the GUI at the end of the pipeline
          //
          pipeline.addExecutionFinishedListener( p -> updateGui() );
        }
      } else {
        modalMessageDialog( BaseMessages.getString( PKG, "PipelineLog.Dialog.DoNoStartPipelineTwice.Title" ),
          BaseMessages.getString( PKG, "PipelineLog.Dialog.DoNoStartPipelineTwice.Message" ), SWT.OK | SWT.ICON_WARNING );
      }
    } else {
      if ( pipelineMeta.hasChanged() ) {
        showSaveFileMessage();
      }
    }
  }

  public void showSaveFileMessage() {
    modalMessageDialog( BaseMessages.getString( PKG, "PipelineLog.Dialog.SavePipelineBeforeRunning.Title" ),
      BaseMessages.getString( PKG, "PipelineLog.Dialog.SavePipelineBeforeRunning.Message" ), SWT.OK | SWT.ICON_WARNING );
  }

  public void addAllTabs() {

    CTabItem tabItemSelection = null;
    if ( extraViewTabFolder != null && !extraViewTabFolder.isDisposed() ) {
      tabItemSelection = extraViewTabFolder.getSelection();
    }

    pipelineLogDelegate.addPipelineLog();
    pipelineGridDelegate.addPipelineGrid();
    pipelineMetricsDelegate.addPipelineMetrics();
    pipelinePreviewDelegate.addPipelinePreview();
    pipelinePerfDelegate.addPipelinePerf();

    /*
    List<HopUiExtenderPluginInterface> relevantExtenders = HopUiExtenderPluginType.getInstance().getRelevantExtenders( HopGuiPipelineGraph.class, LOAD_TAB );
    for ( HopUiExtenderPluginInterface relevantExtender : relevantExtenders ) {
      relevantExtender.uiEvent( this, LOAD_TAB );
    }
     */

    if ( tabItemSelection != null ) {
      extraViewTabFolder.setSelection( tabItemSelection );
    } else {
      extraViewTabFolder.setSelection( pipelineGridDelegate.getPipelineGridTab() );
    }

    ToolItem item = toolBarWidgets.findToolItem( TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS );
    item.setImage( GuiResource.getInstance().getImageHideResults() );
    item.setToolTipText( BaseMessages.getString( PKG, "HopGui.Tooltip.HideExecutionResults" ) );
  }

  public synchronized void debug( PipelineExecutionConfiguration executionConfiguration, PipelineDebugMeta pipelineDebugMeta ) {
    if ( !isRunning() ) {
      try {
        this.lastPipelineDebugMeta = pipelineDebugMeta;

        log.setLogLevel( executionConfiguration.getLogLevel() );
        if ( log.isDetailed() ) {
          log.logDetailed( BaseMessages.getString( PKG, "PipelineLog.Log.DoPreview" ) );
        }
        pipelineMeta.injectVariables( executionConfiguration.getVariablesMap() );

        // Set the named parameters
        Map<String, String> paramMap = executionConfiguration.getParametersMap();
        Set<String> keys = paramMap.keySet();
        for ( String key : keys ) {
          pipelineMeta.setParameterValue( key, Const.NVL( paramMap.get( key ), "" ) );
        }

        pipelineMeta.activateParameters();

        // Do we need to clear the log before running?
        //
        if ( executionConfiguration.isClearingLog() ) {
          pipelineLogDelegate.clearLog();
        }

        // Do we have a previous execution to clean up in the logging registry?
        //
        if ( pipeline != null ) {
          HopLogStore.discardLines( pipeline.getLogChannelId(), false );
          LoggingRegistry.getInstance().removeIncludingChildren( pipeline.getLogChannelId() );
        }

        // Create a new pipeline to execution
        //
        pipeline = new LocalPipelineEngine( pipelineMeta );
        pipeline.setPreview( true );
        pipeline.setMetadataProvider( hopGui.getMetadataProvider() );
        pipeline.prepareExecution();

        // Add the row listeners to the allocated threads
        //
        pipelineDebugMeta.addRowListenersToPipeline( pipeline );

        // What method should we call back when a break-point is hit?

        pipelineDebugMeta.addBreakPointListers( ( pipelineDebugMeta1, transformDebugMeta, rowBufferMeta, rowBuffer )
          -> showPreview( pipelineDebugMeta1, transformDebugMeta, rowBufferMeta, rowBuffer ) );

        // Capture data?
        //
        pipelinePreviewDelegate.capturePreviewData( pipeline, pipelineMeta.getTransforms() );

        // Start the threads for the transforms...
        //
        startThreads();

        debug = true;

        // Show the execution results view...
        //
        hopDisplay().asyncExec( new Runnable() {
          @Override
          public void run() {
            addAllTabs();
          }
        } );
      } catch ( Exception e ) {
        new ErrorDialog( hopShell(), BaseMessages.getString( PKG, "PipelineLog.Dialog.UnexpectedErrorDuringPreview.Title" ),
          BaseMessages.getString( PKG, "PipelineLog.Dialog.UnexpectedErrorDuringPreview.Message" ), e );
      }
    } else {
      modalMessageDialog( BaseMessages.getString( PKG, "PipelineLog.Dialog.DoNoPreviewWhileRunning.Title" ),
        BaseMessages.getString( PKG, "PipelineLog.Dialog.DoNoPreviewWhileRunning.Message" ), SWT.OK | SWT.ICON_WARNING );
    }
    checkErrorVisuals();
  }

  public synchronized void showPreview( final PipelineDebugMeta pipelineDebugMeta, final TransformDebugMeta transformDebugMeta,
                                        final IRowMeta rowBufferMeta, final List<Object[]> rowBuffer ) {
    hopDisplay().asyncExec( new Runnable() {

      @Override
      public void run() {

        if ( isDisposed() ) {
          return;
        }

        updateGui();
        checkErrorVisuals();

        PreviewRowsDialog previewRowsDialog = new PreviewRowsDialog( hopShell(), pipelineMeta,
          SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.APPLICATION_MODAL | SWT.SHEET,
          transformDebugMeta.getTransformMeta().getName(), rowBufferMeta, rowBuffer
        );
        previewRowsDialog.setProposingToGetMoreRows( true );
        previewRowsDialog.setProposingToStop( true );
        previewRowsDialog.open();

        if ( previewRowsDialog.isAskingForMoreRows() ) {
          // clear the row buffer.
          // That way if you click resume, you get the next N rows for the transform :-)
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
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_STOP,
    // label = "Stop",
    toolTip = "Stop the execution of the pipeline",
    image = "ui/images/toolbar/stop.svg"
  )
  @Override
  public void stop() {
    if ( safeStopping ) {
      modalMessageDialog( BaseMessages.getString( PKG, "PipelineLog.Log.SafeStopAlreadyStarted.Title" ),
        BaseMessages.getString( PKG, "PipelineLog.Log.SafeStopAlreadyStarted" ), SWT.ICON_ERROR | SWT.OK );
      return;
    }
    if ( ( isRunning() && !halting ) ) {
      halting = true;
      pipeline.stopAll();
      log.logMinimal( BaseMessages.getString( PKG, "PipelineLog.Log.ProcessingOfPipelineStopped" ) );

      halted = false;
      halting = false;

      pipelineMeta.setInternalHopVariables(); // set the original vars back as they may be changed by a mapping
    }
    updateGui();
  }

  public synchronized void pauseResume() {
    if ( isRunning() ) {
      // Get the pause toolbar item
      //
      if ( !pipeline.isPaused() ) {
        pipeline.pauseExecution();
        updateGui();
      } else {
        pipeline.resumeExecution();
        updateGui();
      }
    }
  }

  private synchronized void preparePipeline( final Thread parentThread ) {
    Runnable runnable = () -> {
      try {
        pipeline.prepareExecution();

        // Refresh tool bar buttons and so on
        //
        updateGui();

        // Capture data?
        //
        pipelinePreviewDelegate.capturePreviewData( pipeline, pipelineMeta.getTransforms() );

        initialized = true;
      } catch ( HopException e ) {
        log.logError( pipeline.getPipelineMeta().getName() + ": preparing pipeline execution failed", e );
        checkErrorVisuals();
      }

      halted = pipeline.hasHaltedComponents();
      if ( pipeline.isReadyToStart() ) {
        checkStartThreads(); // After init, launch the threads.
      } else {
        initialized = false;
        checkErrorVisuals();
      }
    };
    Thread thread = new Thread( runnable );
    thread.start();
  }

  private void checkStartThreads() {
    if ( initialized && !isRunning() && pipeline != null ) {
      startThreads();
    }
  }

  private synchronized void startThreads() {
    try {
      // Add a listener to the pipeline.
      // If the pipeline is done, we want to do the end processing, etc.
      //
      pipeline.addExecutionFinishedListener( pipeline -> {
          checkPipelineEnded();
          checkErrorVisuals();
          stopRedrawTimer();

          pipelineMetricsDelegate.resetLastRefreshTime();
          pipelineMetricsDelegate.updateGraph();
        }
      );

      pipeline.startThreads();

      startRedrawTimer();

      updateGui();
    } catch ( HopException e ) {
      log.logError( "Error starting transform threads", e );
      checkErrorVisuals();
      stopRedrawTimer();
    }

    // See if we have to fire off the performance graph updater etc.
    //
    getDisplay().asyncExec( () -> {
      if ( pipelinePerfDelegate.getPipelinePerfTab() != null ) {
        // If there is a tab open, try to the correct content on there now
        //
        pipelinePerfDelegate.setupContent();
        pipelinePerfDelegate.layoutPerfComposite();
      }
    } );
  }

  private void startRedrawTimer() {

    redrawTimer = new Timer( "HopGuiPipelineGraph: redraw timer" );
    TimerTask timtask = new TimerTask() {
      @Override
      public void run() {
        if ( !hopDisplay().isDisposed() ) {
          hopDisplay().asyncExec( () -> {
            if ( !HopGuiPipelineGraph.this.canvas.isDisposed() ) {
              HopGuiPipelineGraph.this.canvas.redraw();
              HopGuiPipelineGraph.this.updateGui();
            }
          } );
        }
      }
    };

    redrawTimer.schedule( timtask, 0L, ConstUi.INTERVAL_MS_PIPELINE_CANVAS_REFRESH );

  }

  protected void stopRedrawTimer() {
    if ( redrawTimer != null ) {
      redrawTimer.cancel();
      redrawTimer.purge();
      redrawTimer = null;
    }

  }

  private void checkPipelineEnded() {
    if ( pipeline != null ) {
      if ( pipeline.isFinished() && ( isRunning() || halted ) ) {
        log.logMinimal( BaseMessages.getString( PKG, "PipelineLog.Log.PipelineHasFinished" ) );

        initialized = false;
        halted = false;
        halting = false;
        safeStopping = false;

        updateGui();

        // OK, also see if we had a debugging session going on.
        // If so and we didn't hit a breakpoint yet, display the show
        // preview dialog...
        //
        if ( debug && lastPipelineDebugMeta != null && lastPipelineDebugMeta.getTotalNumberOfHits() == 0 ) {
          debug = false;
          showLastPreviewResults();
        }
        debug = false;

        checkErrorVisuals();

        hopDisplay().asyncExec( new Runnable() {
          @Override
          public void run() {
            // hopGui.fireMenuControlers();
            updateGui();
          }
        } );
      }
    }
  }

  private void checkErrorVisuals() {
    if ( pipeline.getErrors() > 0 ) {
      // Get the logging text and filter it out. Store it in the transformLogMap...
      //
      transformLogMap = new HashMap<>();
      hopDisplay().syncExec( new Runnable() {

        @Override
        public void run() {
          for ( IEngineComponent component : pipeline.getComponents() ) {
            if ( component.getErrors() > 0 ) {
              String logText = component.getLogText();
              transformLogMap.put( component.getName(), logText );
            }
          }
        }
      } );

    } else {
      transformLogMap = null;
    }
    // Redraw the canvas to show the error icons etc.
    //
    hopDisplay().asyncExec( new Runnable() {
      @Override
      public void run() {
        redraw();
      }
    } );
  }

  public synchronized void showLastPreviewResults() {
    if ( lastPipelineDebugMeta == null || lastPipelineDebugMeta.getTransformDebugMetaMap().isEmpty() ) {
      return;
    }

    final List<String> transformnames = new ArrayList<>();
    final List<IRowMeta> rowMetas = new ArrayList<>();
    final List<List<Object[]>> rowBuffers = new ArrayList<>();

    // Assemble the buffers etc in the old style...
    //
    for ( TransformMeta transformMeta : lastPipelineDebugMeta.getTransformDebugMetaMap().keySet() ) {
      TransformDebugMeta transformDebugMeta = lastPipelineDebugMeta.getTransformDebugMetaMap().get( transformMeta );

      transformnames.add( transformMeta.getName() );
      rowMetas.add( transformDebugMeta.getRowBufferMeta() );
      rowBuffers.add( transformDebugMeta.getRowBuffer() );
    }

    hopDisplay().asyncExec( () -> {
      EnterPreviewRowsDialog dialog = new EnterPreviewRowsDialog( hopShell(), SWT.NONE, transformnames, rowMetas, rowBuffers );
      dialog.open();
    } );
  }

  public boolean isRunning() {
    if ( pipeline == null ) {
      return false;
    }
    if ( pipeline.isStopped() ) {
      return false;
    }
    if ( pipeline.isPreparing() ) {
      return true;
    }
    if ( pipeline.isRunning() ) {
      return true;
    }
    return false;
  }

  /**
   * @return the lastPipelineDebugMeta
   */
  public PipelineDebugMeta getLastPipelineDebugMeta() {
    return lastPipelineDebugMeta;
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
   * @return the transformLogMap
   */
  public Map<String, String> getTransformLogMap() {
    return transformLogMap;
  }

  /**
   * @param transformLogMap the transformLogMap to set
   */
  public void setTransformLogMap( Map<String, String> transformLogMap ) {
    this.transformLogMap = transformLogMap;
  }

  @Override
  public IHasLogChannel getLogChannelProvider() {
    return new IHasLogChannel() {
      @Override
      public ILogChannel getLogChannel() {
        return getPipeline() != null ? getPipeline().getLogChannel() : getPipelineMeta().getLogChannel();
      }
    };
  }

  @GuiContextAction(
    id = "pipeline-graph-transform-12000-sniff-output",
    parentId = HopGuiPipelineTransformContext.CONTEXT_ID,
    type = GuiActionType.Info,
    name = "Sniff output",
    tooltip = "Take a look at 50 rows coming out of the selected transform",
    image = "ui/images/preview.svg"
  )
  public void sniff( HopGuiPipelineTransformContext context ) {
    TransformMeta transformMeta = context.getTransformMeta();

    if ( pipeline == null || pipeline.isFinished() ) {
      MessageBox messageBox = new MessageBox( hopShell(), SWT.ICON_INFORMATION | SWT.OK );
      messageBox.setText( BaseMessages.getString( PKG, "PipelineGraph.SniffTestingAvailableWhenRunning.Title" ) );
      messageBox.setMessage( BaseMessages.getString( PKG, "PipelineGraph.SniffTestingAvailableWhenRunning.Message" ) );
      messageBox.open();
      return;
    }

    try {
      pipeline.retrieveComponentOutput( transformMeta.getName(), 0, 50, ( ( pipelineEngine, rowBuffer ) -> {
        hopDisplay().asyncExec( () -> {
          PreviewRowsDialog dialog = new PreviewRowsDialog( hopShell(), hopGui.getVariables(), SWT.NONE, transformMeta.getName(), rowBuffer.getRowMeta(), rowBuffer.getBuffer() );
          dialog.open();
        } );
      } ) );
    } catch ( HopException e ) {
      new ErrorDialog( hopShell(), "Error", "Error sniffing rows", e );
    }
  }

  @Override public ILogChannel getLogChannel() {
    return log;
  }

  /**
   * Edit the transform of the given pipeline
   *
   * @param pipelineMeta
   * @param transformMeta
   */
  public void editTransform( PipelineMeta pipelineMeta, TransformMeta transformMeta ) {
    pipelineTransformDelegate.editTransform( pipelineMeta, transformMeta );
  }

  public String buildTabName() throws HopException {
    String tabName = null;
    String realFilename = pipelineMeta.environmentSubstitute( pipelineMeta.getFilename() );
    if ( StringUtils.isEmpty( realFilename ) ) {
      tabName = pipelineMeta.getName();
    } else {
      try {
        FileObject fileObject = HopVfs.getFileObject( pipelineMeta.getFilename() );
        FileName fileName = fileObject.getName();
        tabName = fileName.getBaseName();
      } catch ( Exception e ) {
        throw new HopException( "Unable to get information from file name '" + pipelineMeta.getFilename() + "'", e );
      }
    }
    return tabName;
  }

  public void handlePipelineMetaChanges( PipelineMeta pipelineMeta ) throws HopException {
    if ( pipelineMeta.hasChanged() ) {
      if ( hopGui.getProps().getAutoSave() ) {
        save();
      } else {
        MessageDialogWithToggle md =
          new MessageDialogWithToggle( hopShell(), BaseMessages.getString( PKG, "PipelineLog.Dialog.FileHasChanged.Title" ),
            null, BaseMessages.getString( PKG, "PipelineLog.Dialog.FileHasChanged1.Message" ) + Const.CR
            + BaseMessages.getString( PKG, "PipelineLog.Dialog.FileHasChanged2.Message" ) + Const.CR,
            MessageDialog.QUESTION, new String[] { BaseMessages.getString( PKG, "System.Button.Yes" ),
            BaseMessages.getString( PKG, "System.Button.No" ) }, 0, BaseMessages.getString( PKG,
            "PipelineLog.Dialog.Option.AutoSavePipeline" ), hopGui.getProps().getAutoSave() );
        MessageDialogWithToggle.setDefaultImage( GuiResource.getInstance().getImageHopUi() );
        int answer = md.open();
        if ( ( answer & 0xFF ) == 0 ) {
          save();
        }
        hopGui.getProps().setAutoSave( md.getToggleState() );
      }
    }
  }

  private TransformMeta lastChained = null;

  public void addTransformToChain( IPlugin transformPlugin, boolean shift ) {
    // Is the lastChained entry still valid?
    //
    if ( lastChained != null && pipelineMeta.findTransform( lastChained.getName() ) == null ) {
      lastChained = null;
    }

    // If there is exactly one selected transform, pick that one as last chained.
    //
    List<TransformMeta> sel = pipelineMeta.getSelectedTransforms();
    if ( sel.size() == 1 ) {
      lastChained = sel.get( 0 );
    }

    // Where do we add this?

    Point p = null;
    if ( lastChained == null ) {
      p = pipelineMeta.getMaximum();
      p.x -= 100;
    } else {
      p = new Point( lastChained.getLocation().x, lastChained.getLocation().y );
    }

    p.x += 200;

    // Which is the new transform?

    TransformMeta newTransform = pipelineTransformDelegate.newTransform( pipelineMeta, transformPlugin.getIds()[ 0 ], transformPlugin.getName(), transformPlugin.getName(), false, true, p );
    if ( newTransform == null ) {
      return;
    }
    newTransform.setLocation( p.x, p.y );

    if ( lastChained != null ) {
      PipelineHopMeta hop = new PipelineHopMeta( lastChained, newTransform );
      pipelineHopDelegate.newHop( pipelineMeta, hop );
    }

    lastChained = newTransform;

    if ( shift ) {
      editTransform( newTransform );
    }

    pipelineMeta.unselectAll();
    newTransform.setSelected( true );

    updateGui();
  }

  public HopGui getHopGui() {
    return hopGui;
  }

  public void setHopGui( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  @Override public Object getSubject() {
    return pipelineMeta;
  }

  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  public IPipelineEngine<PipelineMeta> getPipeline() {
    return pipeline;
  }

  private void setHopEnabled( PipelineHopMeta hop, boolean enabled ) {
    hop.setEnabled( enabled );
    pipelineMeta.clearCaches();
  }

  private void modalMessageDialog( String title, String message, int swtFlags ) {
    MessageBox messageBox = new MessageBox( hopShell(), swtFlags );
    messageBox.setMessage( message );
    messageBox.setText( title );
    messageBox.open();
  }

  /**
   * Gets fileType
   *
   * @return value of fileType
   */
  public HopPipelineFileType getFileType() {
    return fileType;
  }

  /**
   * @param fileType The fileType to set
   */
  public void setFileType( HopPipelineFileType fileType ) {
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
    HopGuiPipelineGraph that = (HopGuiPipelineGraph) o;
    return Objects.equals( pipelineMeta, that.pipelineMeta ) &&
      Objects.equals( id, that.id );
  }

  @Override public int hashCode() {
    return Objects.hash( pipelineMeta, id );
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_UNDO_ID,
    // label = "Undo",
    toolTip = "Undo an operation",
    image = "ui/images/toolbar/Antu_edit-undo.svg",
    disabledImage = "ui/images/toolbar/Antu_edit-undo-disabled.svg",
    separator = true
  )
  @GuiKeyboardShortcut( control = true, key = 'z' )
  @Override public void undo() {
    pipelineUndoDelegate.undoPipelineAction( this, pipelineMeta );
    forceFocus();
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_REDO_ID,
    // label = "Redo",
    toolTip = "Redo an operation",
    image = "ui/images/toolbar/Antu_edit-redo.svg",
    disabledImage = "ui/images/toolbar/Antu_edit-redo-disabled.svg"
  )
  @GuiKeyboardShortcut( control = true, shift = true, key = 'z' )
  @Override public void redo() {
    pipelineUndoDelegate.redoPipelineAction( this, pipelineMeta );
    forceFocus();
  }

  /**
   * Update the representation, toolbar, menus and so on. This is needed after a file, context or capabilities changes
   */
  @Override public void updateGui() {

    if ( hopGui == null || toolBarWidgets == null || toolBar == null || toolBar.isDisposed() ) {
      return;
    }

    hopDisplay().asyncExec( () -> {
      setZoomLabel();

      // Enable/disable the undo/redo toolbar buttons...
      //
      toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_UNDO_ID, pipelineMeta.viewThisUndo() != null );
      toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_REDO_ID, pipelineMeta.viewNextUndo() != null );

      // Enable/disable the align/distribute toolbar buttons
      //
      boolean selectedTransform = !pipelineMeta.getSelectedTransforms().isEmpty();
      toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_SNAP_TO_GRID, selectedTransform );

      boolean selectedTransforms = pipelineMeta.getSelectedTransforms().size() > 1;
      toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_ALIGN_LEFT, selectedTransforms );
      toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_ALIGN_RIGHT, selectedTransforms );
      toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_ALIGN_TOP, selectedTransforms );
      toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_ALIGN_BOTTOM, selectedTransforms );
      toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_DISTRIBUTE_HORIZONTALLY, selectedTransforms );
      toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_DISTRIBUTE_VERTICALLY, selectedTransforms );

      boolean running = isRunning();
      boolean paused = running && pipeline.isPaused();
      toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_START, !running || paused );
      toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_STOP, running );
      toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_PAUSE, running && !paused );

      hopGui.setUndoMenu( pipelineMeta );
      hopGui.handleFileCapabilities( fileType, running, paused );

      HopGuiPipelineGraph.super.redraw();
    } );

  }

  public boolean forceFocus() {
    return canvas.forceFocus();
  }

  @GuiKeyboardShortcut( control = true, key = 'a' )
  @GuiOsxKeyboardShortcut( command = true, key = 'a' )
  @Override public void selectAll() {
    pipelineMeta.selectAll();
    updateGui();
  }

  @GuiKeyboardShortcut( key = SWT.ESC )
  @Override public void unselectAll() {
    clearSettings();
    updateGui();
  }

  @GuiKeyboardShortcut( control = true, key = 'c' )
  @GuiOsxKeyboardShortcut( command = true, key = 'c' )
  @Override public void copySelectedToClipboard() {
    if ( pipelineLogDelegate.hasSelectedText() ) {
      pipelineLogDelegate.copySelected();
    } else {
      pipelineClipboardDelegate.copySelected( pipelineMeta, pipelineMeta.getSelectedTransforms(), pipelineMeta.getSelectedNotes() );
    }
  }

  @GuiKeyboardShortcut( control = true, key = 'x' )
  @GuiOsxKeyboardShortcut( command = true, key = 'x' )
  @Override public void cutSelectedToClipboard() {
    pipelineClipboardDelegate.copySelected( pipelineMeta, pipelineMeta.getSelectedTransforms(), pipelineMeta.getSelectedNotes() );
    pipelineTransformDelegate.delTransforms( pipelineMeta, pipelineMeta.getSelectedTransforms() );
    notePadDelegate.deleteNotes( pipelineMeta, pipelineMeta.getSelectedNotes() );
  }

  @GuiKeyboardShortcut( key = SWT.DEL )
  @Override public void deleteSelected() {
    delSelected( null );
    updateGui();
  }

  @GuiKeyboardShortcut( control = true, key = 'v' )
  @GuiOsxKeyboardShortcut( command = true, key = 'v' )
  @Override public void pasteFromClipboard() {
    pasteFromClipboard( new Point( currentMouseX, currentMouseY ) );
  }

  public void pasteFromClipboard( Point location ) {
    final String clipboard = pipelineClipboardDelegate.fromClipboard();
    pipelineClipboardDelegate.pasteXml( pipelineMeta, clipboard, location );
  }

  @GuiContextAction(
    id = "pipeline-graph-pipeline-paste",
    parentId = HopGuiPipelineContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Paste from the clipboard",
    tooltip = "Paste transforms, notes or a whole pipeline from the clipboard",
    image = "ui/images/CPY.svg"
  )
  public void pasteFromClipboard( HopGuiPipelineContext context ) {
    pasteFromClipboard( context.getClick() );
  }

  @Override public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> handlers = new ArrayList<>();
    return handlers;
  }

  /**
   * Gets toolBarWidgets
   *
   * @return value of toolBarWidgets
   */
  public GuiToolbarWidgets getToolBarWidgets() {
    return toolBarWidgets;
  }
}
