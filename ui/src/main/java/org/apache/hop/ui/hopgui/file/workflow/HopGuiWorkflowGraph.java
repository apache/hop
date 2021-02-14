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

package org.apache.hop.ui.hopgui.file.workflow;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.IEngineMeta;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.Props;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.gui.AreaOwner;
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
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.IHasLogChannel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILogParentProvided;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.history.AuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.laf.BasePropertyHandler;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePainter;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.widget.CheckBoxToolTip;
import org.apache.hop.ui.core.widget.OsHelper;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.GuiContextUtil;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.dialog.NotePadDialog;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.delegates.HopGuiNotePadDelegate;
import org.apache.hop.ui.hopgui.file.shared.HopGuiTooltipExtension;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowActionContext;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowContext;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowHopContext;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowNoteContext;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowActionDelegate;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowClipboardDelegate;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowGridDelegate;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowHopDelegate;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowLogDelegate;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowRunDelegate;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowUndoDelegate;
import org.apache.hop.ui.hopgui.file.workflow.extension.HopGuiWorkflowGraphExtension;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopGuiAbstractGraph;
import org.apache.hop.ui.hopgui.shared.SwtGc;
import org.apache.hop.ui.hopgui.shared.SwtScrollBar;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.IActionListener;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.WorkflowPainter;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;
import org.eclipse.jface.window.DefaultToolTip;
import org.eclipse.jface.window.ToolTip;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ControlAdapter;
import org.eclipse.swt.events.ControlEvent;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseListener;
import org.eclipse.swt.events.MouseMoveListener;
import org.eclipse.swt.events.MouseTrackListener;
import org.eclipse.swt.events.MouseWheelListener;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.ScrollBar;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Handles the display of Workflows in HopGui, in a graphical form.
 *
 * @author Matt Created on 17-may-2003
 */
@GuiPlugin(description = "Workflow Graph tab")
public class HopGuiWorkflowGraph extends HopGuiAbstractGraph
    implements IRedrawable,
        MouseListener,
        MouseMoveListener,
        MouseTrackListener,
        MouseWheelListener,
        IHasLogChannel,
        ILogParentProvided,
        IHopFileTypeHandler,
        IGuiRefresher {

  private static final Class<?> PKG = HopGuiWorkflowGraph.class; // For Translator

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiWorkflowGraph-Toolbar";
  public static final String TOOLBAR_ITEM_START = "HopGuiWorkflowGraph-ToolBar-10010-Run";
  public static final String TOOLBAR_ITEM_STOP = "HopGuiWorkflowGraph-ToolBar-10030-Stop";

  public static final String TOOLBAR_ITEM_UNDO_ID = "HopGuiWorkflowGraph-ToolBar-10100-Undo";
  public static final String TOOLBAR_ITEM_REDO_ID = "HopGuiWorkflowGraph-ToolBar-10110-Redo";

  public static final String TOOLBAR_ITEM_SNAP_TO_GRID =
      "HopGuiWorkflowGraph-ToolBar-10190-Snap-To-Grid";
  public static final String TOOLBAR_ITEM_ALIGN_LEFT =
      "HopGuiWorkflowGraph-ToolBar-10200-Align-Left";
  public static final String TOOLBAR_ITEM_ALIGN_RIGHT =
      "HopGuiWorkflowGraph-ToolBar-10210-Align-Right";
  public static final String TOOLBAR_ITEM_ALIGN_TOP =
      "HopGuiWorkflowGraph-ToolBar-10250-Align-Ttop";
  public static final String TOOLBAR_ITEM_ALIGN_BOTTOM =
      "HopGuiWorkflowGraph-ToolBar-10260-Align-Bottom";
  public static final String TOOLBAR_ITEM_DISTRIBUTE_HORIZONTALLY =
      "HopGuiWorkflowGraph-ToolBar-10300-Distribute-Horizontally";
  public static final String TOOLBAR_ITEM_DISTRIBUTE_VERTICALLY =
      "HopGuiWorkflowGraph-ToolBar-10310-Distribute-Vertically";

  public static final String TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS =
      "HopGuiWorkflowGraph-ToolBar-10400-Execution-Results";

  public static final String TOOLBAR_ITEM_ZOOM_LEVEL =
      "HopGuiWorkflowGraph-ToolBar-10500-Zoom-Level";

  public static final String TOOLBAR_ITEM_EDIT_WORKFLOW = "HopGuiWorkflowGrpah-ToolBar-10450-EditWorkflow";

  private static final String STRING_PARALLEL_WARNING_PARAMETER = "ParallelActionsWarning";

  private static final int HOP_SEL_MARGIN = 9;

  private static final int TOOLTIP_HIDE_DELAY_FLASH = 2000;

  private final HopDataOrchestrationPerspective perspective;

  protected ILogChannel log;

  protected WorkflowMeta workflowMeta;

  protected IWorkflowEngine<WorkflowMeta> workflow;

  protected Thread workflowThread;

  protected PropsUi props;

  protected int iconSize;

  protected int lineWidth;

  protected Point lastClick;

  protected List<ActionMeta> selectedActions;

  protected ActionMeta selectedAction;

  private List<NotePadMeta> selectedNotes;
  protected NotePadMeta selectedNote;

  protected Point lastMove;

  protected WorkflowHopMeta hopCandidate;

  protected HopGui hopGui;

  // public boolean shift, control;
  protected boolean splitHop;

  protected int lastButton;

  protected WorkflowHopMeta lastHopSplit;

  protected org.apache.hop.core.gui.Rectangle selectionRegion;

  protected static final double theta = Math.toRadians(10); // arrowhead sharpness

  protected static final int size = 30; // arrowhead length

  protected int currentMouseX = 0;

  protected int currentMouseY = 0;

  protected NotePadMeta ni = null;

  // private Text filenameLabel;
  private SashForm sashForm;

  public Composite extraViewComposite;

  public CTabFolder extraViewTabFolder;

  private ToolBar toolBar;
  private GuiToolbarWidgets toolBarWidgets;

  private boolean halting;

  public HopGuiWorkflowLogDelegate workflowLogDelegate;
  public HopGuiWorkflowGridDelegate workflowGridDelegate;
  public HopGuiWorkflowClipboardDelegate workflowClipboardDelegate;
  public HopGuiWorkflowRunDelegate workflowRunDelegate;
  public HopGuiWorkflowUndoDelegate workflowUndoDelegate;
  public HopGuiWorkflowActionDelegate workflowActionDelegate;
  public HopGuiWorkflowHopDelegate workflowHopDelegate;
  public HopGuiNotePadDelegate notePadDelegate;

  private Composite mainComposite;

  private Label closeButton;

  private Label minMaxButton;

  private CheckBoxToolTip helpTip;

  private List<AreaOwner> areaOwners;

  private HopWorkflowFileType<WorkflowMeta> fileType;

  private ActionMeta startHopAction;
  private Point endHopLocation;

  private ActionMeta endHopAction;
  private ActionMeta noInputAction;
  private DefaultToolTip toolTip;
  private Point[] previous_transform_locations;
  private Point[] previous_note_locations;
  private ActionMeta currentAction;
  private boolean ignoreNextClick;
  private boolean doubleClick;
  private WorkflowHopMeta clickedWorkflowHop;

  public HopGuiWorkflowGraph(
      Composite parent,
      final HopGui hopGui,
      final CTabItem parentTabItem,
      final HopDataOrchestrationPerspective perspective,
      final WorkflowMeta workflowMeta,
      final HopWorkflowFileType<WorkflowMeta> fileType) {
    super(hopGui, parent, SWT.NONE, parentTabItem);
    this.perspective = perspective;
    this.workflowMeta = workflowMeta;
    this.fileType = fileType;

    this.log = hopGui.getLog();
    this.hopGui = hopGui;
    this.workflowMeta = workflowMeta;

    this.props = PropsUi.getInstance();
    this.areaOwners = new ArrayList<>();

    workflowLogDelegate = new HopGuiWorkflowLogDelegate(hopGui, this);
    workflowGridDelegate = new HopGuiWorkflowGridDelegate(hopGui, this);
    workflowClipboardDelegate = new HopGuiWorkflowClipboardDelegate(hopGui, this);
    workflowRunDelegate = new HopGuiWorkflowRunDelegate(hopGui, this);
    workflowUndoDelegate = new HopGuiWorkflowUndoDelegate(hopGui, this);
    workflowActionDelegate = new HopGuiWorkflowActionDelegate(hopGui, this);
    workflowHopDelegate = new HopGuiWorkflowHopDelegate(hopGui, this);
    notePadDelegate = new HopGuiNotePadDelegate(hopGui, this);

    // TODO: ADD TOOLBAR

    setLayout(new FormLayout());
    setLayoutData(new GridData(GridData.FILL_BOTH));

    // Add a tool-bar at the top of the tab
    // The form-data is set on the native widget automatically
    //
    addToolBar();

    // The main composite contains the graph view, but if needed also
    // a view with an extra tab containing log, etc.
    //
    mainComposite = new Composite(this, SWT.NONE);
    mainComposite.setLayout(new FillLayout());

    FormData toolbarFd = new FormData();
    toolbarFd.left = new FormAttachment(0, 0);
    toolbarFd.right = new FormAttachment(100, 0);
    toolBar.setLayoutData(toolbarFd);

    // ------------------------

    FormData fdMainComposite = new FormData();
    fdMainComposite.left = new FormAttachment(0, 0);
    fdMainComposite.top = new FormAttachment(toolBar, 0);
    fdMainComposite.right = new FormAttachment(100, 0);
    fdMainComposite.bottom = new FormAttachment(100, 0);
    mainComposite.setLayoutData(fdMainComposite);

    // To allow for a splitter later on, we will add the splitter here...
    //
    sashForm = new SashForm(mainComposite, SWT.VERTICAL);

    // Add a canvas below it, use up all space
    //
    wsCanvas = new ScrolledComposite( sashForm, SWT.V_SCROLL | SWT.H_SCROLL | SWT.NO_BACKGROUND | SWT.BORDER );
    wsCanvas.setAlwaysShowScrollBars( true );
    wsCanvas.setLayout( new FormLayout() );
    FormData fdsCanvas = new FormData();
    fdsCanvas.left = new FormAttachment(0, 0);
    fdsCanvas.top = new FormAttachment(0, 0);
    fdsCanvas.right = new FormAttachment(100, 0);
    fdsCanvas.bottom = new FormAttachment(100, 0);
    wsCanvas.setLayoutData( fdsCanvas );

    canvas = new Canvas(wsCanvas, SWT.NO_BACKGROUND );
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment(0, 0);
    fdCanvas.top = new FormAttachment(0, 0);
    fdCanvas.right = new FormAttachment(100, 0);
    fdCanvas.bottom = new FormAttachment(100, 0);
    canvas.setLayoutData(fdCanvas);

    sashForm.setWeights(
        new int[] {
          100,
        });

    toolTip = new DefaultToolTip(canvas, ToolTip.NO_RECREATE, true);
    toolTip.setRespectMonitorBounds(true);
    toolTip.setRespectDisplayBounds(true);
    toolTip.setPopupDelay(350);
    toolTip.setShift(
        new org.eclipse.swt.graphics.Point(ConstUi.TOOLTIP_OFFSET, ConstUi.TOOLTIP_OFFSET));

    helpTip = new CheckBoxToolTip(canvas);
    helpTip.addCheckBoxToolTipListener(
        enabled -> hopGui.getProps().setShowingHelpToolTips(enabled));

    newProps();

    selectionRegion = null;
    hopCandidate = null;
    lastHopSplit = null;

    selectedActions = null;
    selectedNote = null;

    ScrollBar horizontalBar = wsCanvas.getHorizontalBar();
    ScrollBar verticalBar = wsCanvas.getVerticalBar();

    horizontalBar.setMinimum(1);
    horizontalBar.setMaximum(100);
    horizontalBar.setVisible(true);
    verticalBar.setMinimum(1);
    verticalBar.setMaximum(100);
    verticalBar.setVisible(true);
    if (!EnvironmentUtils.getInstance().isWeb()) {
      horizontalBar.setIncrement(5);
      verticalBar.setIncrement(5);
    }

    if (OsHelper.isWindows()) {
      horizontalBar.addListener(
        SWT.Selection,
        e -> canvas.redraw() );
      verticalBar.addListener(
        SWT.Selection,
        e -> canvas.redraw() );
    }

    setVisible(true);

    canvas.addPaintListener(this::paintControl);

    selectedActions = null;
    lastClick = null;

    canvas.addMouseListener(this);
    canvas.addMouseMoveListener(this);
    canvas.addMouseTrackListener(this);
    canvas.addMouseWheelListener(this);

    hopGui.replaceKeyboardShortcutListeners(this);

    // Scrolled composite ...
    //
    canvas.pack();
    Rectangle bounds = canvas.getBounds();

    wsCanvas.setContent(canvas);
    wsCanvas.setExpandHorizontal( true );
    wsCanvas.setExpandVertical( true );
    wsCanvas.setMinWidth( bounds.width );
    wsCanvas.setMinHeight( bounds.height );

    setBackground(GuiResource.getInstance().getColorBackground());

    wsCanvas.addControlListener( new ControlAdapter() {
      @Override public void controlResized( ControlEvent e ) {
        new Thread( () -> {
          try {
            Thread.sleep(250);
          } catch ( Exception e1 ) {
            // ignore
          }
          getDisplay().asyncExec( () ->
            adjustScrolling()
          );
        } ).start();
      }
    } );

    updateGui();
  }

  public static HopGuiWorkflowGraph getInstance() {
    return HopGui.getActiveWorkflowGraph();
  }

  protected void hideToolTips() {
    toolTip.hide();
    helpTip.hide();
  }

  public void mouseDoubleClick(MouseEvent e) {

    if (!PropsUi.getInstance().useDoubleClick()) {
      return;
    }

    doubleClick = true;
    clearSettings();

    Point real = screen2real(e.x, e.y);

    // Hide the tooltip!
    hideToolTips();

    AreaOwner areaOwner = getVisibleAreaOwner(real.x, real.y);

    try {
      HopGuiWorkflowGraphExtension ext = new HopGuiWorkflowGraphExtension(this, e, real, areaOwner);
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.WorkflowGraphMouseDoubleClick.id, ext);
      if (ext.isPreventingDefault()) {
        return;
      }
    } catch (Exception ex) {
      LogChannel.GENERAL.logError("Error calling JobGraphMouseDoubleClick extension point", ex);
    }

    ActionMeta action = workflowMeta.getAction(real.x, real.y, iconSize);
    if (action != null) {
      if (e.button == 1) {
        editAction(action);
      } else {
        // open tab in HopGui
        launchStuff(action);
      }
    } else {
      // Check if point lies on one of the many hop-lines...
      WorkflowHopMeta online = findWorkflowHop(real.x, real.y);
      if (online == null) {
        NotePadMeta ni = workflowMeta.getNote(real.x, real.y);
        if (ni != null) {
          editNote(ni);
        } else {
          // Clicked on the background...
          //
          editWorkflowProperties();
        }
      }
    }
  }

  public void mouseDown(MouseEvent e) {
    doubleClick = false;

    if (ignoreNextClick) {
      ignoreNextClick = false;
      return;
    }

    boolean control = (e.stateMask & SWT.MOD1) != 0;
    boolean shift = (e.stateMask & SWT.SHIFT) != 0;

    lastButton = e.button;
    Point real = screen2real(e.x, e.y);
    lastClick = new Point(real.x, real.y);

    // Hide the tooltip!
    hideToolTips();

    // Set the pop-up menu
    if (e.button == 3) {
      setMenu(real.x, real.y);
      return;
    }

    AreaOwner areaOwner = getVisibleAreaOwner(real.x, real.y);

    try {
      HopGuiWorkflowGraphExtension ext = new HopGuiWorkflowGraphExtension(this, e, real, areaOwner);
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.WorkflowGraphMouseDown.id, ext);
      if (ext.isPreventingDefault()) {
        return;
      }
    } catch (Exception ex) {
      LogChannel.GENERAL.logError("Error calling JobGraphMouseDown extension point", ex);
    }

    // A single left or middle click on one of the area owners...
    //
    if (e.button == 1 || e.button == 2) {
      if (areaOwner != null && areaOwner.getAreaType() != null) {
        switch (areaOwner.getAreaType()) {

          case ACTION_ICON:
            ActionMeta actionCopy = (ActionMeta) areaOwner.getOwner();
            currentAction = actionCopy;

            if (hopCandidate != null) {
              addCandidateAsHop();

            } else if (e.button == 2 || (e.button == 1 && shift)) {
              // SHIFT CLICK is start of drag to create a new hop
              //
              startHopAction = actionCopy;

            } else {
              selectedActions = workflowMeta.getSelectedActions();
              selectedAction = actionCopy;
              //
              // When an icon is moved that is not selected, it gets
              // selected too late.
              // It is not captured here, but in the mouseMoveListener...
              //
              previous_transform_locations = workflowMeta.getSelectedLocations();

              Point p = actionCopy.getLocation();
              iconOffset = new Point(real.x - p.x, real.y - p.y);
            }
            updateGui();
            break;

          case NOTE:
            ni = (NotePadMeta) areaOwner.getOwner();
            selectedNotes = workflowMeta.getSelectedNotes();
            selectedNote = ni;
            Point loc = ni.getLocation();

            previous_note_locations = workflowMeta.getSelectedNoteLocations();

            noteOffset = new Point(real.x - loc.x, real.y - loc.y);

            updateGui();
            break;

            // If you click on an evaluating icon, change the evaluation...
            //
          case WORKFLOW_HOP_ICON:
            WorkflowHopMeta hop = (WorkflowHopMeta) areaOwner.getOwner();
            if (hop.getFromAction().isEvaluation()) {
              if (hop.isUnconditional()) {
                hop.setUnconditional(false);
                hop.setEvaluation(true);
              } else {
                if (hop.getEvaluation()) {
                  hop.setEvaluation(false);
                } else {
                  hop.setUnconditional(true);
                }
              }
              updateGui();
            }
            break;
          default:
            break;
        }
      } else {
        WorkflowHopMeta hop = findWorkflowHop(real.x, real.y);
        if (hop != null) {
          // User held control and clicked a hop between steps - We want to flip the active state of
          // the hop.
          //
          if (e.button == 2 || (e.button == 1 && control)) {
            hop.setEnabled(!hop.isEnabled());
            updateGui();
          } else {
            // A hop: show the hop context menu in the mouseUp() listener
            //
            clickedWorkflowHop = hop;
          }
        } else {
          // No area-owner means: background:
          //
          startHopAction = null;
          if (!control) {
            selectionRegion = new org.apache.hop.core.gui.Rectangle(real.x, real.y, 0, 0);
          }
          updateGui();
        }
      }
    }
  }

  private enum SingleClickType {
    Workflow,
    Action,
    Note,
    Hop,
  }

  public void mouseUp(MouseEvent e) {
    boolean control = (e.stateMask & SWT.MOD1) != 0;

    boolean singleClick = false;
    SingleClickType singleClickType = null;
    ActionMeta singleClickAction = null;
    NotePadMeta singleClickNote = null;
    WorkflowHopMeta singleClickHop = null;

    if (iconOffset == null) {
      iconOffset = new Point(0, 0);
    }
    Point real = screen2real(e.x, e.y);
    Point icon = new Point(real.x - iconOffset.x, real.y - iconOffset.y);
    AreaOwner areaOwner = getVisibleAreaOwner(real.x, real.y);

    // Quick new hop option? (drag from one action to another)
    //
    if (areaOwner != null && areaOwner.getAreaType() != null) {
      switch (areaOwner.getAreaType()) {
        case ACTION_ICON:
          if (startHopAction != null) {
            currentAction = (ActionMeta) areaOwner.getOwner();
            hopCandidate = new WorkflowHopMeta(startHopAction, currentAction);
            addCandidateAsHop();
            // startHopAction=null;
            redraw();
          }
          break;
        case ACTION_NAME:
          if (startHopAction == null
              && selectionRegion == null
              && selectedActions == null
              && selectedNotes == null) {
            // This is available only in single click mode...
            //
            startHopAction = null;
            selectionRegion = null;
            ActionMeta actionMeta = (ActionMeta) areaOwner.getParent();
            editAction(actionMeta);
            return;
          }
        default:
          break;
      }
    }

    // Did we select a region on the screen? Mark actions in region as selected
    //
    if (selectionRegion != null) {
      selectionRegion.width = real.x - selectionRegion.x;
      selectionRegion.height = real.y - selectionRegion.y;

      if (selectionRegion.isEmpty()) {
        singleClick = true;
        singleClickType = SingleClickType.Workflow;
      } else {
        workflowMeta.unselectAll();
        selectInRect(workflowMeta, selectionRegion);
      }
      selectionRegion = null;
      updateGui();
    } else {
      // Clicked on an icon?
      //
      if (selectedAction != null && startHopAction == null) {
        if (e.button == 1) {
          Point realclick = screen2real(e.x, e.y);
          if (lastClick.x == realclick.x && lastClick.y == realclick.y) {
            // Flip selection when control is pressed!
            if (control) {
              selectedAction.flipSelected();
            } else {
              singleClick = true;
              singleClickType = SingleClickType.Action;
              singleClickAction = selectedAction;
            }
          } else {
            // Find out which Transforms & Notes are selected
            selectedActions = workflowMeta.getSelectedActions();
            selectedNotes = workflowMeta.getSelectedNotes();

            // We moved around some items: store undo info...
            //
            boolean also = false;
            if (selectedNotes != null
                && selectedNotes.size() > 0
                && previous_note_locations != null) {
              int[] indexes = workflowMeta.getNoteIndexes(selectedNotes);

              addUndoPosition(
                  selectedNotes.toArray(new NotePadMeta[selectedNotes.size()]),
                  indexes,
                  previous_note_locations,
                  workflowMeta.getSelectedNoteLocations(),
                  also);
              also = selectedActions != null && selectedActions.size() > 0;
            }
            if (selectedActions != null
                && selectedActions.size() > 0
                && previous_transform_locations != null) {
              int[] indexes = workflowMeta.getActionIndexes(selectedActions);
              addUndoPosition(
                  selectedActions.toArray(new ActionMeta[selectedActions.size()]),
                  indexes,
                  previous_transform_locations,
                  workflowMeta.getSelectedLocations(),
                  also);
            }
          }
        }

        // OK, we moved the transform, did we move it across a hop?
        // If so, ask to split the hop!
        if (splitHop) {
          WorkflowHopMeta hi =
              findHop(icon.x + iconSize / 2, icon.y + iconSize / 2, selectedAction);
          if (hi != null) {
            int id = 0;
            if (!hopGui.getProps().getAutoSplit()) {
              MessageDialogWithToggle md =
                  new MessageDialogWithToggle(
                      hopShell(),
                      BaseMessages.getString(PKG, "PipelineGraph.Dialog.SplitHop.Title"),
                      BaseMessages.getString(PKG, "PipelineGraph.Dialog.SplitHop.Message")
                          + Const.CR
                          + hi.toString(),
                      SWT.ICON_QUESTION,
                      new String[] {
                        BaseMessages.getString(PKG, "System.Button.Yes"),
                        BaseMessages.getString(PKG, "System.Button.No")
                      },
                      BaseMessages.getString(
                          PKG, "PipelineGraph.Dialog.Option.SplitHop.DoNotAskAgain"),
                      hopGui.getProps().getAutoSplit());
              id = md.open();
              hopGui.getProps().setAutoSplit(md.getToggleState());
            }

            if ((id & 0xFF) == 0) {
              // Means: "Yes" button clicked!

              // Only split A-->--B by putting C in between IF...
              // C-->--A or B-->--C don't exists...
              // A ==> hi.getFromEntry()
              // B ==> hi.getToEntry();
              // C ==> selectedTransform
              //
              if (workflowMeta.findWorkflowHop(selectedAction, hi.getFromAction()) == null
                  && workflowMeta.findWorkflowHop(hi.getToAction(), selectedAction) == null) {

                if (workflowMeta.findWorkflowHop(hi.getFromAction(), selectedAction, true)
                    == null) {
                  WorkflowHopMeta newhop1 = new WorkflowHopMeta(hi.getFromAction(), selectedAction);
                  if (hi.getFromAction().getAction().isUnconditional()) {
                    newhop1.setUnconditional();
                  }
                  workflowMeta.addWorkflowHop(newhop1);
                  hopGui.undoDelegate.addUndoNew(
                      workflowMeta,
                      new WorkflowHopMeta[] {
                        newhop1,
                      },
                      new int[] {
                        workflowMeta.indexOfWorkflowHop(newhop1),
                      },
                      true);
                }
                if (workflowMeta.findWorkflowHop(selectedAction, hi.getToAction(), true) == null) {
                  WorkflowHopMeta newhop2 = new WorkflowHopMeta(selectedAction, hi.getToAction());
                  if (selectedAction.getAction().isUnconditional()) {
                    newhop2.setUnconditional();
                  }
                  workflowMeta.addWorkflowHop(newhop2);
                  hopGui.undoDelegate.addUndoNew(
                      workflowMeta,
                      new WorkflowHopMeta[] {
                        newhop2,
                      },
                      new int[] {
                        workflowMeta.indexOfWorkflowHop(newhop2),
                      },
                      true);
                }

                int idx = workflowMeta.indexOfWorkflowHop(hi);
                hopGui.undoDelegate.addUndoDelete(
                    workflowMeta, new WorkflowHopMeta[] {hi}, new int[] {idx}, true);
                workflowMeta.removeWorkflowHop(idx);
              }
              // else: Silently discard this hop-split attempt.
            }
          }
          splitHop = false;
        }

        selectedActions = null;
        selectedNotes = null;
        selectedAction = null;
        selectedNote = null;
        startHopAction = null;
        endHopLocation = null;

        updateGui();
      } else {
        // Notes?
        if (selectedNote != null) {
          if (e.button == 1) {
            if (lastClick.x == real.x && lastClick.y == real.y) {
              // Flip selection when control is pressed!
              if (control) {
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
              selectedActions = workflowMeta.getSelectedActions();
              selectedNotes = workflowMeta.getSelectedNotes();

              // We moved around some items: store undo info...
              boolean also = false;
              if (selectedNotes != null
                  && selectedNotes.size() > 0
                  && previous_note_locations != null) {
                int[] indexes = workflowMeta.getNoteIndexes(selectedNotes);
                addUndoPosition(
                    selectedNotes.toArray(new NotePadMeta[selectedNotes.size()]),
                    indexes,
                    previous_note_locations,
                    workflowMeta.getSelectedNoteLocations(),
                    also);
                also = selectedActions != null && selectedActions.size() > 0;
              }
              if (selectedActions != null
                  && selectedActions.size() > 0
                  && previous_transform_locations != null) {
                int[] indexes = workflowMeta.getActionIndexes(selectedActions);
                addUndoPosition(
                    selectedActions.toArray(new ActionMeta[selectedActions.size()]),
                    indexes,
                    previous_transform_locations,
                    workflowMeta.getSelectedLocations(),
                    also);
              }
            }
          }

          selectedNotes = null;
          selectedActions = null;
          selectedAction = null;
          selectedNote = null;
          startHopAction = null;
          endHopLocation = null;
        }
      }
    }

    if (clickedWorkflowHop != null) {
      // Clicked on a hop
      //
      singleClick = true;
      singleClickType = SingleClickType.Hop;
      singleClickHop = clickedWorkflowHop;
    }
    clickedWorkflowHop = null;

    // Only do this "mouseUp()" if this is not part of a double click...
    //
    final boolean fSingleClick = singleClick;
    final SingleClickType fSingleClickType = singleClickType;
    final ActionMeta fSingleClickAction = singleClickAction;
    final NotePadMeta fSingleClickNote = singleClickNote;
    final WorkflowHopMeta fSingleClickHop = singleClickHop;

    if (PropsUi.getInstance().useDoubleClick()) {
      Display.getDefault()
          .timerExec(
              Display.getDefault().getDoubleClickTime(),
              () ->
                  showContextDialog(
                      e,
                      real,
                      fSingleClick,
                      fSingleClickType,
                      fSingleClickAction,
                      fSingleClickNote,
                      fSingleClickHop));
    } else {
      showContextDialog(
          e,
          real,
          fSingleClick,
          fSingleClickType,
          fSingleClickAction,
          fSingleClickNote,
          fSingleClickHop);
    }

    lastButton = 0;
  }

  private void showContextDialog(
      MouseEvent e,
      Point real,
      boolean fSingleClick,
      SingleClickType fSingleClickType,
      ActionMeta fSingleClickAction,
      NotePadMeta fSingleClickNote,
      WorkflowHopMeta fSingleClickHop) {

    // In any case clear the selection region...
    //
    selectionRegion = null;

    // See if there are transforms selected.
    // If we get a background single click then simply clear selection...
    //
    if (fSingleClickType == SingleClickType.Workflow) {
      if (workflowMeta.getSelectedActions().size() > 0
          || workflowMeta.getSelectedNotes().size() > 0) {
        workflowMeta.unselectAll();
        selectionRegion = null;
        updateGui();

        // Show a short tooltip
        //
        toolTip.hide();
        toolTip.setHideDelay(TOOLTIP_HIDE_DELAY_FLASH);
        toolTip.setImage(GuiResource.getInstance().getImageInfo());
        toolTip.setText(Const.CR + "  Selection cleared " + Const.CR);
        toolTip.show(new org.eclipse.swt.graphics.Point(e.x, e.y));

        return;
      }
    }

    if (!doubleClick) {

      // Just a single click on the background:
      // We have a bunch of possible actions for you...
      //
      if (fSingleClick && fSingleClickType != null) {
        IGuiContextHandler contextHandler = null;
        String message = null;
        switch (fSingleClickType) {
          case Workflow:
            message = "Select the action to execute or the action to create:";
            contextHandler = new HopGuiWorkflowContext(workflowMeta, this, real);
            break;
          case Action:
            message = "Select the action to take on action '" + fSingleClickAction.getName() + "':";
            contextHandler =
                new HopGuiWorkflowActionContext(workflowMeta, fSingleClickAction, this, real);
            break;
          case Note:
            message = "Select the note action to take:";
            contextHandler =
                new HopGuiWorkflowNoteContext(workflowMeta, fSingleClickNote, this, real);
            break;
          case Hop:
            message = "Select the hop action to take:";
            contextHandler =
                new HopGuiWorkflowHopContext(workflowMeta, fSingleClickHop, this, real);
            break;
          default:
            break;
        }
        if (contextHandler != null) {
          Shell parent = hopShell();
          org.eclipse.swt.graphics.Point p = parent.getDisplay().map(canvas, null, e.x, e.y);

          // Show the context dialog
          //
          ignoreNextClick =
              GuiContextUtil.getInstance().handleActionSelection(
                  parent, message, new Point(p.x, p.y), contextHandler);
        }
      }
    }
  }

  public void mouseMove(MouseEvent e) {
    boolean shift = (e.stateMask & SWT.SHIFT) != 0;
    noInputAction = null;

    // disable the tooltip
    //
    toolTip.hide();

    Point real = screen2real(e.x, e.y);
    // Remember the last position of the mouse for paste with keyboard
    //
    lastMove = real;

    if (iconOffset == null) {
      iconOffset = new Point(0, 0);
    }
    Point icon = new Point(real.x - iconOffset.x, real.y - iconOffset.y);

    if (noteOffset == null) {
      noteOffset = new Point(0, 0);
    }
    Point note = new Point(real.x - noteOffset.x, real.y - noteOffset.y);

    // Moved over an area?
    //
    AreaOwner areaOwner = getVisibleAreaOwner(real.x, real.y);
    if (areaOwner != null && areaOwner.getAreaType() != null) {
      ActionMeta actionCopy = null;
      switch (areaOwner.getAreaType()) {
        case ACTION_ICON:
          actionCopy = (ActionMeta) areaOwner.getOwner();
          break;
        default:
          break;
      }
    }

    //
    // First see if the icon we clicked on was selected.
    // If the icon was not selected, we should un-select all other
    // icons, selected and move only the one icon
    //
    if (selectedAction != null && !selectedAction.isSelected()) {
      workflowMeta.unselectAll();
      selectedAction.setSelected(true);
      selectedActions = new ArrayList<>();
      selectedActions.add(selectedAction);
      previous_transform_locations = new Point[] {selectedAction.getLocation()};
      redraw();
    } else if (selectedNote != null && !selectedNote.isSelected()) {
      workflowMeta.unselectAll();
      selectedNote.setSelected(true);
      selectedNotes = new ArrayList<>();
      selectedNotes.add(selectedNote);
      previous_note_locations = new Point[] {selectedNote.getLocation()};
      redraw();
    } else if (selectionRegion != null && startHopAction == null) {
      // Did we select a region...?
      //
      selectionRegion.width = real.x - selectionRegion.x;
      selectionRegion.height = real.y - selectionRegion.y;
      redraw();
    } else if (selectedAction != null && lastButton == 1 && !shift && startHopAction == null) {
      // Move around transforms & notes
      //
      //
      // One or more icons are selected and moved around...
      //
      // new : new position of the ICON (not the mouse pointer) dx : difference with previous
      // position
      //
      int dx = icon.x - selectedAction.getLocation().x;
      int dy = icon.y - selectedAction.getLocation().y;

      // See if we have a hop-split candidate
      //
      WorkflowHopMeta hi = findHop(icon.x + iconSize / 2, icon.y + iconSize / 2, selectedAction);
      if (hi != null) {
        // OK, we want to split the hop in 2
        //
        if (!hi.getFromAction().equals(selectedAction)
            && !hi.getToAction().equals(selectedAction)) {
          splitHop = true;
          lastHopSplit = hi;
          hi.split = true;
        }
      } else {
        if (lastHopSplit != null) {
          lastHopSplit.split = false;
          lastHopSplit = null;
          splitHop = false;
        }
      }

      selectedNotes = workflowMeta.getSelectedNotes();
      selectedActions = workflowMeta.getSelectedActions();

      // Adjust location of selected transforms...
      if (selectedActions != null) {
        for (int i = 0; i < selectedActions.size(); i++) {
          ActionMeta actionCopy = selectedActions.get(i);
          PropsUi.setLocation(
              actionCopy, actionCopy.getLocation().x + dx, actionCopy.getLocation().y + dy);
        }
        adjustScrolling();
      }
      // Adjust location of selected hops...
      if (selectedNotes != null) {
        for (int i = 0; i < selectedNotes.size(); i++) {
          NotePadMeta ni = selectedNotes.get(i);
          PropsUi.setLocation(ni, ni.getLocation().x + dx, ni.getLocation().y + dy);
        }
        adjustScrolling();
      }

      redraw();
    } else if ((startHopAction != null && endHopAction == null)
        || (endHopAction != null && startHopAction == null)) {
      // Are we creating a new hop with the middle button or pressing SHIFT?
      //

      ActionMeta actionCopy = workflowMeta.getAction(real.x, real.y, iconSize);
      endHopLocation = new Point(real.x, real.y);
      if (actionCopy != null
          && ((startHopAction != null && !startHopAction.equals(actionCopy))
              || (endHopAction != null && !endHopAction.equals(actionCopy)))) {
        if (hopCandidate == null) {
          // See if the transform accepts input. If not, we can't create a new hop...
          //
          if (startHopAction != null) {
            if (!actionCopy.isStart()) {
              hopCandidate = new WorkflowHopMeta(startHopAction, actionCopy);
              endHopLocation = null;
            } else {
              noInputAction = actionCopy;
              toolTip.setImage(null);
              toolTip.setText("The start action can only be used at the start of a Workflow");
              toolTip.show(new org.eclipse.swt.graphics.Point(real.x, real.y));
            }
          } else if (endHopAction != null) {
            hopCandidate = new WorkflowHopMeta(actionCopy, endHopAction);
            endHopLocation = null;
          }
        }
      } else {
        if (hopCandidate != null) {
          hopCandidate = null;
          redraw();
        }
      }

      redraw();
    }

    // Move around notes & transforms
    //
    if (selectedNote != null) {
      if (lastButton == 1 && !shift) {
        /*
         * One or more notes are selected and moved around...
         *
         * new : new position of the note (not the mouse pointer) dx : difference with previous position
         */
        int dx = note.x - selectedNote.getLocation().x;
        int dy = note.y - selectedNote.getLocation().y;

        selectedNotes = workflowMeta.getSelectedNotes();
        selectedActions = workflowMeta.getSelectedActions();

        // Adjust location of selected transforms...
        if (selectedActions != null) {
          for (int i = 0; i < selectedActions.size(); i++) {
            ActionMeta actionCopy = selectedActions.get(i);
            PropsUi.setLocation(
                actionCopy, actionCopy.getLocation().x + dx, actionCopy.getLocation().y + dy);
          }
        }
        // Adjust location of selected hops...
        if (selectedNotes != null) {
          for (int i = 0; i < selectedNotes.size(); i++) {
            NotePadMeta ni = selectedNotes.get(i);
            PropsUi.setLocation(ni, ni.getLocation().x + dx, ni.getLocation().y + dy);
          }
        }

        redraw();
      }
    }
  }

  public void mouseHover(MouseEvent e) {

    boolean tip = true;

    // toolTip.hide();
    Point real = screen2real(e.x, e.y);

    // Show a tool tip upon mouse-over of an object on the canvas
    if (tip && !helpTip.isVisible()) {
      setToolTip(real.x, real.y, e.x, e.y);
    }
  }

  public void mouseEnter(MouseEvent event) {}

  public void mouseExit(MouseEvent event) {}

  public void mouseScrolled(MouseEvent event) {
    if (event.count > 0) {
      // scroll up
      zoomIn();
    } else if (event.count < 0) {
      // scroll down
      zoomOut();
    }
  }

  public void adjustScrolling() {
    // What's the new canvas size?
    //
    adjustScrolling(workflowMeta.getMaximum());
  }

  private void addCandidateAsHop() {
    if (hopCandidate != null) {

      // A couple of sanity checks...
      //
      if (hopCandidate.getFromAction() == null || hopCandidate.getToAction() == null) {
        return;
      }
      if (hopCandidate.getFromAction().equals(hopCandidate.getToAction())) {
        return;
      }

      if (!hopCandidate.getFromAction().isEvaluation()
          && hopCandidate.getFromAction().isUnconditional()) {
        hopCandidate.setUnconditional();
      } else {
        hopCandidate.setConditional();
        int nr = workflowMeta.findNrNextActions(hopCandidate.getFromAction());

        // If there is one green link: make this one red! (or
        // vice-versa)
        if (nr == 1) {
          ActionMeta jge = workflowMeta.findNextAction(hopCandidate.getFromAction(), 0);
          WorkflowHopMeta other = workflowMeta.findWorkflowHop(hopCandidate.getFromAction(), jge);
          if (other != null) {
            hopCandidate.setEvaluation(!other.getEvaluation());
          }
        }
      }

      if (checkIfHopAlreadyExists(workflowMeta, hopCandidate)) {
        boolean cancel = false;
        workflowMeta.addWorkflowHop(hopCandidate);
        if (workflowMeta.hasLoop(hopCandidate.getToAction())) {
          MessageBox mb = new MessageBox(hopGui.getShell(), SWT.OK | SWT.CANCEL | SWT.ICON_WARNING);
          mb.setMessage(BaseMessages.getString(PKG, "WorkflowGraph.Dialog.HopCausesLoop.Message"));
          mb.setText(BaseMessages.getString(PKG, "WorkflowGraph.Dialog.HopCausesLoop.Title"));
          int choice = mb.open();
          if (choice == SWT.CANCEL) {
            workflowMeta.removeWorkflowHop(hopCandidate);
            cancel = true;
          }
        }
        if (!cancel) {
          hopGui.undoDelegate.addUndoNew(
              workflowMeta,
              new WorkflowHopMeta[] {hopCandidate},
              new int[] {workflowMeta.indexOfWorkflowHop(hopCandidate)});
        }
        clearSettings();
        redraw();
      }
    }
  }

  public boolean checkIfHopAlreadyExists(WorkflowMeta workflowMeta, WorkflowHopMeta newHop) {
    boolean ok = true;
    if (workflowMeta.findWorkflowHop(newHop.getFromAction(), newHop.getToAction(), true) != null) {
      MessageBox mb = new MessageBox(hopShell(), SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "WorkflowGraph.Dialog.HopExists.Message")); // "This hop already exists!"
      mb.setText(BaseMessages.getString(PKG, "WorkflowGraph.Dialog.HopExists.Title")); // Error!
      mb.open();
      ok = false;
    }

    return ok;
  }

  public AreaOwner getVisibleAreaOwner(int x, int y) {
    for (int i = areaOwners.size() - 1; i >= 0; i--) {
      AreaOwner areaOwner = areaOwners.get(i);
      if (areaOwner.contains(x, y)) {
        return areaOwner;
      }
    }
    return null;
  }

  protected void asyncRedraw() {
    hopGui
        .getDisplay()
        .asyncExec(
            () -> {
              if (!isDisposed()) {
                redraw();
              }
            });
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ZOOM_LEVEL,
      label = "i18n:org.apache.hop.ui.hopgui:HopGui.Toolbar.Zoom",
      toolTip = "Zoom in our out",
      // TODO: i18n does not work on dropdown
      // toolTip = "i18n::HopGui.Toolbar.Zoom.ToolTip",
      type = GuiToolbarElementType.COMBO,
      alignRight = true,
      comboValuesMethod = "getZoomLevels")
  public void zoomLevel() {
    readMagnification();
    setFocus();
  }

  public List<String> getZoomLevels() {
    return Arrays.asList(PipelinePainter.magnificationDescriptions);
  }

  private void addToolBar() {

    try {
      // Create a new toolbar at the top of the main composite...
      //
      toolBar = new ToolBar(this, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
      toolBarWidgets = new GuiToolbarWidgets();
      toolBarWidgets.registerGuiPluginObject(this);
      toolBarWidgets.createToolbarWidgets(toolBar, GUI_PLUGIN_TOOLBAR_PARENT_ID);
      FormData layoutData = new FormData();
      layoutData.left = new FormAttachment(0, 0);
      layoutData.top = new FormAttachment(0, 0);
      layoutData.right = new FormAttachment(100, 0);
      toolBar.setLayoutData(layoutData);
      toolBar.pack();

      // enable / disable the icons in the toolbar too.
      //
      updateGui();

    } catch (Throwable t) {
      log.logError("Error setting up the navigation toolbar for HopUI", t);
      new ErrorDialog(
          hopShell(),
          "Error",
          "Error setting up the navigation toolbar for HopGUI",
          new Exception(t));
    }
  }

  public void setZoomLabel() {
    Combo zoomLabel = (Combo) toolBarWidgets.getWidgetsMap().get(TOOLBAR_ITEM_ZOOM_LEVEL);
    if (zoomLabel == null || zoomLabel.isDisposed()) {
      return;
    }
    String newString = Math.round(magnification * 100) + "%";
    String oldString = zoomLabel.getText();
    if (!newString.equals(oldString)) {
      zoomLabel.setText(Math.round(magnification * 100) + "%");
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_START,
      // label = "Start",
      toolTip = "i18n::WorkflowGraph.Toolbar.Start.Tooltip",
      image = "ui/images/run.svg")
  @Override
  public void start() {
    workflowMeta.setShowDialog(workflowMeta.isAlwaysShowRunOptions());
    Thread thread =
        new Thread() {
          @Override
          public void run() {
            getDisplay()
                .asyncExec(
                    () -> {
                      try {
                        workflowRunDelegate.executeWorkflow(
                            hopGui.getVariables(), workflowMeta, null);
                      } catch (Exception e) {
                        new ErrorDialog(
                            getShell(),
                            "Execute workflow",
                            "There was an error during workflow execution",
                            e);
                      }
                    });
          }
        };
    thread.start();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_STOP,
      // label = "Stop",
      toolTip = "i18n::WorkflowGraph.Toolbar.Stop.Tooltip",
      image = "ui/images/stop.svg")
  @Override
  public void stop() {

    if ((isRunning() && !halting)) {
      halting = true;
      workflow.stopExecution();
      log.logBasic(BaseMessages.getString(PKG, "WorkflowLog.Log.ProcessingOfWorkflowStopped"));

      halting = false;

      updateGui();
    }
  }

  @Override
  public void pause() {
    // TODO: Implement on a workflow level
  }

  @Override
  public void resume() {
    // TODO: Implement on a workflow level
  }

  @Override
  public void preview() {
    // Not possible for workflows
  }

  @Override
  public void debug() {
    // Not possible for workflows (yet)
  }

  /** Allows for magnifying to any percentage entered by the user... */
  private void readMagnification() {
    Combo zoomLabel = (Combo) toolBarWidgets.getWidgetsMap().get(TOOLBAR_ITEM_ZOOM_LEVEL);
    if (zoomLabel == null || zoomLabel.isDisposed()) {
      return;
    }
    String possibleText = zoomLabel.getText();
    possibleText = possibleText.replace("%", "");

    float possibleFloatMagnification;
    try {
      possibleFloatMagnification = Float.parseFloat(possibleText) / 100;
      magnification = possibleFloatMagnification;
      if (zoomLabel.getText().indexOf('%') < 0) {
        zoomLabel.setText(zoomLabel.getText().concat("%"));
      }
    } catch (Exception e) {
      MessageBox mb = new MessageBox(hopShell(), SWT.YES | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "PipelineGraph.Dialog.InvalidZoomMeasurement.Message", zoomLabel.getText()));
      mb.setText(BaseMessages.getString(PKG, "PipelineGraph.Dialog.InvalidZoomMeasurement.Title"));
      mb.open();
    }

    adjustScrolling();

    /*// When zooming out we want to correct the scroll bars.
    //
    float factor = magnification / oldMagnification;
    int newHThumb = Math.min((int) (horizontalScrollBar.getThumb() / factor), 100);
    horizontalScrollBar.setThumb(newHThumb);
    horizontalScrollBar.setSelection((int) (horizontalScrollBar.getSelection() * factor));
    int newVThumb = Math.min((int) (verticalScrollBar.getThumb() / factor), 100);
    verticalScrollBar.setThumb(newVThumb);
    verticalScrollBar.setSelection((int) (verticalScrollBar.getSelection() * factor));*/

    redraw();
  }

  public void selectInRect(WorkflowMeta workflowMeta, org.apache.hop.core.gui.Rectangle rect) {
    int i;
    for (i = 0; i < workflowMeta.nrActions(); i++) {
      ActionMeta je = workflowMeta.getAction(i);
      Point p = je.getLocation();
      if (((p.x >= rect.x && p.x <= rect.x + rect.width)
              || (p.x >= rect.x + rect.width && p.x <= rect.x))
          && ((p.y >= rect.y && p.y <= rect.y + rect.height)
              || (p.y >= rect.y + rect.height && p.y <= rect.y))) {
        je.setSelected(true);
      }
    }
    for (i = 0; i < workflowMeta.nrNotes(); i++) {
      NotePadMeta ni = workflowMeta.getNote(i);
      Point a = ni.getLocation();
      Point b = new Point(a.x + ni.width, a.y + ni.height);
      if (rect.contains(a.x, a.y) && rect.contains(b.x, b.y)) {
        ni.setSelected(true);
      }
    }
  }

  public boolean setFocus() {
    return (canvas != null && !canvas.isDisposed()) ? canvas.setFocus() : false;
  }

  public static void showOnlyStartOnceMessage(Shell shell) {
    MessageBox mb = new MessageBox(shell, SWT.YES | SWT.ICON_ERROR);
    mb.setMessage(BaseMessages.getString(PKG, "WorkflowGraph.Dialog.OnlyUseStartOnce.Message"));
    mb.setText(BaseMessages.getString(PKG, "WorkflowGraph.Dialog.OnlyUseStartOnce.Title"));
    mb.open();
  }

  public void deleteSelected(ActionMeta selectedAction) {
    List<ActionMeta> selection = workflowMeta.getSelectedActions();
    if (currentAction == null
        && selectedAction == null
        && selection.isEmpty()
        && workflowMeta.getSelectedNotes().isEmpty()) {
      return; // nothing to do
    }

    if (selectedAction != null && selection.size() == 0) {
      workflowActionDelegate.deleteAction(workflowMeta, selectedAction);
      return;
    }

    if (selection.size() > 0) {
      workflowActionDelegate.deleteActions(workflowMeta, selection);
    }
    if (workflowMeta.getSelectedNotes().size() > 0) {
      notePadDelegate.deleteNotes(workflowMeta, workflowMeta.getSelectedNotes());
    }
  }

  public void clearSettings() {
    selectedAction = null;
    selectedNote = null;
    selectedActions = null;
    selectedNotes = null;
    selectionRegion = null;
    hopCandidate = null;
    lastHopSplit = null;
    lastButton = 0;
    startHopAction = null;
    endHopAction = null;
    iconOffset = null;
    workflowMeta.unselectAll();
    for (int i = 0; i < workflowMeta.nrWorkflowHops(); i++) {
      workflowMeta.getWorkflowHop(i).setSplit(false);
    }
  }

  public Point getRealPosition(Composite canvas, int x, int y) {
    Point p = new Point(0, 0);
    Composite follow = canvas;
    while (follow != null) {
      Point xy = new Point(follow.getLocation().x, follow.getLocation().y);
      p.x += xy.x;
      p.y += xy.y;
      follow = follow.getParent();
    }

    p.x = x - p.x - 8;
    p.y = y - p.y - 48;

    return screen2real(p.x, p.y);
  }

  /**
   * See if location (x,y) is on a line between two transforms: the hop!
   *
   * @param x
   * @param y
   * @return the pipeline hop on the specified location, otherwise: null
   */
  private WorkflowHopMeta findWorkflowHop(int x, int y) {
    return findHop(x, y, null);
  }

  /**
   * See if location (x,y) is on a line between two transforms: the hop!
   *
   * @param x
   * @param y
   * @param exclude the transform to exclude from the hops (from or to location). Specify null if no
   *     transform is to be excluded.
   * @return the pipeline hop on the specified location, otherwise: null
   */
  private WorkflowHopMeta findHop(int x, int y, ActionMeta exclude) {
    int i;
    WorkflowHopMeta online = null;
    for (i = 0; i < workflowMeta.nrWorkflowHops(); i++) {
      WorkflowHopMeta hi = workflowMeta.getWorkflowHop(i);
      ActionMeta fs = hi.getFromAction();
      ActionMeta ts = hi.getToAction();

      if (fs == null || ts == null) {
        return null;
      }

      // If either the "from" or "to" transform is excluded, skip this hop.
      //
      if (exclude != null && (exclude.equals(fs) || exclude.equals(ts))) {
        continue;
      }

      int[] line = getLine(fs, ts);

      if (pointOnLine(x, y, line)) {
        online = hi;
      }
    }
    return online;
  }

  protected int[] getLine(ActionMeta fs, ActionMeta ts) {
    if (fs == null || ts == null) {
      return null;
    }

    Point from = fs.getLocation();
    Point to = ts.getLocation();
    offset = getOffset();

    int x1 = from.x + iconSize / 2;
    int y1 = from.y + iconSize / 2;

    int x2 = to.x + iconSize / 2;
    int y2 = to.y + iconSize / 2;

    return new int[] {x1, y1, x2, y2};
  }

  private void showHelpTip(int x, int y, String tipTitle, String tipMessage) {

    helpTip.setTitle(tipTitle);
    helpTip.setMessage(tipMessage);
    helpTip.setCheckBoxMessage(
        BaseMessages.getString(PKG, "WorkflowGraph.HelpToolTip.DoNotShowAnyMoreCheckBox.Message"));
    // helpTip.hide();
    // int iconSize = spoon.props.getIconSize();
    org.eclipse.swt.graphics.Point location = new org.eclipse.swt.graphics.Point(x - 5, y - 5);

    helpTip.show(location);
  }

  @GuiContextAction(
      id = "workflow-graph-action-10050-create-hop",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Create,
      name = "Create hop",
      tooltip = "Create a new hop between 2 actions",
      image = "ui/images/hop.svg",
      category = "Basic",
      categoryOrder = "1")
  public void newHopCandidate(HopGuiWorkflowActionContext context) {
    startHopAction = context.getActionMeta();
    endHopAction = null;
    redraw();
  }

  @GuiContextAction(
      id = "workflow-graph-action-10800-edit-description",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Edit action description",
      tooltip = "Modify the action description",
      image = "ui/images/edit.svg",
      category = "Basic",
      categoryOrder = "1")
  public void editActionDescription(HopGuiWorkflowActionContext context) {
    ActionMeta action = context.getActionMeta();
    String title = BaseMessages.getString(PKG, "WorkflowGraph.Dialog.EditDescription.Title");
    String message = BaseMessages.getString(PKG, "WorkflowGraph.Dialog.EditDescription.Message");
    EnterTextDialog dd =
        new EnterTextDialog(hopShell(), title, message, context.getActionMeta().getDescription());
    String des = dd.open();
    if (des != null) {
      action.setDescription(des);
      action.setChanged();
      updateGui();
    }
  }

  /** Go from serial to parallel to serial execution */
  @GuiContextAction(
      id = "workflow-graph-transform-10600-parallel",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Parallel execution",
      tooltip = "Enable of disable parallel execution of next actions",
      image = "ui/images/parallel.svg",
      category = "Advanced",
      categoryOrder = "3")
  public void editActionParallel(HopGuiWorkflowActionContext context) {

    ActionMeta action = context.getActionMeta();
    ActionMeta originalAction = (ActionMeta) action.cloneDeep();

    action.setLaunchingInParallel(!action.isLaunchingInParallel());
    ActionMeta jeNew = (ActionMeta) action.cloneDeep();

    hopGui.undoDelegate.addUndoChange(
        workflowMeta,
        new ActionMeta[] {originalAction},
        new ActionMeta[] {jeNew},
        new int[] {workflowMeta.indexOfAction(jeNew)});
    workflowMeta.setChanged();

    if (action.isLaunchingInParallel()) {
      // Show a warning (optional)
      //
      if ("Y"
          .equalsIgnoreCase(
              hopGui.getProps().getCustomParameter(STRING_PARALLEL_WARNING_PARAMETER, "Y"))) {
        MessageDialogWithToggle md =
            new MessageDialogWithToggle(
                hopShell(),
                BaseMessages.getString(PKG, "WorkflowGraph.ParallelActionsWarning.DialogTitle"),
                BaseMessages.getString(
                        PKG, "WorkflowGraph.ParallelActionsWarning.DialogMessage", Const.CR)
                    + Const.CR,
                SWT.ICON_WARNING,
                new String[] {
                  BaseMessages.getString(PKG, "WorkflowGraph.ParallelActionsWarning.Option1")
                },
                BaseMessages.getString(PKG, "WorkflowGraph.ParallelActionsWarning.Option2"),
                "N"
                    .equalsIgnoreCase(
                        hopGui
                            .getProps()
                            .getCustomParameter(STRING_PARALLEL_WARNING_PARAMETER, "Y")));
        md.open();
        hopGui
            .getProps()
            .setCustomParameter(STRING_PARALLEL_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y");
      }
    }
    redraw();
  }

  @GuiContextAction(
      id = "workflow-graph-action-10900-delete",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Delete,
      name = "Delete this action",
      tooltip = "Delete the selected action from the workflow",
      image = "ui/images/delete.svg",
      category = "Basic",
      categoryOrder = "1")
  public void deleteAction(HopGuiWorkflowActionContext context) {
    deleteSelected(context.getActionMeta());
    adjustScrolling();
    redraw();
  }

  protected synchronized void setMenu(int x, int y) {

    currentMouseX = x;
    currentMouseY = y;
  }

  @GuiKeyboardShortcut(control = true, key = 'a')
  @GuiOsxKeyboardShortcut(command = true, key = 'a')
  public void selectAll() {
    workflowMeta.selectAll();
    updateGui();
  }

  @GuiKeyboardShortcut(key = SWT.ESC)
  @Override
  public void unselectAll() {
    clearSettings();
    updateGui();
  }

  @GuiKeyboardShortcut(control = true, key = 'c')
  @GuiOsxKeyboardShortcut(command = true, key = 'c')
  @Override
  public void copySelectedToClipboard() {
    if (workflowLogDelegate.hasSelectedText()) {
      workflowLogDelegate.copySelected();
    } else {
      workflowClipboardDelegate.copySelected(
          workflowMeta, workflowMeta.getSelectedActions(), workflowMeta.getSelectedNotes());
    }
  }

  @GuiKeyboardShortcut(control = true, key = 'x')
  @GuiOsxKeyboardShortcut(command = true, key = 'x')
  @Override
  public void cutSelectedToClipboard() {
    workflowClipboardDelegate.copySelected(
        workflowMeta, workflowMeta.getSelectedActions(), workflowMeta.getSelectedNotes());
    deleteSelected();
  }

  @GuiKeyboardShortcut(key = SWT.DEL)
  @Override
  public void deleteSelected() {
    deleteSelected(null);
  }

  @GuiKeyboardShortcut(control = true, key = 'v')
  @GuiOsxKeyboardShortcut(command = true, key = 'v')
  @Override
  public void pasteFromClipboard() {
    workflowClipboardDelegate.pasteXml(
        workflowMeta,
        workflowClipboardDelegate.fromClipboard(),
        lastMove == null ? new Point(50, 50) : lastMove);
  }

  @GuiContextAction(
      id = "workflow-graph-workflow-paste",
      parentId = HopGuiWorkflowContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Paste from the clipboard",
      tooltip = "Paste actions, notes or a whole workflow from the clipboard",
      image = "ui/images/paste.svg",
      category = "Basic",
      categoryOrder = "1")
  public void pasteFromClipboard(HopGuiWorkflowContext context) {
    workflowClipboardDelegate.pasteXml(
        workflowMeta, workflowClipboardDelegate.fromClipboard(), context.getClick());
    adjustScrolling();
  }

  @GuiContextAction(
      id = "workflow-graph-edit-workflow",
      parentId = HopGuiWorkflowContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Edit workflow",
      tooltip = "Edit the workflow properties",
      image = "ui/images/workflow.svg",
      category = "Basic",
      categoryOrder = "1")
  public void editWorkflowProperties(HopGuiWorkflowContext context) {
    editProperties(workflowMeta, hopGui, true);
  }

  @GuiToolbarElement(
    root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    id = TOOLBAR_ITEM_EDIT_WORKFLOW,
    toolTip = "i18n::WorkflowGraph.Toolbar.EditWorkflow.Tooltip",
    image = "ui/images/workflow.svg"
    )
  @GuiKeyboardShortcut(control=true, key='l')
  @GuiOsxKeyboardShortcut(command = true, key='l')
  public void editWorkflowProperties() {
    editProperties(workflowMeta, hopGui, true);
  }

  @GuiContextAction(
      id = "workflow-graph-new-note",
      parentId = HopGuiWorkflowContext.CONTEXT_ID,
      type = GuiActionType.Create,
      name = "Create a note",
      tooltip = "Create a new note",
      image = "ui/images/note-add.svg",
      category = "Basic",
      categoryOrder = "1")
  public void newNote(HopGuiWorkflowContext context) {
    String title = BaseMessages.getString(PKG, "WorkflowGraph.Dialog.EditNote.Title");
    NotePadDialog dd = new NotePadDialog(variables, hopShell(), title);
    NotePadMeta n = dd.open();
    if (n != null) {
      NotePadMeta npi =
          new NotePadMeta(
              n.getNote(),
              context.getClick().x,
              context.getClick().y,
              ConstUi.NOTE_MIN_SIZE,
              ConstUi.NOTE_MIN_SIZE,
              n.getFontName(),
              n.getFontSize(),
              n.isFontBold(),
              n.isFontItalic(),
              n.getFontColorRed(),
              n.getFontColorGreen(),
              n.getFontColorBlue(),
              n.getBackGroundColorRed(),
              n.getBackGroundColorGreen(),
              n.getBackGroundColorBlue(),
              n.getBorderColorRed(),
              n.getBorderColorGreen(),
              n.getBorderColorBlue());
      workflowMeta.addNote(npi);
      hopGui.undoDelegate.addUndoNew(
          workflowMeta, new NotePadMeta[] {npi}, new int[] {workflowMeta.indexOfNote(npi)});
      adjustScrolling();
      redraw();
    }
  }

  public void setCurrentNote(NotePadMeta ni) {
    this.ni = ni;
  }

  public NotePadMeta getCurrentNote() {
    return ni;
  }

  @GuiContextAction(
      id = "workflow-graph-10-edit-note",
      parentId = HopGuiWorkflowNoteContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Edit",
      tooltip = "Edit the note",
      image = "ui/images/edit.svg",
      category = "Basic",
      categoryOrder = "1")
  public void editNote(HopGuiWorkflowNoteContext context) {
    selectionRegion = null;
    editNote(context.getNotePadMeta());
  }

  @GuiContextAction(
      id = "workflow-graph-20-delete-note",
      parentId = HopGuiWorkflowNoteContext.CONTEXT_ID,
      type = GuiActionType.Delete,
      name = "Delete",
      tooltip = "Delete the note",
      image = "ui/images/delete.svg",
      category = "Basic",
      categoryOrder = "1")
  public void deleteNote(HopGuiWorkflowNoteContext context) {
    selectionRegion = null;
    NotePadMeta note = context.getNotePadMeta();
    int idx = workflowMeta.indexOfNote(note);
    if (idx >= 0) {
      workflowMeta.removeNote(idx);
      hopGui.undoDelegate.addUndoDelete(workflowMeta, new NotePadMeta[] {note}, new int[] {idx});
    }
    adjustScrolling();
    redraw();
  }

  public void raiseNote() {
    selectionRegion = null;
    int idx = workflowMeta.indexOfNote(getCurrentNote());
    if (idx >= 0) {
      workflowMeta.raiseNote(idx);
      // spoon.addUndoRaise(workflowMeta, new NotePadMeta[] {getCurrentNote()}, new int[] {idx} );
    }
    redraw();
  }

  public void lowerNote() {
    selectionRegion = null;
    int idx = workflowMeta.indexOfNote(getCurrentNote());
    if (idx >= 0) {
      workflowMeta.lowerNote(idx);
      // spoon.addUndoLower(workflowMeta, new NotePadMeta[] {getCurrentNote()}, new int[] {idx} );
    }
    redraw();
  }

  @GuiContextAction(
      id = "workflow-graph-hop-10010-hop-enable",
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Enable hop",
      tooltip = "Enable the hop",
      image = "ui/images/hop.svg",
      category = "Basic",
      categoryOrder = "1")
  public void enableHop(HopGuiWorkflowHopContext context) {
    WorkflowHopMeta hop = context.getHopMeta();
    if (!hop.isEnabled()) {
      WorkflowHopMeta before = hop.clone();
      hop.setEnabled(true);
      if (checkHopLoop(hop, false)) {
        WorkflowHopMeta after = hop.clone();
        hopGui.undoDelegate.addUndoChange(
            workflowMeta,
            new WorkflowHopMeta[] {before},
            new WorkflowHopMeta[] {after},
            new int[] {workflowMeta.indexOfWorkflowHop(hop)});
      }
      updateGui();
    }
  }

  @GuiContextAction(
      id = "workflow-graph-hop-10000-hop-disable",
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Disable hop",
      tooltip = "Disable the hop",
      image = "ui/images/HOP_disable.svg",
      category = "Basic",
      categoryOrder = "1")
  public void disableHop(HopGuiWorkflowHopContext context) {
    WorkflowHopMeta hop = context.getHopMeta();
    if (hop.isEnabled()) {
      WorkflowHopMeta before = hop.clone();
      hop.setEnabled(false);
      updateGui();
      WorkflowHopMeta after = hop.clone();
      hopGui.undoDelegate.addUndoChange(
          workflowMeta,
          new WorkflowHopMeta[] {before},
          new WorkflowHopMeta[] {after},
          new int[] {workflowMeta.indexOfWorkflowHop(hop)});
    }
  }

  private boolean checkHopLoop(WorkflowHopMeta hop, boolean originalState) {
    if (!originalState && (workflowMeta.hasLoop(hop.getToAction()))) {
      MessageBox mb = new MessageBox(hopShell(), SWT.CANCEL | SWT.OK | SWT.ICON_WARNING);
      mb.setMessage(
          BaseMessages.getString(PKG, "WorkflowGraph.Dialog.LoopAfterHopEnabled.Message"));
      mb.setText(BaseMessages.getString(PKG, "WorkflowGraph.Dialog.LoopAfterHopEnabled.Title"));
      int choice = mb.open();
      if (choice == SWT.CANCEL) {
        hop.setEnabled(originalState);
        return false;
      }
    }
    return true;
  }

  @GuiContextAction(
      id = "workflow-graph-hop-10020-hop-delete",
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Delete,
      name = "Delete hop",
      tooltip = "Delete the hop between 2 actions",
      image = "ui/images/HOP_delete.svg",
      category = "Basic",
      categoryOrder = "1")
  public void deleteHop(HopGuiWorkflowHopContext context) {
    workflowHopDelegate.delHop(workflowMeta, context.getHopMeta());
    updateGui();
  }

  @GuiContextAction(
      id = "workflow-graph-hop-10030-hop-unconditional",
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Unconditional hop",
      tooltip = "Go to the next execution regardless of execution outcome",
      image = "ui/images/unconditional-hop.svg",
      category = "Routing",
      categoryOrder = "2")
  public void setHopUnconditional(HopGuiWorkflowHopContext context) {
    WorkflowHopMeta hop = context.getHopMeta();
    WorkflowHopMeta before = hop.clone();
    if (!hop.isUnconditional()) {
      hop.setUnconditional();
      WorkflowHopMeta after = hop.clone();
      hopGui.undoDelegate.addUndoChange(
          workflowMeta,
          new WorkflowHopMeta[] {before},
          new WorkflowHopMeta[] {after},
          new int[] {workflowMeta.indexOfWorkflowHop(hop)});
    }
    updateGui();
  }

  @GuiContextAction(
      id = "workflow-graph-hop-10040-hop-evaluation-success",
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Success hop",
      tooltip = "Go to the next execution when successful",
      image = "ui/images/true.svg",
      category = "Routing",
      categoryOrder = "2")
  public void setHopEvaluationTrue(HopGuiWorkflowHopContext context) {
    WorkflowHopMeta hop = context.getHopMeta();
    WorkflowHopMeta before = hop.clone();
    hop.setConditional();
    hop.setEvaluation(true);
    WorkflowHopMeta after = hop.clone();
    hopGui.undoDelegate.addUndoChange(
        workflowMeta,
        new WorkflowHopMeta[] {before},
        new WorkflowHopMeta[] {after},
        new int[] {workflowMeta.indexOfWorkflowHop(hop)});

    updateGui();
  }

  @GuiContextAction(
      id = "workflow-graph-hop-10050-hop-evaluation-failure",
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Failure hop",
      tooltip = "Go to the next execution when unsuccessful",
      image = "ui/images/false.svg",
      category = "Routing",
      categoryOrder = "2")
  public void setHopEvaluationFalse(HopGuiWorkflowHopContext context) {
    WorkflowHopMeta hop = context.getHopMeta();
    WorkflowHopMeta before = hop.clone();
    hop.setConditional();
    hop.setEvaluation(false);
    WorkflowHopMeta after = hop.clone();
    hopGui.undoDelegate.addUndoChange(
        workflowMeta,
        new WorkflowHopMeta[] {before},
        new WorkflowHopMeta[] {after},
        new int[] {workflowMeta.indexOfWorkflowHop(hop)});

    updateGui();
  }

  // TODO
  public void enableHopsBetweenSelectedActions() {
    enableHopsBetweenSelectedActions(true);
  }

  // TODO
  public void disableHopsBetweenSelectedActions() {
    enableHopsBetweenSelectedActions(false);
  }

  /** This method enables or disables all the hops between the selected Actions. */
  public void enableHopsBetweenSelectedActions(boolean enabled) {
    List<ActionMeta> list = workflowMeta.getSelectedActions();

    boolean hasLoop = false;

    for (int i = 0; i < workflowMeta.nrWorkflowHops(); i++) {
      WorkflowHopMeta hop = workflowMeta.getWorkflowHop(i);
      if (list.contains(hop.getFromAction()) && list.contains(hop.getToAction())) {

        WorkflowHopMeta before = (WorkflowHopMeta) hop.clone();
        hop.setEnabled(enabled);
        WorkflowHopMeta after = (WorkflowHopMeta) hop.clone();
        hopGui.undoDelegate.addUndoChange(
            workflowMeta,
            new WorkflowHopMeta[] {before},
            new WorkflowHopMeta[] {after},
            new int[] {workflowMeta.indexOfWorkflowHop(hop)});
        if (workflowMeta.hasLoop(hop.getToAction())) {
          hasLoop = true;
        }
      }
    }

    if (hasLoop && enabled) {
      MessageBox mb = new MessageBox(hopShell(), SWT.OK | SWT.ICON_WARNING);
      mb.setMessage(
          BaseMessages.getString(PKG, "WorkflowGraph.Dialog.LoopAfterHopEnabled.Message"));
      mb.setText(BaseMessages.getString(PKG, "WorkflowGraph.Dialog.LoopAfterHopEnabled.Title"));
      mb.open();
    }

    updateGui();
  }

  @GuiContextAction(
      id = "workflow-graph-hop-10060-hop-enable-downstream",
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Enable downstream hops",
      tooltip = "Enable all disabled downstream hops",
      image = "ui/images/HOP_enable_downstream.svg",
      category = "Bulk",
      categoryOrder = "3")
  public void enableHopsDownstream(HopGuiWorkflowHopContext context) {
    enableDisableHopsDownstream(context.getHopMeta(), true);
  }

  @GuiContextAction(
      id = "workflow-graph-hop-10070-hop-disable-downstream",
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Disable downstream hops",
      tooltip = "Disable all enabled downstream hops",
      image = "ui/images/HOP_disable_downstream.svg",
      category = "Bulk",
      categoryOrder = "3")
  public void disableHopsDownstream(HopGuiWorkflowHopContext context) {
    enableDisableHopsDownstream(context.getHopMeta(), false);
  }

  public void enableDisableHopsDownstream(WorkflowHopMeta hop, boolean enabled) {
    if (hop == null) {
      return;
    }
    WorkflowHopMeta before = (WorkflowHopMeta) hop.clone();
    hop.setEnabled(enabled);
    WorkflowHopMeta after = (WorkflowHopMeta) hop.clone();
    hopGui.undoDelegate.addUndoChange(
        workflowMeta,
        new WorkflowHopMeta[] {before},
        new WorkflowHopMeta[] {after},
        new int[] {workflowMeta.indexOfWorkflowHop(hop)});

    Set<ActionMeta> checkedActions =
        enableDisableNextHops(hop.getToAction(), enabled, new HashSet<>());

    if (checkedActions.stream().anyMatch(action -> workflowMeta.hasLoop(action))) {
      MessageBox mb = new MessageBox(hopShell(), SWT.OK | SWT.ICON_WARNING);
      mb.setMessage(
          BaseMessages.getString(PKG, "WorkflowGraph.Dialog.LoopAfterHopEnabled.Message"));
      mb.setText(BaseMessages.getString(PKG, "WorkflowGraph.Dialog.LoopAfterHopEnabled.Title"));
      mb.open();
    }

    updateGui();
  }

  private Set<ActionMeta> enableDisableNextHops(
      ActionMeta from, boolean enabled, Set<ActionMeta> checkedActions) {
    checkedActions.add(from);
    workflowMeta.getWorkflowHops().stream()
        .filter(hop -> from.equals(hop.getFromAction()))
        .forEach(
            hop -> {
              if (hop.isEnabled() != enabled) {
                WorkflowHopMeta before = (WorkflowHopMeta) hop.clone();
                hop.setEnabled(enabled);
                WorkflowHopMeta after = (WorkflowHopMeta) hop.clone();
                hopGui.undoDelegate.addUndoChange(
                    workflowMeta,
                    new WorkflowHopMeta[] {before},
                    new WorkflowHopMeta[] {after},
                    new int[] {workflowMeta.indexOfWorkflowHop(hop)});
              }
              if (!checkedActions.contains(hop.getToAction())) {
                enableDisableNextHops(hop.getToAction(), enabled, checkedActions);
              }
            });
    return checkedActions;
  }

  private void modalMessageDialog(String title, String message, int swtFlags) {
    MessageBox messageBox = new MessageBox(hopShell(), swtFlags);
    messageBox.setMessage(message);
    messageBox.setText(title);
    messageBox.open();
  }

  protected void setToolTip(int x, int y, int screenX, int screenY) {
    if (!hopGui.getProps().showToolTips()) {
      return;
    }

    canvas.setToolTipText(null);

    Image tipImage = null;
    WorkflowHopMeta hi = findWorkflowHop(x, y);

    // check the area owner list...
    //
    StringBuilder tip = new StringBuilder();
    AreaOwner areaOwner = getVisibleAreaOwner(x, y);
    if (areaOwner != null && areaOwner.getAreaType() != null) {
      ActionMeta actionCopy;
      switch (areaOwner.getAreaType()) {
        case WORKFLOW_HOP_ICON:
          hi = (WorkflowHopMeta) areaOwner.getOwner();
          if (hi.isUnconditional()) {
            tipImage = GuiResource.getInstance().getImageUnconditionalHop();
            tip.append(
                BaseMessages.getString(
                    PKG,
                    "WorkflowGraph.Hop.Tooltip.Unconditional",
                    hi.getFromAction().getName(),
                    Const.CR));
          } else {
            if (hi.getEvaluation()) {
              tip.append(
                  BaseMessages.getString(
                      PKG,
                      "WorkflowGraph.Hop.Tooltip.EvaluatingTrue",
                      hi.getFromAction().getName(),
                      Const.CR));
              tipImage = GuiResource.getInstance().getImageTrue();
            } else {
              tip.append(
                  BaseMessages.getString(
                      PKG,
                      "WorkflowGraph.Hop.Tooltip.EvaluatingFalse",
                      hi.getFromAction().getName(),
                      Const.CR));
              tipImage = GuiResource.getInstance().getImageFalse();
            }
          }
          break;

        case WORKFLOW_HOP_PARALLEL_ICON:
          hi = (WorkflowHopMeta) areaOwner.getOwner();
          tip.append(
              BaseMessages.getString(
                  PKG,
                  "WorkflowGraph.Hop.Tooltip.Parallel",
                  hi.getFromAction().getName(),
                  Const.CR));
          tipImage = GuiResource.getInstance().getImageParallelHop();
          break;

        case CUSTOM:
          String message = (String) areaOwner.getOwner();
          tip.append(message);
          tipImage = null;
          GuiResource.getInstance().getImagePipeline();
          break;

        case ACTION_RESULT_FAILURE:
        case ACTION_RESULT_SUCCESS:
          ActionResult actionResult = (ActionResult) areaOwner.getOwner();
          actionCopy = (ActionMeta) areaOwner.getParent();
          Result result = actionResult.getResult();
          tip.append("'").append(actionCopy.getName()).append("' ");
          if (result.getResult()) {
            tipImage = GuiResource.getInstance().getImageSuccess();
            tip.append("finished successfully.");
          } else {
            tipImage = GuiResource.getInstance().getImageFailure();
            tip.append("failed.");
          }
          tip.append(Const.CR).append("------------------------").append(Const.CR).append(Const.CR);
          tip.append("Result         : ").append(result.getResult()).append(Const.CR);
          tip.append("Errors         : ").append(result.getNrErrors()).append(Const.CR);

          if (result.getNrLinesRead() > 0) {
            tip.append("Lines read     : ").append(result.getNrLinesRead()).append(Const.CR);
          }
          if (result.getNrLinesWritten() > 0) {
            tip.append("Lines written  : ").append(result.getNrLinesWritten()).append(Const.CR);
          }
          if (result.getNrLinesInput() > 0) {
            tip.append("Lines input    : ").append(result.getNrLinesInput()).append(Const.CR);
          }
          if (result.getNrLinesOutput() > 0) {
            tip.append("Lines output   : ").append(result.getNrLinesOutput()).append(Const.CR);
          }
          if (result.getNrLinesUpdated() > 0) {
            tip.append("Lines updated  : ").append(result.getNrLinesUpdated()).append(Const.CR);
          }
          if (result.getNrLinesDeleted() > 0) {
            tip.append("Lines deleted  : ").append(result.getNrLinesDeleted()).append(Const.CR);
          }
          if (result.getNrLinesRejected() > 0) {
            tip.append("Lines rejected : ").append(result.getNrLinesRejected()).append(Const.CR);
          }
          if (result.getResultFiles() != null && !result.getResultFiles().isEmpty()) {
            tip.append(Const.CR).append("Result files:").append(Const.CR);
            if (result.getResultFiles().size() > 10) {
              tip.append(" (10 files of ").append(result.getResultFiles().size()).append(" shown");
            }
            List<ResultFile> files = new ArrayList<>(result.getResultFiles().values());
            for (int i = 0; i < files.size(); i++) {
              ResultFile file = files.get(i);
              tip.append("  - ").append(file.toString()).append(Const.CR);
            }
          }
          if (result.getRows() != null && !result.getRows().isEmpty()) {
            tip.append(Const.CR).append("Result rows: ");
            if (result.getRows().size() > 10) {
              tip.append(" (10 rows of ").append(result.getRows().size()).append(" shown");
            }
            tip.append(Const.CR);
            for (int i = 0; i < result.getRows().size() && i < 10; i++) {
              RowMetaAndData row = result.getRows().get(i);
              tip.append("  - ").append(row.toString()).append(Const.CR);
            }
          }
          break;

        case ACTION_RESULT_CHECKPOINT:
          tip.append(
              "The workflow started here since this is the furthest checkpoint "
                  + "that was reached last time the pipeline was executed.");
          tipImage = GuiResource.getInstance().getImageCheckpoint();
          break;
        case ACTION_ICON:
          ActionMeta jec = (ActionMeta) areaOwner.getOwner();
          if (jec.isDeprecated()) { // only need tooltip if action is deprecated
            tip.append(BaseMessages.getString(PKG, "WorkflowGraph.DeprecatedEntry.Tooltip.Title"))
                .append(Const.CR);
            String tipNext =
                BaseMessages.getString(
                    PKG, "WorkflowGraph.DeprecatedEntry.Tooltip.Message1", jec.getName());
            int length = tipNext.length() + 5;
            for (int i = 0; i < length; i++) {
              tip.append("-");
            }
            tip.append(Const.CR).append(tipNext).append(Const.CR);
            tip.append(
                BaseMessages.getString(PKG, "WorkflowGraph.DeprecatedEntry.Tooltip.Message2"));
            if (!Utils.isEmpty(jec.getSuggestion())
                && !(jec.getSuggestion().startsWith("!") && jec.getSuggestion().endsWith("!"))) {
              tip.append(" ");
              tip.append(
                  BaseMessages.getString(
                      PKG, "WorkflowGraph.DeprecatedEntry.Tooltip.Message3", jec.getSuggestion()));
            }
            tipImage = GuiResource.getInstance().getImageDeprecated();
          }
          break;
        default:
          // For plugins...
          //
          try {
            HopGuiTooltipExtension tooltipExt =
                new HopGuiTooltipExtension(x, y, screenX, screenY, areaOwner, tip);
            ExtensionPointHandler.callExtensionPoint(
                hopGui.getLog(),
                variables,
                HopExtensionPoint.HopGuiWorkflowGraphAreaHover.name(),
                tooltipExt);
            tipImage = tooltipExt.tooltipImage;
          } catch (Exception ex) {
            hopGui
                .getLog()
                .logError(
                    "Error calling extension point "
                        + HopExtensionPoint.HopGuiWorkflowGraphAreaHover.name(),
                    ex);
          }
          break;
      }
    }

    if (hi != null && tip.length() == 0) {
      // Set the tooltip for the hop:
      tip.append(BaseMessages.getString(PKG, "WorkflowGraph.Dialog.HopInfo")).append(Const.CR);
      tip.append(BaseMessages.getString(PKG, "WorkflowGraph.Dialog.HopInfo.SourceEntry"))
          .append(" ")
          .append(hi.getFromAction().getName())
          .append(Const.CR);
      tip.append(BaseMessages.getString(PKG, "WorkflowGraph.Dialog.HopInfo.TargetEntry"))
          .append(" ")
          .append(hi.getToAction().getName())
          .append(Const.CR);
      tip.append(BaseMessages.getString(PKG, "PipelineGraph.Dialog.HopInfo.Status")).append(" ");
      tip.append(
          (hi.isEnabled()
              ? BaseMessages.getString(PKG, "WorkflowGraph.Dialog.HopInfo.Enable")
              : BaseMessages.getString(PKG, "WorkflowGraph.Dialog.HopInfo.Disable")));
      if (hi.isUnconditional()) {
        tipImage = GuiResource.getInstance().getImageUnconditionalHop();
      } else {
        if (hi.getEvaluation()) {
          tipImage = GuiResource.getInstance().getImageTrue();
        } else {
          tipImage = GuiResource.getInstance().getImageFalse();
        }
      }
    }

    if (tip == null || tip.length() == 0) {
      toolTip.hide();
    } else {
      if (!tip.toString().equalsIgnoreCase(getToolTipText())) {
        if (tipImage != null) {
          toolTip.setImage(tipImage);
        } else {
          toolTip.setImage(GuiResource.getInstance().getImageHopUi());
        }
        toolTip.setText(tip.toString());
        toolTip.hide();
        toolTip.show(new org.eclipse.swt.graphics.Point(screenX, screenY));
      }
    }
  }

  public void launchStuff(ActionMeta actionCopy) {
    String[] references = actionCopy.getAction().getReferencedObjectDescriptions();
    if (!Utils.isEmpty(references)) {
      loadReferencedObject(actionCopy, 0);
    }
  }

  protected void loadReferencedObject(ActionMeta actionCopy, int index) {
    try {
      IHasFilename referencedMeta =
          actionCopy
              .getAction()
              .loadReferencedObject(index, hopGui.getMetadataProvider(), variables);
      if (referencedMeta == null) {
        return; // Sorry, nothing loaded
      }
      IHopFileType fileTypeHandler =
          hopGui.getPerspectiveManager().findFileTypeHandler(referencedMeta);
      fileTypeHandler.openFile(hopGui, referencedMeta.getFilename(), hopGui.getVariables());
    } catch (Exception e) {
      new ErrorDialog(hopShell(), "Error", "The referenced file couldn't be loaded", e);
    }
  }

  public synchronized void setWorkflow(IWorkflowEngine<WorkflowMeta> workflow) {
    this.workflow = workflow;
  }

  public void paintControl(PaintEvent e) {
    Point area = getArea();
    if (area.x == 0 || area.y == 0) {
      return; // nothing to do!
    }

    // Do double buffering to prevent flickering on Windows
    //
    boolean needsDoubleBuffering =
        Const.isWindows() && "GUI".equalsIgnoreCase(Const.getHopPlatformRuntime());

    Image image = null;
    GC swtGc = e.gc;

    if (needsDoubleBuffering) {
      image = new Image(hopDisplay(), area.x, area.y);
      swtGc = new GC(image);
    }

    try {
      drawWorkflowImage(swtGc, area.x, area.y, magnification);

      if (needsDoubleBuffering) {
        // Draw the image onto the canvas and get rid of the resources
        //
        e.gc.drawImage(image, 0, 0);
        swtGc.dispose();
        image.dispose();
      }

    } catch (Exception ex) {
      new ErrorDialog(hopGui.getShell(), "Error", "Error drawing workflow", ex);
    }
  }

  public void drawWorkflowImage(GC swtGc, int width, int height, float magnificationFactor)
      throws HopException {

    IGc gc = new SwtGc(swtGc, width, height, iconSize);
    try {
      PropsUi propsUi = PropsUi.getInstance();

      int gridSize = propsUi.isShowCanvasGridEnabled() ? propsUi.getCanvasGridSize() : 1;
      ScrollBar horizontalScrollBar = wsCanvas.getHorizontalBar();
      ScrollBar verticalScrollBar = wsCanvas.getVerticalBar();

      WorkflowPainter workflowPainter =
          new WorkflowPainter(
              gc,
              variables,
              workflowMeta,
              new Point(width, height),
              horizontalScrollBar==null ? null : new SwtScrollBar(horizontalScrollBar),
              verticalScrollBar==null ? null : new SwtScrollBar(verticalScrollBar),
              hopCandidate,
            selectionRegion,
              areaOwners,
              propsUi.getIconSize(),
              propsUi.getLineWidth(),
              gridSize,
              propsUi.getNoteFont().getName(),
              propsUi.getNoteFont().getHeight(),
              propsUi.getZoomFactor(),
              !propsUi.useDoubleClick());

      // correct the magnification with the overall zoom factor
      //
      float correctedMagnification = (float) (magnificationFactor * propsUi.getZoomFactor());

      workflowPainter.setMagnification(correctedMagnification);
      workflowPainter.setStartHopAction(startHopAction);
      workflowPainter.setEndHopLocation(endHopLocation);
      workflowPainter.setEndHopAction(endHopAction);
      workflowPainter.setNoInputAction(noInputAction);
      if (workflow != null) {
        workflowPainter.setActionResults(workflow.getActionResults());
      } else {
        workflowPainter.setActionResults(new ArrayList<>());
      }

      List<ActionMeta> activeActions = new ArrayList<>();
      if (workflow != null) {
        if (workflow.getActiveActionWorkflows().size() > 0) {
          activeActions.addAll(workflow.getActiveActionWorkflows().keySet());
        }
        if (workflow.getActiveActionPipeline().size() > 0) {
          activeActions.addAll(workflow.getActiveActionPipeline().keySet());
        }
      }
      workflowPainter.setActiveActions(activeActions);

      try {
        workflowPainter.drawWorkflow();

        if (workflowMeta.isEmpty()
            || (workflowMeta.nrNotes() == 0
                && workflowMeta.nrActions() == 1
                && workflowMeta.getAction(0).isStart())) {
          SvgFile svgFile =
              new SvgFile(
                  BasePropertyHandler.getProperty("WorkflowCanvas_image"),
                  getClass().getClassLoader());
          gc.drawImage(svgFile, 200, 200, 400, 124, gc.getMagnification() * 1.5f, 0);
        }
      } catch (HopException e) {
        throw new HopException("Error drawing workflow", e);
      }
    } finally {
      gc.dispose();
    }
  }

  protected Point getOffset() {
    Point area = getArea();
    Point max = workflowMeta.getMaximum();
    Point thumb = getThumb(area, max);
    return getOffset(thumb, area);
  }

  @Override
  public boolean hasChanged() {
    return workflowMeta.hasChanged();
  }

  protected void newHop() {
    List<ActionMeta> selection = workflowMeta.getSelectedActions();
    if (selection == null || selection.size() < 2) {
      return;
    }
    ActionMeta fr = selection.get(0);
    ActionMeta to = selection.get(1);
    workflowHopDelegate.newHop(workflowMeta, fr, to);
  }

  @GuiContextAction(
      id = "workflow-graph-action-10000-edit",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Edit the action",
      tooltip = "Edit the action properties",
      image = "ui/images/edit.svg",
      category = "Basic",
      categoryOrder = "1")
  public void editAction(HopGuiWorkflowActionContext context) {

    workflowActionDelegate.editAction(workflowMeta, context.getActionMeta());
  }

  public void editAction(ActionMeta je) {
    workflowActionDelegate.editAction(workflowMeta, je);
  }

  protected void editNote(NotePadMeta notePadMeta) {
    NotePadMeta before = notePadMeta.clone();
    String title = BaseMessages.getString(PKG, "WorkflowGraph.Dialog.EditNote.Title");

    NotePadDialog dd = new NotePadDialog(variables, hopShell(), title, notePadMeta);
    NotePadMeta n = dd.open();
    if (n != null) {
      notePadMeta.setChanged();
      notePadMeta.setNote(n.getNote());
      notePadMeta.setFontName(n.getFontName());
      notePadMeta.setFontSize(n.getFontSize());
      notePadMeta.setFontBold(n.isFontBold());
      notePadMeta.setFontItalic(n.isFontItalic());
      // font color
      notePadMeta.setFontColorRed(n.getFontColorRed());
      notePadMeta.setFontColorGreen(n.getFontColorGreen());
      notePadMeta.setFontColorBlue(n.getFontColorBlue());
      // background color
      notePadMeta.setBackGroundColorRed(n.getBackGroundColorRed());
      notePadMeta.setBackGroundColorGreen(n.getBackGroundColorGreen());
      notePadMeta.setBackGroundColorBlue(n.getBackGroundColorBlue());
      // border color
      notePadMeta.setBorderColorRed(n.getBorderColorRed());
      notePadMeta.setBorderColorGreen(n.getBorderColorGreen());
      notePadMeta.setBorderColorBlue(n.getBorderColorBlue());

      hopGui.undoDelegate.addUndoChange(
          workflowMeta,
          new NotePadMeta[] {before},
          new NotePadMeta[] {notePadMeta},
          new int[] {workflowMeta.indexOfNote(notePadMeta)});
      notePadMeta.width = ConstUi.NOTE_MIN_SIZE;
      notePadMeta.height = ConstUi.NOTE_MIN_SIZE;

      updateGui();
    }
  }

  protected void drawArrow(GC gc, int[] line) {
    int mx, my;
    int x1 = line[0] + offset.x;
    int y1 = line[1] + offset.y;
    int x2 = line[2] + offset.x;
    int y2 = line[3] + offset.y;
    int x3;
    int y3;
    int x4;
    int y4;
    int a, b, dist;
    double factor;
    double angle;

    // gc.setLineWidth(1);
    // WuLine(gc, black, x1, y1, x2, y2);

    gc.drawLine(x1, y1, x2, y2);

    // What's the distance between the 2 points?
    a = Math.abs(x2 - x1);
    b = Math.abs(y2 - y1);
    dist = (int) Math.sqrt(a * a + b * b);

    // determine factor (position of arrow to left side or right side 0-->100%)
    if (dist >= 2 * iconSize) {
      factor = 1.5;
    } else {
      factor = 1.2;
    }

    // in between 2 points
    mx = (int) (x1 + factor * (x2 - x1) / 2);
    my = (int) (y1 + factor * (y2 - y1) / 2);

    // calculate points for arrowhead
    angle = Math.atan2(y2 - y1, x2 - x1) + Math.PI;

    x3 = (int) (mx + Math.cos(angle - theta) * size);
    y3 = (int) (my + Math.sin(angle - theta) * size);

    x4 = (int) (mx + Math.cos(angle + theta) * size);
    y4 = (int) (my + Math.sin(angle + theta) * size);

    // draw arrowhead
    // gc.drawLine(mx, my, x3, y3);
    // gc.drawLine(mx, my, x4, y4);
    // gc.drawLine( x3, y3, x4, y4 );
    Color fore = gc.getForeground();
    Color back = gc.getBackground();
    gc.setBackground(fore);
    gc.fillPolygon(new int[] {mx, my, x3, y3, x4, y4});
    gc.setBackground(back);
  }

  protected boolean pointOnLine(int x, int y, int[] line) {
    int dx, dy;
    int pm = HOP_SEL_MARGIN / 2;
    boolean retval = false;

    for (dx = -pm; dx <= pm && !retval; dx++) {
      for (dy = -pm; dy <= pm && !retval; dy++) {
        retval = pointOnThinLine(x + dx, y + dy, line);
      }
    }

    return retval;
  }

  protected boolean pointOnThinLine(int x, int y, int[] line) {
    int x1 = line[0];
    int y1 = line[1];
    int x2 = line[2];
    int y2 = line[3];

    // Not in the square formed by these 2 points: ignore!
    // CHECKSTYLE:LineLength:OFF
    if (!(((x >= x1 && x <= x2) || (x >= x2 && x <= x1))
        && ((y >= y1 && y <= y2) || (y >= y2 && y <= y1)))) {
      return false;
    }

    double angle_line = Math.atan2(y2 - y1, x2 - x1) + Math.PI;
    double angle_point = Math.atan2(y - y1, x - x1) + Math.PI;

    // Same angle, or close enough?
    if (angle_point >= angle_line - 0.01 && angle_point <= angle_line + 0.01) {
      return true;
    }

    return false;
  }

  protected SnapAllignDistribute createSnapAllignDistribute() {

    List<ActionMeta> elements = workflowMeta.getSelectedActions();
    int[] indices = workflowMeta.getActionIndexes(elements);
    return new SnapAllignDistribute(workflowMeta, elements, indices, hopGui.undoDelegate, this);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_SNAP_TO_GRID,
      // label = "Snap to grid",
      toolTip = "i18n::WorkflowGraph.Toolbar.SnapToGrid.Tooltip",
      image = "ui/images/snap-to-grid.svg",
      disabledImage = "ui/images/snap-to-grid-disabled.svg")
  public void snapToGrid() {
    snapToGrid(ConstUi.GRID_SIZE);
  }

  protected void snapToGrid(int size) {
    createSnapAllignDistribute().snapToGrid(size);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ALIGN_LEFT,
      toolTip = "i18n::WorkflowGraph.Toolbar.AlignLeft.Tooltip",
      image = "ui/images/align-left.svg",
      disabledImage = "ui/images/align-left-disabled.svg")
  @GuiKeyboardShortcut(control = true, key = SWT.ARROW_LEFT)
  @GuiOsxKeyboardShortcut(command = true, key = SWT.ARROW_LEFT)
  public void alignLeft() {
    createSnapAllignDistribute().allignleft();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ALIGN_RIGHT,
      toolTip = "i18n::WorkflowGraph.Toolbar.AlignRight.Tooltip",
      image = "ui/images/align-right.svg",
      disabledImage = "ui/images/align-right-disabled.svg")
  @GuiKeyboardShortcut(control = true, key = SWT.ARROW_RIGHT)
  @GuiOsxKeyboardShortcut(command = true, key = SWT.ARROW_RIGHT)
  public void alignRight() {
    createSnapAllignDistribute().allignright();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ALIGN_TOP,
      toolTip = "i18n::WorkflowGraph.Toolbar.AlignTop.Tooltip",
      image = "ui/images/align-top.svg",
      disabledImage = "ui/images/align-top-disabled.svg")
  @GuiKeyboardShortcut(control = true, key = SWT.ARROW_UP)
  @GuiOsxKeyboardShortcut(command = true, key = SWT.ARROW_UP)
  public void alignTop() {
    createSnapAllignDistribute().alligntop();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ALIGN_BOTTOM,
      toolTip = "i18n::WorkflowGraph.Toolbar.AlignBottom.Tooltip",
      image = "ui/images/align-bottom.svg",
      disabledImage = "ui/images/align-bottom-disabled.svg")
  @GuiKeyboardShortcut(control = true, key = SWT.ARROW_DOWN)
  @GuiOsxKeyboardShortcut(command = true, key = SWT.ARROW_DOWN)
  public void alignBottom() {
    createSnapAllignDistribute().allignbottom();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DISTRIBUTE_HORIZONTALLY,
      toolTip = "i18n::WorkflowGraph.Toolbar.DistributeHorizontal.Tooltip",
      image = "ui/images/distribute-horizontally.svg",
      disabledImage = "ui/images/distribute-horizontally-disabled.svg")
  @GuiKeyboardShortcut(alt = true, key = SWT.ARROW_RIGHT)
  @GuiOsxKeyboardShortcut(alt = true, key = SWT.ARROW_RIGHT)
  public void distributeHorizontal() {
    createSnapAllignDistribute().distributehorizontal();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DISTRIBUTE_VERTICALLY,
      toolTip = "i18n::WorkflowGraph.Toolbar.DistributeVertical.Tooltip",
      image = "ui/images/distribute-vertically.svg",
      disabledImage = "ui/images/distribute-vertically-disabled.svg")
  @GuiKeyboardShortcut(alt = true, key = SWT.ARROW_UP)
  @GuiOsxKeyboardShortcut(alt = true, key = SWT.ARROW_UP)
  public void distributeVertical() {
    createSnapAllignDistribute().distributevertical();
  }

  @GuiContextAction(
      id = "workflow-graph-action-10100-action-detach",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "Detach action",
      tooltip = "Remove hops to and from this action",
      image = "ui/images/HOP_delete.svg",
      category = "Basic",
      categoryOrder = "1")
  public void detachAction(HopGuiWorkflowActionContext context) {
    ActionMeta actionMeta = context.getActionMeta();
    WorkflowHopMeta fromHop = workflowMeta.findWorkflowHopTo(actionMeta);
    WorkflowHopMeta toHop = workflowMeta.findWorkflowHopFrom(actionMeta);

    for (int i = workflowMeta.nrWorkflowHops() - 1; i >= 0; i--) {
      WorkflowHopMeta hop = workflowMeta.getWorkflowHop(i);
      if (actionMeta.equals(hop.getFromAction()) || actionMeta.equals(hop.getToAction())) {
        // Action is connected with a hop, remove this hop.
        //
        hopGui.undoDelegate.addUndoNew(workflowMeta, new WorkflowHopMeta[] {hop}, new int[] {i});
        workflowMeta.removeWorkflowHop(i);
      }
    }

    // If the transform was part of a chain, re-connect it.
    //
    if (fromHop != null && toHop != null) {
      workflowHopDelegate.newHop(
          workflowMeta, new WorkflowHopMeta(fromHop.getFromAction(), toHop.getToAction()));
    }

    updateGui();
  }

  @GuiContextAction(
      id = "pipeline-graph-transform-10010-copy-transform-to-clipboard",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Custom,
      name = "Copy Action to clipboard",
      tooltip = "Copy the XML representation of this Action to the clipboard",
      image = "ui/images/copy.svg",
      category = "Basic",
      categoryOrder = "1")
  public void copyActionToClipboard(HopGuiWorkflowActionContext context) {
    workflowClipboardDelegate.copySelected(
        workflowMeta, Arrays.asList(context.getActionMeta()), workflowMeta.getSelectedNotes());
  }

  public void newProps() {
    iconSize = hopGui.getProps().getIconSize();
    lineWidth = hopGui.getProps().getLineWidth();
  }

  public String toString() {
    if (workflowMeta == null) {
      return HopGui.APP_NAME;
    } else {
      return workflowMeta.getName();
    }
  }

  public IEngineMeta getMeta() {
    return workflowMeta;
  }

  /**
   * @param workflowMeta the workflowMeta to set
   * @return the workflowMeta / public WorkflowMeta getWorkflowMeta() { return workflowMeta; }
   *     <p>/**
   */
  public void setWorkflowMeta(WorkflowMeta workflowMeta) {
    this.workflowMeta = workflowMeta;
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_UNDO_ID,
      // label = "Undo",
      toolTip = "i18n:org.apache.hop.ui.hopgui:HopGui.Toolbar.Undo.Tooltip",
      image = "ui/images/undo.svg",
      disabledImage = "ui/images/undo-disabled.svg",
      separator = true)
  @GuiKeyboardShortcut(control = true, key = 'z')
  @Override
  public void undo() {
    workflowUndoDelegate.undoWorkflowAction(this, workflowMeta);
    forceFocus();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REDO_ID,
      // label = "Redo",
      toolTip = "i18n:org.apache.hop.ui.hopgui:HopGui.Toolbar.Redo.Tooltip",
      image = "ui/images/redo.svg",
      disabledImage = "ui/images/redo-disabled.svg")
  @GuiKeyboardShortcut(control = true, shift = true, key = 'z')
  @Override
  public void redo() {
    workflowUndoDelegate.redoWorkflowAction(this, workflowMeta);
    forceFocus();
  }

  public boolean isRunning() {
    if (workflow == null) {
      return false;
    }
    if (workflow.isFinished()) {
      return false;
    }
    if (workflow.isStopped()) {
      return false;
    }
    if (workflow.isActive()) {
      return true;
    }
    if (workflow.isInitialized()) {
      return true;
    }
    return false;
  }

  /**
   * Update the representation, toolbar, menus and so on. This is needed after a file, context or
   * capabilities changes
   */
  @Override
  public void updateGui() {

    if (hopGui == null || toolBarWidgets == null || toolBar == null || toolBar.isDisposed()) {
      return;
    }

    hopDisplay()
        .asyncExec(
            () -> {
              setZoomLabel();

              // Enable/disable the undo/redo toolbar buttons...
              //
              toolBarWidgets.enableToolbarItem(
                  TOOLBAR_ITEM_UNDO_ID, workflowMeta.viewThisUndo() != null);
              toolBarWidgets.enableToolbarItem(
                  TOOLBAR_ITEM_REDO_ID, workflowMeta.viewNextUndo() != null);

              // Enable/disable the align/distribute toolbar buttons
              //
              boolean selectedAction = !workflowMeta.getSelectedActions().isEmpty();
              toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_SNAP_TO_GRID, selectedAction);

              boolean selectedActions = workflowMeta.getSelectedActions().size() > 1;
              toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_ALIGN_LEFT, selectedActions);
              toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_ALIGN_RIGHT, selectedActions);
              toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_ALIGN_TOP, selectedActions);
              toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_ALIGN_BOTTOM, selectedActions);
              toolBarWidgets.enableToolbarItem(
                  TOOLBAR_ITEM_DISTRIBUTE_HORIZONTALLY, selectedActions);
              toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DISTRIBUTE_VERTICALLY, selectedActions);

              boolean running = isRunning() && !workflow.isStopped();
              toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_START, !running);
              toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_STOP, running);

              hopGui.setUndoMenu(workflowMeta);
              hopGui.handleFileCapabilities(fileType, running, false);
              HopGuiWorkflowGraph.super.redraw();
            });
  }

  public boolean canBeClosed() {
    return !workflowMeta.hasChanged();
  }

  public WorkflowMeta getManagedObject() {
    return workflowMeta;
  }

  public boolean hasContentChanged() {
    return workflowMeta.hasChanged();
  }

  public static int showChangedWarning(Shell shell, String name) {
    MessageBox mb = new MessageBox(shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_WARNING);
    mb.setMessage(BaseMessages.getString(PKG, "WorkflowGraph.Dialog.PromptSave.Message", name));
    mb.setText(BaseMessages.getString(PKG, "WorkflowGraph.Dialog.PromptSave.Title"));
    return mb.open();
  }

  public boolean editProperties(
      WorkflowMeta workflowMeta, HopGui hopGui, boolean allowDirectoryChange) {
    if (workflowMeta == null) {
      return false;
    }

    WorkflowDialog jd = new WorkflowDialog(hopGui.getShell(), SWT.NONE, variables, workflowMeta);
    if (jd.open() != null) {
      // If we added properties, add them to the variables too, so that they appear in the
      // CTRL-SPACE variable completion.
      //
      hopGui.setParametersAsVariablesInUI(workflowMeta, variables);

      updateGui();
      perspective.updateTabs();
      return true;
    }
    return false;
  }

  @Override
  public void save() throws HopException {
    try {
      ExtensionPointHandler.callExtensionPoint(
          log, variables, HopExtensionPoint.WorkflowBeforeSave.id, workflowMeta);

      if (StringUtils.isEmpty(workflowMeta.getFilename())) {
        throw new HopException("No filename: please specify a filename for this workflow");
      }

      // Keep track of save
      //
      AuditManager.registerEvent(
          HopNamespace.getNamespace(), "file", workflowMeta.getFilename(), "save");

      String xml = workflowMeta.getXml();
      OutputStream out = HopVfs.getOutputStream(workflowMeta.getFilename(), false);
      try {
        out.write(XmlHandler.getXmlHeader(Const.XML_ENCODING).getBytes(Const.XML_ENCODING));
        out.write(xml.getBytes(Const.XML_ENCODING));
        workflowMeta.clearChanged();
        updateGui();
        HopGui.getDataOrchestrationPerspective().updateTabs();
      } finally {
        out.flush();
        out.close();

        ExtensionPointHandler.callExtensionPoint(
            log, variables, HopExtensionPoint.WorkflowAfterSave.id, workflowMeta);
      }
    } catch (Exception e) {
      throw new HopException(
          "Error saving workflow to file '" + workflowMeta.getFilename() + "'", e);
    }
  }

  @Override
  public void saveAs(String filename) throws HopException {
    try {

      // Enforce file extension
      if (!filename.toLowerCase().endsWith(this.getFileType().getDefaultFileExtension())) {
        filename = filename + this.getFileType().getDefaultFileExtension();
      }

      FileObject fileObject = HopVfs.getFileObject(filename);
      if (fileObject.exists()) {
        MessageBox box = new MessageBox(hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
        box.setText("Overwrite?");
        box.setMessage("Are you sure you want to overwrite file '" + filename + "'?");
        int answer = box.open();
        if ((answer & SWT.YES) == 0) {
          return;
        }
      }

      workflowMeta.setFilename(filename);
      save();
    } catch (Exception e) {
      new HopException("Error validating file existence for '" + filename + "'", e);
    }
  }

  /** @return the lastMove */
  public Point getLastMove() {
    return lastMove;
  }

  /** @param lastMove the lastMove to set */
  public void setLastMove(Point lastMove) {
    this.lastMove = lastMove;
  }

  /** Add an extra view to the main composite SashForm */
  public void addExtraView() {
    extraViewComposite = new Composite(sashForm, SWT.NONE);
    FormLayout extraCompositeFormLayout = new FormLayout();
    extraCompositeFormLayout.marginWidth = 2;
    extraCompositeFormLayout.marginHeight = 2;
    extraViewComposite.setLayout(extraCompositeFormLayout);

    // Put a close and max button to the upper right corner...
    //
    closeButton = new Label(extraViewComposite, SWT.NONE);
    closeButton.setImage(GuiResource.getInstance().getImageClosePanel());
    closeButton.setToolTipText(
        BaseMessages.getString(PKG, "WorkflowGraph.ExecutionResultsPanel.CloseButton.Tooltip"));
    FormData fdClose = new FormData();
    fdClose.right = new FormAttachment(100, 0);
    fdClose.top = new FormAttachment(0, 0);
    closeButton.setLayoutData(fdClose);
    closeButton.addMouseListener(
        new MouseAdapter() {
          public void mouseDown(MouseEvent e) {
            disposeExtraView();
          }
        });

    minMaxButton = new Label(extraViewComposite, SWT.NONE);
    minMaxButton.setImage(GuiResource.getInstance().getImageMaximizePanel());
    minMaxButton.setToolTipText(
        BaseMessages.getString(PKG, "WorkflowGraph.ExecutionResultsPanel.MaxButton.Tooltip"));
    FormData fdMinMax = new FormData();
    fdMinMax.right = new FormAttachment(closeButton, -props.getMargin());
    fdMinMax.top = new FormAttachment(0, 0);
    minMaxButton.setLayoutData(fdMinMax);
    minMaxButton.addMouseListener(
        new MouseAdapter() {
          public void mouseDown(MouseEvent e) {
            minMaxExtraView();
          }
        });

    // Add a label at the top: Results
    //
    Label wResultsLabel = new Label(extraViewComposite, SWT.LEFT);
    wResultsLabel.setFont(GuiResource.getInstance().getFontMediumBold());
    wResultsLabel.setBackground(GuiResource.getInstance().getColorWhite());
    wResultsLabel.setText(BaseMessages.getString(PKG, "WorkflowLog.ResultsPanel.NameLabel"));
    FormData fdResultsLabel = new FormData();
    fdResultsLabel.left = new FormAttachment(0, 0);
    fdResultsLabel.right = new FormAttachment(100, 0);
    fdResultsLabel.top = new FormAttachment(0, 0);
    wResultsLabel.setLayoutData(fdResultsLabel);

    // Add a tab folder ...
    //
    extraViewTabFolder = new CTabFolder(extraViewComposite, SWT.MULTI);
    hopGui.getProps().setLook(extraViewTabFolder, Props.WIDGET_STYLE_TAB);

    extraViewTabFolder.addMouseListener(
        new MouseAdapter() {

          @Override
          public void mouseDoubleClick(MouseEvent arg0) {
            if (sashForm.getMaximizedControl() == null) {
              sashForm.setMaximizedControl(extraViewComposite);
            } else {
              sashForm.setMaximizedControl(null);
            }
          }
        });

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.top = new FormAttachment(wResultsLabel, props.getMargin());
    fdTabFolder.bottom = new FormAttachment(100, 0);
    extraViewTabFolder.setLayoutData(fdTabFolder);

    sashForm.setWeights(
        new int[] {
          60, 40,
        });
  }

  /** If the extra tab view at the bottom is empty, we close it. */
  public void checkEmptyExtraView() {
    if (extraViewTabFolder.getItemCount() == 0) {
      disposeExtraView();
    }
  }

  private void disposeExtraView() {
    extraViewComposite.dispose();
    sashForm.layout();
    sashForm.setWeights(
        new int[] {
          100,
        });

    ToolItem item = toolBarWidgets.findToolItem(TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS);
    item.setToolTipText(BaseMessages.getString(PKG, "HopGui.Tooltip.ShowExecutionResults"));
    item.setImage(GuiResource.getInstance().getImageShowResults());
  }

  private void minMaxExtraView() {
    // What is the state?
    //
    boolean maximized = sashForm.getMaximizedControl() != null;
    if (maximized) {
      // Minimize
      //
      sashForm.setMaximizedControl(null);
      minMaxButton.setImage(GuiResource.getInstance().getImageMaximizePanel());
      minMaxButton.setToolTipText(
          BaseMessages.getString(PKG, "WorkflowGraph.ExecutionResultsPanel.MaxButton.Tooltip"));
    } else {
      // Maximize
      //
      sashForm.setMaximizedControl(extraViewComposite);
      minMaxButton.setImage(GuiResource.getInstance().getImageMinimizePanel());
      minMaxButton.setToolTipText(
          BaseMessages.getString(PKG, "WorkflowGraph.ExecutionResultsPanel.MinButton.Tooltip"));
    }
  }

  public boolean isExecutionResultsPaneVisible() {
    return extraViewComposite != null && !extraViewComposite.isDisposed();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS,
      // label = "HopGui.Menu.ShowExecutionResults",
      toolTip = "i18n:org.apache.hop.ui.hopgui:HopGui.Tooltip.ShowExecutionResults",
      image = "ui/images/show-results.svg",
      separator = true)
  public void showExecutionResults() {
    if (isExecutionResultsPaneVisible()) {
      disposeExtraView();
    } else {
      addAllTabs();
    }
  }

  public void addAllTabs() {

    CTabItem tabItemSelection = null;
    if (extraViewTabFolder != null && !extraViewTabFolder.isDisposed()) {
      tabItemSelection = extraViewTabFolder.getSelection();
    }

    workflowLogDelegate.addJobLog();
    workflowGridDelegate.addWorkflowGrid();

    if (tabItemSelection != null) {
      extraViewTabFolder.setSelection(tabItemSelection);
    } else {
      extraViewTabFolder.setSelection(workflowGridDelegate.getWorkflowGridTab());
    }

    ToolItem toolItem = toolBarWidgets.findToolItem(TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS);
    toolItem.setToolTipText(BaseMessages.getString(PKG, "HopGui.Tooltip.HideExecutionResults"));
    toolItem.setImage(GuiResource.getInstance().getImageHideResults());
  }

  /* TODO: re-introduce
  public void getSql() {
    hopGui.getSql();
  }
    */

  /* TODO: re-introduce
  public void exploreDatabase() {
     hopGui.exploreDatabase();
   }
     */

  public void close() {
    hopGui.menuFileClose();
  }

  @Override
  public boolean isCloseable() {
    try {
      // Check if the file is saved. If not, ask for it to be saved.
      //
      if (workflowMeta.hasChanged()) {

        MessageBox messageDialog =
            new MessageBox(hopShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO | SWT.CANCEL);
        messageDialog.setText("Save file?");
        messageDialog.setMessage(
            "Do you want to save file '" + buildTabName() + "' before closing?");
        int answer = messageDialog.open();
        if ((answer & SWT.YES) != 0) {
          if (StringUtils.isEmpty(this.getFilename())) {
            // Ask for the filename
            //
            String filename =
                BaseDialog.presentFileDialog(
                    true,
                    hopGui.getShell(),
                    fileType.getFilterExtensions(),
                    fileType.getFilterNames(),
                    true);
            if (filename == null) {
              return false;
            }

            filename = hopGui.getVariables().resolve(filename);
            saveAs(filename);
          } else {
            save();
          }
          return true;
        }
        if ((answer & SWT.NO) != 0) {
          // User doesn't want to save but close
          return true;
        }
        return false;
      } else {
        return true;
      }
    } catch (Exception e) {
      new ErrorDialog(hopShell(), "Error", "Error preparing file close", e);
    }
    return false;
  }

  public String buildTabName() throws HopException {
    String tabName = null;
    String realFilename = variables.resolve(workflowMeta.getFilename());
    if (StringUtils.isEmpty(realFilename)) {
      tabName = workflowMeta.getName();
    } else {
      try {
        FileObject fileObject = HopVfs.getFileObject(workflowMeta.getFilename());
        FileName fileName = fileObject.getName();
        tabName = fileName.getBaseName();
      } catch (Exception e) {
        throw new HopException(
            "Unable to get information from file name '" + workflowMeta.getFilename() + "'", e);
      }
    }
    return tabName;
  }

  public synchronized void start(WorkflowExecutionConfiguration executionConfiguration)
      throws HopException {

    // If filename set & not changed ?
    //
    if (handleWorkflowMetaChanges(workflowMeta)) {

      // If the workflow is not running, start the workflow...
      //
      if (!isRunning()) {
        try {

          // Make sure we clear the log before executing again...
          //
          if (executionConfiguration.isClearingLog()) {
            workflowLogDelegate.clearLog();
          }

          // Also make sure to clear the old log actions in the central log
          // store & registry
          //
          if (workflow != null) {
            HopLogStore.discardLines(workflow.getLogChannelId(), true);
          }

          WorkflowMeta runWorkflowMeta;

          runWorkflowMeta =
              new WorkflowMeta(
                  hopGui.getVariables(),
                  workflowMeta.getFilename(),
                  workflowMeta.getMetadataProvider());

          String hopGuiObjectId = UUID.randomUUID().toString();
          SimpleLoggingObject hopGuiLoggingObject =
              new SimpleLoggingObject("HOPGUI", LoggingObjectType.HOP_GUI, null);
          hopGuiLoggingObject.setContainerObjectId(hopGuiObjectId);
          hopGuiLoggingObject.setLogLevel(executionConfiguration.getLogLevel());

          // Set the log level
          //
          if (executionConfiguration.getLogLevel() != null) {
            workflowMeta.setLogLevel(executionConfiguration.getLogLevel());
          }

          // Set the start transform name
          //
          if (executionConfiguration.getStartActionName() != null) {
            workflowMeta.setStartActionName(executionConfiguration.getStartActionName());
          }

          // Set the run options
          //
          workflowMeta.setClearingLog(executionConfiguration.isClearingLog());

          // Allow plugins to change the workflow metadata
          //
          ExtensionPointHandler.callExtensionPoint(
              log, variables, HopExtensionPoint.HopGuiWorkflowMetaExecutionStart.id, workflowMeta);

          workflow =
              WorkflowEngineFactory.createWorkflowEngine(
                  variables,
                  executionConfiguration.getRunConfiguration(),
                  hopGui.getMetadataProvider(),
                  runWorkflowMeta,
                  hopGuiLoggingObject);

          workflow.setLogLevel(executionConfiguration.getLogLevel());
          workflow.setInteractive(true);
          workflow.setGatheringMetrics(executionConfiguration.isGatheringMetrics());

          // Set the variables that where specified...
          //
          for (String varName : executionConfiguration.getVariablesMap().keySet()) {
            String varValue = executionConfiguration.getVariablesMap().get(varName);
            if (StringUtils.isNotEmpty(varValue)) {
              workflow.setVariable(varName, varValue);
            }
          }

          // Set and activate the parameters...
          //
          for (String paramName : executionConfiguration.getParametersMap().keySet()) {
            String paramValue = executionConfiguration.getParametersMap().get(paramName);
            workflow.setParameterValue(paramName, paramValue);
          }
          workflow.activateParameters(workflow);

          // Pass specific extension points...
          //
          workflow.getExtensionDataMap().putAll(executionConfiguration.getExtensionOptions());

          // Add action listeners
          //
          workflow.addActionListener(createRefreshActionListener());

          // If there is an alternative start action, pass it to the workflow
          //
          if (!Utils.isEmpty(executionConfiguration.getStartActionName())) {
            ActionMeta startActionMeta =
                runWorkflowMeta.findAction(executionConfiguration.getStartActionName());
            workflow.setStartActionMeta(startActionMeta);
          }

          // Set the named parameters
          Map<String, String> paramMap = executionConfiguration.getParametersMap();
          Set<String> keys = paramMap.keySet();
          for (String key : keys) {
            workflow.setParameterValue(key, Const.NVL(paramMap.get(key), ""));
          }
          workflow.activateParameters(workflow);

          try {
            ExtensionPointHandler.callExtensionPoint(
                LogChannel.UI, variables, HopExtensionPoint.HopGuiWorkflowBeforeStart.id, workflow);
          } catch (HopException e) {
            LogChannel.UI.logError(e.getMessage(), workflowMeta.getFilename());
            return;
          }

          log.logBasic(BaseMessages.getString(PKG, "WorkflowLog.Log.StartingWorkflow"));
          workflowThread = new Thread(() -> workflow.startExecution());
          workflowThread.start();
          workflowGridDelegate.previousNrItems = -1;
          // Link to the new workflowTracker!
          workflowGridDelegate.workflowTracker = workflow.getWorkflowTracker();

          updateGui();

          // Attach a listener to notify us that the pipeline has finished.
          //
          workflow.addWorkflowFinishedListener(workflow -> HopGuiWorkflowGraph.this.jobFinished());

          // Show the execution results views
          //
          addAllTabs();
        } catch (HopException e) {
          new ErrorDialog(
              hopShell(),
              BaseMessages.getString(PKG, "WorkflowLog.Dialog.CanNotOpenWorkflow.Title"),
              BaseMessages.getString(PKG, "WorkflowLog.Dialog.CanNotOpenWorkflow.Message"),
              e);
          workflow = null;
        }
      } else {
        MessageBox m = new MessageBox(hopShell(), SWT.OK | SWT.ICON_WARNING);
        m.setText(BaseMessages.getString(PKG, "WorkflowLog.Dialog.WorkflowIsAlreadyRunning.Title"));
        m.setMessage(
            BaseMessages.getString(PKG, "WorkflowLog.Dialog.WorkflowIsAlreadyRunning.Message"));
        m.open();
      }
    } else {
      showSaveFileMessage();
    }
  }

  public void showSaveFileMessage() {
    MessageBox m = new MessageBox(hopShell(), SWT.OK | SWT.ICON_WARNING);
    m.setText(BaseMessages.getString(PKG, "WorkflowLog.Dialog.WorkflowHasChangedSave.Title"));
    m.setMessage(BaseMessages.getString(PKG, "WorkflowLog.Dialog.WorkflowHasChangedSave.Message"));
    m.open();
  }

  private IActionListener createRefreshActionListener() {
    return new IActionListener<WorkflowMeta>() {

      public void beforeExecution(
          IWorkflowEngine<WorkflowMeta> workflow, ActionMeta actionCopy, IAction action) {
        asyncRedraw();
      }

      public void afterExecution(
          IWorkflowEngine<WorkflowMeta> workflow,
          ActionMeta actionCopy,
          IAction action,
          Result result) {
        asyncRedraw();
      }
    };
  }

  /** This gets called at the very end, when everything is done. */
  protected void jobFinished() {
    // Do a final check to see if it all ended...
    //
    if (workflow != null && workflow.isInitialized() && workflow.isFinished()) {
      log.logBasic(BaseMessages.getString(PKG, "WorkflowLog.Log.WorkflowHasEnded"));
    }
    updateGui();
  }

  public IHasLogChannel getLogChannelProvider() {
    return () ->
        getWorkflow() != null ? getWorkflow().getLogChannel() : getWorkflowMeta().getLogChannel();
  }

  // Change of transform, connection, hop or note...
  public void addUndoPosition(Object[] obj, int[] pos, Point[] prev, Point[] curr) {
    addUndoPosition(obj, pos, prev, curr, false);
  }

  // Change of transform, connection, hop or note...
  public void addUndoPosition(
      Object[] obj, int[] pos, Point[] prev, Point[] curr, boolean nextAlso) {
    // It's better to store the indexes of the objects, not the objects itself!
    workflowMeta.addUndo(obj, null, pos, prev, curr, PipelineMeta.TYPE_UNDO_POSITION, nextAlso);
    hopGui.setUndoMenu(workflowMeta);
  }

  /**
   * Handle if workflow filename is set and changed saved
   *
   * <p>Prompt auto save feature...
   *
   * @param workflowMeta
   * @return true if workflow meta has name and if changed is saved
   * @throws HopException
   */
  public boolean handleWorkflowMetaChanges(WorkflowMeta workflowMeta) throws HopException {
    if (workflowMeta.hasChanged()) {
      if (StringUtils.isNotEmpty(workflowMeta.getFilename()) && hopGui.getProps().getAutoSave()) {
        if (log.isDetailed()) {
          log.logDetailed(BaseMessages.getString(PKG, "WorkflowLog.Log.AutoSaveFileBeforeRunning"));
        }
        save();
      } else {
        MessageDialogWithToggle md =
            new MessageDialogWithToggle(
                hopShell(),
                BaseMessages.getString(PKG, "WorkflowLog.Dialog.SaveChangedFile.Title"),
                BaseMessages.getString(PKG, "WorkflowLog.Dialog.SaveChangedFile.Message")
                    + Const.CR
                    + BaseMessages.getString(PKG, "WorkflowLog.Dialog.SaveChangedFile.Message2")
                    + Const.CR,
                SWT.ICON_QUESTION,
                new String[] {
                  BaseMessages.getString(PKG, "System.Button.Yes"),
                  BaseMessages.getString(PKG, "System.Button.No")
                },
                BaseMessages.getString(PKG, "WorkflowLog.Dialog.SaveChangedFile.Toggle"),
                hopGui.getProps().getAutoSave());
        int answer = md.open();

        if (answer == 0) { // Yes button
          String filename = workflowMeta.getFilename();
          if (StringUtils.isEmpty(filename)) {
            // Ask for the filename: saveAs
            //
            filename =
                BaseDialog.presentFileDialog(
                    true,
                    hopGui.getShell(),
                    fileType.getFilterExtensions(),
                    fileType.getFilterNames(),
                    true);
            if (filename != null) {
              filename = hopGui.getVariables().resolve(filename);
              saveAs(filename);
            }
          } else {
            save();
          }
        }
        hopGui.getProps().setAutoSave(md.getToggleState());
      }
    }

    return StringUtils.isNotEmpty(workflowMeta.getFilename()) && !workflowMeta.hasChanged();
  }

  private ActionMeta lastChained = null;

  public void addActionToChain(String typeDesc, boolean shift) {

    // Is the lastChained action still valid?
    //
    if (lastChained != null
        && workflowMeta.findAction(lastChained.getName()) == null) {
      lastChained = null;
    }

    // If there is exactly one selected transform, pick that one as last chained.
    //
    List<ActionMeta> sel = workflowMeta.getSelectedActions();
    if (sel.size() == 1) {
      lastChained = sel.get(0);
    }

    // Where do we add this?

    Point p = null;
    if (lastChained == null) {
      p = workflowMeta.getMaximum();
      p.x -= 100;
    } else {
      p = new Point(lastChained.getLocation().x, lastChained.getLocation().y);
    }

    p.x += 200;

    // Which is the new action?

    ActionMeta newEntry = workflowActionDelegate.newAction(workflowMeta, null, typeDesc, false, p);
    if (newEntry == null) {
      return;
    }
    newEntry.setLocation(p.x, p.y);

    if (lastChained != null) {
      workflowHopDelegate.newHop(workflowMeta, lastChained, newEntry);
    }

    lastChained = newEntry;
    adjustScrolling();
    updateGui();

    if (shift) {
      editAction(newEntry);
    }

    workflowMeta.unselectAll();
    newEntry.setSelected(true);
    updateGui();
  }

  @Override
  public Object getSubject() {
    return workflowMeta;
  }

  public WorkflowMeta getWorkflowMeta() {
    return workflowMeta;
  }

  public IWorkflowEngine<WorkflowMeta> getWorkflow() {
    return workflow;
  }

  @Override
  public ILogChannel getLogChannel() {
    return log;
  }

  // TODO
  public void editAction(WorkflowMeta workflowMeta, ActionMeta actionCopy) {}

  @Override
  public String getName() {
    return workflowMeta.getName();
  }

  @Override
  public void setName(String name) {
    workflowMeta.setName(name);
  }

  @Override
  public String getFilename() {
    return workflowMeta.getFilename();
  }

  @Override
  public void setFilename(String filename) {
    workflowMeta.setFilename(filename);
  }

  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopGui() {
    return hopGui;
  }

  /**
   * Gets perspective
   *
   * @return value of perspective
   */
  public HopDataOrchestrationPerspective getPerspective() {
    return perspective;
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /**
   * Gets log
   *
   * @return value of log
   */
  public ILogChannel getLog() {
    return log;
  }

  /** @param log The log to set */
  public void setLog(ILogChannel log) {
    this.log = log;
  }

  /**
   * Gets props
   *
   * @return value of props
   */
  public PropsUi getProps() {
    return props;
  }

  /** @param props The props to set */
  public void setProps(PropsUi props) {
    this.props = props;
  }

  /** @param hopGui The hopGui to set */
  public void setHopGui(HopGui hopGui) {
    this.hopGui = hopGui;
  }

  /**
   * Gets fileType
   *
   * @return value of fileType
   */
  @Override
  public HopWorkflowFileType<WorkflowMeta> getFileType() {
    return fileType;
  }

  /** @param fileType The fileType to set */
  public void setFileType(HopWorkflowFileType<WorkflowMeta> fileType) {
    this.fileType = fileType;
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return null;
  }

  /**
   * Gets workflowThread
   *
   * @return value of workflowThread
   */
  public Thread getWorkflowThread() {
    return workflowThread;
  }
}
