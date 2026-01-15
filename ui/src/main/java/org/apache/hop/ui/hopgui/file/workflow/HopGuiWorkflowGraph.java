/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.file.workflow;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.base.AbstractMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.IEngineMeta;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.Props;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.action.GuiContextActionFilter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.IRedrawable;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.gui.SnapAllignDistribute;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.core.gui.plugin.IGuiRefresher;
import org.apache.hop.core.gui.plugin.action.GuiAction;
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
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.util.ExecutorUtil;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.IExecutionInfoLocation;
import org.apache.hop.history.AuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.laf.BasePropertyHandler;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.pipeline.PipelinePainter;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ContextDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.CanvasFacade;
import org.apache.hop.ui.hopgui.CanvasListener;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiExtensionPoint;
import org.apache.hop.ui.hopgui.ServerPushSessionFacade;
import org.apache.hop.ui.hopgui.context.GuiContextUtil;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.dialog.NotePadDialog;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.delegates.HopGuiNotePadDelegate;
import org.apache.hop.ui.hopgui.file.shared.HopGuiAbstractGraph;
import org.apache.hop.ui.hopgui.file.shared.HopGuiTooltipExtension;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowActionContext;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowContext;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowHopContext;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowNoteContext;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowActionDelegate;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowCheckDelegate;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowClipboardDelegate;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowGridDelegate;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowHopDelegate;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowLogDelegate;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowRunDelegate;
import org.apache.hop.ui.hopgui.file.workflow.delegates.HopGuiWorkflowUndoDelegate;
import org.apache.hop.ui.hopgui.file.workflow.extension.HopGuiWorkflowGraphExtension;
import org.apache.hop.ui.hopgui.perspective.execution.ExecutionPerspective;
import org.apache.hop.ui.hopgui.perspective.execution.IExecutionViewer;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.selection.HopGuiSelectionTracker;
import org.apache.hop.ui.hopgui.shared.SwtGc;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.apache.hop.ui.util.HelpUtils;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.IActionListener;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.WorkflowPainter;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.MouseListener;
import org.eclipse.swt.events.MouseMoveListener;
import org.eclipse.swt.events.MouseTrackListener;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.ToolTip;

/** Handles the display of Workflows in HopGui, in a graphical form. */
@GuiPlugin(name = "i18n::WorkflowGraph.Name", description = "Workflow Graph GUI plugin")
@SuppressWarnings("java:S1104")
public class HopGuiWorkflowGraph extends HopGuiAbstractGraph
    implements IRedrawable,
        MouseListener,
        MouseMoveListener,
        MouseTrackListener,
        IHasLogChannel,
        ILogParentProvided,
        IHopFileTypeHandler,
        IGuiRefresher {

  private static final Class<?> PKG = HopGuiWorkflowGraph.class;

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiWorkflowGraph-Toolbar";
  public static final String TOOLBAR_ITEM_START = "HopGuiWorkflowGraph-ToolBar-10010-Run";
  public static final String TOOLBAR_ITEM_STOP = "HopGuiWorkflowGraph-ToolBar-10030-Stop";
  public static final String TOOLBAR_ITEM_CHECK = "HopGuiWorkflowGraph-ToolBar-10040-Check";
  public static final String TOOLBAR_ITEM_UNDO_ID = "HopGuiWorkflowGraph-ToolBar-10100-Undo";
  public static final String TOOLBAR_ITEM_REDO_ID = "HopGuiWorkflowGraph-ToolBar-10110-Redo";

  public static final String TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS =
      "HopGuiWorkflowGraph-ToolBar-10400-Execution-Results";

  public static final String TOOLBAR_ITEM_ZOOM_LEVEL =
      "HopGuiWorkflowGraph-ToolBar-10500-Zoom-Level";

  public static final String TOOLBAR_ITEM_ZOOM_OUT = "HopGuiWorkflowGraph-ToolBar-10510-Zoom-Out";

  public static final String TOOLBAR_ITEM_ZOOM_IN = "HopGuiWorkflowGraph-ToolBar-10520-Zoom-In";

  public static final String TOOLBAR_ITEM_ZOOM_100PCT =
      "HopGuiWorkflowGraph-ToolBar-10530-Zoom-100Pct";

  public static final String TOOLBAR_ITEM_ZOOM_TO_FIT =
      "HopGuiWorkflowGraph-ToolBar-10530-Zoom-To-Fit";

  public static final String TOOLBAR_ITEM_EDIT_WORKFLOW =
      "HopGuiWorkflowGraph-ToolBar-10450-EditWorkflow";

  public static final String TOOLBAR_ITEM_TO_EXECUTION_INFO =
      "HopGuiWorkflowGraph-ToolBar-10475-ToExecutionInfo";

  private static final String STRING_PARALLEL_WARNING_PARAMETER = "ParallelActionsWarning";

  private static final int HOP_SEL_MARGIN = 9;

  public static final String ACTION_ID_WORKFLOW_GRAPH_HOP_ENABLE =
      "workflow-graph-hop-10010-hop-enable";
  public static final String ACTION_ID_WORKFLOW_GRAPH_HOP_DISABLE =
      "workflow-graph-hop-10000-hop-disable";
  public static final String ACTION_ID_WORKFLOW_GRAPH_HOP_HOP_UNCONDITIONAL =
      "workflow-graph-hop-10030-hop-unconditional";
  public static final String ACTION_ID_WORKFLOW_GRAPH_HOP_HOP_EVALUATION_SUCCESS =
      "workflow-graph-hop-10040-hop-evaluation-success";
  public static final String ACTION_ID_WORKFLOW_GRAPH_HOP_HOP_EVALUATION_FAILURE =
      "workflow-graph-hop-10050-hop-evaluation-failure";
  public static final String ACTION_ID_WORKFLOW_GRAPH_ACTION_VIEW_EXECUTION_INFO =
      "workflow-graph-action-11000-view-execution-info";
  public static final String CONST_ERROR = "Error";
  public static final String CONST_WORKFLOW_GRAPH_DIALOG_LOOP_AFTER_HOP_ENABLED_MESSAGE =
      "WorkflowGraph.Dialog.LoopAfterHopEnabled.Message";
  public static final String CONST_WORKFLOW_GRAPH_DIALOG_LOOP_AFTER_HOP_ENABLED_TITLE =
      "WorkflowGraph.Dialog.LoopAfterHopEnabled.Title";
  public static final String START_HOP_NODE = "startHopNode";

  @Getter private final ExplorerPerspective perspective;

  @Setter @Getter protected ILogChannel log;

  @Getter protected WorkflowMeta workflowMeta;

  @Getter protected IWorkflowEngine<WorkflowMeta> workflow;

  @Getter protected Thread workflowThread;

  @Setter @Getter protected PropsUi props;

  protected int iconSize;

  protected int lineWidth;

  protected Point lastClick;

  protected List<ActionMeta> selectedActions;

  protected ActionMeta selectedAction;

  private List<NotePadMeta> selectedNotes;
  protected NotePadMeta selectedNote;

  @Getter @Setter protected Point lastMove;

  protected WorkflowHopMeta hopCandidate;

  @Getter @Setter protected HopGui hopGui;

  protected boolean splitHop;

  // Keep track if a contextual dialog box is open, do not display the tooltip
  private boolean openedContextDialog = false;

  protected int lastButton;

  protected WorkflowHopMeta lastHopSplit;

  protected org.apache.hop.core.gui.Rectangle selectionRegion;

  protected static final double THETA = Math.toRadians(10); // arrowhead sharpness

  protected static final int SIZE = 30; // arrowhead length

  protected int currentMouseX = 0;

  protected int currentMouseY = 0;

  private NotePadMeta currentNotePad = null;

  private SashForm sashForm;

  public CTabFolder extraViewTabFolder;

  private ToolBar toolBar;
  @Getter private GuiToolbarWidgets toolBarWidgets;

  private boolean halting;

  public HopGuiWorkflowLogDelegate workflowLogDelegate;
  public HopGuiWorkflowGridDelegate workflowGridDelegate;
  public HopGuiWorkflowCheckDelegate workflowCheckDelegate;
  public HopGuiWorkflowClipboardDelegate workflowClipboardDelegate;
  public HopGuiWorkflowRunDelegate workflowRunDelegate;
  public HopGuiWorkflowUndoDelegate workflowUndoDelegate;
  public HopGuiWorkflowActionDelegate workflowActionDelegate;
  public HopGuiWorkflowHopDelegate workflowHopDelegate;
  public HopGuiNotePadDelegate notePadDelegate;

  private Composite mainComposite;

  private ToolItem rotateItem;
  private ToolItem minMaxItem;

  private List<AreaOwner> areaOwners;

  @Setter private HopWorkflowFileType<WorkflowMeta> fileType;

  private ActionMeta startHopAction;
  private Point endHopLocation;

  private ActionMeta endHopAction;
  private ActionMeta forbiddenAction;
  private Point[] previousActionLocations;
  private Point[] previousNoteLocations;
  private ActionMeta currentAction;
  private boolean ignoreNextClick;
  private boolean doubleClick;
  private boolean dragSelection;
  private WorkflowHopMeta clickedWorkflowHop;

  private Timer redrawTimer;

  public HopGuiWorkflowGraph(
      Composite parent,
      final HopGui hopGui,
      final ExplorerPerspective perspective,
      final WorkflowMeta workflowMeta,
      final HopWorkflowFileType<WorkflowMeta> fileType) {
    super(hopGui, parent, SWT.NO_BACKGROUND);
    this.perspective = perspective;
    this.workflowMeta = workflowMeta;
    this.fileType = fileType;
    this.log = hopGui.getLog();
    this.hopGui = hopGui;

    this.props = PropsUi.getInstance();
    this.areaOwners = new ArrayList<>();

    // Adjust the internal variables
    //
    workflowMeta.setInternalHopVariables(variables);

    workflowLogDelegate = new HopGuiWorkflowLogDelegate(hopGui, this);
    workflowGridDelegate = new HopGuiWorkflowGridDelegate(hopGui, this);
    workflowCheckDelegate = new HopGuiWorkflowCheckDelegate(hopGui, this);
    workflowClipboardDelegate = new HopGuiWorkflowClipboardDelegate(hopGui, this);
    workflowRunDelegate = new HopGuiWorkflowRunDelegate(hopGui, this);
    workflowUndoDelegate = new HopGuiWorkflowUndoDelegate(hopGui, this);
    workflowActionDelegate = new HopGuiWorkflowActionDelegate(hopGui, this);
    workflowHopDelegate = new HopGuiWorkflowHopDelegate(hopGui, this);
    notePadDelegate = new HopGuiNotePadDelegate(hopGui, this);

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
    sashForm =
        new SashForm(
            mainComposite,
            PropsUi.getInstance().isGraphExtraViewVerticalOrientation()
                ? SWT.VERTICAL
                : SWT.HORIZONTAL);

    // Add a canvas below it, use up all space
    //
    canvas = new Canvas(sashForm, SWT.NO_BACKGROUND | SWT.BORDER);
    Listener listener = CanvasListener.getInstance();
    canvas.addListener(SWT.MouseDown, listener);
    canvas.addListener(SWT.MouseMove, listener);
    canvas.addListener(SWT.MouseUp, listener);
    canvas.addListener(SWT.Paint, listener);
    FormData fdCanvas = new FormData();
    fdCanvas.left = new FormAttachment(0, 0);
    fdCanvas.top = new FormAttachment(0, 0);
    fdCanvas.right = new FormAttachment(100, 0);
    fdCanvas.bottom = new FormAttachment(100, 0);
    canvas.setLayoutData(fdCanvas);

    sashForm.setWeights(100);

    toolTip = new ToolTip(getShell(), SWT.BALLOON);
    toolTip.setAutoHide(true);

    newProps();

    selectionRegion = null;
    hopCandidate = null;
    lastHopSplit = null;

    selectedActions = null;
    selectedNote = null;

    setVisible(true);

    // Set canvas background to match application background for web
    if (EnvironmentUtils.getInstance().isWeb()) {
      canvas.setBackground(GuiResource.getInstance().getColorBackground());
    }

    canvas.addPaintListener(this::paintControl);

    selectedActions = null;
    lastClick = null;

    canvas.addMouseListener(this);
    if (!EnvironmentUtils.getInstance().isWeb()) {
      canvas.addMouseMoveListener(this);
      canvas.addMouseTrackListener(this);
      canvas.addMouseWheelListener(this::mouseScrolled);
    }

    hopGui.replaceKeyboardShortcutListeners(this);
    setBackground(GuiResource.getInstance().getColorBackground());

    canvas.pack();

    updateGui();
  }

  public static HopGuiWorkflowGraph getInstance() {
    return HopGui.getActiveWorkflowGraph();
  }

  @Override
  public void dispose() {
    disposeExtraView();
    super.dispose();
  }

  protected void hideToolTips() {
    toolTip.setVisible(false);
  }

  @Override
  public void mouseDoubleClick(MouseEvent event) {

    if (!PropsUi.getInstance().useDoubleClick()) {
      return;
    }

    doubleClick = true;
    clearSettings();

    Point real = screen2real(event.x, event.y);

    // Hide the tooltip!
    hideToolTips();

    AreaOwner areaOwner = getVisibleAreaOwner(real.x, real.y);

    try {
      HopGuiWorkflowGraphExtension ext =
          new HopGuiWorkflowGraphExtension(this, event, real, areaOwner);
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.WorkflowGraphMouseDoubleClick.id, ext);
      if (ext.isPreventingDefault()) {
        return;
      }
    } catch (Exception ex) {
      LogChannel.GENERAL.logError(
          "Error calling WorkflowGraphMouseDoubleClick extension point", ex);
    }

    ActionMeta action = workflowMeta.getAction(real.x, real.y, iconSize);
    if (action != null) {
      if (event.button == 1) {
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

  @Override
  public void mouseDown(MouseEvent event) {
    if (EnvironmentUtils.getInstance().isWeb()) {
      // RAP does not support certain mouse events.
      mouseHover(event);
    }
    doubleClick = false;
    resize = null;

    if (ignoreNextClick) {
      ignoreNextClick = false;
      return;
    }

    boolean control = (event.stateMask & SWT.MOD1) != 0;
    boolean shift = (event.stateMask & SWT.SHIFT) != 0;

    Point real = screen2real(event.x, event.y);
    lastClick = new Point(real.x, real.y);
    lastButton = event.button;
    dragSelection = false;

    // Hide the tooltip!
    hideToolTips();

    AreaOwner areaOwner = getVisibleAreaOwner(real.x, real.y);

    try {
      HopGuiWorkflowGraphExtension ext =
          new HopGuiWorkflowGraphExtension(this, event, real, areaOwner);
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.WorkflowGraphMouseDown.id, ext);
      if (ext.isPreventingDefault()) {
        return;
      }
    } catch (Exception ex) {
      LogChannel.GENERAL.logError("Error calling WorkflowGraphMouseDown extension point", ex);
    }

    // Layer 0: See if we're dragging around the view-port over the workflow graph.
    //
    Point clickScreen = new Point(event.x, event.y);
    if (setupDragViewPort(clickScreen)) {
      return;
    }

    // A single left or middle click on one of the area owners...
    //
    boolean done = false;

    // Layer 1: Click on an area owner except note (else if a note is present in the background,
    // you cannot click the hop link).
    if (areaOwner != null && areaOwner.getAreaType() != null) {
      switch (areaOwner.getAreaType()) {
        case ACTION_ICON:
          if (shift && control) {
            openReferencedObject();
            return;
          }

          currentAction = (ActionMeta) areaOwner.getOwner();

          if (startHopAction != null) {
            // If we click on the start hop action or a forbidden action, then we don't have a
            // candidate hop, but we need to ignore this click to not start a drag operation
            if (hopCandidate != null) {
              addCandidateAsHop();
            }
          } else if (event.button == 2 || (event.button == 1 && shift)) {
            // SHIFT CLICK is start of drag to create a new hop
            //
            canvas.setData("mode", "hop");
            canvas.setData(START_HOP_NODE, currentAction.getName());
            startHopAction = currentAction;
          } else {
            canvas.setData("mode", "drag");
            dragSelection = true;
            selectedActions = workflowMeta.getSelectedActions();
            selectedAction = currentAction;
            //
            // When an icon is moved that is not selected, it gets
            // selected too late.
            // It is not captured here, but in the mouseMoveListener...
            //
            previousActionLocations = workflowMeta.getSelectedLocations();

            Point p = currentAction.getLocation();
            iconOffset = new Point(real.x - p.x, real.y - p.y);
          }
          updateGui();
          done = true;
          break;

        case ACTION_NAME:
          done = true;
          break;

        case ACTION_INFO_ICON:
          // Click on the info icon means: Edit action description
          //
          editActionDescription((ActionMeta) areaOwner.getOwner());
          done = true;
          break;

          // If you click on an evaluating icon, change the evaluation...
          //
        case WORKFLOW_HOP_ICON:
          WorkflowHopMeta hop = (WorkflowHopMeta) areaOwner.getOwner();
          WorkflowHopMeta originalHop = hop.clone();
          if (hop.getFromAction().isEvaluation()) {
            if (hop.isUnconditional()) {
              hop.setUnconditional(false);
              hop.setEvaluation(true);
            } else {
              if (hop.isEvaluation()) {
                hop.setEvaluation(false);
              } else {
                hop.setUnconditional(true);
              }
            }
            hopGui.undoDelegate.addUndoChange(
                workflowMeta,
                new WorkflowHopMeta[] {originalHop},
                new WorkflowHopMeta[] {hop},
                new int[] {workflowMeta.indexOfWorkflowHop(hop)});
            updateGui();
          }
          done = true;
          break;
        case NOTE:
        default:
          break;
      }
    }

    // Layer 2: click on hop links between actions
    if (!done && startHopAction == null) {
      // Hop links between actions are found searching by (x,y) coordinates.
      WorkflowHopMeta hop = findWorkflowHop(real.x, real.y);
      if (hop != null) {
        // Delete hop with on click
        if (event.button == 1 && shift && control) {
          // Delete the hop
          workflowHopDelegate.delHop(workflowMeta, hop);
          updateGui();
        }
        // User held control and clicked a hop between actions - We want to flip the active state
        // of
        // the hop.
        //
        else if (event.button == 2 || (event.button == 1 && control)) {
          hop.setEnabled(!hop.isEnabled());
          updateGui();
        } else {
          // A hop: show the hop context menu in the mouseUp() listener
          //
          clickedWorkflowHop = hop;
        }
        done = true;
      }
    }

    // Layer 3: click on a note
    if (!done
        && startHopAction == null
        && areaOwner != null
        && areaOwner.getAreaType() == AreaOwner.AreaType.NOTE) {
      currentNotePad = (NotePadMeta) areaOwner.getOwner();
      selectedNotes = workflowMeta.getSelectedNotes();
      selectedNote = currentNotePad;
      // Track that a note was selected
      HopGuiSelectionTracker.getInstance()
          .setLastSelectionType(HopGuiSelectionTracker.SelectionType.WORKFLOW_GRAPH);
      Point loc = currentNotePad.getLocation();

      previousNoteLocations = workflowMeta.getSelectedNoteLocations();

      noteOffset = new Point(real.x - loc.x, real.y - loc.y);

      canvas.setData("mode", "resize");
      resize = this.getResize(areaOwner.getArea(), real);

      if (resize != null) {
        // Keep the original area of the resizing note
        resizeArea =
            new Rectangle(
                currentNotePad.getLocation().x,
                currentNotePad.getLocation().y,
                currentNotePad.getWidth(),
                currentNotePad.getHeight());
      } else {
        dragSelection = true;
      }

      updateGui();
      done = true;
    }

    // Layer 4: Click on the background of the graph
    if (!done) {

      // If we're dragging a candidate hop around and click on the background it simply needs to
      // go away.
      //
      if (startHopAction != null) {
        canvas.setData("mode", "null");
        canvas.setData(START_HOP_NODE, null);
        startHopAction = null;
        hopCandidate = null;
        endHopLocation = null;
        lastClick = null;
        redraw();
        return;
      }

      // Click to drag the background
      if (setupDragView(event.button, control, clickScreen)) {
        return;
      }

      // Click to create a lasso
      //
      canvas.setData("mode", "select");
      if (!control && event.button == 1) {
        selectionRegion = new org.apache.hop.core.gui.Rectangle(real.x, real.y, 0, 0);
        // Change cursor when selecting a region
        setCursor(getDisplay().getSystemCursor(SWT.CURSOR_CROSS));
      }
      updateGui();
    }

    if (EnvironmentUtils.getInstance().isWeb()) {
      // RAP does not support certain mouse events.
      mouseMove(event);
    }
  }

  @SuppressWarnings("java:S115")
  private enum SingleClickType {
    Workflow,
    Action,
    Note,
    Hop,
  }

  @Override
  public void mouseUp(MouseEvent event) {
    resize = null;
    dragSelection = false;
    forbiddenAction = null;

    // Only clear mode if we're not in the middle of creating a hop
    // Otherwise shift+click won't work (mode gets cleared before we can select the target)
    if (startHopAction == null && endHopAction == null) {
      canvas.setData("mode", "null");
      canvas.setData(START_HOP_NODE, null);
    }
    if (EnvironmentUtils.getInstance().isWeb()) {
      // RAP does not support certain mouse events.
      mouseMove(event);
    }

    // Default cursor
    setCursor(null);

    if (viewPortNavigation || viewDrag) {
      viewDrag = false;
      viewDragStart = null;
      viewPortNavigation = false;
      viewPortStart = null;
      return;
    }

    boolean control = (event.stateMask & SWT.MOD1) != 0;

    boolean singleClick = false;
    SingleClickType singleClickType = null;
    ActionMeta singleClickAction = null;
    NotePadMeta singleClickNote = null;
    WorkflowHopMeta singleClickHop = null;
    mouseOverName = null;

    if (iconOffset == null) {
      iconOffset = new Point(0, 0);
    }
    Point real = screen2real(event.x, event.y);
    Point icon = new Point(real.x - iconOffset.x, real.y - iconOffset.y);
    AreaOwner areaOwner = getVisibleAreaOwner(real.x, real.y);

    try {
      HopGuiWorkflowGraphExtension ext =
          new HopGuiWorkflowGraphExtension(this, event, real, areaOwner);
      ExtensionPointHandler.callExtensionPoint(
          LogChannel.GENERAL, variables, HopExtensionPoint.WorkflowGraphMouseUp.id, ext);
      if (ext.isPreventingDefault()) {
        redraw();
        clearSettings();
        return;
      }
    } catch (Exception ex) {
      LogChannel.GENERAL.logError("Error calling WorkflowGraphMouseUp extension point", ex);
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
        selectionRegion = null;
        // Track that actions/notes were selected via region selection
        if (!workflowMeta.getSelectedActions().isEmpty()
            || !workflowMeta.getSelectedNotes().isEmpty()) {
          HopGuiSelectionTracker.getInstance()
              .setLastSelectionType(HopGuiSelectionTracker.SelectionType.WORKFLOW_GRAPH);
        }
        updateGui();
        return;
      }
    }

    // Quick new hop option? (drag from one action to another)
    //
    if (areaOwner != null && areaOwner.getAreaType() != null) {
      switch (areaOwner.getAreaType()) {
        case ACTION_ICON:
          if (startHopAction != null) {
            // Mouse up while drawing a hop candidate
            addCandidateAsHop();
            redraw();
          }
          break;
        case ACTION_NAME:
          if (startHopAction == null
              && selectionRegion == null
              && selectedActions == null
              && selectedNotes == null) {
            // This is available only in single click mode...
            ActionMeta actionMeta = (ActionMeta) areaOwner.getParent();
            editAction(actionMeta);
            return;
          }
          break;
        default:
          break;
      }
    }

    // Clicked on an icon?
    //
    if (selectedAction != null && startHopAction == null) {
      if (event.button == 1) {
        Point realclick = screen2real(event.x, event.y);
        if (lastClick.x == realclick.x && lastClick.y == realclick.y) {
          // Flip selection when control is pressed!
          if (control) {
            selectedAction.flipSelected();
          } else {
            singleClick = true;
            singleClickType = SingleClickType.Action;
            singleClickAction = selectedAction;

            // If the clicked action is not part of the current selection, cancel the
            // current selection
            if (!selectedAction.isSelected()) {
              workflowMeta.unselectAll();
              selectedAction.setSelected(true);
            }
            // Track that an action was selected
            HopGuiSelectionTracker.getInstance()
                .setLastSelectionType(HopGuiSelectionTracker.SelectionType.WORKFLOW_GRAPH);
          }
        } else {
          // Find out which Transforms & Notes are selected
          selectedActions = workflowMeta.getSelectedActions();
          selectedNotes = workflowMeta.getSelectedNotes();
          // Track that actions/notes were selected
          if (!selectedActions.isEmpty() || !selectedNotes.isEmpty()) {
            HopGuiSelectionTracker.getInstance()
                .setLastSelectionType(HopGuiSelectionTracker.SelectionType.WORKFLOW_GRAPH);
          }

          // We moved around some items: store undo info...
          //
          boolean also = false;
          if (!Utils.isEmpty(selectedNotes) && previousNoteLocations != null) {
            int[] indexes = workflowMeta.getNoteIndexes(selectedNotes);

            addUndoPosition(
                selectedNotes.toArray(new NotePadMeta[selectedNotes.size()]),
                indexes,
                previousNoteLocations,
                workflowMeta.getSelectedNoteLocations(),
                also);
            also = !Utils.isEmpty(selectedActions);
          }
          if (selectedActions != null
              && !selectedActions.isEmpty()
              && previousActionLocations != null) {
            int[] indexes = workflowMeta.getActionIndexes(selectedActions);
            addUndoPosition(
                selectedActions.toArray(new ActionMeta[selectedActions.size()]),
                indexes,
                previousActionLocations,
                workflowMeta.getSelectedLocations(),
                also);
          }
        }
      }

      // OK, we moved the action, did we move it across a hop?
      // If so, ask to split the hop!
      if (splitHop) {
        WorkflowHopMeta hop = findHop(icon.x + iconSize / 2, icon.y + iconSize / 2, selectedAction);
        if (hop != null) {
          int id = 0;
          if (!hopGui.getProps().getAutoSplit()) {
            MessageDialogWithToggle md =
                new MessageDialogWithToggle(
                    hopShell(),
                    BaseMessages.getString(PKG, "HopGuiWorkflowGraph.Dialog.SplitHop.Title"),
                    BaseMessages.getString(PKG, "HopGuiWorkflowGraph.Dialog.SplitHop.Message")
                        + Const.CR
                        + hop,
                    SWT.ICON_QUESTION,
                    new String[] {
                      BaseMessages.getString(PKG, "System.Button.Yes"),
                      BaseMessages.getString(PKG, "System.Button.No")
                    },
                    BaseMessages.getString(
                        PKG, "HopGuiWorkflowGraph.Dialog.Option.SplitHop.DoNotAskAgain"),
                    hopGui.getProps().getAutoSplit());
            id = md.open();
            hopGui.getProps().setAutoSplit(md.getToggleState());
          }

          if ((id & 0xFF) == 0) { // Means: "Yes" button clicked!
            workflowActionDelegate.insertAction(workflowMeta, hop, selectedAction);
          }
        }
        // Discard this hop-split attempt.
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
        if (event.button == 1) {
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

              // If the clicked note is not part of the current selection, cancel the
              // current selection
              if (!selectedNote.isSelected()) {
                workflowMeta.unselectAll();
                selectedNote.setSelected(true);
              }
              // Track that a note was selected
              HopGuiSelectionTracker.getInstance()
                  .setLastSelectionType(HopGuiSelectionTracker.SelectionType.WORKFLOW_GRAPH);
            }
          } else {
            // Find out which Transforms & Notes are selected
            selectedActions = workflowMeta.getSelectedActions();
            selectedNotes = workflowMeta.getSelectedNotes();
            // Track that actions/notes were selected
            if (!selectedActions.isEmpty() || !selectedNotes.isEmpty()) {
              HopGuiSelectionTracker.getInstance()
                  .setLastSelectionType(HopGuiSelectionTracker.SelectionType.WORKFLOW_GRAPH);
            }

            // We moved around some items: store undo info...
            boolean also = false;
            if (selectedNotes != null
                && !selectedNotes.isEmpty()
                && previousNoteLocations != null) {
              int[] indexes = workflowMeta.getNoteIndexes(selectedNotes);
              addUndoPosition(
                  selectedNotes.toArray(new NotePadMeta[selectedNotes.size()]),
                  indexes,
                  previousNoteLocations,
                  workflowMeta.getSelectedNoteLocations(),
                  also);
              also = !Utils.isEmpty(selectedActions);
            }
            if (selectedActions != null
                && !selectedActions.isEmpty()
                && previousActionLocations != null) {
              int[] indexes = workflowMeta.getActionIndexes(selectedActions);
              addUndoPosition(
                  selectedActions.toArray(new ActionMeta[selectedActions.size()]),
                  indexes,
                  previousActionLocations,
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

        updateGui();
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
      hopGui
          .getDisplay()
          .timerExec(
              hopGui.getDisplay().getDoubleClickTime(),
              () ->
                  showContextDialog(
                      event,
                      real,
                      fSingleClick,
                      fSingleClickType,
                      fSingleClickAction,
                      fSingleClickNote,
                      fSingleClickHop));
    } else {
      showContextDialog(
          event,
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
      MouseEvent event,
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
    if (fSingleClickType == SingleClickType.Workflow
        && (!workflowMeta.getSelectedActions().isEmpty()
            || !workflowMeta.getSelectedNotes().isEmpty())) {
      workflowMeta.unselectAll();
      updateGui();

      // Show a short tooltip
      //
      toolTip.setVisible(false);
      toolTip.setText(Const.CR + "  Selection cleared " + Const.CR);
      showToolTip(new org.eclipse.swt.graphics.Point(event.x, event.y));

      return;
    }

    // Just a single click on the background:
    // We have a bunch of possible actions for you...
    //
    if (fSingleClick && fSingleClickType != null && !doubleClick) {
      IGuiContextHandler contextHandler = null;
      String message = null;
      switch (fSingleClickType) {
        case Workflow:
          message =
              BaseMessages.getString(
                  PKG, "HopGuiWorkflowGraph.ContextualActionDialog.Workflow.Header");
          contextHandler = new HopGuiWorkflowContext(workflowMeta, this, real);
          break;
        case Action:
          message =
              BaseMessages.getString(
                  PKG,
                  "HopGuiWorkflowGraph.ContextualActionDialog.Action.Header",
                  fSingleClickAction.getName());
          contextHandler =
              new HopGuiWorkflowActionContext(workflowMeta, fSingleClickAction, this, real);
          break;
        case Note:
          message =
              BaseMessages.getString(PKG, "HopGuiWorkflowGraph.ContextualActionDialog.Note.Header");
          contextHandler =
              new HopGuiWorkflowNoteContext(workflowMeta, fSingleClickNote, this, real);
          break;
        case Hop:
          message =
              BaseMessages.getString(PKG, "HopGuiWorkflowGraph.ContextualActionDialog.Hop.Header");
          contextHandler = new HopGuiWorkflowHopContext(workflowMeta, fSingleClickHop, this, real);
          break;
        default:
          break;
      }
      if (contextHandler != null) {
        Shell parent = hopShell();
        org.eclipse.swt.graphics.Point p = parent.getDisplay().map(canvas, null, event.x, event.y);

        this.openedContextDialog = true;
        this.hideToolTips();

        // Show the context dialog
        //
        ignoreNextClick =
            GuiContextUtil.getInstance()
                .handleActionSelection(parent, message, new Point(p.x, p.y), contextHandler);

        this.openedContextDialog = false;
      }
    }
  }

  @Override
  public void mouseMove(MouseEvent event) {
    boolean shift = (event.stateMask & SWT.SHIFT) != 0;
    boolean doRedraw = false;

    // disable the tooltip
    //
    hideToolTips();

    // First, check for operations that have been started, such as move selection, dragging the
    // view, creating a hop or resizing a note.

    // Drag the view around with middle button on the background?
    //
    if (viewDrag && lastClick != null) {
      dragView(viewDragStart, new Point(event.x, event.y));
      return;
    }

    // Check to see if we're navigating with the view port
    //
    if (viewPortNavigation) {
      dragViewPort(new Point(event.x, event.y));
      return;
    }

    Point real = screen2real(event.x, event.y);

    // Remember the last position of the mouse for paste with keyboard
    //
    lastMove = real;

    // Resizing the current note
    if (resize != null) {
      resizeNote(selectedNote, real);
      return;
    }

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
    Resize resizeOver = null;

    // Mouse over an area only if no other operation is in progress
    if (areaOwner != null
        && this.startHopAction == null
        && this.selectionRegion == null
        && !dragSelection) {
      // Mouse over the name of the action
      //
      if (areaOwner.getAreaType() == AreaOwner.AreaType.ACTION_NAME) {
        if (!PropsUi.getInstance().useDoubleClick()) {
          if (mouseOverName == null) {
            doRedraw = true;
          }
          mouseOverName = (String) areaOwner.getOwner();
        }
      }
      // Mouse over note
      else if (areaOwner.getAreaType() == AreaOwner.AreaType.NOTE) {
        // Check if the mouse is over the border to activate a resize cursor
        resizeOver = this.getResize(areaOwner.getArea(), real);

        // Remove over name (note behind an action)
        if (mouseOverName != null) {
          mouseOverName = null;
          doRedraw = true;
        }
      }
    } else {
      if (mouseOverName != null) {
        mouseOverName = null;
        doRedraw = true;
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
      previousActionLocations = new Point[] {selectedAction.getLocation()};
      doRedraw = true;
    } else if (selectedNote != null && !selectedNote.isSelected()) {
      workflowMeta.unselectAll();
      selectedNote.setSelected(true);
      selectedNotes = new ArrayList<>();
      selectedNotes.add(selectedNote);
      previousNoteLocations = new Point[] {selectedNote.getLocation()};
      doRedraw = true;
    } else if (selectionRegion != null && startHopAction == null) {
      // Did we select a region...?
      //
      selectionRegion.width = real.x - selectionRegion.x;
      selectionRegion.height = real.y - selectionRegion.y;
      doRedraw = true;
    } else if (selectedAction != null && lastButton == 1 && !shift && startHopAction == null) {
      // Move around actions & notes
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

        // Check if we can split A-->--B and insert the selected transform C if
        // C-->--A or C-->--B or A-->--C or B-->--C don't exists...
        //
        if (workflowMeta.findWorkflowHop(selectedAction, hi.getFromAction()) == null
            && workflowMeta.findWorkflowHop(selectedAction, hi.getToAction()) == null
            && workflowMeta.findWorkflowHop(hi.getToAction(), selectedAction) == null
            && workflowMeta.findWorkflowHop(hi.getFromAction(), selectedAction) == null) {
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

      moveSelected(dx, dy);

      doRedraw = true;
    } else if ((startHopAction != null && endHopAction == null)
        || (endHopAction != null && startHopAction == null)) {

      // Are we creating a new hop with the middle button or pressing SHIFT?
      //
      ActionMeta actionMeta = workflowMeta.getAction(real.x, real.y, iconSize);
      endHopLocation = new Point(real.x, real.y);
      if (actionMeta != null) {

        // Checks if mouse over another action
        if ((startHopAction != null && !startHopAction.equals(actionMeta))
            || (endHopAction != null && !endHopAction.equals(actionMeta))) {

          if (startHopAction != null) {
            // Check if the start action
            if (actionMeta.isStart()) {
              forbiddenAction = actionMeta;
              toolTip.setText(
                  BaseMessages.getString(PKG, "WorkflowGraph.Hop.CreateHopToStartAction.Tooltip"));
              showToolTip(new org.eclipse.swt.graphics.Point(event.x, event.y));
            }
            // Check if the hop already exists
            else if (workflowMeta.findWorkflowHop(startHopAction, actionMeta, true) != null) {
              forbiddenAction = actionMeta;
              toolTip.setText(
                  BaseMessages.getString(PKG, "WorkflowGraph.Dialog.HopExists.Message"));
              showToolTip(new org.eclipse.swt.graphics.Point(event.x, event.y));
            }
            // Check if the hop candidate creates a loop
            else {
              hopCandidate = new WorkflowHopMeta(startHopAction, actionMeta);
              workflowMeta.addWorkflowHop(hopCandidate);
              boolean hasLoop = workflowMeta.hasLoop(actionMeta);
              workflowMeta.removeWorkflowHop(hopCandidate);
              if (hasLoop) {
                hopCandidate = null;
                forbiddenAction = actionMeta;
                toolTip.setText(
                    BaseMessages.getString(PKG, "WorkflowGraph.Dialog.HopCausesLoop.Message"));
                showToolTip(new org.eclipse.swt.graphics.Point(event.x, event.y));
              }
            }
          } else if (endHopAction != null) {
            hopCandidate = new WorkflowHopMeta(actionMeta, endHopAction);
            endHopLocation = null;
          }
        }
      } else {
        hopCandidate = null;
        forbiddenAction = null;
      }

      doRedraw = true;
    }

    // Move around notes & actions
    //
    if (selectedNote != null && lastButton == 1 && !shift) {
      /*
       * One or more notes are selected and moved around...
       *
       * new : new position of the note (not the mouse pointer) dx : difference with previous position
       */
      int dx = note.x - selectedNote.getLocation().x;
      int dy = note.y - selectedNote.getLocation().y;

      moveSelected(dx, dy);

      doRedraw = true;
    }

    //  If an operation is already in progress, do not change the cursor.
    if (this.startHopAction == null && this.selectionRegion == null && !dragSelection) {
      // Change the cursor when the mouse is on the resize edge of a note
      if (resizeOver != null) {
        setCursor(getDisplay().getSystemCursor(resizeOver.getCursor()));
      }
      // Change cursor when the mouse is on a hop or an area that support hovering
      else if ((areaOwner != null
              && areaOwner.getAreaType() != null
              && areaOwner.getAreaType().isSupportHover())
          || findWorkflowHop(real.x, real.y) != null) {
        setCursor(getDisplay().getSystemCursor(SWT.CURSOR_HAND));
      } else {
        // Reset cursor after mouse hover
        setCursor(null);
      }
    }

    if (doRedraw) {
      redraw();
    }
  }

  @Override
  public void mouseHover(MouseEvent event) {

    boolean tip = true;

    Point real = screen2real(event.x, event.y);

    // Show a tool tip upon mouse-over of an object on the canvas
    if (tip) {
      setToolTip(real.x, real.y, event.x, event.y);
    }
  }

  @Override
  public void mouseEnter(MouseEvent event) {
    // Do nothing
  }

  @Override
  public void mouseExit(MouseEvent event) {
    // Do nothing
  }

  private void addCandidateAsHop() {

    if (hopCandidate == null) {
      return;
    }

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
        ActionMeta actionMeta = workflowMeta.findNextAction(hopCandidate.getFromAction(), 0);
        WorkflowHopMeta other =
            workflowMeta.findWorkflowHop(hopCandidate.getFromAction(), actionMeta);
        if (other != null) {
          hopCandidate.setEvaluation(!other.isEvaluation());
        }
      }
    }

    // Stop drawing the hop candidate
    startHopAction = null;

    workflowHopDelegate.newHop(workflowMeta, hopCandidate);
    clearSettings();
  }

  public boolean checkIfHopAlreadyExists(WorkflowMeta workflowMeta, WorkflowHopMeta newHop) {

    return workflowMeta.findWorkflowHop(newHop.getFromAction(), newHop.getToAction(), true) == null;
  }

  /**
   * Find the last area owner, the one drawn last, for the given coordinate. In other words, this
   * searches the provided list back-to-front.
   *
   * @param x The x coordinate
   * @param y The y coordinate
   * @return The area owner or null if nothing could be found
   */
  public synchronized AreaOwner getVisibleAreaOwner(int x, int y) {
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
      toolTip = "i18n::HopGuiWorkflowGraph.GuiAction.ZoomInOut.Tooltip",
      type = GuiToolbarElementType.COMBO,
      alignRight = true,
      comboValuesMethod = "getZoomLevels")
  public void zoomLevel() {
    readMagnification();
    setFocus();
  }

  @Override
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ZOOM_IN,
      toolTip = "i18n::HopGuiWorkflowGraph.GuiAction.ZoomIn.Tooltip",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/zoom-in.svg")
  public void zoomIn() {
    super.zoomIn();
  }

  @Override
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ZOOM_OUT,
      toolTip = "i18n::HopGuiWorkflowGraph.GuiAction.ZoomOut.Tooltip",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/zoom-out.svg")
  public void zoomOut() {
    super.zoomOut();
  }

  @Override
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ZOOM_100PCT,
      toolTip = "i18n::HopGuiWorkflowGraph.GuiAction.Zoom100.Tooltip",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/zoom-100.svg")
  public void zoom100Percent() {
    super.zoom100Percent();
  }

  @Override
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_ZOOM_TO_FIT,
      toolTip = "i18n::HopGuiWorkflowGraph.GuiAction.ZoomFitToScreen.Tooltip",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/zoom-fit.svg")
  public void zoomFitToScreen() {
    super.zoomFitToScreen();
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
      PropsUi.setLook(toolBar, Props.WIDGET_STYLE_TOOLBAR);

      // enable / disable the icons in the toolbar too.
      //
      updateGui();

    } catch (Throwable t) {
      log.logError("Error setting up the navigation toolbar for HopUI", t);
      new ErrorDialog(
          hopShell(),
          CONST_ERROR,
          "Error setting up the navigation toolbar for HopGUI",
          new Exception(t));
    }
  }

  @Override
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
    ServerPushSessionFacade.start();

    Thread thread =
        new Thread(
            () ->
                getDisplay()
                    .asyncExec(
                        () -> {
                          try {
                            workflowRunDelegate.executeWorkflow(
                                hopGui.getVariables(), workflowMeta, null);
                            ServerPushSessionFacade.stop();
                          } catch (Exception e) {
                            stopRedrawTimer();
                            new ErrorDialog(
                                getShell(),
                                "Execute workflow",
                                "There was an error during workflow execution",
                                e);
                          }
                        }));
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

    redraw();
  }

  /**
   * Select all the actions and notes in a certain (screen) rectangle
   *
   * @param rect The selection area as a rectangle
   */
  public void selectInRect(WorkflowMeta workflowMeta, Rectangle rect) {

    // Normalize the selection area
    // Only for people not dragging from left top to right bottom
    if (rect.height < 0) {
      rect.y = rect.y + rect.height;
      rect.height = -rect.height;
    }
    if (rect.width < 0) {
      rect.x = rect.x + rect.width;
      rect.width = -rect.width;
    }

    for (ActionMeta action : workflowMeta.getActions()) {
      if (rect.contains(action.getLocation())) {
        action.setSelected(true);
      }
    }
    for (NotePadMeta note : workflowMeta.getNotes()) {
      Point a = note.getLocation();
      Point b = new Point(a.x + note.width, a.y + note.height);
      if (rect.contains(a) && rect.contains(b)) {
        note.setSelected(true);
      }
    }
  }

  @Override
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

    if (selectedAction != null && selection.isEmpty()) {
      workflowActionDelegate.deleteAction(workflowMeta, selectedAction);
      return;
    }

    if (!selection.isEmpty()) {
      workflowActionDelegate.deleteActions(workflowMeta, selection);
    }
    if (!workflowMeta.getSelectedNotes().isEmpty()) {
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
    canvas.setData("mode", "null");
    canvas.setData(START_HOP_NODE, null);
    startHopAction = null;
    endHopAction = null;
    iconOffset = null;
    dragSelection = false;
    workflowMeta.unselectAll();
    for (WorkflowHopMeta hop : workflowMeta.getWorkflowHops()) {
      hop.setSplit(false);
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
   * See if the location (x,y) is on a line between two actions: the hop!
   *
   * @param x coordinate
   * @param y coordinate
   * @return the workflow hop on the specified location, otherwise: null
   */
  private WorkflowHopMeta findWorkflowHop(int x, int y) {
    return findHop(x, y, null);
  }

  /**
   * See if the location (x,y) is on a line between two actions: the hop!
   *
   * @param x coordinate
   * @param y coordinate
   * @param exclude the action to exclude from the hops (from or to location). Specify null if no
   *     action is to be excluded.
   * @return the workflow hop on the specified location, otherwise: null
   */
  private WorkflowHopMeta findHop(int x, int y, ActionMeta exclude) {
    WorkflowHopMeta online = null;
    for (WorkflowHopMeta hop : workflowMeta.getWorkflowHops()) {
      ActionMeta fs = hop.getFromAction();
      ActionMeta ts = hop.getToAction();

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
        online = hop;
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

    int x1 = from.x + iconSize / 2;
    int y1 = from.y + iconSize / 2;

    int x2 = to.x + iconSize / 2;
    int y2 = to.y + iconSize / 2;

    return new int[] {x1, y1, x2, y2};
  }

  @GuiContextAction(
      id = "workflow-graph-action-10040-start-workflow-here",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.StartWorkflowHere.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.StartWorkflowHere.Tooltip",
      image = "ui/images/run.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void startWorkflowHere(HopGuiWorkflowActionContext context) {
    workflowMeta.setShowDialog(workflowMeta.isAlwaysShowRunOptions());
    ServerPushSessionFacade.start();
    Thread thread =
        new Thread(
            () ->
                hopGui
                    .getDisplay()
                    .asyncExec(
                        () -> {
                          try {
                            workflowRunDelegate.executeWorkflow(
                                hopGui.getVariables(),
                                workflowMeta,
                                context.getActionMeta().getName());
                            ServerPushSessionFacade.stop();
                          } catch (Exception e) {
                            new ErrorDialog(
                                hopGui.getActiveShell(),
                                "Execute workflow",
                                "There was an error during workflow execution",
                                e);
                          }
                        }));
    thread.start();
  }

  @GuiContextAction(
      id = "workflow-graph-action-10050-create-hop",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Create,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.CreateHop.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.CreateHop.Tooltip",
      image = "ui/images/hop.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void newHopCandidate(HopGuiWorkflowActionContext context) {
    startHopAction = context.getActionMeta();
    endHopAction = null;
    canvas.setData("mode", "hop");
    canvas.setData(START_HOP_NODE, startHopAction.getName());
    redraw();
  }

  @GuiContextAction(
      id = "workflow-graph-action-10800-edit-description",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.EditActionDescription.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.EditActionDescription.Tooltip",
      image = "ui/images/edit-description.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void editActionDescription(HopGuiWorkflowActionContext context) {
    this.editActionDescription(context.getActionMeta());
  }

  public void editActionDescription(ActionMeta actionMeta) {
    String title = BaseMessages.getString(PKG, "WorkflowGraph.Dialog.EditDescription.Title");
    String message = BaseMessages.getString(PKG, "WorkflowGraph.Dialog.EditDescription.Message");
    EnterTextDialog dialog =
        new EnterTextDialog(hopShell(), title, message, actionMeta.getDescription());
    String description = dialog.open();
    if (description != null) {
      actionMeta.setDescription(description);
      actionMeta.setChanged();
      updateGui();
    }
  }

  /** Go from serial to parallel to serial execution */
  @GuiContextAction(
      id = "workflow-graph-transform-10600-parallel",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.ParallelExecution.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.ParallelExecution.Tooltip",
      image = "ui/images/parallel.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Advanced.Text",
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

    if (action.isLaunchingInParallel()
        && "Y"
            .equalsIgnoreCase(
                hopGui.getProps().getCustomParameter(STRING_PARALLEL_WARNING_PARAMETER, "Y"))) {
      // Show a warning (optional)
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
    redraw();
  }

  @GuiContextAction(
      id = "workflow-graph-action-10900-delete",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Delete,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.DeleteAction.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.DeleteAction.Tooltip",
      image = "ui/images/delete.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void deleteAction(HopGuiWorkflowActionContext context) {
    deleteSelected(context.getActionMeta());
    redraw();
  }

  @GuiContextAction(
      id = "workflow-graph-action-90000-help",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::System.Button.Help",
      tooltip = "i18n::System.Tooltip.Help",
      image = "ui/images/help.svg",
      category = "Basic",
      categoryOrder = "1")
  public void openActionHelp(HopGuiWorkflowActionContext context) {
    IPlugin plugin =
        PluginRegistry.getInstance()
            .getPlugin(ActionPluginType.class, context.getActionMeta().getAction());

    HelpUtils.openHelp(getShell(), plugin);
  }

  @Override
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
    // Only handle delete if a workflow graph item was the last selected item
    HopGuiSelectionTracker selectionTracker = HopGuiSelectionTracker.getInstance();
    if (!selectionTracker.isLastSelection(HopGuiSelectionTracker.SelectionType.WORKFLOW_GRAPH)) {
      return;
    }

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
      id = "workflow-graph-workflow-clipboard-paste",
      parentId = HopGuiWorkflowContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.PasteFromClipboard.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.PasteFromClipboard.Tooltip",
      image = "ui/images/paste.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void pasteFromClipboard(HopGuiWorkflowContext context) {
    workflowClipboardDelegate.pasteXml(
        workflowMeta, workflowClipboardDelegate.fromClipboard(), context.getClick());
  }

  @GuiContextAction(
      id = "workflow-graph-transform-10110-copy-notepad-to-clipboard",
      parentId = HopGuiWorkflowNoteContext.CONTEXT_ID,
      type = GuiActionType.Custom,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.CopyToClipboard.Name",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.CopyToClipboard.Tooltip",
      image = "ui/images/copy.svg",
      category = "Basic",
      categoryOrder = "1")
  public void copyNotePadToClipboard(HopGuiWorkflowNoteContext context) {
    workflowClipboardDelegate.copySelected(
        workflowMeta, Collections.emptyList(), Arrays.asList(context.getNotePadMeta()));
  }

  @GuiContextAction(
      id = "workflow-graph-edit-workflow",
      parentId = HopGuiWorkflowContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.EditWorkflow.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.EditWorkflow.Tooltip",
      image = "ui/images/workflow.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void editWorkflowProperties(HopGuiWorkflowContext context) {
    editProperties(workflowMeta, hopGui, true);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_EDIT_WORKFLOW,
      toolTip = "i18n::WorkflowGraph.Toolbar.EditWorkflow.Tooltip",
      image = "ui/images/workflow.svg")
  @GuiKeyboardShortcut(control = true, key = 'l')
  @GuiOsxKeyboardShortcut(command = true, key = 'l')
  public void editWorkflowProperties() {
    editProperties(workflowMeta, hopGui, true);
  }

  @GuiContextAction(
      id = "workflow-graph-new-note",
      parentId = HopGuiWorkflowContext.CONTEXT_ID,
      type = GuiActionType.Create,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.CreateNote.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.CreateNote.Tooltip",
      image = "ui/images/note-add.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void newNote(HopGuiWorkflowContext context) {
    String title = BaseMessages.getString(PKG, "WorkflowGraph.Dialog.EditNote.Title");
    NotePadDialog dialog = new NotePadDialog(variables, hopShell(), title);
    NotePadMeta note = dialog.open();
    if (note != null) {
      NotePadMeta newNote =
          new NotePadMeta(
              note.getNote(),
              context.getClick().x,
              context.getClick().y,
              ConstUi.NOTE_MIN_SIZE,
              ConstUi.NOTE_MIN_SIZE,
              note.getFontName(),
              note.getFontSize(),
              note.isFontBold(),
              note.isFontItalic(),
              note.getFontColorRed(),
              note.getFontColorGreen(),
              note.getFontColorBlue(),
              note.getBackGroundColorRed(),
              note.getBackGroundColorGreen(),
              note.getBackGroundColorBlue(),
              note.getBorderColorRed(),
              note.getBorderColorGreen(),
              note.getBorderColorBlue());
      workflowMeta.addNote(newNote);
      hopGui.undoDelegate.addUndoNew(
          workflowMeta, new NotePadMeta[] {newNote}, new int[] {workflowMeta.indexOfNote(newNote)});
      redraw();
    }
  }

  public void setCurrentNote(NotePadMeta notePad) {
    this.currentNotePad = notePad;
  }

  public NotePadMeta getCurrentNote() {
    return currentNotePad;
  }

  @GuiContextAction(
      id = "workflow-graph-10-edit-note",
      parentId = HopGuiWorkflowNoteContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.EditNote.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.EditNote.Tooltip",
      image = "ui/images/edit.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void editNote(HopGuiWorkflowNoteContext context) {
    selectionRegion = null;
    editNote(context.getNotePadMeta());
  }

  @GuiContextAction(
      id = "workflow-graph-20-delete-note",
      parentId = HopGuiWorkflowNoteContext.CONTEXT_ID,
      type = GuiActionType.Delete,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.DeleteNote.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.DeleteNote.Tooltip",
      image = "ui/images/delete.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void deleteNote(HopGuiWorkflowNoteContext context) {
    selectionRegion = null;
    NotePadMeta note = context.getNotePadMeta();
    int idx = workflowMeta.indexOfNote(note);
    if (idx >= 0) {
      workflowMeta.removeNote(idx);
      hopGui.undoDelegate.addUndoDelete(workflowMeta, new NotePadMeta[] {note}, new int[] {idx});
    }
    redraw();
  }

  @GuiContextAction(
      id = "workflow-graph-30-bring-note-to-front",
      parentId = HopGuiWorkflowNoteContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.BringNoteToFront.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.BringNoteToFront.Tooltip",
      image = "ui/images/bring-to-front.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Arrange.Text",
      categoryOrder = "2")
  public void bringNoteToFront(HopGuiWorkflowNoteContext context) {
    selectionRegion = null;
    NotePadMeta note = context.getNotePadMeta();
    int idx = workflowMeta.indexOfNote(note);
    if (idx >= 0) {
      workflowMeta.raiseNote(idx);
      hopGui.undoDelegate.addUndoDelete(workflowMeta, new NotePadMeta[] {note}, new int[] {idx});
    }
    redraw();
  }

  @GuiContextAction(
      id = "workflow-graph-40-send-note-to-back",
      parentId = HopGuiWorkflowNoteContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.SendNoteToBack.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.SendNoteToBack.Tooltip",
      image = "ui/images/send-to-back.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Arrange.Text",
      categoryOrder = "2")
  public void sendNoteToBack(HopGuiWorkflowNoteContext context) {
    selectionRegion = null;
    NotePadMeta note = context.getNotePadMeta();
    int idx = workflowMeta.indexOfNote(note);
    if (idx >= 0) {
      workflowMeta.lowerNote(idx);
      hopGui.undoDelegate.addUndoDelete(workflowMeta, new NotePadMeta[] {note}, new int[] {idx});
    }
    redraw();
  }

  @GuiContextAction(
      id = ACTION_ID_WORKFLOW_GRAPH_HOP_ENABLE,
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.EnableHop.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.EnableHop.Tooltip",
      image = "ui/images/hop.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
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
      id = ACTION_ID_WORKFLOW_GRAPH_HOP_DISABLE,
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.DisableHop.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.DisableHop.Tooltip",
      image = "ui/images/hop-disable.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
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
          BaseMessages.getString(PKG, CONST_WORKFLOW_GRAPH_DIALOG_LOOP_AFTER_HOP_ENABLED_MESSAGE));
      mb.setText(
          BaseMessages.getString(PKG, CONST_WORKFLOW_GRAPH_DIALOG_LOOP_AFTER_HOP_ENABLED_TITLE));
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
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.DeleteHop.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.DeleteHop.Tooltip",
      image = "ui/images/hop-delete.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void deleteHop(HopGuiWorkflowHopContext context) {
    workflowHopDelegate.delHop(workflowMeta, context.getHopMeta());
    updateGui();
  }

  @GuiContextAction(
      id = ACTION_ID_WORKFLOW_GRAPH_HOP_HOP_UNCONDITIONAL,
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.UnconditionalHop.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.UnconditionalHop.Tooltip",
      image = "ui/images/unconditional.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Routing.Text",
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
      id = ACTION_ID_WORKFLOW_GRAPH_HOP_HOP_EVALUATION_SUCCESS,
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.SuccessHop.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.SuccessHop.Tooltip",
      image = "ui/images/true.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Routing.Text",
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
      id = ACTION_ID_WORKFLOW_GRAPH_HOP_HOP_EVALUATION_FAILURE,
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.FailureHop.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.FailureHop.Tooltip",
      image = "ui/images/false.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Routing.Text",
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

  /**
   * We're filtering out the disable action for hops which are already disabled. The same for the
   * enabled hops.
   *
   * @param contextActionId
   * @param context
   * @return True if the action should be shown and false otherwise.
   */
  @GuiContextActionFilter(parentId = HopGuiWorkflowHopContext.CONTEXT_ID)
  public boolean filterWorkflowHopActions(
      String contextActionId, HopGuiWorkflowHopContext context) {

    // Enable / disable
    //
    if (contextActionId.equals(ACTION_ID_WORKFLOW_GRAPH_HOP_ENABLE)) {
      return !context.getHopMeta().isEnabled();
    }
    if (contextActionId.equals(ACTION_ID_WORKFLOW_GRAPH_HOP_DISABLE)) {
      return context.getHopMeta().isEnabled();
    }
    return true;
  }

  @GuiContextAction(
      id = "workflow-graph-hop-10065-hop-enable-between-selected-actions",
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.EnableBetweenSelectedActions.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.EnableBetweenSelectedActions.Tooltip",
      image = "ui/images/hop-enable-between-selected.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Bulk.Text",
      categoryOrder = "3")
  public void enableHopsBetweenSelectedActions(final HopGuiWorkflowHopContext context) {
    enableHopsBetweenSelectedActions(true);
  }

  @GuiContextAction(
      id = "workflow-graph-hop-10075-hop-disable-between-selected-actions",
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.DisableBetweenSelectedActions.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.DisableBetweenSelectedActions.Tooltip",
      image = "ui/images/hop-disable-between-selected.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Bulk.Text",
      categoryOrder = "3")
  public void disableHopsBetweenSelectedActions(final HopGuiWorkflowHopContext context) {
    enableHopsBetweenSelectedActions(false);
  }

  /** This method enables or disables all the hops between the selected Actions. */
  public void enableHopsBetweenSelectedActions(boolean enabled) {
    List<ActionMeta> list = workflowMeta.getSelectedActions();

    boolean hasLoop = false;

    for (int i = 0; i < workflowMeta.nrWorkflowHops(); i++) {
      WorkflowHopMeta hop = workflowMeta.getWorkflowHop(i);
      if (list.contains(hop.getFromAction()) && list.contains(hop.getToAction())) {

        WorkflowHopMeta before = hop.clone();
        hop.setEnabled(enabled);
        WorkflowHopMeta after = hop.clone();
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
          BaseMessages.getString(PKG, CONST_WORKFLOW_GRAPH_DIALOG_LOOP_AFTER_HOP_ENABLED_MESSAGE));
      mb.setText(
          BaseMessages.getString(PKG, CONST_WORKFLOW_GRAPH_DIALOG_LOOP_AFTER_HOP_ENABLED_TITLE));
      mb.open();
    }

    updateGui();
  }

  @GuiContextAction(
      id = "workflow-graph-hop-10060-hop-enable-downstream",
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.EnableDownstream.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.EnableDownstream.Tooltip",
      image = "ui/images/hop-enable-downstream.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Bulk.Text",
      categoryOrder = "3")
  public void enableHopsDownstream(HopGuiWorkflowHopContext context) {
    enableDisableHopsDownstream(context.getHopMeta(), true);
  }

  @GuiContextAction(
      id = "workflow-graph-hop-10070-hop-disable-downstream",
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.DisableDownstream.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.DisableDownstream.Tooltip",
      image = "ui/images/hop-disable-downstream.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Bulk.Text",
      categoryOrder = "3")
  public void disableHopsDownstream(HopGuiWorkflowHopContext context) {
    enableDisableHopsDownstream(context.getHopMeta(), false);
  }

  @GuiContextAction(
      id = "workflow-graph-hop-10080-hop-insert-action",
      parentId = HopGuiWorkflowHopContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.InsetAction.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.InsetAction.Tooltip",
      image = "ui/images/add-item.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "3")
  public void insertAction(HopGuiWorkflowHopContext context) {

    // Build actions list
    //
    List<GuiAction> guiActions = new ArrayList<>();
    PluginRegistry registry = PluginRegistry.getInstance();
    for (IPlugin plugin : registry.getPlugins(ActionPluginType.class)) {

      GuiAction guiAction =
          new GuiAction(
              "workflow-graph-insert-action-" + plugin.getIds()[0],
              GuiActionType.Create,
              plugin.getName(),
              plugin.getDescription(),
              plugin.getImageFile(),
              (shiftClicked, controlClicked, t) ->
                  workflowActionDelegate.insertAction(
                      workflowMeta,
                      context.getHopMeta(),
                      plugin.getIds()[0],
                      plugin.getName(),
                      context.getClick()));
      guiAction.getKeywords().addAll(Arrays.asList(plugin.getKeywords()));
      guiAction.getKeywords().add(plugin.getCategory());
      guiAction.setCategory(plugin.getCategory());
      guiAction.setCategoryOrder(plugin.getCategory());

      try {
        guiAction.setClassLoader(registry.getClassLoader(plugin));
      } catch (HopPluginException e) {
        LogChannel.UI.logError(
            "Unable to get classloader for action plugin " + plugin.getIds()[0], e);
      }

      guiActions.add(guiAction);
    }

    String message =
        BaseMessages.getString(
            PKG, "HopGuiWorkflowGraph.ContextualActionDialog.InsertAction.Header");

    ContextDialog contextDialog =
        new ContextDialog(
            hopShell(), message, context.getClick(), guiActions, HopGuiWorkflowContext.CONTEXT_ID);

    GuiAction selectedAction = contextDialog.open();

    if (selectedAction != null) {
      IGuiActionLambda<?> actionLambda = selectedAction.getActionLambda();
      actionLambda.executeAction(contextDialog.isShiftClicked(), contextDialog.isCtrlClicked());
    }
  }

  public void enableDisableHopsDownstream(WorkflowHopMeta hop, boolean enabled) {
    if (hop == null) {
      return;
    }
    WorkflowHopMeta before = hop.clone();
    hop.setEnabled(enabled);
    WorkflowHopMeta after = hop.clone();
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
          BaseMessages.getString(PKG, CONST_WORKFLOW_GRAPH_DIALOG_LOOP_AFTER_HOP_ENABLED_MESSAGE));
      mb.setText(
          BaseMessages.getString(PKG, CONST_WORKFLOW_GRAPH_DIALOG_LOOP_AFTER_HOP_ENABLED_TITLE));
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
                WorkflowHopMeta before = hop.clone();
                hop.setEnabled(enabled);
                WorkflowHopMeta after = hop.clone();
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

  protected void moveSelected(int dx, int dy) {
    selectedNotes = workflowMeta.getSelectedNotes();
    selectedActions = workflowMeta.getSelectedActions();

    // Check minimum location of selected elements
    if (selectedActions != null) {
      for (ActionMeta action : selectedActions) {
        Point location = action.getLocation();
        if (location.x + dx < 0) {
          dx = -location.x;
        }
        if (location.y + dy < 0) {
          dy = -location.y;
        }
      }
    }
    if (selectedNotes != null) {
      for (NotePadMeta notePad : selectedNotes) {
        Point location = notePad.getLocation();
        if (location.x + dx < 0) {
          dx = -location.x;
        }
        if (location.y + dy < 0) {
          dy = -location.y;
        }
      }
    }

    // Adjust location of selected actions...
    if (selectedActions != null) {
      for (ActionMeta action : selectedActions) {
        PropsUi.setLocation(action, action.getLocation().x + dx, action.getLocation().y + dy);
      }
    }
    // Adjust location of selected notes...
    if (selectedNotes != null) {
      for (NotePadMeta notePad : selectedNotes) {
        PropsUi.setLocation(notePad, notePad.getLocation().x + dx, notePad.getLocation().y + dy);
      }
    }
  }

  private void modalMessageDialog(String title, String message, int swtFlags) {
    MessageBox messageBox = new MessageBox(hopShell(), swtFlags);
    messageBox.setMessage(message);
    messageBox.setText(title);
    messageBox.open();
  }

  @SuppressWarnings({"java:S1854", "java:S1481"})
  // Ignore warning of setting tipImage
  protected void setToolTip(int x, int y, int screenX, int screenY) {
    if (!hopGui.getProps().showToolTips() || openedContextDialog) {
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
            if (hi.isEvaluation()) {
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

        case ACTION_RESULT_FAILURE, ACTION_RESULT_SUCCESS:
          ActionResult actionResult = (ActionResult) areaOwner.getOwner();
          actionCopy = (ActionMeta) areaOwner.getParent();
          Result result = actionResult.getResult();
          tip.append("'").append(actionCopy.getName()).append("' ");
          if (result.isResult()) {
            tipImage = GuiResource.getInstance().getImageSuccess();
            tip.append("finished successfully.");
          } else {
            tipImage = GuiResource.getInstance().getImageFailure();
            tip.append("failed.");
          }
          tip.append(Const.CR).append("------------------------").append(Const.CR).append(Const.CR);
          tip.append("Result         : ").append(result.isResult()).append(Const.CR);
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
          if (!Utils.isEmpty(result.getResultFiles())) {
            tip.append(Const.CR).append("Result files:").append(Const.CR);
            if (result.getResultFiles().size() > 10) {
              tip.append(" (10 files of ").append(result.getResultFiles().size()).append(" shown");
            }
            List<ResultFile> files = new ArrayList<>(result.getResultFiles().values());
            for (ResultFile file : files) {
              tip.append("  - ").append(file.toString()).append(Const.CR);
            }
          }
          if (!Utils.isEmpty(result.getRows())) {
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
        case ACTION_INFO_ICON, ACTION_ICON:
          ActionMeta actionMetaInfo = (ActionMeta) areaOwner.getOwner();

          // If transform is deprecated, display first
          if (actionMetaInfo.isDeprecated()) { // only need tooltip if action is deprecated
            tip.append(BaseMessages.getString(PKG, "WorkflowGraph.DeprecatedEntry.Tooltip.Title"))
                .append(Const.CR);
            String tipNext =
                BaseMessages.getString(
                    PKG,
                    "WorkflowGraph.DeprecatedEntry.Tooltip.Message1",
                    actionMetaInfo.getName());
            int length = tipNext.length() + 5;
            for (int i = 0; i < length; i++) {
              tip.append("-");
            }
            tip.append(Const.CR).append(tipNext).append(Const.CR);
            tip.append(
                BaseMessages.getString(PKG, "WorkflowGraph.DeprecatedEntry.Tooltip.Message2"));
            if (!Utils.isEmpty(actionMetaInfo.getSuggestion())
                && !(actionMetaInfo.getSuggestion().startsWith("!")
                    && actionMetaInfo.getSuggestion().endsWith("!"))) {
              tip.append(" ");
              tip.append(
                  BaseMessages.getString(
                      PKG,
                      "WorkflowGraph.DeprecatedEntry.Tooltip.Message3",
                      actionMetaInfo.getSuggestion()));
            }
            tipImage = GuiResource.getInstance().getImageDeprecated();
          } else if (!Utils.isEmpty(actionMetaInfo.getDescription())) {
            tip.append(actionMetaInfo.getDescription());
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

    if (hi != null && tip.isEmpty()) {
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
      tip.append(BaseMessages.getString(PKG, "WorkflowGraph.Dialog.HopInfo.Status")).append(" ");
      tip.append(
          (hi.isEnabled()
              ? BaseMessages.getString(PKG, "WorkflowGraph.Dialog.HopInfo.Enable")
              : BaseMessages.getString(PKG, "WorkflowGraph.Dialog.HopInfo.Disable")));
      if (hi.isUnconditional()) {
        tipImage = GuiResource.getInstance().getImageUnconditionalHop();
      } else {
        if (hi.isEvaluation()) {
          tipImage = GuiResource.getInstance().getImageTrue();
        } else {
          tipImage = GuiResource.getInstance().getImageFalse();
        }
      }
    }

    if (Utils.isEmpty(tip)) {
      toolTip.setVisible(false);
    } else {
      if (!tip.toString().equalsIgnoreCase(getToolTipText())) {
        toolTip.setText(tip.toString());
        toolTip.setVisible(false);
        showToolTip(new org.eclipse.swt.graphics.Point(screenX, screenY));
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
      IHopFileType fileType = hopGui.getPerspectiveManager().findFileTypeHandler(referencedMeta);
      fileType.openFile(hopGui, referencedMeta.getFilename(), hopGui.getVariables());
    } catch (Exception e) {
      new ErrorDialog(
          hopShell(),
          BaseMessages.getString(PKG, "HopGuiWorkflowGraph.ErrorDialog.FileNotLoaded.Header"),
          BaseMessages.getString(PKG, "HopGuiWorkflowGraph.ErrorDialog.FileNotLoaded.Message"),
          e);
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
      new ErrorDialog(
          hopGui.getActiveShell(),
          BaseMessages.getString(PKG, "HopGuiWorkflowGraph.ErrorDialog.WorkflowDrawing.Header"),
          BaseMessages.getString(PKG, "HopGuiWorkflowGraph.ErrorDialog.WorkflowDrawing.Message"),
          ex);
    }
  }

  public void drawWorkflowImage(GC swtGc, int width, int height, float magnificationFactor)
      throws HopException {

    IGc gc = new SwtGc(swtGc, width, height, iconSize);
    try {
      PropsUi propsUi = PropsUi.getInstance();

      maximum = workflowMeta.getMaximum();
      int gridSize = propsUi.isShowCanvasGridEnabled() ? propsUi.getCanvasGridSize() : 1;

      WorkflowPainter workflowPainter =
          new WorkflowPainter(
              gc,
              variables,
              workflowMeta,
              new Point(width, height),
              offset,
              hopCandidate,
              selectionRegion,
              areaOwners,
              propsUi.getIconSize(),
              propsUi.getLineWidth(),
              gridSize,
              propsUi.getNoteFont().getName(),
              propsUi.getNoteFont().getHeight(),
              propsUi.getZoomFactor(),
              propsUi.isBorderDrawnAroundCanvasNames(),
              mouseOverName);

      // correct the magnification with the overall zoom factor
      //
      float correctedMagnification = (float) (magnificationFactor * propsUi.getZoomFactor());

      workflowPainter.setMagnification(correctedMagnification);
      workflowPainter.setStartHopAction(startHopAction);
      workflowPainter.setEndHopLocation(endHopLocation);
      workflowPainter.setEndHopAction(endHopAction);
      workflowPainter.setNoInputAction(forbiddenAction);
      if (workflow != null) {
        workflowPainter.setActionResults(workflow.getActionResults());
      } else {
        workflowPainter.setActionResults(new ArrayList<>());
      }

      List<ActionMeta> activeActions = new ArrayList<>();
      if (workflow != null) {
        activeActions.addAll(workflow.getActiveActions());
      }
      workflowPainter.setActiveActions(activeActions);
      workflowPainter.setMaximum(maximum);
      workflowPainter.setShowingNavigationView(true);
      workflowPainter.setScreenMagnification(magnification);
      workflowPainter.setShowingNavigationView(!PropsUi.getInstance().isHideViewportEnabled());

      try {
        workflowPainter.drawWorkflow();

        // Keep the rectangles of the navigation view around
        //
        this.viewPort = workflowPainter.getViewPort();
        this.graphPort = workflowPainter.getGraphPort();

        if (workflowMeta.isEmpty()
            || (workflowMeta.nrNotes() == 0
                && workflowMeta.nrActions() == 1
                && workflowMeta.getAction(0).isStart())) {
          SvgFile svgFile =
              new SvgFile(
                  BasePropertyHandler.getProperty("WorkflowCanvas_image"),
                  getClass().getClassLoader());
          gc.setTransform(0.0f, 0.0f, (float) (magnification * PropsUi.getNativeZoomFactor()));
          gc.drawImage(svgFile, 150, 150, 32, 40, gc.getMagnification(), 0);
          gc.drawText(
              BaseMessages.getString(PKG, "HopGuiWorkflowGraph.NewWorkflowBackgroundMessage"),
              155,
              125,
              true);
        }
      } catch (Exception e) {
        throw new HopException("Error drawing workflow", e);
      }

    } finally {
      gc.dispose();
    }
    CanvasFacade.setData(canvas, magnification, offset, workflowMeta);
  }

  @Override
  public boolean hasChanged() {
    return workflowMeta.hasChanged();
  }

  @Override
  public void setChanged() {
    workflowMeta.setChanged();
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
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.EditAction.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.EditAction.Tooltip",
      image = "ui/images/edit.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void editAction(HopGuiWorkflowActionContext context) {
    workflowMeta.unselectAll();
    updateGui();
    workflowActionDelegate.editAction(workflowMeta, context.getActionMeta());
  }

  public void editAction(ActionMeta actionMeta) {
    workflowMeta.unselectAll();
    updateGui();
    workflowActionDelegate.editAction(workflowMeta, actionMeta);
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

      updateGui();
    }
  }

  protected boolean pointOnLine(int x, int y, int[] line) {
    int dx;
    int dy;
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
    if (!(((x >= x1 && x <= x2) || (x >= x2 && x <= x1))
        && ((y >= y1 && y <= y2) || (y >= y2 && y <= y1)))) {
      return false;
    }

    double angleLine = Math.atan2(y2 - y1, x2 - x1) + Math.PI;
    double anglePoint = Math.atan2(y - y1, x - x1) + Math.PI;

    // Same angle, or close enough?
    return anglePoint >= angleLine - 0.01 && anglePoint <= angleLine + 0.01;
  }

  @Override
  public SnapAllignDistribute createSnapAlignDistribute() {
    List<ActionMeta> elements = workflowMeta.getSelectedActions();
    int[] indices = workflowMeta.getActionIndexes(elements);
    return new SnapAllignDistribute(workflowMeta, elements, indices, hopGui.undoDelegate, this);
  }

  @GuiContextAction(
      id = "workflow-graph-action-10100-action-detach",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.DetachAction.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.DetachAction.Tooltip",
      image = "ui/images/hop-delete.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
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
      id = "workflow-graph-action-10010-copy-notepad-to-clipboard",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Custom,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.CopyAction.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.CopyAction.Tooltip",
      image = "ui/images/copy.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void copyActionToClipboard(HopGuiWorkflowActionContext context) {
    workflowClipboardDelegate.copySelected(
        workflowMeta, Arrays.asList(context.getActionMeta()), workflowMeta.getSelectedNotes());
  }

  public void newProps() {
    iconSize = hopGui.getProps().getIconSize();
    lineWidth = hopGui.getProps().getLineWidth();
  }

  @Override
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
   */
  public void setWorkflowMeta(WorkflowMeta workflowMeta) {
    this.workflowMeta = workflowMeta;
    if (workflowMeta != null) {
      workflowMeta.setInternalHopVariables(variables);
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_UNDO_ID,
      // label = "Undo",
      toolTip = "i18n:org.apache.hop.ui.hopgui:HopGui.Toolbar.Undo.Tooltip",
      image = "ui/images/undo.svg",
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
      image = "ui/images/redo.svg")
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
    return workflow.isInitialized();
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

              // Enable/disable the execution toolbar buttons
              //
              boolean running = isRunning() && !workflow.isStopped();
              toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_START, !running);
              toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_STOP, running);

              // Enable/disable the navigate to execution info button
              //
              boolean hasExecutionInfoLocations = false;
              try {
                IHopMetadataSerializer<ExecutionInfoLocation> serializer =
                    hopGui.getMetadataProvider().getSerializer(ExecutionInfoLocation.class);
                List<String> locationNames = serializer.listObjectNames();
                hasExecutionInfoLocations = !locationNames.isEmpty();
              } catch (Exception e) {
                // Ignore, button will be disabled
              }
              toolBarWidgets.enableToolbarItem(
                  TOOLBAR_ITEM_TO_EXECUTION_INFO, hasExecutionInfoLocations);

              hopGui.setUndoMenu(workflowMeta);
              hopGui.handleFileCapabilities(fileType, workflowMeta.hasChanged(), running, false);

              // Enable the align/distribute toolbar menus if one or more actions are selected.
              //
              super.enableSnapAlignDistributeMenuItems(
                  fileType, !workflowMeta.getSelectedActions().isEmpty());

              try {
                ExtensionPointHandler.callExtensionPoint(
                    LogChannel.UI,
                    variables,
                    HopGuiExtensionPoint.HopGuiWorkflowGraphUpdateGui.id,
                    this);
              } catch (Exception xe) {
                LogChannel.UI.logError(
                    "Error handling extension point 'HopGuiWorkflowGraphUpdateGui'", xe);
              }

              perspective.updateTabItem(this);
              perspective.updateTreeItem(this);

              HopGuiWorkflowGraph.super.redraw();
            });
  }

  public boolean canBeClosed() {
    return !workflowMeta.hasChanged();
  }

  public WorkflowMeta getManagedObject() {
    return workflowMeta;
  }

  /**
   * @deprecated Use method hasChanged()
   */
  @Deprecated(since = "2.10")
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

    Shell shell = hopGui.getActiveShell();
    if (shell == null) {
      shell = hopGui.getShell();
    }
    WorkflowDialog jd = new WorkflowDialog(shell, SWT.NONE, variables, workflowMeta);
    if (jd.open() != null) {
      // If we added properties, add them to the variables too, so that they appear in the
      // CTRL-SPACE variable completion.
      //
      hopGui.setParametersAsVariablesInUI(workflowMeta, variables);

      updateGui();
      return true;
    }
    return false;
  }

  @Override
  public synchronized void save() throws HopException {
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

      boolean fileExist = HopVfs.fileExists(workflowMeta.getFilename());

      String xml = workflowMeta.getXml(variables);
      OutputStream out = HopVfs.getOutputStream(workflowMeta.getFilename(), false);
      try {
        out.write(XmlHandler.getXmlHeader(Const.XML_ENCODING).getBytes(StandardCharsets.UTF_8));
        out.write(xml.getBytes(StandardCharsets.UTF_8));
        workflowMeta.clearChanged();
        updateGui();
      } finally {
        out.flush();
        out.close();

        ExtensionPointHandler.callExtensionPoint(
            log, variables, HopExtensionPoint.WorkflowAfterSave.id, workflowMeta);

        // If we create a new file, refresh the explorer perspective tree
        // TODO: find a better way to refresh only a partial tree item
        if (!fileExist) {
          perspective.refresh();
        }
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

      // Normalize file name
      filename = HopVfs.normalize(filename);

      FileObject fileObject = HopVfs.getFileObject(filename);
      if (fileObject.exists()) {
        MessageBox box =
            new MessageBox(hopGui.getActiveShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
        box.setText("Overwrite?");
        box.setMessage("Are you sure you want to overwrite file '" + filename + "'?");
        int answer = box.open();
        if ((answer & SWT.YES) == 0) {
          return;
        }
      }

      workflowMeta.setFilename(filename);
      save();
      hopGui.fileRefreshDelegate.register(filename, this);
    } catch (Exception e) {
      throw new HopException("Error validating file existence for '" + filename + "'", e);
    }
  }

  /** Add an extra view to the main composite SashForm */
  public void addExtraView() {

    // Add a tab folder ...
    //
    extraViewTabFolder = new CTabFolder(sashForm, SWT.MULTI);
    PropsUi.setLook(extraViewTabFolder, Props.WIDGET_STYLE_TAB);

    extraViewTabFolder.addMouseListener(
        new MouseAdapter() {

          @Override
          public void mouseDoubleClick(MouseEvent arg0) {
            if (sashForm.getMaximizedControl() == null) {
              sashForm.setMaximizedControl(extraViewTabFolder);
            } else {
              sashForm.setMaximizedControl(null);
            }
          }
        });

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.top = new FormAttachment(0, 0);
    fdTabFolder.bottom = new FormAttachment(100, 0);
    extraViewTabFolder.setLayoutData(fdTabFolder);

    // Create toolbar for close and min/max to the upper right corner...
    //
    ToolBar extraViewToolBar = new ToolBar(extraViewTabFolder, SWT.FLAT);
    extraViewTabFolder.setTopRight(extraViewToolBar, SWT.RIGHT);
    PropsUi.setLook(extraViewToolBar);

    minMaxItem = new ToolItem(extraViewToolBar, SWT.PUSH);
    minMaxItem.setImage(GuiResource.getInstance().getImageMaximizePanel());
    minMaxItem.setToolTipText(
        BaseMessages.getString(PKG, "WorkflowGraph.ExecutionResultsPanel.MaxButton.Tooltip"));
    minMaxItem.addListener(SWT.Selection, e -> minMaxExtraView());

    rotateItem = new ToolItem(extraViewToolBar, SWT.PUSH);
    rotateItem.setImage(
        PropsUi.getInstance().isGraphExtraViewVerticalOrientation()
            ? GuiResource.getInstance().getImageRotateRight()
            : GuiResource.getInstance().getImageRotateLeft());
    rotateItem.setToolTipText(
        BaseMessages.getString(PKG, "WorkflowGraph.ExecutionResultsPanel.RotateButton.Tooltip"));
    rotateItem.addListener(SWT.Selection, e -> rotateExtraView());

    ToolItem closeItem = new ToolItem(extraViewToolBar, SWT.PUSH);
    closeItem.setImage(GuiResource.getInstance().getImageClose());
    closeItem.setToolTipText(
        BaseMessages.getString(PKG, "WorkflowGraph.ExecutionResultsPanel.CloseButton.Tooltip"));
    closeItem.addListener(SWT.Selection, e -> disposeExtraView());

    int height = extraViewToolBar.computeSize(SWT.DEFAULT, SWT.DEFAULT).y;
    extraViewTabFolder.setTabHeight(Math.max(height, extraViewTabFolder.getTabHeight()));

    sashForm.setWeights(new int[] {60, 40});
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_CHECK,
      toolTip = "i18n:org.apache.hop.ui.hopgui:HopGui.Tooltip.VerifyWorkflow",
      image = "ui/images/check.svg",
      separator = true)
  @GuiKeyboardShortcut(key = SWT.F7)
  public void checkWorkflow() {
    // Show the results views
    //
    addAllTabs();

    workflowCheckDelegate.checkWorkflow();
  }

  /** If the extra tab view at the bottom is empty, we close it. */
  public void checkEmptyExtraView() {
    if (extraViewTabFolder.getItemCount() == 0) {
      disposeExtraView();
    }
  }

  private void rotateExtraView() {

    // Toggle orientation
    boolean orientation = !PropsUi.getInstance().isGraphExtraViewVerticalOrientation();
    PropsUi.getInstance().setGraphExtraViewVerticalOrientation(orientation);

    if (orientation) {
      sashForm.setOrientation(SWT.VERTICAL);
      rotateItem.setImage(GuiResource.getInstance().getImageRotateRight());
    } else {
      sashForm.setOrientation(SWT.HORIZONTAL);
      rotateItem.setImage(GuiResource.getInstance().getImageRotateLeft());
    }
  }

  private void disposeExtraView() {
    if (extraViewTabFolder == null) {
      return;
    }

    extraViewTabFolder.dispose();
    sashForm.layout();
    sashForm.setWeights(100);

    ToolItem item = toolBarWidgets.findToolItem(TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS);
    item.setToolTipText(
        TranslateUtil.translate(
            "i18n:org.apache.hop.ui.hopgui:HopGui.Tooltip.ShowExecutionResults", PKG));
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
      minMaxItem.setImage(GuiResource.getInstance().getImageMaximizePanel());
      minMaxItem.setToolTipText(
          BaseMessages.getString(PKG, "WorkflowGraph.ExecutionResultsPanel.MaxButton.Tooltip"));
    } else {
      // Maximize
      //
      sashForm.setMaximizedControl(extraViewTabFolder);
      minMaxItem.setImage(GuiResource.getInstance().getImageMinimizePanel());
      minMaxItem.setToolTipText(
          BaseMessages.getString(PKG, "WorkflowGraph.ExecutionResultsPanel.MinButton.Tooltip"));
    }
  }

  public boolean isExecutionResultsPaneVisible() {
    return extraViewTabFolder != null && !extraViewTabFolder.isDisposed();
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

    workflowLogDelegate.addWorkflowLog();
    workflowGridDelegate.addWorkflowGrid();
    workflowCheckDelegate.addWorkflowCheck();
    if (extraViewTabFolder.getSelectionIndex() == -1) {
      extraViewTabFolder.setSelection(0);
    }

    ToolItem toolItem = toolBarWidgets.findToolItem(TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS);
    toolItem.setToolTipText(
        TranslateUtil.translate(
            "i18n:org.apache.hop.ui.hopgui:HopGui.Tooltip.HideExecutionResults", PKG));
    toolItem.setImage(GuiResource.getInstance().getImageHideResults());
  }

  @Override
  public void close() {
    perspective.remove(this);
  }

  @Override
  public boolean isCloseable() {
    try {
      // Check if the file is saved. If not, ask for it to be stopped before closing
      //
      if (workflow != null && (workflow.isActive())) {
        MessageBox messageDialog =
            new MessageBox(hopShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO | SWT.CANCEL);
        messageDialog.setText(
            BaseMessages.getString(PKG, "HopGuiWorkflowGraph.RunningFile.Dialog.Header"));
        messageDialog.setMessage(
            BaseMessages.getString(
                PKG, "HopGuiWorkflowGraph.RunningFile.Dialog.Message", buildTabName()));
        int answer = messageDialog.open();
        // The NO answer means: ignore the state of the workflow and just let it run in the
        // background
        // It can be seen in the execution information perspective if a location was set up.
        //
        if ((answer & SWT.YES) != 0) {
          // Stop the execution and close if the file hasn't been changed
          workflow.stopExecution();
        } else if ((answer & SWT.CANCEL) != 0) {
          return false;
        }
      }
      // Check if the file is saved. If not, ask for it to be saved.
      //
      if (workflowMeta.hasChanged()) {
        MessageBox messageDialog =
            new MessageBox(hopShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO | SWT.CANCEL);
        messageDialog.setText(
            BaseMessages.getString(PKG, "HopGuiWorkflowGraph.SaveFile.Dialog.Header"));
        messageDialog.setMessage(
            BaseMessages.getString(
                PKG, "HopGuiWorkflowGraph.SaveFile.Dialog.Message", buildTabName()));
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
        // User doesn't want to save but close
        return (answer & SWT.NO) != 0;
      } else {
        return true;
      }
    } catch (Exception e) {
      new ErrorDialog(hopShell(), CONST_ERROR, "Error preparing file close", e);
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
                  variables, workflowMeta.getFilename(), workflowMeta.getMetadataProvider());

          String hopGuiObjectId = UUID.randomUUID().toString();
          SimpleLoggingObject hopGuiLoggingObject =
              new SimpleLoggingObject("HOPGUI", LoggingObjectType.HOP_GUI, null);
          hopGuiLoggingObject.setContainerObjectId(hopGuiObjectId);
          hopGuiLoggingObject.setLogLevel(executionConfiguration.getLogLevel());

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
                  variables.resolve(executionConfiguration.getRunConfiguration()),
                  hopGui.getMetadataProvider(),
                  runWorkflowMeta,
                  hopGuiLoggingObject);

          workflow.setLogLevel(executionConfiguration.getLogLevel());
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

          // Link to the new workflow tracker
          workflowGridDelegate.setWorkflowTracker(workflow.getWorkflowTracker());

          startRedrawTimer();

          updateGui();

          // Attach a listener to notify us that the workflow has finished.
          //
          workflow.addExecutionFinishedListener(e -> HopGuiWorkflowGraph.this.workflowFinished());
          workflow.addExecutionStoppedListener(e -> HopGuiWorkflowGraph.this.workflowStopped());
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
    return new IActionListener<>() {

      @Override
      public void beforeExecution(
          IWorkflowEngine<WorkflowMeta> workflow, ActionMeta actionCopy, IAction action) {
        asyncRedraw();
      }

      @Override
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
  protected void workflowFinished() {
    // Do a final check to see if it all ended...
    //
    if (workflow != null && workflow.isInitialized() && workflow.isFinished()) {
      log.logBasic(
          BaseMessages.getString(PKG, "WorkflowLog.Log.WorkflowHasEnded", workflowMeta.getName()));
    }

    stopRedrawTimer();

    updateGui();
  }

  protected void workflowStopped() {
    if (workflow != null && workflow.isInitialized() && workflow.isStopped()) {
      log.logBasic(
          BaseMessages.getString(
              PKG, "WorkflowLog.Log.ProcessingOfWorkflowStopped", workflowMeta.getName()));
    }

    stopRedrawTimer();
    updateGui();
  }

  @Override
  public IHasLogChannel getLogChannelProvider() {
    return () -> getWorkflow() != null ? getWorkflow().getLogChannel() : LogChannel.GENERAL;
  }

  // Change of transform, connection, hop or note...
  public void addUndoPosition(Object[] obj, int[] pos, Point[] prev, Point[] curr) {
    addUndoPosition(obj, pos, prev, curr, false);
  }

  // Change of transform, connection, hop or note...
  public void addUndoPosition(
      Object[] obj, int[] pos, Point[] prev, Point[] curr, boolean nextAlso) {
    // It's better to store the indexes of the objects, not the objects itself!
    workflowMeta.addUndo(obj, null, pos, prev, curr, AbstractMeta.TYPE_UNDO_POSITION, nextAlso);
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
    if (lastChained != null && workflowMeta.findAction(lastChained.getName()) == null) {
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
    updateGui();

    if (shift) {
      editAction(newEntry);
    }

    workflowMeta.unselectAll();
    newEntry.setSelected(true);
    updateGui();
  }

  @GuiKeyboardShortcut(key = 'z')
  @GuiOsxKeyboardShortcut(key = 'z')
  public void openReferencedObject() {
    if (lastMove != null) {

      // Hide the tooltip!
      hideToolTips();

      // Find the transform
      ActionMeta action = workflowMeta.getAction(lastMove.x, lastMove.y, iconSize);
      if (action != null) {
        // Open referenced object...
        //
        IAction iAction = action.getAction();
        String[] objectDescriptions = iAction.getReferencedObjectDescriptions();
        if (objectDescriptions == null || objectDescriptions.length == 0) {
          return;
        }
        // Only one reference?: open immediately
        //
        if (objectDescriptions.length == 1) {
          HopGuiWorkflowActionContext.openReferencedObject(
              workflowMeta, variables, iAction, objectDescriptions[0], 0);
        } else {
          // Show Selection dialog...
          //
          EnterSelectionDialog dialog =
              new EnterSelectionDialog(
                  getShell(),
                  objectDescriptions,
                  BaseMessages.getString(
                      PKG, "HopGuiWorkflowGraph.OpenReferencedObject.Selection.Title"),
                  BaseMessages.getString(
                      PKG, "HopGuiWorkflowGraph.OpenReferencedObject.Selection.Message"));
          String answer = dialog.open(0);
          if (answer != null) {
            int index = dialog.getSelectionNr();
            HopGuiWorkflowActionContext.openReferencedObject(
                workflowMeta, variables, iAction, answer, index);
          }
        }
      }
    }
  }

  @Override
  public Object getSubject() {
    return workflowMeta;
  }

  @Override
  public ILogChannel getLogChannel() {
    return log;
  }

  // TODO
  public void editAction(WorkflowMeta workflowMeta, ActionMeta actionCopy) {
    // Do nothing
  }

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
   * Gets id
   *
   * @return value of id
   */
  @Override
  public String getId() {
    return id;
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

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return null;
  }

  @GuiContextAction(
      id = "workflow-graph-navigate-to-execution-info",
      parentId = HopGuiWorkflowContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::HopGuiWorkflowGraph.ContextualAction.NavigateToExecutionInfo.Text",
      tooltip = "i18n::HopGuiWorkflowGraph.ContextualAction.NavigateToExecutionInfo.Tooltip",
      image = "ui/images/execution.svg",
      category = "i18n::HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void navigateToExecutionInfo(HopGuiWorkflowContext context) {
    navigateToExecutionInfo();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_TO_EXECUTION_INFO,
      toolTip = "i18n:org.apache.hop.ui.hopgui:HopGui.Toolbar.ToExecutionInfo",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/execution.svg")
  public void navigateToExecutionInfo() {
    try {
      // Is there an active IWorkflow?
      //
      ExecutionPerspective executionPerspective = HopGui.getExecutionPerspective();

      if (workflow != null) {
        IExecutionViewer viewer =
            executionPerspective.findViewer(workflow.getLogChannelId(), workflowMeta.getName());
        if (viewer != null) {
          executionPerspective.setActiveViewer(viewer);
          executionPerspective.activate();
          return;
        } else {
          // We know the location, look it up
          //
          executionPerspective.refresh();

          // Get the location
          String locationName =
              variables.resolve(
                  workflow.getWorkflowRunConfiguration().getExecutionInfoLocationName());
          if (StringUtils.isNotEmpty(locationName)) {
            ExecutionInfoLocation location =
                executionPerspective.getLocationMap().get(locationName);
            IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();
            Execution execution = iLocation.getExecution(workflow.getLogChannelId());
            if (execution != null) {
              ExecutionState executionState = iLocation.getExecutionState(execution.getId());
              executionPerspective.createExecutionViewer(locationName, execution, executionState);
              executionPerspective.activate();
              return;
            }
          }
        }
      }

      MultiMetadataProvider metadataProvider = hopGui.getMetadataProvider();

      // As a fallback, try to open the last execution info for this workflow
      //
      IHopMetadataSerializer<ExecutionInfoLocation> serializer =
          metadataProvider.getSerializer(ExecutionInfoLocation.class);
      List<String> locationNames = serializer.listObjectNames();
      if (locationNames.isEmpty()) {
        return;
      }
      String locationName;
      if (locationNames.size() == 1) {
        // No need to ask which location, just pick this one
        locationName = locationNames.get(0);
      } else {
        EnterSelectionDialog dialog =
            new EnterSelectionDialog(
                getShell(),
                locationNames.toArray(new String[0]),
                "Select location",
                "Select the execution information location to query");
        locationName = dialog.open();
        if (locationName == null) {
          return;
        }
      }

      // This activates the perspective, refreshes elements.
      //
      executionPerspective.activate();

      // The refresh means the location is available over there now.
      //
      executionPerspective.createLastExecutionView(
          locationName, ExecutionType.Workflow, workflowMeta.getName());

    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          CONST_ERROR,
          "Error navigating to the latest execution information for this workflow",
          e);
    }
  }

  @Override
  public void reload() {
    try {
      workflowMeta.loadXml(hopGui.getVariables(), getFilename(), hopGui.getMetadataProvider());
    } catch (HopXmlException e) {
      LogChannel.GENERAL.logError("Error reloading workflow xml file", e);
    }
    redraw();
    updateGui();
  }

  @GuiContextAction(
      id = ACTION_ID_WORKFLOW_GRAPH_ACTION_VIEW_EXECUTION_INFO,
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Info,
      name = "i18n::HopGuiWorkflowGraph.ActionAction.ViewExecutionInfo.Name",
      tooltip = "i18n::HopGuiWorkflowGraph.ActionAction.ViewExecutionInfo.Tooltip",
      image = "ui/images/execution.svg",
      category = "Basic",
      categoryOrder = "1")
  public void viewActionExecutionInfo(HopGuiWorkflowActionContext context) {
    try {
      if (workflow == null) {
        return;
      }
      WorkflowRunConfiguration runConfiguration = workflow.getWorkflowRunConfiguration();
      String locationName = variables.resolve(runConfiguration.getExecutionInfoLocationName());
      if (StringUtils.isEmpty(locationName)) {
        return;
      }

      ExecutionPerspective executionPerspective = HopGui.getExecutionPerspective();
      executionPerspective.refresh();

      ExecutionInfoLocation location = executionPerspective.getLocationMap().get(locationName);
      if (location == null) {
        throw new HopException(
            "Unable to find execution information location '"
                + locationName
                + "' in the execution information perspective");
      }
      IExecutionInfoLocation iLocation = location.getExecutionInfoLocation();

      ActionMeta actionMeta = context.getActionMeta();
      WorkflowTracker<WorkflowMeta> workflowTracker = workflow.getWorkflowTracker();
      WorkflowTracker<WorkflowMeta> actionTracker = workflowTracker.findWorkflowTracker(actionMeta);
      if (actionTracker == null) {
        throw new HopException(
            "Unable to find the execution information for action '"
                + actionMeta
                + "'. "
                + "Has it been executed yet?");
      }
      ActionResult actionResult = actionTracker.getActionResult();
      if (actionResult == null) {
        throw new HopException("There is no action result yet for action '" + actionMeta + "'.");
      }
      String actionId = actionResult.getLogChannelId();

      List<Execution> executions = iLocation.findExecutions(actionId);
      if (!executions.isEmpty()) {
        Execution execution = executions.get(0);
        ExecutionState executionState = iLocation.getExecutionState(execution.getId());
        executionPerspective.createExecutionViewer(locationName, execution, executionState);
        executionPerspective.activate();
      }
    } catch (Exception e) {
      new ErrorDialog(getShell(), CONST_ERROR, "Error looking up execution information", e);
    }
  }

  @GuiContextActionFilter(parentId = HopGuiWorkflowActionContext.CONTEXT_ID)
  public boolean filterWorkflowAction(String contextActionId, HopGuiWorkflowActionContext context) {
    ActionMeta actionMeta = context.getActionMeta();

    if (contextActionId.equals(ACTION_ID_WORKFLOW_GRAPH_ACTION_VIEW_EXECUTION_INFO)) {
      if (workflow == null) {
        return false;
      }
      WorkflowRunConfiguration runConfiguration = workflow.getWorkflowRunConfiguration();
      String locationName = variables.resolve(runConfiguration.getExecutionInfoLocationName());
      if (StringUtils.isEmpty(locationName)) {
        return false;
      }

      WorkflowTracker<WorkflowMeta> workflowTracker = workflow.getWorkflowTracker();
      WorkflowTracker<WorkflowMeta> actionTracker = workflowTracker.findWorkflowTracker(actionMeta);
      if (actionTracker == null) {
        // Not executed yet, not started.
        return false;
      }
      ActionResult actionResult = actionTracker.getActionResult();
      // No execution information available yet (not started)
      return actionResult != null;
    }
    return true;
  }

  private void startRedrawTimer() {
    redrawTimer = new Timer("WorkflowGraph auto refresh: " + workflow.getWorkflowName());
    TimerTask timerTask =
        new TimerTask() {
          @Override
          public void run() {
            if (!hopDisplay().isDisposed()) {
              hopDisplay()
                  .asyncExec(
                      () -> {
                        if (!HopGuiWorkflowGraph.this.canvas.isDisposed()
                            && perspective.isActive()
                            && HopGuiWorkflowGraph.this.isVisible()) {
                          updateGui();
                        }
                      });
            }
          }
        };

    redrawTimer.schedule(timerTask, 0L, ConstUi.INTERVAL_MS_PIPELINE_CANVAS_REFRESH);
  }

  protected void stopRedrawTimer() {
    ExecutorUtil.cleanup(redrawTimer);
    redrawTimer = null;
  }
}
