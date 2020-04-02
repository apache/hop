// CHECKSTYLE:FileLength:OFF
/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.hopgui.file.job;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.EngineMetaInterface;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.Props;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.dnd.DragAndDropContainer;
import org.apache.hop.core.dnd.XMLTransfer;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.GCInterface;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.Redrawable;
import org.apache.hop.core.gui.SnapAllignDistribute;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.GuiOSXKeyboardShortcut;
import org.apache.hop.core.gui.plugin.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.IGuiRefresher;
import org.apache.hop.core.logging.HasLogChannelInterface;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.logging.LogParentProvidedInterface;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobAdapter;
import org.apache.hop.job.JobEntryListener;
import org.apache.hop.job.JobEntryResult;
import org.apache.hop.job.JobExecutionConfiguration;
import org.apache.hop.job.JobHopMeta;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.JobPainter;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.job.entry.JobEntryInterface;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePainter;
import org.apache.hop.ui.core.ConstUI;
import org.apache.hop.ui.core.PrintSpool;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.widget.CheckBoxToolTip;
import org.apache.hop.ui.core.widget.CheckBoxToolTipListener;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.GuiContextUtil;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.dialog.NotePadDialog;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypeInterface;
import org.apache.hop.ui.hopgui.file.delegates.HopGuiNotePadDelegate;
import org.apache.hop.ui.hopgui.file.job.context.HopGuiJobContext;
import org.apache.hop.ui.hopgui.file.job.context.HopGuiJobEntryContext;
import org.apache.hop.ui.hopgui.file.job.context.HopGuiJobNoteContext;
import org.apache.hop.ui.hopgui.file.job.delegates.HopGuiJobClipboardDelegate;
import org.apache.hop.ui.hopgui.file.job.delegates.HopGuiJobEntryDelegate;
import org.apache.hop.ui.hopgui.file.job.delegates.HopGuiJobGridDelegate;
import org.apache.hop.ui.hopgui.file.job.delegates.HopGuiJobHopDelegate;
import org.apache.hop.ui.hopgui.file.job.delegates.HopGuiJobLogDelegate;
import org.apache.hop.ui.hopgui.file.job.delegates.HopGuiJobMetricsDelegate;
import org.apache.hop.ui.hopgui.file.job.delegates.HopGuiJobRunDelegate;
import org.apache.hop.ui.hopgui.file.job.delegates.HopGuiJobUndoDelegate;
import org.apache.hop.ui.hopgui.file.job.extension.HopGuiJobGraphExtension;
import org.apache.hop.ui.hopgui.file.shared.DelayTimer;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopGuiAbstractGraph;
import org.apache.hop.ui.hopgui.shared.SWTGC;
import org.apache.hop.ui.hopgui.shared.SwtScrollBar;
import org.apache.hop.ui.job.dialog.JobDialog;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.dialogs.MessageDialogWithToggle;
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
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.printing.Printer;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Handles the display of Jobs in HopGui, in a graphical form.
 *
 * @author Matt Created on 17-may-2003
 */
public class HopGuiJobGraph extends HopGuiAbstractGraph
  implements Redrawable, MouseListener, MouseMoveListener, MouseTrackListener, MouseWheelListener, KeyListener,
  HasLogChannelInterface, LogParentProvidedInterface,
  HopFileTypeHandlerInterface,
  IGuiRefresher {

  private static Class<?> PKG = HopGuiJobGraph.class; // for i18n purposes, needed by Translator!!


  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGuiJobGraph-Toolbar";
  public static final String TOOLBAR_ITEM_START = "HopGuiJobGraph-ToolBar-10010-Run";
  public static final String TOOLBAR_ITEM_STOP = "HopGuiJobGraph-ToolBar-10030-Stop";

  public static final String TOOLBAR_ITEM_UNDO_ID = "HopGuiJobGraph-ToolBar-10100-Undo";
  public static final String TOOLBAR_ITEM_REDO_ID = "HopGuiJobGraph-ToolBar-10110-Redo";

  public static final String TOOLBAR_ITEM_SNAP_TO_GRID = "HopGuiJobGraph-ToolBar-10190-Snap-To-Grid";
  public static final String TOOLBAR_ITEM_ALIGN_LEFT = "HopGuiJobGraph-ToolBar-10200-Align-Left";
  public static final String TOOLBAR_ITEM_ALIGN_RIGHT = "HopGuiJobGraph-ToolBar-10210-Align-Right";
  public static final String TOOLBAR_ITEM_ALIGN_TOP = "HopGuiJobGraph-ToolBar-10250-Align-Ttop";
  public static final String TOOLBAR_ITEM_ALIGN_BOTTOM = "HopGuiJobGraph-ToolBar-10260-Align-Bottom";
  public static final String TOOLBAR_ITEM_DISTRIBUTE_HORIZONTALLY = "HopGuiJobGraph-ToolBar-10300-Distribute-Horizontally";
  public static final String TOOLBAR_ITEM_DISTRIBUTE_VERTICALLY = "HopGuiJobGraph-ToolBar-10310-Distribute-Vertically";

  public static final String TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS = "HopGuiJobGraph-ToolBar-10400-Execution-Results";

  public static final String START_TEXT = BaseMessages.getString( PKG, "JobLog.Button.Start" );
  public static final String STOP_TEXT = BaseMessages.getString( PKG, "JobLog.Button.Stop" );

  private static final String STRING_PARALLEL_WARNING_PARAMETER = "ParallelJobEntriesWarning";

  private static final int HOP_SEL_MARGIN = 9;
  private final HopDataOrchestrationPerspective perspective;

  protected LogChannelInterface log;

  protected JobMeta jobMeta;

  public Job job;

  protected PropsUI props;

  protected int iconsize;

  protected int linewidth;

  protected Point lastclick;

  protected List<JobEntryCopy> selectedEntries;

  protected JobEntryCopy selectedEntry;

  protected Point[] previousLocations;

  private List<NotePadMeta> selectedNotes;
  protected NotePadMeta selectedNote;

  protected Point previous_note_location;

  protected Point lastMove;

  protected JobHopMeta hop_candidate;

  protected Point drop_candidate;

  protected HopGui hopUi;

  // public boolean shift, control;
  protected boolean split_hop;

  protected int lastButton;

  protected JobHopMeta last_hop_split;

  protected org.apache.hop.core.gui.Rectangle selectionRegion;

  protected static final double theta = Math.toRadians( 10 ); // arrowhead sharpness

  protected static final int size = 30; // arrowhead length

  protected int shadowsize;

  protected int currentMouseX = 0;

  protected int currentMouseY = 0;

  protected JobEntryCopy jobEntry;

  protected NotePadMeta ni = null;

  protected JobHopMeta currentHop;

  // private Text filenameLabel;
  private SashForm sashForm;

  public Composite extraViewComposite;

  public CTabFolder extraViewTabFolder;

  private ToolBar toolBar;
  private GuiCompositeWidgets toolBarWidgets;

  private boolean running;
  private boolean halted;
  private boolean halting;
  private boolean pausing;

  public HopGuiJobLogDelegate jobLogDelegate;
  public HopGuiJobGridDelegate jobGridDelegate;
  public HopGuiJobMetricsDelegate jobMetricsDelegate;
  public HopGuiJobClipboardDelegate jobClipboardDelegate;
  public HopGuiJobRunDelegate jobRunDelegate;
  public HopGuiJobUndoDelegate jobUndoDelegate;
  public HopGuiJobEntryDelegate jobEntryDelegate;
  public HopGuiJobHopDelegate jobHopDelegate;
  public HopGuiNotePadDelegate notePadDelegate;

  private Composite mainComposite;

  private Label closeButton;

  private Label minMaxButton;

  private CheckBoxToolTip helpTip;

  private List<AreaOwner> areaOwners;

  /**
   * A map that keeps track of which log line was written by which job entry
   */
  private Map<JobEntryCopy, String> entryLogMap;

  private Map<JobEntryCopy, DelayTimer> delayTimers;

  private ToolItem stopItem;
  private Combo zoomLabel;

  private HopJobFileType fileType;


  private JobEntryCopy startHopEntry;
  private Point endHopLocation;

  private JobEntryCopy endHopEntry;
  private JobEntryCopy noInputEntry;
  private DefaultToolTip toolTip;
  private Point[] previous_transform_locations;
  private Point[] previous_note_locations;
  private JobEntryCopy currentEntry;

  public HopGuiJobGraph( Composite parent, final HopGui hopUi, final CTabItem parentTabItem,
                         final HopDataOrchestrationPerspective perspective, final JobMeta jobMeta, final HopJobFileType fileType ) {
    super( hopUi, parent, SWT.NONE, parentTabItem );
    this.perspective = perspective;
    this.jobMeta = jobMeta;
    this.fileType = fileType;

    this.log = hopUi.getLog();
    this.hopUi = hopUi;
    this.jobMeta = jobMeta;

    this.props = PropsUI.getInstance();
    this.areaOwners = new ArrayList<>();
    this.delayTimers = new HashMap<>();

    jobLogDelegate = new HopGuiJobLogDelegate( hopUi, this );
    jobGridDelegate = new HopGuiJobGridDelegate( hopUi, this );
    jobMetricsDelegate = new HopGuiJobMetricsDelegate( hopUi, this );
    jobClipboardDelegate = new HopGuiJobClipboardDelegate( hopUi, this );
    jobRunDelegate = new HopGuiJobRunDelegate( hopUi, this );
    jobUndoDelegate = new HopGuiJobUndoDelegate( hopUi, this );
    jobEntryDelegate = new HopGuiJobEntryDelegate( hopUi, this );
    jobHopDelegate = new HopGuiJobHopDelegate( hopUi, this );
    notePadDelegate = new HopGuiNotePadDelegate( hopUi, this );

    // TODO: ADD TOOLBAR

    setLayout( new FormLayout() );
    setLayoutData( new GridData( GridData.FILL_BOTH ) );

    // Add a tool-bar at the top of the tab
    // The form-data is set on the native widget automatically
    //
    addToolBar();

    // The main composite contains the graph view, but if needed also
    // a view with an extra tab containing log, etc.
    //
    mainComposite = new Composite( this, SWT.NONE );
    mainComposite.setLayout( new FillLayout() );

    FormData toolbarFd = new FormData();
    toolbarFd.left = new FormAttachment( 0, 0 );
    toolbarFd.right = new FormAttachment( 100, 0 );
    toolBar.setLayoutData( toolbarFd );

    // ------------------------

    FormData fdMainComposite = new FormData();
    fdMainComposite.left = new FormAttachment( 0, 0 );
    fdMainComposite.top = new FormAttachment( toolBar, 0 );
    fdMainComposite.right = new FormAttachment( 100, 0 );
    fdMainComposite.bottom = new FormAttachment( 100, 0 );
    mainComposite.setLayoutData( fdMainComposite );

    // To allow for a splitter later on, we will add the splitter here...
    //
    sashForm = new SashForm( mainComposite, SWT.VERTICAL );

    // Add a canvas below it, use up all space initially
    //
    canvas = new Canvas( sashForm, SWT.V_SCROLL | SWT.H_SCROLL | SWT.NO_BACKGROUND | SWT.BORDER );

    sashForm.setWeights( new int[] { 100, } );


    toolTip = new DefaultToolTip( canvas, ToolTip.NO_RECREATE, true );
    toolTip.setRespectMonitorBounds( true );
    toolTip.setRespectDisplayBounds( true );
    toolTip.setPopupDelay( 350 );
    toolTip.setShift( new org.eclipse.swt.graphics.Point( ConstUI.TOOLTIP_OFFSET, ConstUI.TOOLTIP_OFFSET ) );

    helpTip = new CheckBoxToolTip( canvas );
    helpTip.addCheckBoxToolTipListener( new CheckBoxToolTipListener() {

      public void checkBoxSelected( boolean enabled ) {
        hopUi.getProps().setShowingHelpToolTips( enabled );
      }
    } );

    newProps();

    selectionRegion = null;
    hop_candidate = null;
    last_hop_split = null;

    selectedEntries = null;
    selectedNote = null;

    hori = canvas.getHorizontalBar();
    vert = canvas.getVerticalBar();

    hori.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        redraw();
      }
    } );
    vert.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        redraw();
      }
    } );
    hori.setThumb( 100 );
    vert.setThumb( 100 );

    hori.setVisible( true );
    vert.setVisible( true );

    setVisible( true );

    canvas.addPaintListener( new PaintListener() {
      public void paintControl( PaintEvent e ) {
        HopGuiJobGraph.this.paintControl( e );
      }
    } );

    selectedEntries = null;
    lastclick = null;

    canvas.addMouseListener( this );
    canvas.addMouseMoveListener( this );
    canvas.addMouseTrackListener( this );
    canvas.addMouseWheelListener( this );

    // Drag & Drop for transforms
    Transfer[] ttypes = new Transfer[] { XMLTransfer.getInstance() };
    DropTarget ddTarget = new DropTarget( canvas, DND.DROP_MOVE );
    ddTarget.setTransfer( ttypes );
    ddTarget.addDropListener( new DropTargetListener() {
      public void dragEnter( DropTargetEvent event ) {
        drop_candidate = PropsUI.calculateGridPosition( getRealPosition( canvas, event.x, event.y ) );
        redraw();
      }

      public void dragLeave( DropTargetEvent event ) {
        drop_candidate = null;
        redraw();
      }

      public void dragOperationChanged( DropTargetEvent event ) {
      }

      public void dragOver( DropTargetEvent event ) {
        drop_candidate = PropsUI.calculateGridPosition( getRealPosition( canvas, event.x, event.y ) );
        redraw();
      }

      public void drop( DropTargetEvent event ) {
        // no data to copy, indicate failure in event.detail
        if ( event.data == null ) {
          event.detail = DND.DROP_NONE;
          return;
        }

        Point p = getRealPosition( canvas, event.x, event.y );

        try {
          DragAndDropContainer container = (DragAndDropContainer) event.data;
          String entry = container.getData();

          switch ( container.getType() ) {
             /*
            case DragAndDropContainer.TYPE_BASE_JOB_ENTRY: // Create a new Job Entry on the canvas

              JobEntryCopy jge = hopUi.newJobEntry( jobMeta, entry, false );
              if ( jge != null ) {
                PropsUI.setLocation( jge, p.x, p.y );
                jge.setDrawn();
                redraw();

                // See if we want to draw a tool tip explaining how to create new hops...
                //
                if ( jobMeta.nrJobEntries() > 1
                  && jobMeta.nrJobEntries() < 5 && hopUi.getProps().isShowingHelpToolTips() ) {
                  showHelpTip(
                    p.x, p.y, BaseMessages.getString( PKG, "JobGraph.HelpToolTip.CreatingHops.Title" ),
                    BaseMessages.getString( PKG, "JobGraph.HelpToolTip.CreatingHops.Message" ) );
                }
              }

              break;

            case DragAndDropContainer.TYPE_JOB_ENTRY: // Drag existing one onto the canvas
              jge = jobMeta.findJobEntry( entry, 0, true );
              if ( jge != null ) {
                // Create duplicate of existing entry

                // There can be only 1 start!
                if ( jge.isStart() && jge.isDrawn() ) {
                  showOnlyStartOnceMessage( shell );
                  return;
                }

                boolean jge_changed = false;

                // For undo :
                JobEntryCopy before = (JobEntryCopy) jge.clone_deep();

                JobEntryCopy newjge = jge;
                if ( jge.isDrawn() ) {
                  newjge = (JobEntryCopy) jge.clone();
                  if ( newjge != null ) {
                    // newjge.setEntry(jge.getEntry());
                    if ( log.isDebug() ) {
                      log.logDebug( "entry aft = " + ( (Object) jge.getEntry() ).toString() );
                    }

                    newjge.setNr( jobMeta.findUnusedNr( newjge.getName() ) );

                    jobMeta.addJobEntry( newjge );
                    hopUi.undoDelegate.addUndoNew( jobMeta, new JobEntryCopy[] { newjge }, new int[] { jobMeta
                      .indexOfJobEntry( newjge ) } );
                  } else {
                    if ( log.isDebug() ) {
                      log.logDebug( "jge is not cloned!" );
                    }
                  }
                } else {
                  if ( log.isDebug() ) {
                    log.logDebug( jge.toString() + " is not drawn" );
                  }
                  jge_changed = true;
                }
                PropsUI.setLocation( newjge, p.x, p.y );
                newjge.setDrawn();
                if ( jge_changed ) {
                  hopUi.undoDelegate.addUndoChange(
                    jobMeta, new JobEntryCopy[] { before }, new JobEntryCopy[] { newjge }, new int[] { jobMeta
                      .indexOfJobEntry( newjge ) } );
                }
                redraw();
                hopUi.refreshTree();
                log.logBasic( "DropTargetEvent", "DROP "
                  + newjge.toString() + "!, type=" + newjge.getEntry().getPluginId() );
              } else {
                log.logError( "Unknown job entry dropped onto the canvas." );
              }
            break;

            */

            default:
              break;
          }
        } catch ( Exception e ) {
          new ErrorDialog(
            hopShell(), BaseMessages.getString( PKG, "JobGraph.Dialog.ErrorDroppingObject.Message" ), BaseMessages
            .getString( PKG, "JobGraph.Dialog.ErrorDroppingObject.Title" ), e );
        }
      }

      public void dropAccept( DropTargetEvent event ) {
        drop_candidate = null;
      }
    } );

    canvas.addKeyListener( this );

    setBackground( GUIResource.getInstance().getColorBackground() );

    updateGui();
  }

  protected void hideToolTips() {
    toolTip.hide();
    helpTip.hide();
  }

  public void mouseDoubleClick( MouseEvent e ) {
    clearSettings();

    Point real = screen2real( e.x, e.y );

    // Hide the tooltip!
    hideToolTips();

    try {
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.JobGraphMouseDoubleClick.id, new HopGuiJobGraphExtension( this, e, real ) );
    } catch ( Exception ex ) {
      LogChannel.GENERAL.logError( "Error calling JobGraphMouseDoubleClick extension point", ex );
    }

    JobEntryCopy jobentry = jobMeta.getJobEntryCopy( real.x, real.y, iconsize );
    if ( jobentry != null ) {
      if ( e.button == 1 ) {
        editEntry( jobentry );
      } else {
        // open tab in HopGui
        launchStuff( jobentry );
      }
    } else {
      // Check if point lies on one of the many hop-lines...
      JobHopMeta online = findJobHop( real.x, real.y );
      if ( online == null ) {
        NotePadMeta ni = jobMeta.getNote( real.x, real.y );
        if ( ni != null ) {
          editNote( ni );
        } else {
          // Clicked on the background...
          //
          editJobProperties();
        }
      }

    }
  }

  public void mouseDown( MouseEvent e ) {

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
      ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, HopExtensionPoint.JobGraphMouseDown.id,
        new HopGuiJobGraphExtension( this, e, real ) );
    } catch ( Exception ex ) {
      LogChannel.GENERAL.logError( "Error calling JobGraphMouseDown extension point", ex );
    }

    // A single left or middle click on one of the area owners...
    //
    if ( e.button == 1 || e.button == 2 ) {
      AreaOwner areaOwner = getVisibleAreaOwner( real.x, real.y );
      if ( areaOwner != null && areaOwner.getAreaType() != null ) {
        switch ( areaOwner.getAreaType() ) {
          case JOB_ENTRY_MINI_ICON_OUTPUT:
            // Click on the output icon means: start of drag
            // Action: We show the input icons on the other transforms...
            //
            selectedEntry = null;
            startHopEntry = (JobEntryCopy) areaOwner.getOwner();
            // stopEntryMouseOverDelayTimer(startHopEntry);
            break;

          case JOB_ENTRY_MINI_ICON_INPUT:
            // Click on the input icon means: start to a new hop
            // In this case, we set the end hop transform...
            //
            selectedEntry = null;
            startHopEntry = null;
            endHopEntry = (JobEntryCopy) areaOwner.getOwner();
            // stopEntryMouseOverDelayTimer(endHopEntry);
            break;

          case JOB_ENTRY_MINI_ICON_EDIT:
            clearSettings();
            currentEntry = (JobEntryCopy) areaOwner.getOwner();
            stopEntryMouseOverDelayTimer( currentEntry );
            editEntry( currentEntry );
            break;

          case JOB_ENTRY_MINI_ICON_CONTEXT:
            clearSettings();
            JobEntryCopy jobEntryCopy = (JobEntryCopy) areaOwner.getOwner();
            setMenu( jobEntryCopy.getLocation().x, jobEntryCopy.getLocation().y );
            break;

          case JOB_ENTRY_ICON:
            jobEntryCopy = (JobEntryCopy) areaOwner.getOwner();
            currentEntry = jobEntryCopy;

            if ( hop_candidate != null ) {
              addCandidateAsHop();

            } else if ( e.button == 2 || ( e.button == 1 && shift ) ) {
              // SHIFT CLICK is start of drag to create a new hop
              //
              startHopEntry = jobEntryCopy;

            } else {
              selectedEntries = jobMeta.getSelectedEntries();
              selectedEntry = jobEntryCopy;
              //
              // When an icon is moved that is not selected, it gets
              // selected too late.
              // It is not captured here, but in the mouseMoveListener...
              //
              previous_transform_locations = jobMeta.getSelectedLocations();

              Point p = jobEntryCopy.getLocation();
              iconoffset = new Point( real.x - p.x, real.y - p.y );
            }
            updateGui();
            break;

          case NOTE:
            ni = (NotePadMeta) areaOwner.getOwner();
            selectedNotes = jobMeta.getSelectedNotes();
            selectedNote = ni;
            Point loc = ni.getLocation();

            previous_note_locations = jobMeta.getSelectedNoteLocations();

            noteoffset = new Point( real.x - loc.x, real.y - loc.y );

            updateGui();
            break;

          // If you click on an evaluating icon, change the evaluation...
          //
          case JOB_HOP_ICON:
            JobHopMeta hop = (JobHopMeta) areaOwner.getOwner();
            if ( hop.getFromEntry().evaluates() ) {
              if ( hop.isUnconditional() ) {
                hop.setUnconditional( false );
                hop.setEvaluation( true );
              } else {
                if ( hop.getEvaluation() ) {
                  hop.setEvaluation( false );
                } else {
                  hop.setUnconditional( true );
                }
              }
              updateGui();
            }
            break;
          default:
            break;
        }
      } else {
        // A hop? --> enable/disable
        //
        JobHopMeta hop = findJobHop( real.x, real.y );
        if ( hop != null ) {
          JobHopMeta before = (JobHopMeta) hop.clone();
          hop.setEnabled( !hop.isEnabled() );
          if ( hop.isEnabled() && ( jobMeta.hasLoop( hop.getToEntry() ) ) ) {
            MessageBox mb = new MessageBox( hopShell(), SWT.CANCEL | SWT.OK | SWT.ICON_WARNING );
            mb.setMessage( BaseMessages.getString( PKG, "JobGraph.Dialog.LoopAfterHopEnabled.Message" ) );
            mb.setText( BaseMessages.getString( PKG, "JobGraph.Dialog.LoopAfterHopEnabled.Title" ) );
            int choice = mb.open();
            if ( choice == SWT.CANCEL ) {
              hop.setEnabled( false );
            }
          }
          JobHopMeta after = (JobHopMeta) hop.clone();
          hopUi.undoDelegate.addUndoChange( jobMeta, new JobHopMeta[] { before }, new JobHopMeta[] { after }, new int[] { jobMeta.indexOfJobHop( hop ) } );

          updateGui();
        } else {
          // No area-owner means: background:
          //
          startHopEntry = null;
          if ( !control ) {
            selectionRegion = new org.apache.hop.core.gui.Rectangle( real.x, real.y, 0, 0 );
          }
          updateGui();
        }
      }
    }
  }

  private enum SingleClickType {
    Job,
    Entry,
    Note
  }

  public void mouseUp( MouseEvent e ) {
    boolean control = ( e.stateMask & SWT.MOD1 ) != 0;

    boolean singleClick = false;
    HopGuiJobGraph.SingleClickType singleClickType = null;
    JobEntryCopy singleClickEntry = null;
    NotePadMeta singleClickNote = null;

    if ( iconoffset == null ) {
      iconoffset = new Point( 0, 0 );
    }
    Point real = screen2real( e.x, e.y );
    Point icon = new Point( real.x - iconoffset.x, real.y - iconoffset.y );
    AreaOwner areaOwner = getVisibleAreaOwner( real.x, real.y );

    // Quick new hop option? (drag from one transform to another)
    //
    if ( hop_candidate != null && areaOwner != null && areaOwner.getAreaType() != null ) {
      switch ( areaOwner.getAreaType() ) {
        case JOB_ENTRY_ICON:
          currentEntry = (JobEntryCopy) areaOwner.getOwner();
          break;
        default:
          break;
      }
      addCandidateAsHop();
      redraw();
    } else {
      // Did we select a region on the screen? Mark entries in region as selected
      //
      if ( selectionRegion != null ) {
        selectionRegion.width = real.x - selectionRegion.x;
        selectionRegion.height = real.y - selectionRegion.y;

        if ( selectionRegion.width == 0 && selectionRegion.height == 0 ) {
          singleClick = true;
          singleClickType = HopGuiJobGraph.SingleClickType.Job;
        }
        jobMeta.unselectAll();
        selectInRect( jobMeta, selectionRegion );
        selectionRegion = null;
        stopEntryMouseOverDelayTimers();
        redraw();
      } else {
        // Clicked on an icon?
        //
        if ( selectedEntry != null && startHopEntry == null ) {
          if ( e.button == 1 ) {
            Point realclick = screen2real( e.x, e.y );
            if ( lastclick.x == realclick.x && lastclick.y == realclick.y ) {
              // Flip selection when control is pressed!
              if ( control ) {
                selectedEntry.flipSelected();
              } else {
                singleClick = true;
                singleClickType = SingleClickType.Entry;
                singleClickEntry = selectedEntry;
              }
            } else {
              // Find out which Transforms & Notes are selected
              selectedEntries = jobMeta.getSelectedEntries();
              selectedNotes = jobMeta.getSelectedNotes();

              // We moved around some items: store undo info...
              //
              boolean also = false;
              if ( selectedNotes != null && selectedNotes.size() > 0 && previous_note_locations != null ) {
                int[] indexes = jobMeta.getNoteIndexes( selectedNotes );

                addUndoPosition(
                  selectedNotes.toArray( new NotePadMeta[ selectedNotes.size() ] ), indexes,
                  previous_note_locations, jobMeta.getSelectedNoteLocations(), also );
                also = selectedEntries != null && selectedEntries.size() > 0;
              }
              if ( selectedEntries != null && selectedEntries.size() > 0 && previous_transform_locations != null ) {
                int[] indexes = jobMeta.getEntryIndexes( selectedEntries );
                addUndoPosition(
                  selectedEntries.toArray( new JobEntryCopy[ selectedEntries.size() ] ), indexes,
                  previous_transform_locations, jobMeta.getSelectedLocations(), also );
              }
            }
          }

          // OK, we moved the transform, did we move it across a hop?
          // If so, ask to split the hop!
          if ( split_hop ) {
            JobHopMeta hi = findHop( icon.x + iconsize / 2, icon.y + iconsize / 2, selectedEntry );
            if ( hi != null ) {
              int id = 0;
              if ( !hopUi.getProps().getAutoSplit() ) {
                MessageDialogWithToggle md =
                  new MessageDialogWithToggle(
                    hopShell(),
                    BaseMessages.getString( PKG, "PipelineGraph.Dialog.SplitHop.Title" ),
                    null,
                    BaseMessages.getString( PKG, "PipelineGraph.Dialog.SplitHop.Message" )
                      + Const.CR + hi.toString(),
                    MessageDialog.QUESTION,
                    new String[] {
                      BaseMessages.getString( PKG, "System.Button.Yes" ),
                      BaseMessages.getString( PKG, "System.Button.No" ) },
                    0,
                    BaseMessages.getString( PKG, "PipelineGraph.Dialog.Option.SplitHop.DoNotAskAgain" ),
                    hopUi.getProps().getAutoSplit() );
                MessageDialogWithToggle.setDefaultImage( GUIResource.getInstance().getImageHopUi() );
                id = md.open();
                hopUi.getProps().setAutoSplit( md.getToggleState() );
              }

              if ( ( id & 0xFF ) == 0 ) {
                // Means: "Yes" button clicked!

                // Only split A-->--B by putting C in between IF...
                // C-->--A or B-->--C don't exists...
                // A ==> hi.getFromEntry()
                // B ==> hi.getToEntry();
                // C ==> selectedTransform
                //
                if ( jobMeta.findJobHop( selectedEntry, hi.getFromEntry() ) == null
                  && jobMeta.findJobHop( hi.getToEntry(), selectedEntry ) == null ) {

                  if ( jobMeta.findJobHop( hi.getFromEntry(), selectedEntry, true ) == null ) {
                    JobHopMeta newhop1 = new JobHopMeta( hi.getFromEntry(), selectedEntry );
                    if ( hi.getFromEntry().getEntry().isUnconditional() ) {
                      newhop1.setUnconditional();
                    }
                    jobMeta.addJobHop( newhop1 );
                    hopUi.undoDelegate.addUndoNew( jobMeta, new JobHopMeta[] { newhop1, }, new int[] { jobMeta.indexOfJobHop( newhop1 ), }, true );
                  }
                  if ( jobMeta.findJobHop( selectedEntry, hi.getToEntry(), true ) == null ) {
                    JobHopMeta newhop2 = new JobHopMeta( selectedEntry, hi.getToEntry() );
                    if ( selectedEntry.getEntry().isUnconditional() ) {
                      newhop2.setUnconditional();
                    }
                    jobMeta.addJobHop( newhop2 );
                    hopUi.undoDelegate.addUndoNew( jobMeta, new JobHopMeta[] { newhop2, }, new int[] { jobMeta.indexOfJobHop( newhop2 ), }, true );
                  }

                  int idx = jobMeta.indexOfJobHop( hi );
                  hopUi.undoDelegate.addUndoDelete( jobMeta, new JobHopMeta[] { hi }, new int[] { idx }, true );
                  jobMeta.removeJobHop( idx );
                }
                // else: Silently discard this hop-split attempt.
              }
            }
            split_hop = false;
          }

          selectedEntries = null;
          selectedNotes = null;
          selectedEntry = null;
          selectedNote = null;
          startHopEntry = null;
          endHopLocation = null;

          updateGui();
        } else {
          // Notes?
          if ( selectedNote != null ) {
            if ( e.button == 1 ) {
              if ( lastclick.x == e.x && lastclick.y == e.y ) {
                // Flip selection when control is pressed!
                if ( control ) {
                  selectedNote.flipSelected();
                } else {
                  // single click on a note: ask what needs to happen...
                  //
                  singleClick = true;
                  singleClickType = HopGuiJobGraph.SingleClickType.Note;
                  singleClickNote = selectedNote;
                }
              } else {
                // Find out which Transforms & Notes are selected
                selectedEntries = jobMeta.getSelectedEntries();
                selectedNotes = jobMeta.getSelectedNotes();

                // We moved around some items: store undo info...
                boolean also = false;
                if ( selectedNotes != null && selectedNotes.size() > 0 && previous_note_locations != null ) {
                  int[] indexes = jobMeta.getNoteIndexes( selectedNotes );
                  addUndoPosition(
                    selectedNotes.toArray( new NotePadMeta[ selectedNotes.size() ] ), indexes,
                    previous_note_locations, jobMeta.getSelectedNoteLocations(), also );
                  also = selectedEntries != null && selectedEntries.size() > 0;
                }
                if ( selectedEntries != null && selectedEntries.size() > 0 && previous_transform_locations != null ) {
                  int[] indexes = jobMeta.getEntryIndexes( selectedEntries );
                  addUndoPosition(
                    selectedEntries.toArray( new JobEntryCopy[ selectedEntries.size() ] ), indexes,
                    previous_transform_locations, jobMeta.getSelectedLocations(), also );
                }
              }
            }

            selectedNotes = null;
            selectedEntries = null;
            selectedEntry = null;
            selectedNote = null;
            startHopEntry = null;
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
        case Job:
          message = "Select the action to execute or the job entry to create:";
          contextHandler = new HopGuiJobContext( jobMeta, this, real );
          break;
        case Entry:
          message = "Select the action to take on job entry '" + singleClickEntry.getName() + "':";
          contextHandler = new HopGuiJobEntryContext( jobMeta, singleClickEntry, this, real );
          break;
        case Note:
          message = "Select the note action to take:";
          contextHandler = new HopGuiJobNoteContext( jobMeta, singleClickNote, this, real );
          break;
        default:
          break;
      }
      if ( contextHandler != null ) {
        Shell parent = hopShell();
        org.eclipse.swt.graphics.Point p = parent.getDisplay().map( canvas, null, e.x, e.y );
        GuiContextUtil.handleActionSelection( parent, message, new Point( p.x, p.y ), contextHandler.getSupportedActions() );
      }
    }

    lastButton = 0;
  }

  public void mouseMove( MouseEvent e ) {
    boolean shift = ( e.stateMask & SWT.SHIFT ) != 0;
    noInputEntry = null;

    // disable the tooltip
    //
    toolTip.hide();

    Point real = screen2real( e.x, e.y );
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
      JobEntryCopy jobEntryCopy = null;
      switch ( areaOwner.getAreaType() ) {
        case JOB_ENTRY_ICON:
          jobEntryCopy = (JobEntryCopy) areaOwner.getOwner();
          resetDelayTimer( jobEntryCopy );
          break;
        case MINI_ICONS_BALLOON: // Give the timer a bit more time
          jobEntryCopy = (JobEntryCopy) areaOwner.getOwner();
          resetDelayTimer( jobEntryCopy );
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
    if ( selectedEntry != null && !selectedEntry.isSelected() ) {
      jobMeta.unselectAll();
      selectedEntry.setSelected( true );
      selectedEntries = new ArrayList<>();
      selectedEntries.add( selectedEntry );
      previous_transform_locations = new Point[] { selectedEntry.getLocation() };
      redraw();
    } else if ( selectedNote != null && !selectedNote.isSelected() ) {
      jobMeta.unselectAll();
      selectedNote.setSelected( true );
      selectedNotes = new ArrayList<>();
      selectedNotes.add( selectedNote );
      previous_note_locations = new Point[] { selectedNote.getLocation() };
      redraw();
    } else if ( selectionRegion != null && startHopEntry == null ) {
      // Did we select a region...?
      //
      selectionRegion.width = real.x - selectionRegion.x;
      selectionRegion.height = real.y - selectionRegion.y;
      redraw();
    } else if ( selectedEntry != null && lastButton == 1 && !shift && startHopEntry == null ) {
      // Move around transforms & notes
      //
      //
      // One or more icons are selected and moved around...
      //
      // new : new position of the ICON (not the mouse pointer) dx : difference with previous position
      //
      int dx = icon.x - selectedEntry.getLocation().x;
      int dy = icon.y - selectedEntry.getLocation().y;

      // See if we have a hop-split candidate
      //
      JobHopMeta hi = findHop( icon.x + iconsize / 2, icon.y + iconsize / 2, selectedEntry );
      if ( hi != null ) {
        // OK, we want to split the hop in 2
        //
        if ( !hi.getFromEntry().equals( selectedEntry ) && !hi.getToEntry().equals( selectedEntry ) ) {
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

      selectedNotes = jobMeta.getSelectedNotes();
      selectedEntries = jobMeta.getSelectedEntries();

      // Adjust location of selected transforms...
      if ( selectedEntries != null ) {
        for ( int i = 0; i < selectedEntries.size(); i++ ) {
          JobEntryCopy jobEntryCopy = selectedEntries.get( i );
          PropsUI.setLocation( jobEntryCopy, jobEntryCopy.getLocation().x + dx, jobEntryCopy.getLocation().y + dy );
          stopEntryMouseOverDelayTimer( jobEntryCopy );
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
    } else if ( ( startHopEntry != null && endHopEntry == null )
      || ( endHopEntry != null && startHopEntry == null ) ) {
      // Are we creating a new hop with the middle button or pressing SHIFT?
      //

      JobEntryCopy jobEntryCopy = jobMeta.getJobEntryCopy( real.x, real.y, iconsize );
      endHopLocation = new Point( real.x, real.y );
      if ( jobEntryCopy != null
        && ( ( startHopEntry != null && !startHopEntry.equals( jobEntryCopy ) ) || ( endHopEntry != null && !endHopEntry
        .equals( jobEntryCopy ) ) ) ) {
        if ( hop_candidate == null ) {
          // See if the transform accepts input. If not, we can't create a new hop...
          //
          if ( startHopEntry != null ) {
            if ( !jobEntryCopy.isStart() ) {
              hop_candidate = new JobHopMeta( startHopEntry, jobEntryCopy );
              endHopLocation = null;
            } else {
              noInputEntry = jobEntryCopy;
              toolTip.setImage( null );
              toolTip.setText( "The start entry can only be used at the start of a Job" );
              toolTip.show( new org.eclipse.swt.graphics.Point( real.x, real.y ) );
            }
          } else if ( endHopEntry != null ) {
            hop_candidate = new JobHopMeta( jobEntryCopy, endHopEntry );
            endHopLocation = null;
          }
        }
      } else {
        if ( hop_candidate != null ) {
          hop_candidate = null;
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

        selectedNotes = jobMeta.getSelectedNotes();
        selectedEntries = jobMeta.getSelectedEntries();

        // Adjust location of selected transforms...
        if ( selectedEntries != null ) {
          for ( int i = 0; i < selectedEntries.size(); i++ ) {
            JobEntryCopy jobEntryCopy = selectedEntries.get( i );
            PropsUI.setLocation( jobEntryCopy, jobEntryCopy.getLocation().x + dx, jobEntryCopy.getLocation().y
              + dy );
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

  public void mouseHover( MouseEvent e ) {

    boolean tip = true;
    boolean isDeprecated = false;

    // toolTip.hide();
    Point real = screen2real( e.x, e.y );

    AreaOwner areaOwner = getVisibleAreaOwner( real.x, real.y );
    if ( areaOwner != null && areaOwner.getAreaType() != null ) {
      switch ( areaOwner.getAreaType() ) {
        case JOB_ENTRY_ICON:
        default:
          break;
      }
    }

    // Show a tool tip upon mouse-over of an object on the canvas
    if ( ( tip && !helpTip.isVisible() ) || isDeprecated ) {
      setToolTip( real.x, real.y, e.x, e.y );
    }
  }

  public void mouseEnter( MouseEvent event ) {
  }

  public void mouseExit( MouseEvent event ) {
  }

  public void mouseScrolled( MouseEvent e ) {
    /*
     * if (e.count == 3) { // scroll up zoomIn(); } else if (e.count == -3) { // scroll down zoomOut(); }
     */
  }

  private void addCandidateAsHop() {
    if ( hop_candidate != null ) {

      if ( !hop_candidate.getFromEntry().evaluates() && hop_candidate.getFromEntry().isUnconditional() ) {
        hop_candidate.setUnconditional();
      } else {
        hop_candidate.setConditional();
        int nr = jobMeta.findNrNextJobEntries( hop_candidate.getFromEntry() );

        // If there is one green link: make this one red! (or
        // vice-versa)
        if ( nr == 1 ) {
          JobEntryCopy jge = jobMeta.findNextJobEntry( hop_candidate.getFromEntry(), 0 );
          JobHopMeta other = jobMeta.findJobHop( hop_candidate.getFromEntry(), jge );
          if ( other != null ) {
            hop_candidate.setEvaluation( !other.getEvaluation() );
          }
        }
      }

      if ( checkIfHopAlreadyExists( jobMeta, hop_candidate ) ) {
        boolean cancel = false;
        jobMeta.addJobHop( hop_candidate );
        if ( jobMeta.hasLoop( hop_candidate.getToEntry() ) ) {
          MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.CANCEL | SWT.ICON_WARNING );
          mb.setMessage( BaseMessages.getString( PKG, "JobGraph.Dialog.HopCausesLoop.Message" ) );
          mb.setText( BaseMessages.getString( PKG, "JobGraph.Dialog.HopCausesLoop.Title" ) );
          int choice = mb.open();
          if ( choice == SWT.CANCEL ) {
            jobMeta.removeJobHop( hop_candidate );
            cancel = true;
          }
        }
        if ( !cancel ) {
          hopUi.undoDelegate.addUndoNew( jobMeta, new JobHopMeta[] { hop_candidate }, new int[] { jobMeta
            .indexOfJobHop( hop_candidate ) } );
        }
        clearSettings();
        redraw();
      }
    }
  }

  public boolean checkIfHopAlreadyExists( JobMeta jobMeta, JobHopMeta newHop ) {
    boolean ok = true;
    if ( jobMeta.findJobHop( newHop.getFromEntry(), newHop.getToEntry(), true ) != null ) {
      MessageBox mb = new MessageBox( hopShell(), SWT.OK | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "JobGraph.Dialog.HopExists.Message" ) ); // "This hop already exists!"
      mb.setText( BaseMessages.getString( PKG, "JobGraph.Dialog.HopExists.Title" ) ); // Error!
      mb.open();
      ok = false;
    }

    return ok;
  }

  public AreaOwner getVisibleAreaOwner( int x, int y ) {
    for ( int i = areaOwners.size() - 1; i >= 0; i-- ) {
      AreaOwner areaOwner = areaOwners.get( i );
      if ( areaOwner.contains( x, y ) ) {
        return areaOwner;
      }
    }
    return null;
  }

  private void stopEntryMouseOverDelayTimer( final JobEntryCopy jobEntryCopy ) {
    DelayTimer delayTimer = delayTimers.get( jobEntryCopy );
    if ( delayTimer != null ) {
      delayTimer.stop();
    }
  }

  private void stopEntryMouseOverDelayTimers() {
    for ( DelayTimer timer : delayTimers.values() ) {
      timer.stop();
    }
  }

  private void resetDelayTimer( JobEntryCopy jobEntryCopy ) {
    DelayTimer delayTimer = delayTimers.get( jobEntryCopy );
    if ( delayTimer != null ) {
      delayTimer.reset();
    }
  }

  protected void asyncRedraw() {
    hopUi.getDisplay().asyncExec( new Runnable() {
      public void run() {
        if ( !isDisposed() ) {
          redraw();
        }
      }
    } );
  }

  private void addToolBar() {

    try {
      // Create a new toolbar at the top of the main composite...
      //
      toolBar = new ToolBar( this, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL );
      toolBarWidgets = new GuiCompositeWidgets( HopGui.getInstance().getVariableSpace() );
      toolBarWidgets.createCompositeWidgets( this, null, toolBar, GUI_PLUGIN_TOOLBAR_PARENT_ID, null );
      FormData layoutData = new FormData();
      layoutData.left = new FormAttachment( 0, 0 );
      layoutData.top = new FormAttachment( 0, 0 );
      layoutData.right = new FormAttachment( 100, 0 );
      toolBar.setLayoutData( layoutData );
      toolBar.pack();

      // Add a zoom label widget: TODO: move to GuiElement
      //
      new ToolItem( toolBar, SWT.SEPARATOR );
      ToolItem sep = new ToolItem( toolBar, SWT.SEPARATOR );

      zoomLabel = new Combo( toolBar, SWT.DROP_DOWN );
      zoomLabel.setItems( PipelinePainter.magnificationDescriptions );
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
      updateGui();

    } catch ( Throwable t ) {
      log.logError( "Error setting up the navigation toolbar for HopUI", t );
      new ErrorDialog( hopShell(), "Error", "Error setting up the navigation toolbar for HopGUI", new Exception( t ) );
    }
  }

  public void setZoomLabel() {
    String newString = Integer.toString( Math.round( magnification * 100 ) ) + "%";
    String oldString = zoomLabel.getText();
    if ( !newString.equals( oldString ) ) {
      zoomLabel.setText( Integer.toString( Math.round( magnification * 100 ) ) + "%" );
    }
  }

  @GuiToolbarElement(
    type = GuiElementType.TOOLBAR_BUTTON,
    id = TOOLBAR_ITEM_START,
    label = "Start",
    toolTip = "Start the execution of the pipeline",
    image = "ui/images/toolbar/run.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  @Override
  public void start() {
    jobMeta.setShowDialog( jobMeta.isAlwaysShowRunOptions() );
    Thread thread = new Thread() {
      @Override
      public void run() {
        getDisplay().asyncExec( new Runnable() {
          @Override
          public void run() {
            try {
              jobRunDelegate.executeJob( jobMeta, true, false, false, null, 0 );
            } catch ( Exception e ) {
              new ErrorDialog( getShell(), "Execute job", "There was an error during job execution", e );
            }
          }
        } );
      }
    };
    thread.start();
  }

  @GuiToolbarElement(
    id = TOOLBAR_ITEM_STOP,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "Stop",
    toolTip = "Stop the execution of the job",
    image = "ui/images/toolbar/stop.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  @Override
  public void stop() {

    if ( ( running && !halting ) ) {
      halting = true;
      job.stopAll();
      log.logMinimal( BaseMessages.getString( PKG, "JobLog.Log.ProcessingOfJobStopped" ) );

      running = false;
      halted = false;
      halting = false;

      updateGui();

      jobMeta.setInternalHopVariables(); // set the original vars back as they may be changed by a mapping
    }
  }

  @Override public void pause() {
    // TODO: Implement on a job level
  }

  @Override public void preview() {
    // Not possible for jobs
  }

  @Override public void debug() {
    // Not possible for jobs (yet)
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
      MessageBox mb = new MessageBox( hopShell(), SWT.YES | SWT.ICON_ERROR );
      mb.setMessage( BaseMessages.getString( PKG, "PipelineGraph.Dialog.InvalidZoomMeasurement.Message", zoomLabel
        .getText() ) );
      mb.setText( BaseMessages.getString( PKG, "PipelineGraph.Dialog.InvalidZoomMeasurement.Title" ) );
      mb.open();
    }
    redraw();
  }

  public void keyPressed( KeyEvent e ) {

    // Delete
    if ( e.keyCode == SWT.DEL ) {
      List<JobEntryCopy> copies = jobMeta.getSelectedEntries();
      if ( copies != null && copies.size() > 0 ) {
        delSelected();
      }
    }

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
      snapToGrid( ConstUI.GRID_SIZE );
    }
    // CTRL-W or CTRL-F4 : close tab
    if ( ( e.keyCode == 'w' && ( e.stateMask & SWT.MOD1 ) != 0 )
      || ( e.keyCode == SWT.F4 && ( e.stateMask & SWT.MOD1 ) != 0 ) ) {
      close();
    }
  }

  public void keyReleased( KeyEvent e ) {
  }

  public void selectInRect( JobMeta jobMeta, org.apache.hop.core.gui.Rectangle rect ) {
    int i;
    for ( i = 0; i < jobMeta.nrJobEntries(); i++ ) {
      JobEntryCopy je = jobMeta.getJobEntry( i );
      Point p = je.getLocation();
      if ( ( ( p.x >= rect.x && p.x <= rect.x + rect.width ) || ( p.x >= rect.x + rect.width && p.x <= rect.x ) )
        && ( ( p.y >= rect.y && p.y <= rect.y + rect.height ) || ( p.y >= rect.y + rect.height && p.y <= rect.y ) ) ) {
        je.setSelected( true );
      }
    }
    for ( i = 0; i < jobMeta.nrNotes(); i++ ) {
      NotePadMeta ni = jobMeta.getNote( i );
      Point a = ni.getLocation();
      Point b = new Point( a.x + ni.width, a.y + ni.height );
      if ( rect.contains( a.x, a.y ) && rect.contains( b.x, b.y ) ) {
        ni.setSelected( true );
      }
    }
  }

  public boolean setFocus() {
    return ( canvas != null && !canvas.isDisposed() ) ? canvas.setFocus() : false;
  }

  /**
   * Method gets called, when the user wants to change a job entries name and he indeed entered a different name then
   * the old one. Make sure that no other job entry matches this name and rename in case of uniqueness.
   *
   * @param jobEntry
   * @param newName
   */
  public void renameJobEntry( JobEntryCopy jobEntry, String newName ) {
    JobEntryCopy[] jobs = jobMeta.getAllJobGraphEntries( newName );
    if ( jobs != null && jobs.length > 0 ) {
      MessageBox mb = new MessageBox( hopShell(), SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.JobEntryNameExists.Message", newName ) );
      mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.JobEntryNameExists.Title" ) );
      mb.open();
    } else {
      jobEntry.setName( newName );
      jobEntry.setChanged();
      redraw();
    }
  }

  public static void showOnlyStartOnceMessage( Shell shell ) {
    MessageBox mb = new MessageBox( shell, SWT.YES | SWT.ICON_ERROR );
    mb.setMessage( BaseMessages.getString( PKG, "JobGraph.Dialog.OnlyUseStartOnce.Message" ) );
    mb.setText( BaseMessages.getString( PKG, "JobGraph.Dialog.OnlyUseStartOnce.Title" ) );
    mb.open();
  }

  public void delSelected() {
    delSelected( getJobEntry() );
  }

  public void delSelected( JobEntryCopy clickedEntry ) {
    List<JobEntryCopy> copies = jobMeta.getSelectedEntries();
    int nrsels = copies.size();
    if ( nrsels == 0 ) {
      if ( clickedEntry != null ) {
        jobEntryDelegate.deleteJobEntryCopies( jobMeta, clickedEntry );
      }
      return;
    }

    jobEntryDelegate.deleteJobEntryCopies( jobMeta, copies );
  }

  public void clearSettings() {
    selectedEntry = null;
    selectedNote = null;
    selectedEntries = null;
    selectedNotes = null;
    selectionRegion = null;
    hop_candidate = null;
    last_hop_split = null;
    lastButton = 0;
    startHopEntry = null;
    endHopEntry = null;
    iconoffset = null;
    jobMeta.unselectAll();
    for ( int i = 0; i < jobMeta.nrJobHops(); i++ ) {
      jobMeta.getJobHop( i ).setSplit( false );
    }

    stopEntryMouseOverDelayTimers();
  }

  public Point getRealPosition( Composite canvas, int x, int y ) {
    Point p = new Point( 0, 0 );
    Composite follow = canvas;
    while ( follow != null ) {
      Point xy = new Point( follow.getLocation().x, follow.getLocation().y );
      p.x += xy.x;
      p.y += xy.y;
      follow = follow.getParent();
    }

    p.x = x - p.x - 8;
    p.y = y - p.y - 48;

    return screen2real( p.x, p.y );
  }

  /**
   * See if location (x,y) is on a line between two transforms: the hop!
   *
   * @param x
   * @param y
   * @return the pipeline hop on the specified location, otherwise: null
   */
  private JobHopMeta findJobHop( int x, int y ) {
    return findHop( x, y, null );
  }

  /**
   * See if location (x,y) is on a line between two transforms: the hop!
   *
   * @param x
   * @param y
   * @param exclude the transform to exclude from the hops (from or to location). Specify null if no transform is to be excluded.
   * @return the pipeline hop on the specified location, otherwise: null
   */
  private JobHopMeta findHop( int x, int y, JobEntryCopy exclude ) {
    int i;
    JobHopMeta online = null;
    for ( i = 0; i < jobMeta.nrJobHops(); i++ ) {
      JobHopMeta hi = jobMeta.getJobHop( i );
      JobEntryCopy fs = hi.getFromEntry();
      JobEntryCopy ts = hi.getToEntry();

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

  protected int[] getLine( JobEntryCopy fs, JobEntryCopy ts ) {
    if ( fs == null || ts == null ) {
      return null;
    }

    Point from = fs.getLocation();
    Point to = ts.getLocation();
    offset = getOffset();

    int x1 = from.x + iconsize / 2;
    int y1 = from.y + iconsize / 2;

    int x2 = to.x + iconsize / 2;
    int y2 = to.y + iconsize / 2;

    return new int[] { x1, y1, x2, y2 };
  }

  private void showHelpTip( int x, int y, String tipTitle, String tipMessage ) {

    helpTip.setTitle( tipTitle );
    helpTip.setMessage( tipMessage );
    helpTip.setCheckBoxMessage( BaseMessages.getString(
      PKG, "JobGraph.HelpToolTip.DoNotShowAnyMoreCheckBox.Message" ) );
    // helpTip.hide();
    // int iconSize = spoon.props.getIconSize();
    org.eclipse.swt.graphics.Point location = new org.eclipse.swt.graphics.Point( x - 5, y - 5 );

    helpTip.show( location );
  }

  public void setJobEntry( JobEntryCopy jobEntry ) {
    this.jobEntry = jobEntry;
  }

  public JobEntryCopy getJobEntry() {
    return jobEntry;
  }

  @GuiContextAction(
    id = "jobgraph-entry-10050-create-hop",
    parentId = HopGuiJobEntryContext.CONTEXT_ID,
    type = GuiActionType.Create,
    name = "Create hop",
    tooltip = "Create a new hop between 2 entries",
    image = "ui/images/HOP.svg"
  )
  public void newHopCandidate( HopGuiJobEntryContext context ) {
    startHopEntry = context.getJobEntryCopy();
    endHopEntry = null;
    redraw();
  }

  public void newHopClick() {
    selectedEntries = null;
    newHop();
  }

  public void editEntryClick() {
    selectedEntries = null;
    editEntry( getJobEntry() );
  }

  @GuiContextAction(
    id = "jobgraph-entry-10800-edit-description",
    parentId = HopGuiJobEntryContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Edit entry description",
    tooltip = "Modify the job entry description",
    image = "ui/images/Edit.svg"
  )
  public void editEntryDescription( HopGuiJobEntryContext context ) {
    String title = BaseMessages.getString( PKG, "JobGraph.Dialog.EditDescription.Title" );
    String message = BaseMessages.getString( PKG, "JobGraph.Dialog.EditDescription.Message" );
    EnterTextDialog dd = new EnterTextDialog( hopShell(), title, message, context.getJobEntryCopy().getDescription() );
    String des = dd.open();
    if ( des != null ) {
      jobEntry.setDescription( des );
      jobEntry.setChanged();
      updateGui();
    }
  }

  /**
   * Go from serial to parallel to serial execution
   */
  @GuiContextAction(
    id = "jobgraph-transform-10600-parallel",
    parentId = HopGuiJobEntryContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Parallel execution",
    tooltip = "Enable of disable parallel execution of next job entries",
    image = "ui/images/parallel-hop.svg"
  )
  public void editEntryParallel( HopGuiJobEntryContext context ) {

    JobEntryCopy je = context.getJobEntryCopy();
    JobEntryCopy jeOld = (JobEntryCopy) je.clone_deep();

    je.setLaunchingInParallel( !je.isLaunchingInParallel() );
    JobEntryCopy jeNew = (JobEntryCopy) je.clone_deep();

    hopUi.undoDelegate.addUndoChange( jobMeta, new JobEntryCopy[] { jeOld }, new JobEntryCopy[] { jeNew }, new int[] { jobMeta.indexOfJobEntry( jeNew ) } );
    jobMeta.setChanged();

    if ( getJobEntry().isLaunchingInParallel() ) {
      // Show a warning (optional)
      //
      if ( "Y".equalsIgnoreCase( hopUi.getProps().getCustomParameter( STRING_PARALLEL_WARNING_PARAMETER, "Y" ) ) ) {
        MessageDialogWithToggle md =
          new MessageDialogWithToggle( hopShell(),
            BaseMessages.getString( PKG, "JobGraph.ParallelJobEntriesWarning.DialogTitle" ),
            null,
            BaseMessages.getString( PKG, "JobGraph.ParallelJobEntriesWarning.DialogMessage", Const.CR ) + Const.CR,
            MessageDialog.WARNING,
            new String[] { BaseMessages.getString( PKG, "JobGraph.ParallelJobEntriesWarning.Option1" ) },
            0,
            BaseMessages.getString( PKG, "JobGraph.ParallelJobEntriesWarning.Option2" ),
            "N".equalsIgnoreCase( hopUi.getProps().getCustomParameter( STRING_PARALLEL_WARNING_PARAMETER, "Y" ) ) );
        MessageDialogWithToggle.setDefaultImage( GUIResource.getInstance().getImageHopUi() );
        md.open();
        hopUi.getProps().setCustomParameter( STRING_PARALLEL_WARNING_PARAMETER, md.getToggleState() ? "N" : "Y" );
        hopUi.getProps().saveProps();
      }
    }
    redraw();

  }

  private boolean canBeDuplicated( JobEntryCopy entry ) {
    return !entry.isStart();
  }

  public void detachEntry() {
    detach( getJobEntry() );
    jobMeta.unselectAll();
  }

  @GuiContextAction(
    id = "jobgraph-entry-10900-delete",
    parentId = HopGuiJobEntryContext.CONTEXT_ID,
    type = GuiActionType.Delete,
    name = "Delete this entry",
    tooltip = "Delete the selected job entry from the job",
    image = "ui/images/generic-delete.svg"
  )
  public void deleteEntry( HopGuiJobEntryContext context ) {
    delSelected( context.getJobEntryCopy() );
    redraw();
  }

  protected synchronized void setMenu( int x, int y ) {

    currentMouseX = x;
    currentMouseY = y;


  }

  @GuiKeyboardShortcut( control = true, key = 'a' )
  @GuiOSXKeyboardShortcut( command = true, key = 'a' )
  public void selectAll() {
    jobMeta.selectAll();
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
    if ( jobLogDelegate.hasSelectedText() ) {
      jobLogDelegate.copySelected();
    } else {
      jobClipboardDelegate.copySelected( jobMeta, jobMeta.getSelectedEntries(), jobMeta.getSelectedNotes() );
    }
  }

  @GuiKeyboardShortcut( control = true, key = 'x' )
  @GuiOSXKeyboardShortcut( command = true, key = 'x' )
  @Override public void cutSelectedToClipboard() {
    jobClipboardDelegate.copySelected( jobMeta, jobMeta.getSelectedEntries(), jobMeta.getSelectedNotes() );
    deleteSelected();
  }

  @GuiKeyboardShortcut( key = SWT.DEL )
  @Override public void deleteSelected() {
    jobEntryDelegate.deleteJobEntryCopies( jobMeta, jobMeta.getSelectedEntries() );
    notePadDelegate.deleteNotes( jobMeta, jobMeta.getSelectedNotes() );
  }

  @GuiKeyboardShortcut( control = true, key = 'v' )
  @GuiOSXKeyboardShortcut( command = true, key = 'v' )
  @Override public void pasteFromClipboard() {
    jobClipboardDelegate.pasteXML( jobMeta, jobClipboardDelegate.fromClipboard(), new Point( 50, 50 ) );
  }

  @GuiContextAction(
    id = "jobgraph-job-paste",
    parentId = HopGuiJobContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Paste from the clipboard",
    tooltip = "Paste job entries, notes or a whole job from the clipboard",
    image = "ui/images/CPY.svg"
  )
  public void pasteFromClipboard( HopGuiJobContext context ) {
    jobClipboardDelegate.pasteXML( jobMeta, jobClipboardDelegate.fromClipboard(), context.getClick() );
  }

  @GuiContextAction(
    id = "jobgraph-edit-job",
    parentId = HopGuiJobContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Edit job",
    tooltip = "Edit the job properties",
    image = "ui/images/JOB.svg"
  )
  public void editJobProperties( HopGuiJobContext context ) {
    editProperties( jobMeta, hopUi, true );
  }

  public void editJobProperties() {
    editProperties( jobMeta, hopUi, true );
  }

  @GuiContextAction(
    id = "jobgraph-new-note",
    parentId = HopGuiJobContext.CONTEXT_ID,
    type = GuiActionType.Create,
    name = "Create a note",
    tooltip = "Create a new note",
    image = "ui/images/new.svg"
  )
  public void newNote( HopGuiJobContext context ) {
    String title = BaseMessages.getString( PKG, "JobGraph.Dialog.EditNote.Title" );
    NotePadDialog dd = new NotePadDialog( jobMeta, hopShell(), title );
    NotePadMeta n = dd.open();
    if ( n != null ) {
      NotePadMeta npi = new NotePadMeta( n.getNote(), context.getClick().x, context.getClick().y, ConstUI.NOTE_MIN_SIZE, ConstUI.NOTE_MIN_SIZE, n
        .getFontName(), n.getFontSize(), n.isFontBold(), n.isFontItalic(), n.getFontColorRed(), n
        .getFontColorGreen(), n.getFontColorBlue(), n.getBackGroundColorRed(), n.getBackGroundColorGreen(), n
        .getBackGroundColorBlue(), n.getBorderColorRed(), n.getBorderColorGreen(), n.getBorderColorBlue(), n
        .isDrawShadow() );
      jobMeta.addNote( npi );
      hopUi.undoDelegate.addUndoNew( jobMeta, new NotePadMeta[] { npi }, new int[] { jobMeta.indexOfNote( npi ) } );
      redraw();
    }
  }

  public void setCurrentNote( NotePadMeta ni ) {
    this.ni = ni;
  }

  public NotePadMeta getCurrentNote() {
    return ni;
  }

  public void editNote() {
    selectionRegion = null;
    editNote( getCurrentNote() );
  }

  @GuiContextAction(
    id = "jobgraph-delete-note",
    parentId = HopGuiJobNoteContext.CONTEXT_ID,
    type = GuiActionType.Delete,
    name = "Delete the note",
    tooltip = "Delete the note",
    image = "ui/images/generic-delete.svg"
  )
  public void deleteNote( HopGuiJobNoteContext context ) {
    selectionRegion = null;
    NotePadMeta note = context.getNotePadMeta();
    int idx = jobMeta.indexOfNote( note );
    if ( idx >= 0 ) {
      jobMeta.removeNote( idx );
      hopUi.undoDelegate.addUndoDelete( jobMeta, new NotePadMeta[] { note }, new int[] { idx } );
    }
    redraw();
  }

  public void raiseNote() {
    selectionRegion = null;
    int idx = jobMeta.indexOfNote( getCurrentNote() );
    if ( idx >= 0 ) {
      jobMeta.raiseNote( idx );
      // spoon.addUndoRaise(jobMeta, new NotePadMeta[] {getCurrentNote()}, new int[] {idx} );
    }
    redraw();
  }

  public void lowerNote() {
    selectionRegion = null;
    int idx = jobMeta.indexOfNote( getCurrentNote() );
    if ( idx >= 0 ) {
      jobMeta.lowerNote( idx );
      // spoon.addUndoLower(jobMeta, new NotePadMeta[] {getCurrentNote()}, new int[] {idx} );
    }
    redraw();
  }

  public void flipHop() {
    selectionRegion = null;
    JobEntryCopy origFrom = currentHop.getFromEntry();
    JobEntryCopy origTo = currentHop.getToEntry();
    currentHop.setFromEntry( currentHop.getToEntry() );
    currentHop.setToEntry( origFrom );

    boolean cancel = false;
    if ( jobMeta.hasLoop( currentHop.getToEntry() ) ) {
      MessageBox mb = new MessageBox( hopShell(), SWT.OK | SWT.CANCEL | SWT.ICON_WARNING );
      mb.setMessage( BaseMessages.getString( PKG, "JobGraph.Dialog.HopFlipCausesLoop.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "JobGraph.Dialog.HopCausesLoop.Title" ) );
      int choice = mb.open();
      if ( choice == SWT.CANCEL ) {
        cancel = true;
        currentHop.setFromEntry( origFrom );
        currentHop.setToEntry( origTo );
      }
    }
    if ( !cancel ) {
      currentHop.setChanged();
    }
    updateGui();
  }

  public void disableHop() {
    selectionRegion = null;
    boolean orig = currentHop.isEnabled();
    currentHop.setEnabled( !currentHop.isEnabled() );

    if ( !orig && ( jobMeta.hasLoop( currentHop.getToEntry() ) ) ) {
      MessageBox mb = new MessageBox( hopShell(), SWT.CANCEL | SWT.OK | SWT.ICON_WARNING );
      mb.setMessage( BaseMessages.getString( PKG, "JobGraph.Dialog.LoopAfterHopEnabled.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "JobGraph.Dialog.LoopAfterHopEnabled.Title" ) );
      int choice = mb.open();
      if ( choice == SWT.CANCEL ) {
        currentHop.setEnabled( orig );
      }
    }
    updateGui();
  }

  public void deleteHop() {
    selectionRegion = null;
    int idx = jobMeta.indexOfJobHop( currentHop );
    jobMeta.removeJobHop( idx );
    updateGui();
  }

  public void setHopUnconditional() {
    currentHop.setUnconditional();
    updateGui();
    ;
  }

  public void setHopEvaluationTrue() {
    currentHop.setConditional();
    currentHop.setEvaluation( true );
    updateGui();
    ;
  }

  public void setHopEvaluationFalse() {
    currentHop.setConditional();
    currentHop.setEvaluation( false );
    updateGui();
    ;
  }

  protected void setCurrentHop( JobHopMeta hop ) {
    currentHop = hop;
  }

  protected JobHopMeta getCurrentHop() {
    return currentHop;
  }

  public void enableHopsBetweenSelectedEntries() {
    enableHopsBetweenSelectedEntries( true );
  }

  public void disableHopsBetweenSelectedEntries() {
    enableHopsBetweenSelectedEntries( false );
  }

  /**
   * This method enables or disables all the hops between the selected Entries.
   **/
  public void enableHopsBetweenSelectedEntries( boolean enabled ) {
    List<JobEntryCopy> list = jobMeta.getSelectedEntries();

    boolean hasLoop = false;

    for ( int i = 0; i < jobMeta.nrJobHops(); i++ ) {
      JobHopMeta hop = jobMeta.getJobHop( i );
      if ( list.contains( hop.getFromEntry() ) && list.contains( hop.getToEntry() ) ) {

        JobHopMeta before = (JobHopMeta) hop.clone();
        hop.setEnabled( enabled );
        JobHopMeta after = (JobHopMeta) hop.clone();
        hopUi.undoDelegate.addUndoChange( jobMeta, new JobHopMeta[] { before }, new JobHopMeta[] { after }, new int[] { jobMeta
          .indexOfJobHop( hop ) } );
        if ( jobMeta.hasLoop( hop.getToEntry() ) ) {
          hasLoop = true;
        }
      }
    }

    if ( hasLoop && enabled ) {
      MessageBox mb = new MessageBox( hopShell(), SWT.OK | SWT.ICON_WARNING );
      mb.setMessage( BaseMessages.getString( PKG, "JobGraph.Dialog.LoopAfterHopEnabled.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "JobGraph.Dialog.LoopAfterHopEnabled.Title" ) );
      mb.open();
    }

    updateGui();
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
    JobHopMeta before = (JobHopMeta) currentHop.clone();
    currentHop.setEnabled( enabled );
    JobHopMeta after = (JobHopMeta) currentHop.clone();
    hopUi.undoDelegate.addUndoChange( jobMeta, new JobHopMeta[] { before }, new JobHopMeta[] { after }, new int[] { jobMeta
      .indexOfJobHop( currentHop ) } );

    Set<JobEntryCopy> checkedEntries = enableDisableNextHops( currentHop.getToEntry(), enabled, new HashSet<>() );

    if ( checkedEntries.stream().anyMatch( entry -> jobMeta.hasLoop( entry ) ) ) {
      MessageBox mb = new MessageBox( hopShell(), SWT.OK | SWT.ICON_WARNING );
      mb.setMessage( BaseMessages.getString( PKG, "JobGraph.Dialog.LoopAfterHopEnabled.Message" ) );
      mb.setText( BaseMessages.getString( PKG, "JobGraph.Dialog.LoopAfterHopEnabled.Title" ) );
      mb.open();
    }

    updateGui();
  }

  private Set<JobEntryCopy> enableDisableNextHops( JobEntryCopy from, boolean enabled, Set<JobEntryCopy> checkedEntries ) {
    checkedEntries.add( from );
    jobMeta.getJobhops().stream()
      .filter( hop -> from.equals( hop.getFromEntry() ) )
      .forEach( hop -> {
        if ( hop.isEnabled() != enabled ) {
          JobHopMeta before = (JobHopMeta) hop.clone();
          hop.setEnabled( enabled );
          JobHopMeta after = (JobHopMeta) hop.clone();
          hopUi.undoDelegate.addUndoChange( jobMeta, new JobHopMeta[] { before }, new JobHopMeta[] { after }, new int[] { jobMeta
            .indexOfJobHop( hop ) } );
        }
        if ( !checkedEntries.contains( hop.getToEntry() ) ) {
          enableDisableNextHops( hop.getToEntry(), enabled, checkedEntries );
        }
      } );
    return checkedEntries;
  }

  private void modalMessageDialog( String title, String message, int swtFlags ) {
    MessageBox messageBox = new MessageBox( hopShell(), swtFlags );
    messageBox.setMessage( message );
    messageBox.setText( title );
    messageBox.open();
  }

  protected void setToolTip( int x, int y, int screenX, int screenY ) {
    if ( !hopUi.getProps().showToolTips() ) {
      return;
    }

    canvas.setToolTipText( null );

    Image tipImage = null;
    JobHopMeta hi = findJobHop( x, y );

    // check the area owner list...
    //
    StringBuilder tip = new StringBuilder();
    AreaOwner areaOwner = getVisibleAreaOwner( x, y );
    if ( areaOwner != null && areaOwner.getAreaType() != null ) {
      JobEntryCopy jobEntryCopy;
      switch ( areaOwner.getAreaType() ) {
        case JOB_HOP_ICON:
          hi = (JobHopMeta) areaOwner.getOwner();
          if ( hi.isUnconditional() ) {
            tipImage = GUIResource.getInstance().getImageUnconditionalHop();
            tip.append( BaseMessages.getString( PKG, "JobGraph.Hop.Tooltip.Unconditional", hi
              .getFromEntry().getName(), Const.CR ) );
          } else {
            if ( hi.getEvaluation() ) {
              tip.append( BaseMessages.getString( PKG, "JobGraph.Hop.Tooltip.EvaluatingTrue", hi
                .getFromEntry().getName(), Const.CR ) );
              tipImage = GUIResource.getInstance().getImageTrue();
            } else {
              tip.append( BaseMessages.getString( PKG, "JobGraph.Hop.Tooltip.EvaluatingFalse", hi
                .getFromEntry().getName(), Const.CR ) );
              tipImage = GUIResource.getInstance().getImageFalse();
            }
          }
          break;

        case JOB_HOP_PARALLEL_ICON:
          hi = (JobHopMeta) areaOwner.getOwner();
          tip.append( BaseMessages.getString(
            PKG, "JobGraph.Hop.Tooltip.Parallel", hi.getFromEntry().getName(), Const.CR ) );
          tipImage = GUIResource.getInstance().getImageParallelHop();
          break;

        case CUSTOM:
          String message = (String) areaOwner.getOwner();
          tip.append( message );
          tipImage = null;
          GUIResource.getInstance().getImagePipelineGraph();
          break;

        case JOB_ENTRY_MINI_ICON_INPUT:
          tip.append( BaseMessages.getString( PKG, "JobGraph.EntryInputConnector.Tooltip" ) );
          tipImage = GUIResource.getInstance().getImageHopInput();
          resetDelayTimer( (JobEntryCopy) areaOwner.getOwner() );
          break;

        case JOB_ENTRY_MINI_ICON_OUTPUT:
          tip.append( BaseMessages.getString( PKG, "JobGraph.EntryOutputConnector.Tooltip" ) );
          tipImage = GUIResource.getInstance().getImageHopOutput();
          resetDelayTimer( (JobEntryCopy) areaOwner.getOwner() );
          break;

        case JOB_ENTRY_MINI_ICON_EDIT:
          tip.append( BaseMessages.getString( PKG, "JobGraph.EditTransform.Tooltip" ) );
          tipImage = GUIResource.getInstance().getImageEdit();
          resetDelayTimer( (JobEntryCopy) areaOwner.getOwner() );
          break;

        case JOB_ENTRY_MINI_ICON_CONTEXT:
          tip.append( BaseMessages.getString( PKG, "JobGraph.ShowMenu.Tooltip" ) );
          tipImage = GUIResource.getInstance().getImageContextMenu();
          resetDelayTimer( (JobEntryCopy) areaOwner.getOwner() );
          break;

        case JOB_ENTRY_RESULT_FAILURE:
        case JOB_ENTRY_RESULT_SUCCESS:
          JobEntryResult jobEntryResult = (JobEntryResult) areaOwner.getOwner();
          jobEntryCopy = (JobEntryCopy) areaOwner.getParent();
          Result result = jobEntryResult.getResult();
          tip.append( "'" ).append( jobEntryCopy.getName() ).append( "' " );
          if ( result.getResult() ) {
            tipImage = GUIResource.getInstance().getImageTrue();
            tip.append( "finished successfully." );
          } else {
            tipImage = GUIResource.getInstance().getImageFalse();
            tip.append( "failed." );
          }
          tip.append( Const.CR ).append( "------------------------" ).append( Const.CR ).append( Const.CR );
          tip.append( "Result         : " ).append( result.getResult() ).append( Const.CR );
          tip.append( "Errors         : " ).append( result.getNrErrors() ).append( Const.CR );

          if ( result.getNrLinesRead() > 0 ) {
            tip.append( "Lines read     : " ).append( result.getNrLinesRead() ).append( Const.CR );
          }
          if ( result.getNrLinesWritten() > 0 ) {
            tip.append( "Lines written  : " ).append( result.getNrLinesWritten() ).append( Const.CR );
          }
          if ( result.getNrLinesInput() > 0 ) {
            tip.append( "Lines input    : " ).append( result.getNrLinesInput() ).append( Const.CR );
          }
          if ( result.getNrLinesOutput() > 0 ) {
            tip.append( "Lines output   : " ).append( result.getNrLinesOutput() ).append( Const.CR );
          }
          if ( result.getNrLinesUpdated() > 0 ) {
            tip.append( "Lines updated  : " ).append( result.getNrLinesUpdated() ).append( Const.CR );
          }
          if ( result.getNrLinesDeleted() > 0 ) {
            tip.append( "Lines deleted  : " ).append( result.getNrLinesDeleted() ).append( Const.CR );
          }
          if ( result.getNrLinesRejected() > 0 ) {
            tip.append( "Lines rejected : " ).append( result.getNrLinesRejected() ).append( Const.CR );
          }
          if ( result.getResultFiles() != null && !result.getResultFiles().isEmpty() ) {
            tip.append( Const.CR ).append( "Result files:" ).append( Const.CR );
            if ( result.getResultFiles().size() > 10 ) {
              tip.append( " (10 files of " ).append( result.getResultFiles().size() ).append( " shown" );
            }
            List<ResultFile> files = new ArrayList<>( result.getResultFiles().values() );
            for ( int i = 0; i < files.size(); i++ ) {
              ResultFile file = files.get( i );
              tip.append( "  - " ).append( file.toString() ).append( Const.CR );
            }
          }
          if ( result.getRows() != null && !result.getRows().isEmpty() ) {
            tip.append( Const.CR ).append( "Result rows: " );
            if ( result.getRows().size() > 10 ) {
              tip.append( " (10 rows of " ).append( result.getRows().size() ).append( " shown" );
            }
            tip.append( Const.CR );
            for ( int i = 0; i < result.getRows().size() && i < 10; i++ ) {
              RowMetaAndData row = result.getRows().get( i );
              tip.append( "  - " ).append( row.toString() ).append( Const.CR );
            }
          }
          break;

        case JOB_ENTRY_RESULT_CHECKPOINT:
          tip.append( "The job started here since this is the furthest checkpoint "
            + "that was reached last time the pipeline was executed." );
          tipImage = GUIResource.getInstance().getImageCheckpoint();
          break;
        case JOB_ENTRY_ICON:
          JobEntryCopy jec = (JobEntryCopy) areaOwner.getOwner();
          if ( jec.isDeprecated() ) { // only need tooltip if job entry is deprecated
            tip.append( BaseMessages.getString( PKG, "JobGraph.DeprecatedEntry.Tooltip.Title" ) ).append( Const.CR );
            String tipNext = BaseMessages.getString( PKG, "JobGraph.DeprecatedEntry.Tooltip.Message1", jec.getName() );
            int length = tipNext.length() + 5;
            for ( int i = 0; i < length; i++ ) {
              tip.append( "-" );
            }
            tip.append( Const.CR ).append( tipNext ).append( Const.CR );
            tip.append( BaseMessages.getString( PKG, "JobGraph.DeprecatedEntry.Tooltip.Message2" ) );
            if ( !Utils.isEmpty( jec.getSuggestion() )
              && !( jec.getSuggestion().startsWith( "!" ) && jec.getSuggestion().endsWith( "!" ) ) ) {
              tip.append( " " );
              tip.append( BaseMessages.getString( PKG, "JobGraph.DeprecatedEntry.Tooltip.Message3",
                jec.getSuggestion() ) );
            }
            tipImage = GUIResource.getInstance().getImageDeprecated();
          }
          break;
        default:
          break;
      }
    }

    if ( hi != null && tip.length() == 0 ) {
      // Set the tooltip for the hop:
      tip.append( BaseMessages.getString( PKG, "JobGraph.Dialog.HopInfo" ) ).append( Const.CR );
      tip.append( BaseMessages.getString( PKG, "JobGraph.Dialog.HopInfo.SourceEntry" ) ).append( " " ).append(
        hi.getFromEntry().getName() ).append( Const.CR );
      tip.append( BaseMessages.getString( PKG, "JobGraph.Dialog.HopInfo.TargetEntry" ) ).append( " " ).append(
        hi.getToEntry().getName() ).append( Const.CR );
      tip.append( BaseMessages.getString( PKG, "PipelineGraph.Dialog.HopInfo.Status" ) ).append( " " );
      tip.append( ( hi.isEnabled()
        ? BaseMessages.getString( PKG, "JobGraph.Dialog.HopInfo.Enable" ) : BaseMessages.getString(
        PKG, "JobGraph.Dialog.HopInfo.Disable" ) ) );
      if ( hi.isUnconditional() ) {
        tipImage = GUIResource.getInstance().getImageUnconditionalHop();
      } else {
        if ( hi.getEvaluation() ) {
          tipImage = GUIResource.getInstance().getImageTrue();
        } else {
          tipImage = GUIResource.getInstance().getImageFalse();
        }
      }
    }

    if ( tip == null || tip.length() == 0 ) {
      toolTip.hide();
    } else {
      if ( !tip.toString().equalsIgnoreCase( getToolTipText() ) ) {
        if ( tipImage != null ) {
          toolTip.setImage( tipImage );
        } else {
          toolTip.setImage( GUIResource.getInstance().getImageHopUi() );
        }
        toolTip.setText( tip.toString() );
        toolTip.hide();
        toolTip.show( new org.eclipse.swt.graphics.Point( screenX, screenY ) );
      }
    }
  }

  public void launchStuff( JobEntryCopy jobEntryCopy ) {
    String[] references = jobEntryCopy.getEntry().getReferencedObjectDescriptions();
    if ( !Utils.isEmpty( references ) ) {
      loadReferencedObject( jobEntryCopy, 0 );
    }
  }

  public void launchStuff() {
    if ( jobEntry != null ) {
      launchStuff( jobEntry );
    }
  }

  protected void loadReferencedObject( JobEntryCopy jobEntryCopy, int index ) {
    try {
      IHasFilename referencedMeta = jobEntryCopy.getEntry().loadReferencedObject( index, hopUi.getMetaStore(), jobMeta );
      if ( referencedMeta == null ) {
        return; // Sorry, nothing loaded
      }
      HopFileTypeInterface fileTypeHandler = hopUi.getPerspectiveManager().findFileTypeHandler( referencedMeta );
      fileTypeHandler.openFile( hopUi, referencedMeta.getFilename(), hopUi.getVariableSpace() );
    } catch ( Exception e ) {
      new ErrorDialog( hopShell(), "Error", "The referenced file couldn't be loaded", e );
    }
  }

  public synchronized void setJob( Job job ) {
    this.job = job;
  }


  public void paintControl( PaintEvent e ) {
    Point area = getArea();
    if ( area.x == 0 || area.y == 0 ) {
      return; // nothing to do!
    }

    Display disp = hopDisplay();

    Image img = getJobImage( disp, area.x, area.y, magnification );
    e.gc.drawImage( img, 0, 0 );
    if ( jobMeta.nrJobEntries() == 0 ) {
      e.gc.setForeground( GUIResource.getInstance().getColorCrystalText() );
      e.gc.setBackground( GUIResource.getInstance().getColorBackground() );
      e.gc.setFont( GUIResource.getInstance().getFontMedium() );

      Image welcomeImage = GUIResource.getInstance().getImageJobCanvas();
      int leftPosition = ( area.x - welcomeImage.getBounds().width ) / 2;
      int topPosition = ( area.y - welcomeImage.getBounds().height ) / 2;
      e.gc.drawImage( welcomeImage, leftPosition, topPosition );

    }
    img.dispose();

  }

  public Image getJobImage( Device device, int x, int y, float magnificationFactor ) {
    GCInterface gc = new SWTGC( device, new Point( x, y ), iconsize );

    int gridSize =
      PropsUI.getInstance().isShowCanvasGridEnabled() ? PropsUI.getInstance().getCanvasGridSize() : 1;

    JobPainter jobPainter =
      new JobPainter(
        gc, jobMeta, new Point( x, y ), new SwtScrollBar( hori ), new SwtScrollBar( vert ), hop_candidate,
        drop_candidate, selectionRegion, areaOwners, PropsUI.getInstance().getIconSize(),
        PropsUI.getInstance().getLineWidth(), gridSize, PropsUI
        .getInstance().getShadowSize(), PropsUI.getInstance().isAntiAliasingEnabled(), PropsUI
        .getInstance().getNoteFont().getName(), PropsUI.getInstance().getNoteFont().getHeight(), PropsUI.getInstance().getZoomFactor() );

    // correct the magnifacation with the overall zoom factor
    //
    float correctedMagnification = (float) ( magnificationFactor * PropsUI.getInstance().getZoomFactor() );

    jobPainter.setMagnification( correctedMagnification );
    jobPainter.setEntryLogMap( entryLogMap );
    jobPainter.setStartHopEntry( startHopEntry );
    jobPainter.setEndHopLocation( endHopLocation );
    jobPainter.setEndHopEntry( endHopEntry );
    jobPainter.setNoInputEntry( noInputEntry );
    if ( job != null ) {
      jobPainter.setJobEntryResults( job.getJobEntryResults() );
    } else {
      jobPainter.setJobEntryResults( new ArrayList<JobEntryResult>() );
    }

    List<JobEntryCopy> activeJobEntries = new ArrayList<>();
    if ( job != null ) {
      if ( job.getActiveJobEntryJobs().size() > 0 ) {
        activeJobEntries.addAll( job.getActiveJobEntryJobs().keySet() );
      }
      if ( job.getActiveJobEntryPipeline().size() > 0 ) {
        activeJobEntries.addAll( job.getActiveJobEntryPipeline().keySet() );
      }
    }
    jobPainter.setActiveJobEntries( activeJobEntries );

    jobPainter.drawJob();

    return (Image) gc.getImage();
  }

  protected Point getOffset() {
    Point area = getArea();
    Point max = jobMeta.getMaximum();
    Point thumb = getThumb( area, max );
    return getOffset( thumb, area );
  }

  @Override public boolean hasChanged() {
    return jobMeta.hasChanged();
  }

  protected void newHop() {
    List<JobEntryCopy> selection = jobMeta.getSelectedEntries();
    if ( selection == null || selection.size() < 2 ) {
      return;
    }
    JobEntryCopy fr = selection.get( 0 );
    JobEntryCopy to = selection.get( 1 );
    jobHopDelegate.newHop( jobMeta, fr, to );
  }

  @GuiContextAction(
    id = "jobgraph-entry-10000-edit",
    parentId = HopGuiJobEntryContext.CONTEXT_ID,
    type = GuiActionType.Modify,
    name = "Edit the entry",
    tooltip = "Edit the job entry properties",
    image = "ui/images/Edit.svg"
  )
  public void editEntry( HopGuiJobEntryContext context ) {

    jobEntryDelegate.editJobEntry( jobMeta, context.getJobEntryCopy() );
  }

  public void editEntry( JobEntryCopy je ) {
    jobEntryDelegate.editJobEntry( jobMeta, je );
  }

  protected void editNote( NotePadMeta ni ) {
    NotePadMeta before = (NotePadMeta) ni.clone();
    String title = BaseMessages.getString( PKG, "JobGraph.Dialog.EditNote.Title" );

    NotePadDialog dd = new NotePadDialog( jobMeta, hopShell(), title, ni );
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

      hopUi.undoDelegate.addUndoChange( jobMeta, new NotePadMeta[] { before }, new NotePadMeta[] { ni }, new int[] { jobMeta
        .indexOfNote( ni ) } );
      ni.width = ConstUI.NOTE_MIN_SIZE;
      ni.height = ConstUI.NOTE_MIN_SIZE;

      updateGui();
    }
  }

  protected void drawArrow( GC gc, int[] line ) {
    int mx, my;
    int x1 = line[ 0 ] + offset.x;
    int y1 = line[ 1 ] + offset.y;
    int x2 = line[ 2 ] + offset.x;
    int y2 = line[ 3 ] + offset.y;
    int x3;
    int y3;
    int x4;
    int y4;
    int a, b, dist;
    double factor;
    double angle;

    // gc.setLineWidth(1);
    // WuLine(gc, black, x1, y1, x2, y2);

    gc.drawLine( x1, y1, x2, y2 );

    // What's the distance between the 2 points?
    a = Math.abs( x2 - x1 );
    b = Math.abs( y2 - y1 );
    dist = (int) Math.sqrt( a * a + b * b );

    // determine factor (position of arrow to left side or right side 0-->100%)
    if ( dist >= 2 * iconsize ) {
      factor = 1.5;
    } else {
      factor = 1.2;
    }

    // in between 2 points
    mx = (int) ( x1 + factor * ( x2 - x1 ) / 2 );
    my = (int) ( y1 + factor * ( y2 - y1 ) / 2 );

    // calculate points for arrowhead
    angle = Math.atan2( y2 - y1, x2 - x1 ) + Math.PI;

    x3 = (int) ( mx + Math.cos( angle - theta ) * size );
    y3 = (int) ( my + Math.sin( angle - theta ) * size );

    x4 = (int) ( mx + Math.cos( angle + theta ) * size );
    y4 = (int) ( my + Math.sin( angle + theta ) * size );

    // draw arrowhead
    // gc.drawLine(mx, my, x3, y3);
    // gc.drawLine(mx, my, x4, y4);
    // gc.drawLine( x3, y3, x4, y4 );
    Color fore = gc.getForeground();
    Color back = gc.getBackground();
    gc.setBackground( fore );
    gc.fillPolygon( new int[] { mx, my, x3, y3, x4, y4 } );
    gc.setBackground( back );
  }

  protected boolean pointOnLine( int x, int y, int[] line ) {
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

  protected boolean pointOnThinLine( int x, int y, int[] line ) {
    int x1 = line[ 0 ];
    int y1 = line[ 1 ];
    int x2 = line[ 2 ];
    int y2 = line[ 3 ];

    // Not in the square formed by these 2 points: ignore!
    //CHECKSTYLE:LineLength:OFF
    if ( !( ( ( x >= x1 && x <= x2 ) || ( x >= x2 && x <= x1 ) ) && ( ( y >= y1 && y <= y2 ) || ( y >= y2 && y <= y1 ) ) ) ) {
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

  protected SnapAllignDistribute createSnapAllignDistribute() {

    List<JobEntryCopy> elements = jobMeta.getSelectedEntries();
    int[] indices = jobMeta.getEntryIndexes( elements );
    return new SnapAllignDistribute( jobMeta, elements, indices, hopUi.undoDelegate, this );
  }

  @GuiToolbarElement(
    id = TOOLBAR_ITEM_SNAP_TO_GRID,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "Snap to grid",
    toolTip = "Align the selected entries to the specified grid size",
    image = "ui/images/toolbar/snap-to-grid.svg",
    disabledImage = "ui/images/toolbar/snap-to-grid-disabled.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  public void snapToGrid() {
    snapToGrid( ConstUI.GRID_SIZE );
  }

  protected void snapToGrid( int size ) {
    createSnapAllignDistribute().snapToGrid( size );
  }

  @GuiToolbarElement(
    id = TOOLBAR_ITEM_ALIGN_LEFT,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "Left-align selected entries",
    toolTip = "Align the entries with the left-most entry in your selection",
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
    label = "Right-align selected entries",
    toolTip = "Align the entries with the right-most entry in your selection",
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
    label = "Top-align selected entries",
    toolTip = "Align the entries with the top-most entry in your selection",
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
    label = "Bottom-align selected entries",
    toolTip = "Align the entries with the bottom-most entry in your selection",
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
    label = "Horizontally distribute selected entries",
    toolTip = "Distribute the selected entries evenly between the left-most and right-most entry in your selection",
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
    label = "Vertically distribute selected entries",
    toolTip = "Distribute the selected entries evenly between the top-most and bottom-most entry in your selection",
    image = "ui/images/toolbar/distribute-vertically.svg",
    disabledImage = "ui/images/toolbar/distribute-vertically-disabled.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  public void distributeVertical() {
    createSnapAllignDistribute().distributevertical();
  }

  protected void drawRect( GC gc, Rectangle rect ) {
    if ( rect == null ) {
      return;
    }

    gc.setLineStyle( SWT.LINE_DASHDOT );
    gc.setLineWidth( 1 );
    gc.setForeground( GUIResource.getInstance().getColorDarkGray() );
    // PDI-2619: SWT on Windows doesn't cater for negative rect.width/height so handle here.
    Point s = new Point( rect.x + offset.x, rect.y + offset.y );
    if ( rect.width < 0 ) {
      s.x = s.x + rect.width;
    }
    if ( rect.height < 0 ) {
      s.y = s.y + rect.height;
    }
    gc.drawRoundRectangle( s.x, s.y, Math.abs( rect.width ), Math.abs( rect.height ), 3, 3 );
    gc.setLineStyle( SWT.LINE_SOLID );
  }

  protected void detach( JobEntryCopy je ) {
    JobHopMeta hfrom = jobMeta.findJobHopTo( je );
    JobHopMeta hto = jobMeta.findJobHopFrom( je );

    if ( hfrom != null && hto != null ) {
      if ( jobMeta.findJobHop( hfrom.getFromEntry(), hto.getToEntry() ) == null ) {
        JobHopMeta hnew = new JobHopMeta( hfrom.getFromEntry(), hto.getToEntry() );
        jobMeta.addJobHop( hnew );
        hopUi.undoDelegate.addUndoNew( jobMeta, new JobHopMeta[] { (JobHopMeta) hnew.clone() }, new int[] { jobMeta
          .indexOfJobHop( hnew ) } );
      }
    }
    if ( hfrom != null ) {
      int fromidx = jobMeta.indexOfJobHop( hfrom );
      if ( fromidx >= 0 ) {
        jobMeta.removeJobHop( fromidx );
        hopUi.undoDelegate.addUndoDelete( jobMeta, new JobHopMeta[] { hfrom }, new int[] { fromidx } );
      }
    }
    if ( hto != null ) {
      int toidx = jobMeta.indexOfJobHop( hto );
      if ( toidx >= 0 ) {
        jobMeta.removeJobHop( toidx );
        hopUi.undoDelegate.addUndoDelete( jobMeta, new JobHopMeta[] { hto }, new int[] { toidx } );
      }
    }
    updateGui();
  }

  public void newProps() {
    iconsize = hopUi.getProps().getIconSize();
    linewidth = hopUi.getProps().getLineWidth();
  }

  public String toString() {
    if ( jobMeta == null ) {
      return HopGui.APP_NAME;
    } else {
      return jobMeta.getName();
    }
  }

  public EngineMetaInterface getMeta() {
    return jobMeta;
  }


  /**
   * @param jobMeta the jobMeta to set
   * @return the jobMeta / public JobMeta getJobMeta() { return jobMeta; }
   * <p>
   * /**
   */
  public void setJobMeta( JobMeta jobMeta ) {
    this.jobMeta = jobMeta;
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
    jobUndoDelegate.undoJobAction( this, jobMeta );
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
    jobUndoDelegate.redoJobAction( this, jobMeta );
    forceFocus();
  }

  /**
   * Update the representation, toolbar, menus and so on. This is needed after a file, context or capabilities changes
   */
  @Override public void updateGui() {

    if ( hopUi == null || toolBarWidgets == null || toolBar == null || toolBar.isDisposed() ) {
      return;
    }

    hopDisplay().asyncExec( new Runnable() {
      @Override public void run() {
        setZoomLabel();

        // Enable/disable the undo/redo toolbar buttons...
        //
        toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_UNDO_ID, jobMeta.viewThisUndo() != null );
        toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_REDO_ID, jobMeta.viewNextUndo() != null );

        // Enable/disable the align/distribute toolbar buttons
        //
        boolean selectedTransform = !jobMeta.getSelectedEntries().isEmpty();
        toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_SNAP_TO_GRID, selectedTransform );

        boolean selectedEntries = !jobMeta.getSelectedEntries().isEmpty();
        toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_SNAP_TO_GRID, selectedEntries );
        toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_ALIGN_LEFT, selectedEntries );
        toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_ALIGN_RIGHT, selectedEntries );
        toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_ALIGN_TOP, selectedEntries );
        toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_ALIGN_BOTTOM, selectedEntries );
        toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_DISTRIBUTE_HORIZONTALLY, selectedEntries );
        toolBarWidgets.enableToolbarItem( TOOLBAR_ITEM_DISTRIBUTE_VERTICALLY, selectedEntries );

        hopUi.setUndoMenu( jobMeta );
        hopUi.handleFileCapabilities( fileType );
        HopGuiJobGraph.super.redraw();
      }
    } );
  }

  public boolean canBeClosed() {
    return !jobMeta.hasChanged();
  }

  public JobMeta getManagedObject() {
    return jobMeta;
  }

  public boolean hasContentChanged() {
    return jobMeta.hasChanged();
  }

  public static int showChangedWarning( Shell shell, String name ) {
    MessageBox mb = new MessageBox( shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_WARNING );
    mb.setMessage( BaseMessages.getString( PKG, "JobGraph.Dialog.PromptSave.Message", name ) );
    mb.setText( BaseMessages.getString( PKG, "JobGraph.Dialog.PromptSave.Title" ) );
    return mb.open();
  }

  public boolean editProperties( JobMeta jobMeta, HopGui hopUi, boolean allowDirectoryChange ) {
    if ( jobMeta == null ) {
      return false;
    }

    JobDialog jd = new JobDialog( hopUi.getShell(), SWT.NONE, jobMeta );
    jd.setDirectoryChangeAllowed( allowDirectoryChange );
    JobMeta newJobMeta = jd.open();

    // If we added properties, add them to the variables too, so that they appear in the CTRL-SPACE variable completion.
    //
    hopUi.setParametersAsVariablesInUI( jobMeta, jobMeta );

    if ( newJobMeta != null ) {
      updateGui();
    }

    updateGui();
    return newJobMeta != null;
  }

  @Override
  public void save() throws HopException {
    String filename = jobMeta.getFilename();
    try {
      if ( StringUtils.isEmpty( filename ) ) {
        throw new HopException( "Please give the job a filename" );
      }
      String xml = jobMeta.getXML();
      OutputStream out = HopVFS.getOutputStream( jobMeta.getFilename(), false );
      try {
        out.write( XMLHandler.getXMLHeader( Const.XML_ENCODING ).getBytes( Const.XML_ENCODING ) );
        out.write( xml.getBytes( Const.XML_ENCODING ) );
        jobMeta.clearChanged();
        redraw();
      } finally {
        out.flush();
        out.close();
      }
    } catch ( Exception e ) {
      throw new HopException( "Error saving job to file '" + filename + "'", e );
    }
  }

  @Override
  public void saveAs( String filename ) throws HopException {
    jobMeta.setFilename( filename );
    save();
  }

  /**
   * @return the lastMove
   */
  public Point getLastMove() {
    return lastMove;
  }

  /**
   * @param lastMove the lastMove to set
   */
  public void setLastMove( Point lastMove ) {
    this.lastMove = lastMove;
  }

  /**
   * Add an extra view to the main composite SashForm
   */
  public void addExtraView() {
    extraViewComposite = new Composite( sashForm, SWT.NONE );
    FormLayout extraCompositeFormLayout = new FormLayout();
    extraCompositeFormLayout.marginWidth = 2;
    extraCompositeFormLayout.marginHeight = 2;
    extraViewComposite.setLayout( extraCompositeFormLayout );

    // Put a close and max button to the upper right corner...
    //
    closeButton = new Label( extraViewComposite, SWT.NONE );
    closeButton.setImage( GUIResource.getInstance().getImageClosePanel() );
    closeButton
      .setToolTipText( BaseMessages.getString( PKG, "JobGraph.ExecutionResultsPanel.CloseButton.Tooltip" ) );
    FormData fdClose = new FormData();
    fdClose.right = new FormAttachment( 100, 0 );
    fdClose.top = new FormAttachment( 0, 0 );
    closeButton.setLayoutData( fdClose );
    closeButton.addMouseListener( new MouseAdapter() {
      public void mouseDown( MouseEvent e ) {
        disposeExtraView();
      }
    } );

    minMaxButton = new Label( extraViewComposite, SWT.NONE );
    minMaxButton.setImage( GUIResource.getInstance().getImageMaximizePanel() );
    minMaxButton
      .setToolTipText( BaseMessages.getString( PKG, "JobGraph.ExecutionResultsPanel.MaxButton.Tooltip" ) );
    FormData fdMinMax = new FormData();
    fdMinMax.right = new FormAttachment( closeButton, -props.getMargin() );
    fdMinMax.top = new FormAttachment( 0, 0 );
    minMaxButton.setLayoutData( fdMinMax );
    minMaxButton.addMouseListener( new MouseAdapter() {
      public void mouseDown( MouseEvent e ) {
        minMaxExtraView();
      }
    } );

    // Add a label at the top: Results
    //
    Label wResultsLabel = new Label( extraViewComposite, SWT.LEFT );
    wResultsLabel.setFont( GUIResource.getInstance().getFontMediumBold() );
    wResultsLabel.setBackground( GUIResource.getInstance().getColorWhite() );
    wResultsLabel.setText( BaseMessages.getString( PKG, "JobLog.ResultsPanel.NameLabel" ) );
    FormData fdResultsLabel = new FormData();
    fdResultsLabel.left = new FormAttachment( 0, 0 );
    fdResultsLabel.right = new FormAttachment( 100, 0 );
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
    item.setImage( GUIResource.getInstance().getImageShowResults() );
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
      minMaxButton.setToolTipText( BaseMessages
        .getString( PKG, "JobGraph.ExecutionResultsPanel.MaxButton.Tooltip" ) );
    } else {
      // Maximize
      //
      sashForm.setMaximizedControl( extraViewComposite );
      minMaxButton.setImage( GUIResource.getInstance().getImageMinimizePanel() );
      minMaxButton.setToolTipText( BaseMessages
        .getString( PKG, "JobGraph.ExecutionResultsPanel.MinButton.Tooltip" ) );
    }
  }

  public boolean isExecutionResultsPaneVisible() {
    return extraViewComposite != null && !extraViewComposite.isDisposed();
  }

  @GuiToolbarElement(
    id = TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS,
    type = GuiElementType.TOOLBAR_BUTTON,
    label = "HopGui.Menu.ShowExecutionResults",
    toolTip = "HopGui.Tooltip.ShowExecutionResults",
    i18nPackageClass = HopGui.class,
    image = "ui/images/show-results.svg",
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID,
    separator = true
  )
  public void showExecutionResults() {
    if ( isExecutionResultsPaneVisible() ) {
      disposeExtraView();
    } else {
      addAllTabs();
    }
  }

  public void addAllTabs() {

    CTabItem tabItemSelection = null;
    if ( extraViewTabFolder != null && !extraViewTabFolder.isDisposed() ) {
      tabItemSelection = extraViewTabFolder.getSelection();
    }

    jobLogDelegate.addJobLog();
    jobGridDelegate.addJobGrid();
    jobMetricsDelegate.addJobMetrics();

    if ( tabItemSelection != null ) {
      extraViewTabFolder.setSelection( tabItemSelection );
    } else {
      extraViewTabFolder.setSelection( jobGridDelegate.getJobGridTab() );
    }

    ToolItem toolItem = toolBarWidgets.findToolItem( TOOLBAR_ITEM_SHOW_EXECUTION_RESULTS );
    toolItem.setToolTipText( BaseMessages.getString( PKG, "HopGui.Tooltip.HideExecutionResults" ) );
    toolItem.setImage( GUIResource.getInstance().getImageHideResults() );
  }


  @GuiToolbarElement(
    type = GuiElementType.TOOLBAR_BUTTON,
    id = "HopGuiJobGraph-ToolBar-10060-Print",
    label = "Print",
    toolTip = "Print this job",
    image = "ui/images/print.svg",
    separator = true,
    parentId = GUI_PLUGIN_TOOLBAR_PARENT_ID
  )
  @Override
  public void print() {
    PrintSpool ps = new PrintSpool();
    Printer printer = ps.getPrinter( hopShell() );

    // Create an image of the screen
    Point max = jobMeta.getMaximum();
    Image img = getJobImage( printer, max.x, max.y, 1.0f );
    ps.printImage( hopShell(), img );

    img.dispose();
    ps.dispose();
  }

   /* TODO: re-introduce
  public void getSQL() {
    hopUi.getSQL();
  }
    */


   /* TODO: re-introduce
 public void exploreDatabase() {
    hopUi.exploreDatabase();
  }
    */

  public void close() {
    hopUi.menuFileClose();
  }

  @Override public boolean isCloseable() {
    try {
      // Check if the file is saved. If not, ask for it to be saved.
      //
      if ( jobMeta.hasChanged() ) {

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

  public String buildTabName() throws HopException {
    String tabName = null;
    String realFilename = jobMeta.environmentSubstitute( jobMeta.getFilename() );
    if ( StringUtils.isEmpty( realFilename ) ) {
      tabName = jobMeta.getName();
    } else {
      try {
        FileObject fileObject = HopVFS.getFileObject( jobMeta.getFilename() );
        FileName fileName = fileObject.getName();
        tabName = fileName.getBaseName();
      } catch ( Exception e ) {
        throw new HopException( "Unable to get information from file name '" + jobMeta.getFilename() + "'", e );
      }
    }
    return tabName;
  }

  public synchronized void startJob( JobExecutionConfiguration executionConfiguration ) throws HopException {

    // If the job is not running, start the pipeline...
    //
    if ( job == null || ( job.isFinished() || job.isStopped() ) && !job.isActive() ) {
      // Auto save feature...
      //
      handleJobMetaChanges( jobMeta );

      // Is there a filename set?
      //
      if ( jobMeta.getFilename() != null && !jobMeta.hasChanged() ) { // Didn't change
        if ( job == null || ( job != null && !job.isActive() ) ) {
          try {

            // Make sure we clear the log before executing again...
            //
            if ( executionConfiguration.isClearingLog() ) {
              jobLogDelegate.clearLog();
            }

            // Also make sure to clear the old log entries in the central log
            // store & registry
            //
            if ( job != null ) {
              HopLogStore.discardLines( job.getLogChannelId(), true );
            }

            JobMeta runJobMeta;


            runJobMeta = new JobMeta( null, jobMeta.getFilename(), jobMeta.getMetaStore() );

            String spoonObjectId = UUID.randomUUID().toString();
            SimpleLoggingObject spoonLoggingObject =
              new SimpleLoggingObject( "HOPUI", LoggingObjectType.HOPUI, null );
            spoonLoggingObject.setContainerObjectId( spoonObjectId );
            spoonLoggingObject.setLogLevel( executionConfiguration.getLogLevel() );
            job = new Job( runJobMeta, spoonLoggingObject );

            job.setLogLevel( executionConfiguration.getLogLevel() );
            job.shareVariablesWith( jobMeta );
            job.setInteractive( true );
            job.setGatheringMetrics( executionConfiguration.isGatheringMetrics() );

            // Pass specific extension points...
            //
            job.getExtensionDataMap().putAll( executionConfiguration.getExtensionOptions() );

            // Add job entry listeners
            //
            job.addJobEntryListener( createRefreshJobEntryListener() );

            // If there is an alternative start job entry, pass it to the job
            //
            if ( !Utils.isEmpty( executionConfiguration.getStartCopyName() ) ) {
              JobEntryCopy startJobEntryCopy =
                runJobMeta.findJobEntry( executionConfiguration.getStartCopyName(), executionConfiguration.getStartCopyNr() );
              job.setStartJobEntryCopy( startJobEntryCopy );
            }

            // Set the named parameters
            Map<String, String> paramMap = executionConfiguration.getParams();
            Set<String> keys = paramMap.keySet();
            for ( String key : keys ) {
              job.getJobMeta().setParameterValue( key, Const.NVL( paramMap.get( key ), "" ) );
            }
            job.getJobMeta().activateParameters();

            log.logMinimal( BaseMessages.getString( PKG, "JobLog.Log.StartingJob" ) );
            job.start();
            jobGridDelegate.previousNrItems = -1;
            // Link to the new jobTracker!
            jobGridDelegate.jobTracker = job.getJobTracker();

            // Attach a listener to notify us that the pipeline has
            // finished.
            job.addJobListener( new JobAdapter() {
              public void jobFinished( Job job ) {
                HopGuiJobGraph.this.jobFinished();
              }
            } );

            // Show the execution results views
            //
            addAllTabs();
          } catch ( HopException e ) {
            new ErrorDialog(
              hopShell(), BaseMessages.getString( PKG, "JobLog.Dialog.CanNotOpenJob.Title" ), BaseMessages.getString(
              PKG, "JobLog.Dialog.CanNotOpenJob.Message" ), e );
            job = null;
          }
        } else {
          MessageBox m = new MessageBox( hopShell(), SWT.OK | SWT.ICON_WARNING );
          m.setText( BaseMessages.getString( PKG, "JobLog.Dialog.JobIsAlreadyRunning.Title" ) );
          m.setMessage( BaseMessages.getString( PKG, "JobLog.Dialog.JobIsAlreadyRunning.Message" ) );
          m.open();
        }
      } else {
        if ( jobMeta.hasChanged() ) {
          showSaveFileMessage();
        }
      }
      updateGui();
    }
  }

  public void showSaveFileMessage() {
    MessageBox m = new MessageBox( hopShell(), SWT.OK | SWT.ICON_WARNING );
    m.setText( BaseMessages.getString( PKG, "JobLog.Dialog.JobHasChangedSave.Title" ) );
    m.setMessage( BaseMessages.getString( PKG, "JobLog.Dialog.JobHasChangedSave.Message" ) );
    m.open();
  }

  private JobEntryListener createRefreshJobEntryListener() {
    return new JobEntryListener() {

      public void beforeExecution( Job job, JobEntryCopy jobEntryCopy, JobEntryInterface jobEntryInterface ) {
        asyncRedraw();
      }

      public void afterExecution( Job job, JobEntryCopy jobEntryCopy, JobEntryInterface jobEntryInterface,
                                  Result result ) {
        asyncRedraw();
      }
    };
  }

  /**
   * This gets called at the very end, when everything is done.
   */
  protected void jobFinished() {
    // Do a final check to see if it all ended...
    //
    if ( job != null && job.isInitialized() && job.isFinished() ) {
      jobMetricsDelegate.resetLastRefreshTime();
      jobMetricsDelegate.updateGraph();
      log.logMinimal( BaseMessages.getString( PKG, "JobLog.Log.JobHasEnded" ) );
    }
    updateGui();
  }

  public synchronized void stopJob() {
    if ( job != null && job.isActive() && job.isInitialized() ) {
      job.stopAll();
      job.waitUntilFinished( 5000 ); // wait until everything is stopped, maximum 5 seconds...

      log.logMinimal( BaseMessages.getString( PKG, "JobLog.Log.JobWasStopped" ) );
    }
    updateGui();
  }

  public HasLogChannelInterface getLogChannelProvider() {
    return new HasLogChannelInterface() {
      @Override
      public LogChannelInterface getLogChannel() {
        return getJob() != null ? getJob().getLogChannel() : getJobMeta().getLogChannel();
      }
    };
  }

  // Change of transform, connection, hop or note...
  public void addUndoPosition( Object[] obj, int[] pos, Point[] prev, Point[] curr ) {
    addUndoPosition( obj, pos, prev, curr, false );
  }

  // Change of transform, connection, hop or note...
  public void addUndoPosition( Object[] obj, int[] pos, Point[] prev, Point[] curr, boolean nextAlso ) {
    // It's better to store the indexes of the objects, not the objects itself!
    jobMeta.addUndo( obj, null, pos, prev, curr, PipelineMeta.TYPE_UNDO_POSITION, nextAlso );
    hopUi.setUndoMenu( jobMeta );
  }

  public void handleJobMetaChanges( JobMeta jobMeta ) throws HopException {
    if ( jobMeta.hasChanged() ) {
      if ( hopUi.getProps().getAutoSave() ) {
        if ( log.isDetailed() ) {
          log.logDetailed( BaseMessages.getString( PKG, "JobLog.Log.AutoSaveFileBeforeRunning" ) );
        }
        save();
      } else {
        MessageDialogWithToggle md =
          new MessageDialogWithToggle(
            hopShell(), BaseMessages.getString( PKG, "JobLog.Dialog.SaveChangedFile.Title" ), null, BaseMessages
            .getString( PKG, "JobLog.Dialog.SaveChangedFile.Message" )
            + Const.CR
            + BaseMessages.getString( PKG, "JobLog.Dialog.SaveChangedFile.Message2" )
            + Const.CR,
            MessageDialog.QUESTION,
            new String[] {
              BaseMessages.getString( PKG, "System.Button.Yes" ),
              BaseMessages.getString( PKG, "System.Button.No" ) },
            0, BaseMessages.getString( PKG, "JobLog.Dialog.SaveChangedFile.Toggle" ), hopUi.getProps().getAutoSave() );
        int answer = md.open();
        if ( ( answer & 0xFF ) == 0 ) {
          save();
        }
        hopUi.getProps().setAutoSave( md.getToggleState() );
      }
    }
  }

  private JobEntryCopy lastChained = null;

  public void addJobEntryToChain( String typeDesc, boolean shift ) {

    //Is the lastChained entry still valid?
    //
    if ( lastChained != null && jobMeta.findJobEntry( lastChained.getName(), lastChained.getNr() ) == null ) {
      lastChained = null;
    }

    // If there is exactly one selected transform, pick that one as last chained.
    //
    List<JobEntryCopy> sel = jobMeta.getSelectedEntries();
    if ( sel.size() == 1 ) {
      lastChained = sel.get( 0 );
    }

    // Where do we add this?

    Point p = null;
    if ( lastChained == null ) {
      p = jobMeta.getMaximum();
      p.x -= 100;
    } else {
      p = new Point( lastChained.getLocation().x, lastChained.getLocation().y );
    }

    p.x += 200;

    // Which is the new entry?

    JobEntryCopy newEntry = jobEntryDelegate.newJobEntry( jobMeta, null, typeDesc, false, p );
    if ( newEntry == null ) {
      return;
    }
    newEntry.setLocation( p.x, p.y );

    if ( lastChained != null ) {
      jobHopDelegate.newHop( jobMeta, lastChained, newEntry );
    }

    lastChained = newEntry;
    updateGui();

    if ( shift ) {
      editEntry( newEntry );
    }

    jobMeta.unselectAll();
    newEntry.setSelected( true );
    updateGui();
  }

  public JobMeta getJobMeta() {
    return jobMeta;
  }

  public Job getJob() {
    return job;
  }

  @Override public LogChannelInterface getLogChannel() {
    return log;
  }

  // TODO
  public void editJobEntry( JobMeta jobMeta, JobEntryCopy entryCopy ) {
  }

  @Override public String getFilename() {
    return jobMeta.getFilename();
  }

  @Override public void setFilename( String filename ) {
    jobMeta.setFilename( filename );
  }

  /**
   * Gets hopUi
   *
   * @return value of hopUi
   */
  public HopGui getHopUi() {
    return hopUi;
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
  public LogChannelInterface getLog() {
    return log;
  }

  /**
   * @param log The log to set
   */
  public void setLog( LogChannelInterface log ) {
    this.log = log;
  }

  /**
   * Gets props
   *
   * @return value of props
   */
  public PropsUI getProps() {
    return props;
  }

  /**
   * @param props The props to set
   */
  public void setProps( PropsUI props ) {
    this.props = props;
  }

  /**
   * @param hopUi The hopUi to set
   */
  public void setHopUi( HopGui hopUi ) {
    this.hopUi = hopUi;
  }

  /**
   * Gets zoomLabel
   *
   * @return value of zoomLabel
   */
  public Combo getZoomLabel() {
    return zoomLabel;
  }

  /**
   * @param zoomLabel The zoomLabel to set
   */
  public void setZoomLabel( Combo zoomLabel ) {
    this.zoomLabel = zoomLabel;
  }

  /**
   * Gets fileType
   *
   * @return value of fileType
   */
  @Override public HopJobFileType getFileType() {
    return fileType;
  }

  /**
   * @param fileType The fileType to set
   */
  public void setFileType( HopJobFileType fileType ) {
    this.fileType = fileType;
  }

  @Override public List<IGuiContextHandler> getContextHandlers() {
    return null;
  }
}
