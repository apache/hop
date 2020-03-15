package org.apache.hop.ui.hopgui;

import org.apache.commons.io.output.TeeOutputStream;
import org.apache.hop.cluster.ClusterSchema;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.UndoInterface;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.GuiMenuElement;
import org.apache.hop.core.gui.plugin.GuiOSXKeyboardShortcut;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.KeyboardShortcut;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.plugins.Plugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.undo.TransAction;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.i18n.LanguageChoice;
import org.apache.hop.metastore.MetaStoreConst;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.stores.delegate.DelegatingMetaStore;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.core.metastore.MetaStoreManager;
import org.apache.hop.ui.core.widget.OsHelper;
import org.apache.hop.ui.hopgui.context.IActionContextHandlersProvider;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.context.metastore.MetaStoreContext;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileDelegate;
import org.apache.hop.ui.hopgui.delegates.HopGuiNewDelegate;
import org.apache.hop.ui.hopgui.delegates.HopGuiUndoDelegate;
import org.apache.hop.ui.hopgui.file.empty.EmptyFileType;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypeInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.perspective.EmptyHopPerspective;
import org.apache.hop.ui.hopgui.perspective.HopGuiPerspectiveManager;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePluginType;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.hopui.Sleak;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.DeviceData;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;

import javax.swing.*;
import javax.swing.plaf.metal.MetalLookAndFeel;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

@GuiPlugin(
  id = "HopGUI",
  description = "The main hop graphical user interface"
)
public class HopGui implements IActionContextHandlersProvider {
  private static Class<?> PKG = HopUi.class;

  // The main Menu IDs
  public static final String ID_MAIN_MENU = "HopGui-Menu";
  public static final String ID_MAIN_MENU_FILE               = "10000-menu-file";
  public static final String ID_MAIN_MENU_FILE_NEW           = "10010-menu-file-new";
  public static final String ID_MAIN_MENU_FILE_OPEN          = "10020-menu-file-open";
  public static final String ID_MAIN_MENU_FILE_SAVE          = "10030-menu-file-save";
  public static final String ID_MAIN_MENU_FILE_SAVE_AS       = "10040-menu-file-save-as";
  public static final String ID_MAIN_MENU_FILE_CLOSE         = "10090-menu-file-close";
  public static final String ID_MAIN_MENU_FILE_CLOSE_ALL     = "10100-menu-file-close-all";
  public static final String ID_MAIN_MENU_FILE_EXIT          = "10900-menu-file-exit";

  public static final String ID_MAIN_MENU_EDIT_PARENT_ID     = "20000-menu-edit";
  public static final String ID_MAIN_MENU_EDIT_UNDO          = "20010-menu-edit-undo";
  public static final String ID_MAIN_MENU_EDIT_REDO          = "20020-menu-edit-redo";
  public static final String ID_MAIN_MENU_EDIT_SELECT_ALL    = "20050-menu-edit-select-all";
  public static final String ID_MAIN_MENU_EDIT_UNSELECT_ALL  = "20060-menu-edit-unselect-all";
  public static final String ID_MAIN_MENU_EDIT_COPY          = "20080-menu-edit-copy";
  public static final String ID_MAIN_MENU_EDIT_PASTE         = "20090-menu-edit-paste";
  public static final String ID_MAIN_MENU_EDIT_CUT           = "20100-menu-edit-cut";
  public static final String ID_MAIN_MENU_EDIT_DELETE        = "20110-menu-edit-delete";
  public static final String ID_MAIN_MENU_EDIT_NAV_PREV      = "20200-menu-edit-nav-previous";
  public static final String ID_MAIN_MENU_EDIT_NAV_NEXT      = "20210-menu-edit-nav-next";


  public static final String ID_MAIN_MENU_RUN_PARENT_ID      = "30000-menu-run";
  public static final String ID_MAIN_MENU_RUN_START          = "30010-menu-run-execute";
  public static final String ID_MAIN_MENU_RUN_PAUSE          = "30030-menu-run-pause";
  public static final String ID_MAIN_MENU_RUN_STOP           = "30040-menu-run-stop";
  public static final String ID_MAIN_MENU_RUN_PREVIEW        = "30050-menu-run-preview";
  public static final String ID_MAIN_MENU_RUN_DEBUG          = "30060-menu-run-debug";

  // The main toolbar IDs
  public static final String ID_MAIN_TOOLBAR = "HopGui-Toolbar";
  public static final String ID_MAIN_TOOLBAR_NEW = "toolbar-10010-new";
  public static final String ID_MAIN_TOOLBAR_OPEN = "toolbar-10010-open";
  public static final String ID_MAIN_TOOLBAR_SAVE = "toolbar-10010-save";
  public static final String ID_MAIN_TOOLBAR_SAVE_AS = "toolbar-10010-save-as";

  public static final String GUI_PLUGIN_PERSPECTIVES_PARENT_ID = "HopGui-Perspectives";


  private static final String UNDO_UNAVAILABLE = BaseMessages.getString( PKG, "Spoon.Menu.Undo.NotAvailable" );
  private static final String REDO_UNAVAILABLE = BaseMessages.getString( PKG, "Spoon.Menu.Redo.NotAvailable" );

  private static final String CONTEXT_ID = "HopGui";

  private static HopGui hopGui;

  private DelegatingMetaStore metaStore;
  private MetaStoreContext metaStoreContext;

  private Shell shell;
  private Display display;
  private List<String> commandLineArguments;
  private VariableSpace variableSpace;
  private PropsUI props;
  private LogChannelInterface log;

  private Menu mainMenu;
  private GuiMenuWidgets mainMenuWidgets;
  private Composite mainHopGuiComposite;

  private ToolBar mainToolbar;
  private GuiCompositeWidgets mainToolbarWidgets;

  private ToolBar perspectivesToolbar;
  private GuiCompositeWidgets perspectivesToolbarWidgets;
  private Composite mainPerspectivesComposite;
  private HopGuiPerspectiveManager perspectiveManager;
  private IHopPerspective activePerspective;

  private static PrintStream originalSystemOut = System.out;
  private static PrintStream originalSystemErr = System.err;

  public MetaStoreManager<DatabaseMeta> databaseMetaManager;
  public MetaStoreManager<SlaveServer> slaveServerManager;
  public MetaStoreManager<PartitionSchema> partitionManager;
  public MetaStoreManager<ClusterSchema> clusterManager;

  public HopGuiFileDelegate fileDelegate;
  public HopGuiUndoDelegate undoDelegate;
  public HopGuiNewDelegate newDelegate;

  private HopGui( Display display ) {
    this.display = display;
    commandLineArguments = new ArrayList<>();
    variableSpace = Variables.getADefaultVariableSpace();
    props = PropsUI.getInstance();
    log = LogChannel.UI;

    activePerspective = new EmptyHopPerspective();

    fileDelegate = new HopGuiFileDelegate( this );
    undoDelegate = new HopGuiUndoDelegate( this );
    newDelegate = new HopGuiNewDelegate( this );

    // TODO: create metastore plugin system
    //
    metaStore = new DelegatingMetaStore();
    try {
      IMetaStore localMetaStore = MetaStoreConst.openLocalHopMetaStore();
      metaStore.addMetaStore( localMetaStore );
      metaStore.setActiveMetaStoreName( localMetaStore.getName() );
    } catch ( MetaStoreException e ) {
      new ErrorDialog( shell, "Error opening Hop Metastore", "Unable to open the local Hop Metastore", e );
    }

    databaseMetaManager = new MetaStoreManager<>( variableSpace, metaStore, DatabaseMeta.class );
    slaveServerManager = new MetaStoreManager<>( variableSpace, metaStore, SlaveServer.class );
    partitionManager = new MetaStoreManager<>( variableSpace, metaStore, PartitionSchema.class );
    clusterManager = new MetaStoreManager<>( variableSpace, metaStore, ClusterSchema.class );

    metaStoreContext = new MetaStoreContext( this, metaStore );
  }

  public static final HopGui getInstance() {
    return hopGui;
  }

  public static void main( String[] arguments ) {
    try {
      setupConsoleLogging();
      HopEnvironment.init();
      OsHelper.setAppName();
      Display display = setupDisplay();

      // Note: this needs to be done before the look and feel is set
      OsHelper.initOsHandlers( display );
      UIManager.setLookAndFeel( new MetalLookAndFeel() );

      // Load images and so on...
      //
      PropsUI.init( display );

      // Initialize the logging backend
      //
      HopLogStore.init( PropsUI.getInstance().getMaxNrLinesInLog(), PropsUI.getInstance().getMaxLogLineTimeoutMinutes() );
      Locale.setDefault( LanguageChoice.getInstance().getDefaultLocale() );

      hopGui = new HopGui( display );
      hopGui.getCommandLineArguments().addAll( Arrays.asList( arguments ) );

      // Add and load the Hop GUI Plugins...
      // - Load perspectives
      //
      HopGuiEnvironment.init();

      hopGui.open();

      System.exit( 0 );
    } catch ( Throwable e ) {
      originalSystemErr.println( "Error starting the Hop GUI: " + e.getMessage() );
      e.printStackTrace( originalSystemErr );
      System.exit( 1 );
    }
  }

  /**
   * Build the shell
   */
  protected void open() {
    shell = new Shell( display, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX );
    shell.setImage( GUIResource.getInstance().getImageHopUi() );

    shell.setText( BaseMessages.getString( PKG, "Spoon.Application.Name" ) );
    addMainMenu();
    addMainToolbar();
    addPerspectivesToolbar();
    addMainPerspectivesComposite();

    loadPerspectives();

    replaceKeyboardShortcutListeners( this );

    // Open the Hop GUI shell and wait until it's closed
    //
    // shell.pack();
    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    display.dispose();
  }

  private void loadPerspectives() {
    try {
      perspectiveManager = new HopGuiPerspectiveManager( this, mainPerspectivesComposite );
      PluginRegistry pluginRegistry = PluginRegistry.getInstance();
      boolean first = true;
      List<Plugin> perspectivePlugins = PluginRegistry.getInstance().getPlugins( HopPerspectivePluginType.class );
      // Sort by ID
      //
      Collections.sort( perspectivePlugins, new Comparator<Plugin>() {
        @Override public int compare( Plugin p1, Plugin p2 ) {
          return p1.getIds()[ 0 ].compareTo( p2.getIds()[ 0 ] );
        }
      } );
      for ( Plugin perspectivePlugin : perspectivePlugins ) {
        Class<IHopPerspective> perspectiveClass = pluginRegistry.getClass( perspectivePlugin, IHopPerspective.class );
        Method method = perspectiveClass.getDeclaredMethod( "getInstance" );
        if ( method == null ) {
          throw new HopException( "Unable to find the getInstance() method in class " + perspectiveClass.getName() + " : make it a singleton" );
        }
        // Get the singleton
        //
        IHopPerspective perspective = (IHopPerspective) method.invoke( null );
        perspective.initialize( this, mainPerspectivesComposite );
        perspectiveManager.addPerspective( perspective );
        if ( first ) {
          first = false;
          perspective.show();
          activePerspective = perspective;
        } else {
          perspective.hide();
        }
      }
      mainPerspectivesComposite.layout( true, true );
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error loading perspectives", e );
    }
  }

  private static Display setupDisplay() {
    // Bootstrap Hop
    //
    Display display;
    if ( System.getProperties().containsKey( "SLEAK" ) ) {
      DeviceData data = new DeviceData();
      data.tracking = true;
      display = new Display( data );
      Sleak sleak = new Sleak();
      Shell sleakShell = new Shell( display );
      sleakShell.setText( "S-Leak" );
      org.eclipse.swt.graphics.Point size = sleakShell.getSize();
      sleakShell.setSize( size.x / 2, size.y / 2 );
      sleak.create( sleakShell );
      sleakShell.open();
    } else {
      display = new Display();
    }
    return display;
  }

  private static void setupConsoleLogging() {
    boolean doConsoleRedirect = !Boolean.getBoolean( "HopUi.Console.Redirect.Disabled" );
    if ( doConsoleRedirect ) {
      try {
        Path parent = Paths.get( System.getProperty( "user.dir" ) + File.separator + "logs" );
        Files.createDirectories( parent );
        Files.deleteIfExists( Paths.get( parent.toString(), "hopui.log" ) );
        Path path = Files.createFile( Paths.get( parent.toString(), "hopui.log" ) );
        System.setProperty( "LOG_PATH", path.toString() );
        final FileOutputStream fos = new FileOutputStream( path.toFile() );
        System.setOut( new PrintStream( new TeeOutputStream( originalSystemOut, fos ) ) );
        System.setErr( new PrintStream( new TeeOutputStream( originalSystemErr, fos ) ) );
        HopLogStore.OriginalSystemOut = System.out;
        HopLogStore.OriginalSystemErr = System.err;
      } catch ( Throwable ignored ) {
        // ignored
      }
    }
  }

  private void addMainMenu() {
    mainMenu = new Menu( shell, SWT.BAR );

    mainMenuWidgets = new GuiMenuWidgets( variableSpace );
    mainMenuWidgets.createMenuWidgets( this, shell, mainMenu, ID_MAIN_MENU );

    shell.setMenuBar( mainMenu );
    setUndoMenu( null );
    handleFileCapabilities( new EmptyFileType() );
  }

  @GuiMenuElement( id = ID_MAIN_MENU_FILE, type = GuiElementType.MENU_ITEM, label = "&File", parentId = ID_MAIN_MENU )
  public void menuFile() {
    // Nothing is done here.
  }

  @GuiMenuElement( id = ID_MAIN_MENU_FILE_NEW, type = GuiElementType.MENU_ITEM, label = "New", image = "ui/images/new.svg", parentId = ID_MAIN_MENU_FILE )
  @GuiToolbarElement( id = ID_MAIN_TOOLBAR_NEW, type = GuiElementType.TOOLBAR_BUTTON, image = "ui/images/new.svg", toolTip = "New", parentId = ID_MAIN_TOOLBAR )
  @GuiKeyboardShortcut( control = true, key = 'n' )
  @GuiOSXKeyboardShortcut( command = true, key = 'n' )
  public void menuFileNew() {
    newDelegate.fileNew();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_FILE_OPEN, type = GuiElementType.MENU_ITEM, label = "Open", image = "ui/images/open.svg", parentId = ID_MAIN_MENU_FILE )
  @GuiToolbarElement( id = ID_MAIN_TOOLBAR_OPEN, type = GuiElementType.TOOLBAR_BUTTON, image = "ui/images/open.svg", toolTip = "Open", parentId = ID_MAIN_TOOLBAR, separator = true )
  @GuiKeyboardShortcut( control = true, key = 'o' )
  @GuiOSXKeyboardShortcut( command = true, key = 'o' )
  public void menuFileOpen() {
    fileDelegate.fileOpen();
  }
  
  @GuiMenuElement( id = ID_MAIN_MENU_FILE_SAVE, type = GuiElementType.MENU_ITEM, label = "Save", image = "ui/images/save.svg", parentId = ID_MAIN_MENU_FILE )
  @GuiToolbarElement( id = ID_MAIN_TOOLBAR_SAVE, type = GuiElementType.TOOLBAR_BUTTON, image = "ui/images/save.svg", toolTip = "Save", parentId = ID_MAIN_TOOLBAR )
  @GuiKeyboardShortcut( control = true, key = 's' )
  @GuiOSXKeyboardShortcut( command = true, key = 's' )
  public void menuFileSave() {
    fileDelegate.fileSave();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_FILE_SAVE_AS, type = GuiElementType.MENU_ITEM, label = "Save As...", image = "ui/images/saveas.svg", parentId = ID_MAIN_MENU_FILE )
  @GuiToolbarElement( id = ID_MAIN_TOOLBAR_SAVE_AS, type = GuiElementType.TOOLBAR_BUTTON, image = "ui/images/saveas.svg", toolTip = "Save as...", parentId = ID_MAIN_TOOLBAR )
  public void menuFileSaveAs() {
    System.out.println( "fileSaveAs" );
  }

  @GuiMenuElement( id = ID_MAIN_MENU_FILE_CLOSE, type = GuiElementType.MENU_ITEM, label = "Close", parentId = ID_MAIN_MENU_FILE, separator = true )
  @GuiKeyboardShortcut( control = true, key = 'w' )
  @GuiOSXKeyboardShortcut( command = true, key = 'w' )
  public void menuFileClose() {
    fileDelegate.fileClose();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_FILE_CLOSE_ALL, type = GuiElementType.MENU_ITEM, label = "Close all", parentId = ID_MAIN_MENU_FILE )
  public void menuFileCloseAll() {
    System.out.println( "TODO: implement HopGui.menuFileCloseAll()" );
  }

  @GuiMenuElement( id = ID_MAIN_MENU_FILE_EXIT, type = GuiElementType.MENU_ITEM, label = "Exit", parentId = ID_MAIN_MENU_FILE, separator = true )
  @GuiKeyboardShortcut( control = true, key = 'q' )
  @GuiOSXKeyboardShortcut( command = true, key = 'q' )
  public void menuFileExit() {
    System.out.println( "TODO: implement HopGui.menuFileExit()" );
  }


  @GuiMenuElement( id = ID_MAIN_MENU_EDIT_PARENT_ID, type = GuiElementType.MENU_ITEM, label = "Edit", parentId = ID_MAIN_MENU )
  public void menuEdit() {
    // Nothing is done here.
  }

  @GuiMenuElement( id = ID_MAIN_MENU_EDIT_UNDO, type = GuiElementType.MENU_ITEM, label = "Undo", parentId = ID_MAIN_MENU_EDIT_PARENT_ID )
  @GuiKeyboardShortcut( control = true, key = 'z' )
  @GuiOSXKeyboardShortcut( command = true, key = 'z' )
  public void menuEditUndo() {
    getActiveFileTypeHandler().undo();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_EDIT_REDO, type = GuiElementType.MENU_ITEM, label = "Redo", parentId = ID_MAIN_MENU_EDIT_PARENT_ID )
  @GuiKeyboardShortcut( control = true, shift = true, key = 'z' )
  @GuiOSXKeyboardShortcut( command = true, shift = true, key = 'z' )
  public void menuEditRedo() {
    getActiveFileTypeHandler().redo();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_EDIT_SELECT_ALL, type = GuiElementType.MENU_ITEM, label = "Select all", parentId = ID_MAIN_MENU_EDIT_PARENT_ID, separator = true)
  @GuiKeyboardShortcut( control = true, key = 'a' )
  @GuiOSXKeyboardShortcut( command = true, key = 'a' )
  public void menuEditSelectAll() {
    getActiveFileTypeHandler().selectAll();
  }


  @GuiMenuElement( id = ID_MAIN_MENU_EDIT_UNSELECT_ALL, type = GuiElementType.MENU_ITEM, label = "Clear selection", parentId = ID_MAIN_MENU_EDIT_PARENT_ID )
  @GuiKeyboardShortcut( key = SWT.ESC )
  @GuiOSXKeyboardShortcut( key = SWT.ESC )
  public void menuEditUnselectAll() {
    getActiveFileTypeHandler().unselectAll();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_EDIT_COPY, type = GuiElementType.MENU_ITEM, label = "Copy selected to clipboard", parentId = ID_MAIN_MENU_EDIT_PARENT_ID, separator = true)
  @GuiKeyboardShortcut( control = true, key = 'c' )
  @GuiOSXKeyboardShortcut( command = true, key = 'c' )
  public void menuEditCopySelected() {
    getActiveFileTypeHandler().copySelectedToClipboard();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_EDIT_PASTE, type = GuiElementType.MENU_ITEM, label = "Paste from clipboard", parentId = ID_MAIN_MENU_EDIT_PARENT_ID )
  @GuiKeyboardShortcut( control = true, key = 'v' )
  @GuiOSXKeyboardShortcut( command = true, key = 'v' )
  public void menuEditPaste() {
    getActiveFileTypeHandler().pasteFromClipboard();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_EDIT_CUT, type = GuiElementType.MENU_ITEM, label = "Cut selected to clipboard", parentId = ID_MAIN_MENU_EDIT_PARENT_ID )
  @GuiKeyboardShortcut( control = true, key = 'x' )
  @GuiOSXKeyboardShortcut( command = true, key = 'x' )
  public void menuEditCutSelected() {
    getActiveFileTypeHandler().cutSelectedToClipboard();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_EDIT_DELETE, type = GuiElementType.MENU_ITEM, label = "Delete selected", parentId = ID_MAIN_MENU_EDIT_PARENT_ID )
  @GuiKeyboardShortcut( control = true, key = 'x' )
  @GuiOSXKeyboardShortcut( command = true, key = 'x' )
  public void menuEditDeleteSelected() {
    getActiveFileTypeHandler().deleteSelected();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_EDIT_NAV_PREV, type = GuiElementType.MENU_ITEM, label = "Go to previous file", parentId = ID_MAIN_MENU_EDIT_PARENT_ID, separator = true )
  @GuiKeyboardShortcut( alt = true, key = SWT.ARROW_LEFT )
  public void menuEditNavigatePreviousFile() {
    getActivePerspective().navigateToPreviousFile();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_EDIT_NAV_NEXT, type = GuiElementType.MENU_ITEM, label = "Go to next file", parentId = ID_MAIN_MENU_EDIT_PARENT_ID )
  @GuiKeyboardShortcut( alt = true, key = SWT.ARROW_RIGHT )
  public void menuEditNavigateNextFile() {
    getActivePerspective().navigateToNextFile();
  }


  @GuiMenuElement( id = ID_MAIN_MENU_RUN_PARENT_ID, type = GuiElementType.MENU_ITEM, label = "Run", parentId = ID_MAIN_MENU )
  public void menuRun() {
    // Nothing is done here.
  }

  @GuiMenuElement( id = ID_MAIN_MENU_RUN_START, type = GuiElementType.MENU_ITEM, label = "Start execution",  image = "ui/images/toolbar/run.svg", parentId = ID_MAIN_MENU_RUN_PARENT_ID )
  @GuiKeyboardShortcut( key = SWT.F8 )
  public void menuRunStart() {
    getActiveFileTypeHandler().start();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_RUN_STOP, type = GuiElementType.MENU_ITEM, label = "Stop execution", image = "ui/images/toolbar/stop.svg", parentId = ID_MAIN_MENU_RUN_PARENT_ID )
  public void menuRunStop() {
    getActiveFileTypeHandler().stop();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_RUN_PAUSE, type = GuiElementType.MENU_ITEM, label = "Pause execution", image = "ui/images/toolbar/pause.svg", parentId = ID_MAIN_MENU_RUN_PARENT_ID, separator = true )
  public void menuRunPause() {
    getActiveFileTypeHandler().pause();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_RUN_PREVIEW, type = GuiElementType.MENU_ITEM, label = "Preview", parentId = ID_MAIN_MENU_RUN_PARENT_ID, separator = true )
  public void menuRunPreview() {
    getActiveFileTypeHandler().preview();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_RUN_DEBUG, type = GuiElementType.MENU_ITEM, label = "Debug", parentId = ID_MAIN_MENU_RUN_PARENT_ID )
  public void menuRunDebug() {
    getActiveFileTypeHandler().debug();
  }

  protected void addMainToolbar() {
    mainToolbar = new ToolBar( shell, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL );
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment( 0, 0 );
    fdToolBar.top = new FormAttachment( 0, 0 );
    fdToolBar.right = new FormAttachment( 100, 0 );
    mainToolbar.setLayoutData( fdToolBar );
    props.setLook( mainToolbar, Props.WIDGET_STYLE_TOOLBAR );

    mainToolbarWidgets = new GuiCompositeWidgets( variableSpace );
    mainToolbarWidgets.createCompositeWidgets( this, null, mainToolbar, ID_MAIN_TOOLBAR, null );
    mainToolbar.pack();
  }

  protected void addPerspectivesToolbar() {
    // We can't mix horizontal and vertical toolbars so we need to add a composite.
    //
    shell.setLayout( new FormLayout() );
    mainHopGuiComposite = new Composite( shell, SWT.NO_BACKGROUND );
    mainHopGuiComposite.setLayout( new FormLayout() );
    FormData formData = new FormData();
    formData.left = new FormAttachment( 0, 0 );
    formData.right = new FormAttachment( 100, 0 );
    formData.top = new FormAttachment( mainToolbar, 0 );
    formData.bottom = new FormAttachment( 100, 0 );
    mainHopGuiComposite.setLayoutData( formData );

    perspectivesToolbar = new ToolBar( mainHopGuiComposite,SWT.WRAP | SWT.RIGHT | SWT.VERTICAL );
    props.setLook( perspectivesToolbar, PropsUI.WIDGET_STYLE_TOOLBAR );
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment( 0, 0 );
    fdToolBar.top = new FormAttachment( 0, 0 );
    fdToolBar.bottom = new FormAttachment( 100, 0 );
    perspectivesToolbar.setLayoutData( fdToolBar );

    perspectivesToolbarWidgets = new GuiCompositeWidgets( variableSpace );
    perspectivesToolbarWidgets.createCompositeWidgets( this, GUI_PLUGIN_PERSPECTIVES_PARENT_ID, perspectivesToolbar, GUI_PLUGIN_PERSPECTIVES_PARENT_ID, null );
    perspectivesToolbar.pack();
  }

  /**
   * Add a main composite where the various perspectives can parent on to show stuff...
   * Its area is to just below the main toolbar and to the right of the perspectives toolbar
   */
  private void addMainPerspectivesComposite() {
    mainPerspectivesComposite = new Composite( mainHopGuiComposite, SWT.NO_BACKGROUND );
    mainPerspectivesComposite.setLayout( new FormLayout() );
    FormData fdMain = new FormData();
    fdMain.top = new FormAttachment( 0, 0 );
    fdMain.left = new FormAttachment( perspectivesToolbar, 0 );
    fdMain.bottom = new FormAttachment( 100, 0 );
    fdMain.right = new FormAttachment( 100, 0 );
    mainPerspectivesComposite.setLayoutData( fdMain );
  }


  public void setShellText() {
    // TODO: show current file in main Hop shell
  }

  public void setUndoMenu( UndoInterface undoInterface ) {
    // Grab the undo and redo menu items...
    //
    MenuItem undoItem = mainMenuWidgets.findMenuItem( ID_MAIN_MENU_EDIT_UNDO );
    MenuItem redoItem = mainMenuWidgets.findMenuItem( ID_MAIN_MENU_EDIT_REDO );
    if ( undoItem == null || redoItem == null ) {
      return;
    }

    TransAction prev = null;
    TransAction next = null;

    if ( undoInterface != null ) {
      prev = undoInterface.viewThisUndo();
      next = undoInterface.viewNextUndo();
    }

    undoItem.setEnabled( prev != null );
    if ( prev == null ) {
      undoItem.setText( UNDO_UNAVAILABLE );
    } else {
      undoItem.setText( BaseMessages.getString( PKG, "Spoon.Menu.Undo.Available", prev.toString() ) );
    }
    KeyboardShortcut undoShortcut = mainMenuWidgets.findKeyboardShortcut( ID_MAIN_MENU_EDIT_UNDO );
    if (undoShortcut!=null) {
      GuiMenuWidgets.appendShortCut( undoItem, undoShortcut );
    }

    redoItem.setEnabled( next != null );
    if ( next == null ) {
      redoItem.setText( REDO_UNAVAILABLE );
    } else {
      redoItem.setText( BaseMessages.getString( PKG, "Spoon.Menu.Redo.Available", next.toString() ) );
    }
    KeyboardShortcut redoShortcut = mainMenuWidgets.findKeyboardShortcut( ID_MAIN_MENU_EDIT_REDO );
    if (redoShortcut!=null) {
      GuiMenuWidgets.appendShortCut( redoItem, redoShortcut );
    }
  }

  /**
   * We're given a bunch of capabilities from {@link HopFileTypeInterface}
   * In this method we'll enable/disable menu and toolbar items
   *
   * @param fileType The type of file to handle giving you its capabilities to take into account from {@link HopFileTypeInterface} or set by a plugin
   */
  public void handleFileCapabilities( HopFileTypeInterface fileType ) {

    mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_FILE_SAVE, HopFileTypeInterface.CAPABILITY_SAVE );
    mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_FILE_SAVE_AS, HopFileTypeInterface.CAPABILITY_SAVE );
    mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_FILE_CLOSE, HopFileTypeInterface.CAPABILITY_CLOSE );

    mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_EDIT_SELECT_ALL, HopFileTypeInterface.CAPABILITY_SELECT );
    mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_EDIT_UNSELECT_ALL, HopFileTypeInterface.CAPABILITY_SELECT );

    mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_EDIT_COPY, HopFileTypeInterface.CAPABILITY_COPY );
    mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_EDIT_PASTE, HopFileTypeInterface.CAPABILITY_PASTE );
    mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_EDIT_CUT, HopFileTypeInterface.CAPABILITY_CUT );
    mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_EDIT_DELETE, HopFileTypeInterface.CAPABILITY_DELETE );

    mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_RUN_START, HopFileTypeInterface.CAPABILITY_START );
    mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_RUN_STOP, HopFileTypeInterface.CAPABILITY_STOP );
    mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_RUN_PAUSE, HopFileTypeInterface.CAPABILITY_PAUSE );
    mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_RUN_PREVIEW, HopFileTypeInterface.CAPABILITY_PREVIEW );
    mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_RUN_DEBUG, HopFileTypeInterface.CAPABILITY_DEBUG );

    MenuItem navPrevItem = mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_EDIT_NAV_PREV, HopFileTypeInterface.CAPABILITY_FILE_HISTORY );
    navPrevItem.setEnabled( getActivePerspective().hasNavigationPreviousFile() );
    MenuItem navNextItem = mainMenuWidgets.enableMenuItem( fileType, ID_MAIN_MENU_EDIT_NAV_NEXT, HopFileTypeInterface.CAPABILITY_FILE_HISTORY );
    navNextItem.setEnabled( getActivePerspective().hasNavigationNextFile() );
  }

  public HopFileTypeHandlerInterface getActiveFileTypeHandler() {
    return getActivePerspective().getActiveFileTypeHandler();
  }

  /**
   * Replace the listeners based on the @{@link GuiKeyboardShortcut} annotations
   * @param parentObject The parent object containing the annotations and methods
   */
  public void replaceKeyboardShortcutListeners( Object parentObject ) {
    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle( parentObject );
    replaceKeyboardShortcutListeners( shell, keyHandler );
  }

  public void replaceKeyboardShortcutListeners( Control control, HopGuiKeyHandler keyHandler ) {

    control.removeKeyListener( keyHandler );
    control.addKeyListener( keyHandler );

    // Add it to all the children as well so we don't have any focus issues
    //
    if (control instanceof Composite) {
      for (Control child : ((Composite)control).getChildren()) {
        replaceKeyboardShortcutListeners( child, keyHandler );
      }
    }
  }

  /**
   * Gets metaStore
   *
   * @return value of metaStore
   */
  public DelegatingMetaStore getMetaStore() {
    return metaStore;
  }

  /**
   * @param metaStore The metaStore to set
   */
  public void setMetaStore( DelegatingMetaStore metaStore ) {
    this.metaStore = metaStore;
  }

  /**
   * Gets shell
   *
   * @return value of shell
   */
  public Shell getShell() {
    return shell;
  }

  /**
   * Gets display
   *
   * @return value of display
   */
  public Display getDisplay() {
    return display;
  }

  /**
   * Gets commandLineArguments
   *
   * @return value of commandLineArguments
   */
  public List<String> getCommandLineArguments() {
    return commandLineArguments;
  }

  /**
   * @param commandLineArguments The commandLineArguments to set
   */
  public void setCommandLineArguments( List<String> commandLineArguments ) {
    this.commandLineArguments = commandLineArguments;
  }

  /**
   * Gets mainPerspectivesComposite
   *
   * @return value of mainPerspectivesComposite
   */
  public Composite getMainPerspectivesComposite() {
    return mainPerspectivesComposite;
  }

  /**
   * @param mainPerspectivesComposite The mainPerspectivesComposite to set
   */
  public void setMainPerspectivesComposite( Composite mainPerspectivesComposite ) {
    this.mainPerspectivesComposite = mainPerspectivesComposite;
  }

  /**
   * Gets perspectiveManager
   *
   * @return value of perspectiveManager
   */
  public HopGuiPerspectiveManager getPerspectiveManager() {
    return perspectiveManager;
  }

  /**
   * @param perspectiveManager The perspectiveManager to set
   */
  public void setPerspectiveManager( HopGuiPerspectiveManager perspectiveManager ) {
    this.perspectiveManager = perspectiveManager;
  }

  /**
   * Gets space
   *
   * @return value of space
   */
  public VariableSpace getVariableSpace() {
    return variableSpace;
  }

  /**
   * @param variableSpace The space to set
   */
  public void setVariableSpace( VariableSpace variableSpace ) {
    this.variableSpace = variableSpace;
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
   * Gets mainMenu
   *
   * @return value of mainMenu
   */
  public Menu getMainMenu() {
    return mainMenu;
  }

  /**
   * @param mainMenu The mainMenu to set
   */
  public void setMainMenu( Menu mainMenu ) {
    this.mainMenu = mainMenu;
  }

  /**
   * Gets mainToolbar
   *
   * @return value of mainToolbar
   */
  public ToolBar getMainToolbar() {
    return mainToolbar;
  }

  /**
   * @param mainToolbar The mainToolbar to set
   */
  public void setMainToolbar( ToolBar mainToolbar ) {
    this.mainToolbar = mainToolbar;
  }

  /**
   * Gets perspectivesToolbar
   *
   * @return value of perspectivesToolbar
   */
  public ToolBar getPerspectivesToolbar() {
    return perspectivesToolbar;
  }

  /**
   * @param perspectivesToolbar The perspectivesToolbar to set
   */
  public void setPerspectivesToolbar( ToolBar perspectivesToolbar ) {
    this.perspectivesToolbar = perspectivesToolbar;
  }

  /**
   * Gets mainHopGuiComposite
   *
   * @return value of mainHopGuiComposite
   */
  public Composite getMainHopGuiComposite() {
    return mainHopGuiComposite;
  }

  /**
   * @param mainHopGuiComposite The mainHopGuiComposite to set
   */
  public void setMainHopGuiComposite( Composite mainHopGuiComposite ) {
    this.mainHopGuiComposite = mainHopGuiComposite;
  }

  /**
   * @param activePerspective The activePerspective to set
   */
  public void setActivePerspective( IHopPerspective activePerspective ) {
    this.activePerspective = activePerspective;
    if (activePerspective==null) {
      this.activePerspective=getActivePerspective();
    }
  }

  /**
   *  What are the contexts to consider:
   *      - the file types registered
   *      - the available IMetaStore element types
   *
   * @return The list of context handlers
   */
  @Override public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> contextHandlers = new ArrayList<>();

    // Get all the file context handlers
    //
    HopFileTypeRegistry registry = HopFileTypeRegistry.getInstance();
    List<HopFileTypeInterface> hopFileTypes = registry.getFileTypes();
    for (HopFileTypeInterface hopFileType : hopFileTypes) {
      contextHandlers.addAll( hopFileType.getContextHandlers() );
    }

    // Get all the metastore context handlers...
    //
    contextHandlers.addAll( metaStoreContext.getContextHandlers() );

    return contextHandlers;
  }

  /**
   * Gets databaseMetaManager
   *
   * @return value of databaseMetaManager
   */
  public MetaStoreManager<DatabaseMeta> getDatabaseMetaManager() {
    return databaseMetaManager;
  }

  /**
   * @param databaseMetaManager The databaseMetaManager to set
   */
  public void setDatabaseMetaManager( MetaStoreManager<DatabaseMeta> databaseMetaManager ) {
    this.databaseMetaManager = databaseMetaManager;
  }

  /**
   * Gets partitionManager
   *
   * @return value of partitionManager
   */
  public MetaStoreManager<PartitionSchema> getPartitionManager() {
    return partitionManager;
  }

  /**
   * @param partitionManager The partitionManager to set
   */
  public void setPartitionManager( MetaStoreManager<PartitionSchema> partitionManager ) {
    this.partitionManager = partitionManager;
  }

  /**
   * Gets clusterManager
   *
   * @return value of clusterManager
   */
  public MetaStoreManager<ClusterSchema> getClusterManager() {
    return clusterManager;
  }

  /**
   * @param clusterManager The clusterManager to set
   */
  public void setClusterManager( MetaStoreManager<ClusterSchema> clusterManager ) {
    this.clusterManager = clusterManager;
  }

  /**
   * Gets fileDelegate
   *
   * @return value of fileDelegate
   */
  public HopGuiFileDelegate getFileDelegate() {
    return fileDelegate;
  }

  /**
   * @param fileDelegate The fileDelegate to set
   */
  public void setFileDelegate( HopGuiFileDelegate fileDelegate ) {
    this.fileDelegate = fileDelegate;
  }

  /**
   * Gets undoDelegate
   *
   * @return value of undoDelegate
   */
  public HopGuiUndoDelegate getUndoDelegate() {
    return undoDelegate;
  }

  /**
   * @param undoDelegate The undoDelegate to set
   */
  public void setUndoDelegate( HopGuiUndoDelegate undoDelegate ) {
    this.undoDelegate = undoDelegate;
  }

  /**
   * Gets activePerspective
   *
   * @return value of activePerspective
   */
  public IHopPerspective getActivePerspective() {
    return activePerspective;
  }

  /**
   * Gets metaStoreContext
   *
   * @return value of metaStoreContext
   */
  public MetaStoreContext getMetaStoreContext() {
    return metaStoreContext;
  }



}
