package org.apache.hop.ui.hopgui;

import org.apache.commons.io.output.TeeOutputStream;
import org.apache.hop.cluster.ClusterSchema;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.UndoInterface;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.GuiMenuElement;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
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
import org.apache.hop.ui.hopgui.delegates.HopGuiFileDelegate;
import org.apache.hop.ui.hopgui.delegates.HopGuiUndoDelegate;
import org.apache.hop.ui.hopgui.file.HopFileTypeHandlerInterface;
import org.apache.hop.ui.hopgui.file.HopFileTypeInterface;
import org.apache.hop.ui.hopgui.perspective.HopGuiPerspectiveManager;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePluginType;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.hopui.Sleak;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.KeyListener;
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
public class HopGui {
  private static Class<?> PKG = HopUi.class;

  // The main Menu IDs
  public static final String ID_MAIN_MENU = "HopGui-Menu";
  public static final String ID_MAIN_MENU_FILE = "menu-10000-file";
  public static final String ID_MAIN_MENU_FILE_NEW = "menu-100010-file-new";
  public static final String ID_MAIN_MENU_FILE_OPEN = "menu-100020-file-open";
  public static final String ID_MAIN_MENU_FILE_SAVE = "menu-100030-file-save";
  public static final String ID_MAIN_MENU_FILE_SAVE_AS = "menu-10040-file-save-as";
  public static final String ID_MAIN_MENU_FILE_CLOSE = "menu-10090-file-close";
  public static final String ID_MAIN_MENU_FILE_CLOSE_ALL = "menu-10100-file-close-all";
  public static final String ID_MAIN_MENU_FILE_EXIT = "menu-10900-file-exit";
  public static final String ID_MAIN_MENU_EDIT_PARENT_ID = "menu-20000-edit";
  public static final String ID_MAIN_MENU_EDIT_UNDO = "menu-20010-edit-undo";
  public static final String ID_MAIN_MENU_EDIT_REDO = "menu-20020-edit-redo";

  // The main toolbar IDs
  public static final String ID_MAIN_TOOLBAR = "HopGui-Toolbar";
  public static final String ID_MAIN_TOOLBAR_NEW = "toolbar-10010-new";
  public static final String ID_MAIN_TOOLBAR_OPEN = "toolbar-10010-open";
  public static final String ID_MAIN_TOOLBAR_SAVE = "toolbar-10010-save";
  public static final String ID_MAIN_TOOLBAR_SAVE_AS = "toolbar-10010-save-as";

  public static final String GUI_PLUGIN_PERSPECTIVES_PARENT_ID = "HopGui-Perspectives";


  private static final String UNDO_UNAVAILABLE = BaseMessages.getString( PKG, "Spoon.Menu.Undo.NotAvailable" );
  private static final String REDO_UNAVAILABLE = BaseMessages.getString( PKG, "Spoon.Menu.Redo.NotAvailable" );

  private static HopGui hopGui;

  private DelegatingMetaStore metaStore;
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
  public MetaStoreManager<PartitionSchema> partitionManager;
  public MetaStoreManager<ClusterSchema> clusterManager;
  public HopGuiFileDelegate fileDelegate;
  public HopGuiUndoDelegate undoDelegate;

  private HopGui( Display display ) {
    this.display = display;
    commandLineArguments = new ArrayList<>();
    variableSpace = Variables.getADefaultVariableSpace();
    props = PropsUI.getInstance();
    log = LogChannel.UI;

    databaseMetaManager = new MetaStoreManager<>( variableSpace, metaStore, DatabaseMeta.class, shell );
    partitionManager = new MetaStoreManager<>( variableSpace, metaStore, PartitionSchema.class, shell );
    clusterManager = new MetaStoreManager<>( variableSpace, metaStore, ClusterSchema.class, shell );

    fileDelegate = new HopGuiFileDelegate( this );
    undoDelegate = new HopGuiUndoDelegate( this );

    // TODO: create metastore plugin system
    //
    metaStore = new DelegatingMetaStore(  );
    try {
      IMetaStore localMetaStore = MetaStoreConst.openLocalHopMetaStore();
      metaStore.addMetaStore( localMetaStore );
      metaStore.setActiveMetaStoreName( localMetaStore.getName() );

    } catch ( MetaStoreException e ) {
      new ErrorDialog( shell, "Error opening Hop Metastore", "Unable to open the local Hop Metastore", e );
    }
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
    handleFileCapabilities( null );
  }

  @GuiMenuElement( id = ID_MAIN_MENU_FILE, type = GuiElementType.MENU_ITEM, label = "&File", parentId = ID_MAIN_MENU )
  public void menuFile() {
    // Nothing is done here.
  }

  @GuiMenuElement( id = ID_MAIN_MENU_FILE_NEW, type = GuiElementType.MENU_ITEM, label = "New", parentId = ID_MAIN_MENU_FILE )
  @GuiToolbarElement( id = ID_MAIN_TOOLBAR_NEW, type = GuiElementType.TOOLBAR_BUTTON, image = "ui/images/new.svg", toolTip = "New", parentId = ID_MAIN_TOOLBAR )
  public void menuFileNew() {
    System.out.println( "fileNew" );
  }

  @GuiMenuElement( id = ID_MAIN_MENU_FILE_OPEN, type = GuiElementType.MENU_ITEM, label = "Open", parentId = ID_MAIN_MENU_FILE )
  @GuiToolbarElement( id = ID_MAIN_TOOLBAR_OPEN, type = GuiElementType.TOOLBAR_BUTTON, image = "ui/images/open.svg", toolTip = "Open", parentId = ID_MAIN_TOOLBAR, separator = true )
  @GuiKeyboardShortcut( control = true, key = 'o' )
  public void menuFileOpen() {
    fileDelegate.fileOpen();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_FILE_SAVE, type = GuiElementType.MENU_ITEM, label = "Save", parentId = ID_MAIN_MENU_FILE )
  @GuiToolbarElement( id = ID_MAIN_TOOLBAR_SAVE, type = GuiElementType.TOOLBAR_BUTTON, image = "ui/images/save.svg", toolTip = "Save", parentId = ID_MAIN_TOOLBAR )
  @GuiKeyboardShortcut( control = true, key = 's' )
  public void menuFileSave() {
    fileDelegate.fileSave();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_FILE_SAVE_AS, type = GuiElementType.MENU_ITEM, label = "Save As...", parentId = ID_MAIN_MENU_FILE )
  @GuiToolbarElement( id = ID_MAIN_TOOLBAR_SAVE_AS, type = GuiElementType.TOOLBAR_BUTTON, image = "ui/images/saveas.svg", toolTip = "Save as...", parentId = ID_MAIN_TOOLBAR )
  public void menuFileSaveAs() {
    System.out.println( "fileSaveAs" );
  }

  @GuiMenuElement( id = ID_MAIN_MENU_FILE_CLOSE, type = GuiElementType.MENU_ITEM, label = "Close", parentId = ID_MAIN_MENU_FILE, separator = true )
  public void menuFileClose() {
    fileDelegate.fileClose();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_FILE_CLOSE_ALL, type = GuiElementType.MENU_ITEM, label = "Close all", parentId = ID_MAIN_MENU_FILE )
  public void menuFileCloseAll() {
    System.out.println("TODO: implement HopGui.menuFileCloseAll()");
  }

  @GuiMenuElement( id = ID_MAIN_MENU_FILE_EXIT, type = GuiElementType.MENU_ITEM, label = "Exit", parentId = ID_MAIN_MENU_FILE, separator = true )
  public void menuFileExit() {
    System.out.println("TODO: implement HopGui.menuFileExit()");
  }



  @GuiMenuElement( id = ID_MAIN_MENU_EDIT_PARENT_ID, type = GuiElementType.MENU_ITEM, label = "Edit", parentId = ID_MAIN_MENU )
  public void menuEdit() {
    // Nothing is done here.
  }

  @GuiMenuElement( id = ID_MAIN_MENU_EDIT_UNDO, type = GuiElementType.MENU_ITEM, label = "Undo", parentId = ID_MAIN_MENU_EDIT_PARENT_ID )
  public void menuEditUndo() {
    HopFileTypeHandlerInterface handler = getActiveFileTypeHandler();
    if (handler==null) {
      return;
    }
    handler.undo();
  }

  @GuiMenuElement( id = ID_MAIN_MENU_EDIT_REDO, type = GuiElementType.MENU_ITEM, label = "Redo", parentId = ID_MAIN_MENU_EDIT_PARENT_ID )
  public void menuEditRedo() {
    HopFileTypeHandlerInterface handler = getActiveFileTypeHandler();
    if (handler==null) {
      return;
    }
    handler.redo();
  }


  protected void addMainToolbar() {
    mainToolbar = new ToolBar( shell, SWT.BORDER | SWT.WRAP | SWT.SHADOW_OUT | SWT.LEFT | SWT.HORIZONTAL );
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

    perspectivesToolbar = new ToolBar( mainHopGuiComposite, SWT.BORDER | SWT.WRAP | SWT.SHADOW_OUT | SWT.RIGHT | SWT.VERTICAL );
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

    redoItem.setEnabled( next != null );
    if ( next == null ) {
      redoItem.setText( REDO_UNAVAILABLE );
    } else {
      redoItem.setText( BaseMessages.getString( PKG, "Spoon.Menu.Redo.Available", next.toString() ) );
    }
  }

  /**
   * We're given a bunch of capabilities from {@link HopFileTypeInterface}
   * In this method we'll enable/disable menu and toolbar items
   *
   * @param fileType The type of file to handle giving you its capabilities to take into account from {@link HopFileTypeInterface} or set by a plugin
   */
  public void handleFileCapabilities( HopFileTypeInterface fileType ) {

    boolean saveEnabled = false;
    boolean saveAsEnabled = false;
    boolean closeEnabled = false;

    if (fileType!=null) {
      saveEnabled = fileType.getCapabilities().getProperty( HopFileTypeInterface.CAPABILITY_SAVE )!=null;
      saveAsEnabled = fileType.getCapabilities().getProperty( HopFileTypeInterface.CAPABILITY_SAVE_AS )!=null;
      closeEnabled = fileType.getCapabilities().getProperty( HopFileTypeInterface.CAPABILITY_CLOSE )!=null;
    }

    mainMenuWidgets.findMenuItem( ID_MAIN_MENU_FILE_SAVE ).setEnabled( saveEnabled );
    mainMenuWidgets.findMenuItem( ID_MAIN_MENU_FILE_SAVE_AS ).setEnabled( saveAsEnabled );
    mainMenuWidgets.findMenuItem( ID_MAIN_MENU_FILE_CLOSE ).setEnabled( closeEnabled );
  }

  public HopFileTypeHandlerInterface getActiveFileTypeHandler() {
    IHopPerspective perspective = getActivePerspective();
    if (perspective==null) {
      return null;
    }
    return perspective.getActiveFileTypeHandler();
  }

  public void addKeyBoardListeners( Control control, Object parentObject, boolean addHopGuiListeners) {
    if (addHopGuiListeners) {
      addKeyBoardListeners( control, this, false );
    }

    List<KeyboardShortcut> shortcuts = GuiRegistry.getInstance().getKeyboardShortcuts( parentObject.getClass().getName() );
    if (shortcuts==null) {
      return;
    }
    for (KeyboardShortcut shortcut : shortcuts) {
      control.addKeyListener( new KeyListener() {
        @Override public void keyPressed( KeyEvent e ) {
          if (e.keyCode!=shortcut.getKeyCode()) {
            return;
          }
          if (shortcut.isAlt() && (e.stateMask&SWT.ALT)==0) {
            return;
          }
          if (shortcut.isShift() && (e.stateMask&SWT.SHIFT)==0) {
            return;
          }
          if (shortcut.isControl() && (e.stateMask&SWT.CONTROL)==0) {
            return;
          }
          // This is the key
          // Call the method
          //
          try {
            Class<?> parentClass = parentObject.getClass();
            Method method = parentClass.getMethod( shortcut.getParentMethodName() );
            if (method!=null) {
              method.invoke( parentObject );
            }
          } catch(Exception ex) {
            LogChannel.UI.logError( "Error calling keyboard shortcut method on parent object "+parentObject.toString(), ex );
          }
        }

        @Override public void keyReleased( KeyEvent e ) {
        }
      } );
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
}
