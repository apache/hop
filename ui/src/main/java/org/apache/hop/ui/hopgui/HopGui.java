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

package org.apache.hop.ui.hopgui;

import org.apache.commons.io.output.TeeOutputStream;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Props;
import org.apache.hop.core.config.DescribedVariable;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.IUndo;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.KeyboardShortcut;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.parameters.INamedParameterDefinitions;
import org.apache.hop.core.plugins.Plugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.search.ISearchableProvider;
import org.apache.hop.core.search.ISearchablesLocation;
import org.apache.hop.core.undo.ChangeAction;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.i18n.LanguageChoice;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.server.HopServer;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.bus.HopGuiEventsHandler;
import org.apache.hop.ui.core.dialog.EnterOptionsDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.HopDescribedVariablesDialog;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.OsHelper;
import org.apache.hop.ui.hopgui.context.IActionContextHandlersProvider;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.context.metadata.MetadataContext;
import org.apache.hop.ui.hopgui.delegates.HopGuiAuditDelegate;
import org.apache.hop.ui.hopgui.delegates.HopGuiContextDelegate;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileDelegate;
import org.apache.hop.ui.hopgui.delegates.HopGuiUndoDelegate;
import org.apache.hop.ui.hopgui.dialog.AboutDialog;
import org.apache.hop.ui.hopgui.file.HopFileTypeRegistry;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyFileType;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.perspective.EmptyHopPerspective;
import org.apache.hop.ui.hopgui.perspective.HopPerspectiveManager;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePluginType;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.ui.hopgui.perspective.search.HopSearchPerspective;
import org.apache.hop.ui.hopgui.search.HopGuiSearchLocation;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StackLayout;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

@GuiPlugin(description = "The main hop graphical user interface")
public class HopGui
    implements IActionContextHandlersProvider, ISearchableProvider, IHasHopMetadataProvider {
  private static final Class<?> PKG = HopGui.class;

  // The main Menu IDs
  public static final String ID_MAIN_MENU = "HopGui-Menu";
  public static final String ID_MAIN_MENU_FILE = "10000-menu-file";
  public static final String ID_MAIN_MENU_FILE_NEW = "10010-menu-file-new";
  public static final String ID_MAIN_MENU_FILE_OPEN = "10020-menu-file-open";
  public static final String ID_MAIN_MENU_FILE_OPEN_RECENT = "10025-menu-file-open-recent";
  public static final String ID_MAIN_MENU_FILE_SAVE = "10030-menu-file-save";
  public static final String ID_MAIN_MENU_FILE_SAVE_AS = "10040-menu-file-save-as";
  public static final String ID_MAIN_MENU_FILE_EXPORT_TO_SVG = "10050-menu-file-export-to-svg";
  public static final String ID_MAIN_MENU_FILE_CLOSE = "10090-menu-file-close";
  public static final String ID_MAIN_MENU_FILE_CLOSE_ALL = "10100-menu-file-close-all";
  public static final String ID_MAIN_MENU_FILE_EXIT = "10900-menu-file-exit";

  public static final String ID_MAIN_MENU_EDIT_PARENT_ID = "20000-menu-edit";
  public static final String ID_MAIN_MENU_EDIT_UNDO = "20010-menu-edit-undo";
  public static final String ID_MAIN_MENU_EDIT_REDO = "20020-menu-edit-redo";
  public static final String ID_MAIN_MENU_EDIT_SELECT_ALL = "20050-menu-edit-select-all";
  public static final String ID_MAIN_MENU_EDIT_UNSELECT_ALL = "20060-menu-edit-unselect-all";
  public static final String ID_MAIN_MENU_EDIT_FIND = "20070-menu-edit-find";
  public static final String ID_MAIN_MENU_EDIT_COPY = "20080-menu-edit-copy";
  public static final String ID_MAIN_MENU_EDIT_PASTE = "20090-menu-edit-paste";
  public static final String ID_MAIN_MENU_EDIT_CUT = "20100-menu-edit-cut";
  public static final String ID_MAIN_MENU_EDIT_DELETE = "20110-menu-edit-delete";
  public static final String ID_MAIN_MENU_EDIT_NAV_PREV = "20200-menu-edit-nav-previous";
  public static final String ID_MAIN_MENU_EDIT_NAV_NEXT = "20210-menu-edit-nav-next";

  public static final String ID_MAIN_MENU_RUN_PARENT_ID = "30000-menu-run";
  public static final String ID_MAIN_MENU_RUN_START = "30010-menu-run-execute";
  public static final String ID_MAIN_MENU_RUN_PAUSE = "30030-menu-run-pause";
  public static final String ID_MAIN_MENU_RUN_RESUME = "30035-menu-run-resume";
  public static final String ID_MAIN_MENU_RUN_STOP = "30040-menu-run-stop";
  public static final String ID_MAIN_MENU_RUN_PREVIEW = "30050-menu-run-preview";
  public static final String ID_MAIN_MENU_RUN_DEBUG = "30060-menu-run-debug";

  public static final String ID_MAIN_MENU_TOOLS_PARENT_ID = "40000-menu-tools";
  public static final String ID_MAIN_MENU_TOOLS_OPTIONS = "40010-menu-tools-options";
  public static final String ID_MAIN_MENU_TOOLS_SYSPROPS = "40020-menu-tools-system-properties";
  public static final String ID_MAIN_MENU_TOOLS_DATABASE_CLEAR_CACHE =
      "40030-menu-tools-database-clearcache";

  public static final String ID_MAIN_MENU_HELP_PARENT_ID = "90000-menu-help";
  public static final String ID_MAIN_MENU_HELP_ABOUT = "90009-menu-help-about";

  // The main toolbar IDs
  public static final String ID_MAIN_TOOLBAR = "HopGui-Toolbar";
  public static final String ID_MAIN_TOOLBAR_NEW = "toolbar-10010-new";
  public static final String ID_MAIN_TOOLBAR_OPEN = "toolbar-10020-open";
  public static final String ID_MAIN_TOOLBAR_SAVE = "toolbar-10040-save";
  public static final String ID_MAIN_TOOLBAR_SAVE_AS = "toolbar-10050-save-as";

  public static final String GUI_PLUGIN_PERSPECTIVES_PARENT_ID = "HopGui-Perspectives";

  public static final String DEFAULT_HOP_GUI_NAMESPACE = "hop-gui";

  private static final String UNDO_UNAVAILABLE =
      BaseMessages.getString(PKG, "HopGui.Menu.Undo.NotAvailable");
  private static final String REDO_UNAVAILABLE =
      BaseMessages.getString(PKG, "HopGui.Menu.Redo.NotAvailable");

  public static final String APP_NAME = "Hop";

  private String id;

  private IHopMetadataProvider metadataProvider;

  private HopGuiEventsHandler eventsHandler;

  private ILoggingObject loggingObject;
  private ILogChannel log;

  private Shell shell;
  private Display display;
  private List<String> commandLineArguments;
  private IVariables variables;
  private PropsUi props;

  private Menu mainMenu;
  private GuiMenuWidgets mainMenuWidgets;
  private Composite mainHopGuiComposite;

  private ToolBar mainToolbar;
  private GuiToolbarWidgets mainToolbarWidgets;

  private ToolBar perspectivesToolbar;
  private Composite mainPerspectivesComposite;
  private HopPerspectiveManager perspectiveManager;
  private IHopPerspective activePerspective;

  private static PrintStream originalSystemOut = System.out;
  private static PrintStream originalSystemErr = System.err;

  public MetadataManager<DatabaseMeta> databaseMetaManager;
  public MetadataManager<HopServer> hopServerManager;
  public MetadataManager<PartitionSchema> partitionManager;

  public HopGuiFileDelegate fileDelegate;
  public HopGuiUndoDelegate undoDelegate;
  public HopGuiContextDelegate contextDelegate;
  public HopGuiAuditDelegate auditDelegate;

  private boolean openingLastFiles;

  protected HopGui() {
    this(Display.getCurrent());
  }

  private HopGui(Display display) {
    this.display = display;
    this.id = UUID.randomUUID().toString();

    commandLineArguments = new ArrayList<>();
    variables = Variables.getADefaultVariableSpace();

    loggingObject = new LoggingObject(APP_NAME);
    log = new LogChannel(APP_NAME);

    activePerspective = new EmptyHopPerspective();

    fileDelegate = new HopGuiFileDelegate(this);
    undoDelegate = new HopGuiUndoDelegate(this);
    contextDelegate = new HopGuiContextDelegate(this);
    auditDelegate = new HopGuiAuditDelegate(this);

    // TODO: create metadata plugin system
    //
    metadataProvider = HopMetadataUtil.getStandardHopMetadataProvider(variables);

    eventsHandler = new HopGuiEventsHandler();

    updateMetadataManagers();

    HopNamespace.setNamespace(DEFAULT_HOP_GUI_NAMESPACE);
    shell = new Shell(display, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
  }

  private void updateMetadataManagers() {
    databaseMetaManager = new MetadataManager<>(variables, metadataProvider, DatabaseMeta.class);
    hopServerManager = new MetadataManager<>(variables, metadataProvider, HopServer.class);
    partitionManager = new MetadataManager<>(variables, metadataProvider, PartitionSchema.class);
  }

  private static final ISingletonProvider PROVIDER;

  static {
    PROVIDER = (ISingletonProvider) ImplementationLoader.newInstance(HopGui.class);
  }

  public static final HopGui getInstance() {
    return (HopGui) PROVIDER.getInstanceInternal();
  }

  public static void main(String[] arguments) {
    try {

      /*
            System.out.println("Sleeping for 10s for debugging");
            Thread.sleep(10000);
      */

      setupConsoleLogging();
      HopEnvironment.init();
      OsHelper.setAppName();
      Display display = setupDisplay();

      // Note: this needs to be done before the look and feel is set
      OsHelper.initOsHandlers(display);

      // Initialize the logging backend
      //
      HopLogStore.init();
      Locale.setDefault(LanguageChoice.getInstance().getDefaultLocale());

      HopGui hopGui = HopGui.getInstance();
      hopGui.getCommandLineArguments().addAll(Arrays.asList(arguments));
      hopGui.setProps(PropsUi.getInstance());

      // Add and load the Hop GUI Plugins...
      // - Load perspectives
      //
      HopGuiEnvironment.init();

      try {
        ExtensionPointHandler.callExtensionPoint(
            hopGui.getLog(), hopGui.variables, HopExtensionPoint.HopGuiInit.id, hopGui);
      } catch (Exception e) {
        hopGui.getLog().logError("Error calling extension point plugin(s) for HopGuiInit", e);
      }

      boolean errors = false;
      try {
        hopGui.open();
      } catch (Throwable e) {
        originalSystemErr.println(
            "Serious error detected in the Hop GUI: "
                + e.getMessage()
                + Const.CR
                + Const.getStackTracker(e));
        errors = true;
      }

      System.exit(errors ? 1 : 0);
    } catch (Throwable e) {
      originalSystemErr.println("Error starting the Hop GUI: " + e.getMessage());
      e.printStackTrace(originalSystemErr);
      System.exit(1);
    }
  }

  /** Build the shell */
  protected void open() {
    shell.setImage(GuiResource.getInstance().getImageHopUi());

    shell.setText(BaseMessages.getString(PKG, "HopGui.Application.Name"));
    addMainMenu();
    addMainToolbar();
    addPerspectivesToolbar();
    addMainPerspectivesComposite();

    handleFileCapabilities(new EmptyFileType(), false, false);

    loadPerspectives();

    replaceKeyboardShortcutListeners(this);

    shell.addListener(SWT.Close, this::closeEvent);

    BaseTransformDialog.setSize(shell);

    // Open the Hop GUI shell
    //
    shell.open();
    if (EnvironmentUtils.getInstance().isWeb()) {
      shell.setMaximized(true);
    }

    display.asyncExec(
        () -> {
          openingLastFiles = true;

          // Here the projects plugin opens up the last project/environment.
          // It sets openingLastFiles to false so the block below is not executed.
          //
          try {
            ExtensionPointHandler.callExtensionPoint(
                log, variables, HopExtensionPoint.HopGuiStart.id, this);
          } catch (Exception e) {
            new ErrorDialog(
                shell,
                "Error",
                "Error calling extension point '" + HopExtensionPoint.HopGuiStart.id + "'",
                e);
          }

          // Open the previously used files. Extension points can disable this
          //
          if (openingLastFiles) {
            auditDelegate.openLastFiles();
          }
        });

    // On RAP, return here otherwise UIThread doesn't get terminated properly.
    if (EnvironmentUtils.getInstance().isWeb()) {
      return;
    }
    boolean retry = true;
    while (retry) {
      try {
        // Wait until the Hop GUI is closed/disposed/killed
        //
        while (!shell.isDisposed()) {
          if (!display.readAndDispatch()) {
            display.sleep();
          }
        }
        retry = false;
      } catch (Throwable throwable) {
        System.err.println(
            "Error in the Hop GUI : "
                + throwable.getMessage()
                + Const.CR
                + Const.getClassicStackTrace(throwable));
      }
    }
    display.dispose();
  }

  private void closeEvent(Event event) {
    event.doit = fileDelegate.fileExit();
  }

  private void loadPerspectives() {
    try {
      // Pre-load the perspectives and store them in the manager as well as the GuiRegistry
      //
      perspectiveManager = new HopPerspectiveManager(this);
      PluginRegistry pluginRegistry = PluginRegistry.getInstance();
      boolean first = true;
      List<Plugin> perspectivePlugins = pluginRegistry.getPlugins(HopPerspectivePluginType.class);

      // Sort by id
      //
      Collections.sort(perspectivePlugins, Comparator.comparing(p -> p.getIds()[0]));

      for (Plugin perspectivePlugin : perspectivePlugins) {
        Class<IHopPerspective> perspectiveClass =
            pluginRegistry.getClass(perspectivePlugin, IHopPerspective.class);

        // Create a new instance & initialize.
        //
        IHopPerspective perspective = perspectiveClass.newInstance();
        perspective.initialize(this, mainPerspectivesComposite);
        perspectiveManager.addPerspective(perspective);

        // Create a toolbar item
        //
        ToolItem item = new ToolItem(this.perspectivesToolbar, SWT.RADIO);
        item.setToolTipText(Const.NVL(perspectivePlugin.getName(), perspective.getId()));
        item.setData(perspective);
        item.addListener(
            SWT.Selection,
            (event) -> setActivePerspective((IHopPerspective) event.widget.getData()));

        ClassLoader classLoader = pluginRegistry.getClassLoader(perspectivePlugin);
        Image image =
            GuiResource.getInstance()
                .getImage(
                    perspectivePlugin.getImageFile(),
                    classLoader,
                    ConstUi.SMALL_ICON_SIZE,
                    ConstUi.SMALL_ICON_SIZE);
        if (image != null) {
          item.setImage(image);
        }

        if (first) {
          first = false;
          item.setSelection(true);
        }
      }
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error loading perspectives", e);
    }
  }

  private static Display setupDisplay() {
    // Bootstrap Hop
    //
    Display display = new Display();
    return display;
  }

  private static void setupConsoleLogging() {
    boolean doConsoleRedirect = !Boolean.getBoolean("HopUi.Console.Redirect.Disabled");
    if (doConsoleRedirect) {
      try {
        Path parent = Paths.get(Const.HOP_AUDIT_FOLDER);
        Files.createDirectories(parent);
        Files.deleteIfExists(Paths.get(parent.toString(), "hopui.log"));
        Path path = Files.createFile(Paths.get(parent.toString(), "hopui.log"));
        System.setProperty("LOG_PATH", path.toString());
        final FileOutputStream fos = new FileOutputStream(path.toFile());
        System.setOut(new PrintStream(new TeeOutputStream(originalSystemOut, fos)));
        System.setErr(new PrintStream(new TeeOutputStream(originalSystemErr, fos)));
        HopLogStore.OriginalSystemOut = System.out;
        HopLogStore.OriginalSystemErr = System.err;
      } catch (Throwable ignored) {
        // ignored
      }
    }
  }

  private void addMainMenu() {
    mainMenu = new Menu(shell, SWT.BAR);

    mainMenuWidgets = new GuiMenuWidgets();
    mainMenuWidgets.registerGuiPluginObject(this);
    mainMenuWidgets.createMenuWidgets(ID_MAIN_MENU, shell, mainMenu);

    if (EnvironmentUtils.getInstance().isWeb()) {
      mainMenuWidgets.enableMenuItem(HopGui.ID_MAIN_MENU_FILE_EXIT, false);
    }

    shell.setMenuBar(mainMenu);
    setUndoMenu(null);
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_FILE,
      label = "i18n::HopGui.Menu.File",
      parentId = ID_MAIN_MENU)
  public void menuFile() {
    // Nothing is done here.
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_FILE_NEW,
      label = "i18n::HopGui.Menu.File.New",
      image = "ui/images/add.svg",
      parentId = ID_MAIN_MENU_FILE)
  @GuiToolbarElement(
      root = ID_MAIN_TOOLBAR,
      id = ID_MAIN_TOOLBAR_NEW,
      image = "ui/images/add.svg",
      toolTip = "i18n::HopGui.Menu.File.New")
  @GuiKeyboardShortcut(control = true, key = 'n')
  @GuiOsxKeyboardShortcut(command = true, key = 'n')
  public void menuFileNew() {
    contextDelegate.fileNew();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_FILE_OPEN,
      label = "i18n::HopGui.Menu.File.Open",
      image = "ui/images/open.svg",
      parentId = ID_MAIN_MENU_FILE)
  @GuiToolbarElement(
      root = ID_MAIN_TOOLBAR,
      id = ID_MAIN_TOOLBAR_OPEN,
      image = "ui/images/open.svg",
      toolTip = "i18n::HopGui.Menu.File.Open",
      separator = true)
  @GuiKeyboardShortcut(control = true, key = 'o')
  @GuiOsxKeyboardShortcut(command = true, key = 'o')
  public void menuFileOpen() {
    fileDelegate.fileOpen();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_FILE_OPEN_RECENT,
      label = "i18n::HopGui.Menu.File.Open.Recent",
      image = "ui/images/open.svg",
      parentId = ID_MAIN_MENU_FILE)
  public void menuFileOpenRecent() {
    fileDelegate.fileOpenRecent();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_FILE_SAVE,
      label = "i18n::HopGui.Menu.File.Save",
      image = "ui/images/save.svg",
      parentId = ID_MAIN_MENU_FILE)
  @GuiToolbarElement(
      root = ID_MAIN_TOOLBAR,
      id = ID_MAIN_TOOLBAR_SAVE,
      image = "ui/images/save.svg",
      toolTip = "i18n::HopGui.Menu.File.Save")
  @GuiKeyboardShortcut(control = true, key = 's')
  @GuiOsxKeyboardShortcut(command = true, key = 's')
  public void menuFileSave() {
    fileDelegate.fileSave();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_FILE_SAVE_AS,
      label = "i18n::HopGui.Menu.File.SaveAs",
      image = "ui/images/save-as.svg",
      parentId = ID_MAIN_MENU_FILE)
  @GuiToolbarElement(
      root = ID_MAIN_TOOLBAR,
      id = ID_MAIN_TOOLBAR_SAVE_AS,
      image = "ui/images/save-as.svg",
      toolTip = "i18n::HopGui.Menu.File.SaveAs")
  @GuiKeyboardShortcut(control = true, shift=true, key = 's')
  @GuiOsxKeyboardShortcut(command = true, shift=true, key = 's')  
  public void menuFileSaveAs() {
    fileDelegate.fileSaveAs();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_FILE_EXPORT_TO_SVG,
      separator = true,
      label = "i18n::HopGui.Menu.File.ExportToSVG",
      image = "ui/images/image.svg",
      parentId = ID_MAIN_MENU_FILE)
  public void menuFileExportToSvg() {
    fileDelegate.exportToSvg();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_FILE_CLOSE,
      label = "i18n::HopGui.Menu.File.Close",
      parentId = ID_MAIN_MENU_FILE,
      separator = true)
  @GuiKeyboardShortcut(control = true, key = 'w')
  @GuiOsxKeyboardShortcut(command = true, key = 'w')
  public void menuFileClose() {
    fileDelegate.fileClose();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_FILE_CLOSE_ALL,
      label = "i18n::HopGui.Menu.File.Close.All",
      parentId = ID_MAIN_MENU_FILE)
  @GuiKeyboardShortcut(control = true, shift = true, key = 'w')
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'w')
  public void menuFileCloseAll() {
    if (fileDelegate.saveGuardAllFiles()) {
      fileDelegate.closeAllFiles();
    }
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_FILE_EXIT,
      label = "i18n::HopGui.Menu.File.Quit",
      parentId = ID_MAIN_MENU_FILE,
      separator = true)
  @GuiKeyboardShortcut(control = true, key = 'q')
  @GuiOsxKeyboardShortcut(command = true, key = 'q')
  public void menuFileExit() {

    if (fileDelegate.fileExit()) {
      // Save the shell size and position
      //
      props.setScreen(new WindowProperty(shell));

      shell.dispose();
    }
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_EDIT_PARENT_ID,
      label = "i18n::HopGui.Menu.Edit",
      parentId = ID_MAIN_MENU)
  public void menuEdit() {
    // Nothing is done here.
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_EDIT_UNDO,
      label = "i18n::HopGui.Menu.Edit.Undo",
      image = "ui/images/undo.svg",
      parentId = ID_MAIN_MENU_EDIT_PARENT_ID)
  @GuiKeyboardShortcut(control = true, key = 'z')
  @GuiOsxKeyboardShortcut(command = true, key = 'z')
  public void menuEditUndo() {
    getActiveFileTypeHandler().undo();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_EDIT_REDO,
      label = "i18n::HopGui.Menu.Edit.Redo",
      image = "ui/images/redo.svg",
      parentId = ID_MAIN_MENU_EDIT_PARENT_ID)
  @GuiKeyboardShortcut(control = true, shift = true, key = 'z')
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'z')
  public void menuEditRedo() {
    getActiveFileTypeHandler().redo();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_EDIT_SELECT_ALL,
      label = "i18n::HopGui.Menu.Edit.SelectAll",
      parentId = ID_MAIN_MENU_EDIT_PARENT_ID,
      separator = true)
  @GuiKeyboardShortcut(control = true, key = 'a')
  @GuiOsxKeyboardShortcut(command = true, key = 'a')
  public void menuEditSelectAll() {
    getActiveFileTypeHandler().selectAll();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_EDIT_UNSELECT_ALL,
      label = "i18n::HopGui.Menu.Edit.ClearSelection",
      parentId = ID_MAIN_MENU_EDIT_PARENT_ID)
  @GuiKeyboardShortcut(key = SWT.ESC)
  @GuiOsxKeyboardShortcut(key = SWT.ESC)
  public void menuEditUnselectAll() {
    getActiveFileTypeHandler().unselectAll();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_EDIT_FIND,
      label = "i18n::HopGui.Menu.Edit.Find",
      image = "ui/images/search.svg",
      parentId = ID_MAIN_MENU_EDIT_PARENT_ID)
  @GuiKeyboardShortcut(key = 'f', control = true)
  @GuiOsxKeyboardShortcut(key = 'f', command = true)
  public void menuEditFind() {
    IHopPerspective perspective = perspectiveManager.findPerspective(HopSearchPerspective.class);
    if (perspective != null) {
      ((HopSearchPerspective) perspective).activate();
    }
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_EDIT_COPY,
      label = "i18n::HopGui.Menu.Edit.CopyToClipboard",
      image = "ui/images/copy.svg",
      parentId = ID_MAIN_MENU_EDIT_PARENT_ID,
      separator = true)
  @GuiKeyboardShortcut(control = true, key = 'c')
  @GuiOsxKeyboardShortcut(command = true, key = 'c')
  public void menuEditCopySelected() {
    getActiveFileTypeHandler().copySelectedToClipboard();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_EDIT_PASTE,
      label = "i18n::HopGui.Menu.Edit.PasteFromClipboard",
      image = "ui/images/paste.svg",
      parentId = ID_MAIN_MENU_EDIT_PARENT_ID)
  @GuiKeyboardShortcut(control = true, key = 'v')
  @GuiOsxKeyboardShortcut(command = true, key = 'v')
  public void menuEditPaste() {
    getActiveFileTypeHandler().pasteFromClipboard();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_EDIT_CUT,
      label = "i18n::HopGui.Menu.Edit.Cut",
      image = "ui/images/cut.svg",
      parentId = ID_MAIN_MENU_EDIT_PARENT_ID)
  @GuiKeyboardShortcut(control = true, key = 'x')
  @GuiOsxKeyboardShortcut(command = true, key = 'x')
  public void menuEditCutSelected() {
    getActiveFileTypeHandler().cutSelectedToClipboard();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_EDIT_DELETE,
      label = "i18n::HopGui.Menu.Edit.Delete",
      image = "ui/images/delete.svg",
      parentId = ID_MAIN_MENU_EDIT_PARENT_ID)
  @GuiKeyboardShortcut(control = false, key = SWT.DEL)
  @GuiOsxKeyboardShortcut(command = false, key = SWT.DEL)
  public void menuEditDeleteSelected() {
    getActiveFileTypeHandler().deleteSelected();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_EDIT_NAV_PREV,
      label = "i18n::HopGui.Menu.Edit.Navigate.Previous",
      parentId = ID_MAIN_MENU_EDIT_PARENT_ID,
      separator = true)
  @GuiKeyboardShortcut(alt = true, key = SWT.ARROW_LEFT)
  public void menuEditNavigatePreviousFile() {
    getActivePerspective().navigateToPreviousFile();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_EDIT_NAV_NEXT,
      label = "i18n::HopGui.Menu.Edit.Navigate.Next",
      parentId = ID_MAIN_MENU_EDIT_PARENT_ID)
  @GuiKeyboardShortcut(alt = true, key = SWT.ARROW_RIGHT)
  public void menuEditNavigateNextFile() {
    getActivePerspective().navigateToNextFile();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_RUN_PARENT_ID,
      label = "i18n::HopGui.Menu.Run",
      parentId = ID_MAIN_MENU)
  public void menuRun() {
    // Nothing is done here.
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_RUN_START,
      label = "i18n::HopGui.Menu.Run.StartExecution",
      image = "ui/images/run.svg",
      parentId = ID_MAIN_MENU_RUN_PARENT_ID)
  @GuiKeyboardShortcut(key = SWT.F8)
  public void menuRunStart() {
    getActiveFileTypeHandler().start();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_RUN_STOP,
      label = "i18n::HopGui.Menu.Run.StopExecution",
      image = "ui/images/stop.svg",
      parentId = ID_MAIN_MENU_RUN_PARENT_ID)
  public void menuRunStop() {
    getActiveFileTypeHandler().stop();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_RUN_PAUSE,
      label = "i18n::HopGui.Menu.Run.PauseExecution",
      image = "ui/images/pause.svg",
      parentId = ID_MAIN_MENU_RUN_PARENT_ID,
      separator = true)
  public void menuRunPause() {
    getActiveFileTypeHandler().pause();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_RUN_RESUME,
      label = "i18n::HopGui.Menu.Run.ResumeExecution",
      image = "ui/images/run.svg",
      parentId = ID_MAIN_MENU_RUN_PARENT_ID)
  public void menuRunResume() {
    getActiveFileTypeHandler().pause();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_RUN_PREVIEW,
      label = "i18n::HopGui.Menu.Run.Preview",
      image = "ui/images/preview.svg",
      parentId = ID_MAIN_MENU_RUN_PARENT_ID,
      separator = true)
  public void menuRunPreview() {
    getActiveFileTypeHandler().preview();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_RUN_DEBUG,
      label = "i18n::HopGui.Menu.Run.Debug",
      image = "ui/images/debug.svg",
      parentId = ID_MAIN_MENU_RUN_PARENT_ID)
  public void menuRunDebug() {
    getActiveFileTypeHandler().debug();
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_PARENT_ID,
      label = "i18n::HopGui.Menu.Tools",
      parentId = ID_MAIN_MENU)
  public void menuTools() {
    // Nothing is done here.
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_OPTIONS,
      label = "i18n::HopGui.Menu.Edit.Options",
      parentId = ID_MAIN_MENU_TOOLS_PARENT_ID)
  public void menuToolsOptions() {
    if (new EnterOptionsDialog(getShell()).open() != null) {
      try {
        HopConfig.getInstance().saveToFile();
      } catch (Exception e) {
        new ErrorDialog(
            getShell(),
            "Error",
            "Error saving the configuration file '"
                + HopConfig.getInstance().getConfigFilename()
                + "'",
            e);
      }
    }
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_SYSPROPS,
      label = "i18n::HopGui.Menu.Tools.EditConfigVariables",
      parentId = ID_MAIN_MENU_TOOLS_PARENT_ID)
  public void menuToolsEditConfigVariables() {
    List<DescribedVariable> describedVariables = HopConfig.getInstance().getDescribedVariables();
    String message = "Editing file: " + HopConfig.getInstance().getConfigFilename();
    HopDescribedVariablesDialog dialog =
        new HopDescribedVariablesDialog(shell, message, describedVariables, null);
    if (dialog.open() != null) {
      try {
        HopConfig.getInstance().setDescribedVariables(describedVariables);
        HopConfig.getInstance().saveToFile();
      } catch (Exception e) {
        new ErrorDialog(
            getShell(),
            "Error",
            "Error saving config variables to configuration file '"
                + HopConfig.getInstance().getConfigFilename()
                + "'",
            e);
      }
    }
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_DATABASE_CLEAR_CACHE,
      label = "i18n::HopGui.Menu.Tools.DatabaseClearCache",
      parentId = ID_MAIN_MENU_TOOLS_PARENT_ID,
      separator = true)
  public void menuToolsDatabaseClearCache() {
    DbCache.getInstance().clear(null);
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_HELP_PARENT_ID,
      label = "i18n::HopGui.Menu.Help",
      parentId = ID_MAIN_MENU)
  public void menuHelp() {
    // Nothing is done here.
  }

  @GuiMenuElement(
      root = ID_MAIN_MENU,
      id = ID_MAIN_MENU_HELP_ABOUT,
      label = "i18n::HopGui.Menu.Help.About",
      parentId = ID_MAIN_MENU_HELP_PARENT_ID)
  public void menuHelpAbout() {
    AboutDialog dialog = new AboutDialog(getShell());
    dialog.open();
  }

  protected void addMainToolbar() {
    mainToolbar = new ToolBar(shell, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment(0, 0);
    fdToolBar.top = new FormAttachment(0, 0);
    fdToolBar.right = new FormAttachment(100, 0);
    mainToolbar.setLayoutData(fdToolBar);
    props.setLook(mainToolbar, Props.WIDGET_STYLE_TOOLBAR);

    mainToolbarWidgets = new GuiToolbarWidgets();
    mainToolbarWidgets.registerGuiPluginObject(this);
    mainToolbarWidgets.createToolbarWidgets(mainToolbar, ID_MAIN_TOOLBAR);
    mainToolbar.pack();
  }

  protected void addPerspectivesToolbar() {
    // We can't mix horizontal and vertical toolbars so we need to add a composite.
    //
    shell.setLayout(new FormLayout());
    mainHopGuiComposite = new Composite(shell, SWT.NO_BACKGROUND);
    mainHopGuiComposite.setLayout(new FormLayout());
    FormData formData = new FormData();
    formData.left = new FormAttachment(0, 0);
    formData.right = new FormAttachment(100, 0);
    formData.top = new FormAttachment(mainToolbar, 0);
    formData.bottom = new FormAttachment(100, 0);
    mainHopGuiComposite.setLayoutData(formData);

    perspectivesToolbar = new ToolBar(mainHopGuiComposite, SWT.WRAP | SWT.RIGHT | SWT.VERTICAL);
    props.setLook(perspectivesToolbar, PropsUi.WIDGET_STYLE_TOOLBAR);
    FormData fdToolBar = new FormData();
    fdToolBar.left = new FormAttachment(0, 0);
    fdToolBar.top = new FormAttachment(0, 0);
    fdToolBar.bottom = new FormAttachment(100, 0);
    perspectivesToolbar.setLayoutData(fdToolBar);
  }

  /**
   * Add a main composite where the various perspectives can parent on to show stuff... Its area is
   * to just below the main toolbar and to the right of the perspectives toolbar
   */
  private void addMainPerspectivesComposite() {
    mainPerspectivesComposite = new Composite(mainHopGuiComposite, SWT.NO_BACKGROUND);
    mainPerspectivesComposite.setLayout(new StackLayout());
    FormData fdMain = new FormData();
    fdMain.top = new FormAttachment(0, 0);
    fdMain.left = new FormAttachment(perspectivesToolbar, 0);
    fdMain.bottom = new FormAttachment(100, 0);
    fdMain.right = new FormAttachment(100, 0);
    mainPerspectivesComposite.setLayoutData(fdMain);
  }

  public void setUndoMenu(IUndo undoInterface) {
    // Grab the undo and redo menu items...
    //
    MenuItem undoItem = mainMenuWidgets.findMenuItem(ID_MAIN_MENU_EDIT_UNDO);
    MenuItem redoItem = mainMenuWidgets.findMenuItem(ID_MAIN_MENU_EDIT_REDO);
    if (undoItem == null || redoItem == null || undoItem.isDisposed() || redoItem.isDisposed()) {
      return;
    }

    ChangeAction prev = null;
    ChangeAction next = null;

    if (undoInterface != null) {
      prev = undoInterface.viewThisUndo();
      next = undoInterface.viewNextUndo();
    }

    undoItem.setEnabled(prev != null);
    if (prev == null) {
      undoItem.setText(UNDO_UNAVAILABLE);
    } else {
      undoItem.setText(BaseMessages.getString(PKG, "HopGui.Menu.Undo.Available", prev.toString()));
    }
    KeyboardShortcut undoShortcut = mainMenuWidgets.findKeyboardShortcut(ID_MAIN_MENU_EDIT_UNDO);
    if (undoShortcut != null) {
      GuiMenuWidgets.appendShortCut(undoItem, undoShortcut);
    }

    redoItem.setEnabled(next != null);
    if (next == null) {
      redoItem.setText(REDO_UNAVAILABLE);
    } else {
      redoItem.setText(BaseMessages.getString(PKG, "HopGui.Menu.Redo.Available", next.toString()));
    }
    KeyboardShortcut redoShortcut = mainMenuWidgets.findKeyboardShortcut(ID_MAIN_MENU_EDIT_REDO);
    if (redoShortcut != null) {
      GuiMenuWidgets.appendShortCut(redoItem, redoShortcut);
    }
  }

  /**
   * We're given a bunch of capabilities from {@link IHopFileType} In this method we'll
   * enable/disable menu and toolbar items
   *
   * @param fileType The type of file to handle giving you its capabilities to take into account
   *     from {@link IHopFileType} or set by a plugin
   * @param running set this to true if the current file is running
   * @param paused set this to true if the current file is paused
   */
  public void handleFileCapabilities(IHopFileType<?> fileType, boolean running, boolean paused) {

    mainMenuWidgets.enableMenuItem(fileType, ID_MAIN_MENU_FILE_SAVE, IHopFileType.CAPABILITY_SAVE);
    mainMenuWidgets.enableMenuItem(
        fileType, ID_MAIN_MENU_FILE_SAVE_AS, IHopFileType.CAPABILITY_SAVE_AS);
    mainMenuWidgets.enableMenuItem(
        fileType, ID_MAIN_MENU_FILE_EXPORT_TO_SVG, IHopFileType.CAPABILITY_EXPORT_TO_SVG);
    mainMenuWidgets.enableMenuItem(
        fileType, ID_MAIN_MENU_FILE_CLOSE, IHopFileType.CAPABILITY_CLOSE);
    mainMenuWidgets.enableMenuItem(
        fileType, ID_MAIN_MENU_FILE_CLOSE_ALL, IHopFileType.CAPABILITY_CLOSE);

    mainMenuWidgets.enableMenuItem(
        fileType, ID_MAIN_MENU_EDIT_SELECT_ALL, IHopFileType.CAPABILITY_SELECT);
    mainMenuWidgets.enableMenuItem(
        fileType, ID_MAIN_MENU_EDIT_UNSELECT_ALL, IHopFileType.CAPABILITY_SELECT);

    mainMenuWidgets.enableMenuItem(fileType, ID_MAIN_MENU_EDIT_COPY, IHopFileType.CAPABILITY_COPY);
    mainMenuWidgets.enableMenuItem(
        fileType, ID_MAIN_MENU_EDIT_PASTE, IHopFileType.CAPABILITY_PASTE);
    mainMenuWidgets.enableMenuItem(fileType, ID_MAIN_MENU_EDIT_CUT, IHopFileType.CAPABILITY_CUT);
    mainMenuWidgets.enableMenuItem(
        fileType, ID_MAIN_MENU_EDIT_DELETE, IHopFileType.CAPABILITY_DELETE);

    mainMenuWidgets.enableMenuItem(
        fileType, ID_MAIN_MENU_RUN_START, IHopFileType.CAPABILITY_START, !running);
    mainMenuWidgets.enableMenuItem(
        fileType, ID_MAIN_MENU_RUN_STOP, IHopFileType.CAPABILITY_STOP, running);
    mainMenuWidgets.enableMenuItem(
        fileType, ID_MAIN_MENU_RUN_PAUSE, IHopFileType.CAPABILITY_PAUSE, running && !paused);
    mainMenuWidgets.enableMenuItem(
        fileType, ID_MAIN_MENU_RUN_RESUME, IHopFileType.CAPABILITY_PAUSE, paused);
    mainMenuWidgets.enableMenuItem(
        fileType, ID_MAIN_MENU_RUN_PREVIEW, IHopFileType.CAPABILITY_PREVIEW);
    mainMenuWidgets.enableMenuItem(fileType, ID_MAIN_MENU_RUN_DEBUG, IHopFileType.CAPABILITY_DEBUG);

    mainMenuWidgets.enableMenuItem(
        fileType,
        ID_MAIN_MENU_EDIT_NAV_PREV,
        IHopFileType.CAPABILITY_FILE_HISTORY,
        getActivePerspective().hasNavigationPreviousFile());
    mainMenuWidgets.enableMenuItem(
        fileType,
        ID_MAIN_MENU_EDIT_NAV_NEXT,
        IHopFileType.CAPABILITY_FILE_HISTORY,
        getActivePerspective().hasNavigationNextFile());

    mainToolbarWidgets.enableToolbarItem(
        fileType, ID_MAIN_TOOLBAR_SAVE, IHopFileType.CAPABILITY_SAVE);
    mainToolbarWidgets.enableToolbarItem(
        fileType, ID_MAIN_TOOLBAR_SAVE_AS, IHopFileType.CAPABILITY_SAVE_AS);
  }

  public IHopFileTypeHandler getActiveFileTypeHandler() {
    return getActivePerspective().getActiveFileTypeHandler();
  }

  /**
   * Replace the listeners based on the @{@link GuiKeyboardShortcut} annotations
   *
   * @param parentObject The parent object containing the annotations and methods
   */
  public void replaceKeyboardShortcutListeners(Object parentObject) {
    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(parentObject);
    replaceKeyboardShortcutListeners(shell, keyHandler);
  }

  public void replaceKeyboardShortcutListeners(Control control, HopGuiKeyHandler keyHandler) {

    control.removeKeyListener(keyHandler);
    control.addKeyListener(keyHandler);

    // Add it to all the children as well so we don't have any focus issues
    //
    if (control instanceof Composite) {
      for (Control child : ((Composite) control).getChildren()) {
        replaceKeyboardShortcutListeners(child, keyHandler);
      }
    }
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  public IHopMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /** @param metadataProvider The metadataProvider to set */
  public void setMetadataProvider(IHopMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
    updateMetadataManagers();
  }

  /**
   * Gets shell
   *
   * @return value of shell
   */
  public Shell getShell() {
    return shell;
  }

  public void setShell(Shell shell) {
    this.shell = shell;
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

  /** @param commandLineArguments The commandLineArguments to set */
  public void setCommandLineArguments(List<String> commandLineArguments) {
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

  /** @param mainPerspectivesComposite The mainPerspectivesComposite to set */
  public void setMainPerspectivesComposite(Composite mainPerspectivesComposite) {
    this.mainPerspectivesComposite = mainPerspectivesComposite;
  }

  /**
   * Gets perspectiveManager
   *
   * @return value of perspectiveManager
   */
  public HopPerspectiveManager getPerspectiveManager() {
    return perspectiveManager;
  }

  /** @param perspectiveManager The perspectiveManager to set */
  public void setPerspectiveManager(HopPerspectiveManager perspectiveManager) {
    this.perspectiveManager = perspectiveManager;
  }

  /**
   * Gets the variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /** @param variables The variables to set */
  public void setVariables(IVariables variables) {
    this.variables = variables;
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

  /**
   * Gets log
   *
   * @return value of log
   */
  public ILogChannel getLog() {
    return log;
  }

  /**
   * Gets mainMenu
   *
   * @return value of mainMenu
   */
  public Menu getMainMenu() {
    return mainMenu;
  }

  /** @param mainMenu The mainMenu to set */
  public void setMainMenu(Menu mainMenu) {
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

  /** @param mainToolbar The mainToolbar to set */
  public void setMainToolbar(ToolBar mainToolbar) {
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

  /** @param perspectivesToolbar The perspectivesToolbar to set */
  public void setPerspectivesToolbar(ToolBar perspectivesToolbar) {
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

  /** @param mainHopGuiComposite The mainHopGuiComposite to set */
  public void setMainHopGuiComposite(Composite mainHopGuiComposite) {
    this.mainHopGuiComposite = mainHopGuiComposite;
  }

  /** @param perspective The perspective to active */
  public void setActivePerspective(IHopPerspective perspective) {

    if (perspective == null) {
      perspective = getDataOrchestrationPerspective();
    }

    activePerspective = perspective;

    // Move perspective control on top
    //
    StackLayout layout = (StackLayout) mainPerspectivesComposite.getLayout();
    layout.topControl = perspective.getControl();
    mainPerspectivesComposite.layout();

    // Select toolbar item
    //
    if (perspectivesToolbar != null && !perspectivesToolbar.isDisposed()) {
      for (ToolItem item : perspectivesToolbar.getItems()) {
        item.setSelection(perspective.equals(item.getData()));
      }
    }

    // Notify the perspective that it has been activated.
    //
    perspective.perspectiveActivated();

    perspectiveManager.notifyPerspectiveActiviated(perspective);
  }

  public boolean isActivePerspective(IHopPerspective perspective) {
    if (perspective != null) {
      for (ToolItem item : perspectivesToolbar.getItems()) {
        if (perspective.equals(item.getData())) {
          return item.getSelection();
        }
      }
    }
    return false;
  }

  /**
   * What are the contexts to consider: - the file types registered - the available metadata types
   *
   * @return The list of context handlers
   */
  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    List<IGuiContextHandler> contextHandlers = new ArrayList<>();

    // Get all the file context handlers
    //
    HopFileTypeRegistry registry = HopFileTypeRegistry.getInstance();
    List<IHopFileType> hopFileTypes = registry.getFileTypes();
    for (IHopFileType hopFileType : hopFileTypes) {
      contextHandlers.addAll(hopFileType.getContextHandlers());
    }

    // Get all the metadata context handlers...
    //
    contextHandlers.addAll(new MetadataContext(this, metadataProvider).getContextHandlers());

    return contextHandlers;
  }

  public void setParametersAsVariablesInUI(
      INamedParameterDefinitions namedParameters, IVariables variables) {
    for (String param : namedParameters.listParameters()) {
      try {
        variables.setVariable(
            param,
            Const.NVL(
                variables.getVariable(param),
                Const.NVL(namedParameters.getParameterDefault(param), "")));
      } catch (Exception e) {
        // ignore this
      }
    }
  }

  /**
   * Convenience method to pick up the active pipeline graph
   *
   * @return The active pipeline graph or null if none is active
   */
  public static HopGuiPipelineGraph getActivePipelineGraph() {
    IHopPerspective activePerspective = HopGui.getInstance().getActivePerspective();
    if (!(activePerspective instanceof HopDataOrchestrationPerspective)) {
      return null;
    }
    HopDataOrchestrationPerspective perspective =
        (HopDataOrchestrationPerspective) activePerspective;
    IHopFileTypeHandler typeHandler = perspective.getActiveFileTypeHandler();
    if (!(typeHandler instanceof HopGuiPipelineGraph)) {
      return null;
    }
    return (HopGuiPipelineGraph) typeHandler;
  }

  public static HopGuiWorkflowGraph getActiveWorkflowGraph() {
    IHopPerspective activePerspective = HopGui.getInstance().getActivePerspective();
    if (!(activePerspective instanceof HopDataOrchestrationPerspective)) {
      return null;
    }
    HopDataOrchestrationPerspective perspective =
        (HopDataOrchestrationPerspective) activePerspective;
    IHopFileTypeHandler typeHandler = perspective.getActiveFileTypeHandler();
    if (!(typeHandler instanceof HopGuiWorkflowGraph)) {
      return null;
    }
    return (HopGuiWorkflowGraph) typeHandler;
  }

  public static HopDataOrchestrationPerspective getDataOrchestrationPerspective() {
    return (HopDataOrchestrationPerspective)
        HopGui.getInstance()
            .getPerspectiveManager()
            .findPerspective(HopDataOrchestrationPerspective.class);
  }

  /**
   * Create a list of all the searcheables locations. By default this means HopGui, the the current
   * metadata
   *
   * @return
   */
  @Override
  public List<ISearchablesLocation> getSearchablesLocations() {
    List<ISearchablesLocation> locations = new ArrayList<>();

    locations.add(new HopGuiSearchLocation(this));

    // Allow plugins to add other locations as well
    //
    try {
      ExtensionPointHandler.callExtensionPoint(
          log, variables, HopExtensionPoint.HopGuiGetSearchablesLocations.name(), locations);
    } catch (Exception e) {
      log.logError("Error adding to the list of searchables locations", e);
    }
    return locations;
  }

  public static boolean editConfigFile(
      Shell shell,
      String configFilename,
      DescribedVariablesConfigFile variablesConfigFile,
      String selectedVariable)
      throws HopException {
    String message = "Editing configuration file: " + configFilename;
    HopDescribedVariablesDialog variablesDialog =
        new HopDescribedVariablesDialog(
            shell, message, variablesConfigFile.getDescribedVariables(), selectedVariable);
    List<DescribedVariable> vars = variablesDialog.open();
    if (vars != null) {
      variablesConfigFile.setDescribedVariables(vars);
      variablesConfigFile.saveToFile();
      return true;
    }
    return false;
  }

  /**
   * Gets databaseMetaManager
   *
   * @return value of databaseMetaManager
   */
  public MetadataManager<DatabaseMeta> getDatabaseMetaManager() {
    return databaseMetaManager;
  }

  /** @param databaseMetaManager The databaseMetaManager to set */
  public void setDatabaseMetaManager(MetadataManager<DatabaseMeta> databaseMetaManager) {
    this.databaseMetaManager = databaseMetaManager;
  }

  /**
   * Gets partitionManager
   *
   * @return value of partitionManager
   */
  public MetadataManager<PartitionSchema> getPartitionManager() {
    return partitionManager;
  }

  /** @param partitionManager The partitionManager to set */
  public void setPartitionManager(MetadataManager<PartitionSchema> partitionManager) {
    this.partitionManager = partitionManager;
  }

  /**
   * Gets fileDelegate
   *
   * @return value of fileDelegate
   */
  public HopGuiFileDelegate getFileDelegate() {
    return fileDelegate;
  }

  /** @param fileDelegate The fileDelegate to set */
  public void setFileDelegate(HopGuiFileDelegate fileDelegate) {
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

  /** @param undoDelegate The undoDelegate to set */
  public void setUndoDelegate(HopGuiUndoDelegate undoDelegate) {
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
   * Gets loggingObject
   *
   * @return value of loggingObject
   */
  public ILoggingObject getLoggingObject() {
    return loggingObject;
  }

  /**
   * Gets mainMenuWidgets
   *
   * @return value of mainMenuWidgets
   */
  public GuiMenuWidgets getMainMenuWidgets() {
    return mainMenuWidgets;
  }

  /** @param mainMenuWidgets The mainMenuWidgets to set */
  public void setMainMenuWidgets(GuiMenuWidgets mainMenuWidgets) {
    this.mainMenuWidgets = mainMenuWidgets;
  }

  /**
   * Gets mainToolbarWidgets
   *
   * @return value of mainToolbarWidgets
   */
  public GuiToolbarWidgets getMainToolbarWidgets() {
    return mainToolbarWidgets;
  }

  /** @param mainToolbarWidgets The mainToolbarWidgets to set */
  public void setMainToolbarWidgets(GuiToolbarWidgets mainToolbarWidgets) {
    this.mainToolbarWidgets = mainToolbarWidgets;
  }

  /**
   * Gets openingLastFiles
   *
   * @return value of openingLastFiles
   */
  public boolean isOpeningLastFiles() {
    return openingLastFiles;
  }

  /** @param openingLastFiles The openingLastFiles to set */
  public void setOpeningLastFiles(boolean openingLastFiles) {
    this.openingLastFiles = openingLastFiles;
  }

  /**
   * Gets the unique id of this HopGui instance
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /**
   * Gets eventsHandler
   *
   * @return value of eventsHandler
   */
  public HopGuiEventsHandler getEventsHandler() {
    return eventsHandler;
  }

  /** @param eventsHandler The eventsHandler to set */
  public void setEventsHandler(HopGuiEventsHandler eventsHandler) {
    this.eventsHandler = eventsHandler;
  }
}
