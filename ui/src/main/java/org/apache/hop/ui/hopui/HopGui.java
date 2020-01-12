package org.apache.hop.ui.hopui;

import org.apache.commons.io.output.TeeOutputStream;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.LanguageChoice;
import org.apache.hop.metastore.stores.delegate.DelegatingMetaStore;
import org.apache.hop.ui.core.PropsUI;
import org.apache.hop.ui.core.gui.GUIResource;
import org.apache.hop.ui.core.gui.GuiElementWidgets;
import org.apache.hop.ui.core.widget.OsHelper;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.graphics.DeviceData;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class HopGui {
  private static Class<?> PKG = HopUi.class;

  public static final String GUI_PLUGIN_MENU_PARENT_ID = "HopGui-Menu";
  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "HopGui-Toolbar";

  private static HopGui hopGui;

  private DelegatingMetaStore metaStore;
  private Shell shell;
  private Display display;
  private List<String> commandLineArguments;

  private Menu mainMenu;
  private ToolBar mainToolbar;
  private CTabFolder perspectivesFolder;

  private static PrintStream originalSystemOut = System.out;
  private static PrintStream originalSystemErr = System.err;

  private HopGui() {
    commandLineArguments = new ArrayList<>();
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

      hopGui = new HopGui();
      hopGui.getCommandLineArguments().addAll( Arrays.asList( arguments ) );
      hopGui.open();

      System.exit(0);
    } catch(Throwable e) {
      originalSystemErr.println( "Error starting the Hop GUI: "+e.getMessage() );
      e.printStackTrace( originalSystemErr );
      System.exit(1);
    }
  }

  /**
   * Build the shell
   */
  protected void open() {
    shell = new Shell(display, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    shell.setImage( GUIResource.getInstance().getImageHopUi() );

    addMainMenu();

  }

  private void addMainMenu() {
    mainMenu = new Menu(shell, SWT.BAR);

    GuiElementWidgets guiElementWidgets = new GuiElementWidgets( Variables.getADefaultVariableSpace() );
    guiElementWidgets.createCompositeWidgets( hopGui, mainMenu, GUI_PLUGIN_MENU_PARENT_ID, null );

    // File menu
    //
    MenuItem fileMenuHeader = new MenuItem( mainMenu, SWT.CASCADE );

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
}
