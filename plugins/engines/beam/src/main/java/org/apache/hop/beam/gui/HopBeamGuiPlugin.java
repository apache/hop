package org.apache.hop.beam.gui;

import org.apache.hop.beam.pipeline.fatjar.FatJarBuilder;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.io.File;
import java.io.FilenameFilter;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@GuiPlugin
public class HopBeamGuiPlugin {

  public static final String ID_MAIN_MENU_TOOLS_FAT_JAR = "40200-menu-tools-fat-jar";

  private static HopBeamGuiPlugin instance;

  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static HopBeamGuiPlugin getInstance() {
    if ( instance == null ) {
      instance = new HopBeamGuiPlugin();
    }
    return instance;
  }

  @GuiMenuElement(
    root = HopGui.ID_MAIN_MENU,
    id = ID_MAIN_MENU_TOOLS_FAT_JAR,
    label = "Generate a Hop fat jar...",
    parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
    separator = true
  )
  public void menuToolsFatJar() {
    HopGui hopGui = HopGui.getInstance();
    final Shell shell = hopGui.getShell();

    MessageBox box = new MessageBox( shell, SWT.OK | SWT.CANCEL | SWT.ICON_INFORMATION );
    box.setText( "Create a Hop fat jar file" );
    box.setMessage( "This function generates a single Java library which contains the Hop code and all it's current plugins." + Const.CR +
      "If you hit OK simply select the file to save the so called 'fat jar' file to afterwards. All the code will then be collected automatically." );
    int answer = box.open();
    if ( ( answer & SWT.CANCEL ) != 0 ) {
      return;
    }

    // Ask
    //
    String filename = BaseDialog.presentFileDialog( true, shell,
      new String[] { "*.jar", "*.*" },
      new String[] { "Jar files (*.jar)", "All Files (*.*)" },
      true
    );
    if ( filename == null ) {
      return;
    }

    try {
      Set<String> folders = new HashSet<>();
      folders.add( "lib" );
      folders.add( "libswt/linux/x86_64" );

      String[] pluginFolders = new File( "plugins" ).list( new FilenameFilter() {
        @Override public boolean accept( File file, String name ) {
          return !"lib".equals( name );
        }
      } );
      folders.addAll( Arrays.asList( pluginFolders ) );

      for ( String folder : folders ) {
        System.out.println( "Found folder to include : " + folder );
      }


      IRunnableWithProgress op = iProgressMonitor -> {
        try {
          List<String> jarFiles = BeamConst.findLibraryFilesToStage( new File( "." ), false, folders );

          FatJarBuilder fatJarBuilder = new FatJarBuilder( filename, jarFiles );
          fatJarBuilder.setExtraStepPluginClasses( null );
          fatJarBuilder.setExtraXpPluginClasses( null );
          fatJarBuilder.buildTargetJar();

        } catch ( Exception e ) {
          throw new InvocationTargetException( e, "Error building fat jar: " + e.getMessage() );
        }
      };

      ProgressMonitorDialog pmd = new ProgressMonitorDialog( shell );
      pmd.run( true, true, op );

      box = new MessageBox( shell, SWT.CLOSE | SWT.ICON_INFORMATION );
      box.setText( "Fat jar created" );
      box.setMessage( "A fat jar was successfully created : " + filename );
      box.open();
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error creating fat jar", e );
    }
  }
}
