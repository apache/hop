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

package org.apache.hop.beam.gui;

import org.apache.commons.io.FileUtils;
import org.apache.hop.beam.pipeline.fatjar.FatJarBuilder;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
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
      List<String> jarFilenames = findInstalledJarFilenames();

      IRunnableWithProgress op = monitor -> {
        try {
          monitor.setTaskName( "Building a Hop fat jar..." );
          FatJarBuilder fatJarBuilder = new FatJarBuilder( hopGui.getVariables(), filename, jarFilenames );
          fatJarBuilder.setExtraTransformPluginClasses( null );
          fatJarBuilder.setExtraXpPluginClasses( null );
          fatJarBuilder.buildTargetJar();

        } catch ( Exception e ) {
          throw new InvocationTargetException( e, "Error building fat jar: " + e.getMessage() );
        }
      };

      ProgressMonitorDialog pmd = new ProgressMonitorDialog( shell );
      pmd.run( true, false, op );

      GuiResource.getInstance().toClipboard( filename );

      box = new MessageBox( shell, SWT.CLOSE | SWT.ICON_INFORMATION );
      box.setText( "Fat jar created" );
      box.setMessage( "A fat jar was successfully created : " + filename + Const.CR+"The filename was copied to the clipboard." );
      box.open();
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error creating fat jar", e );
    }
  }

  public static final List<String> findInstalledJarFilenames() {
    Set<File> jarFiles = new HashSet<>();
    jarFiles.addAll( FileUtils.listFiles( new File("lib"), new String[] { "jar" }, true ) );
    jarFiles.addAll( FileUtils.listFiles( new File("libswt/linux/x86_64"), new String[] { "jar" }, true ) );
    jarFiles.addAll( FileUtils.listFiles( new File("plugins"), new String[] { "jar" }, true ) );

    List<String> jarFilenames = new ArrayList<>();
    jarFiles.forEach( file -> jarFilenames.add(file.toString()) );
    return jarFilenames;
  }
}
