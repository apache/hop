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

package org.apache.hop.beam.gui;

import org.apache.commons.io.FileUtils;
import org.apache.hop.beam.pipeline.fatjar.FatJarBuilder;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRunnableWithProgress;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.metadata.SerializableMetadataProvider;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.io.File;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@GuiPlugin
public class HopBeamGuiPlugin {

  public static final Class<?> PKG = HopBeamGuiPlugin.class; // i18n

  public static final String ID_MAIN_MENU_TOOLS_FAT_JAR = "40200-menu-tools-fat-jar";
  public static final String ID_MAIN_MENU_TOOLS_EXPORT_METADATA =
      "40210-menu-tools-export-metadata";

  private static HopBeamGuiPlugin instance;

  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static HopBeamGuiPlugin getInstance() {
    if (instance == null) {
      instance = new HopBeamGuiPlugin();
    }
    return instance;
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_FAT_JAR,
      label = "i18n::BeamGuiPlugin.Menu.GenerateFatJar.Text",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
      separator = true)
  public void menuToolsFatJar() {
    HopGui hopGui = HopGui.getInstance();
    final Shell shell = hopGui.getShell();

    MessageBox box = new MessageBox(shell, SWT.OK | SWT.CANCEL | SWT.ICON_INFORMATION);
    box.setText(BaseMessages.getString(PKG, "BeamGuiPlugin.GenerateFatJar.Dialog.Header"));
    box.setMessage(
        BaseMessages.getString(PKG, "BeamGuiPlugin.GenerateFatJar.Dialog.Message1")
            + Const.CR
            + BaseMessages.getString(PKG, "BeamGuiPlugin.GenerateFatJar.Dialog.Message2"));
    int answer = box.open();
    if ((answer & SWT.CANCEL) != 0) {
      return;
    }

    // Ask
    //
    String filename =
        BaseDialog.presentFileDialog(
            true,
            shell,
            new String[] {"*.jar", "*.*"},
            new String[] {
              BaseMessages.getString(PKG, "BeamGuiPlugin.FileTypes.Jars.Label"),
              BaseMessages.getString(PKG, "BeamGuiPlugin.FileTypes.All.Label")
            },
            true);
    if (filename == null) {
      return;
    }

    try {
      List<String> jarFilenames = findInstalledJarFilenames();

      IRunnableWithProgress op =
          monitor -> {
            try {
              monitor.setTaskName(
                  BaseMessages.getString(PKG, "BeamGuiPlugin.GenerateFatJar.Progress.Message"));
              FatJarBuilder fatJarBuilder =
                  new FatJarBuilder(hopGui.getLog(), hopGui.getVariables(), filename, jarFilenames);
              fatJarBuilder.setExtraTransformPluginClasses(null);
              fatJarBuilder.setExtraXpPluginClasses(null);
              fatJarBuilder.buildTargetJar();
              monitor.done();
            } catch (Exception e) {
              throw new InvocationTargetException(e, "Error building fat jar: " + e.getMessage());
            }
          };

      ProgressMonitorDialog pmd = new ProgressMonitorDialog(shell);
      pmd.run(false, op);

      GuiResource.getInstance().toClipboard(filename);

      box = new MessageBox(shell, SWT.CLOSE | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "BeamGuiPlugin.FatJarCreated.Dialog.Header"));
      box.setMessage(
          BaseMessages.getString(PKG, "BeamGuiPlugin.FatJarCreated.Dialog.Message1", filename)
              + Const.CR
              + BaseMessages.getString(PKG, "BeamGuiPlugin.FatJarCreated.Dialog.Message2"));
      box.open();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error creating fat jar", e);
    }
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_EXPORT_METADATA,
      label = "i18n::BeamGuiPlugin.Menu.ExportMetadata.Text",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
      separator = true)
  public void menuToolsExportMetadata() {
    HopGui hopGui = HopGui.getInstance();
    final Shell shell = hopGui.getShell();

    MessageBox box = new MessageBox(shell, SWT.OK | SWT.CANCEL | SWT.ICON_INFORMATION);
    box.setText(BaseMessages.getString(PKG, "BeamGuiPlugin.ExportMetadata.Dialog.Header"));
    box.setMessage(BaseMessages.getString(PKG, "BeamGuiPlugin.ExportMetadata.Dialog.Message"));
    int answer = box.open();
    if ((answer & SWT.CANCEL) != 0) {
      return;
    }

    // Ask
    //
    String filename =
        BaseDialog.presentFileDialog(
            true,
            shell,
            new String[] {"*.json", "*.*"},
            new String[] {
              BaseMessages.getString(PKG, "BeamGuiPlugin.FileTypes.Json.Label"),
              BaseMessages.getString(PKG, "BeamGuiPlugin.FileTypes.All.Label")
            },
            true);
    if (filename == null) {
      return;
    }

    try {
      // Save HopGui metadata to JSON...
      //
      SerializableMetadataProvider metadataProvider =
          new SerializableMetadataProvider(hopGui.getMetadataProvider());
      String jsonString = metadataProvider.toJson();
      String realFilename = hopGui.getVariables().resolve(filename);

      try (OutputStream outputStream = HopVfs.getOutputStream(realFilename, false)) {
        outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8));
      }
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error saving metadata to JSON file : " + filename, e);
    }
  }

  public static final List<String> findInstalledJarFilenames() {
    Set<File> jarFiles = new HashSet<>();
    jarFiles.addAll(FileUtils.listFiles(new File("lib"), new String[] {"jar"}, true));
    jarFiles.addAll(
        FileUtils.listFiles(new File("libswt/linux/x86_64"), new String[] {"jar"}, true));
    jarFiles.addAll(FileUtils.listFiles(new File("plugins"), new String[] {"jar"}, true));

    List<String> jarFilenames = new ArrayList<>();
    jarFiles.forEach(file -> jarFilenames.add(file.toString()));
    return jarFilenames;
  }
}
