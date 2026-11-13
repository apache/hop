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

package org.apache.hop.ui.hopgui.fatjar;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRunnableWithProgress;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.fatjar.FatJarEngine;
import org.apache.hop.fatjar.FatJarGenerator;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;

/** Tools, Generate a Hop fat jar: a fat jar of this installation for the engine you pick. */
@GuiPlugin
public class HopGuiFatJarPlugin {

  private static final Class<?> PKG = HopGuiFatJarPlugin.class;

  public static final String ID_MAIN_MENU_TOOLS_FAT_JAR = "40200-menu-tools-fat-jar";

  public static final String ID_MAIN_MENU_TOOLS_STAGING_CLEANUP =
      "40210-menu-tools-staging-cleanup";

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_STAGING_CLEANUP,
      label = "i18n::HopGuiFatJarPlugin.Menu.StagingCleanup.Text",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID)
  public void menuToolsStagingCleanup() {
    HopGui hopGui = HopGui.getInstance();
    new StagingCleanupDialog(
            hopGui.getShell(), hopGui.getVariables(), hopGui.getMetadataProvider(), hopGui.getLog())
        .open();
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_FAT_JAR,
      label = "i18n::HopGuiFatJarPlugin.Menu.GenerateFatJar.Text",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
      separator = true)
  public void menuToolsFatJar() {
    HopGui hopGui = HopGui.getInstance();
    Shell shell = hopGui.getShell();

    List<FatJarEngine> engines = FatJarGenerator.availableEngines();
    if (engines.isEmpty()) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "HopGuiFatJarPlugin.NoEngine.Header"));
      box.setMessage(BaseMessages.getString(PKG, "HopGuiFatJarPlugin.NoEngine.Message"));
      box.open();
      return;
    }

    FatJarEngine engine = engines.get(0);
    if (engines.size() > 1) {
      String[] choices = engines.stream().map(FatJarEngine::getDescription).toArray(String[]::new);
      EnterSelectionDialog dialog =
          new EnterSelectionDialog(
              shell,
              choices,
              BaseMessages.getString(PKG, "HopGuiFatJarPlugin.Engine.Header"),
              BaseMessages.getString(PKG, "HopGuiFatJarPlugin.Engine.Message"));
      dialog.setAvoidQuickSearch();
      String choice = dialog.open();
      if (choice == null) {
        return;
      }
      engine = engines.get(dialog.getSelectionNr());
    }

    String filename =
        BaseDialog.presentFileDialog(
            true,
            shell,
            new String[] {"*.jar", "*.*"},
            new String[] {
              BaseMessages.getString(PKG, "HopGuiFatJarPlugin.FileTypes.Jars.Label"),
              BaseMessages.getString(PKG, "HopGuiFatJarPlugin.FileTypes.All.Label")
            },
            true);
    if (filename == null) {
      return;
    }

    final FatJarEngine selected = engine;
    ProgressMonitorDialog progressDialog = new ProgressMonitorDialog(shell);
    try {
      IRunnableWithProgress op =
          monitor -> {
            try {
              monitor.setTaskName(
                  BaseMessages.getString(
                      PKG, "HopGuiFatJarPlugin.Progress.Message", selected.getDescription()));
              // A Beam jar keeps honouring the Spark client pack of HOP_SPARK_CLIENT_VERSION
              FatJarGenerator.generate(
                  hopGui.getLog(),
                  hopGui.getVariables(),
                  selected,
                  filename,
                  selected == FatJarEngine.BEAM
                      ? FatJarGenerator.sparkClientVersionFromEnvironment()
                      : null,
                  false,
                  monitor);
              monitor.done();
            } catch (Exception e) {
              throw new InvocationTargetException(e, "Error building fat jar: " + e.getMessage());
            }
          };
      progressDialog.run(true, op);

      GuiResource.getInstance().toClipboard(filename);
      MessageBox box = new MessageBox(shell, SWT.CLOSE | SWT.ICON_INFORMATION);
      box.setText(BaseMessages.getString(PKG, "HopGuiFatJarPlugin.Created.Header"));
      box.setMessage(
          BaseMessages.getString(
                  PKG, "HopGuiFatJarPlugin.Created.Message1", selected.getDescription(), filename)
              + Const.CR
              + BaseMessages.getString(PKG, "HopGuiFatJarPlugin.Created.Message2"));
      box.open();
    } catch (Exception e) {
      if (progressDialog.getProgressMonitor().isCanceled()) {
        // Canceled: the builder removed the partial jar, nothing to report
        return;
      }
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "HopGuiFatJarPlugin.Error.Header"),
          BaseMessages.getString(PKG, "HopGuiFatJarPlugin.Error.Message"),
          e);
    }
  }
}
