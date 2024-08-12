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
 *
 */

package org.apache.hop.beam.gui;

import java.lang.reflect.Method;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiPluginType;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.welcome.WelcomeDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;

@GuiPlugin
public class WelcomeBeam {

  private static final Class<?> PKG = WelcomeBeam.class; // i18n

  private static final String WELCOME_BEAM_PARENT_ID = "WelcomeBeam.Parent.ID";
  public static final String CONST_ERROR = "Error";

  @GuiWidgetElement(
      type = GuiElementType.COMPOSITE,
      id = "20000-beam-welcome",
      label = "Apache Beam",
      parentId = WelcomeDialog.PARENT_ID_WELCOME_WIDGETS)
  public void welcome(Composite parent) {
    PropsUi props = PropsUi.getInstance();

    Composite parentComposite = new Composite(parent, SWT.NONE);
    parentComposite.setLayout(props.createFormLayout());
    FormData fdParentComposite = new FormData();
    fdParentComposite.left = new FormAttachment(0, 0);
    fdParentComposite.right = new FormAttachment(100, 0);
    fdParentComposite.top = new FormAttachment(0, 0);
    fdParentComposite.bottom = new FormAttachment(100, 0);
    parentComposite.setLayoutData(fdParentComposite);
    PropsUi.setLook(parentComposite);

    GuiCompositeWidgets compositeWidgets =
        new GuiCompositeWidgets(HopGui.getInstance().getVariables());
    compositeWidgets.createCompositeWidgets(
        this, null, parentComposite, WELCOME_BEAM_PARENT_ID, null);
    // No data to set on these widgets
  }

  public static final String WEB_NAME_BEAM_GETTING_STARTED =
      "Getting started with Apache Beam page";
  public static final String WEB_LINK_BEAM_GETTING_STARTED =
      "https://hop.apache.org/manual/latest/pipeline/beam/getting-started-with-beam.html";

  @GuiWidgetElement(
      id = "WelcomeBeam.10010.getting-started",
      parentId = WELCOME_BEAM_PARENT_ID,
      type = GuiElementType.LINK,
      label =
          "To get started with building and running Beam pipelines, see our <a>"
              + WEB_NAME_BEAM_GETTING_STARTED
              + "</a>.\n\nYou can also open one of the 'Hello, world' examples below:")
  public void homepageLink(Event event) {
    handleWebLinkEvent(event, WEB_NAME_BEAM_GETTING_STARTED, WEB_LINK_BEAM_GETTING_STARTED);
  }

  private static final String EXAMPLE1_NAME = "input-process-output.hpl";
  private static final String EXAMPLE1_FILE =
      "${PROJECT_HOME}/beam/pipelines/input-process-output.hpl";

  @GuiWidgetElement(
      id = "WelcomeBeam.11000.example1",
      parentId = WELCOME_BEAM_PARENT_ID,
      type = GuiElementType.LINK,
      label =
          "- A simple pipeline which read from and writes to a file: <a>" + EXAMPLE1_NAME + "</a>")
  public void openBeamSample1(Event event) {
    openSampleFileEvent(event, EXAMPLE1_NAME, EXAMPLE1_FILE);
  }

  private static final String EXAMPLE2_NAME = "complex.hpl";
  private static final String EXAMPLE2_FILE = "${PROJECT_HOME}/beam/pipelines/complex.hpl";

  @GuiWidgetElement(
      id = "WelcomeBeam.11000.example1",
      parentId = WELCOME_BEAM_PARENT_ID,
      type = GuiElementType.LINK,
      label =
          "- Open a more complex pipeline showcasing the possibilities: <a>"
              + EXAMPLE2_NAME
              + "</a>")
  public void openBeamSample2(Event event) {
    openSampleFileEvent(event, EXAMPLE2_NAME, EXAMPLE2_FILE);
  }

  private static final String EI_PERSPECTIVE_NAME = "execution information perspective";
  private static final String EXP_PERSPECTIVE_NAME = "file explorer perspective";

  @GuiWidgetElement(
      id = "WelcomeBeam.12000.running-a-sample",
      parentId = WELCOME_BEAM_PARENT_ID,
      type = GuiElementType.LINK,
      label =
          "\nTo run a sample, click on the start icon (triangle).  You can safely use the Direct, Spark and Flink run configurations to execute.\n"
              + "During the execution you can take a look in the <a>"
              + EI_PERSPECTIVE_NAME
              + "</a>.\n"
              + "The results of the pipelines are written to the beam/output folder in the form of CSV files. "
              + "You can list and even open these files with the <a>"
              + EXP_PERSPECTIVE_NAME
              + "</a>.\n")
  public void runningSamples(Event event) {
    if (EI_PERSPECTIVE_NAME.equals(event.text)) {
      HopGui.getExecutionPerspective().activate();
    }
    if (EXP_PERSPECTIVE_NAME.equals(event.text)) {
      HopGui.getExplorerPerspective().activate();
    }
  }

  private void openSampleFileEvent(Event event, String expectedText, String filename) {
    try {
      if (expectedText.equals(event.text)) {
        if (!checkProject()) {
          return;
        }

        // Open a simple example
        //
        IVariables variables = HopGui.getInstance().getVariables();
        HopGui.getInstance().fileDelegate.fileOpen(variables.resolve(filename));
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(), CONST_ERROR, "Error opening sample file " + filename, e);
    }
  }

  private void handleWebLinkEvent(Event event, String text, String url) {
    try {
      if (text.equals(event.text)) {
        EnvironmentUtils.getInstance().openUrl(url);
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(), CONST_ERROR, "Error opening link to " + url, e);
    }
  }

  private boolean checkProject() {
    String projectName = HopNamespace.getNamespace();
    if ("samples".equalsIgnoreCase(projectName)) {
      return true;
    }

    // Show a dialog asking the user to switch to the samples project.
    //
    MessageBox box = new MessageBox(HopGui.getInstance().getShell(), SWT.YES | SWT.NO | SWT.CANCEL);
    box.setText("Switch to samples project");
    box.setMessage(
        "This example works best in the samples project.\n"
            + "Do you want to switch to the samples project now?\n");
    int answer = box.open();
    if ((answer & SWT.NO) != 0) {
      return true;
    }
    if ((answer & SWT.YES) != 0) {
      // Switch to the samples project
      //
      try {
        PluginRegistry registry = PluginRegistry.getInstance();
        String guiPluginClassName = "org.apache.hop.projects.gui.ProjectsGuiPlugin";
        IPlugin plugin = registry.findPluginWithId(GuiPluginType.class, guiPluginClassName);
        if (plugin == null) {
          throw new HopException(
              "Unable to switch projects because the projects GUI plugin couldn't be found");
        }
        ClassLoader classLoader = registry.getClassLoader(plugin);
        Class<?> guiPluginClass = classLoader.loadClass(guiPluginClassName);

        Method method = guiPluginClass.getMethod("enableProject", String.class);
        // Switch to the samples project
        method.invoke(null, "samples");
        return true;
      } catch (Exception e) {
        new ErrorDialog(
            HopGui.getInstance().getShell(),
            CONST_ERROR,
            "Sorry, I couldn't switch to the samples project",
            e);
        return false;
      }
    }

    return false;
  }
}
