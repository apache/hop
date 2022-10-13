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

import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.welcome.WelcomeDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;

@GuiPlugin
public class WelcomeBeam {

  private static final Class<?> PKG = WelcomeBeam.class; // i18n

  private static final String WELCOME_BEAM_PARENT_ID = "WelcomeBeam.Parent.ID";

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
    props.setLook(parentComposite);

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
              + "</a>.\n")
  public void homepageLink(Event event) {
    handleWebLinkEvent(event, WEB_NAME_BEAM_GETTING_STARTED, WEB_LINK_BEAM_GETTING_STARTED);
  }

  private void handleWebLinkEvent(Event event, String text, String url) {
    try {
      if (text.equals(event.text)) {
        Program.launch(url);
      }
    } catch (Exception e) {
      new ErrorDialog(HopGui.getInstance().getShell(), "Error", "Error opening link to " + url, e);
    }
  }

  private static final String EXAMPLE1_NAME = "input-process-output.hpl";
  private static final String EXAMPLE1_FILE =
      "config/projects/samples/beam/pipelines/input-process-output.hpl";

  @GuiWidgetElement(
      id = "WelcomeBeam.11000.example1",
      parentId = WELCOME_BEAM_PARENT_ID,
      type = GuiElementType.LINK,
      label =
          "Open a simple pipeline which read from and writes to a file: <a>"
              + EXAMPLE1_NAME
              + "</a>")
  public void openBeamSample1(Event event) {
    openSampleFileEvent(event, EXAMPLE1_NAME, EXAMPLE1_FILE);
  }

  private static final String EXAMPLE2_NAME = "complex.hpl";
  private static final String EXAMPLE2_FILE = "config/projects/samples/beam/pipelines/complex.hpl";

  @GuiWidgetElement(
      id = "WelcomeBeam.11000.example1",
      parentId = WELCOME_BEAM_PARENT_ID,
      type = GuiElementType.LINK,
      label =
          "Open a more complex pipeline showcasing the possibilities: <a>" + EXAMPLE2_NAME + "</a>")
  public void openBeamSample2(Event event) {
    openSampleFileEvent(event, EXAMPLE2_NAME, EXAMPLE2_FILE);
  }

  private void openSampleFileEvent(Event event, String expectedText, String filename) {
    try {
      if (expectedText.equals(event.text)) {
        if (!checkProject()) {
          return;
        }

        // Open a simple example
        //
        HopGui.getInstance().fileDelegate.fileOpen(filename);
      }
    } catch (Exception e) {
      new ErrorDialog(
          HopGui.getInstance().getShell(), "Error", "Error opening sample file " + filename, e);
    }
  }

  private boolean checkProject() {
    return true;
  }
}
