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

package org.apache.hop.ui.hopgui.welcome;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import org.apache.hop.core.SwtUniversalImageSvg;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.gui.plugin.GuiElements;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElementType;
import org.apache.hop.core.svg.SvgCache;
import org.apache.hop.core.svg.SvgCacheEntry;
import org.apache.hop.core.svg.SvgFile;
import org.apache.hop.core.svg.SvgImage;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.List;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Widget;

/** We show this dialog at the start of the application. */
@GuiPlugin
public class WelcomeDialog {
  public static final String PARENT_ID_WELCOME_WIDGETS = "WelcomeDialog.Parent.ID";
  public static final String HOP_CONFIG_NO_SHOW_OPTION = "doNotShowWelcomeDialog";
  public static final String VARIABLE_HOP_NO_WELCOME_DIALOG = "HOP_NO_WELCOME_DIALOG";

  private Shell shell;
  private Image logoImage;
  private Font titleFont;

  private Button doNotShow;
  private List wTopics;
  private Composite wPluginsComp;

  public WelcomeDialog() {
    // Do nothing
  }

  public void open() {
    Shell parent = HopGui.getInstance().getShell();
    try {
      shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.CLOSE | SWT.RESIZE | SWT.MAX);
      shell.setLayout(new FormLayout());
      shell.setText("Apache Hop");
      PropsUi.setLook(shell);

      PropsUi props = PropsUi.getInstance();
      int margin = PropsUi.getMargin();

      // Logo at the top left
      //
      Label logoLabel = new Label(shell, SWT.NONE);
      SvgCacheEntry cacheEntry =
          SvgCache.loadSvg(new SvgFile("ui/images/logo_hop.svg", getClass().getClassLoader()));
      SwtUniversalImageSvg imageSvg =
          new SwtUniversalImageSvg(new SvgImage(cacheEntry.getSvgDocument()));
      int logoSize = (int) (75 * props.getZoomFactor());
      this.logoImage = imageSvg.getAsBitmapForSize(shell.getDisplay(), logoSize, logoSize);
      logoLabel.setImage(this.logoImage);
      FormData fdLogoLabel = new FormData();
      fdLogoLabel.left = new FormAttachment(0, 0);
      fdLogoLabel.top = new FormAttachment(0, 0);
      logoLabel.setLayoutData(fdLogoLabel);

      // Apache Hop
      //
      Label welcome = new Label(shell, SWT.CENTER);
      PropsUi.setLook(welcome);
      welcome.setText("Apache Hop");
      titleFont =
          new Font(shell.getDisplay(), "Open Sans", (int) (18 * props.getZoomFactor()), SWT.NONE);
      welcome.setFont(titleFont);
      FormData fdWelcome = new FormData();
      fdWelcome.left = new FormAttachment(logoLabel, PropsUi.getMargin(), SWT.RIGHT);
      fdWelcome.right = new FormAttachment(100, 0);
      fdWelcome.top = new FormAttachment(logoLabel, 0, SWT.CENTER);
      welcome.setLayoutData(fdWelcome);

      // An area at the bottom that shows the "don't show this again" option.
      //
      doNotShow = new Button(shell, SWT.CHECK);
      doNotShow.setText("Don't show this at startup (find me in the Help menu)");
      doNotShow.addListener(SWT.Selection, this::dontShowAgain);
      doNotShow.setSelection(HopConfig.readOptionBoolean(HOP_CONFIG_NO_SHOW_OPTION, false));
      PropsUi.setLook(doNotShow);
      FormData fdDoNotShow = new FormData();
      fdDoNotShow.bottom = new FormAttachment(100, 0);
      fdDoNotShow.left = new FormAttachment(0, margin);
      fdDoNotShow.right = new FormAttachment(100, 0);
      doNotShow.setLayoutData(fdDoNotShow);

      // The rest of the dialog is for plugin specific stuff
      // On the left we have the welcome/help topics
      //
      wTopics = new List(shell, SWT.SINGLE | SWT.V_SCROLL | SWT.BORDER | SWT.LEFT);
      PropsUi.setLook(wTopics);
      FormData fdTopics = new FormData();
      fdTopics.left = new FormAttachment(0, 0);
      fdTopics.right = new FormAttachment(logoLabel, 0, SWT.RIGHT);
      fdTopics.top = new FormAttachment(logoLabel, 2 * margin);
      fdTopics.bottom = new FormAttachment(doNotShow, -2 * margin);
      wTopics.setLayoutData(fdTopics);

      wPluginsComp = new Composite(shell, SWT.NONE);
      PropsUi.setLook(wPluginsComp);
      wPluginsComp.setLayout(new FormLayout());
      FormData fdPluginsComp = new FormData();
      fdPluginsComp.left = new FormAttachment(logoLabel, 2 * margin, SWT.RIGHT);
      fdPluginsComp.right = new FormAttachment(100, 0);
      fdPluginsComp.top = new FormAttachment(logoLabel, 2 * margin, SWT.BOTTOM);
      fdPluginsComp.bottom = new FormAttachment(doNotShow, -2 * margin);
      wPluginsComp.setLayoutData(fdPluginsComp);

      // What is the list?  Look in the GUI plugin registry and look for widgets with the parent.
      //
      GuiRegistry guiRegistry = GuiRegistry.getInstance();
      java.util.List<GuiElements> elementsList =
          guiRegistry.getCompositeGuiElements().get(PARENT_ID_WELCOME_WIDGETS);
      elementsList.sort(Comparator.comparing(GuiElements::getId));

      for (GuiElements elements : elementsList) {
        wTopics.add(elements.getLabel());
        wTopics.setData(elements.getLabel(), elements);
      }

      if (wTopics.getItemCount() > 0) {
        wTopics.setSelection(0);
        topicSelected();
      }

      wTopics.addListener(SWT.Selection, e -> topicSelected());

      BaseDialog.defaultShellHandling(shell, e -> close(), e -> close());
    } catch (Exception e) {
      new ErrorDialog(parent, "Error", "Error showing welcome dialog!", e);
    } finally {
      logoImage.dispose();
      titleFont.dispose();
    }
  }

  private void topicSelected() {
    String selectedLabel = wTopics.getSelection()[0];
    GuiElements elements = (GuiElements) wTopics.getData(selectedLabel);
    if (elements == null) {
      return;
    }
    // call the referenced method with composite as argument.
    //
    try {
      // Get rid of the old children of the composite
      //
      Arrays.stream(wPluginsComp.getChildren()).forEach(Widget::dispose);

      wPluginsComp.setBackground(GuiResource.getInstance().getColorLightGray());
      wPluginsComp.setForeground(GuiResource.getInstance().getColorDarkGray());

      Object object = elements.getButtonMethod().getDeclaringClass().getConstructor().newInstance();

      elements.getButtonMethod().invoke(object, wPluginsComp);

      wPluginsComp.layout(true, true);
    } catch (Exception ex) {
      throw new RuntimeException(
          "Unable to invoke welcome method with the parent Composite as the argument", ex);
    }
  }

  private void dontShowAgain(Event event) {
    boolean doNotShow = ((Button) event.widget).getSelection();
    HopConfig.getInstance().saveOption(HOP_CONFIG_NO_SHOW_OPTION, doNotShow);
  }

  public void close() {
    HopConfig.saveOptions(
        Map.of(WelcomeDialog.HOP_CONFIG_NO_SHOW_OPTION, doNotShow.getSelection()));
    shell.dispose();
  }

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      parentId = HopGui.ID_MAIN_MENU_HELP_PARENT_ID,
      type = GuiMenuElementType.MENU_ITEM,
      id = "help.welcome",
      label = "Welcome",
      image = "ui/images/logo_hop.svg")
  public void menuHelpWelcome() {
    open();
  }
}
