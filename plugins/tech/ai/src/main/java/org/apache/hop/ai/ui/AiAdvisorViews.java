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

package org.apache.hop.ai.ui;

import org.apache.hop.ai.advisor.AiAdvisorOpenRequest;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorSessionStore;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.terminal.HopGuiBottomDock;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;

/**
 * Opens the AI workbench as a perspective, floating window, or bottom-dock tab. hopper-edw and Hop
 * GUI plugins in this JAR can call {@link #openSession(HopGui, AiAdvisorOpenRequest)}. Other
 * plugins should use {@code HopGui.openAiAdvisorSession} / {@code
 * HopExtensionPoint.HopGuiAiAdvisorOpenSession}.
 */
@GuiPlugin(classLoaderGroup = "hop-ai")
public class AiAdvisorViews {

  public static final Class<?> PKG = AiAdvisorPerspective.class;

  public static final String DOCK_TOOL_ID = "ai-advisor-workbench";

  public static final String ID_MAIN_MENU_TOOLS_AI_WINDOW = "40027-menu-tools-ai-window";

  @GuiMenuElement(
      root = HopGui.ID_MAIN_MENU,
      id = ID_MAIN_MENU_TOOLS_AI_WINDOW,
      label = "i18n::AiAdvisorPerspective.Menu.Tools.Window",
      parentId = HopGui.ID_MAIN_MENU_TOOLS_PARENT_ID,
      image = "ai-provider.svg")
  public void menuToolsAiWindow() {
    HopGui hopGui;
    try {
      hopGui = HopGui.peekInstance();
    } catch (Throwable e) {
      return;
    }
    if (hopGui == null) {
      return;
    }
    openDialog(hopGui);
  }

  public static void openPerspective(HopGui hopGui) {
    AiAdvisorPerspective perspective = AiAdvisorPerspective.getInstance();
    if (perspective != null) {
      perspective.activate();
    }
  }

  public static void openDialog(HopGui hopGui) {
    AiAdvisorDialog.open(hopGui);
  }

  public static boolean isDialogOpen(HopGui hopGui) {
    if (hopGui == null || hopGui.getShell() == null || hopGui.getShell().isDisposed()) {
      return false;
    }
    Object existing = hopGui.getShell().getData(AiAdvisorDialog.SHELL_DATA_KEY);
    return existing instanceof AiAdvisorDialog dialog && dialog.isOpen();
  }

  public static void openDock(HopGui hopGui) {
    if (hopGui == null) {
      return;
    }
    HopGuiBottomDock dock = hopGui.getTerminalPanel();
    if (dock == null || dock.isDisposed()) {
      return;
    }
    String title = BaseMessages.getString(PKG, "AiAdvisorPerspective.Name");
    dock.focusOrOpenToolTab(
        DOCK_TOOL_ID,
        title,
        GuiResource.getInstance()
            .getImage(
                "ai-provider.svg",
                AiAdvisorViews.class.getClassLoader(),
                ConstUi.SMALL_ICON_SIZE,
                ConstUi.SMALL_ICON_SIZE),
        true,
        container -> createDockedWorkbench(container, hopGui));
  }

  public static boolean isDockOpen(HopGui hopGui) {
    if (hopGui == null) {
      return false;
    }
    HopGuiBottomDock dock = hopGui.getTerminalPanel();
    if (dock == null || dock.isDisposed()) {
      return false;
    }
    CTabItem item = dock.findToolTab(DOCK_TOOL_ID);
    return item != null && !item.isDisposed();
  }

  /**
   * Create or reuse a session and bring a workbench host to the front. Prefers an already-open
   * dialog or dock so the user keeps their layout. When the request prefers a floating window
   * (toolbar Help on a graph or model), opens that window so the subject stays visible. Other
   * callers fall back to the perspective.
   */
  public static AiAdvisorSession openSession(HopGui hopGui, AiAdvisorOpenRequest request) {
    AiAdvisorSessionStore store = AiAdvisorSessionStore.get(hopGui);
    AiAdvisorSession session = store.open(request);
    if (isDialogOpen(hopGui)) {
      openDialog(hopGui);
    } else if (isDockOpen(hopGui)) {
      openDock(hopGui);
    } else if (shouldOpenFloatingWindow(request)) {
      openDialog(hopGui);
    } else {
      openPerspective(hopGui);
    }
    return session;
  }

  /**
   * Callers looking at a subject (pipeline, workflow, Data Vault model, lineage view, …) set {@link
   * AiAdvisorOpenRequest#isPreferFloatingWindow()} so that subject stays visible.
   */
  static boolean shouldOpenFloatingWindow(AiAdvisorOpenRequest request) {
    return request != null && request.isPreferFloatingWindow();
  }

  private static Control createDockedWorkbench(Composite container, HopGui hopGui) {
    HopGuiAiAdvisorWorkbenchHost host =
        new HopGuiAiAdvisorWorkbenchHost(
            hopGui, () -> !container.isDisposed(), () -> openDock(hopGui), null);
    AiAdvisorWorkbench workbench = new AiAdvisorWorkbench(container, host);
    workbench.setLayoutData(new FormDataBuilder().fullSize().result());
    return workbench;
  }
}
