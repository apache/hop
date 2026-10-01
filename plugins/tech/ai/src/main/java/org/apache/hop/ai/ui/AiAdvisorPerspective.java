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

import java.util.List;
import org.apache.hop.ai.advisor.AiAdvisorOpenRequest;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

@HopPerspectivePlugin(
    id = "175-HopAiAdvisorPerspective",
    name = "i18n::AiAdvisorPerspective.Name",
    description = "i18n::AiAdvisorPerspective.Description",
    image = "ai-provider.svg",
    documentationUrl = "/hop-gui/perspective-ai-advisor.html",
    classLoaderGroup = "hop-ai")
@GuiPlugin(
    name = "i18n::AiAdvisorPerspective.Name",
    description = "i18n::AiAdvisorPerspective.GuiPlugin.Description",
    classLoaderGroup = "hop-ai")
public class AiAdvisorPerspective implements IHopPerspective, IAiAdvisorWorkbenchHost {

  public static final Class<?> PKG = AiAdvisorPerspective.class;

  private static AiAdvisorPerspective instance;

  private HopGui hopGui;
  private AiAdvisorWorkbench workbench;

  public AiAdvisorPerspective() {
    instance = this;
  }

  public static AiAdvisorPerspective getInstance() {
    try {
      AiAdvisorPerspective fromGui = HopGui.findSessionPerspective(AiAdvisorPerspective.class);
      if (fromGui != null) {
        return fromGui;
      }
    } catch (Throwable e) {
      // No HopGui in unit tests
    }
    return instance;
  }

  private boolean isInitialized() {
    return hopGui != null && workbench != null && !workbench.isDisposed();
  }

  @Override
  public String getId() {
    return "ai-advisor-perspective";
  }

  @GuiKeyboardShortcut(control = true, shift = true, key = 'a', global = true)
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'a', global = true)
  @Override
  public void activate() {
    if (!isInitialized()) {
      return;
    }
    hopGui.setActivePerspective(this);
  }

  @Override
  public void perspectiveActivated() {
    // Sessions refresh through the store listener.
  }

  @Override
  public boolean isActive() {
    return isInitialized() && hopGui.isActivePerspective(this);
  }

  @Override
  public void initialize(HopGui hopGui, Composite parent) {
    this.hopGui = hopGui;
    workbench = new AiAdvisorWorkbench(parent, this);
    workbench.setLayoutData(new FormDataBuilder().fullSize().result());

    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(this);
    keyHandler.addParentObjectToHandle(workbench);
    hopGui.replaceKeyboardShortcutListeners(workbench, keyHandler);
  }

  @Override
  public Control getControl() {
    return workbench;
  }

  public AiAdvisorSession openSession(AiAdvisorOpenRequest request) {
    if (!isInitialized()) {
      return null;
    }
    return workbench.openSession(request);
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return List.of();
  }

  @Override
  public HopGui getHopGui() {
    return hopGui;
  }

  @Override
  public Shell getShell() {
    return hopGui.getShell();
  }

  @Override
  public Display getDisplay() {
    return hopGui.getDisplay();
  }

  @Override
  public IVariables getVariables() {
    return hopGui.getVariables();
  }

  @Override
  public IHopMetadataProvider getMetadataProvider() {
    return hopGui.getMetadataProvider();
  }

  @Override
  public void asyncExec(Runnable runnable) {
    Display display = hopGui.getDisplay();
    if (display == null || display.isDisposed()) {
      return;
    }
    display.asyncExec(
        () -> {
          if (workbench == null || workbench.isDisposed()) {
            return;
          }
          runnable.run();
        });
  }
}
