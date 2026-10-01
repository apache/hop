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

package org.apache.hop.ui.core.gui;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.junit.jupiter.api.Test;

/**
 * Only classes which can be instantiated without an owner are pre-created to make their keyboard
 * shortcuts available. Perspectives and widgets are created (and registered with the keyboard
 * handler) by their owner: pre-creating one leaves a second, uninitialized copy behind.
 */
class GuiMenuWidgetsPreRegistrationTest {

  /** A plain GUI plugin class: safe to create up front. */
  public static class PlainShortcutHost {
    public void doSomething() {
      // nothing to do
    }
  }

  /** A GUI plugin class created by its owner, e.g. a dialog or a tab. */
  public static class OwnedShortcutHost {
    public OwnedShortcutHost(HopGui hopGui) {
      // Needs its owner
    }
  }

  /** A widget: created and registered by whoever adds it to the UI. */
  public abstract static class WidgetShortcutHost extends Composite {
    protected WidgetShortcutHost(Composite parent, int style) {
      super(parent, style);
    }
  }

  /** A perspective: created and initialized by HopGui while loading the perspectives. */
  @HopPerspectivePlugin(id = "TestPerspective", name = "Test", description = "Test perspective")
  public static class PerspectiveShortcutHost implements IHopPerspective {
    @Override
    public String getId() {
      return "test-perspective";
    }

    @Override
    public void activate() {
      // nothing to do
    }

    @Override
    public void perspectiveActivated() {
      // nothing to do
    }

    @Override
    public boolean isActive() {
      return false;
    }

    @Override
    public void initialize(HopGui hopGui, Composite parent) {
      // nothing to do
    }

    @Override
    public Control getControl() {
      return null;
    }

    @Override
    public List<IGuiContextHandler> getContextHandlers() {
      return List.of();
    }
  }

  private static final ClassLoader CLASS_LOADER =
      GuiMenuWidgetsPreRegistrationTest.class.getClassLoader();

  @Test
  void classWithDefaultConstructorCanBePreRegistered() {
    assertTrue(GuiMenuWidgets.canPreRegister(CLASS_LOADER, PlainShortcutHost.class.getName()));
  }

  @Test
  void classWithoutDefaultConstructorIsSkipped() {
    assertFalse(GuiMenuWidgets.canPreRegister(CLASS_LOADER, OwnedShortcutHost.class.getName()));
  }

  @Test
  void widgetIsSkipped() {
    assertFalse(GuiMenuWidgets.canPreRegister(CLASS_LOADER, WidgetShortcutHost.class.getName()));
  }

  @Test
  void perspectiveIsSkipped() {
    assertFalse(
        GuiMenuWidgets.canPreRegister(CLASS_LOADER, PerspectiveShortcutHost.class.getName()));
  }

  @Test
  void unknownClassIsSkipped() {
    assertFalse(GuiMenuWidgets.canPreRegister(CLASS_LOADER, "org.apache.hop.does.not.Exist"));
  }
}
