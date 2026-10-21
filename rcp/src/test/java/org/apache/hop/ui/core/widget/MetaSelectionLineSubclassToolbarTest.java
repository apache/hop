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

package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.awt.GraphicsEnvironment;
import java.lang.reflect.Method;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * A graph connection line is a {@link MetaSelectionLine} subclass. The edit icon and the toolbar
 * buttons are declared on the base class, so they have to resolve the subclass instance (issue
 * #8986).
 *
 * <p>Lives in hop-ui-rcp because constructing the widget needs the desktop look-and-feel
 * implementations.
 */
@Tag("uitest")
class MetaSelectionLineSubclassToolbarTest {

  private static final String EDIT_TOOLTIP = "Edit this metadata element";

  private Display display;
  private Shell shell;

  @BeforeAll
  static void registerToolbar() {
    GuiRegistry registry = GuiRegistry.getInstance();
    for (Method method : MetaSelectionLine.class.getDeclaredMethods()) {
      GuiToolbarElement element = method.getAnnotation(GuiToolbarElement.class);
      if (element != null && registry.findGuiToolbarItem(element.root(), element.id()) == null) {
        registry.addGuiToolbarElement(
            MetaSelectionLine.class.getName(),
            element,
            method,
            MetaSelectionLine.class.getClassLoader());
      }
    }
  }

  @BeforeEach
  void openShell() throws Exception {
    assumeFalse(GraphicsEnvironment.isHeadless(), "No display available; skipping SWT test.");
    HopEnvironment.init();
    display = Display.getDefault();
    PropsUi.getInstance();
    GuiResource.getInstance();
    shell = new Shell(display, SWT.NONE);
  }

  @AfterEach
  void disposeShell() {
    if (shell != null && !shell.isDisposed()) {
      shell.dispose();
    }
  }

  @Test
  void subclassShowsTheEditIconAndEditsItself() {
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    ConnectionSelectionLine line =
        new ConnectionSelectionLine(
            shell, provider, "Graph database connection", "Connection tooltip");
    line.setText("neo4j");

    ToolItem edit = itemWithTooltip(line, EDIT_TOOLTIP);
    assertNotNull(edit.getImage(), "edit button image");
    edit.notifyListeners(SWT.Selection, new Event());
    assertTrue(line.edited, "Edit calls the connection line, not a new MetaSelectionLine");
  }

  @Test
  void selectionLineStillShowsTheEditIcon() {
    MetaSelectionLine<ExecutionInfoLocation> line =
        new MetaSelectionLine<>(
            Variables.getADefaultVariableSpace(),
            new MemoryMetadataProvider(),
            ExecutionInfoLocation.class,
            shell,
            SWT.NONE,
            "Location",
            "Location tooltip");

    ToolItem edit = itemWithTooltip(line, EDIT_TOOLTIP);
    assertNotNull(edit.getImage(), "edit button image");
  }

  private static ToolItem itemWithTooltip(MetaSelectionLine<?> line, String tooltip) {
    ToolBar toolBar = null;
    for (Control child : line.getChildren()) {
      if (child instanceof ToolBar bar) {
        toolBar = bar;
        break;
      }
    }
    assertNotNull(toolBar, "metadata selection line toolbar");
    StringBuilder seen = new StringBuilder();
    for (ToolItem item : toolBar.getItems()) {
      String tip = item.getToolTipText();
      if (tip != null) {
        seen.append(tip).append("; ");
        if (tip.startsWith(tooltip)) {
          return item;
        }
      }
    }
    fail("No toolbar item with tooltip '" + tooltip + "'. Saw: " + seen);
    return null;
  }

  /** Stands in for {@code NeoConnectionSelectionLine} without the Neo4j plugin. */
  private static final class ConnectionSelectionLine
      extends MetaSelectionLine<ExecutionInfoLocation> {

    private boolean edited;

    private ConnectionSelectionLine(
        Shell shell, MemoryMetadataProvider provider, String label, String tooltip) {
      super(
          Variables.getADefaultVariableSpace(),
          provider,
          ExecutionInfoLocation.class,
          shell,
          SWT.NONE,
          label,
          tooltip);
    }

    @Override
    protected boolean editMetadata() {
      edited = true;
      return true;
    }
  }
}
