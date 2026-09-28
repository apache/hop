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

package org.apache.hop.workflow.actions.dbt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiImpl;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.workflow.WorkflowMeta;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.waits.DefaultCondition;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * End-to-end coverage for the dbt action dialog, which {@link
 * org.apache.hop.ui.core.gui.GuiCompositeWidgets} builds from the annotations on {@link ActionDbt}.
 * Issue #8589: the fields used to be laid out on the shell itself, which left the two name/value
 * tables squeezed under a stack of eight rows.
 *
 * <p>Tagged {@code uitest} so it is skipped when there is no display. Wrap Maven with {@code
 * tools/with-isolated-display.sh} so the dialog does not steal focus.
 */
@Tag("uitest")
class ActionDbtDialogTest extends SwtBotTestBase {

  private static final String DIALOG_TITLE = "dbt";
  private static final String PROJECT_NAME = "demo-project";
  private static final List<String> EXPECTED_TABS =
      List.of("dbt project", "Selection", "Execution", "Variables");

  /**
   * The annotated widgets are looked up in the registry by class name. The unit-test JVM does not
   * always scan the plugin classes, so register them the way the {@code GuiPluginType} scan does.
   */
  @BeforeAll
  static void registerActionWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    if (registry.findGuiElements(ActionDbt.class.getName(), ActionDbt.GUI_PLUGIN_ELEMENT_PARENT_ID)
        != null) {
      return;
    }
    for (Field field : ActionDbt.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(ActionDbt.class.getName(), element, field);
      }
    }
  }

  /**
   * The metadata selection line reads its provider from the running Hop GUI, which does not exist
   * here. Stand a mock in for it, holding the dbt project the action refers to.
   */
  @BeforeAll
  static void installHopGui() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    DbtProject project = new DbtProject();
    project.setName(PROJECT_NAME);
    metadataProvider.getSerializer(DbtProject.class).save(project);

    Variables variables = new Variables();
    HopGui hopGui = mock(HopGui.class);
    when(hopGui.getMetadataProvider())
        .thenReturn(new MultiMetadataProvider(variables, metadataProvider));
    when(hopGui.getVariables()).thenReturn(variables);
    setHopGui(hopGui);
  }

  @AfterAll
  static void removeHopGui() throws Exception {
    setHopGui(null);
  }

  private static void setHopGui(HopGui hopGui) throws Exception {
    Field instance = HopGuiImpl.class.getDeclaredField("instance");
    instance.setAccessible(true);
    instance.set(null, hopGui);
  }

  @Test
  void theOptionsAreLaidOutOnFourTabsAboveTheButtons() {
    List<String> tabs = new ArrayList<>();
    List<String> tablesOutsideTheVariablesTab = new ArrayList<>();
    List<Rectangle> folderBounds = new ArrayList<>();
    List<Rectangle> okBounds = new ArrayList<>();

    withDialog(
        parent ->
            new ActionDbtDialog(parent, actionWithValues(), new WorkflowMeta(), new Variables())
                .open(),
        bot -> {
          SWTBotShell dialogShell = dialogShell(bot);
          display.syncExec(
              () -> {
                Shell shell = dialogShell.widget;
                shell.layout(true, true);

                // The action image is loaded before the metadata widget is built, which is the
                // order #8020 worked around; the dbt project line below still fills.
                assertNotNull(shell.getImage(), "The dbt action image was not loaded");

                CTabFolder folder = findTabFolder(shell);
                assertNotNull(folder, "The options are not laid out on tabs");
                for (int i = 0; i < folder.getItemCount(); i++) {
                  tabs.add(folder.getItem(i).getText());
                }
                folderBounds.add(displayBounds(folder));

                // Both tables belong to the last tab, not to the shell.
                Control variablesTab = folder.getItem(folder.getItemCount() - 1).getControl();
                for (TableView table : collectTables(shell)) {
                  if (!isDescendantOf(table, variablesTab)) {
                    tablesOutsideTheVariablesTab.add(table.toString());
                  }
                }

                Button ok = findOkButton(shell);
                assertNotNull(ok, "The dialog has no OK button");
                okBounds.add(displayBounds(ok));
              });
          dialogShell.bot().button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals(EXPECTED_TABS, tabs);
    assertTrue(
        tablesOutsideTheVariablesTab.isEmpty(),
        "Every name/value table belongs on the Variables tab: " + tablesOutsideTheVariablesTab);
    assertFalse(
        folderBounds.get(0).intersects(okBounds.get(0)),
        "The OK button overlaps the options, which is what issue #8589 is about");
    assertTrue(
        okBounds.get(0).y >= folderBounds.get(0).y + folderBounds.get(0).height,
        "The OK button should sit below the options");
  }

  /**
   * The point of issue #8589: at the size the dialog opens with - no saved geometry, so {@code
   * BaseTransformDialog.setSize} packs it - both name/value tables have to be usable. A table that
   * is only given the leftovers of a packed layout collapses to a header strip.
   */
  @Test
  void bothTablesOpenWithAUsableHeight() {
    List<Integer> heights = new ArrayList<>();
    List<Point> shellSize = new ArrayList<>();

    withDialog(
        parent ->
            new ActionDbtDialog(parent, actionWithValues(), new WorkflowMeta(), new Variables())
                .open(),
        bot -> {
          SWTBotShell dialogShell = dialogShell(bot);
          display.syncExec(
              () -> {
                Shell shell = dialogShell.widget;
                shellSize.add(shell.getSize());
                CTabFolder folder = findTabFolder(shell);
                folder.setSelection(folder.getItemCount() - 1);
                shell.layout(true, true);
                for (TableView table : collectTables(shell)) {
                  heights.add(table.getBounds().height);
                }
              });
          dialogShell.bot().button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals(2, heights.size(), "Expected the variables and the environment table");
    for (int height : heights) {
      assertTrue(
          height >= 80,
          "A name/value table opens "
              + height
              + "px high in a "
              + shellSize.get(0)
              + " dialog, which is not usable");
    }
  }

  @Test
  void okKeepsEveryValueOfTheAction() {
    ActionDbt action = actionWithValues();

    withDialog(
        parent -> new ActionDbtDialog(parent, action, new WorkflowMeta(), new Variables()).open(),
        bot -> dialogShell(bot).bot().button(buttonLabel("System.Button.OK")).click());

    assertEquals("dbt build", action.getName());
    assertEquals(PROJECT_NAME, action.getDbtProjectName());
    assertEquals(DbtOperation.BUILD.getCode(), action.getOperation());
    assertEquals("prod", action.getTarget());
    assertEquals("tag:daily", action.getSelect());
    assertEquals("tag:slow", action.getExclude());
    assertEquals("4", action.getThreads());
    assertEquals("900", action.getTimeout());
    assertTrue(action.isFullRefresh());
    assertTrue(action.isEmitOpenLineage());
    assertEquals(1, action.getVars().size());
    assertEquals("run_date", action.getVars().get(0).getName());
    assertEquals("2026-01-31", action.getVars().get(0).getValue());
    assertEquals(1, action.getEnvVars().size());
    assertEquals("DBT_PASSWORD", action.getEnvVars().get(0).getName());
    assertEquals("secret", action.getEnvVars().get(0).getValue());
  }

  /**
   * The generated operation combo cannot be read-only, so a typo would otherwise be stored and run
   * as {@code dbt run} - {@link DbtOperation#fromCode} falls back to it for anything it does not
   * recognise. The dialog refuses to close instead.
   */
  @Test
  void okRefusesAnUnknownOperation() {
    ActionDbt action = actionWithValues();

    withDialog(
        parent -> new ActionDbtDialog(parent, action, new WorkflowMeta(), new Variables()).open(),
        bot -> {
          SWTBotShell dialogShell = dialogShell(bot);
          display.syncExec(() -> operationCombo(dialogShell.widget).setText("buidl"));
          dialogShell.bot().button(buttonLabel("System.Button.OK")).click();

          SWTBotShell complaint =
              bot.shell(
                  BaseMessages.getString(ActionDbt.class, "ActionDbt.UnknownOperation.Title"));
          complaint.bot().button(buttonLabel("System.Button.OK")).click();

          // The dbt dialog is still open, so nothing was written back.
          dialogShell.bot().button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals(DbtOperation.BUILD.getCode(), action.getOperation());
  }

  /** The combo holding the dbt operations, found by its contents rather than by index. */
  private static Combo operationCombo(Composite parent) {
    for (Control child : parent.getChildren()) {
      if (child instanceof Combo combo
          && List.of(combo.getItems()).contains(DbtOperation.BUILD.getCode())) {
        return combo;
      }
      if (child instanceof Composite composite) {
        Combo found = operationCombo(composite);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  private static ActionDbt actionWithValues() {
    ActionDbt action = new ActionDbt("dbt build");
    action.setDbtProjectName(PROJECT_NAME);
    action.setOperation(DbtOperation.BUILD.getCode());
    action.setTarget("prod");
    action.setSelect("tag:daily");
    action.setExclude("tag:slow");
    action.setThreads("4");
    action.setTimeout("900");
    action.setFullRefresh(true);
    action.setEmitOpenLineage(true);
    action.getVars().add(new DbtNameValue("run_date", "2026-01-31"));
    action.getEnvVars().add(new DbtNameValue("DBT_PASSWORD", "secret"));
    return action;
  }

  private SWTBotShell dialogShell(SWTBot bot) {
    SWTBotShell dialogShell = bot.shell(DIALOG_TITLE);
    bot.waitUntil(
        new DefaultCondition() {
          @Override
          public boolean test() {
            return dialogShell.isOpen() && dialogShell.isVisible();
          }

          @Override
          public String getFailureMessage() {
            return "The " + DIALOG_TITLE + " dialog never became visible";
          }
        });
    dialogShell.activate();
    return dialogShell;
  }

  private static CTabFolder findTabFolder(Composite parent) {
    for (Control child : parent.getChildren()) {
      if (child instanceof CTabFolder folder) {
        return folder;
      }
      if (child instanceof Composite composite) {
        CTabFolder found = findTabFolder(composite);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  private static Button findOkButton(Composite parent) {
    String okLabel = buttonLabel("System.Button.OK");
    for (Control child : parent.getChildren()) {
      if (child instanceof Button button && okLabel.equals(button.getText().replace("&", ""))) {
        return button;
      }
    }
    return null;
  }

  private static List<TableView> collectTables(Composite parent) {
    List<TableView> tables = new ArrayList<>();
    for (Control child : parent.getChildren()) {
      if (child instanceof TableView tableView) {
        tables.add(tableView);
      } else if (child instanceof Composite composite) {
        tables.addAll(collectTables(composite));
      }
    }
    return tables;
  }

  private static boolean isDescendantOf(Control control, Control ancestor) {
    for (Composite parent = control.getParent(); parent != null; parent = parent.getParent()) {
      if (parent == ancestor) {
        return true;
      }
    }
    return false;
  }

  private static Rectangle displayBounds(Control control) {
    Rectangle bounds = control.getBounds();
    Point origin = control.getParent().toDisplay(bounds.x, bounds.y);
    return new Rectangle(origin.x, origin.y, bounds.width, bounds.height);
  }
}
