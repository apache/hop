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

package org.apache.hop.pipeline.transforms.databasejoin;

import static org.eclipse.swtbot.swt.finder.matchers.WidgetMatcherFactory.widgetOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.exceptions.WidgetNotFoundException;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * SWTBot coverage for the tabbed layout of {@link DatabaseJoinDialog} (#8655).
 *
 * <p>The dialog used to stack the connection, cache, SQL editor, resolved parameters, query options
 * and the parameter grid into one flat form with hardcoded pixel heights, so it could not be
 * resized sensibly: the editor and both grids competed for the same vertical space. The three
 * sections are now separate tabs.
 *
 * <p>Tagged {@code uitest} so it is skipped when there is no display. Wrap Maven with {@code
 * tools/with-isolated-display.sh} so the dialog does not steal focus.
 */
@Tag("uitest")
class DatabaseJoinDialogTest extends SwtBotTestBase {

  private static final Class<?> PKG = DatabaseJoinMeta.class;

  private static final String TRANSFORM_NAME = "database join";
  private static final String SHELL_TITLE = "Database join";
  private static final String ERROR_TITLE =
      BaseMessages.getString(PKG, "DatabaseJoinDialog.InvalidConnection.DialogTitle");

  private static final String TAB_CONNECTION = "Connection";
  private static final String TAB_SQL = "SQL";
  private static final String TAB_PARAMETERS = "Parameters";

  private static final String CONNECTION = "none";
  private static final String SQL = "select id from lookup where code = ?";

  /**
   * The tabs are built from the annotated widgets on {@link DatabaseJoinMeta}, which the framework
   * looks up in the registry by class name. The unit-test JVM does not always run the {@code
   * GuiPluginType} scan, so register them the way that scan does.
   */
  @BeforeAll
  static void registerMetaWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    if (registry.findGuiElements(
            DatabaseJoinMeta.class.getName(), DatabaseJoinMeta.GUI_PLUGIN_ELEMENT_PARENT_ID)
        != null) {
      return;
    }
    for (Field field : DatabaseJoinMeta.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(DatabaseJoinMeta.class.getName(), element, field);
      }
    }
  }

  @Test
  void theThreeSectionsAreSeparateTabsInOrder() {
    withDialog(
        openerFor(new DatabaseJoinMeta()),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();

          assertEquals(
              List.of(TAB_CONNECTION, TAB_SQL, TAB_PARAMETERS),
              tabTitles(dialog),
              "the dialog should be split into Connection, SQL and Parameters, in that order");

          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  /** The editor and the resolved-parameter table belong on the SQL tab, not a tab each. */
  @Test
  void theSqlTabHoldsBothTheEditorAndTheResolvedParameters() {
    DatabaseJoinMeta meta = new DatabaseJoinMeta();
    meta.setConnection(CONNECTION);
    meta.setSql(SQL);

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();

          selectTab(dialog, TAB_SQL);
          assertEquals(
              SQL,
              sqlEditorText(dialog),
              "the stored SQL should be shown in the editor on the tab");
          assertEquals(
              1,
              tableCount(dialog),
              "the SQL tab holds the resolved-parameter table next to the editor");

          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  /** The declared parameter grid is the only table on the Parameters tab. */
  @Test
  void theParametersTabHoldsTheDeclaredParameterGrid() {
    DatabaseJoinMeta meta = new DatabaseJoinMeta();
    meta.setConnection(CONNECTION);
    meta.setSql(SQL);

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();

          selectTab(dialog, TAB_PARAMETERS);
          assertEquals(1, tableCount(dialog), "the Parameters tab holds one grid");
          assertEquals(
              null, sqlEditorText(dialog), "the SQL editor belongs on the SQL tab, not here");

          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  @Test
  void okStoresTheEditorContents() {
    DatabaseJoinMeta meta = new DatabaseJoinMeta();
    meta.setConnection(CONNECTION);
    meta.setSql(SQL);

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();
          selectTab(dialog, TAB_SQL);
          dialog.button(buttonLabel("System.Button.OK")).click();
        });

    assertEquals(SQL, meta.getSql(), "OK must store the SQL held in the editor");
  }

  /**
   * A transform with no connection cannot run, and the dialog says so. OK used to show that error
   * and then dispose anyway, committing the very connection it had just called invalid.
   */
  @Test
  void okKeepsTheDialogOpenWhenTheConnectionIsInvalid() {
    DatabaseJoinMeta meta = new DatabaseJoinMeta();
    meta.setSql(SQL);

    withDialog(
        openerFor(meta),
        bot -> {
          SWTBot dialog = bot.shell(SHELL_TITLE).activate().bot();

          dialog.button(buttonLabel("System.Button.OK")).click();

          // Dismiss the error box the OK press raises, then check the dialog is still there.
          SWTBot error = bot.shell(ERROR_TITLE).activate().bot();
          error.button(buttonLabel("System.Button.OK")).click();

          assertTrue(
              shellIsOpen(dialog),
              "the dialog must stay open after reporting an invalid connection");

          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
  }

  private boolean shellIsOpen(SWTBot dialog) {
    for (SWTBotShell shell : dialog.shells()) {
      if (SHELL_TITLE.equals(shell.getText()) && shell.isOpen()) {
        return true;
      }
    }
    return false;
  }

  private List<String> tabTitles(SWTBot dialog) {
    List<String> titles = new ArrayList<>();
    display.syncExec(
        () -> {
          for (CTabItem item : tabFolder(dialog).getItems()) {
            // The look-and-feel pads tab labels with spaces.
            titles.add(item.getText().trim());
          }
        });
    return titles;
  }

  /**
   * Brings a tab to the front. Done on the folder rather than through {@code bot.cTabItem(...)}
   * because SWTBot's widget finder only walks controls and never sees a dialog's tab items.
   */
  private void selectTab(SWTBot dialog, String title) {
    display.syncExec(
        () -> {
          CTabFolder folder = tabFolder(dialog);
          for (CTabItem item : folder.getItems()) {
            if (item.getText().trim().equals(title)) {
              folder.setSelection(item);
              folder.layout();
              return;
            }
          }
        });
    assertTrue(
        tabTitles(dialog).contains(title),
        "expected a '" + title + "' tab, found " + tabTitles(dialog));
  }

  private CTabFolder tabFolder(SWTBot dialog) {
    return (CTabFolder) dialog.widget(widgetOfType(CTabFolder.class));
  }

  /** The SQL editor on the selected tab, or null when the tab holds none. */
  private String sqlEditorText(SWTBot dialog) {
    try {
      return dialog.styledText().getText();
    } catch (WidgetNotFoundException e) {
      return null;
    }
  }

  /**
   * How many grids the selected tab shows. Only the selected tab is laid out and visible to SWTBot,
   * so this is a per-tab count: the SQL tab has the resolved-parameter table, the Parameters tab
   * the declared-parameter grid.
   */
  private int tableCount(SWTBot dialog) {
    int count = 0;
    for (int i = 0; ; i++) {
      try {
        dialog.table(i);
        count++;
      } catch (WidgetNotFoundException | IndexOutOfBoundsException e) {
        return count;
      }
    }
  }

  private Consumer<Shell> openerFor(DatabaseJoinMeta meta) {
    PipelineMeta pipelineMeta = pipelineWith(meta);
    return parent -> new DatabaseJoinDialog(parent, new Variables(), meta, pipelineMeta).open();
  }

  private static PipelineMeta pipelineWith(DatabaseJoinMeta meta) {
    String pluginId = PluginRegistry.getInstance().getPluginId(TransformPluginType.class, meta);
    assertNotNull(pluginId, "Database join transform must be registered via HopEnvironment.init()");
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(new TransformMeta(pluginId, TRANSFORM_NAME, meta));
    return pipelineMeta;
  }
}
