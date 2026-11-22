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

package org.apache.hop.workflow.actions.airflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.airflow.metadata.AirflowConnection;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiImpl;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.workflow.WorkflowMeta;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.utils.SWTUtils;
import org.eclipse.swtbot.swt.finder.waits.DefaultCondition;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The Trigger Airflow DAG run dialog, built by GuiCompositeWidgets from the annotations on the
 * action. Run with tools/with-isolated-display.sh and -Puitest. Add -Dairflow.gui.capture=true to
 * write a screenshot of every tab to target/.
 */
@Tag("uitest")
class ActionAirflowTriggerDagRunDialogTest extends SwtBotTestBase {

  private static final Class<?> PKG = ActionAirflowTriggerDagRun.class;
  private static final String CONNECTION = "airflow";

  @BeforeAll
  static void registerActionWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    if (registry.findGuiElements(
            ActionAirflowTriggerDagRun.class.getName(),
            ActionAirflowTriggerDagRun.GUI_PLUGIN_ELEMENT_PARENT_ID)
        != null) {
      return;
    }
    for (Field field : ActionAirflowTriggerDagRun.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(ActionAirflowTriggerDagRun.class.getName(), element, field);
      }
    }
  }

  /** The connection line reads its metadata provider from the running Hop GUI. */
  @BeforeAll
  static void installHopGui() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    AirflowConnection connection = new AirflowConnection();
    connection.setName(CONNECTION);
    metadataProvider.getSerializer(AirflowConnection.class).save(connection);

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
  void theOptionsAreOnFourTabsAboveTheButtons() {
    List<String> tabs = new ArrayList<>();
    List<Rectangle> folderBounds = new ArrayList<>();
    List<Rectangle> okBounds = new ArrayList<>();

    withDialog(
        parent ->
            new ActionAirflowTriggerDagRunDialog(
                    parent, actionWithValues(), new WorkflowMeta(), new Variables())
                .open(),
        bot -> {
          SWTBotShell dialogShell = dialogShell(bot);
          display.syncExec(
              () -> {
                Shell shell = dialogShell.widget;
                shell.layout(true, true);
                CTabFolder folder = findTabFolder(shell);
                assertNotNull(folder, "The options are not laid out on tabs");
                for (int i = 0; i < folder.getItemCount(); i++) {
                  tabs.add(folder.getItem(i).getText());
                }
                folderBounds.add(displayBounds(folder));
                Button ok = findOkButton(shell);
                assertNotNull(ok, "The dialog has no OK button");
                okBounds.add(displayBounds(ok));
              });
          if (Boolean.getBoolean("airflow.gui.capture")) {
            for (int i = 0; i < 4; i++) {
              int tab = i;
              display.syncExec(
                  () -> {
                    findTabFolder(dialogShell.widget).setSelection(tab);
                    dialogShell.widget.layout(true, true);
                  });
              bot.sleep(250);
              display.syncExec(
                  () ->
                      SWTUtils.captureScreenshot(
                          "target/airflow-trigger-dag-run-tab-" + (tab + 1) + ".png"));
            }
          }
          dialogShell.bot().button(buttonLabel("System.Button.Cancel")).click();
        });

    assertEquals(
        List.of(
            BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Group.DagRun"),
            BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Group.Conf"),
            BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Group.Wait"),
            BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Group.Result")),
        tabs);
    assertFalse(folderBounds.get(0).intersects(okBounds.get(0)), "OK overlaps the options");
    assertTrue(
        okBounds.get(0).y >= folderBounds.get(0).y + folderBounds.get(0).height,
        "OK should sit below the options");
  }

  @Test
  void okKeepsEveryValueOfTheAction() {
    ActionAirflowTriggerDagRun action = actionWithValues();

    withDialog(
        parent ->
            new ActionAirflowTriggerDagRunDialog(
                    parent, action, new WorkflowMeta(), new Variables())
                .open(),
        bot -> dialogShell(bot).bot().button(buttonLabel("System.Button.OK")).click());

    assertEquals("Start the nightly DAG", action.getName());
    assertEquals(CONNECTION, action.getConnectionName());
    assertEquals("nightly", action.getDagId());
    assertEquals("hop-${RUN}", action.getDagRunId());
    assertEquals("2026-11-21T10:00:00+01:00", action.getLogicalDate());
    assertEquals("{\"count\": 3}", action.getConfJson());
    assertEquals(2, action.getConfEntries().size());
    assertEquals("greeting", action.getConfEntries().get(0).getName());
    assertEquals("hello", action.getConfEntries().get(0).getValue());
    assertEquals("source", action.getConfEntries().get(1).getName());
    assertEquals("${SOURCE}", action.getConfEntries().get(1).getValue());
    assertFalse(action.isWaitingForCompletion());
    assertEquals("30", action.getPollIntervalSeconds());
    assertEquals("3600", action.getTimeoutSeconds());
    assertEquals("RUN_ID", action.getDagRunIdVariable());
    assertEquals("RUN_STATE", action.getStateVariable());
  }

  private static ActionAirflowTriggerDagRun actionWithValues() {
    ActionAirflowTriggerDagRun action = new ActionAirflowTriggerDagRun("Start the nightly DAG");
    action.setConnectionName(CONNECTION);
    action.setDagId("nightly");
    action.setDagRunId("hop-${RUN}");
    action.setLogicalDate("2026-11-21T10:00:00+01:00");
    action.setConfJson("{\"count\": 3}");
    action.getConfEntries().add(new AirflowConfEntry("greeting", "hello"));
    action.getConfEntries().add(new AirflowConfEntry("source", "${SOURCE}"));
    action.setWaitingForCompletion(false);
    action.setPollIntervalSeconds("30");
    action.setTimeoutSeconds("3600");
    action.setDagRunIdVariable("RUN_ID");
    action.setStateVariable("RUN_STATE");
    return action;
  }

  private SWTBotShell dialogShell(SWTBot bot) {
    String title = BaseMessages.getString(PKG, "ActionAirflowTriggerDagRun.Name");
    SWTBotShell dialogShell = bot.shell(title);
    bot.waitUntil(
        new DefaultCondition() {
          @Override
          public boolean test() {
            return dialogShell.isOpen() && dialogShell.isVisible();
          }

          @Override
          public String getFailureMessage() {
            return "The " + title + " dialog never became visible";
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

  private static Rectangle displayBounds(Control control) {
    Rectangle bounds = control.getBounds();
    Point origin = control.getParent().toDisplay(bounds.x, bounds.y);
    return new Rectangle(origin.x, origin.y, bounds.width, bounds.height);
  }
}
