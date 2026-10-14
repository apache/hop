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

package org.apache.hop.pipeline.transforms.openformula;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The formula grid stays above the button bar. */
@Tag("uitest")
class OpenFormulaDialogLayoutTest extends SwtBotTestBase {

  @BeforeAll
  static void registerDialogWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    String className = OpenFormulaMeta.class.getName();
    for (Method method : OpenFormulaMeta.class.getDeclaredMethods()) {
      GuiWidgetElement element = method.getAnnotation(GuiWidgetElement.class);
      if (element == null) {
        continue;
      }
      var parent = registry.findGuiElements(className, element.parentId());
      if (parent == null || parent.findChild(element.id()) == null) {
        registry.addGuiWidgetElement(
            element, method, className, OpenFormulaMeta.class.getClassLoader());
      }
    }
  }

  @Test
  void formulaTableSitsAboveOk() {
    OpenFormulaMeta meta = new OpenFormulaMeta();
    meta.getFormulas()
        .add(new OpenFormulaMetaFunction("label", "IF([flag];\"Y\";\"N\")", 2, -1, -1, ""));
    TransformMeta transformMeta = new TransformMeta("calc", meta);
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(transformMeta);

    withDialog(
        parent -> new OpenFormulaDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBotShell dialog =
              bot.shell(
                  BaseMessages.getString(OpenFormulaMeta.class, "OpenFormulaDialog.Shell.Title"));
          dialog.activate();
          display.syncExec(
              () -> {
                Shell shell = dialog.widget;
                shell.layout(true, true);
                TableView table = findTable(shell);
                Button ok = findButton(shell, BaseMessages.getString("System.Button.OK"));
                assertNotNull(table, "formula table");
                assertNotNull(ok, "OK button");
                Rectangle tableBounds = displayBounds(table);
                Rectangle okBounds = displayBounds(ok);
                assertTrue(
                    tableBounds.y + tableBounds.height <= okBounds.y,
                    "formula table overlaps the OK button");
              });
          dialog.close();
        });
  }

  private static TableView findTable(Composite parent) {
    for (Control child : parent.getChildren()) {
      if (child instanceof TableView tableView) {
        return tableView;
      }
      if (child instanceof Composite composite) {
        TableView found = findTable(composite);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  private static Button findButton(Composite parent, String text) {
    for (Control child : parent.getChildren()) {
      if (child instanceof Button button && text.equals(button.getText())) {
        return button;
      }
      if (child instanceof Composite composite) {
        Button found = findButton(composite, text);
        if (found != null) {
          return found;
        }
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
