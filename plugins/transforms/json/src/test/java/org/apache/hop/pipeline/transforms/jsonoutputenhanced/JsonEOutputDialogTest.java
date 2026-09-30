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

package org.apache.hop.pipeline.transforms.jsonoutputenhanced;

import static org.eclipse.swtbot.swt.finder.matchers.WidgetMatcherFactory.widgetOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.utils.SWTBotPreferences;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotButton;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("uitest")
class JsonEOutputDialogTest extends SwtBotTestBase {
  /**
   * Enhanced JSON output builds several tables before its event loop starts. SWTBot's 5 s default
   * expires while that construction is still on the UI thread.
   */
  private static final long DIALOG_TIMEOUT_MS = 30_000L;

  private static long defaultTimeout;

  @BeforeAll
  static void slowDownSwtBot() {
    defaultTimeout = SWTBotPreferences.TIMEOUT;
    SWTBotPreferences.TIMEOUT = DIALOG_TIMEOUT_MS;
  }

  @AfterAll
  static void restoreSwtBotTimeout() {
    SWTBotPreferences.TIMEOUT = defaultTimeout;
  }

  @BeforeAll
  static void registerWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    Class<JsonEOutputMeta> type = JsonEOutputMeta.class;
    for (Field field : type.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(type.getName(), element, field);
      }
    }
    for (Method method : type.getDeclaredMethods()) {
      GuiWidgetElement element = method.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(element, method, type.getName(), type.getClassLoader());
      }
    }
  }

  private static TableView table(JsonEOutputDialog dialog, String name) {
    try {
      Field field = JsonEOutputDialog.class.getDeclaredField(name);
      field.setAccessible(true);
      return (TableView) field.get(dialog);
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  private static Rectangle onDisplay(Control control) {
    Point point = control.toDisplay(0, 0);
    Rectangle bounds = control.getBounds();
    return new Rectangle(point.x, point.y, bounds.width, bounds.height);
  }

  /**
   * Upstream fields come from the dialog's {@code getPrevTransformFields(variables, transformName)}
   * call. The string overload is not final, so the test supplies the live row without the field
   * helper from the unmerged dialog-retention pull request.
   */
  private PipelineMeta pipeline(JsonEOutputMeta meta, String... fieldNames) {
    PipelineMeta pipeline =
        new PipelineMeta() {
          @Override
          public IRowMeta getPrevTransformFields(IVariables variables, String transformName) {
            RowMeta row = new RowMeta();
            for (String fieldName : fieldNames) {
              row.addValueMeta(new ValueMetaString(fieldName));
            }
            return row;
          }
        };
    pipeline.setMetadataProvider(new MemoryMetadataProvider());
    pipeline.addTransform(new TransformMeta("EnhancedJsonOutput", "json", meta));
    return pipeline;
  }

  @Test
  void getKeysUsesLiveSelectionsAndCancelKeepsMetadata() {
    JsonEOutputMeta meta = new JsonEOutputMeta();
    meta.setOperationType(JsonEOutputMeta.OperationType.OUTPUT_VALUE);
    JsonEOutputField payload = new JsonEOutputField();
    payload.setFieldName("payload");
    payload.setElementName("payload");
    meta.getOutputFields().add(payload);
    JsonEOutputKeyField existing = new JsonEOutputKeyField("already");
    existing.setElementName("custom_alias");
    meta.getKeyFields().add(existing);
    PipelineMeta pipeline = pipeline(meta, "payload", "grp", "already", "unsaved");
    AtomicReference<JsonEOutputDialog> opened = new AtomicReference<>();
    AtomicReference<List<String>> names = new AtomicReference<>();
    AtomicReference<List<String>> aliases = new AtomicReference<>();
    AtomicReference<Rectangle> tableBounds = new AtomicReference<>();
    AtomicReference<Rectangle> buttonBounds = new AtomicReference<>();
    String title = BaseMessages.getString(JsonEOutputMeta.class, "JsonEOutputDialog.DialogTitle");
    String keyTab =
        BaseMessages.getString(JsonEOutputMeta.class, "JsonEOutputDialog.KeyConfigTab.TabTitle");
    String getButton =
        BaseMessages.getString(JsonEOutputMeta.class, "JsonEOutputDialog.Get.Button");
    withDialog(
        parent -> {
          opened.set(new JsonEOutputDialog(parent, new Variables(), meta, pipeline));
          opened.get().open();
        },
        bot -> {
          SWTBot dialog = bot.shell(title).activate().bot();
          display.syncExec(
              () -> {
                TableItem item = new TableItem(table(opened.get(), "wFields").table, SWT.NONE);
                item.setText(1, "unsaved");
                item.setText(2, "unsaved_alias");
              });
          Composite keyTabControl = activateTab(dialog, keyTab);
          Button getControl = findPushButton(keyTabControl, getButton);
          new SWTBotButton(getControl).click();
          new SWTBotButton(getControl).click();
          var shell = bot.shell(title);
          display.syncExec(
              () -> {
                TableView keys = table(opened.get(), "wKeyFields");
                names.set(keys.getNonEmptyItems().stream().map(item -> item.getText(1)).toList());
                aliases.set(keys.getNonEmptyItems().stream().map(item -> item.getText(2)).toList());
                shell.widget.setSize(600, 400);
                shell.widget.layout(true, true);
                tableBounds.set(onDisplay(keys));
                buttonBounds.set(onDisplay(getControl));
              });
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
    assertEquals(List.of("already", "grp"), names.get());
    assertEquals(List.of("custom_alias", "grp"), aliases.get());
    assertEquals(1, meta.getKeyFields().size(), "Cancel does not persist table additions");
    assertTrue(tableBounds.get().width > 0 && tableBounds.get().height > 0);
    assertFalse(
        tableBounds.get().intersects(buttonBounds.get()),
        tableBounds.get() + " overlaps " + buttonBounds.get());
  }

  @Test
  void ndjsonOptionIsSavedFromTheGroupedTab() {
    JsonEOutputMeta meta = new JsonEOutputMeta();
    meta.setOperationType(JsonEOutputMeta.OperationType.WRITE_TO_FILE);
    PipelineMeta pipeline = pipeline(meta, "payload");
    String title = BaseMessages.getString(JsonEOutputMeta.class, "JsonEOutputDialog.DialogTitle");
    String formatTab =
        BaseMessages.getString(JsonEOutputMeta.class, "JsonEOutputDialog.FileFormat.TabTitle");
    String ndjson = BaseMessages.getString(JsonEOutputMeta.class, "JsonEOutputDialog.NdJson.Label");
    withDialog(
        parent -> {
          silenceSortWarning();
          new JsonEOutputDialog(parent, new Variables(), meta, pipeline).open();
        },
        bot -> {
          SWTBot dialog = bot.shell(title).activate().bot();
          activateTab(dialog, formatTab);
          Button checkbox = checkBoxNextTo(dialog, ndjson);
          display.syncExec(() -> checkbox.setSelection(true));
          dialog.button(buttonLabel("System.Button.OK")).click();
        });
    assertTrue(meta.isNewlineDelimited());
  }

  @Test
  void ndjsonOptionCancelKeepsMetadata() {
    JsonEOutputMeta meta = new JsonEOutputMeta();
    meta.setOperationType(JsonEOutputMeta.OperationType.WRITE_TO_FILE);
    PipelineMeta pipeline = pipeline(meta, "payload");
    String title = BaseMessages.getString(JsonEOutputMeta.class, "JsonEOutputDialog.DialogTitle");
    String formatTab =
        BaseMessages.getString(JsonEOutputMeta.class, "JsonEOutputDialog.FileFormat.TabTitle");
    String ndjson = BaseMessages.getString(JsonEOutputMeta.class, "JsonEOutputDialog.NdJson.Label");
    withDialog(
        parent -> new JsonEOutputDialog(parent, new Variables(), meta, pipeline).open(),
        bot -> {
          SWTBot dialog = bot.shell(title).activate().bot();
          activateTab(dialog, formatTab);
          Button checkbox = checkBoxNextTo(dialog, ndjson);
          display.syncExec(() -> checkbox.setSelection(true));
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
    assertFalse(meta.isNewlineDelimited());
  }

  @Test
  void ndjsonOptionIsDisabledForOutputValue() {
    JsonEOutputMeta meta = new JsonEOutputMeta();
    meta.setOperationType(JsonEOutputMeta.OperationType.OUTPUT_VALUE);
    PipelineMeta pipeline = pipeline(meta, "payload");
    String title = BaseMessages.getString(JsonEOutputMeta.class, "JsonEOutputDialog.DialogTitle");
    String formatTab =
        BaseMessages.getString(JsonEOutputMeta.class, "JsonEOutputDialog.FileFormat.TabTitle");
    String ndjson = BaseMessages.getString(JsonEOutputMeta.class, "JsonEOutputDialog.NdJson.Label");
    AtomicBoolean enabled = new AtomicBoolean(true);
    withDialog(
        parent -> new JsonEOutputDialog(parent, new Variables(), meta, pipeline).open(),
        bot -> {
          SWTBot dialog = bot.shell(title).activate().bot();
          activateTab(dialog, formatTab);
          Button checkbox = checkBoxNextTo(dialog, ndjson);
          display.syncExec(() -> enabled.set(checkbox.getEnabled()));
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });
    assertFalse(enabled.get());
  }

  private static void silenceSortWarning() {
    PropsUi.getInstance().setCustomParameter(JsonEOutputDialog.STRING_SORT_WARNING_PARAMETER, "N");
  }

  /** Annotated checkboxes keep their title on a separate label. */
  private Button checkBoxNextTo(SWTBot dialog, String labelText) {
    Label label = dialog.label(labelText).widget;
    AtomicReference<Button> found = new AtomicReference<>();
    display.syncExec(
        () -> {
          for (Control child : label.getParent().getChildren()) {
            if (child instanceof Button button && (button.getStyle() & SWT.CHECK) != 0) {
              found.set(button);
              return;
            }
          }
          throw new AssertionError("no checkbox next to " + labelText);
        });
    if (found.get() == null) {
      throw new AssertionError("no checkbox next to " + labelText);
    }
    return found.get();
  }

  /**
   * SWTBot's finder walks controls and does not see the items of a {@code CTabFolder}. Select the
   * tab on the folder. The Fields tab has another button with the same Get Fields label, so the
   * click stays inside this tab.
   */
  private Composite activateTab(SWTBot dialog, String title) {
    CTabFolder tabFolder = (CTabFolder) dialog.widget(widgetOfType(CTabFolder.class));
    AtomicReference<Composite> selected = new AtomicReference<>();
    display.syncExec(
        () -> {
          List<String> titles = new ArrayList<>();
          for (CTabItem item : tabFolder.getItems()) {
            String itemTitle = item.getText() == null ? "" : item.getText().trim();
            titles.add(itemTitle);
            if (title.equals(itemTitle)) {
              tabFolder.setSelection(item);
              tabFolder.layout(true, true);
              selected.set((Composite) item.getControl());
              return;
            }
          }
          throw new AssertionError("no tab titled " + title + " in " + titles);
        });
    return selected.get();
  }

  private Button findPushButton(Composite root, String message) {
    AtomicReference<Button> found = new AtomicReference<>();
    List<String> seen = new ArrayList<>();
    display.syncExec(() -> found.set(searchPushButton(root, message, seen)));
    if (found.get() == null) {
      throw new AssertionError("no button [" + message + "] among " + seen);
    }
    return found.get();
  }

  private static Button searchPushButton(Control control, String message, List<String> seen) {
    if (control instanceof Button button && (button.getStyle() & SWT.PUSH) != 0) {
      seen.add(button.getText());
      if (sameButtonLabel(button.getText(), message)) {
        return button;
      }
    }
    if (control instanceof Composite composite) {
      for (Control child : composite.getChildren()) {
        Button match = searchPushButton(child, message, seen);
        if (match != null) {
          return match;
        }
      }
    }
    return null;
  }

  private static boolean sameButtonLabel(String widgetText, String message) {
    String widget = widgetText == null ? "" : widgetText.replace("&", "");
    String expected = message == null ? "" : message.replace("&", "");
    return widget.equals(expected);
  }
}
