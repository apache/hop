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

package org.apache.hop.pipeline.transforms.jsoninput;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.exception.HopTransformException;
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
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.utils.SWTBotPreferences;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotCCombo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The source field stays put when the previous transform cannot supply its fields. */
@Tag("uitest")
class JsonInputSourceFieldDialogTest extends SwtBotTestBase {

  private static final String CONFIGURED_FIELD = "id_json";

  /**
   * JSON Input builds several tables before its event loop starts. SWTBot's 5 s default expires
   * while that construction is still on the UI thread.
   */
  private static final long DIALOG_TIMEOUT_MS = 30_000L;

  private static long defaultTimeout;

  @BeforeAll
  static void registerWidgets() {
    GuiRegistry registry = GuiRegistry.getInstance();
    for (Field field : JsonInputMeta.class.getDeclaredFields()) {
      GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);
      if (element != null) {
        registry.addGuiWidgetElement(JsonInputMeta.class.getName(), element, field);
      }
    }
  }

  @BeforeAll
  static void slowDownSwtBot() {
    defaultTimeout = SWTBotPreferences.TIMEOUT;
    SWTBotPreferences.TIMEOUT = DIALOG_TIMEOUT_MS;
  }

  @AfterAll
  static void restoreSwtBotTimeout() {
    SWTBotPreferences.TIMEOUT = defaultTimeout;
  }

  @Test
  void failedLookupKeepsTheConfiguredSourceField() {
    JsonInputMeta meta = sourceMeta();
    withDialog(
        parent ->
            new JsonInputDialog(parent, new Variables(), meta, new MissingFieldsPipeline(meta))
                .open(),
        bot -> {
          dismissFieldLookupError();
          SWTBot dialog = jsonDialog(bot);
          assertEquals(CONFIGURED_FIELD, sourceCombo(dialog).getText());
          dialog.button(buttonLabel("System.Button.OK")).click();
        });
    assertEquals(CONFIGURED_FIELD, meta.getFieldValue());
  }

  @Test
  void loadedFieldsKeepANameThatIsNotInTheList() {
    JsonInputMeta meta = sourceMeta();
    withDialog(
        parent ->
            new JsonInputDialog(parent, new Variables(), meta, new OtherFieldsPipeline(meta))
                .open(),
        bot -> {
          SWTBot dialog = jsonDialog(bot);
          SWTBotCCombo combo = sourceCombo(dialog);
          assertEquals(CONFIGURED_FIELD, combo.getText());
          AtomicReference<String> firstItem = new AtomicReference<>();
          display.syncExec(() -> firstItem.set(combo.widget.getItem(0)));
          assertEquals("other", firstItem.get());
          dialog.button(buttonLabel("System.Button.OK")).click();
        });
    assertEquals(CONFIGURED_FIELD, meta.getFieldValue());
  }

  private static JsonInputMeta sourceMeta() {
    JsonInputMeta meta = new JsonInputMeta();
    meta.setInFields(true);
    meta.setFieldValue(CONFIGURED_FIELD);
    return meta;
  }

  private static SWTBot jsonDialog(SWTBot bot) {
    return bot.shell(BaseMessages.getString(JsonInputMeta.class, "JsonInputDialog.DialogTitle"))
        .activate()
        .bot();
  }

  private static SWTBotCCombo sourceCombo(SWTBot dialog) {
    return dialog.ccomboBox();
  }

  private static void dismissFieldLookupError() {
    SWTBot error =
        new SWTBot()
            .shell(
                BaseMessages.getString(
                    JsonInputMeta.class, "JsonInputDialog.FailedToGetFields.DialogTitle"))
            .activate()
            .bot();
    error.button(buttonLabel("System.Button.OK")).click();
  }

  /** Previous-field lookup fails the way a broken upstream Table Input does. */
  private static final class MissingFieldsPipeline extends PipelineMeta {
    private MissingFieldsPipeline(JsonInputMeta meta) {
      prepare(this, meta);
    }

    @Override
    public IRowMeta getPrevTransformFields(IVariables variables, String transformName)
        throws HopTransformException {
      throw new HopTransformException("missing table");
    }
  }

  /** The lookup succeeds and does not contain the name already stored on the transform. */
  private static final class OtherFieldsPipeline extends PipelineMeta {
    private OtherFieldsPipeline(JsonInputMeta meta) {
      prepare(this, meta);
    }

    @Override
    public IRowMeta getPrevTransformFields(IVariables variables, String transformName) {
      RowMeta row = new RowMeta();
      row.addValueMeta(new ValueMetaString("other"));
      return row;
    }
  }

  private static void prepare(PipelineMeta pipeline, JsonInputMeta meta) {
    pipeline.setMetadataProvider(new MemoryMetadataProvider());
    pipeline.addTransform(new TransformMeta("JsonInput", "json", meta));
  }
}
