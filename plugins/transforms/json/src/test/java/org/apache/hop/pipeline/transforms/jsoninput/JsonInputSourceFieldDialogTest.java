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

  /**
   * The dialog loads fields once while it is built, then looks them up again from {@code getData}.
   * {@code removeAll()} before that second lookup clears both the text and the list just loaded.
   */
  @Test
  void secondFailedLookupKeepsTheFieldAndTheLoadedList() {
    JsonInputMeta meta = sourceMeta();
    withDialog(
        parent ->
            new JsonInputDialog(parent, new Variables(), meta, new FailAfterFirstLookup(meta))
                .open(),
        bot -> {
          dismissFieldLookupError();
          SWTBot dialog = jsonDialog(bot);
          SWTBotCCombo combo = sourceCombo(dialog);
          assertEquals(CONFIGURED_FIELD, combo.getText());
          int[] itemCount = {0};
          String[] firstItem = {null};
          display.syncExec(
              () -> {
                itemCount[0] = combo.widget.getItemCount();
                if (itemCount[0] > 0) {
                  firstItem[0] = combo.widget.getItem(0);
                }
              });
          assertEquals(1, itemCount[0]);
          assertEquals("other", firstItem[0]);
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

  /**
   * The construction-time lookup succeeds. The lookup from {@code getData} fails, which is where
   * {@code removeAll()} used to wipe the configured name.
   */
  private static final class FailAfterFirstLookup extends PipelineMeta {
    private int calls;

    private FailAfterFirstLookup(JsonInputMeta meta) {
      prepare(this, meta);
    }

    @Override
    public IRowMeta getPrevTransformFields(IVariables variables, String transformName)
        throws HopTransformException {
      if (calls++ == 0) {
        RowMeta row = new RowMeta();
        row.addValueMeta(new ValueMetaString("other"));
        return row;
      }
      throw new HopTransformException("missing table");
    }
  }

  private static void prepare(PipelineMeta pipeline, JsonInputMeta meta) {
    pipeline.setMetadataProvider(new MemoryMetadataProvider());
    pipeline.addTransform(new TransformMeta("JsonInput", "json", meta));
  }
}
