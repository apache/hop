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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The "Select field" value of JSON Input must survive when the incoming fields can't be loaded, for
 * example because an upstream Table Input has broken SQL (issue #5953).
 */
@Tag("uitest")
class JsonInputDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "json";
  private static final String TITLE =
      BaseMessages.getString(JsonInputMeta.class, "JsonInputDialog.DialogTitle");

  @Test
  void sourceFieldSurvivesFailingUpstream() {
    JsonInputMeta meta = sourceFromField("id_json");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    openAndPressOk(meta, pipelineMeta);

    assertEquals("id_json", meta.getFieldValue(), "OK must keep the configured source field");
  }

  @Test
  void sourceFieldNotInUpstreamIsKept() {
    JsonInputMeta meta = sourceFromField("renamed_field");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "id_json");

    openAndPressOk(meta, pipelineMeta);

    assertEquals("renamed_field", meta.getFieldValue());
  }

  @Test
  void dialogIsLaidOutAfterFailingUpstream() {
    JsonInputMeta meta = sourceFromField("id_json");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);
    AtomicReference<JsonInputDialog> dialog = new AtomicReference<>();
    AtomicReference<Rectangle> fieldBounds = new AtomicReference<>();
    AtomicInteger errors = new AtomicInteger();

    withDialog(
        parent -> {
          dialog.set(new JsonInputDialog(parent, new Variables(), meta, pipelineMeta));
          dialog.get().open();
        },
        bot -> {
          errors.set(closeOtherShells(TITLE, 3000));
          SWTBot dialogBot = bot.shell(TITLE).activate().bot();
          // The error must not interrupt building the dialog: its widgets must be laid out.
          display.syncExec(
              () ->
                  fieldBounds.set(((Control) readField(dialog.get(), "wFieldValue")).getBounds()));
          dialogBot.button(buttonLabel("System.Button.OK")).click();
        });

    assertEquals(1, errors.get(), "the failure to load the fields is reported once");
    assertTrue(
        fieldBounds.get().width > 0 && fieldBounds.get().height > 0,
        "the source field combo must be laid out, was " + fieldBounds.get());
  }

  private static Object readField(Object target, String name) {
    try {
      return FieldUtils.readField(target, name, true);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  private void openAndPressOk(JsonInputMeta meta, PipelineMeta pipelineMeta) {
    withDialog(
        parent -> new JsonInputDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          // Opening the dialog tries to load the incoming fields; dismiss the error it shows.
          closeOtherShells(TITLE, 3000);
          bot.shell(TITLE).activate().bot().button(buttonLabel("System.Button.OK")).click();
        });
  }

  private static JsonInputMeta sourceFromField(String fieldName) {
    JsonInputMeta meta = new JsonInputMeta();
    meta.setDefault();
    meta.setInFields(true);
    meta.setFieldValue(fieldName);
    return meta;
  }
}
