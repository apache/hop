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

package org.apache.hop.pipeline.transforms.yamlinput;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Widget;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The YAML source field of YAML Input must survive clicking into its drop-down, whether the
 * incoming fields load or not (issue #5953).
 */
@Tag("uitest")
class YamlInputDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "yamlinput";
  private static final String TITLE =
      BaseMessages.getString(YamlInputMeta.class, "YamlInputDialog.DialogTitle");

  @Test
  void yamlFieldSurvivesFailingUpstream() {
    YamlInputMeta meta = configured("source_field");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusAndPressOk(meta, pipelineMeta);

    assertEquals("source_field", meta.getYamlField(), "OK must keep the configured field");
  }

  @Test
  void yamlFieldSurvivesLoadedUpstream() {
    YamlInputMeta meta = configured("source_field");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "source_field");

    focusAndPressOk(meta, pipelineMeta);

    assertEquals("source_field", meta.getYamlField());
  }

  @Test
  void yamlFieldNotInUpstreamIsKept() {
    YamlInputMeta meta = configured("renamed_field");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "source_field");

    focusAndPressOk(meta, pipelineMeta);

    assertEquals("renamed_field", meta.getYamlField());
  }

  /** Clicks into the drop-down, as a user does, dismisses any error and presses OK. */
  private void focusAndPressOk(YamlInputMeta meta, PipelineMeta pipelineMeta) {
    AtomicReference<YamlInputDialog> dialog = new AtomicReference<>();
    withDialog(
        parent -> {
          YamlInputDialog d = new YamlInputDialog(parent, new Variables(), meta, pipelineMeta);
          dialog.set(d);
          d.open();
        },
        bot -> {
          bot.shell(TITLE).activate();
          postEvent(widget(dialog.get(), "wYAMLLField"), SWT.FocusIn);
          closeOtherShells(TITLE, 3000);
          bot.shell(TITLE).activate().bot().button(buttonLabel("System.Button.OK")).click();
        });
  }

  private static Widget widget(Object dialog, String fieldName) {
    try {
      return (Widget) FieldUtils.readField(dialog, fieldName, true);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  private static YamlInputMeta configured(String fieldName) {
    YamlInputMeta meta = new YamlInputMeta();
    meta.setDefault();
    meta.setInFields(true);
    meta.setYamlField(fieldName);
    return meta;
  }
}
