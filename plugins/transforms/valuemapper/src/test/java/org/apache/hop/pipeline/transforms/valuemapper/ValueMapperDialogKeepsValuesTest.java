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

package org.apache.hop.pipeline.transforms.valuemapper;

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
import org.eclipse.swtbot.swt.finder.waits.Conditions;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The "field to use" of Value Mapper must survive focusing it when the incoming fields can't be
 * loaded, for example because an upstream Table Input has broken SQL. The drop-down is read-only,
 * so a lost value can't be typed back in.
 */
@Tag("uitest")
class ValueMapperDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "valuemapper";
  private static final String TITLE =
      BaseMessages.getString(ValueMapperMeta.class, "ValueMapperDialog.DialogTitle");

  @Test
  void fieldToUseSurvivesFailingUpstream() {
    ValueMapperMeta meta = mapField("status");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusFieldNameAndPressOk(meta, pipelineMeta);

    assertEquals("status", meta.getFieldToUse(), "OK must keep the configured field to use");
  }

  @Test
  void fieldToUseNotInUpstreamIsKept() {
    ValueMapperMeta meta = mapField("status");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "name");

    focusFieldNameAndPressOk(meta, pipelineMeta);

    assertEquals("status", meta.getFieldToUse());
  }

  private void focusFieldNameAndPressOk(ValueMapperMeta meta, PipelineMeta pipelineMeta) {
    AtomicReference<ValueMapperDialog> dialog = new AtomicReference<>();
    withDialog(
        parent -> {
          ValueMapperDialog valueMapperDialog =
              new ValueMapperDialog(parent, new Variables(), meta, pipelineMeta);
          dialog.set(valueMapperDialog);
          valueMapperDialog.open();
        },
        bot -> {
          SWTBotShell shell = bot.shell(TITLE).activate();
          postEvent(widget(dialog.get(), "wFieldName"), SWT.FocusIn);
          closeOtherShells(TITLE, 2000);
          shell.activate().bot().button(buttonLabel("System.Button.OK")).click();
          bot.waitUntil(Conditions.shellCloses(shell));
        });
  }

  private static Widget widget(Object dialog, String fieldName) {
    try {
      return (Widget) FieldUtils.readField(dialog, fieldName, true);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  private static ValueMapperMeta mapField(String fieldToUse) {
    ValueMapperMeta meta = new ValueMapperMeta();
    meta.setDefault();
    meta.setFieldToUse(fieldToUse);
    return meta;
  }
}
