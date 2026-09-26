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

package org.apache.hop.pipeline.transforms.httppost;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.eclipse.swtbot.swt.finder.waits.Conditions;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The URL field and request entity field of HTTP Post must survive focusing those fields when the
 * incoming fields can't be loaded, for example because an upstream Table Input has broken SQL.
 */
@Tag("uitest")
class HttpPostDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "httppost";
  private static final String TITLE =
      BaseMessages.getString(HttpPostMeta.class, "HTTPPOSTDialog.Shell.Title");

  @Test
  void urlFieldSurvivesFailingUpstream() {
    HttpPostMeta meta = urlFromField("url_col", "body_col");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusFieldsAndPressOk(meta, pipelineMeta, "wUrlField");

    assertEquals("url_col", meta.getUrlField(), "OK must keep the configured URL field");
    assertEquals("body_col", meta.getRequestEntity());
  }

  @Test
  void requestEntitySurvivesFailingUpstream() {
    HttpPostMeta meta = urlFromField("url_col", "body_col");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusFieldsAndPressOk(meta, pipelineMeta, "wRequestEntity");

    assertEquals("url_col", meta.getUrlField());
    assertEquals("body_col", meta.getRequestEntity(), "OK must keep the request entity field");
  }

  @Test
  void fieldsNotInUpstreamAreKept() {
    HttpPostMeta meta = urlFromField("url_col", "body_col");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "name");

    focusFieldsAndPressOk(meta, pipelineMeta, "wUrlField", "wRequestEntity");

    assertEquals("url_col", meta.getUrlField());
    assertEquals("body_col", meta.getRequestEntity());
  }

  private void focusFieldsAndPressOk(
      HttpPostMeta meta, PipelineMeta pipelineMeta, String... comboFields) {
    AtomicReference<HttpPostDialog> dialog = new AtomicReference<>();
    withDialog(
        parent -> {
          HttpPostDialog httpPostDialog =
              new HttpPostDialog(parent, new Variables(), meta, pipelineMeta);
          dialog.set(httpPostDialog);
          httpPostDialog.open();
        },
        bot -> {
          SWTBotShell shell = bot.shell(TITLE).activate();
          for (String comboField : comboFields) {
            // The focus listener sits on the CCombo inside the ComboVar.
            postEvent(comboVar(dialog.get(), comboField).getCComboWidget(), SWT.FocusIn);
            closeOtherShells(TITLE, 2000);
          }
          shell.activate().bot().button(buttonLabel("System.Button.OK")).click();
          bot.waitUntil(Conditions.shellCloses(shell));
        });
  }

  private static ComboVar comboVar(HttpPostDialog dialog, String fieldName) {
    try {
      return (ComboVar) FieldUtils.readField(dialog, fieldName, true);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  private static HttpPostMeta urlFromField(String urlField, String requestEntity) {
    HttpPostMeta meta = new HttpPostMeta();
    meta.setDefault();
    meta.setUrlInField(true);
    meta.setUrlField(urlField);
    meta.setRequestEntity(requestEntity);
    return meta;
  }
}
