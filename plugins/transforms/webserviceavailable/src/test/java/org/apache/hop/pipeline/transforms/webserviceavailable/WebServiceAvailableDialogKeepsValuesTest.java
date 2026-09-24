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

package org.apache.hop.pipeline.transforms.webserviceavailable;

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
 * The URL field of Check if webservice is available must survive clicking into its drop-down when
 * the incoming fields can't be loaded, for example because an upstream Table Input has broken SQL
 * (issue #5953).
 */
@Tag("uitest")
class WebServiceAvailableDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "webserviceavailable";
  private static final String TITLE =
      BaseMessages.getString(
          WebServiceAvailableMeta.class, "WebServiceAvailableDialog.Shell.Title");

  @Test
  void urlFieldSurvivesFailingUpstream() {
    WebServiceAvailableMeta meta = configured("source_field");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusAndPressOk(meta, pipelineMeta);

    assertEquals("source_field", meta.getUrlField(), "OK must keep the configured field");
  }

  /** Clicks into the drop-down, as a user does, dismisses any error and presses OK. */
  private void focusAndPressOk(WebServiceAvailableMeta meta, PipelineMeta pipelineMeta) {
    AtomicReference<WebServiceAvailableDialog> dialog = new AtomicReference<>();
    withDialog(
        parent -> {
          WebServiceAvailableDialog d =
              new WebServiceAvailableDialog(parent, new Variables(), meta, pipelineMeta);
          dialog.set(d);
          d.open();
        },
        bot -> {
          bot.shell(TITLE).activate();
          postEvent(widget(dialog.get(), "wURL"), SWT.FocusIn);
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

  private static WebServiceAvailableMeta configured(String fieldName) {
    WebServiceAvailableMeta meta = new WebServiceAvailableMeta();
    meta.setDefault();
    meta.setUrlField(fieldName);
    return meta;
  }
}
