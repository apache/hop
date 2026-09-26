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

package org.apache.hop.pipeline.transforms.salesforceinsert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The module of Salesforce insert must survive when the module list can't be retrieved from
 * Salesforce, for example because the server is unreachable (follow-up of issue #5953).
 */
@Tag("uitest")
class SalesforceInsertDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "salesforce insert";
  private static final String TITLE =
      BaseMessages.getString(SalesforceInsertMeta.class, "SalesforceInsertDialog.DialogTitle");

  /** Nothing listens on port 1, so connecting fails right away without touching the network. */
  private static final String UNREACHABLE_URL = "http://127.0.0.1:1/services/Soap/u/64.0";

  @Test
  void moduleSurvivesFailingModuleLookup() {
    SalesforceInsertMeta meta = new SalesforceInsertMeta();
    meta.setDefault();
    meta.setTargetUrl(UNREACHABLE_URL);
    meta.setUsername("user");
    meta.setPassword("secret");
    meta.setModule("Contact");
    PipelineMeta pipelineMeta = UpstreamFixture.withoutUpstream(TRANSFORM_NAME, meta);

    AtomicReference<SalesforceInsertDialog> dialog = new AtomicReference<>();
    withDialog(
        parent -> {
          dialog.set(new SalesforceInsertDialog(parent, new Variables(), meta, pipelineMeta));
          dialog.get().open();
        },
        bot -> {
          bot.shell(TITLE);
          ComboVar wModule = readField(dialog.get(), "wModule");
          // Focusing the module combo fetches the module list, which fails.
          postEvent(wModule.getCComboWidget(), SWT.FocusIn);
          assertTrue(closeOtherShells(TITLE, 3000) > 0, "the module lookup should have failed");
          bot.shell(TITLE).activate().bot().button(buttonLabel("System.Button.OK")).click();
        });

    assertEquals("Contact", meta.getModule(), "OK must keep the configured module");
  }

  @SuppressWarnings("unchecked")
  private static <T> T readField(Object target, String name) {
    try {
      return (T) FieldUtils.readField(target, name, true);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }
}
