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

package org.apache.hop.pipeline.transforms.salesforceupsert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Widget;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The module and upsert key field of Salesforce upsert must survive when Salesforce can't be
 * reached to list the modules or the module fields (follow-up of issue #5953).
 */
@Tag("uitest")
class SalesforceUpsertDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "salesforce upsert";
  private static final String TITLE =
      BaseMessages.getString(SalesforceUpsertMeta.class, "SalesforceUpsertDialog.DialogTitle");

  /** Nothing listens on port 1, so connecting fails right away without touching the network. */
  private static final String UNREACHABLE_URL = "http://127.0.0.1:1/services/Soap/u/64.0";

  @Test
  void moduleSurvivesFailingModuleLookup() {
    SalesforceUpsertMeta meta = unreachableMeta();

    focusAndPressOk(meta, dialog -> ((ComboVar) readField(dialog, "wModule")).getCComboWidget());

    assertEquals("Contact", meta.getModule(), "OK must keep the configured module");
  }

  @Test
  void upsertFieldSurvivesFailingFieldLookup() {
    SalesforceUpsertMeta meta = unreachableMeta();

    focusAndPressOk(meta, dialog -> readField(dialog, "wUpsertField"));

    assertEquals("ExternalId__c", meta.getUpsertField(), "OK must keep the upsert key field");
  }

  private void focusAndPressOk(
      SalesforceUpsertMeta meta, Function<SalesforceUpsertDialog, Widget> focusTarget) {
    PipelineMeta pipelineMeta = UpstreamFixture.withoutUpstream(TRANSFORM_NAME, meta);
    AtomicReference<SalesforceUpsertDialog> dialog = new AtomicReference<>();
    withDialog(
        parent -> {
          dialog.set(new SalesforceUpsertDialog(parent, new Variables(), meta, pipelineMeta));
          dialog.get().open();
        },
        bot -> {
          bot.shell(TITLE);
          // Focusing the combo fetches its items from Salesforce, which fails.
          postEvent(focusTarget.apply(dialog.get()), SWT.FocusIn);
          assertTrue(closeOtherShells(TITLE, 3000) > 0, "the Salesforce lookup should have failed");
          bot.shell(TITLE).activate().bot().button(buttonLabel("System.Button.OK")).click();
        });
  }

  private static SalesforceUpsertMeta unreachableMeta() {
    SalesforceUpsertMeta meta = new SalesforceUpsertMeta();
    meta.setDefault();
    meta.setTargetUrl(UNREACHABLE_URL);
    meta.setUsername("user");
    meta.setPassword("secret");
    meta.setModule("Contact");
    meta.setUpsertField("ExternalId__c");
    return meta;
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
