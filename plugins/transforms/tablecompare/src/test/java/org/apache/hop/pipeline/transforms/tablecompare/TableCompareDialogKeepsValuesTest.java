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

package org.apache.hop.pipeline.transforms.tablecompare;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.waits.DefaultCondition;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotShell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Table Compare fills its field combos after the dialog opened. When the incoming fields can't be
 * loaded that must not break the dialog, and OK must keep the configured fields.
 */
@Tag("uitest")
class TableCompareDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "tablecompare";
  private static final String TITLE =
      BaseMessages.getString(TableCompareMeta.class, "TableCompareDialog.Shell.Title");

  @Test
  void fieldsSurviveFailingUpstream() {
    TableCompareMeta meta = configured();
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    openAndPressOk(meta, pipelineMeta);

    assertConfigured(meta);
  }

  @Test
  void fieldsNotInUpstreamAreKept() {
    TableCompareMeta meta = configured();
    PipelineMeta pipelineMeta = UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "a", "b");

    openAndPressOk(meta, pipelineMeta);

    assertConfigured(meta);
  }

  private void openAndPressOk(TableCompareMeta meta, PipelineMeta pipelineMeta) {
    withDialog(
        parent -> new TableCompareDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          SWTBot dialogBot = shownDialog(bot);
          // The incoming fields are loaded right after the dialog opened; dismiss the error.
          closeOtherShells(TITLE, 3000);
          // OK complains about the (unset) connections in message boxes, so post it and dismiss.
          postEvent(dialogBot.button(buttonLabel("System.Button.OK")).widget, SWT.Selection);
          closeOtherShells(TITLE, 2000);
        });
  }

  private static void assertConfigured(TableCompareMeta meta) {
    assertEquals("ref_schema", meta.getReferenceSchemaField());
    assertEquals("ref_table", meta.getReferenceTableField());
    assertEquals("cmp_schema", meta.getCompareSchemaField());
    assertEquals("cmp_table", meta.getCompareTableField());
    assertEquals("keys", meta.getKeyFieldsField());
    assertEquals("excludes", meta.getExcludeFieldsField());
    assertEquals("key_desc", meta.getKeyDescriptionField());
    assertEquals("ref_value", meta.getValueReferenceField());
    assertEquals("cmp_value", meta.getValueCompareField());
  }

  private static TableCompareMeta configured() {
    TableCompareMeta meta = new TableCompareMeta();
    meta.setDefault();
    meta.setReferenceSchemaField("ref_schema");
    meta.setReferenceTableField("ref_table");
    meta.setCompareSchemaField("cmp_schema");
    meta.setCompareTableField("cmp_table");
    meta.setKeyFieldsField("keys");
    meta.setExcludeFieldsField("excludes");
    meta.setKeyDescriptionField("key_desc");
    meta.setValueReferenceField("ref_value");
    meta.setValueCompareField("cmp_value");
    return meta;
  }

  /** The shell is found as soon as it is created; wait until open() has built and shown it. */
  private static SWTBot shownDialog(SWTBot bot) {
    SWTBotShell shell = bot.shell(TITLE);
    bot.waitUntil(
        new DefaultCondition() {
          @Override
          public boolean test() {
            return shell.isVisible();
          }

          @Override
          public String getFailureMessage() {
            return "Dialog " + TITLE + " was never shown";
          }
        });
    return shell.bot();
  }
}
