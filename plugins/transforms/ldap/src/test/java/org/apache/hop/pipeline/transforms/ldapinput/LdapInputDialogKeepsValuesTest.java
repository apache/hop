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

package org.apache.hop.pipeline.transforms.ldapinput;

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
 * The dynamic search base and filter fields of LDAP Input must survive focusing them, both when the
 * incoming fields can't be loaded and when they can.
 */
@Tag("uitest")
class LdapInputDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "ldapinput";
  private static final String TITLE =
      BaseMessages.getString(LdapInputMeta.class, "LdapInputDialog.DialogTitle");

  @Test
  void searchBaseFieldSurvivesFailingUpstream() {
    LdapInputMeta meta = dynamicSearch("base_col", "filter_col");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusAndPressOk(meta, pipelineMeta, "wSearchBaseField");

    assertEquals(
        "base_col",
        meta.getDynamicSearchFieldName(),
        "OK must keep the configured search base field");
    assertEquals("filter_col", meta.getDynamicFilterFieldName());
  }

  @Test
  void filterFieldIsNotOverwrittenBySearchBaseField() {
    LdapInputMeta meta = dynamicSearch("base_col", "filter_col");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "base_col", "filter_col");

    focusAndPressOk(meta, pipelineMeta, "wFilterField");

    assertEquals("base_col", meta.getDynamicSearchFieldName());
    assertEquals(
        "filter_col",
        meta.getDynamicFilterFieldName(),
        "loading the incoming fields must not overwrite the filter field");
  }

  @Test
  void fieldsNotInUpstreamAreKept() {
    LdapInputMeta meta = dynamicSearch("base_col", "filter_col");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "name");

    focusAndPressOk(meta, pipelineMeta, "wSearchBaseField");

    assertEquals("base_col", meta.getDynamicSearchFieldName());
    assertEquals("filter_col", meta.getDynamicFilterFieldName());
  }

  private void focusAndPressOk(LdapInputMeta meta, PipelineMeta pipelineMeta, String comboField) {
    AtomicReference<LdapInputDialog> dialog = new AtomicReference<>();
    withDialog(
        parent -> {
          LdapInputDialog ldapDialog =
              new LdapInputDialog(parent, new Variables(), meta, pipelineMeta);
          dialog.set(ldapDialog);
          ldapDialog.open();
        },
        bot -> {
          SWTBotShell shell = bot.shell(TITLE).activate();
          postEvent(widget(dialog.get(), comboField), SWT.FocusIn);
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

  private static LdapInputMeta dynamicSearch(String searchBaseField, String filterField) {
    LdapInputMeta meta = new LdapInputMeta();
    meta.setDefault();
    meta.setDynamicSearch(true);
    meta.setDynamicSearchFieldName(searchBaseField);
    meta.setDynamicFilter(true);
    meta.setDynamicFilterFieldName(filterField);
    return meta;
  }
}
