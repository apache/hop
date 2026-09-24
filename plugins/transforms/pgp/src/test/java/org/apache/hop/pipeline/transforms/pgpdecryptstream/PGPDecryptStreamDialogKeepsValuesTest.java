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

package org.apache.hop.pipeline.transforms.pgpdecryptstream;

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
 * The stream field and passphrase field of PGP decrypt stream must survive focusing either of them
 * when the incoming fields can't be loaded, for example because an upstream Table Input has broken
 * SQL. Both drop-downs are read-only, so a lost value can't be typed back in.
 */
@Tag("uitest")
class PGPDecryptStreamDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "pgpdecrypt";
  private static final String TITLE =
      BaseMessages.getString(PGPDecryptStreamMeta.class, "PGPDecryptStreamDialog.Shell.Title");

  @Test
  void streamFieldsSurviveFailingUpstreamOnStreamFieldFocus() {
    PGPDecryptStreamMeta meta = fromFields("data_col", "other_col");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusAndPressOk(meta, pipelineMeta, "wStreamFieldName");

    assertEquals("data_col", meta.getStreamField(), "OK must keep the configured stream field");
    assertEquals(
        "other_col", meta.getPassPhraseFieldName(), "OK must keep the configured passphrase field");
  }

  @Test
  void streamFieldsSurviveFailingUpstreamOnPassPhraseFieldNameFocus() {
    PGPDecryptStreamMeta meta = fromFields("data_col", "other_col");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusAndPressOk(meta, pipelineMeta, "wPassPhraseFieldName");

    assertEquals("data_col", meta.getStreamField(), "OK must keep the configured stream field");
    assertEquals(
        "other_col", meta.getPassPhraseFieldName(), "OK must keep the configured passphrase field");
  }

  @Test
  void fieldsNotInUpstreamAreKept() {
    PGPDecryptStreamMeta meta = fromFields("data_col", "other_col");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "name");

    focusAndPressOk(meta, pipelineMeta, "wStreamFieldName");

    assertEquals("data_col", meta.getStreamField());
    assertEquals("other_col", meta.getPassPhraseFieldName());
  }

  private void focusAndPressOk(
      PGPDecryptStreamMeta meta, PipelineMeta pipelineMeta, String comboField) {
    AtomicReference<PGPDecryptStreamDialog> dialog = new AtomicReference<>();
    withDialog(
        parent -> {
          PGPDecryptStreamDialog pgpDialog =
              new PGPDecryptStreamDialog(parent, new Variables(), meta, pipelineMeta);
          dialog.set(pgpDialog);
          pgpDialog.open();
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

  private static PGPDecryptStreamMeta fromFields(String streamField, String otherField) {
    PGPDecryptStreamMeta meta = new PGPDecryptStreamMeta();
    meta.setStreamField(streamField);
    meta.setPassPhraseFromField(true);
    meta.setPassPhraseFieldName(otherField);
    return meta;
  }
}
