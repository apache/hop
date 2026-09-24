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

package org.apache.hop.mail.pipeline.transforms.mailinput;

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
 * The dynamic folder field of Email messages input must survive focusing it when the incoming
 * fields can't be loaded, for example because an upstream Table Input has broken SQL.
 */
@Tag("uitest")
class MailInputDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "mailinput";
  private static final String TITLE =
      BaseMessages.getString(MailInputMeta.class, "MailInputdialog.Shell.Title");

  @Test
  void folderFieldSurvivesFailingUpstream() {
    MailInputMeta meta = folderFromField("folder_col");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusFolderFieldAndPressOk(meta, pipelineMeta);

    assertEquals("folder_col", meta.getFolderField(), "OK must keep the configured folder field");
  }

  @Test
  void folderFieldNotInUpstreamIsKept() {
    MailInputMeta meta = folderFromField("folder_col");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "name");

    focusFolderFieldAndPressOk(meta, pipelineMeta);

    assertEquals("folder_col", meta.getFolderField());
  }

  private void focusFolderFieldAndPressOk(MailInputMeta meta, PipelineMeta pipelineMeta) {
    AtomicReference<MailInputDialog> dialog = new AtomicReference<>();
    withDialog(
        parent -> {
          MailInputDialog mailInputDialog =
              new MailInputDialog(parent, new Variables(), meta, pipelineMeta);
          dialog.set(mailInputDialog);
          mailInputDialog.open();
        },
        bot -> {
          SWTBotShell shell = bot.shell(TITLE).activate();
          postEvent(widget(dialog.get(), "wFolderField"), SWT.FocusIn);
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

  private static MailInputMeta folderFromField(String folderField) {
    MailInputMeta meta = new MailInputMeta();
    meta.setDefault();
    meta.setUseDynamicFolder(true);
    meta.setFolderField(folderField);
    return meta;
  }
}
