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

package org.apache.hop.pipeline.transforms.processfiles;

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
 * The source and target filename fields of Process Files must survive focusing either of them when
 * the incoming fields can't be loaded, for example because an upstream Table Input has broken SQL.
 */
@Tag("uitest")
class ProcessFilesDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "processfiles";
  private static final String TITLE =
      BaseMessages.getString(ProcessFilesMeta.class, "ProcessFilesDialog.Shell.Title");

  @Test
  void filenameFieldsSurviveFailingUpstreamOnSourceFocus() {
    ProcessFilesMeta meta = moveFiles("source_col", "target_col");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusAndPressOk(meta, pipelineMeta, "wSourceFileNameField");

    assertEquals("source_col", meta.getSourceFilenameField(), "OK must keep the source field");
    assertEquals("target_col", meta.getTargetFilenameField(), "OK must keep the target field");
  }

  @Test
  void filenameFieldsSurviveFailingUpstreamOnTargetFocus() {
    ProcessFilesMeta meta = moveFiles("source_col", "target_col");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusAndPressOk(meta, pipelineMeta, "wTargetFileNameField");

    assertEquals("source_col", meta.getSourceFilenameField(), "OK must keep the source field");
    assertEquals("target_col", meta.getTargetFilenameField(), "OK must keep the target field");
  }

  @Test
  void filenameFieldsNotInUpstreamAreKept() {
    ProcessFilesMeta meta = moveFiles("source_col", "target_col");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "name");

    focusAndPressOk(meta, pipelineMeta, "wSourceFileNameField");

    assertEquals("source_col", meta.getSourceFilenameField());
    assertEquals("target_col", meta.getTargetFilenameField());
  }

  private void focusAndPressOk(
      ProcessFilesMeta meta, PipelineMeta pipelineMeta, String comboField) {
    AtomicReference<ProcessFilesDialog> dialog = new AtomicReference<>();
    withDialog(
        parent -> {
          ProcessFilesDialog processFilesDialog =
              new ProcessFilesDialog(parent, new Variables(), meta, pipelineMeta);
          dialog.set(processFilesDialog);
          processFilesDialog.open();
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

  private static ProcessFilesMeta moveFiles(String sourceField, String targetField) {
    ProcessFilesMeta meta = new ProcessFilesMeta();
    meta.setDefault();
    meta.setOperationType(ProcessFilesMeta.OPERATION_TYPE_MOVE);
    meta.setSourceFilenameField(sourceField);
    meta.setTargetFilenameField(targetField);
    return meta;
  }
}
