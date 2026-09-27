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

package org.apache.hop.pipeline.transforms.excelinput;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The "accept filenames from field" value of Microsoft Excel input must survive when the incoming
 * fields can't be loaded or don't contain it.
 */
@Tag("uitest")
class ExcelInputDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "excel";
  private static final String TITLE =
      BaseMessages.getString(ExcelInputMeta.class, "ExcelInputDialog.DialogTitle");

  @Test
  void acceptingFieldSurvivesFailingUpstream() {
    ExcelInputMeta meta = acceptingFromField("filename");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    openAndPressOk(meta, pipelineMeta);

    assertEquals("filename", meta.getAcceptingField());
  }

  @Test
  void acceptingFieldNotInUpstreamIsKept() {
    ExcelInputMeta meta = acceptingFromField("renamed_field");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "filename");

    openAndPressOk(meta, pipelineMeta);

    assertEquals("renamed_field", meta.getAcceptingField());
  }

  private void openAndPressOk(ExcelInputMeta meta, PipelineMeta pipelineMeta) {
    withDialog(
        parent -> new ExcelInputDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          // Opening the dialog tries to load the incoming fields; dismiss the error it shows.
          closeOtherShells(TITLE, 3000);
          bot.shell(TITLE).activate().bot().button(buttonLabel("System.Button.OK")).click();
        });
  }

  private static ExcelInputMeta acceptingFromField(String fieldName) {
    ExcelInputMeta meta = new ExcelInputMeta();
    meta.setDefault();
    meta.setAcceptingFilenames(true);
    meta.setAcceptingField(fieldName);
    meta.setAcceptingTransformName(UpstreamFixture.UPSTREAM_NAME);
    return meta;
  }
}
