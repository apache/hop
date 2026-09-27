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

package org.apache.hop.pipeline.transforms.propertyoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Property Output must open, and keep its key, value and filename fields on OK, when the incoming
 * fields can't be loaded.
 */
@Tag("uitest")
class PropertyOutputDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "properties";
  private static final String TITLE =
      BaseMessages.getString(PropertyOutputMeta.class, "PropertyOutputDialog.DialogTitle");

  @Test
  void opensAndKeepsFieldsWithFailingUpstream() {
    PropertyOutputMeta meta = configured();
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    openAndPressOk(meta, pipelineMeta);

    assertEquals("the_key", meta.getKeyField());
    assertEquals("the_value", meta.getValueField());
    assertEquals("the_file", meta.getFileNameField());
  }

  @Test
  void keepsFieldsNotInUpstream() {
    PropertyOutputMeta meta = configured();
    PipelineMeta pipelineMeta = UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "a", "b");

    openAndPressOk(meta, pipelineMeta);

    assertEquals("the_key", meta.getKeyField());
    assertEquals("the_value", meta.getValueField());
    assertEquals("the_file", meta.getFileNameField());
  }

  private void openAndPressOk(PropertyOutputMeta meta, PipelineMeta pipelineMeta) {
    withDialog(
        parent -> new PropertyOutputDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          // Opening the dialog tries to load the incoming fields; dismiss the error it shows.
          closeOtherShells(TITLE, 3000);
          bot.shell(TITLE).activate().bot().button(buttonLabel("System.Button.OK")).click();
        });
  }

  private static PropertyOutputMeta configured() {
    PropertyOutputMeta meta = new PropertyOutputMeta();
    meta.setKeyField("the_key");
    meta.setValueField("the_value");
    meta.setFileNameInField(true);
    meta.setFileNameField("the_file");
    return meta;
  }
}
