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

package org.apache.hop.pipeline.transforms.xml.getxmldata;

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
 * The XML source field of Get data from XML must survive clicking into its drop-down, whether the
 * incoming fields load or not (issue #5953).
 */
@Tag("uitest")
class GetXmlDataDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "get-xml-data";
  private static final String TITLE =
      BaseMessages.getString(GetXmlDataMeta.class, "GetXMLDataDialog.DialogTitle");

  @Test
  void xmlFieldSurvivesFailingUpstream() {
    GetXmlDataMeta meta = sourceFromField("xml_doc");
    PipelineMeta pipelineMeta = UpstreamFixture.failingUpstream(TRANSFORM_NAME, meta);

    focusAndPressOk(meta, pipelineMeta);

    assertEquals("xml_doc", meta.getXmlField(), "OK must keep the configured XML field");
  }

  @Test
  void xmlFieldSurvivesLoadedUpstream() {
    GetXmlDataMeta meta = sourceFromField("xml_doc");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "xml_doc");

    focusAndPressOk(meta, pipelineMeta);

    assertEquals("xml_doc", meta.getXmlField());
  }

  @Test
  void xmlFieldNotInUpstreamIsKept() {
    GetXmlDataMeta meta = sourceFromField("renamed_field");
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(TRANSFORM_NAME, meta, "id", "xml_doc");

    focusAndPressOk(meta, pipelineMeta);

    assertEquals("renamed_field", meta.getXmlField());
  }

  /** Clicks into the XML field drop-down, as a user does, dismisses any error and presses OK. */
  private void focusAndPressOk(GetXmlDataMeta meta, PipelineMeta pipelineMeta) {
    AtomicReference<GetXmlDataDialog> dialog = new AtomicReference<>();
    withDialog(
        parent -> {
          GetXmlDataDialog d = new GetXmlDataDialog(parent, new Variables(), meta, pipelineMeta);
          dialog.set(d);
          d.open();
        },
        bot -> {
          bot.shell(TITLE).activate();
          postEvent(widget(dialog.get(), "wXMLField"), SWT.FocusIn);
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

  private static GetXmlDataMeta sourceFromField(String fieldName) {
    GetXmlDataMeta meta = new GetXmlDataMeta();
    meta.setDefault();
    meta.setInFields(true);
    meta.setXmlField(fieldName);
    return meta;
  }
}
