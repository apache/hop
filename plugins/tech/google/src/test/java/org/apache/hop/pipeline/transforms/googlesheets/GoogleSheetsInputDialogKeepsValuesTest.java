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

package org.apache.hop.pipeline.transforms.googlesheets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The fields of Google Sheets input must survive a "Get fields" that fails, for example because the
 * credentials file can't be read (follow-up of issue #5953). The failure happens before any network
 * call.
 */
@Tag("uitest")
class GoogleSheetsInputDialogKeepsValuesTest extends SwtBotTestBase {

  private static final String TRANSFORM_NAME = "google sheets";
  private static final String TITLE =
      BaseMessages.getString(GoogleSheetsInputMeta.class, "GoogleSheetsInput.transform.Name");
  private static final String GET_FIELDS =
      BaseMessages.getString(GoogleSheetsInputMeta.class, "System.Button.GetFields")
          .replace("&", "")
          .trim();

  @Test
  void fieldsSurviveFailingGetFields() {
    GoogleSheetsInputMeta meta = new GoogleSheetsInputMeta();
    meta.setDefault();
    meta.setJsonCredentialPath("/no/such/folder/missing-credentials.json");
    meta.setSpreadsheetKey("spreadsheet-key");
    meta.setWorksheetId("Sheet1");
    List<GoogleSheetsInputField> fields = new ArrayList<>();
    fields.add(field("id", IValueMeta.TYPE_INTEGER));
    fields.add(field("name", IValueMeta.TYPE_STRING));
    meta.setInputFields(fields);
    PipelineMeta pipelineMeta = UpstreamFixture.withoutUpstream(TRANSFORM_NAME, meta);

    withDialog(
        parent -> new GoogleSheetsInputDialog(parent, new Variables(), meta, pipelineMeta).open(),
        bot -> {
          bot.shell(TITLE).activate();
          Button getFields = findButton(TITLE, GET_FIELDS);
          postEvent(getFields, SWT.Selection);
          assertTrue(closeOtherShells(TITLE, 3000) > 0, "getting the fields should fail");
          bot.shell(TITLE).activate().bot().button(buttonLabel("System.Button.OK")).click();
        });

    List<String> names = new ArrayList<>();
    for (GoogleSheetsInputField field : meta.getInputFields()) {
      names.add(field.getName());
    }
    assertEquals(List.of("id", "name"), names, "OK must keep the configured fields");
  }

  private static GoogleSheetsInputField field(String name, int type) {
    GoogleSheetsInputField field = new GoogleSheetsInputField();
    field.setName(name);
    field.setType(type);
    return field;
  }

  /**
   * Finds a button of the dialog by its label, also on a tab that isn't in front (SWTBot's finder
   * never looks inside the tab items of a CTabFolder).
   */
  private static Button findButton(String shellTitle, String label) {
    AtomicReference<Button> found = new AtomicReference<>();
    display.syncExec(
        () -> {
          for (Shell shell : display.getShells()) {
            if (shellTitle.equals(shell.getText())) {
              found.set(findButton(shell, label));
            }
          }
        });
    assertNotNull(found.get(), "no button labeled " + label);
    return found.get();
  }

  private static Button findButton(Composite parent, String label) {
    for (Control child : parent.getChildren()) {
      if (child instanceof Button button
          && label.equals(button.getText().replace("&", "").trim())) {
        return button;
      }
      if (child instanceof Composite composite) {
        Button button = findButton(composite, label);
        if (button != null) {
          return button;
        }
      }
    }
    return null;
  }
}
