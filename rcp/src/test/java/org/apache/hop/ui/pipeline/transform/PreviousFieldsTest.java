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

package org.apache.hop.ui.pipeline.transform;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.apache.hop.ui.testing.UpstreamFixture;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FillLayout;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** {@link PreviousFields} never throws, reports a failure once, and keeps combo values. */
@Tag("uitest")
class PreviousFieldsTest extends SwtBotTestBase {

  private static final String TRANSFORM = "target";
  private static final String SCENE_TITLE = "Hop SWTBot test";

  @Test
  void failingUpstreamGivesNoFieldsAndOneErrorDialog() {
    PipelineMeta pipelineMeta =
        UpstreamFixture.failingUpstream(TRANSFORM, new UpstreamFixture.UpstreamMeta(new String[0]));
    AtomicReference<PreviousFields> previousFields = new AtomicReference<>();
    AtomicReference<CCombo> combo = new AtomicReference<>();
    AtomicReference<String[]> names = new AtomicReference<>();
    AtomicReference<Boolean> available = new AtomicReference<>();
    AtomicReference<String> text = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          CCombo cCombo = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
          cCombo.setText("configured_field");
          combo.set(cCombo);
          previousFields.set(new PreviousFields(shell, new Variables(), pipelineMeta, TRANSFORM));
        },
        bot -> {
          // Every focus-in of a field combo asks again; only the first may bother the user.
          display.asyncExec(
              () -> {
                for (int i = 0; i < 3; i++) {
                  previousFields.get().fillCombos(combo.get());
                }
                names.set(previousFields.get().getFieldNames());
                available.set(previousFields.get().isAvailable());
                text.set(combo.get().getText());
              });
          assertEquals(1, closeOtherShells(SCENE_TITLE, 5000), "exactly one error dialog");
          display.syncExec(() -> {});
        });

    assertArrayEquals(new String[0], names.get());
    assertFalse(available.get());
    assertEquals("configured_field", text.get(), "a failed fetch must not wipe the value");
  }

  @Test
  void upstreamFieldsFillTheComboAndKeepTheValue() {
    PipelineMeta pipelineMeta =
        UpstreamFixture.upstreamWithFields(
            TRANSFORM, new UpstreamFixture.UpstreamMeta(new String[0]), "id", "name");
    AtomicReference<String[]> items = new AtomicReference<>();
    AtomicReference<String> text = new AtomicReference<>();
    AtomicReference<Boolean> available = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          CCombo cCombo = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
          cCombo.setText("renamed_field");
          PreviousFields previousFields =
              new PreviousFields(shell, new Variables(), pipelineMeta, TRANSFORM);
          previousFields.fillCombos(cCombo);
          items.set(cCombo.getItems());
          text.set(cCombo.getText());
          available.set(previousFields.isAvailable());
        },
        bot -> assertEquals(0, closeOtherShells(SCENE_TITLE, 500), "no error dialog"));

    assertArrayEquals(new String[] {"id", "name"}, items.get());
    assertEquals("renamed_field", text.get());
    assertTrue(available.get());
  }
}
