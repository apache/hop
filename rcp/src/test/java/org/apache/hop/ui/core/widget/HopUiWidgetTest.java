/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FillLayout;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("uitest")
class HopUiWidgetTest extends SwtBotTestBase {

  @Test
  void textVarRoundTripsTypedValue() {
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          new TextVar(new Variables(), shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        },
        bot -> {
          // The variable-insert image label is part of the TextVar composite (proves it built).
          assertNotNull(bot.label(), "TextVar should contribute its variable image label");
          bot.text().setText("Hello ${USER}");
          assertEquals("Hello ${USER}", bot.text().getText());
        });
  }

  @Test
  void labelTextShowsLabelAndCapturesInput() {
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          new LabelText(shell, "Name:", "Enter a name");
        },
        bot -> {
          assertNotNull(bot.label("Name:"), "LabelText should render its label");
          bot.text().setText("Apache Hop");
          assertEquals("Apache Hop", bot.text().getText());
        });
  }
}
