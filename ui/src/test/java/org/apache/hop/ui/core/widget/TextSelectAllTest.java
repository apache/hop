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

package org.apache.hop.ui.core.widget;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Text;
import org.junit.jupiter.api.Test;

class TextSelectAllTest {

  @Test
  void selectsTextComboAndStyledText() {
    Text text = mock(Text.class);
    assertTrue(TextSelectAll.selectAll(text));
    verify(text).selectAll();

    Combo combo = mock(Combo.class);
    when(combo.getText()).thenReturn(null);
    assertTrue(TextSelectAll.selectAll(combo));
    verify(combo).setSelection(new Point(0, 0));

    CCombo ccombo = mock(CCombo.class);
    when(ccombo.getText()).thenReturn("name");
    assertTrue(TextSelectAll.selectAll(ccombo));
    verify(ccombo).setSelection(new Point(0, 4));

    StyledText styled = mock(StyledText.class);
    assertTrue(TextSelectAll.selectAll(styled));
    verify(styled).selectAll();
  }

  @Test
  void leavesOtherWidgetsAlone() {
    assertFalse(TextSelectAll.selectAll(null));
    assertFalse(TextSelectAll.selectAll(mock(Canvas.class)));
  }
}
