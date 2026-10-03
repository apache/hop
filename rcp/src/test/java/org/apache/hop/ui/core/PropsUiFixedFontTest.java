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

package org.apache.hop.ui.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.apache.hop.core.Props;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.StyledTextVar;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The shell theme walk used to replace the fixed-width font on script and SQL editors with the
 * default font. Editors are marked with {@link Props#WIDGET_STYLE_FIXED}; the walk has to keep that
 * font on the text inside the editor composite.
 */
@Tag("uitest")
class PropsUiFixedFontTest extends SwtBotTestBase {

  @Test
  @SuppressWarnings("deprecation") // dialogs still call setLook(editor, WIDGET_STYLE_FIXED)
  void themeWalkKeepsFixedFontOnScriptAndSqlEditors() {
    Shell shell = new Shell(display);
    try {
      Label transformName = new Label(shell, SWT.NONE);
      transformName.setText("Transform name");

      StyledTextVar script =
          new StyledTextVar(
              new Variables(), shell, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL, false, false, false);
      PropsUi.setLook(script, Props.WIDGET_STYLE_FIXED);

      Text sql = new Text(shell, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL);
      PropsUi.setLook(sql, Props.WIDGET_STYLE_FIXED);

      // Same order as a dialog: the editors are marked first, then the shell is themed.
      PropsUi.setTheme(shell);

      Font fixed = GuiResource.getInstance().getFontFixed();
      Font proportional = GuiResource.getInstance().getFontDefault();
      assertNotEquals(
          fontKey(proportional),
          fontKey(fixed),
          "the fixed and default fonts have to differ for this check");
      assertEquals(fontKey(fixed), fontKey(script.getTextWidget().getFont()));
      assertEquals(fontKey(fixed), fontKey(sql.getFont()));
      assertEquals(fontKey(proportional), fontKey(transformName.getFont()));

      // A script tab added after the dialog is open, as the JavaScript editor does.
      StyledTextVar added =
          new StyledTextVar(
              new Variables(), shell, SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL, false, false, false);
      PropsUi.setLook(added, Props.WIDGET_STYLE_FIXED);
      PropsUi.setTheme(shell);
      assertEquals(fontKey(fixed), fontKey(added.getTextWidget().getFont()));
      assertEquals(fontKey(fixed), fontKey(script.getTextWidget().getFont()));
      assertEquals(fontKey(proportional), fontKey(transformName.getFont()));
    } finally {
      shell.dispose();
    }
  }

  private static String fontKey(Font font) {
    FontData data = font.getFontData()[0];
    return data.getName() + "/" + data.getHeight() + "/" + data.getStyle();
  }
}
