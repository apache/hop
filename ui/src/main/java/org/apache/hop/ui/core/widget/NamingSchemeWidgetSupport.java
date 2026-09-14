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

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

/**
 * Naming-scheme support for a standalone SWT {@link Text} that must stay a sibling of other dialog
 * controls. Transform and action name fields are {@code protected Text} for binary compatibility;
 * wrapping them in {@link TextVar} breaks both the field descriptor and {@code
 * FormAttachment(wTransformName)} layout in existing plugins.
 */
public final class NamingSchemeWidgetSupport {

  private NamingSchemeWidgetSupport() {
    // utility
  }

  /**
   * Attach the naming-scheme shortcut to {@code text} without an N indicator. Use this for inline
   * tree editors (F2 rename) where a sibling label cannot be laid out.
   *
   * @param text existing name field
   * @param variables Hop variables (may be null)
   * @param namingSchemeType a {@link NamingSchemeTypes} code
   */
  public static void attachShortcut(Text text, IVariables variables, String namingSchemeType) {
    text.addKeyListener(
        new TextWidgetShortcutKeyAdapter(() -> context(text, variables, namingSchemeType)));
  }

  /**
   * Attach CTRL-SHIFT-N (and the N indicator click) to {@code text}. The returned label is a
   * sibling of {@code text}; use {@link #layoutWithIndicator(Text, Label, FormData)} to place it.
   *
   * @param text existing name field (direct child of the dialog composite)
   * @param variables Hop variables (may be null)
   * @param namingSchemeType a {@link NamingSchemeTypes} code
   * @return the N indicator label
   */
  public static Label enableOnText(Text text, IVariables variables, String namingSchemeType) {
    attachShortcut(text, variables, namingSchemeType);

    Label indicator = new Label(text.getParent(), SWT.NONE);
    PropsUi.setLook(indicator);
    indicator.setImage(GuiResource.getInstance().getImageNamingMini());
    indicator.setToolTipText(BaseMessages.getString(TextVar.class, "TextVar.tooltip.NamingScheme"));
    TextWidgetShortcutKeyAdapter.attachIndicatorClick(
        indicator, () -> context(text, variables, namingSchemeType));
    return indicator;
  }

  /**
   * Place {@code indicator} at the original right edge of {@code fdText} and attach {@code text} to
   * the left of it. {@code text} remains a sibling of other dialog controls.
   *
   * @param text the name field
   * @param indicator the N label from {@link #enableOnText(Text, IVariables, String)}
   * @param fdText form data for the name slot (left/top/right of the whole field)
   */
  public static void layoutWithIndicator(Text text, Label indicator, FormData fdText) {
    FormData fdIndicator = new FormData();
    fdIndicator.top = new FormAttachment(text, 0, SWT.CENTER);
    fdIndicator.right = fdText.right;
    indicator.setLayoutData(fdIndicator);
    fdText.right = new FormAttachment(indicator, 0);
    text.setLayoutData(fdText);
  }

  private static TextWidgetShortcutContext context(
      Text text, IVariables variables, String namingSchemeType) {
    return TextWidgetShortcutContext.builder()
        .control(text)
        .variables(variables)
        .getText(text::getText)
        .setText(text::setText)
        .namingSchemeType(namingSchemeType)
        .variablesEnabled(false)
        .build();
  }
}
