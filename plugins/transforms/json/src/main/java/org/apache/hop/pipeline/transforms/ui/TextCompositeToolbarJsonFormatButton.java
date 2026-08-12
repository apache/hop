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
 *
 */

package org.apache.hop.pipeline.transforms.ui;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementFilter;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextComposite;
import org.eclipse.swt.widgets.Shell;

/**
 * Sample TextComposite toolbar contribution: pretty-print JSON when the editor's {@link
 * TextComposite#getStyleType()} is {@link TextComposite#STYLE_TYPE_JSON}.
 */
@GuiPlugin
public class TextCompositeToolbarJsonFormatButton {
  private static final Class<?> PKG = TextCompositeToolbarJsonFormatButton.class;

  private static final String ID_TOOLBAR_FORMAT_JSON = "textcomposite-toolbar-20010-format-json";

  /**
   * Only show this toolbar button when the content is JSON. Other toolbar item IDs always return
   * {@code true} so built-in buttons are not hidden.
   *
   * <p>Signature must be {@code (String, Object)} for {@link GuiToolbarElementFilter} reflection.
   *
   * <p><strong>Do not call {@link TextComposite#isEditable()} (or {@code getText()}, etc.)
   * here.</strong> Filters run while the {@link TextComposite} constructor is still building the
   * toolbar, before the underlying text widget exists. Check editability in the action method
   * instead.
   *
   * @param buttonId the toolbar button id being evaluated
   * @param guiPluginInstance the registered TextComposite instance (or other owner)
   * @return whether the button should be shown
   */
  @GuiToolbarElementFilter(parentId = TextComposite.ID_TOOLBAR)
  public static boolean isButtonShown(String buttonId, Object guiPluginInstance) {
    if (!ID_TOOLBAR_FORMAT_JSON.equals(buttonId)) {
      return true;
    }
    if (!(guiPluginInstance instanceof TextComposite textComposite)) {
      return false;
    }
    // styleType is set via the constructor before addToolbar(); isEditable() is not safe here.
    return TextComposite.STYLE_TYPE_JSON.equalsIgnoreCase(textComposite.getStyleType());
  }

  @GuiToolbarElement(
      root = TextComposite.ID_TOOLBAR,
      id = ID_TOOLBAR_FORMAT_JSON,
      toolTip = "i18n::TextCompositeToolbarJsonFormatButton.FormatJson.ToolTip",
      separator = true,
      image = "json-input.svg")
  public static void formatJson(TextComposite textComposite) {
    // Runtime checks (editability, disposed, …) belong here — not in the toolbar filter.
    if (textComposite == null || textComposite.isDisposed() || !textComposite.isEditable()) {
      return;
    }

    Shell shell = textComposite.getShell();
    String json = textComposite.getText();
    if (StringUtils.isBlank(json)) {
      return;
    }

    try {
      ObjectMapper mapper = HopJson.newMapper();
      JsonNode tree = mapper.readTree(json);
      String formatted = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(tree);

      // Jackson may leave a trailing newline; normalize so compare is stable
      if (formatted.endsWith("\n") && !json.endsWith("\n")) {
        formatted = formatted.substring(0, formatted.length() - 1);
      }

      if (formatted.equals(json)) {
        return;
      }

      // selectAll + insert so StyledTextVar undo stack records the previous value
      textComposite.selectAll();
      textComposite.insert(formatted);
      textComposite.setCaretPosition(0);
      textComposite.updateToolbar();
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "TextCompositeToolbarJsonFormatButton.Error.Title"),
          BaseMessages.getString(PKG, "TextCompositeToolbarJsonFormatButton.Error.Message"),
          e);
    }
  }
}
