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

package org.apache.hop.ui.hopgui.markdown;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.gui.markdown.MarkdownEditing;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** Asks for the size and alignment of a Markdown table, then returns the pipe-table source. */
public class MarkdownTableDialog extends Dialog {

  private static final Class<?> PKG = MarkdownTableDialogModel.class;

  private final MarkdownTableDialogModel model = new MarkdownTableDialogModel();

  private Shell shell;
  private GuiCompositeWidgets widgets;
  private String markdown;

  public MarkdownTableDialog(Shell parent) {
    super(parent, SWT.NONE);
  }

  /**
   * @return the table source, or {@code null} when cancelled
   */
  public static String open(Shell parent) {
    return new MarkdownTableDialog(parent).openDialog();
  }

  private String openDialog() {
    Shell parent = getParent();
    shell = new Shell(parent, BaseDialog.getDefaultDialogStyle());
    PropsUi.setLook(shell);
    shell.setText(BaseMessages.getString(PKG, "MarkdownTableDialog.Shell.Title"));
    if (parent.getImage() != null) {
      shell.setImage(parent.getImage());
    }

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    int margin = PropsUi.getMargin();

    Label header = new Label(shell, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(header);
    header.setText(BaseMessages.getString(PKG, "MarkdownTableDialog.Header"));
    FormData headerData = new FormData();
    headerData.left = new FormAttachment(0, 0);
    headerData.top = new FormAttachment(0, 0);
    headerData.right = new FormAttachment(100, 0);
    header.setLayoutData(headerData);

    Button ok = new Button(shell, SWT.PUSH);
    ok.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    ok.addListener(SWT.Selection, e -> accept());

    Button cancel = new Button(shell, SWT.PUSH);
    cancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    cancel.addListener(SWT.Selection, e -> shell.dispose());

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {ok, cancel}, margin, null);

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            new Variables(),
            header,
            ok,
            MarkdownTableDialogModel.GUI_PLUGIN_ELEMENT_PARENT_ID,
            model);

    BaseDialog.defaultShellHandling(shell, c -> accept(), c -> shell.dispose(), false);
    return markdown;
  }

  private void accept() {
    if (widgets == null || shell == null || shell.isDisposed()) {
      return;
    }
    widgets.getWidgetsContents(model, MarkdownTableDialogModel.GUI_PLUGIN_ELEMENT_PARENT_ID);
    Integer columns = wholeNumber(model.getColumns());
    Integer rows = wholeNumber(model.getRows());
    if (columns == null || rows == null || columns < 1 || rows < 2) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      box.setText(BaseMessages.getString(PKG, "MarkdownTableDialog.Error.Title"));
      box.setMessage(BaseMessages.getString(PKG, "MarkdownTableDialog.Error.Message"));
      box.open();
      return;
    }
    markdown = MarkdownEditing.table(columns, rows, model.getAlignment());
    shell.dispose();
  }

  private static Integer wholeNumber(String text) {
    if (StringUtils.isBlank(text)) {
      return null;
    }
    try {
      return Integer.valueOf(text.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
