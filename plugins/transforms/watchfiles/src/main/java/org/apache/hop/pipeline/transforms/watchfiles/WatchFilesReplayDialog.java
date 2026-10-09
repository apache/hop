/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import java.util.regex.Pattern;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
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

/** A one-time reprocessing selection; the main pipeline configuration is left unchanged. */
class WatchFilesReplayDialog extends Dialog {
  private final IVariables variables;
  private final WatchFilesMeta options;
  private boolean accepted;
  private Label validation;

  WatchFilesReplayDialog(Shell parent, IVariables variables, WatchFilesMeta options) {
    super(parent, BaseDialog.getDefaultDialogStyle());
    this.variables = variables;
    this.options = options;
  }

  boolean open() {
    Shell shell = new Shell(getParent(), getStyle());
    shell.setText(message("ReprocessTitle"));
    shell.setData(BaseDialog.DIALOG_SUBJECT, options);
    PropsUi.setLook(shell);
    FormLayout layout = new FormLayout();
    layout.marginWidth = 10;
    layout.marginHeight = 10;
    shell.setLayout(layout);
    Button ok = new Button(shell, SWT.PUSH);
    ok.setText(BaseMessages.getString(WatchFilesMeta.class, "System.Button.OK"));
    Button cancel = new Button(shell, SWT.PUSH);
    cancel.setText(BaseMessages.getString(WatchFilesMeta.class, "System.Button.Cancel"));
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {ok, cancel}, 10, null);
    GuiCompositeWidgets widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            null,
            ok,
            WatchFilesMeta.REPLAY_GUI_PARENT_ID,
            options,
            groups ->
                groups.registerExtraGroup(
                    BaseMessages.getString(WatchFilesMeta.class, "WatchFilesMeta.Group.Reprocess"),
                    "01",
                    null,
                    parent -> {
                      var existing = parent.getChildren();
                      Label help = new Label(parent, SWT.WRAP);
                      help.setText(message("ReprocessHelp"));
                      FormData position = new FormData();
                      position.left = new FormAttachment(0);
                      position.right = new FormAttachment(100);
                      position.top = new FormAttachment(existing[existing.length - 1], 10);
                      position.width = 570;
                      help.setLayoutData(position);
                      validation = new Label(parent, SWT.WRAP);
                      validation.setForeground(
                          parent.getDisplay().getSystemColor(SWT.COLOR_DARK_RED));
                      FormData errorPosition = new FormData();
                      errorPosition.left = new FormAttachment(0);
                      errorPosition.right = new FormAttachment(100);
                      errorPosition.top = new FormAttachment(help, 10);
                      errorPosition.width = 570;
                      errorPosition.height = 0;
                      validation.setLayoutData(errorPosition);
                      validation.setVisible(false);
                      errorPosition.top = new FormAttachment(existing[existing.length - 1], 10);
                      position.top = new FormAttachment(validation, 10);
                    }));
    Runnable accept =
        () -> {
          widgets.getWidgetsContents(options, WatchFilesMeta.REPLAY_GUI_PARENT_ID);
          try {
            Pattern.compile(variables.resolve(options.getReplayFilter()));
            accepted = true;
            shell.dispose();
          } catch (RuntimeException invalid) {
            validation.setText(message("InvalidReplayPattern") + invalid.getMessage());
            ((FormData) validation.getLayoutData()).height = SWT.DEFAULT;
            validation.setVisible(true);
            var content = validation.getParent();
            content.layout(true, true);
            if (content.getParent() instanceof org.eclipse.swt.custom.ScrolledComposite scrolled) {
              scrolled.setMinSize(content.computeSize(SWT.DEFAULT, SWT.DEFAULT));
            }
            widgets.getWidgetsMap().get("replayFilter").setFocus();
          }
        };
    ok.addListener(SWT.Selection, event -> accept.run());
    cancel.addListener(SWT.Selection, event -> shell.dispose());
    shell.setDefaultButton(ok);
    shell.addListener(
        SWT.Show,
        event -> {
          var screen = shell.getMonitor().getClientArea();
          var size = shell.getSize();
          shell.setSize(Math.min(size.x, screen.width), Math.min(size.y, screen.height));
          var point = shell.getLocation();
          size = shell.getSize();
          shell.setLocation(
              Math.max(screen.x, Math.min(point.x, screen.x + screen.width - size.x)),
              Math.max(screen.y, Math.min(point.y, screen.y + screen.height - size.y)));
        });
    BaseDialog.defaultShellHandling(shell, value -> accept.run(), value -> shell.dispose());
    return accepted;
  }

  private String message(String key) {
    return BaseMessages.getString(WatchFilesMeta.class, "WatchFilesDialog." + key);
  }
}
