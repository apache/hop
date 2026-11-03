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

package org.apache.hop.ui.hopgui.file.pipeline.debug;

import org.apache.hop.core.Condition;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.debug.DebugPauseScope;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.widget.ConditionEditor;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Shell;

/** Edits the condition that pauses when a row enters a hop, and what that match stops. */
public class DebugHopPauseDialog {

  private static final Class<?> PKG = DebugHopPauseDialog.class;

  private final Shell parent;
  private final IRowMeta fields;
  private Condition condition;
  private DebugPauseScope scope;
  private ConditionEditor editor;

  public DebugHopPauseDialog(
      Shell parent, IRowMeta fields, Condition condition, DebugPauseScope scope) {
    this.parent = parent;
    this.fields = fields;
    this.condition = condition == null ? new Condition() : new Condition(condition);
    this.scope = scope == null ? DebugPauseScope.TRANSFORM : scope;
  }

  public boolean open() {
    Shell shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    shell.setMinimumSize(520, 360);
    PropsUi.setLook(shell);
    shell.setText(BaseMessages.getString(PKG, "DebugHopPauseDialog.Title"));
    FormLayout layout = new FormLayout();
    int margin = PropsUi.getMargin();
    layout.marginWidth = margin;
    layout.marginHeight = margin;
    shell.setLayout(layout);

    Button ok = new Button(shell, SWT.PUSH);
    ok.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    Button cancel = new Button(shell, SWT.PUSH);
    cancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {ok, cancel}, margin, null);
    shell.setDefaultButton(ok);

    DebugHopPauseModel model = new DebugHopPauseModel();
    model.setScope(scope);
    GuiCompositeWidgets widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            new Variables(),
            null,
            ok,
            DebugHopPauseModel.PARENT_ID,
            model,
            compositeWidgets ->
                compositeWidgets.registerExtraGroup(
                    BaseMessages.getString(PKG, "DebugHopPauseDialog.Group.Condition"),
                    "20",
                    null,
                    host -> {
                      editor = new ConditionEditor(host, SWT.BORDER, condition, fields);
                      PropsUi.setLook(editor);
                      FormData editorData = new FormData();
                      editorData.left = new FormAttachment(0, 0);
                      editorData.right = new FormAttachment(100, 0);
                      editorData.top = new FormAttachment(0, 0);
                      editorData.bottom = new FormAttachment(100, 0);
                      editor.setLayoutData(editorData);
                    }));

    ok.addListener(SWT.Selection, e -> accept(shell, widgets, model));
    cancel.addListener(SWT.Selection, e -> cancel(shell));
    BaseDialog.defaultShellHandling(shell, c -> accept(shell, widgets, model), c -> cancel(shell));
    return condition != null;
  }

  public Condition getCondition() {
    return condition;
  }

  public DebugPauseScope getScope() {
    return scope;
  }

  private void accept(Shell shell, GuiCompositeWidgets widgets, DebugHopPauseModel model) {
    if (editor != null && editor.getLevel() > 0) {
      editor.goUp();
      return;
    }
    widgets.getWidgetsContents(model, DebugHopPauseModel.PARENT_ID);
    scope = model.getScope() == null ? DebugPauseScope.TRANSFORM : model.getScope();
    shell.dispose();
  }

  private void cancel(Shell shell) {
    condition = null;
    shell.dispose();
  }
}
