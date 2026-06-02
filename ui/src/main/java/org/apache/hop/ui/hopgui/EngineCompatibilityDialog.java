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

package org.apache.hop.ui.hopgui;

import java.util.List;
import org.apache.hop.pipeline.engine.EngineCompatibilityChecker.Violation;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/**
 * Resizable, position-persisted dialog presenting engine-compatibility violations with a Run anyway
 * / Cancel choice. Replaces a plain SWT {@code MessageBox} so the list scrolls instead of
 * stretching the dialog when many transforms are incompatible, and so the user's preferred size and
 * screen location stick across invocations (via Hop's {@code WindowProperty} persistence).
 *
 * <p>Used by {@link EngineCompatibilityRunGate#confirmRunAnyway}. Returns {@code true} when the
 * user clicks "Run anyway", {@code false} for Cancel, Escape, or window-close.
 */
public final class EngineCompatibilityDialog {

  private static final int DEFAULT_WIDTH = 540;
  private static final int DEFAULT_HEIGHT = 320;

  private final Shell parent;
  private final String header;
  private final List<Violation> violations;

  private Shell shell;
  private boolean runAnyway;

  public EngineCompatibilityDialog(Shell parent, String header, List<Violation> violations) {
    this.parent = parent;
    this.header = header;
    this.violations = violations;
  }

  public boolean open() {
    shell = new Shell(parent, BaseDialog.getDefaultDialogStyle());
    shell.setText("Engine compatibility");
    PropsUi.setLook(shell);

    FormLayout layout = new FormLayout();
    layout.marginWidth = BaseDialog.MARGIN_SIZE;
    layout.marginHeight = BaseDialog.MARGIN_SIZE;
    shell.setLayout(layout);

    Label headerLabel = new Label(shell, SWT.WRAP);
    headerLabel.setText(header);
    PropsUi.setLook(headerLabel);
    FormData headerLayout = new FormData();
    headerLayout.left = new FormAttachment(0, 0);
    headerLayout.right = new FormAttachment(100, 0);
    headerLayout.top = new FormAttachment(0, 0);
    headerLabel.setLayoutData(headerLayout);

    org.eclipse.swt.widgets.List list =
        new org.eclipse.swt.widgets.List(shell, SWT.BORDER | SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(list);
    for (Violation v : violations) {
      list.add(formatRow(v));
    }

    Button cancelButton = new Button(shell, SWT.PUSH);
    cancelButton.setText("Cancel");
    cancelButton.addListener(
        SWT.Selection,
        e -> {
          runAnyway = false;
          shell.close();
        });

    Button runAnywayButton = new Button(shell, SWT.PUSH);
    runAnywayButton.setText("Run anyway");
    runAnywayButton.addListener(
        SWT.Selection,
        e -> {
          runAnyway = true;
          shell.close();
        });

    // Buttons pinned to the bottom-right; Run anyway is the rightmost action so the keyboard
    // default still favours Cancel (safer when the user hits Enter accidentally).
    FormData runAnywayLayout = new FormData();
    runAnywayLayout.right = new FormAttachment(100, 0);
    runAnywayLayout.bottom = new FormAttachment(100, 0);
    runAnywayButton.setLayoutData(runAnywayLayout);

    FormData cancelLayout = new FormData();
    cancelLayout.right = new FormAttachment(runAnywayButton, -BaseDialog.LABEL_SPACING);
    cancelLayout.bottom = new FormAttachment(100, 0);
    cancelButton.setLayoutData(cancelLayout);

    FormData listLayout = new FormData();
    listLayout.left = new FormAttachment(0, 0);
    listLayout.right = new FormAttachment(100, 0);
    listLayout.top = new FormAttachment(headerLabel, BaseDialog.ELEMENT_SPACING);
    listLayout.bottom = new FormAttachment(runAnywayButton, -BaseDialog.ELEMENT_SPACING);
    list.setLayoutData(listLayout);

    shell.setDefaultButton(cancelButton);
    shell.setSize(DEFAULT_WIDTH, DEFAULT_HEIGHT);
    shell.setMinimumSize(360, 200);

    // Persistence + escape-to-cancel; second-arg false keeps the dialog modest for short lists.
    // CRITICAL: these consumers run *during* the SWT.Close event chain that BaseDialog wires up.
    // Calling shell.close() inside them re-fires SWT.Close → infinite recursion → StackOverflow.
    // The button listeners above are the only callers of shell.close(); these consumers just
    // record state. cancelConsumer is a no-op because runAnyway already defaults to false.
    BaseDialog.defaultShellHandling(
        shell,
        ok -> {
          // No Text/Combo/List in this dialog fires DefaultSelection in a useful way; treat
          // Enter-on-list as Cancel (runAnyway stays false). Do NOT call shell.close() here —
          // we are already inside the close event chain.
        },
        cancel -> {
          // No-op (see comment above).
        },
        /* useStandardMinimumSize */ false);

    return runAnyway;
  }

  private static String formatRow(Violation v) {
    if (v == null) {
      return "(unknown)";
    }
    String name = v.elementName() == null ? "(unnamed)" : v.elementName();
    String suffix = v.pluginId() == null ? "" : " (" + v.pluginId() + ")";
    String reason = v.reason() == null || v.reason().isEmpty() ? "" : " — " + v.reason();
    return name + suffix + reason;
  }
}
