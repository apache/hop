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
package org.apache.hop.lint;

import org.apache.hop.i18n.BaseMessages;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.swt.widgets.Shell;

/** Progress dialog for the linter operation */
public class LinterProgressDialog implements ProgressCallback {

  private static final Class<?> PKG = LinterProgressDialog.class; // for i18n purposes

  private Shell shell;
  private Shell dialog;
  private ProgressBar progressBar;
  private Label messageLabel;
  private Button cancelButton;
  private boolean cancelled = false;
  private boolean complete = false;

  public LinterProgressDialog(Shell parent) {
    this.shell = parent;
    createDialog();
  }

  private void createDialog() {
    dialog = new Shell(shell, SWT.DIALOG_TRIM | SWT.RESIZE);
    dialog.setText(BaseMessages.getString(PKG, "LinterProgressDialog.Title"));
    dialog.setSize(400, 120);

    // Center the dialog
    dialog.setLocation(
        shell.getLocation().x + (shell.getSize().x - 400) / 2,
        shell.getLocation().y + (shell.getSize().y - 120) / 2);

    GridLayout layout = new GridLayout(1, false);
    layout.marginWidth = 10;
    layout.marginHeight = 10;
    dialog.setLayout(layout);

    // Message label
    messageLabel = new Label(dialog, SWT.NONE);
    messageLabel.setText(BaseMessages.getString(PKG, "LinterProgressDialog.Message.Initializing"));
    GridData messageData = new GridData(SWT.FILL, SWT.CENTER, true, false);
    messageLabel.setLayoutData(messageData);

    // Progress bar
    progressBar = new ProgressBar(dialog, SWT.HORIZONTAL);
    progressBar.setMinimum(0);
    progressBar.setMaximum(100);
    GridData progressData = new GridData(SWT.FILL, SWT.CENTER, true, false);
    progressData.heightHint = 20;
    progressBar.setLayoutData(progressData);

    // Cancel button
    cancelButton = new Button(dialog, SWT.PUSH);
    cancelButton.setText(BaseMessages.getString(PKG, "LinterProgressDialog.Button.Cancel"));
    GridData cancelData = new GridData(SWT.CENTER, SWT.CENTER, false, false);
    cancelData.widthHint = 80;
    cancelButton.setLayoutData(cancelData);

    cancelButton.addListener(
        SWT.Selection,
        e -> {
          cancelled = true;
          cancelButton.setText(
              BaseMessages.getString(PKG, "LinterProgressDialog.Button.Cancelling"));
          cancelButton.setEnabled(false);
        });

    // Handle dialog close
    dialog.addListener(
        SWT.Close,
        e -> {
          if (!complete) {
            cancelled = true;
          }
        });
  }

  /** Show without blocking the UI thread (safe to call from a background lint thread). */
  public void show() {
    if (!dialog.isDisposed()) {
      Display display = dialog.getDisplay();
      Runnable show =
          () -> {
            if (!dialog.isDisposed() && !dialog.isVisible()) {
              dialog.setVisible(true);
            }
          };
      if (display != null && display.getThread() != Thread.currentThread()) {
        display.asyncExec(show);
      } else {
        show.run();
      }
    }
  }

  /**
   * @deprecated Prefer {@link #show()}; kept for project lint progress.
   */
  public void open() {
    show();
  }

  public void close() {
    if (!dialog.isDisposed()) {
      Runnable hide =
          () -> {
            if (!dialog.isDisposed()) {
              dialog.setVisible(false);
            }
          };
      Display display = dialog.getDisplay();
      if (display != null && display.getThread() != Thread.currentThread()) {
        display.asyncExec(hide);
      } else {
        hide.run();
      }
    }
  }

  @Override
  public void updateProgress(String message, int completed, int total) {
    if (!dialog.isDisposed()) {
      Display.getDefault()
          .asyncExec(
              () -> {
                if (!dialog.isDisposed()) {
                  messageLabel.setText(message);
                  if (total > 0) {
                    int percentage = (completed * 100) / total;
                    progressBar.setSelection(percentage);
                  }
                }
              });
    }
  }

  @Override
  public boolean isCancelled() {
    return cancelled;
  }

  @Override
  public void setComplete(String message) {
    complete = true;
    if (!dialog.isDisposed()) {
      Display.getDefault()
          .asyncExec(
              () -> {
                if (!dialog.isDisposed()) {
                  messageLabel.setText(message);
                  progressBar.setSelection(100);
                  cancelButton.setEnabled(false);
                }
              });
    }
  }

  public boolean isComplete() {
    return complete;
  }
}
