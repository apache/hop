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

package org.apache.hop.ui.core.dialog;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.core.Const;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.IRunnableWithProgress;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.ProgressBar;
import org.eclipse.swt.widgets.Shell;

/** A modal dialog that displays progress during a long running operation. */
public class ProgressMonitorDialog {
  private static final Class<?> PKG = ProgressMonitorDialog.class;

  protected Shell parent;
  protected Shell shell;
  private Display display;
  protected IProgressMonitor progressMonitor;

  private Label wlTask;
  private Label wlSubTask;
  private ProgressBar wProgressBar;
  private boolean isCancelled;
  private InvocationTargetException targetException;
  private InterruptedException interruptedException;

  public ProgressMonitorDialog(Shell parent) {
    this.parent = parent;
    this.progressMonitor = new ProgressMonitor();
  }

  /**
   * Returns the progress monitor to use for operations run in this progress dialog.
   *
   * @return the progress monitor
   */
  public IProgressMonitor getProgressMonitor() {
    return progressMonitor;
  }

  public void run(boolean cancelable, IRunnableWithProgress runnable)
      throws InvocationTargetException, InterruptedException {
    createModalShell(cancelable);
    int margin = layoutProgressWidgets();
    addCancelButtonIfNeeded(cancelable, margin);
    BaseTransformDialog.setSize(shell);
    addShellCloseHandler();
    shell.open();

    Cursor oldCursor = shell.getCursor();
    parent.setCursor(display.getSystemCursor(SWT.CURSOR_WAIT));
    try {
      scheduleBackgroundWork(runnable);
      pumpDisplayUntilShellDisposed();
    } finally {
      parent.setCursor(oldCursor);
    }
  }

  private void createModalShell(boolean cancelable) {
    shell =
        new Shell(parent, SWT.RESIZE | SWT.APPLICATION_MODAL | (cancelable ? SWT.CLOSE : SWT.NONE));
    shell.setText(BaseMessages.getString(PKG, "ProgressMonitorDialog.Shell.Title"));
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    PropsUi.setLook(shell);
    display = shell.getDisplay();

    FormLayout formLayout = new FormLayout();
    formLayout.marginTop = PropsUi.getFormMargin();
    formLayout.marginLeft = PropsUi.getFormMargin();
    formLayout.marginRight = PropsUi.getFormMargin();
    formLayout.marginBottom = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
  }

  private int layoutProgressWidgets() {
    int margin = PropsUi.getMargin();

    Label wlImage = new Label(shell, SWT.NONE);
    wlImage.setImage(GuiResource.getInstance().getImageHopUi());
    PropsUi.setLook(wlImage);
    FormData fdlImage = new FormData();
    fdlImage.right = new FormAttachment(100, 0);
    fdlImage.top = new FormAttachment(0, 0);
    wlImage.setLayoutData(fdlImage);

    wlTask = new Label(shell, SWT.LEFT);
    wlTask.setText(BaseMessages.getString(PKG, "ProgressMonitorDialog.InitialTaskLabel"));
    PropsUi.setLook(wlTask);
    FormData fdlTask = new FormData();
    fdlTask.left = new FormAttachment(0, 0);
    fdlTask.top = new FormAttachment(0, 0);
    fdlTask.right = new FormAttachment(wlImage, -margin);
    wlTask.setLayoutData(fdlTask);

    wlSubTask = new Label(shell, SWT.LEFT);
    wlSubTask.setText(BaseMessages.getString(PKG, "ProgressMonitorDialog.InitialSubTaskLabel"));
    PropsUi.setLook(wlSubTask);
    FormData fdlSubTask = new FormData();
    fdlSubTask.left = new FormAttachment(0, 0);
    fdlSubTask.top = new FormAttachment(wlTask, margin);
    fdlSubTask.right = new FormAttachment(wlImage, -margin);
    wlSubTask.setLayoutData(fdlSubTask);

    wProgressBar = new ProgressBar(shell, SWT.HORIZONTAL);
    wProgressBar.setMinimum(0);
    wProgressBar.setMaximum(100);
    wProgressBar.setSelection(0);
    FormData fdProgressBar = new FormData();
    fdProgressBar.left = new FormAttachment(0, 0);
    fdProgressBar.right = new FormAttachment(wlImage, -margin);
    fdProgressBar.top = new FormAttachment(wlSubTask, margin);
    wProgressBar.setLayoutData(fdProgressBar);

    return margin;
  }

  private void addCancelButtonIfNeeded(boolean cancelable, int margin) {
    if (!cancelable) {
      return;
    }
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString("System.Button.Cancel"));
    wCancel.addListener(
        SWT.Selection,
        e -> {
          isCancelled = true;
          if (display != null && !display.isDisposed()) {
            display.wake();
          }
        });
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wCancel}, margin, wProgressBar);
  }

  private void addShellCloseHandler() {
    shell.addListener(
        SWT.Close,
        e -> {
          e.doit = false;
          isCancelled = true;
          if (display != null && !display.isDisposed()) {
            display.wake();
          }
        });
  }

  private void scheduleBackgroundWork(IRunnableWithProgress runnable) {
    Runnable launch =
        () -> new Thread(() -> runMonitoredWork(runnable), "Hop-ProgressMonitor-Worker").start();
    display.asyncExec(launch);
  }

  private void runMonitoredWork(IRunnableWithProgress runnable) {
    try {
      runnable.run(progressMonitor);
    } catch (InvocationTargetException e) {
      targetException = e;
    } catch (InterruptedException e) {
      interruptedException = e;
      Thread.currentThread().interrupt();
    }
  }

  private void pumpDisplayUntilShellDisposed()
      throws InvocationTargetException, InterruptedException {
    while (!shell.isDisposed()) {
      if (interruptedException != null) {
        dispose();
        throw interruptedException;
      }
      if (targetException != null) {
        dispose();
        throw targetException;
      }
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
  }

  public Shell getShell() {
    return shell;
  }

  private void dispose() {
    if (display == null || display.isDisposed()) {
      return;
    }

    Runnable disposeRunnable =
        () -> {
          if (shell == null || shell.isDisposed()) {
            return;
          }
          try {
            PropsUi.getInstance().setScreen(new WindowProperty(shell));
          } catch (Exception e) {
            // Ignore race condition when shell is already being torn down.
          }
          if (!shell.isDisposed()) {
            shell.dispose();
          }
        };

    if (Display.getCurrent() == display) {
      disposeRunnable.run();
    } else {
      display.syncExec(disposeRunnable);
    }
  }

  private class ProgressMonitor implements IProgressMonitor {

    /** IProgressMonitor.worked() reports deltas; we accumulate for the SWT progress bar. */
    private int workedAccumulated;

    private final AtomicBoolean workedFlushScheduled = new AtomicBoolean(false);

    private final Runnable workedFlushRunnable =
        () -> {
          workedFlushScheduled.set(false);
          synchronized (shell) {
            if (shell.isDisposed() || wlTask.isDisposed()) {
              return;
            }
            try {
              int max = wProgressBar.getMaximum();
              int selection = Math.min(workedAccumulated, max > 0 ? max : workedAccumulated);
              wProgressBar.setSelection(selection);
            } catch (Exception e) {
              // Ignore race condition
            }
          }
        };

    @Override
    public void beginTask(String message, int nrWorks) {
      workedAccumulated = 0;
      display.asyncExec(
          () -> {
            synchronized (shell) {
              if (shell.isDisposed() || wlTask.isDisposed()) {
                return;
              }
              try {
                wlTask.setText(Const.NVL(message, ""));
                wProgressBar.setMaximum(nrWorks);
                wProgressBar.setSelection(0);
              } catch (Exception e) {
                // Ignore race condition
              }
            }
          });
    }

    @Override
    public void subTask(String message) {
      display.asyncExec(
          () -> {
            synchronized (shell) {
              if (shell.isDisposed() || wlSubTask.isDisposed()) {
                return;
              }
              try {
                wlSubTask.setText(Const.NVL(message, ""));
              } catch (Exception e) {
                // Ignore race condition
              }
            }
          });
    }

    @Override
    public boolean isCanceled() {
      return isCancelled;
    }

    @Override
    public void worked(int nrWorks) {
      if (nrWorks <= 0) {
        return;
      }
      workedAccumulated += nrWorks;
      if (workedFlushScheduled.compareAndSet(false, true)) {
        display.asyncExec(workedFlushRunnable);
      }
    }

    @Override
    public void done() {
      workedAccumulated = 0;
      workedFlushScheduled.set(false);
      dispose();
    }

    @Override
    public void setTaskName(String taskName) {
      display.asyncExec(
          () -> {
            synchronized (shell) {
              if (shell.isDisposed() || wlTask.isDisposed()) {
                return;
              }
              try {
                wlTask.setText(Const.NVL(taskName, ""));
              } catch (Exception e) {
                // Ignore race condition
              }
            }
          });
    }
  }
}
