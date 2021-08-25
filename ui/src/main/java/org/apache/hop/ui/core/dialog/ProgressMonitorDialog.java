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
import org.eclipse.swt.widgets.*;

import java.lang.reflect.InvocationTargetException;

public class ProgressMonitorDialog {
  private static final Class<?> PKG = ProgressMonitorDialog.class; // For Translator

  protected Shell parent;
  protected Shell shell;
  private Display display;
  protected IProgressMonitor progressMonitor;

  private Label wlImage;
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
   * Gets progressMonitor
   *
   * @return value of progressMonitor
   */
  public IProgressMonitor getProgressMonitor() {
    return progressMonitor;
  }

  public void run(boolean cancelable, IRunnableWithProgress runnable)
      throws InvocationTargetException, InterruptedException {

    PropsUi props = PropsUi.getInstance();

    shell = new Shell(parent, SWT.RESIZE | (cancelable ? SWT.CLOSE : SWT.NONE));
    shell.setText(BaseMessages.getString(PKG, "ProgressMonitorDialog.Shell.Title"));
    props.setLook(shell);

    display = shell.getDisplay();

    FormLayout formLayout = new FormLayout();
    formLayout.marginTop = Const.FORM_MARGIN;
    formLayout.marginLeft = Const.FORM_MARGIN;
    formLayout.marginRight = Const.FORM_MARGIN;
    formLayout.marginBottom = Const.FORM_MARGIN;

    int margin = Const.MARGIN;

    shell.setLayout(formLayout);

    // An image at the top right...
    // TODO: rotate this image somehow
    //
    wlImage = new Label(shell, SWT.NONE);
    wlImage.setImage(GuiResource.getInstance().getImageHopUi());
    props.setLook(wlImage);
    FormData fdlImage = new FormData();
    fdlImage.right = new FormAttachment(100, 0);
    fdlImage.top = new FormAttachment(0, 0);
    wlImage.setLayoutData(fdlImage);

    wlTask = new Label(shell, SWT.LEFT);
    wlTask.setText(BaseMessages.getString(PKG, "ProgressMonitorDialog.InitialTaskLabel"));
    props.setLook(wlTask);
    FormData fdlTask = new FormData();
    fdlTask.left = new FormAttachment(0, 0);
    fdlTask.top = new FormAttachment(0, 0);
    fdlTask.right = new FormAttachment(wlImage, -margin);
    wlTask.setLayoutData(fdlTask);

    wlSubTask = new Label(shell, SWT.LEFT);
    wlSubTask.setText(BaseMessages.getString(PKG, "ProgressMonitorDialog.InitialSubTaskLabel"));
    props.setLook(wlSubTask);
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

    if (cancelable) {
      Button wCancel = new Button(shell, SWT.PUSH);
      wCancel.setText(BaseMessages.getString("System.Button.Cancel"));
      wCancel.addListener(
          SWT.Selection,
          e -> {
            isCancelled = true;
          });
      BaseTransformDialog.positionBottomButtons(
          shell, new Button[] {wCancel}, margin, wProgressBar);
    }

    BaseTransformDialog.setSize(shell);

    shell.addListener(
        SWT.Close,
        e -> {
          e.doit = false;
          isCancelled = true;
        });

    shell.open();

    Cursor oldCursor = shell.getCursor();

    parent.setCursor(display.getSystemCursor(SWT.CURSOR_WAIT));

    // Execute the long running task
    //
    Runnable longRunnable =
        () -> {
          // Always do the work in a different thread...
          // This keeps the shell updating properly as long as
          // display.asyncExec is used
          //
          new Thread(
                  () -> {
                    try {
                      runnable.run(progressMonitor);
                    } catch (InvocationTargetException e) {
                      targetException = e;
                    } catch (InterruptedException e) {
                      interruptedException = e;
                    }
                  })
              .start();
        };
    display.asyncExec(longRunnable);

    // Handle the event loop until we're done with this shell...
    //
    try {
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
    } finally {
      parent.setCursor(oldCursor);
    }
  }

  public Shell getShell() {
    return shell;
  }

  private void dispose() {
    display.asyncExec(
        () -> {
          PropsUi.getInstance().setScreen(new WindowProperty(shell));
          shell.dispose();
        });
  }

  private class ProgressMonitor implements IProgressMonitor {

    @Override
    public void beginTask(String message, int nrWorks) {
      display.asyncExec(
          () -> {
            synchronized (shell) {
              if (shell.isDisposed() || wlTask.isDisposed()) {
                return;
              }
              try {
                wlTask.setText(Const.NVL(message, ""));
                wProgressBar.setMaximum(nrWorks);
              } catch (Throwable e) {
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
              } catch (Throwable e) {
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
      display.asyncExec(
          () -> {
            synchronized (shell) {
              if (shell.isDisposed() || wlTask.isDisposed()) {
                return;
              }
              try {
                wProgressBar.setSelection(nrWorks);
              } catch (Throwable e) {
                // Ignore race condition
              }
            }
          });
    }

    @Override
    public void done() {
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
              } catch (Throwable e) {
                // Ignore race condition
              }
            }
          });
    }
  }
}
