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

package org.apache.hop.ui.testing;

import java.awt.GraphicsEnvironment;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.local.LocalAuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.junit5.SWTBotJunit5Extension;
import org.eclipse.swtbot.swt.finder.utils.SWTUtils;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SWTBotJunit5Extension.class)
public abstract class SwtBotTestBase {

  /**
   * Optional hold (milliseconds) applied after the interactions of each scene/dialog so the window
   * stays on screen long enough to screenshot. Defaults to 0 so normal/CI runs are not slowed, e.g.
   * {@code -Dswtbot.test.holdMillis=5000}.
   */
  private static final String HOLD_MILLIS_PROPERTY = "swtbot.test.holdMillis";

  protected static Display display;

  @BeforeAll
  static void initHopUiEnvironment() throws Exception {
    Assumptions.assumeFalse(
        GraphicsEnvironment.isHeadless(),
        "No display available (headless); skipping SWTBot UI tests. Run on a desktop or under Xvfb.");
    // Registers the transform/plugin metadata (e.g. the Abort transform) the dialogs look up.
    HopEnvironment.init();
    keepAuditStateOutOfSourceTree();
    ensureDisplay();
    // Warm up the Hop look-and-feel (fonts, zoom factor) against this display.
    PropsUi.getInstance();
    GuiResource.getInstance();
    primeEventLoop();
  }

  /**
   * Redirect the local audit manager - which persists window geometry to {@code
   * <root>/hop-gui/shells-state.json} whenever a dialog closes - to a throwaway temp folder, so the
   * UI tests never write shell state into the checked-out source tree. This is enforced by the
   * harness itself rather than relying on the build's {@code HOP_AUDIT_FOLDER}, so it also holds
   * for IDE runs that don't apply the Maven argLine.
   */
  private static void keepAuditStateOutOfSourceTree() throws IOException {
    Path auditFolder = Files.createTempDirectory("hop-swtbot-audit");
    auditFolder.toFile().deleteOnExit();
    AuditManager.getInstance().setActiveAuditManager(new LocalAuditManager(auditFolder.toString()));
  }

  protected static synchronized void ensureDisplay() {
    if (display == null || display.isDisposed()) {
      display = Display.getDefault();
    }
  }

  /** Opens and briefly pumps a throwaway shell so the platform event loop is live and warm. */
  private static void primeEventLoop() {
    Shell shell = new Shell(display, SWT.NO_TRIM);
    try {
      shell.setSize(1, 1);
      shell.open();
      long deadline = System.currentTimeMillis() + 300;
      while (System.currentTimeMillis() < deadline) {
        if (!display.readAndDispatch()) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }
    } finally {
      shell.dispose();
      while (display.readAndDispatch()) {
        // flush the dispose
      }
    }
  }

  /**
   * Builds a transient shell, lets {@code build} populate it on the UI thread, opens it, then runs
   * {@code interactions} on a worker thread while this (UI) thread pumps the SWT event loop until
   * the worker finishes. Use this for widgets that do not run their own event loop.
   */
  protected void withScene(Consumer<Shell> build, Consumer<SWTBot> interactions) {
    ensureDisplay();
    Shell shell = new Shell(display, SWT.SHELL_TRIM);
    AtomicReference<Throwable> error = new AtomicReference<>();
    AtomicBoolean done = new AtomicBoolean(false);
    try {
      shell.setText("Hop SWTBot test");
      build.accept(shell);
      if (shell.getSize().x == 0 || shell.getSize().y == 0) {
        shell.setSize(520, 260);
      }
      shell.open();

      Thread worker =
          new Thread(
              () -> {
                try {
                  interactions.accept(new SWTBot(shell));
                  hold();
                } catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
                  error.set(ie);
                } catch (Throwable t) {
                  // Take the failure screenshot HERE, while the UI is still on screen. By the time
                  // SWTBotJunit5Extension.testFailed fires, the finally below has already torn the
                  // dialog/shell down and the auto-screenshot would just show an empty display.
                  captureLiveScreenshot(t);
                  error.set(t);
                } finally {
                  done.set(true);
                  display.wake();
                }
              },
              "swtbot-worker");
      worker.start();

      pumpUntil(done);
      join(worker);
      rethrow(error.get());
    } finally {
      if (!shell.isDisposed()) {
        shell.dispose();
      }
      drain();
    }
  }

  /**
   * Drives a dialog that runs its own (blocking) event loop, such as a Hop transform dialog whose
   * {@code open()} pumps until the dialog is disposed.
   *
   * <p>{@code blockingOpener} receives a parent shell and is expected to construct and open the
   * dialog (the call blocks on the UI thread). {@code interactions} run on a worker thread: they
   * locate the dialog with SWTBot, exercise it, and must close it (e.g. click OK/Cancel) so the
   * opener returns. Should the interactions fail first, every open shell is closed so the opener
   * still returns and the failure is reported.
   */
  protected void withDialog(Consumer<Shell> blockingOpener, Consumer<SWTBot> interactions) {
    ensureDisplay();
    Shell parent = new Shell(display, SWT.SHELL_TRIM);
    AtomicReference<Throwable> error = new AtomicReference<>();
    try {
      Thread worker =
          new Thread(
              () -> {
                try {
                  interactions.accept(new SWTBot());
                  hold();
                } catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
                  error.set(ie);
                } catch (Throwable t) {
                  // Take the failure screenshot HERE, while the UI is still on screen. By the time
                  // SWTBotJunit5Extension.testFailed fires, the finally below has already torn the
                  // dialog/shell down and the auto-screenshot would just show an empty display.
                  captureLiveScreenshot(t);
                  error.set(t);
                } finally {
                  // Guarantee the blocking opener returns even if interactions failed early.
                  display.asyncExec(
                      () -> {
                        for (Shell openShell : display.getShells()) {
                          if (!openShell.isDisposed()) {
                            openShell.close();
                          }
                        }
                      });
                  display.wake();
                }
              },
              "swtbot-worker");
      worker.start();

      Throwable openError = null;
      try {
        // Runs the dialog's own event loop on the UI thread until the dialog closes.
        blockingOpener.accept(parent);
      } catch (Throwable t) {
        openError = t;
      }
      // Keep pumping so the worker's SWTBot calls resolve (or time out) and its cleanup runs.
      pumpUntilThreadDone(worker);

      rethrow(error.get() != null ? error.get() : openError);
    } finally {
      if (!parent.isDisposed()) {
        parent.dispose();
      }
      drain();
    }
  }

  /** Resolves a {@code System.Button.*} label the way the dialogs do, minus the SWT mnemonic. */
  protected static String buttonLabel(String key) {
    // SWTBot's mnemonic matcher strips '&' from the widget text but does not trim, so we mirror
    // exactly what the button shows (leading/trailing spaces kept, '&' removed).
    return BaseMessages.getString(ITransform.class, key).replace("&", "");
  }

  private static void hold() throws InterruptedException {
    long holdMillis = Long.getLong(HOLD_MILLIS_PROPERTY, 0L);
    if (holdMillis > 0) {
      Thread.sleep(holdMillis);
    }
  }

  private void pumpUntil(AtomicBoolean done) {
    while (!done.get()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
  }

  private void pumpUntilThreadDone(Thread worker) {
    while (worker.isAlive()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    drain();
  }

  private void drain() {
    while (display.readAndDispatch()) {
      // flush anything the worker posted right before exiting (e.g. closing the parent shell)
    }
  }

  private static void join(Thread worker) {
    try {
      worker.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static final AtomicInteger SCREENSHOT_COUNTER = new AtomicInteger();

  /**
   * Captures the SWT display to {@code screenshots/<TestClass>.<method>-N.png} the moment a UI
   * test's worker thread sees an assertion failure or unexpected exception. We do this here, before
   * the harness's finally tears the dialog/shell down - by the time the SWTBot extension's
   * testFailed runs the UI is gone and its auto-screenshot would just be the empty Xvfb desktop.
   * Best effort: any failure while capturing is swallowed so the original test failure still
   * propagates with its full stack trace.
   */
  private static void captureLiveScreenshot(Throwable failure) {
    String name = "harness-failure";
    for (StackTraceElement frame : failure.getStackTrace()) {
      String cn = frame.getClassName();
      // Skip the harness, JUnit/opentest4j, and JDK frames; the first frame left is the test code
      // (likely a synthetic lambda$<testMethod>$N, which is still a useful filename).
      if (!cn.startsWith("org.apache.hop.ui.testing.")
          && !cn.startsWith("org.junit.")
          && !cn.startsWith("org.opentest4j.")
          && !cn.startsWith("java.")
          && !cn.startsWith("jdk.")) {
        name = cn.substring(cn.lastIndexOf('.') + 1) + "." + frame.getMethodName();
        break;
      }
    }
    String path =
        String.format("screenshots/%s-%d.png", name, SCREENSHOT_COUNTER.incrementAndGet());
    try {
      SWTUtils.captureScreenshot(path);
    } catch (Throwable ignored) {
      // best effort - the original failure must propagate
    }
  }

  private static void rethrow(Throwable t) {
    if (t == null) {
      return;
    }
    if (t instanceof RuntimeException re) {
      throw re;
    }
    if (t instanceof Error err) {
      throw err;
    }
    throw new RuntimeException(t);
  }
}
