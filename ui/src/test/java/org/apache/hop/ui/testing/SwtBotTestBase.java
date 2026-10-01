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
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.local.LocalAuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.junit5.SWTBotJunit5Extension;
import org.eclipse.swtbot.swt.finder.utils.SWTUtils;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Shared SWTBot harness for {@code @Tag("uitest")} tests. The default reactor run includes those
 * tests whenever a display is available; they are skipped only when the JVM is headless. On a
 * desktop wrap Maven with {@code tools/with-isolated-display.sh} so the shells do not steal the
 * interactive session. {@code -Puitest} runs only UI tests; {@code -Pskip-uitest} excludes them.
 */
@ExtendWith(SWTBotJunit5Extension.class)
public abstract class SwtBotTestBase {

  /**
   * Optional hold (milliseconds) applied after the interactions of each scene/dialog so the window
   * stays on screen long enough to screenshot. Defaults to 0 so normal/CI runs are not slowed, e.g.
   * {@code -Dswtbot.test.holdMillis=5000}.
   */
  private static final String HOLD_MILLIS_PROPERTY = "swtbot.test.holdMillis";

  /**
   * Hard ceiling (milliseconds) on a single scene/dialog. A UI test that never finishes - a modal
   * box nobody dismissed, a worker that walked away from the event loop - would otherwise hold the
   * build until CI kills the job hours later. When it expires the harness tears the windows down
   * and fails the test with the thread stacks, e.g. {@code -Dswtbot.test.timeoutMillis=300000}.
   */
  private static final String TIMEOUT_MILLIS_PROPERTY = "swtbot.test.timeoutMillis";

  private static final long DEFAULT_TIMEOUT_MILLIS = 120_000L;

  /** How often the {@link Pump} wakes the display on its own. */
  private static final int PUMP_INTERVAL_MILLIS = 50;

  protected static Display display;

  @BeforeAll
  static void initHopUiEnvironment() throws Exception {
    Assumptions.assumeFalse(
        GraphicsEnvironment.isHeadless(),
        "No display available (headless); skipping SWTBot UI tests. Run on a desktop or under Xvfb.");
    // Registers the transform/plugin metadata (e.g. the Abort transform) the dialogs look up.
    // reset() first so we always get a full re-registration: a bare init() is a no-op when an
    // earlier test already flipped HopEnvironment.initialized, and a test that wiped the
    // PluginRegistry in the meantime would otherwise leave the dialogs without their plugins.
    HopEnvironment.reset();
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
      // Daemon: a worker wedged in a syncExec must never keep the surefire JVM alive.
      worker.setDaemon(true);
      worker.start();

      Pump pump = new Pump(worker);
      try {
        pump.until(done::get);
        // The worker sets `done` from its finally, so it is a hair away from exiting. Keep pumping
        // rather than joining outright: its last act may still need the UI thread.
        pump.until(() -> !worker.isAlive());
      } finally {
        pump.close();
      }
      rethrow(pump.timedOut() ? pump.timeoutFailure() : error.get());
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
      worker.setDaemon(true);
      worker.start();

      // The pump's deadline fires from whichever event loop is dispatching at the time - the
      // dialog's own blocking loop below, or a modal box nested inside it - so a window nobody
      // closed ends the test instead of parking the build.
      Pump pump = new Pump(worker);
      Throwable openError = null;
      try {
        try {
          // Runs the dialog's own event loop on the UI thread until the dialog closes.
          blockingOpener.accept(parent);
        } catch (Throwable t) {
          openError = t;
        }
        // Keep pumping so the worker's SWTBot calls resolve (or time out) and its cleanup runs.
        pump.until(() -> !worker.isAlive());
        drain();
      } finally {
        pump.close();
      }

      if (pump.timedOut()) {
        rethrow(pump.timeoutFailure());
      }
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

  /**
   * Keeps the event loop honest for the length of one scene or dialog, and ends the test if that
   * takes longer than {@link #TIMEOUT_MILLIS_PROPERTY}.
   *
   * <p>Two things run for as long as a pump is open. A heartbeat thread wakes the display every
   * {@value #PUMP_INTERVAL_MILLIS} ms, and a timer on the UI thread carries the deadline.
   *
   * <p>The heartbeat is what makes the rest of this reliable. Waking the loop from the worker's
   * {@link Display#wake()} alone is a lost-wakeup race: the wake can be consumed by a
   * readAndDispatch that runs between the loop's condition check and its {@link Display#sleep()},
   * and that sleep then parks with nobody left to wake it - the worker has already finished. It is
   * a microsecond-wide window locally and a real hang on a loaded CI runner, where the worker can
   * be descheduled between its last statement and the thread actually dying. The heartbeat also
   * gets the deadline timer dispatched on macOS, where a Cocoa run loop does not return for timers
   * (they are not input sources) and a sleeping display would otherwise never run it.
   */
  private final class Pump implements AutoCloseable {

    private final AtomicReference<String> stuckReport = new AtomicReference<>();
    private final Thread worker;
    private final Thread heartbeat;
    private final Runnable deadline;

    private Pump(Thread worker) {
      this.worker = worker;
      this.deadline = this::giveUp;
      this.heartbeat = new Thread(this::beat, "swtbot-heartbeat");
      heartbeat.setDaemon(true);
      display.timerExec((int) timeoutMillis(), deadline);
      heartbeat.start();
    }

    /**
     * Pumps the event loop on this (the UI) thread until {@code done} turns true, or we give up.
     */
    private void until(BooleanSupplier done) {
      while (!done.getAsBoolean() && stuckReport.get() == null) {
        if (!display.readAndDispatch()) {
          display.sleep();
        }
      }
    }

    private boolean timedOut() {
      return stuckReport.get() != null;
    }

    private AssertionError timeoutFailure() {
      return new AssertionError(
          "SWTBot UI test did not finish within "
              + timeoutMillis()
              + " ms; the harness closed the windows so the build could carry on. Where it was "
              + "stuck:"
              + System.lineSeparator()
              + stuckReport.get());
    }

    private void beat() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(PUMP_INTERVAL_MILLIS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        try {
          if (display.isDisposed()) {
            return;
          }
          display.wake();
        } catch (SWTException disposedMeanwhile) {
          return;
        }
      }
    }

    /**
     * Runs on the UI thread once the deadline passes: records what the threads were doing,
     * screenshots the display, then closes - and, where a shell ignores that, disposes - every
     * window so any event loop parked in one returns.
     */
    private void giveUp() {
      if (!stuckReport.compareAndSet(null, describeStuckThreads(worker))) {
        return;
      }
      captureScreenshot("timeout");
      for (Shell openShell : display.getShells()) {
        if (!openShell.isDisposed()) {
          openShell.close();
        }
        if (!openShell.isDisposed()) {
          // A close() the shell vetoed - or never saw, because its own loop is wedged - still has
          // to go, or the loop we are trying to unblock keeps running.
          openShell.dispose();
        }
      }
      if (worker.isAlive()) {
        worker.interrupt();
      }
    }

    @Override
    public void close() {
      heartbeat.interrupt();
      display.timerExec(-1, deadline);
    }
  }

  private static long timeoutMillis() {
    return Long.getLong(TIMEOUT_MILLIS_PROPERTY, DEFAULT_TIMEOUT_MILLIS);
  }

  /**
   * What the UI thread and the worker were doing when the deadline passed, plus the names of the
   * other live threads. A hung job leaves nothing else behind, so this travels in the failure
   * message rather than in output nobody keeps.
   */
  private static String describeStuckThreads(Thread worker) {
    StringBuilder report = new StringBuilder();
    appendStack(report, "UI thread", Thread.currentThread());
    appendStack(report, "worker", worker);
    report.append("other live threads:");
    for (Thread other : Thread.getAllStackTraces().keySet()) {
      if (other != Thread.currentThread() && other != worker) {
        report.append(' ').append(other.getName());
      }
    }
    return report.toString();
  }

  private static void appendStack(StringBuilder report, String label, Thread thread) {
    report
        .append(label)
        .append(" [")
        .append(thread.getName())
        .append("] ")
        .append(thread.getState())
        .append(':')
        .append(System.lineSeparator());
    for (StackTraceElement frame : thread.getStackTrace()) {
      // Drop the frames of the dump itself; the caller wants the frames underneath it.
      if (frame.getClassName().equals(SwtBotTestBase.class.getName())
          && (frame.getMethodName().equals("appendStack")
              || frame.getMethodName().equals("describeStuckThreads"))) {
        continue;
      }
      if (frame.getClassName().equals("java.lang.Thread")
          && frame.getMethodName().equals("getStackTrace")) {
        continue;
      }
      report.append("\tat ").append(frame).append(System.lineSeparator());
    }
  }

  private void drain() {
    while (display.readAndDispatch()) {
      // flush anything the worker posted right before exiting (e.g. closing the parent shell)
    }
  }

  private static final AtomicInteger SCREENSHOT_COUNTER = new AtomicInteger();

  /**
   * Captures the SWT display to {@code target/screenshots/<TestClass>.<method>-N.png} the moment a
   * test's worker thread sees an assertion failure or unexpected exception. We do this here, before
   * the harness's finally tears the dialog/shell down - by the time the SWTBot extension's
   * testFailed runs the UI is gone and its auto-screenshot would just be the empty Xvfb desktop.
   * Best effort: any failure while capturing is swallowed so the original test failure still
   * propagates with its full stack trace.
   */
  private static void captureLiveScreenshot(Throwable failure) {
    captureScreenshot(screenshotName(failure));
  }

  /** The test frame a failure came from, for use as the screenshot's file name. */
  private static String screenshotName(Throwable failure) {
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
    return name;
  }

  private static void captureScreenshot(String name) {
    // Under target/ so a screenshot never lands in the checked-out tree; CI collects the folder
    // from wherever it is (the workflow globs **/screenshots/**).
    String path =
        String.format("target/screenshots/%s-%d.png", name, SCREENSHOT_COUNTER.incrementAndGet());
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
