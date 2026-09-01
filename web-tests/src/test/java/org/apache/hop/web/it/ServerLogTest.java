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

package org.apache.hop.web.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ServerLogTest {

  /** Verbatim from Hop Web opening the Add a checksum dialog. */
  private static final String CHECKSUM_CRASH =
      """
      JSON metadata in folder /usr/local/tomcat/webapps/ROOT/config/projects/default/metadata
      Exception in thread "Thread-9" java.lang.IllegalStateException: Invalid thread access
      \tat org.eclipse.rap.rwt.RWT.checkContext(RWT.java:765)
      \tat org.eclipse.rap.rwt.RWT.getUISession(RWT.java:647)
      \tat org.eclipse.rap.rwt.SingletonUtil.getSessionInstance(SingletonUtil.java:58)
      \tat org.apache.hop.ui.core.PropsUiImpl.getInstanceInternal(PropsUiImpl.java:26)
      \tat org.apache.hop.ui.core.PropsUi.getInstance(PropsUi.java:157)
      \tat org.apache.hop.ui.core.ConstUi.sortFieldNames(ConstUi.java:254)
      \tat org.apache.hop.pipeline.transforms.checksum.CheckSumDialog.setComboBoxes(CheckSumDialog.java:291)
      \tat java.base/java.lang.Thread.run(Thread.java:1583)
      """;

  @Test
  @DisplayName("an uncaught exception is reported, named by the dialog that caused it")
  void reportsUncaughtExceptions() {
    List<String> crashes = ServerLog.crashes(CHECKSUM_CRASH);

    assertEquals(1, crashes.size(), () -> "expected one crash, got " + crashes);
    assertTrue(crashes.get(0).contains("Invalid thread access"), crashes.get(0));
    // The dialog, not PropsUiImpl: every one of these dies in the same shared helper, so naming
    // that would say nothing about where to go and fix it.
    assertTrue(crashes.get(0).contains("CheckSumDialog.setComboBoxes"), crashes.get(0));
  }

  /** Verbatim from Hop Web painting a graph with an image another session had disposed. */
  private static final String DISPOSED_IMAGE_CRASH =
      """
      SEVERE [qtp-1] org.eclipse.rap.rwt.internal.lifecycle.UIThread java.lang.IllegalArgumentException: Argument not valid
      \tat org.eclipse.swt.SWT.error(SWT.java:4527)
      \tat org.eclipse.swt.graphics.GC.drawImage(GC.java:1234)
      \tat org.apache.hop.ui.hopgui.shared.SwtGc.drawImage(SwtGc.java:212)
      \tat org.eclipse.rap.rwt.internal.lifecycle.UIThread.run(UIThread.java:104)
      """;

  @Test
  @DisplayName("a failure Hop caught and logged is a crash too")
  void reportsLoggedFailures() {
    List<String> crashes = ServerLog.crashes(DISPOSED_IMAGE_CRASH);

    assertEquals(1, crashes.size(), () -> "expected one crash, got " + crashes);
    assertTrue(crashes.get(0).contains("Argument not valid"), crashes.get(0));
    assertTrue(crashes.get(0).contains("SwtGc.drawImage"), crashes.get(0));
  }

  @Test
  @DisplayName("a widget used after its session went away is a crash")
  void reportsDisposedWidgets() {
    String log =
        "ERROR: org.eclipse.swt.SWTException: Widget is disposed\n"
            + "\tat org.apache.hop.ui.hopgui.HopGui.handleFileCapabilities(HopGui.java:900)\n";

    assertEquals(1, ServerLog.crashes(log).size());
  }

  @Test
  @DisplayName("the same failure logged over and over is reported once")
  void collapsesRepeats() {
    // A broken repaint logs on every paint; a hundred identical lines say no more than one.
    assertEquals(1, ServerLog.crashes(DISPOSED_IMAGE_CRASH.repeat(20)).size());
  }

  @Test
  @DisplayName("the cause of a reported failure is not counted a second time")
  void ignoresCausedBy() {
    String log =
        "ERROR org.eclipse.swt.SWTException: Invalid thread access\n"
            + "\tat org.apache.hop.ui.core.gui.GuiResource.getImage(GuiResource.java:1)\n"
            + "Caused by: java.lang.IllegalStateException: Invalid thread access\n"
            + "\tat org.eclipse.rap.rwt.RWT.checkContext(RWT.java:765)\n";

    assertEquals(1, ServerLog.crashes(log).size());
  }

  @Test
  @DisplayName("handled problems Hop Web logs all the time are not failures")
  void ignoresHandledProblems() {
    String noise =
        """
        Format problem with key=[SparkSqlDialog.Sql.Tooltip], locale=[en_US] : \
        java.lang.IllegalArgumentException: can't parse argument number: ...
        java.lang.IllegalArgumentException: can't parse argument number: ...
        \tat java.base/java.text.MessageFormat.makeFormat(MessageFormat.java:1449)
        org.w3c.css.sac.CSSException: Failed to read property box-shadow
        INFO Disposing the image cache of a session that timed out
        \tat org.apache.hop.pipeline.Pipeline.run: Invalid thread access recovered
        """;

    assertEquals(List.of(), ServerLog.crashes(noise));
  }

  @Test
  @DisplayName("a quiet log has nothing to report")
  void quietLogIsClean() {
    assertEquals(List.of(), ServerLog.crashes(""));
    assertEquals(List.of(), ServerLog.crashes("Started Hop Web\nJSON metadata in folder /x\n"));
  }

  @Test
  @DisplayName("different crashes are reported separately")
  void reportsEachCrash() {
    assertEquals(2, ServerLog.crashes(CHECKSUM_CRASH + DISPOSED_IMAGE_CRASH).size());
  }
}
