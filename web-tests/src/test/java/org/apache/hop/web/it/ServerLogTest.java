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
  @DisplayName("several crashes are reported separately")
  void reportsEachCrash() {
    assertEquals(2, ServerLog.crashes(CHECKSUM_CRASH + CHECKSUM_CRASH).size());
  }
}
