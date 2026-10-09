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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The Ignore Lint Findings dialog.
 *
 * <p>It defaulted to ignoring every finding on the element, now and later, which also silences
 * rules added afterwards, security ones included.
 *
 * @see <a href="https://github.com/apache/hop/issues/8735">#8735</a>
 */
@Tag("uitest")
class LintSuppressDialogDefaultTest extends SwtBotTestBase {

  @Test
  void ignoringOnlyTheReportedRulesIsTheDefault() {
    List<LintResult> findings =
        List.of(
            new LintResult(
                "SEC-002", "Hardcoded Password", "ERROR", "hardcoded value", "/p/load.hpl"));
    AtomicBoolean listedSelected = new AtomicBoolean();
    AtomicBoolean allSelected = new AtomicBoolean();

    withDialog(
        parent -> new LintSuppressDialog(parent, "HTTP client", findings).open(),
        bot -> {
          SWTBot dialog = bot.shell("Ignore Lint Findings").bot();
          listedSelected.set(dialog.radio("Ignore only the rules reported now").isSelected());
          allSelected.set(
              dialog.radio("Ignore every finding on this element, now and later").isSelected());
          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });

    assertTrue(listedSelected.get(), "only the reported rules");
    assertFalse(allSelected.get(), "not everything, now and later");
  }
}
