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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.lint.registry.RuleRegistry;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Editing a native rule, which says how Hop's own verify remarks are reported.
 *
 * <p>HOP-CHECK has no target, field or condition, so the editor opened it with no target selected
 * and refused every save with "Target type must be selected", even a change of severity.
 *
 * @see <a href="https://github.com/apache/hop/issues/8734">#8734</a>
 */
@Tag("uitest")
class RuleBuilderDialogNativeRuleTest extends SwtBotTestBase {

  @Test
  void theSeverityOfHopCheckCanBeChanged() {
    CustomLintRule hopCheck =
        RuleRegistry.getInstance().resolve(null).getRules().stream()
            .filter(rule -> "HOP-CHECK".equals(rule.generateRuleId()))
            .findFirst()
            .orElseThrow()
            .copy();
    AtomicReference<CustomLintRule> saved = new AtomicReference<>();
    AtomicReference<Boolean> targetEnabled = new AtomicReference<>();

    withDialog(
        parent -> saved.set(new RuleBuilderDialog(parent, hopCheck).open()),
        bot -> {
          SWTBot dialog = bot.shell("Edit Lint Rule").bot();
          targetEnabled.set(dialog.comboBoxWithLabel("Target Type:").isEnabled());
          dialog.comboBoxWithLabel("Severity:").setSelection("ERROR");
          dialog.button("OK").click();
        });

    assertFalse(targetEnabled.get(), "a native rule has no target to choose");
    assertNotNull(saved.get(), "the dialog refused to save");
    assertEquals("ERROR", saved.get().getSeverity());
    assertTrue(saved.get().isNativeVerify(), "still a native rule");
    assertNull(saved.get().getTarget(), "and still without a target");
  }
}
