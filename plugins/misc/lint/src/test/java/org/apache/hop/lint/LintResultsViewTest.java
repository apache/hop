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
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.gui.IGc;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Show Lint Results, the ignored marker and the linter settings.
 *
 * @see <a href="https://github.com/apache/hop/issues/8735">#8735</a>
 */
public class LintResultsViewTest {

  private static LintResult finding(String file, String ruleId) {
    return new LintResult(ruleId, ruleId, "WARNING", "message " + ruleId, file);
  }

  private static List<String> sorted(
      List<LintResult> results, LintResultGrouping.SortKey key, boolean ascending) {
    List<LintResult> copy = new ArrayList<>(results);
    copy.sort(LintResultGrouping.order(key, ascending));
    return copy.stream()
        .map(r -> new java.io.File(r.getFileName()).getName() + " " + r.getRuleId())
        .toList();
  }

  private static final List<LintResult> RESULTS =
      List.of(
          finding("/project/load/b.hpl", "STRUCT-003"),
          finding("/project/main.hwf", "DOC-002"),
          finding("/project/load/a.hpl", "NAMING-004"),
          finding("/project/load/b.hpl", "DOC-001"));

  /** Files came in no clear order, main.hwf between two .hpl files, with rules mixed per file. */
  @Test
  public void resultsAreOrderedByFileThenRule() {
    assertEquals(
        List.of("a.hpl NAMING-004", "b.hpl DOC-001", "b.hpl STRUCT-003", "main.hwf DOC-002"),
        sorted(RESULTS, LintResultGrouping.SortKey.FILE, true));
  }

  @Test
  public void resultsCanBeOrderedByRuleEitherWay() {
    List<String> byRule = sorted(RESULTS, LintResultGrouping.SortKey.RULE, true);
    assertEquals(
        List.of("b.hpl DOC-001", "main.hwf DOC-002", "a.hpl NAMING-004", "b.hpl STRUCT-003"),
        byRule);
    assertEquals(
        byRule.reversed(), sorted(RESULTS, LintResultGrouping.SortKey.RULE, false), "reversed");
  }

  @Test
  public void errorsComeFirst() {
    List<String> severities = new ArrayList<>(List.of("INFO", "CUSTOM", "WARNING", "ERROR"));
    severities.sort(LintResultGrouping.severityOrder());
    assertEquals(List.of("ERROR", "WARNING", "INFO", "CUSTOM"), severities);
  }

  /**
   * A 1px grey rounded outline was all but the normal transform border, so an ignored transform
   * looked like any other.
   */
  @Test
  public void anIgnoredTransformGetsADashedOutlineAndABadge() throws Exception {
    IGc gc = mock(IGc.class);
    when(gc.getMagnification()).thenReturn(1f);

    LintCanvasOverlayHelper.drawIgnoredOverlay(gc, 100, 100, 32, false);

    InOrder order = inOrder(gc);
    order.verify(gc).setLineStyle(IGc.ELineStyle.DASH);
    order.verify(gc).drawRoundRectangle(anyInt(), anyInt(), anyInt(), anyInt(), anyInt(), anyInt());
    order.verify(gc).setLineStyle(IGc.ELineStyle.SOLID);
    verify(gc).drawImage(eq(IGc.EImage.INFO_DISABLED), anyInt(), anyInt(), anyFloat());
  }

  /**
   * "Block Commits on Warnings" means nothing while commits are not blocked, and stayed enabled
   * when they were not.
   */
  @Test
  public void blockingOnWarningsFollowsBlockingCommits() {
    Button preCommit = mock(Button.class);
    Button blockWarnings = mock(Button.class);
    GuiCompositeWidgets widgets = mock(GuiCompositeWidgets.class);
    when(widgets.getWidgetsMap())
        .thenReturn(
            Map.<String, Control>of(
                "linter-pre-commit-enabled", preCommit,
                "linter-pre-commit-block-warnings", blockWarnings));

    when(preCommit.getSelection()).thenReturn(false);
    LinterConfigPlugin.enableDependentWidgets(widgets);
    verify(blockWarnings).setEnabled(false);

    when(preCommit.getSelection()).thenReturn(true);
    LinterConfigPlugin.enableDependentWidgets(widgets);
    verify(blockWarnings).setEnabled(true);
  }
}
