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
package org.apache.hop.ai.ui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.advisor.AiProposalValidation;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The review dialog says per proposal whether it will be applied, whatever the theme. */
@Tag("uitest")
class AiAdvisorProposalReviewDialogUiTest extends SwtBotTestBase {

  private static final Class<?> PKG = AiAdvisorPerspective.class;

  @BeforeAll
  static void init() throws Exception {
    HopGuiEnvironment.init();
  }

  @Test
  void stateIsSpelledOutAndThePreviewFollowsTheHighlightedRow() {
    AiProposal add = proposal("ADD_TRANSFORM", "Add a Dummy");
    AiProposal delete = proposal("DELETE_TRANSFORM", "Delete Input");
    AiProposalValidation optIn = new AiProposalValidation();
    optIn.setOptIn(true);
    AtomicReference<AiAdvisorProposalReviewDialog> dialog = new AtomicReference<>();
    AtomicReference<Boolean> applied = new AtomicReference<>();

    withDialog(
        parent -> {
          dialog.set(
              new AiAdvisorProposalReviewDialog(
                  parent,
                  List.of(add, delete),
                  List.of(new AiProposalValidation(), optIn),
                  proposal -> "preview of " + proposal.getDescription()));
          applied.set(dialog.get().open());
        },
        bot -> {
          SWTBotTable table = bot.table();
          assertEquals(text("AiAdvisorProposalReviewDialog.State.Apply"), table.cell(0, 0));
          assertEquals(text("AiAdvisorProposalReviewDialog.State.Skip"), table.cell(1, 0));
          bot.button(BaseMessages.getString(PKG, "AiAdvisorProposalReviewDialog.Apply.Count", 1));
          assertTrue(bot.text().getText().contains("preview of Add a Dummy"));

          table.select(1);
          assertTrue(bot.text().getText().contains("preview of Delete Input"));

          table.doubleClick(1, 1);
          assertEquals(text("AiAdvisorProposalReviewDialog.State.Apply"), table.cell(1, 0));
          bot.button(BaseMessages.getString(PKG, "AiAdvisorProposalReviewDialog.Apply.Count", 2))
              .click();
        });

    assertTrue(applied.get());
    assertEquals(List.of(add, delete), dialog.get().getSelectedProposals());
  }

  private static String text(String key) {
    return BaseMessages.getString(PKG, key);
  }

  private static AiProposal proposal(String type, String description) {
    AiProposal proposal = new AiProposal();
    proposal.setType(type);
    proposal.setDescription(description);
    proposal.setRiskLevel("LOW");
    return proposal;
  }
}
