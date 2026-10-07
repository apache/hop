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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorTurn;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The transcript as a window shows it when it opens, before any resize. */
@Tag("uitest")
class AiAdvisorTranscriptPanelUiTest extends SwtBotTestBase {

  @BeforeAll
  static void init() throws Exception {
    HopGuiEnvironment.init();
  }

  @Test
  void aShortQuestionTakesOneLineWhenTheWindowOpens() {
    AtomicReference<AiAdvisorTranscriptPanel> panel = new AtomicReference<>();
    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          shell.setSize(1000, 700);
          // Filled before the window is shown, as when the assistant opens on a session.
          panel.set(new AiAdvisorTranscriptPanel(shell));
          AiAdvisorSession session = new AiAdvisorSession();
          AiAdvisorTurn turn = new AiAdvisorTurn();
          turn.setUserPrompt("what can you tell me about the last execution for this pipeline?");
          turn.setAssistantAdvice("It ran without errors.");
          session.addTurn(turn);
          panel.get().showSession(session);
        },
        bot -> {
          bot.sleep(500);
          int[] heights =
              onUi(
                  () -> {
                    Text question = findAll(panel.get(), Text.class).get(0);
                    return new int[] {question.getSize().y, question.getLineHeight()};
                  });
          assertTrue(
              heights[0] < 3 * heights[1],
              "the question is one line high, not "
                  + heights[0]
                  + " pixels for a line height of "
                  + heights[1]);
        });
  }

  private static <T> T onUi(java.util.function.Supplier<T> supplier) {
    AtomicReference<T> result = new AtomicReference<>();
    Display.getDefault().syncExec(() -> result.set(supplier.get()));
    return result.get();
  }

  private static <T extends Control> List<T> findAll(Composite parent, Class<T> type) {
    List<T> found = new ArrayList<>();
    for (Control child : parent.getChildren()) {
      if (type.isInstance(child)) {
        found.add(type.cast(child));
      }
      if (child instanceof Composite composite) {
        found.addAll(findAll(composite, type));
      }
    }
    return found;
  }
}
