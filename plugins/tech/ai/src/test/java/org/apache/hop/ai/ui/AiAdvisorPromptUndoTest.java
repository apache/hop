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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.ui.AiAdvisorSessionPane.TextUndoHistory;
import org.apache.hop.ai.ui.AiAdvisorSessionPane.TextUndoHistory.State;
import org.junit.jupiter.api.Test;

class AiAdvisorPromptUndoTest {

  /** Type the text one character at a time, {@code gap} milliseconds apart. */
  private static String type(TextUndoHistory history, String before, String typed, long[] clock) {
    String text = before;
    for (char c : typed.toCharArray()) {
      String next = text + c;
      history.changed(new State(text, text.length()), next, clock[0]);
      clock[0] += 100;
      text = next;
    }
    return text;
  }

  @Test
  void typingInQuickSuccessionIsUndoneByTheWord() {
    TextUndoHistory history = new TextUndoHistory();
    long[] clock = {1_000};
    String text = type(history, "", "add", clock);
    text = type(history, text, " a dummy", clock);

    State back = history.undo(new State(text, text.length()));
    assertEquals("add a ", back.text());
    back = history.undo(back);
    assertEquals("add ", back.text());
    back = history.undo(back);
    assertEquals("", back.text());
    assertNull(history.undo(back), "nothing left to undo");
  }

  @Test
  void redoGoesForwardAgainUntilTheNextChange() {
    TextUndoHistory history = new TextUndoHistory();
    history.changed(new State("", 0), "why", 0);
    State back = history.undo(new State("why", 3));
    assertEquals("", back.text());
    State forward = history.redo(back);
    assertEquals("why", forward.text());

    history.undo(forward);
    history.changed(new State("", 0), "how", 10_000);
    assertNull(history.redo(new State("how", 3)), "a new change ends the redo");
  }

  @Test
  void aPauseOrAPasteIsAStepOfItsOwn() {
    TextUndoHistory history = new TextUndoHistory();
    history.changed(new State("", 0), "a", 0);
    history.changed(new State("a", 1), "ab", 5_000);
    history.changed(new State("ab", 2), "ab pasted text", 5_100);
    assertEquals("ab", history.undo(new State("ab pasted text", 14)).text());
    assertEquals("a", history.undo(new State("ab", 2)).text());
  }

  @Test
  void resetForgetsEverything() {
    TextUndoHistory history = new TextUndoHistory();
    history.changed(new State("", 0), "sent question", 0);
    history.reset();
    assertNull(history.undo(new State("", 0)));
  }

  @Test
  void onlyThePipelineAndWorkflowAssistantsNeedALinkedFile() {
    assertTrue(
        AiAdvisorSessionPane.needsArtifact(
            new org.apache.hop.ai.advisors.pipeline.PipelineAiAdvisor()));
    assertTrue(
        AiAdvisorSessionPane.needsArtifact(
            new org.apache.hop.ai.advisors.workflow.WorkflowAiAdvisor()));
    assertFalse(AiAdvisorSessionPane.needsArtifact(null));
  }

  @Test
  void unlinkedSessionsAreMarkedInTheTree() {
    AiAdvisorSession session = new AiAdvisorSession();
    session.setTitle("load-orders");
    assertEquals("load-orders (not linked)", AiAdvisorWorkbench.treeLabel(session));
    session.setArtifactFilename("/p/load-orders.hpl");
    assertEquals("load-orders (closed)", AiAdvisorWorkbench.treeLabel(session));
    session.setArtifact(new Object());
    assertEquals("load-orders", AiAdvisorWorkbench.treeLabel(session));
  }

  @Test
  void linkingToAnotherSavedFileIsConfirmed() {
    assertTrue(AiAdvisorWorkbench.isOtherFile("/p/a.hpl", "/p/b.hpl"));
    assertTrue(AiAdvisorWorkbench.isOtherFile("/p/a.hpl", null));
    assertFalse(AiAdvisorWorkbench.isOtherFile("/p/a.hpl", "/p/a.hpl"));
    assertFalse(AiAdvisorWorkbench.isOtherFile(null, "/p/b.hpl"), "never linked: nothing to ask");
  }
}
