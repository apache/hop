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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.ai.advisor.AiAdvisorOpenRequest;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorSessionStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * A session lets go of its pipeline when the pipeline's tab closes. The tab folder only hides the
 * graph of a closed tab, so the tab itself is what tells.
 */
@Tag("uitest")
class AiAdvisorReleaseWhenClosedUiTest extends SwtBotTestBase {

  @Test
  void closingTheTabUnlinksTheSessionAndMovingItDoesNot() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("orders");
    AiAdvisorOpenRequest request = new AiAdvisorOpenRequest();
    request.setArtifact(pipelineMeta);
    AiAdvisorSession session = store.open(request);
    AtomicReference<CTabFolder> left = new AtomicReference<>();
    AtomicReference<CTabFolder> right = new AtomicReference<>();
    AtomicReference<Composite> graph = new AtomicReference<>();

    withScene(
        shell -> {
          shell.setLayout(new FillLayout());
          left.set(new CTabFolder(shell, SWT.BORDER));
          right.set(new CTabFolder(shell, SWT.BORDER));
          graph.set(new Composite(left.get(), SWT.NONE));
          CTabItem tab = new CTabItem(left.get(), SWT.CLOSE);
          tab.setControl(graph.get());
          AiAdvisorViews.releaseWhenClosed(graph.get(), () -> store, pipelineMeta);
        },
        bot -> {
          // Moved to the other pane, as ExplorerPerspective.moveTabToFolder does it.
          onUi(
              () -> {
                CTabItem old = left.get().getItem(0);
                old.setControl(null);
                old.dispose();
                graph.get().setParent(right.get());
                new CTabItem(right.get(), SWT.CLOSE).setControl(graph.get());
                return null;
              });
          drainUi();
          assertSame(pipelineMeta, session.getArtifact(), "a move is not a close");

          // Closed: the folder disposes the tab and only hides the graph.
          onUi(
              () -> {
                right.get().getItem(0).dispose();
                return null;
              });
          assertNull(session.getArtifact(), "the session is unlinked");
          assertFalse(onUi(() -> graph.get().isDisposed()), "closing a tab hides its graph");
        });
  }

  /** Let the asyncExec that watches the tab of a moved graph run. */
  private static void drainUi() {
    onUi(() -> null);
    onUi(() -> null);
  }

  private static <T> T onUi(Supplier<T> supplier) {
    AtomicReference<T> result = new AtomicReference<>();
    Display.getDefault().syncExec(() -> result.set(supplier.get()));
    return result.get();
  }
}
