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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.ai.advisor.AiAdvisorLocations;
import org.apache.hop.ai.advisor.AiAdvisorOpenRequest;
import org.apache.hop.ai.advisor.AiAdvisorPlugin;
import org.apache.hop.ai.advisor.AiAdvisorPluginType;
import org.apache.hop.ai.advisors.pipeline.PipelineAiAdvisor;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorSessionStore;
import org.apache.hop.ai.session.AiAdvisorTurn;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.TreeItem;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The AI Assistant window: the session list, its toolbar, closing and moving. */
@Tag("uitest")
class AiAdvisorWorkbenchUiTest extends SwtBotTestBase {

  @BeforeAll
  static void init() throws Exception {
    HopGuiEnvironment.init();
    PluginRegistry.addPluginType(AiAdvisorPluginType.getInstance());
    if (PluginRegistry.getInstance()
            .findPluginWithId(AiAdvisorPluginType.class, PipelineAiAdvisor.ID)
        == null) {
      PluginRegistry.getInstance()
          .registerPluginClass(
              PipelineAiAdvisor.class.getName(), AiAdvisorPluginType.class, AiAdvisorPlugin.class);
    }
    // The toolbar of the workbench, as Hop GUI registers it from the plugin's annotations.
    GuiRegistry registry = GuiRegistry.getInstance();
    for (Method method : AiAdvisorWorkbench.class.getMethods()) {
      GuiToolbarElement element = method.getAnnotation(GuiToolbarElement.class);
      if (element != null && registry.findGuiToolbarItem(element.root(), element.id()) == null) {
        registry.addGuiToolbarElement(
            AiAdvisorWorkbench.class.getName(),
            element,
            method,
            AiAdvisorWorkbench.class.getClassLoader());
      }
    }
  }

  @Test
  void aNewSessionWithoutAnOpenFileCanBeLinkedLater() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    AtomicReference<AiAdvisorWorkbench> workbench = new AtomicReference<>();
    withScene(
        shell ->
            workbench.set(newWorkbench(shell, store, IAiAdvisorWorkbenchHost.ViewKind.FLOATING)),
        bot -> {
          assertFalse(
              onUi(() -> item(workbench.get(), AiAdvisorWorkbench.TOOLBAR_ITEM_CLOSE).isEnabled()));
          onUi(
              () -> {
                workbench.get().toolbarNew();
                return null;
              });
          assertEquals(1, store.getSessions().size());
          AiAdvisorSession session = store.getActiveSession();
          assertEquals(null, session.getArtifact(), "no pipeline or workflow is open");
          assertTrue(
              onUi(() -> item(workbench.get(), AiAdvisorWorkbench.TOOLBAR_ITEM_LINK).isEnabled()));
          assertTrue(
              onUi(() -> item(workbench.get(), AiAdvisorWorkbench.TOOLBAR_ITEM_CLOSE).isEnabled()));
          assertEquals(List.of(session.displayTitle()), onUi(() -> sessionTitles(workbench.get())));
        });
  }

  @Test
  void sessionsAreListedPerAreaAndTheSelectedOneIsShown() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    AiAdvisorSession orders = store.open(pipelineRequest("orders"));
    AiAdvisorSession customers = store.open(pipelineRequest("customers"));
    AtomicReference<AiAdvisorWorkbench> workbench = new AtomicReference<>();
    withScene(
        shell ->
            workbench.set(newWorkbench(shell, store, IAiAdvisorWorkbenchHost.ViewKind.PERSPECTIVE)),
        bot -> {
          assertEquals(List.of("orders", "customers"), onUi(() -> sessionTitles(workbench.get())));
          assertEquals(customers.getId(), store.getActiveSessionId(), "the newest is shown");
          // A linked session cannot be linked again.
          assertFalse(
              onUi(() -> item(workbench.get(), AiAdvisorWorkbench.TOOLBAR_ITEM_LINK).isEnabled()));

          onUi(
              () -> {
                TreeItem first = workbench.get().getTree().getItem(0).getItem(0);
                workbench.get().getTree().setSelection(first);
                Event event = new Event();
                event.item = first;
                workbench.get().getTree().notifyListeners(SWT.Selection, event);
                return null;
              });
          assertEquals(orders.getId(), store.getActiveSessionId());
        });
  }

  @Test
  void closingAConversationAsksFirst() {
    AiAdvisorSessionStore store = new AiAdvisorSessionStore();
    AiAdvisorSession empty = store.open(pipelineRequest("empty"));
    AiAdvisorSession talked = store.open(pipelineRequest("talked"));
    AiAdvisorTurn turn = new AiAdvisorTurn();
    turn.setUserPrompt("What does it do?");
    talked.addTurn(turn);
    List<String> asked = new ArrayList<>();
    boolean[] answer = {false};
    AtomicReference<AiAdvisorWorkbench> workbench = new AtomicReference<>();
    withScene(
        shell -> {
          workbench.set(newWorkbench(shell, store, IAiAdvisorWorkbenchHost.ViewKind.PERSPECTIVE));
          workbench.get().confirmClose =
              session -> {
                asked.add(session.displayTitle());
                return answer[0];
              };
        },
        bot -> {
          // No: the conversation stays.
          onUi(
              () -> {
                workbench.get().toolbarClose();
                return null;
              });
          assertEquals(List.of("talked"), asked);
          assertEquals(2, store.getSessions().size());

          answer[0] = true;
          onUi(
              () -> {
                workbench.get().toolbarClose();
                return null;
              });
          assertEquals(1, store.getSessions().size());
          assertEquals(empty.getId(), store.getActiveSessionId());

          // A session without a conversation closes without asking.
          onUi(
              () -> {
                workbench.get().toolbarClose();
                return null;
              });
          assertEquals(List.of("talked", "talked"), asked);
          assertTrue(store.getSessions().isEmpty());
        });
  }

  @Test
  void theButtonOfTheCurrentPlaceIsDisabled() {
    AtomicReference<AiAdvisorWorkbench> floating = new AtomicReference<>();
    withScene(
        shell ->
            floating.set(
                newWorkbench(
                    shell, new AiAdvisorSessionStore(), IAiAdvisorWorkbenchHost.ViewKind.FLOATING)),
        bot -> {
          assertFalse(
              onUi(() -> item(floating.get(), AiAdvisorWorkbench.TOOLBAR_ITEM_FLOAT).isEnabled()));
          assertTrue(
              onUi(() -> item(floating.get(), AiAdvisorWorkbench.TOOLBAR_ITEM_DOCK).isEnabled()));
        });
    AtomicReference<AiAdvisorWorkbench> docked = new AtomicReference<>();
    withScene(
        shell ->
            docked.set(
                newWorkbench(
                    shell, new AiAdvisorSessionStore(), IAiAdvisorWorkbenchHost.ViewKind.DOCK)),
        bot -> {
          assertTrue(
              onUi(() -> item(docked.get(), AiAdvisorWorkbench.TOOLBAR_ITEM_FLOAT).isEnabled()));
          assertFalse(
              onUi(() -> item(docked.get(), AiAdvisorWorkbench.TOOLBAR_ITEM_DOCK).isEnabled()));
        });
  }

  // Helpers

  private static AiAdvisorWorkbench newWorkbench(
      Shell shell, AiAdvisorSessionStore store, IAiAdvisorWorkbenchHost.ViewKind kind) {
    shell.setLayout(new FillLayout());
    shell.setSize(1000, 650);
    return new AiAdvisorWorkbench(shell, new TestHost(shell, kind), store);
  }

  private static AiAdvisorOpenRequest pipelineRequest(String name) {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName(name);
    AiAdvisorOpenRequest request = new AiAdvisorOpenRequest();
    request.setAdvisorPluginId(PipelineAiAdvisor.ID);
    request.setLocation(AiAdvisorLocations.PIPELINE_GRAPH);
    request.setAreaLabel("Pipelines");
    request.setArtifact(pipelineMeta);
    request.setArtifactName(name);
    request.setArtifactKind("pipeline");
    request.setTitle(name);
    return request;
  }

  private static ToolItem item(AiAdvisorWorkbench workbench, String id) {
    ToolItem item = workbench.getToolBarWidgets().findToolItem(id);
    assertNotNull(item, "toolbar item " + id);
    return item;
  }

  private static List<String> sessionTitles(AiAdvisorWorkbench workbench) {
    List<String> titles = new ArrayList<>();
    for (TreeItem group : workbench.getTree().getItems()) {
      for (TreeItem session : group.getItems()) {
        titles.add(session.getText());
      }
    }
    return titles;
  }

  private static <T> T onUi(Supplier<T> supplier) {
    AtomicReference<T> result = new AtomicReference<>();
    Display.getDefault().syncExec(() -> result.set(supplier.get()));
    return result.get();
  }

  /** A host without Hop GUI, in the given place. */
  private static final class TestHost implements IAiAdvisorWorkbenchHost {
    private final Shell shell;
    private final ViewKind kind;
    private final IVariables variables = new Variables();
    private final IHopMetadataProvider metadataProvider = new MemoryMetadataProvider();

    private TestHost(Shell shell, ViewKind kind) {
      this.shell = shell;
      this.kind = kind;
    }

    @Override
    public HopGui getHopGui() {
      return null;
    }

    @Override
    public Shell getShell() {
      return shell;
    }

    @Override
    public Display getDisplay() {
      return shell.getDisplay();
    }

    @Override
    public IVariables getVariables() {
      return variables;
    }

    @Override
    public IHopMetadataProvider getMetadataProvider() {
      return metadataProvider;
    }

    @Override
    public void activate() {
      // Nothing to bring to the front.
    }

    @Override
    public void asyncExec(Runnable runnable) {
      shell.getDisplay().asyncExec(runnable);
    }

    @Override
    public ViewKind getViewKind() {
      return kind;
    }
  }
}
