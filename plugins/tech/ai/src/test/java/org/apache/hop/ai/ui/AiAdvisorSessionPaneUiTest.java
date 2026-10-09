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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.ai.advisor.AiAdvisorLocations;
import org.apache.hop.ai.advisor.AiAdvisorOpenRequest;
import org.apache.hop.ai.advisor.AiAdvisorPlugin;
import org.apache.hop.ai.advisor.AiAdvisorPluginType;
import org.apache.hop.ai.advisors.pipeline.PipelineAiAdvisor;
import org.apache.hop.ai.config.HopAiConfigSingleton;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorSessionStore;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** The session pane as the user sees it: hints, Send feedback, the focus line and Sharing. */
@Tag("uitest")
class AiAdvisorSessionPaneUiTest extends SwtBotTestBase {

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
  }

  @Test
  void withoutASessionTheHintIsReadableAndTheQuestionFieldDisabled() {
    AtomicReference<AiAdvisorSessionPane> pane = new AtomicReference<>();
    withScene(
        shell -> {
          pane.set(newPane(shell));
          // What the workbench does when the store has no session.
          pane.get().showSession(null);
        },
        bot -> {
          bot.label(message("AiAdvisor.Status.NoSession"));
          assertFalse(onUi(() -> find(pane.get(), Text.class).isEnabled()));
        });
  }

  @Test
  void sendingAnEmptyQuestionSaysWhatToDo() {
    AtomicReference<AiAdvisorSessionPane> pane = new AtomicReference<>();
    withScene(
        shell -> {
          pane.set(newPane(shell));
          pane.get().showSession(pipelineSession(null));
        },
        bot -> {
          HopAiConfigSingleton.getConfig().setAiEnabled(true);
          onUi(
              () -> {
                pane.get().onSendOrCancel();
                return null;
              });
          bot.label(message("AiAdvisor.Status.NoQuestion"));
        });
  }

  @Test
  void theFocusLineShowsTheTransformAndClearRemovesIt() {
    AtomicReference<AiAdvisorSessionPane> pane = new AtomicReference<>();
    AtomicReference<AiAdvisorSession> session = new AtomicReference<>();
    withScene(
        shell -> {
          pane.set(newPane(shell));
          session.set(pipelineSession("Read orders"));
          pane.get().showSession(session.get());
        },
        bot -> {
          Link focus = onUi(() -> find(pane.get(), Link.class));
          assertTrue(onUi(focus::isVisible));
          assertTrue(onUi(focus::getText).contains("Read orders"));
          assertTrue(onUi(() -> sharingLine(pane.get())).contains("Read orders"));

          onUi(
              () -> {
                focus.notifyListeners(
                    org.eclipse.swt.SWT.Selection, new org.eclipse.swt.widgets.Event());
                return null;
              });
          assertEquals("", session.get().getFocusNodeName());
          assertFalse(onUi(focus::isVisible));
          assertFalse(onUi(() -> sharingLine(pane.get())).contains("Read orders"));
        });
  }

  @Test
  void fullXmlIsDisabledAndNotListedWhileTheGlobalOptionIsOff() {
    AtomicReference<AiAdvisorSessionPane> pane = new AtomicReference<>();
    boolean original = HopAiConfigSingleton.getConfig().isAllowSendFullXml();
    HopAiConfigSingleton.getConfig().setAllowSendFullXml(false);
    try {
      withScene(
          shell -> {
            pane.set(newPane(shell));
            AiAdvisorSession session = pipelineSession(null);
            session.getInclusions().put("xml", true);
            pane.get().showSession(session);
          },
          bot -> {
            // The Sharing panel starts collapsed, so look the checkbox up rather than via SWTBot.
            String label =
                org.apache.hop.i18n.BaseMessages.getString(
                    PipelineAiAdvisor.class, "PipelineAiAdvisor.Inclusion.Xml");
            Button xml =
                onUi(
                    () ->
                        findAll(pane.get(), Button.class).stream()
                            .filter(button -> label.equals(button.getText()))
                            .findFirst()
                            .orElseThrow());
            assertFalse(onUi(xml::isEnabled));
            assertFalse(onUi(xml::getSelection));
            String fullXml =
                org.apache.hop.i18n.BaseMessages.getString(
                    PipelineAiAdvisor.class, "PipelineAiAdvisor.Inclusion.Xml.Summary");
            assertFalse(onUi(() -> sharingLine(pane.get())).contains(fullXml));
          });
    } finally {
      HopAiConfigSingleton.getConfig().setAllowSendFullXml(original);
    }
  }

  @Test
  void anUnlinkedSessionGetsTheDefaultProvider() throws Exception {
    AtomicReference<AiAdvisorSessionPane> pane = new AtomicReference<>();
    MemoryMetadataProvider metadata = new MemoryMetadataProvider();
    AiProvider ollama = new AiProvider();
    ollama.setName("ollama");
    metadata.getSerializer(AiProvider.class).save(ollama);
    String original = HopAiConfigSingleton.getConfig().getDefaultProviderName();
    HopAiConfigSingleton.getConfig().setDefaultProviderName("ollama");
    try {
      withScene(
          shell -> {
            shell.setLayout(new FillLayout());
            shell.setSize(900, 600);
            pane.set(new AiAdvisorSessionPane(shell, new TestHost(shell, metadata)));
            pane.get().showSession(null);
            AiAdvisorOpenRequest general = new AiAdvisorOpenRequest();
            general.setReuseExisting(false);
            general.setTitle("New session");
            pane.get().showSession(new AiAdvisorSessionStore().open(general));
          },
          bot ->
              assertEquals(
                  "ollama", onUi(() -> find(pane.get(), MetaSelectionLine.class).getText())));
    } finally {
      HopAiConfigSingleton.getConfig().setDefaultProviderName(original);
    }
  }

  // Helpers

  private static AiAdvisorSessionPane newPane(Shell shell) {
    shell.setLayout(new FillLayout());
    shell.setSize(900, 600);
    return new AiAdvisorSessionPane(shell, new TestHost(shell, new MemoryMetadataProvider()));
  }

  private static AiAdvisorSession pipelineSession(String focus) {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("orders");
    AiAdvisorOpenRequest request = new AiAdvisorOpenRequest();
    request.setAdvisorPluginId(PipelineAiAdvisor.ID);
    request.setLocation(AiAdvisorLocations.PIPELINE_GRAPH);
    request.setArtifact(pipelineMeta);
    request.setArtifactName("orders");
    request.setArtifactKind("pipeline");
    request.setTitle("orders");
    request.setFocusNodeName(focus);
    return new AiAdvisorSessionStore().open(request);
  }

  private static String sharingLine(AiAdvisorSessionPane pane) {
    for (Label label : findAll(pane, Label.class)) {
      if (label.getText().startsWith(message("AiAdvisor.Sharing.Prefix").trim())) {
        return label.getText();
      }
    }
    return "";
  }

  private static String message(String key) {
    return org.apache.hop.i18n.BaseMessages.getString(AiAdvisorPerspective.class, key);
  }

  private static <T> T onUi(java.util.function.Supplier<T> supplier) {
    AtomicReference<T> result = new AtomicReference<>();
    Display.getDefault().syncExec(() -> result.set(supplier.get()));
    return result.get();
  }

  private static <T extends org.eclipse.swt.widgets.Control> T find(
      org.eclipse.swt.widgets.Composite parent, Class<T> type) {
    java.util.List<T> all = findAll(parent, type);
    return all.isEmpty() ? null : all.get(0);
  }

  private static <T extends org.eclipse.swt.widgets.Control> java.util.List<T> findAll(
      org.eclipse.swt.widgets.Composite parent, Class<T> type) {
    java.util.List<T> found = new java.util.ArrayList<>();
    for (org.eclipse.swt.widgets.Control child : parent.getChildren()) {
      if (type.isInstance(child)) {
        found.add(type.cast(child));
      }
      if (child instanceof org.eclipse.swt.widgets.Composite composite) {
        found.addAll(findAll(composite, type));
      }
    }
    return found;
  }

  /** A host without Hop GUI: the pane only needs variables, metadata and a shell. */
  private static final class TestHost implements IAiAdvisorWorkbenchHost {
    private final Shell shell;
    private final IVariables variables = new Variables();
    private final IHopMetadataProvider metadataProvider;

    private TestHost(Shell shell, IHopMetadataProvider metadataProvider) {
      this.shell = shell;
      this.metadataProvider = metadataProvider;
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
  }
}
