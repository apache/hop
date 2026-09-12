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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hop.ai.advisor.AiAdvisorInclusion;
import org.apache.hop.ai.advisor.AiAdvisorInclusionChoice;
import org.apache.hop.ai.advisor.AiAdvisorLocations;
import org.apache.hop.ai.advisor.AiAdvisorMetadataSelection;
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.ai.advisor.AiAdvisorResponse;
import org.apache.hop.ai.advisor.AiAdvisorScenario;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.advisor.AiProposalValidation;
import org.apache.hop.ai.advisor.IAiAdvisor;
import org.apache.hop.ai.advisors.AiAdvisorInclusions;
import org.apache.hop.ai.config.HopAiConfig;
import org.apache.hop.ai.config.HopAiConfigSingleton;
import org.apache.hop.ai.engine.AiAdvisorEngine;
import org.apache.hop.ai.engine.AiM2PromptSupport;
import org.apache.hop.ai.engine.AiProposalPreview;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorSessionStore;
import org.apache.hop.ai.session.AiAdvisorTurn;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.apache.hop.ui.hopgui.BackgroundThreadFacade;
import org.apache.hop.ui.hopgui.file.pipeline.HopGuiPipelineGraph;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Text;

/** Chat UI for the selected {@link AiAdvisorSession}. */
public class AiAdvisorSessionPane extends Composite {

  private static final Class<?> PKG = AiAdvisorPerspective.class;

  private final IAiAdvisorWorkbenchHost host;
  private final AiAdvisorSessionStore store;

  private AiAdvisorSession session;
  private Combo wAdvisor;
  private Combo wScenario;
  private MetaSelectionLine<AiProvider> wProvider;
  private Composite sharingPanel;
  private Composite sharingHeader;
  private Button wSharingToggle;
  private Label wlSharing;
  private Composite inclusionsComposite;
  private FormData inclusionsFormData;
  private boolean sharingExpanded;
  private final List<AiAdvisorInclusion> currentInclusions = new ArrayList<>();
  private final Map<String, Button> inclusionButtons = new LinkedHashMap<>();
  private final Map<String, Button> inclusionPickers = new LinkedHashMap<>();
  private Label wlStatus;
  private AiAdvisorTranscriptPanel transcript;
  private Text wPrompt;
  private Button wSend;
  private Button wMetadataSelect;
  private final List<IPlugin> advisorPlugins = new ArrayList<>();
  private final SendShortcutGuard sendShortcutGuard = new SendShortcutGuard();
  private boolean updatingUi;

  public AiAdvisorSessionPane(Composite parent, IAiAdvisorWorkbenchHost host) {
    super(parent, SWT.NONE);
    this.host = host;
    this.store = AiAdvisorSessionStore.get(host.getHopGui());
    PropsUi.setLook(this);
    setLayout(PropsUi.getInstance().createFormLayout());
    int margin = PropsUi.getMargin();
    GuiResource gui = GuiResource.getInstance();

    Label wQuestion = new Label(this, SWT.CENTER);
    wQuestion.setImage(
        gui.getImage("ui/images/help.svg", ConstUi.LARGE_ICON_SIZE, ConstUi.LARGE_ICON_SIZE));
    wQuestion.setToolTipText(BaseMessages.getString(PKG, "AiAdvisor.Prompt.Question.Tooltip"));
    PropsUi.setLook(wQuestion);

    wSend = new Button(this, SWT.PUSH | SWT.FLAT);
    wSend.setImage(
        gui.getImage("ui/images/logo_icon.svg", ConstUi.LARGE_ICON_SIZE, ConstUi.LARGE_ICON_SIZE));
    wSend.setToolTipText(BaseMessages.getString(PKG, "AiAdvisor.Send.Tooltip"));
    wSend.addListener(SWT.Selection, e -> onSendOrCancel());

    wPrompt = new Text(this, SWT.MULTI | SWT.WRAP | SWT.BORDER | SWT.V_SCROLL);
    applyPromptFieldLook(wPrompt);
    wPrompt.setMessage(BaseMessages.getString(PKG, "AiAdvisor.Prompt.Message"));
    wPrompt.addPaintListener(e -> paintPromptHint(wPrompt, e));
    wPrompt.setLayoutData(
        new FormDataBuilder()
            .left(wQuestion, margin)
            .right(wSend, -margin)
            .bottom()
            .height((int) (70 * PropsUi.getNativeZoomFactor()))
            .result());
    installSendShortcut(wPrompt);

    wQuestion.setLayoutData(new FormDataBuilder().left().bottom(wPrompt, 0, SWT.CENTER).result());
    wSend.setLayoutData(new FormDataBuilder().right().bottom(wPrompt, 0, SWT.CENTER).result());

    Control top = createHeader(margin);

    wlStatus = new Label(this, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlStatus);
    wlStatus.setLayoutData(new FormDataBuilder().left().right().top(top, margin).result());

    transcript = new AiAdvisorTranscriptPanel(this);
    transcript.setLayoutData(
        new FormDataBuilder()
            .left()
            .right()
            .top(wlStatus, margin)
            .bottom(wPrompt, -margin)
            .result());
  }

  /**
   * Pastel green cue for the question field. Light mode keeps a pale mint; dark mode uses a muted
   * sage so the field does not glare against the rest of the workbench.
   */
  static void applyPromptFieldLook(Text prompt) {
    PropsUi.setLook(prompt);
    GuiResource gui = GuiResource.getInstance();
    if (PropsUi.getInstance().isDarkMode()) {
      prompt.setBackground(gui.getColor(40, 72, 56));
      prompt.setForeground(gui.getColor(214, 236, 222));
    } else {
      prompt.setBackground(gui.getColor(232, 248, 238));
      prompt.setForeground(gui.getColor(20, 45, 35));
    }
  }

  private void paintPromptHint(Text prompt, PaintEvent event) {
    if (prompt.getCharCount() > 0) {
      return;
    }
    GuiResource gui = GuiResource.getInstance();
    boolean dark = PropsUi.getInstance().isDarkMode();
    event.gc.setForeground(dark ? gui.getColor(150, 186, 168) : gui.getColor(110, 140, 120));
    event.gc.drawText(
        BaseMessages.getString(PKG, "AiAdvisor.Prompt.Message"),
        PropsUi.getMargin(),
        PropsUi.getFormMargin(),
        true);
  }

  /**
   * Ctrl/Cmd+Enter sends the question, same as the logo button. Eat the newline on Traverse,
   * KeyDown and Verify so GTK and Windows do not insert a blank line first.
   *
   * <p>GTK delivers both {@link SWT#Traverse} ({@code TRAVERSE_RETURN}) and {@link SWT#KeyDown} for
   * one keystroke. The first event must send; the second must not, or {@link #onSendOrCancel()}
   * would cancel the request it just started. Cancelling the traverse still matters so the shell
   * default button is not activated and so KeyDown is not dropped on GTK.
   */
  private void installSendShortcut(Text prompt) {
    Listener listener =
        (Event event) -> {
          if (event.type == SWT.Verify) {
            if (IContentEditorWidget.isLineDelimiterText(event.text)
                && (sendShortcutGuard.isArmed()
                    || IContentEditorWidget.isExecuteModifier(event.stateMask))) {
              event.doit = false;
            }
            return;
          }
          if (!isSendShortcut(
              event.type, event.detail, event.stateMask, event.keyCode, event.character)) {
            return;
          }
          event.doit = false;
          if (event.type == SWT.Traverse) {
            event.detail = SWT.TRAVERSE_NONE;
          }
          if (!sendShortcutGuard.claim()) {
            return;
          }
          try {
            onSendOrCancel();
          } finally {
            Display display = prompt.getDisplay();
            if (display != null && !display.isDisposed()) {
              display.asyncExec(
                  () -> {
                    if (!display.isDisposed()) {
                      sendShortcutGuard.release();
                    }
                  });
            } else {
              sendShortcutGuard.release();
            }
          }
        };
    prompt.addListener(SWT.KeyDown, listener);
    prompt.addListener(SWT.Traverse, listener);
    prompt.addListener(SWT.Verify, listener);
  }

  static boolean shouldEatSendNewline(int stateMask, String text) {
    return IContentEditorWidget.isLineDelimiterText(text)
        && IContentEditorWidget.isExecuteModifier(stateMask);
  }

  static boolean isSendShortcut(int type, int detail, int stateMask, int keyCode, char character) {
    if (type == SWT.Traverse) {
      return IContentEditorWidget.isExecuteTraverse(detail, stateMask);
    }
    return IContentEditorWidget.isExecuteKey(stateMask, keyCode, character);
  }

  /**
   * One-shot latch for a Ctrl+Enter keystroke. GTK raises Traverse then KeyDown; claiming twice in
   * the same event loop must fail so send is not immediately cancelled.
   */
  static final class SendShortcutGuard {
    private boolean armed;

    boolean claim() {
      if (armed) {
        return false;
      }
      armed = true;
      return true;
    }

    boolean isArmed() {
      return armed;
    }

    void release() {
      armed = false;
    }
  }

  static String userVisibleError(Throwable error) {
    if (error == null) {
      return "";
    }
    if (error instanceof HopException hop) {
      String superMessage = hop.getSuperMessage();
      if (!Utils.isEmpty(superMessage)) {
        return superMessage.trim();
      }
    }
    String message = error.getMessage();
    if (Utils.isEmpty(message) && error.getCause() != null) {
      message = error.getCause().getMessage();
    }
    if (Utils.isEmpty(message)) {
      message = error.getClass().getSimpleName();
    }
    return message;
  }

  private Control createHeader(int margin) {
    Label wlAdvisor = new Label(this, SWT.LEFT);
    wlAdvisor.setText(BaseMessages.getString(PKG, "AiAdvisor.Advisor.Label"));
    PropsUi.setLook(wlAdvisor);
    wlAdvisor.setLayoutData(new FormDataBuilder().left().top().result());

    wAdvisor = new Combo(this, SWT.READ_ONLY | SWT.BORDER);
    PropsUi.setLook(wAdvisor);
    wAdvisor.setLayoutData(
        new FormDataBuilder().left(wlAdvisor, margin).top().right(50, -margin).result());
    wAdvisor.addListener(
        SWT.Selection,
        e -> {
          if (!updatingUi) {
            advisorChanged();
          }
        });

    Label wlScenario = new Label(this, SWT.LEFT);
    wlScenario.setText(BaseMessages.getString(PKG, "AiAdvisor.Scenario.Label"));
    PropsUi.setLook(wlScenario);
    wlScenario.setLayoutData(new FormDataBuilder().left(wAdvisor, margin).top().result());

    wScenario = new Combo(this, SWT.READ_ONLY | SWT.BORDER);
    PropsUi.setLook(wScenario);
    wScenario.setLayoutData(new FormDataBuilder().left(wlScenario, margin).top().right().result());
    wScenario.addListener(
        SWT.Selection,
        e -> {
          if (updatingUi || session == null) {
            return;
          }
          String scenarioId = selectedScenarioId();
          if (Objects.equals(session.getScenarioId(), scenarioId)) {
            return;
          }
          session.setScenarioId(scenarioId);
          store.fireChanged();
        });

    wProvider =
        new MetaSelectionLine<>(
            host.getVariables(),
            host.getMetadataProvider(),
            AiProvider.class,
            this,
            SWT.NONE,
            BaseMessages.getString(PKG, "AiAdvisor.Provider.Label"),
            BaseMessages.getString(PKG, "AiAdvisor.Provider.Tooltip"),
            true);
    wProvider.setLayoutData(new FormDataBuilder().left().right().top(wAdvisor, margin).result());
    wProvider.addModifyListener(
        e -> {
          if (updatingUi || session == null) {
            return;
          }
          String name = Const.NVL(wProvider.getText(), "");
          if (name.equals(Const.NVL(session.getProviderName(), ""))) {
            return;
          }
          session.setProviderName(name);
          store.fireChanged();
        });

    sharingPanel = new Composite(this, SWT.NONE);
    PropsUi.setLook(sharingPanel);
    sharingPanel.setLayout(PropsUi.getInstance().createFormLayout());
    sharingPanel.setLayoutData(
        new FormDataBuilder().left().right().top(wProvider, margin).result());

    sharingHeader = new Composite(sharingPanel, SWT.NONE);
    PropsUi.setLook(sharingHeader);
    sharingHeader.setLayout(PropsUi.getInstance().createFormLayout());
    sharingHeader.setLayoutData(new FormDataBuilder().left().right().top().result());
    sharingHeader.setCursor(getDisplay().getSystemCursor(SWT.CURSOR_HAND));
    sharingHeader.addListener(SWT.MouseDown, e -> toggleSharingPanel());

    wSharingToggle = new Button(sharingHeader, SWT.PUSH | SWT.FLAT);
    PropsUi.setLook(wSharingToggle);
    wSharingToggle.setToolTipText(BaseMessages.getString(PKG, "AiAdvisor.Sharing.Toggle.Tooltip"));
    wSharingToggle.addListener(SWT.Selection, e -> toggleSharingPanel());
    wSharingToggle.setLayoutData(new FormDataBuilder().left().top().result());

    wlSharing = new Label(sharingHeader, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlSharing);
    wlSharing.setToolTipText(BaseMessages.getString(PKG, "AiAdvisor.Sharing.Toggle.Tooltip"));
    wlSharing.setLayoutData(
        new FormDataBuilder()
            .left(wSharingToggle, margin)
            .right()
            .top(wSharingToggle, 0, SWT.CENTER)
            .result());
    wlSharing.addListener(SWT.MouseDown, e -> toggleSharingPanel());

    inclusionsComposite = new Composite(sharingPanel, SWT.NONE);
    PropsUi.setLook(inclusionsComposite);
    RowLayout inclusionLayout = new RowLayout(SWT.HORIZONTAL);
    inclusionLayout.wrap = true;
    inclusionLayout.pack = true;
    inclusionLayout.spacing = margin;
    inclusionLayout.marginLeft = 0;
    inclusionLayout.marginTop = 0;
    inclusionLayout.marginRight = 0;
    inclusionLayout.marginBottom = 0;
    inclusionsComposite.setLayout(inclusionLayout);
    inclusionsFormData = new FormDataBuilder().left().right().top(sharingHeader, margin).result();
    inclusionsComposite.setLayoutData(inclusionsFormData);
    addListener(
        SWT.Resize,
        e -> {
          if (inclusionsComposite != null && !inclusionsComposite.isDisposed()) {
            inclusionsComposite.layout(true, true);
          }
          layout(true, true);
        });
    applySharingPanelExpanded();
    updateSharingSummary();
    return sharingPanel;
  }

  private void toggleSharingPanel() {
    sharingExpanded = !sharingExpanded;
    applySharingPanelExpanded();
  }

  private void applySharingPanelExpanded() {
    if (inclusionsComposite == null || inclusionsComposite.isDisposed()) {
      return;
    }
    inclusionsComposite.setVisible(sharingExpanded);
    inclusionsFormData.height = sharingExpanded ? SWT.DEFAULT : 0;
    inclusionsFormData.top =
        new FormAttachment(sharingHeader, sharingExpanded ? PropsUi.getMargin() : 0);
    updateSharingTwistie();
    sharingPanel.layout(true, true);
    layout(true, true);
  }

  private void updateSharingTwistie() {
    if (wSharingToggle == null || wSharingToggle.isDisposed()) {
      return;
    }
    GuiResource gui = GuiResource.getInstance();
    String image = sharingExpanded ? "ui/images/arrow-down.svg" : "ui/images/arrow-right.svg";
    wSharingToggle.setImage(gui.getImage(image, ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
    wSharingToggle.setToolTipText(
        BaseMessages.getString(
            PKG,
            sharingExpanded
                ? "AiAdvisor.Sharing.Collapse.Tooltip"
                : "AiAdvisor.Sharing.Expand.Tooltip"));
  }

  void updateSharingSummary() {
    if (wlSharing == null || wlSharing.isDisposed()) {
      return;
    }
    List<String> parts = new ArrayList<>();
    parts.add(BaseMessages.getString(PKG, "AiAdvisor.Sharing.Question"));
    IAiAdvisor advisor = loadSelectedAdvisor();
    if (advisor != null) {
      for (String baseline : advisor.listBaselineSharing()) {
        if (!Utils.isEmpty(baseline)) {
          parts.add(baseline);
        }
      }
    }
    for (AiAdvisorInclusion inclusion : currentInclusions) {
      if (!inclusionEnabled(inclusion.getId())) {
        continue;
      }
      parts.add(summaryFor(inclusion));
    }
    wlSharing.setText(
        formatSharingLine(BaseMessages.getString(PKG, "AiAdvisor.Sharing.Prefix"), parts));
    sharingHeader.layout(true, true);
  }

  static String formatSharingLine(String prefix, List<String> parts) {
    String head = prefix != null ? prefix : "";
    if (parts == null || parts.isEmpty()) {
      return head.trim();
    }
    return head + String.join(", ", parts);
  }

  private boolean inclusionEnabled(String id) {
    if (session != null && session.getInclusions().containsKey(id)) {
      return Boolean.TRUE.equals(session.getInclusions().get(id));
    }
    Button check = inclusionButtons.get(id);
    return check != null && check.getSelection();
  }

  private String summaryFor(AiAdvisorInclusion inclusion) {
    if (AiAdvisorInclusions.METADATA.equals(inclusion.getId())) {
      int count =
          session != null && session.getMetadataSelections() != null
              ? session.getMetadataSelections().size()
              : 0;
      if (count > 0) {
        return BaseMessages.getString(
            PKG, "AiAdvisor.Sharing.MetadataCount", Integer.toString(count));
      }
    }
    if (inclusion.isPicker() && !AiAdvisorInclusions.METADATA.equals(inclusion.getId())) {
      int count =
          session != null
              ? session.getInclusionSelections().getOrDefault(inclusion.getId(), List.of()).size()
              : 0;
      if (count > 0) {
        String phrase =
            !Utils.isEmpty(inclusion.getSummary()) ? inclusion.getSummary() : inclusion.getLabel();
        return BaseMessages.getString(
            PKG, "AiAdvisor.Sharing.InclusionCount", Integer.toString(count), phrase);
      }
    }
    if (!Utils.isEmpty(inclusion.getSummary())) {
      return inclusion.getSummary();
    }
    return inclusion.getLabel();
  }

  public void showSession(AiAdvisorSession session) {
    this.session = session;
    updatingUi = true;
    try {
      reloadAdvisors();
      reloadProviders();
      if (session == null) {
        setEnabled(false);
        wlStatus.setText(BaseMessages.getString(PKG, "AiAdvisor.Status.NoSession"));
        transcript.showSession(null);
        updateSendButton();
        return;
      }
      setEnabled(true);
      selectAdvisor(session.getAdvisorPluginId());
      reloadScenariosAndInclusions();
      selectScenario(session.getScenarioId());
      applyInclusionsFromSession();
      updateMetadataSelectButton();
      updateInclusionPickerButtons();
      wlStatus.setText(Const.NVL(session.getStatusMessage(), ""));
      transcript.showSession(session, this::reviewProposalsForTurn);
      updateSendButton();
    } finally {
      updatingUi = false;
    }
  }

  private void advisorChanged() {
    if (updatingUi || session == null) {
      return;
    }
    IPlugin plugin = selectedAdvisorPlugin();
    String pluginId = plugin != null ? plugin.getIds()[0] : "";
    if (Objects.equals(session.getAdvisorPluginId(), pluginId)) {
      return;
    }
    session.setAdvisorPluginId(pluginId);
    updatingUi = true;
    try {
      reloadScenariosAndInclusions();
    } finally {
      updatingUi = false;
    }
    store.fireChanged();
  }

  private void reloadAdvisors() {
    advisorPlugins.clear();
    String location = session != null ? session.getLocation() : AiAdvisorLocations.PERSPECTIVE;
    advisorPlugins.addAll(AiAdvisorPlugins.listForLocation(location));
    String[] names = new String[advisorPlugins.size()];
    for (int i = 0; i < advisorPlugins.size(); i++) {
      names[i] = advisorPlugins.get(i).getName();
    }
    wAdvisor.setItems(names);
    if (session != null && !advisorPlugins.isEmpty()) {
      boolean present = false;
      String current = session.getAdvisorPluginId();
      for (IPlugin plugin : advisorPlugins) {
        if (plugin.getIds()[0].equals(current)) {
          present = true;
          break;
        }
      }
      if (!present) {
        session.setAdvisorPluginId(advisorPlugins.get(0).getIds()[0]);
      }
    } else if (session != null && advisorPlugins.isEmpty()) {
      session.setAdvisorPluginId("");
    }
  }

  private void reloadProviders() {
    try {
      wProvider.fillItems();
    } catch (HopException e) {
      // Combo stays empty; Send explains.
    }
    HopAiConfig config = HopAiConfigSingleton.getConfig();
    if (session != null && Utils.isEmpty(session.getProviderName())) {
      session.setProviderName(Const.NVL(config.getDefaultProviderName(), ""));
    }
    if (session != null && !Utils.isEmpty(session.getProviderName())) {
      wProvider.setText(session.getProviderName());
    } else if (wProvider.getItemCount() == 1) {
      wProvider.select(0);
    }
  }

  private void reloadScenariosAndInclusions() {
    wScenario.setItems(new String[0]);
    for (Control child : inclusionsComposite.getChildren()) {
      child.dispose();
    }
    inclusionButtons.clear();
    inclusionPickers.clear();
    currentInclusions.clear();
    wMetadataSelect = null;
    IAiAdvisor advisor = loadSelectedAdvisor();
    if (advisor == null) {
      updateSharingSummary();
      applySharingPanelExpanded();
      return;
    }
    List<AiAdvisorScenario> scenarios = advisor.listScenarios();
    String[] labels = new String[scenarios.size()];
    for (int i = 0; i < scenarios.size(); i++) {
      labels[i] = scenarios.get(i).getLabel();
    }
    wScenario.setItems(labels);
    if (session != null && Utils.isEmpty(session.getScenarioId()) && !scenarios.isEmpty()) {
      session.setScenarioId(scenarios.get(0).getId());
    }

    currentInclusions.addAll(advisor.listInclusions());
    for (AiAdvisorInclusion inclusion : currentInclusions) {
      Button check = new Button(inclusionsComposite, SWT.CHECK);
      check.setText(inclusion.getLabel());
      check.setToolTipText(
          Utils.isEmpty(inclusion.getDescription())
              ? inclusion.getLabel()
              : inclusion.getDescription());
      PropsUi.setLook(check);
      boolean selected =
          session != null && session.getInclusions().containsKey(inclusion.getId())
              ? Boolean.TRUE.equals(session.getInclusions().get(inclusion.getId()))
              : inclusion.isDefaultSelected();
      check.setSelection(selected);
      String id = inclusion.getId();
      check.addListener(
          SWT.Selection,
          e -> {
            if (session != null) {
              session.getInclusions().put(id, check.getSelection());
            }
            if (check.getSelection() && session != null) {
              if (AiAdvisorInclusions.METADATA.equals(id)
                  && (session.getMetadataSelections() == null
                      || session.getMetadataSelections().isEmpty())) {
                openMetadataPicker();
              } else if (inclusion.isPicker()
                  && !AiAdvisorInclusions.METADATA.equals(id)
                  && session.getInclusionSelections().getOrDefault(id, List.of()).isEmpty()) {
                openInclusionPicker(inclusion);
              }
            }
            updateMetadataSelectButton();
            updateInclusionPickerButtons();
            updateSharingSummary();
          });
      inclusionButtons.put(id, check);
      if (session != null) {
        session.getInclusions().putIfAbsent(id, selected);
      }
      if (AiAdvisorInclusions.METADATA.equals(id)) {
        wMetadataSelect = new Button(inclusionsComposite, SWT.PUSH);
        wMetadataSelect.setToolTipText(
            BaseMessages.getString(PKG, "AiAdvisor.Metadata.Select.Tooltip"));
        PropsUi.setLook(wMetadataSelect);
        wMetadataSelect.addListener(SWT.Selection, e -> openMetadataPicker());
      } else if (inclusion.isPicker()) {
        Button picker = new Button(inclusionsComposite, SWT.PUSH);
        picker.setToolTipText(inclusion.getDescription());
        PropsUi.setLook(picker);
        picker.addListener(SWT.Selection, e -> openInclusionPicker(inclusion));
        inclusionPickers.put(id, picker);
      }
    }
    updateMetadataSelectButton();
    updateInclusionPickerButtons();
    updateSharingSummary();
    applySharingPanelExpanded();
  }

  private void openMetadataPicker() {
    if (session == null) {
      return;
    }
    AiAdvisorMetadataSelectionDialog dialog =
        new AiAdvisorMetadataSelectionDialog(
            getShell(), host.getMetadataProvider(), session.getMetadataSelections());
    List<AiAdvisorMetadataSelection> selected = dialog.open();
    if (selected == null) {
      Button check = inclusionButtons.get(AiAdvisorInclusions.METADATA);
      if (check != null
          && check.getSelection()
          && (session.getMetadataSelections() == null
              || session.getMetadataSelections().isEmpty())) {
        check.setSelection(false);
        session.getInclusions().put(AiAdvisorInclusions.METADATA, false);
      }
      updateSharingSummary();
      return;
    }
    session.setMetadataSelections(new ArrayList<>(selected));
    Button check = inclusionButtons.get(AiAdvisorInclusions.METADATA);
    boolean send = !selected.isEmpty();
    if (check != null) {
      check.setSelection(send);
    }
    session.getInclusions().put(AiAdvisorInclusions.METADATA, send);
    store.fireChanged();
    updateMetadataSelectButton();
    updateSharingSummary();
  }

  private void openInclusionPicker(AiAdvisorInclusion inclusion) {
    if (session == null || inclusion == null) {
      return;
    }
    IAiAdvisor advisor = loadSelectedAdvisor();
    if (advisor == null) {
      return;
    }
    List<AiAdvisorInclusionChoice> choices =
        advisor.listInclusionChoices(inclusion.getId(), pickerRequest());
    if (choices == null || choices.isEmpty()) {
      wlStatus.setText(BaseMessages.getString(PKG, "AiAdvisor.Inclusion.Select.Empty"));
      Button check = inclusionButtons.get(inclusion.getId());
      if (check != null) {
        check.setSelection(false);
      }
      session.getInclusions().put(inclusion.getId(), false);
      updateSharingSummary();
      return;
    }
    String[] labels = labelsOf(choices);
    EnterSelectionDialog dialog =
        new EnterSelectionDialog(
            getShell(),
            labels,
            BaseMessages.getString(PKG, "AiAdvisor.Inclusion.Select.Title"),
            BaseMessages.getString(PKG, "AiAdvisor.Inclusion.Select.Message"));
    dialog.setMulti(inclusion.isMultiSelect());
    List<String> previously =
        session.getInclusionSelections().getOrDefault(inclusion.getId(), List.of());
    dialog.setSelectedNrs(indexesOf(choices, previously));
    String result = dialog.open();
    if (result == null) {
      Button check = inclusionButtons.get(inclusion.getId());
      if (check != null
          && check.getSelection()
          && session
              .getInclusionSelections()
              .getOrDefault(inclusion.getId(), List.of())
              .isEmpty()) {
        check.setSelection(false);
        session.getInclusions().put(inclusion.getId(), false);
      }
      updateSharingSummary();
      return;
    }
    List<String> ids = selectedChoiceIds(choices, dialog.getSelectionIndeces());
    session.getInclusionSelections().put(inclusion.getId(), ids);
    Button check = inclusionButtons.get(inclusion.getId());
    boolean send = !ids.isEmpty();
    if (check != null) {
      check.setSelection(send);
    }
    session.getInclusions().put(inclusion.getId(), send);
    store.fireChanged();
    updateInclusionPickerButtons();
    updateSharingSummary();
  }

  private AiAdvisorRequest pickerRequest() {
    AiAdvisorRequest request = new AiAdvisorRequest();
    request.setArtifact(session.getArtifact());
    request.setVariables(host.getVariables());
    request.setMetadataProvider(host.getMetadataProvider());
    request.setLocation(session.getLocation());
    request.setAttributes(
        session.getAttributes() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(session.getAttributes()));
    return request;
  }

  static String[] labelsOf(List<AiAdvisorInclusionChoice> choices) {
    if (choices == null || choices.isEmpty()) {
      return new String[0];
    }
    String[] labels = new String[choices.size()];
    for (int i = 0; i < choices.size(); i++) {
      String label = choices.get(i).getLabel();
      labels[i] = Utils.isEmpty(label) ? Const.NVL(choices.get(i).getId(), "") : label;
    }
    return labels;
  }

  static List<String> selectedChoiceIds(List<AiAdvisorInclusionChoice> choices, int[] indexes) {
    List<String> ids = new ArrayList<>();
    if (choices == null || indexes == null) {
      return ids;
    }
    for (int index : indexes) {
      if (index >= 0 && index < choices.size() && !Utils.isEmpty(choices.get(index).getId())) {
        ids.add(choices.get(index).getId());
      }
    }
    return ids;
  }

  static int[] indexesOf(List<AiAdvisorInclusionChoice> choices, List<String> ids) {
    if (choices == null || ids == null || ids.isEmpty()) {
      return new int[0];
    }
    List<Integer> indexes = new ArrayList<>();
    for (int i = 0; i < choices.size(); i++) {
      if (ids.contains(choices.get(i).getId())) {
        indexes.add(i);
      }
    }
    int[] result = new int[indexes.size()];
    for (int i = 0; i < indexes.size(); i++) {
      result[i] = indexes.get(i);
    }
    return result;
  }

  private void updateInclusionPickerButtons() {
    for (Map.Entry<String, Button> entry : inclusionPickers.entrySet()) {
      Button button = entry.getValue();
      if (button == null || button.isDisposed()) {
        continue;
      }
      int count =
          session != null
              ? session.getInclusionSelections().getOrDefault(entry.getKey(), List.of()).size()
              : 0;
      if (count == 0) {
        button.setText(BaseMessages.getString(PKG, "AiAdvisor.Inclusion.Select.Label"));
      } else {
        button.setText(
            BaseMessages.getString(
                PKG, "AiAdvisor.Inclusion.Select.Count", Integer.toString(count)));
      }
    }
  }

  private void updateMetadataSelectButton() {
    if (wMetadataSelect == null || wMetadataSelect.isDisposed()) {
      return;
    }
    int count =
        session != null && session.getMetadataSelections() != null
            ? session.getMetadataSelections().size()
            : 0;
    if (count == 0) {
      wMetadataSelect.setText(BaseMessages.getString(PKG, "AiAdvisor.Metadata.Select.Label"));
    } else {
      wMetadataSelect.setText(
          BaseMessages.getString(PKG, "AiAdvisor.Metadata.Select.Count", Integer.toString(count)));
    }
  }

  private void applyInclusionsFromSession() {
    if (session == null) {
      return;
    }
    for (Map.Entry<String, Button> entry : inclusionButtons.entrySet()) {
      Boolean value = session.getInclusions().get(entry.getKey());
      if (value != null) {
        entry.getValue().setSelection(value);
      }
    }
  }

  private void onSendOrCancel() {
    if (session != null && session.isWorking()) {
      cancelInFlight();
      return;
    }
    send();
  }

  private void cancelInFlight() {
    if (session == null || !session.isWorking()) {
      return;
    }
    session.requestCancel();
    session.setStatusMessage(BaseMessages.getString(PKG, "AiAdvisor.Status.Cancelled"));
    wlStatus.setText(session.getStatusMessage());
  }

  private void updateSendButton() {
    GuiResource gui = GuiResource.getInstance();
    boolean working = session != null && session.isWorking();
    if (working) {
      wSend.setImage(
          gui.getImage("ui/images/stop.svg", ConstUi.LARGE_ICON_SIZE, ConstUi.LARGE_ICON_SIZE));
      wSend.setToolTipText(BaseMessages.getString(PKG, "AiAdvisor.Send.Stop.Tooltip"));
      wSend.setEnabled(true);
    } else {
      wSend.setImage(
          gui.getImage(
              "ui/images/logo_icon.svg", ConstUi.LARGE_ICON_SIZE, ConstUi.LARGE_ICON_SIZE));
      wSend.setToolTipText(BaseMessages.getString(PKG, "AiAdvisor.Send.Tooltip"));
      wSend.setEnabled(HopAiConfigSingleton.getConfig().isAiEnabled() && session != null);
    }
  }

  private void send() {
    if (session == null || session.isWorking()) {
      return;
    }
    HopAiConfig config = HopAiConfigSingleton.getConfig();
    if (!config.isAiEnabled()) {
      wlStatus.setText(BaseMessages.getString(PKG, "AiAdvisor.Status.Disabled"));
      return;
    }
    String prompt = wPrompt.getText();
    if (Utils.isEmpty(prompt)) {
      return;
    }
    IAiAdvisor advisor = loadSelectedAdvisor();
    if (advisor == null) {
      wlStatus.setText(BaseMessages.getString(PKG, "AiAdvisor.Status.NoAdvisor"));
      return;
    }
    session.setAdvisorPluginId(selectedAdvisorId());
    session.setScenarioId(selectedScenarioId());
    session.setProviderName(wProvider.getText());
    for (Map.Entry<String, Button> entry : inclusionButtons.entrySet()) {
      session.getInclusions().put(entry.getKey(), entry.getValue().getSelection());
    }

    AiAdvisorTurn turn = new AiAdvisorTurn();
    turn.setUserPrompt(prompt);
    session.addTurn(turn);
    session.setCancelled(false);
    session.setWorking(true);
    session.setStatusMessage(BaseMessages.getString(PKG, "AiAdvisor.Status.Working"));
    wPrompt.setText("");
    updateSendButton();
    wlStatus.setText(session.getStatusMessage());
    transcript.showSession(session, this::reviewProposalsForTurn);
    store.fireChanged();

    AiAdvisorSession target = session;
    // Log widgets are SWT; read them on this UI thread before the background worker starts.
    final String logExcerpt =
        target.getLogSupplier() != null ? target.getLogSupplier().get() : null;
    Thread worker =
        BackgroundThreadFacade.start(
            () -> {
              try {
                AiAdvisorResponse response =
                    AiAdvisorEngine.advise(
                        target,
                        advisor,
                        host.getVariables(),
                        host.getMetadataProvider(),
                        logExcerpt);
                host.asyncExec(() -> completeTurn(target, turn, response, null));
              } catch (Throwable t) {
                if (t instanceof InterruptedException) {
                  Thread.currentThread().interrupt();
                }
                host.asyncExec(() -> completeTurn(target, turn, null, t));
              }
            },
            "hop-ai-advisor");
    target.setWorkerThread(worker);
  }

  private void completeTurn(
      AiAdvisorSession target, AiAdvisorTurn turn, AiAdvisorResponse response, Throwable error) {
    boolean cancelled = target.isCancelled();
    target.setWorking(false);
    target.setWorkerThread(null);
    if (cancelled) {
      target.setStatusMessage(BaseMessages.getString(PKG, "AiAdvisor.Status.Cancelled"));
    } else if (error != null) {
      String message = userVisibleError(error);
      turn.setErrorMessage(message);
      target.setStatusMessage(message);
    } else if (response != null) {
      turn.setAssistantAdvice(response.getMarkdownAdvice());
      turn.setProposalBlockPresent(response.isProposalBlockPresent());
      if (response.getProposals() != null) {
        turn.getProposals().addAll(response.getProposals());
      }
      if (turn.getProposals().isEmpty()) {
        target.setStatusMessage("");
      } else {
        target.setStatusMessage(BaseMessages.getString(PKG, "AiAdvisor.Status.WithProposals"));
      }
    }
    if (session == target) {
      wlStatus.setText(Const.NVL(target.getStatusMessage(), ""));
      transcript.showSession(target, this::reviewProposalsForTurn);
      updateSendButton();
    }
    store.fireChanged();
    if (error != null && !cancelled) {
      new ErrorDialog(
          host.getShell(),
          BaseMessages.getString(PKG, "AiAdvisor.Send.Error.Title"),
          BaseMessages.getString(PKG, "AiAdvisor.Send.Error.Message"),
          error);
    }
  }

  private void reviewProposalsForTurn(int turnIndex) {
    if (session == null || turnIndex < 0 || turnIndex >= session.getTurns().size()) {
      return;
    }
    AiAdvisorTurn turn = session.getTurns().get(turnIndex);
    List<AiProposal> proposals = turn.getProposals();
    if (proposals == null || proposals.isEmpty()) {
      return;
    }
    IAiAdvisor advisor = loadSelectedAdvisor();
    if (advisor == null) {
      wlStatus.setText(BaseMessages.getString(PKG, "AiAdvisor.Status.NoAdvisor"));
      return;
    }
    AiAdvisorRequest request = new AiAdvisorRequest();
    request.setArtifact(session.getArtifact());
    request.setVariables(host.getVariables());
    request.setMetadataProvider(host.getMetadataProvider());
    request.setLocation(session.getLocation());
    request.setAttributes(
        session.getAttributes() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(session.getAttributes()));
    request.getAttributes().put(AiM2PromptSupport.ATTR_HOP_GUI, host.getHopGui());

    List<AiProposalValidation> validation = advisor.validateProposals(request, proposals);
    AiAdvisorProposalReviewDialog reviewDialog =
        new AiAdvisorProposalReviewDialog(
            host.getShell(), proposals, validation, AiProposalPreview::format);
    if (!reviewDialog.open()) {
      return;
    }
    List<AiProposal> selected = reviewDialog.getSelectedProposals();
    if (selected.isEmpty()) {
      return;
    }
    try {
      advisor.applyProposals(request, selected);
      session.recordApplied(turn, selected, advisor);
      advisor.afterApply(request, selected);
      refreshBoundGraph();
      session.setStatusMessage(
          BaseMessages.getString(PKG, "AiAdvisor.Transcript.Applied", selected.size()));
      wlStatus.setText(Const.NVL(session.getStatusMessage(), ""));
      transcript.showSession(session, this::reviewProposalsForTurn);
      store.fireChanged();
    } catch (Exception ex) {
      new ErrorDialog(
          host.getShell(),
          BaseMessages.getString(PKG, "AiAdvisor.Apply.Error.Title"),
          BaseMessages.getString(PKG, "AiAdvisor.Apply.Error.Message"),
          ex instanceof HopException ? ex : new HopException(ex));
    }
  }

  private void refreshBoundGraph() {
    if (host.getHopGui() == null || session == null || session.getArtifact() == null) {
      return;
    }
    ExplorerPerspective explorer =
        host.getHopGui().getPerspectiveManager().findPerspective(ExplorerPerspective.class);
    if (explorer == null || explorer.getItems() == null) {
      return;
    }
    Object artifact = session.getArtifact();
    for (TabItemHandler item : explorer.getItems()) {
      if (item.getTypeHandler() instanceof HopGuiPipelineGraph graph
          && graph.getPipelineMeta() == artifact) {
        graph.setChanged();
        graph.updateGui();
      } else if (item.getTypeHandler() instanceof HopGuiWorkflowGraph graph
          && graph.getWorkflowMeta() == artifact) {
        graph.setChanged();
        graph.updateGui();
      }
    }
  }

  private void selectAdvisor(String pluginId) {
    if (Utils.isEmpty(pluginId)) {
      if (wAdvisor.getItemCount() > 0) {
        wAdvisor.select(0);
      }
      return;
    }
    for (int i = 0; i < advisorPlugins.size(); i++) {
      if (pluginId.equals(advisorPlugins.get(i).getIds()[0])) {
        wAdvisor.select(i);
        return;
      }
    }
  }

  private void selectScenario(String scenarioId) {
    IAiAdvisor advisor = loadSelectedAdvisor();
    if (advisor == null) {
      return;
    }
    List<AiAdvisorScenario> scenarios = advisor.listScenarios();
    for (int i = 0; i < scenarios.size(); i++) {
      if (scenarios.get(i).getId().equals(scenarioId) || Utils.isEmpty(scenarioId) && i == 0) {
        wScenario.select(i);
        if (session != null) {
          session.setScenarioId(scenarios.get(i).getId());
        }
        return;
      }
    }
  }

  private IPlugin selectedAdvisorPlugin() {
    int index = wAdvisor.getSelectionIndex();
    if (index < 0 || index >= advisorPlugins.size()) {
      return advisorPlugins.isEmpty() ? null : advisorPlugins.get(0);
    }
    return advisorPlugins.get(index);
  }

  private String selectedAdvisorId() {
    IPlugin plugin = selectedAdvisorPlugin();
    return plugin == null ? "" : plugin.getIds()[0];
  }

  private String selectedScenarioId() {
    IAiAdvisor advisor = loadSelectedAdvisor();
    if (advisor == null) {
      return "";
    }
    List<AiAdvisorScenario> scenarios = advisor.listScenarios();
    int index = wScenario.getSelectionIndex();
    if (index < 0 || index >= scenarios.size()) {
      return scenarios.isEmpty() ? "" : scenarios.get(0).getId();
    }
    return scenarios.get(index).getId();
  }

  private IAiAdvisor loadSelectedAdvisor() {
    try {
      return AiAdvisorPlugins.load(selectedAdvisorId());
    } catch (HopException e) {
      return null;
    }
  }
}
