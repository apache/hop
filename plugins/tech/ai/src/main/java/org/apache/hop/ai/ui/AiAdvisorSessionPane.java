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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
import org.apache.hop.ai.advisors.pipeline.PipelineAiAdvisor;
import org.apache.hop.ai.advisors.workflow.WorkflowAiAdvisor;
import org.apache.hop.ai.config.AiRequestOnClose;
import org.apache.hop.ai.config.HopAiConfig;
import org.apache.hop.ai.config.HopAiConfigSingleton;
import org.apache.hop.ai.engine.AiAdvisorEngine;
import org.apache.hop.ai.engine.AiAdvisorExtraContext;
import org.apache.hop.ai.engine.AiClipboardProposals;
import org.apache.hop.ai.engine.AiMetadataBackup;
import org.apache.hop.ai.engine.AiMetadataProposalSupport;
import org.apache.hop.ai.engine.AiProposalPreview;
import org.apache.hop.ai.engine.AiProposalTypes;
import org.apache.hop.ai.engine.AiUserException;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorSessionStore;
import org.apache.hop.ai.session.AiAdvisorTurn;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.apache.hop.ui.hopgui.BackgroundThreadFacade;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.shared.HopGuiAbstractGraph;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.apache.hop.ui.util.HelpUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
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
  private Composite questionArea;

  /** Position in the earlier questions while browsing with Up and Down; -1 is the draft. */
  private int historyIndex = -1;

  private String historyDraft = "";
  private boolean showingHistory;
  private FormData questionData;
  private FormData statusData;
  private Link wFocus;
  private FormData focusData;
  private AiAdvisorTranscriptPanel transcript;
  private Text wPrompt;
  private Button wSend;
  private Button wMetadataSelect;
  private final List<IPlugin> advisorPlugins = new ArrayList<>();
  private final SendShortcutGuard sendShortcutGuard = new SendShortcutGuard();

  /** Sessions with a question sent from this view, for {@link #cancelWhenClosed()}. */
  private final Set<AiAdvisorSession> sentFromHere =
      Collections.newSetFromMap(new IdentityHashMap<>());

  private boolean updatingUi;

  public AiAdvisorSessionPane(Composite parent, IAiAdvisorWorkbenchHost host) {
    this(parent, host, AiAdvisorSessionStore.get(host.getHopGui()));
  }

  AiAdvisorSessionPane(
      Composite parent, IAiAdvisorWorkbenchHost host, AiAdvisorSessionStore store) {
    super(parent, SWT.NONE);
    this.host = host;
    this.store = store;
    PropsUi.setLook(this);
    setLayout(PropsUi.getInstance().createFormLayout());
    int margin = PropsUi.getMargin();
    GuiResource gui = GuiResource.getInstance();

    Control top = createHeader(margin);

    // Question at the bottom, transcript above it. The question field grows with its text, from
    // three lines up to twelve or 40% of the pane, like the input of most chat tools; past that it
    // scrolls. When space is short, as in a low bottom dock, the transcript keeps a few lines and
    // the question field gives way, down to one line.
    questionArea = new Composite(this, SWT.NONE);
    questionData = new FormDataBuilder().left().right().bottom().result();
    questionArea.setLayoutData(questionData);

    transcript = new AiAdvisorTranscriptPanel(this);
    transcript.setUndoMetadata(this::undoMetadataForTurn);
    transcript.setLayoutData(
        new FormDataBuilder()
            .left()
            .right()
            .top(top, margin)
            .bottom(questionArea, -margin)
            .result());

    PropsUi.setLook(questionArea);
    FormLayout questionLayout = new FormLayout();
    questionLayout.marginTop = margin;
    questionArea.setLayout(questionLayout);

    // Messages about the question (what is missing, what went wrong) sit right above it.
    wlStatus = new Label(questionArea, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlStatus);
    statusData = new FormDataBuilder().left().right().top().result();
    wlStatus.setLayoutData(statusData);

    wSend = new Button(questionArea, SWT.PUSH | SWT.FLAT);
    wSend.setImage(
        gui.getImage("ui/images/logo_icon.svg", ConstUi.LARGE_ICON_SIZE, ConstUi.LARGE_ICON_SIZE));
    wSend.setToolTipText(BaseMessages.getString(PKG, "AiAdvisor.Send.Tooltip"));
    wSend.addListener(SWT.Selection, e -> onSendOrCancel());

    wPrompt = new Text(questionArea, SWT.MULTI | SWT.WRAP | SWT.BORDER | SWT.V_SCROLL);
    applyPromptFieldLook(wPrompt);
    wPrompt.setMessage(BaseMessages.getString(PKG, "AiAdvisor.Prompt.Message"));
    if (!EnvironmentUtils.getInstance().isWeb()) {
      wPrompt.addPaintListener(e -> paintPromptHint(wPrompt, e));
    }
    wPrompt.setLayoutData(
        new FormDataBuilder().left().right(wSend, -margin).top(wlStatus, margin).bottom().result());
    installSendShortcut(wPrompt);
    wPrompt.addListener(SWT.KeyDown, this::browseHistory);
    wPrompt.addListener(
        SWT.Modify,
        e -> {
          if (!showingHistory) {
            // Typing makes this the draft again; Up starts from the newest question.
            historyIndex = -1;
          }
        });
    wSend.setLayoutData(new FormDataBuilder().right().top(wPrompt, 0, SWT.CENTER).result());

    wPrompt.addListener(SWT.Modify, e -> sizeQuestionArea());
    addListener(SWT.Resize, e -> sizeQuestionArea());
    addListener(SWT.Dispose, e -> cancelWhenClosed());
    setStatus("");
  }

  /**
   * The window or dock closes, or Hop GUI does. With "Cancel the question" configured, questions
   * sent from here that still wait for their answer are cancelled. By default they finish in the
   * background and their answer is recorded in the session. Moving the assistant with Float or Dock
   * is not a close: the sessions go on in the new view.
   */
  private void cancelWhenClosed() {
    boolean cancelled =
        cancelOnClose(
            sentFromHere,
            HopAiConfigSingleton.getConfig().getRequestOnClose(),
            AiAdvisorViews.isMoving());
    sentFromHere.clear();
    if (cancelled) {
      store.fireChanged();
    }
  }

  /**
   * Cancel the questions of these sessions that still wait for an answer, when the configuration
   * says so and the view is not just moving.
   *
   * @return whether a question was cancelled
   */
  static boolean cancelOnClose(
      Collection<AiAdvisorSession> sessions, AiRequestOnClose onClose, boolean moving) {
    if (moving || onClose != AiRequestOnClose.CANCEL) {
      return false;
    }
    boolean cancelled = false;
    for (AiAdvisorSession sent : sessions) {
      if (sent.isWorking()) {
        sent.requestCancel();
        List<AiAdvisorTurn> turns = sent.getTurns();
        if (!turns.isEmpty()) {
          recordResult(sent, turns.get(turns.size() - 1), null, null, true);
        }
        cancelled = true;
      }
    }
    return cancelled;
  }

  /**
   * Up on the first line shows the previous question of this session, Down on the last line the
   * next one, and past the newest the text that was being typed. As in shells and chat tools.
   */
  private void browseHistory(Event event) {
    if (session == null
        || (event.stateMask & SWT.MODIFIER_MASK) != 0
        || (event.keyCode != SWT.ARROW_UP && event.keyCode != SWT.ARROW_DOWN)) {
      return;
    }
    List<String> questions = earlierQuestions(session);
    if (questions.isEmpty()) {
      return;
    }
    if (event.keyCode == SWT.ARROW_UP) {
      if (wPrompt.getCaretLineNumber() != 0 || historyIndex >= questions.size() - 1) {
        return;
      }
      if (historyIndex < 0) {
        historyDraft = wPrompt.getText();
      }
      historyIndex++;
      showQuestion(questions.get(historyIndex));
    } else {
      if (historyIndex < 0 || wPrompt.getCaretLineNumber() != wPrompt.getLineCount() - 1) {
        return;
      }
      historyIndex--;
      showQuestion(historyIndex < 0 ? historyDraft : questions.get(historyIndex));
    }
    event.doit = false;
  }

  /** The questions of a session, newest first, without repeats of the same text in a row. */
  static List<String> earlierQuestions(AiAdvisorSession session) {
    List<String> questions = new ArrayList<>();
    List<AiAdvisorTurn> turns = session.getTurns();
    for (int i = turns.size() - 1; i >= 0; i--) {
      String question = turns.get(i).getUserPrompt();
      if (!Utils.isEmpty(question)
          && (questions.isEmpty() || !questions.get(questions.size() - 1).equals(question))) {
        questions.add(question);
      }
    }
    return questions;
  }

  private void showQuestion(String text) {
    showingHistory = true;
    try {
      wPrompt.setText(Const.NVL(text, ""));
      wPrompt.setSelection(wPrompt.getCharCount());
    } finally {
      showingHistory = false;
    }
  }

  static final int QUESTION_MIN_LINES = 3;
  static final int QUESTION_MAX_LINES = 12;
  static final int QUESTION_MAX_PERCENT = 40;

  /** Lines of the transcript that stay visible when the pane is low. */
  static final int TRANSCRIPT_MIN_LINES = 4;

  /** Fit the question area to its text, within the limits above. */
  void sizeQuestionArea() {
    if (questionArea == null || questionArea.isDisposed() || wPrompt.isDisposed()) {
      return;
    }
    int lineHeight = Math.max(wPrompt.getLineHeight(), 10);
    int trim = wPrompt.computeTrim(0, 0, 0, 0).height;
    int width = Math.max(wPrompt.getSize().x, 100);
    int text = wPrompt.computeSize(width, SWT.DEFAULT).y;
    int status =
        wlStatus.isVisible() ? wlStatus.computeSize(getClientArea().width, SWT.DEFAULT).y : 0;
    int margins = 2 * PropsUi.getMargin();
    int headerBottom =
        sharingPanel != null && !sharingPanel.isDisposed()
            ? sharingPanel.getBounds().y + sharingPanel.getBounds().height + PropsUi.getMargin()
            : 0;
    int prompt =
        promptHeight(
            text,
            lineHeight,
            trim,
            getClientArea().height,
            getClientArea().height - headerBottom - status - margins);
    int height = prompt + status + margins;
    if (questionData.height != height) {
      questionData.height = height;
      layout(true, true);
    }
  }

  /**
   * The height of the question field: its text, between three and twelve lines and at most 40% of
   * the pane, but never so high that the transcript has less than {@link #TRANSCRIPT_MIN_LINES}
   * lines. The field keeps at least one line.
   *
   * @param below the height left under the header for the question field and the transcript
   */
  static int promptHeight(int text, int lineHeight, int trim, int paneHeight, int below) {
    int minimum = QUESTION_MIN_LINES * lineHeight + trim;
    int maximum =
        Math.max(
            minimum,
            Math.min(
                QUESTION_MAX_LINES * lineHeight + trim, paneHeight * QUESTION_MAX_PERCENT / 100));
    int prompt = Math.max(minimum, Math.min(text, maximum));
    if (paneHeight <= 0) {
      // Not laid out yet; sized again on the first resize.
      return prompt;
    }
    int roomLeft = below - TRANSCRIPT_MIN_LINES * lineHeight;
    return Math.max(lineHeight + trim, Math.min(prompt, roomLeft));
  }

  /** A form layout without margins, for panels nested in this one, which has its own. */
  private static FormLayout innerFormLayout() {
    FormLayout layout = new FormLayout();
    layout.marginWidth = 0;
    layout.marginHeight = 0;
    return layout;
  }

  private void updateFocus() {
    if (wFocus == null || wFocus.isDisposed()) {
      return;
    }
    String focus = session != null ? session.getFocusNodeName() : null;
    boolean show = !Utils.isEmpty(focus);
    if (show) {
      String kind =
          "workflow".equals(session.getArtifactKind())
              ? BaseMessages.getString(PKG, "AiAdvisor.Focus.Action")
              : BaseMessages.getString(PKG, "AiAdvisor.Focus.Transform");
      wFocus.setText(
          BaseMessages.getString(PKG, "AiAdvisor.Focus.Label", kind, focus.replace("&", "&&")));
      wFocus.setToolTipText(BaseMessages.getString(PKG, "AiAdvisor.Focus.Tooltip"));
    }
    focusData.height = show ? SWT.DEFAULT : 0;
    wFocus.setVisible(show);
    layout(true, true);
  }

  private void clearFocus() {
    if (session == null) {
      return;
    }
    session.setFocusNodeName("");
    updateFocus();
    updateSharingSummary();
    store.fireChanged();
  }

  /** An empty status line takes no space, so it does not leave a gap above the transcript. */
  private void setStatus(String text) {
    if (wlStatus == null || wlStatus.isDisposed()) {
      return;
    }
    String value = Const.NVL(text, "");
    wlStatus.setText(value);
    statusData.height = value.isEmpty() ? 0 : SWT.DEFAULT;
    wlStatus.setVisible(!value.isEmpty());
    layout(true, true);
    sizeQuestionArea();
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

  /** Whether the error is a problem its message alone explains; see {@link AiUserException}. */
  static boolean isExplained(Throwable error) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (cause instanceof AiUserException) {
        return true;
      }
      if (cause.getCause() == cause) {
        break;
      }
    }
    return false;
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
    // Assistant, scenario and provider share one row, so a small bottom dock keeps room for the
    // conversation.
    wAdvisor.setLayoutData(
        new FormDataBuilder().left(wlAdvisor, margin).top().right(30, -margin).result());
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
    wScenario.setLayoutData(
        new FormDataBuilder().left(wlScenario, margin).top().right(55, -margin).result());
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
    wProvider.setLayoutData(
        new FormDataBuilder()
            .left(wScenario, margin)
            .right()
            .top(wAdvisor, 0, SWT.CENTER)
            .result());
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
          if (providerExists(name)) {
            store.setLastProviderName(name);
          }
          store.fireChanged();
        });

    // Shown when AI Help was opened on a transform or action; its settings go with each question.
    wFocus = new Link(this, SWT.NONE);
    PropsUi.setLook(wFocus);
    focusData = new FormDataBuilder().left().right().top(wProvider, margin).result();
    focusData.height = 0;
    wFocus.setLayoutData(focusData);
    wFocus.setVisible(false);
    wFocus.addListener(SWT.Selection, e -> clearFocus());

    sharingPanel = new Composite(this, SWT.NONE);
    PropsUi.setLook(sharingPanel);
    sharingPanel.setLayout(innerFormLayout());
    sharingPanel.setLayoutData(new FormDataBuilder().left().right().top(wFocus, margin).result());

    sharingHeader = new Composite(sharingPanel, SWT.NONE);
    PropsUi.setLook(sharingHeader);
    sharingHeader.setLayout(innerFormLayout());
    sharingHeader.setLayoutData(new FormDataBuilder().left().right().top().result());
    sharingHeader.setCursor(getDisplay().getSystemCursor(SWT.CURSOR_HAND));
    sharingHeader.addListener(SWT.MouseDown, e -> toggleSharingPanel());

    wSharingToggle = new Button(sharingHeader, SWT.PUSH | SWT.FLAT);
    PropsUi.setLook(wSharingToggle);
    wSharingToggle.setToolTipText(BaseMessages.getString(PKG, "AiAdvisor.Sharing.Toggle.Tooltip"));
    wSharingToggle.addListener(SWT.Selection, e -> toggleSharingPanel());
    wSharingToggle.setLayoutData(new FormDataBuilder().left().top().result());

    // Tooltips cannot hold a link, so the way to the explanation sits on the line itself.
    Link wSharingHelp = new Link(sharingHeader, SWT.NONE);
    wSharingHelp.setText(BaseMessages.getString(PKG, "AiAdvisor.Sharing.Explain.Link"));
    PropsUi.setLook(wSharingHelp);
    wSharingHelp.setLayoutData(
        new FormDataBuilder().right().top(wSharingToggle, 0, SWT.CENTER).result());
    wSharingHelp.addListener(
        SWT.Selection,
        e ->
            HelpUtils.openHelp(
                getShell(),
                Const.getDocUrl("hop-gui/perspective-ai-advisor.html#context-inclusions")));

    wlSharing = new Label(sharingHeader, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wlSharing);
    wlSharing.setToolTipText(BaseMessages.getString(PKG, "AiAdvisor.Sharing.Toggle.Tooltip"));
    wlSharing.setLayoutData(
        new FormDataBuilder()
            .left(wSharingToggle, margin)
            .right(wSharingHelp, -2 * margin)
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
    // What the user chose comes first, so the focused node and the opt-in items stand out. The
    // items that always go along are summed up as "the basics" and listed in the tooltip.
    List<String> chosen = new ArrayList<>();
    Map<String, String> chosenExplanations = new HashMap<>();
    if (session != null && !Utils.isEmpty(session.getFocusNodeName())) {
      String focus =
          BaseMessages.getString(PKG, "AiAdvisor.Sharing.Focus", session.getFocusNodeName());
      chosen.add(focus);
      chosenExplanations.put(focus, BaseMessages.getString(PKG, "AiAdvisor.Sharing.Explain.Focus"));
    }
    for (AiAdvisorInclusion inclusion : currentInclusions) {
      if (!inclusionEnabled(inclusion.getId()) || isBlockedByConfig(inclusion.getId())) {
        continue;
      }
      String summary = summaryFor(inclusion);
      chosen.add(summary);
    }
    List<String> basics = new ArrayList<>();
    basics.add(BaseMessages.getString(PKG, "AiAdvisor.Sharing.Question"));
    IAiAdvisor advisor = loadSelectedAdvisor();
    if (advisor != null) {
      for (String baseline : advisor.listBaselineSharing()) {
        if (!Utils.isEmpty(baseline)) {
          basics.add(baseline);
        }
      }
    }
    if (host != null) {
      for (String extra :
          AiAdvisorExtraContext.sharingPhrases(
              advisor,
              host.getVariables(),
              BaseMessages.getString(PKG, "AiAdvisor.Sharing.ExtraNotes"))) {
        if (!Utils.isEmpty(extra)) {
          basics.add(extra);
        }
      }
    }
    String prefix = BaseMessages.getString(PKG, "AiAdvisor.Sharing.Prefix");
    wlSharing.setText(
        chosen.isEmpty()
            ? prefix + BaseMessages.getString(PKG, "AiAdvisor.Sharing.BasicsOnly")
            : formatSharingLine(prefix, chosen)
                + BaseMessages.getString(PKG, "AiAdvisor.Sharing.Basics"));
    StringBuilder tooltip =
        new StringBuilder(BaseMessages.getString(PKG, "AiAdvisor.Sharing.Tooltip.Header"));
    for (String item : chosen) {
      tooltip.append("\n\u2022 ").append(item);
      String explanation = chosenExplanations.get(item);
      if (!Utils.isEmpty(explanation)) {
        tooltip.append(": ").append(explanation);
      }
    }
    for (String item : basics) {
      tooltip.append("\n\u2022 ").append(item).append(": ").append(explainBasic(item));
    }
    tooltip.append("\n\n").append(BaseMessages.getString(PKG, "AiAdvisor.Sharing.Tooltip.Footer"));
    wlSharing.setToolTipText(tooltip.toString());
    sharingHeader.layout(true, true);
  }

  /** What an item that always goes along is, in a few words, for the Sharing tooltip. */
  static String explainBasic(String item) {
    Map<String, String> known = new HashMap<>();
    known.put(BaseMessages.getString(PKG, "AiAdvisor.Sharing.Question"), "Question");
    known.put(BaseMessages.getString(PKG, "AiAdvisor.Sharing.ExtraNotes"), "ExtraNotes");
    for (Class<?> advisor : new Class<?>[] {PipelineAiAdvisor.class, WorkflowAiAdvisor.class}) {
      String prefix = advisor.getSimpleName();
      known.put(BaseMessages.getString(advisor, prefix + ".Sharing.Baseline"), "Structure");
      known.put(
          BaseMessages.getString(advisor, prefix + ".Sharing.MetadataTypes"), "MetadataTypes");
      known.put(BaseMessages.getString(advisor, prefix + ".Sharing.DatabasePlugins"), "Databases");
    }
    String key = known.get(item);
    if (key != null) {
      return BaseMessages.getString(PKG, "AiAdvisor.Sharing.Explain." + key);
    }
    if ("ai-context.md".equals(item) || item.endsWith("-advisor.md")) {
      return BaseMessages.getString(PKG, "AiAdvisor.Sharing.Explain.PluginNotes", item);
    }
    return BaseMessages.getString(PKG, "AiAdvisor.Sharing.Explain.ContextFile");
  }

  static String formatSharingLine(String prefix, List<String> parts) {
    String head = prefix != null ? prefix : "";
    if (parts == null || parts.isEmpty()) {
      return head.trim();
    }
    return head + String.join(", ", parts);
  }

  /**
   * Full XML also needs Configuration → Plugins → AI Assistant → Allow sending full XML. While that
   * is off the checkbox cannot send anything, so it is disabled and left out of the Sharing line.
   */
  static boolean isBlockedByConfig(String inclusionId) {
    return AiAdvisorInclusions.XML.equals(inclusionId)
        && !HopAiConfigSingleton.getConfig().isAllowSendFullXml();
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
    if (this.session != session) {
      historyIndex = -1;
    }
    this.session = session;
    updatingUi = true;
    try {
      reloadAdvisors();
      reloadProviders();
      if (session == null) {
        // Only the controls are disabled: the hints in the status line and the transcript stay
        // readable.
        setInputEnabled(false);
        updateFocus();
        setStatus(BaseMessages.getString(PKG, "AiAdvisor.Status.NoSession"));
        transcript.showSession(null);
        updateSendButton();
        return;
      }
      setInputEnabled(true);
      selectAdvisor(session.getAdvisorPluginId());
      reloadScenariosAndInclusions();
      selectScenario(session.getScenarioId());
      applyInclusionsFromSession();
      includeLogAfterARun();
      updateFocus();
      updateMetadataSelectButton();
      updateInclusionPickerButtons();
      setStatus(
          Utils.isEmpty(session.getStatusMessage()) && advisorPlugins.isEmpty()
              ? noAdvisorMessage()
              : Const.NVL(session.getStatusMessage(), ""));
      transcript.showSession(session, this::reviewProposalsForTurn);
      updateSendButton();
      startWorkingLine(session);
    } finally {
      updatingUi = false;
    }
  }

  /**
   * While the model works, the line right above the question counts the seconds, so the user sees
   * at the same place every time that the request is alive. It stops when the answer is in, or when
   * this pane shows another session.
   */
  private void startWorkingLine(AiAdvisorSession target) {
    if (target == null || !target.isWorking() || target.getTurns().isEmpty()) {
      return;
    }
    AiAdvisorTurn turn = target.getTurns().get(target.getTurns().size() - 1);
    long started =
        turn.getStartedAtMillis() > 0 ? turn.getStartedAtMillis() : System.currentTimeMillis();
    Runnable tick =
        new Runnable() {
          @Override
          public void run() {
            if (isDisposed() || session != target || !target.isWorking()) {
              return;
            }
            setStatus(
                AiAdvisorTranscriptPanel.workingText(
                    turn, (System.currentTimeMillis() - started) / 1000));
            getDisplay().timerExec(1000, this);
          }
        };
    tick.run();
  }

  /**
   * Once the pipeline or workflow has run, its log is what most questions are about, so the log
   * option switches on by itself, visibly. Not when the user set the option since the latest run:
   * that choice holds until the next run.
   */
  private void includeLogAfterARun() {
    if (session == null) {
      return;
    }
    forgetLogChoiceOfEarlierRun(session);
    if (session.getLogSupplier() == null
        || session.getUserChosenInclusions().contains(AiAdvisorInclusions.LOGS)
        || Boolean.TRUE.equals(session.getInclusions().get(AiAdvisorInclusions.LOGS))) {
      return;
    }
    Button check = inclusionButtons.get(AiAdvisorInclusions.LOGS);
    if (check == null || check.isDisposed()) {
      return;
    }
    String log = session.getLogSupplier().get();
    if (Utils.isEmpty(log) || log.isBlank()) {
      return;
    }
    check.setSelection(true);
    session.getInclusions().put(AiAdvisorInclusions.LOGS, true);
    updateSharingSummary();
  }

  /** A choice for the Logs option made before the latest run no longer holds. */
  static void forgetLogChoiceOfEarlierRun(AiAdvisorSession session) {
    String runId = session.currentRunId();
    if (runId != null
        && session.getUserChosenInclusions().contains(AiAdvisorInclusions.LOGS)
        && !runId.equals(session.getLogChoiceRunId())) {
      session.getUserChosenInclusions().remove(AiAdvisorInclusions.LOGS);
    }
  }

  private void setInputEnabled(boolean enabled) {
    for (Control control : new Control[] {wAdvisor, wScenario, wProvider, sharingPanel, wPrompt}) {
      if (control != null && !control.isDisposed()) {
        control.setEnabled(enabled);
      }
    }
  }

  /** Why no advisor is offered: none is installed, or none works without a pipeline or workflow. */
  private String noAdvisorMessage() {
    if (session != null && session.getArtifact() == null && !AiAdvisorPlugins.list().isEmpty()) {
      return BaseMessages.getString(PKG, "AiAdvisor.Status.NotBound");
    }
    return BaseMessages.getString(PKG, "AiAdvisor.Status.NoAdvisor");
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
      // Hop GUI replaces its metadata provider when another project opens. The field keeps the
      // one it was created with, which lists the providers of the project open back then.
      wProvider.setMetadataProvider(host.getMetadataProvider());
      wProvider.fillItems();
    } catch (HopException e) {
      // Combo stays empty; Send explains.
    }
    HopAiConfig config = HopAiConfigSingleton.getConfig();
    if (session != null && Utils.isEmpty(session.getProviderName())) {
      // The configured default, else the provider used last in this Hop GUI.
      String provider = config.getDefaultProviderName();
      if (Utils.isEmpty(provider) || !providerExists(provider)) {
        provider = store.getLastProviderName();
      }
      session.setProviderName(providerExists(provider) ? provider : "");
    }
    if (session != null && providerExists(session.getProviderName())) {
      wProvider.setText(session.getProviderName());
    } else if (wProvider.getItemCount() == 1) {
      wProvider.select(0);
      if (session != null) {
        session.setProviderName(wProvider.getText());
      }
    } else {
      // A name from another project, or a provider that was deleted or renamed. Leaving it in the
      // combo would make Edit fail on an element that does not exist.
      wProvider.setText("");
    }
  }

  private boolean providerExists(String name) {
    if (Utils.isEmpty(name)) {
      return false;
    }
    for (String item : wProvider.getItems()) {
      if (name.equals(item)) {
        return true;
      }
    }
    return false;
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
      if (isBlockedByConfig(id)) {
        check.setSelection(false);
        check.setEnabled(false);
        check.setToolTipText(
            check.getToolTipText()
                + "\n\n"
                + BaseMessages.getString(PKG, "AiAdvisor.Sharing.XmlBlocked.Tooltip"));
      }
      check.addListener(
          SWT.Selection,
          e -> {
            if (session != null) {
              session.getInclusions().put(id, check.getSelection());
              session.getUserChosenInclusions().add(id);
              if (AiAdvisorInclusions.LOGS.equals(id)) {
                session.setLogChoiceRunId(session.currentRunId());
              }
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
    Button preview = new Button(inclusionsComposite, SWT.PUSH);
    preview.setText(BaseMessages.getString(PKG, "AiAdvisor.Sharing.Preview.Label"));
    preview.setToolTipText(BaseMessages.getString(PKG, "AiAdvisor.Sharing.Preview.Tooltip"));
    PropsUi.setLook(preview);
    preview.addListener(SWT.Selection, e -> previewPayload());

    updateMetadataSelectButton();
    updateInclusionPickerButtons();
    updateSharingSummary();
    applySharingPanelExpanded();
  }

  /** Show exactly what the next question would send, without sending it. */
  private void previewPayload() {
    if (session == null) {
      return;
    }
    try {
      // Log widgets are SWT; read them here on the UI thread, as a send does.
      String logExcerpt = session.getLogSupplier() != null ? session.getLogSupplier().get() : null;
      String text =
          AiAdvisorEngine.preview(
              session,
              loadSelectedAdvisor(),
              host.getVariables(),
              host.getMetadataProvider(),
              logExcerpt,
              wPrompt.getText());
      EnterTextDialog dialog =
          new EnterTextDialog(
              getShell(),
              BaseMessages.getString(PKG, "AiAdvisor.Sharing.Preview.Title"),
              BaseMessages.getString(PKG, "AiAdvisor.Sharing.Preview.Message"),
              text,
              true);
      dialog.setReadOnly();
      dialog.open();
    } catch (Exception ex) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "AiAdvisor.Sharing.Preview.Title"),
          BaseMessages.getString(PKG, "AiAdvisor.Sharing.Preview.Error"),
          ex instanceof HopException ? ex : new HopException(ex));
    }
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
      setStatus(BaseMessages.getString(PKG, "AiAdvisor.Inclusion.Select.Empty"));
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
      if (value != null && !isBlockedByConfig(entry.getKey())) {
        entry.getValue().setSelection(value);
      }
    }
  }

  void onSendOrCancel() {
    if (session != null && session.isWorking()) {
      cancelInFlight();
      return;
    }
    send();
  }

  /**
   * Stop waiting for the answer. The session is free for a new question at once; whether the HTTP
   * request itself stops depends on the provider's client, and its answer is ignored if it comes.
   */
  private void cancelInFlight() {
    if (session == null || !session.isWorking()) {
      return;
    }
    session.requestCancel();
    List<AiAdvisorTurn> turns = session.getTurns();
    if (!turns.isEmpty()) {
      recordResult(session, turns.get(turns.size() - 1), null, null, true);
    }
    setStatus(Const.NVL(session.getStatusMessage(), ""));
    transcript.showSession(session, this::reviewProposalsForTurn);
    updateSendButton();
    store.fireChanged();
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
      // Stays clickable when AI is off or something is missing: Send then says what to do.
      wSend.setEnabled(session != null);
    }
  }

  private void send() {
    if (session == null || session.isWorking()) {
      return;
    }
    HopAiConfig config = HopAiConfigSingleton.getConfig();
    if (!config.isAiEnabled()) {
      setStatus(BaseMessages.getString(PKG, "AiAdvisor.Status.Disabled"));
      return;
    }
    String prompt = wPrompt.getText();
    if (Utils.isEmpty(prompt) || prompt.isBlank()) {
      setStatus(BaseMessages.getString(PKG, "AiAdvisor.Status.NoQuestion"));
      wPrompt.setFocus();
      return;
    }
    IAiAdvisor advisor = loadSelectedAdvisor();
    if (advisor == null) {
      setStatus(noAdvisorMessage());
      return;
    }
    if (!providerExists(wProvider.getText())) {
      setStatus(
          BaseMessages.getString(
              PKG,
              wProvider.getItemCount() == 0
                  ? "AiAdvisor.Status.NoProviderDefined"
                  : "AiAdvisor.Status.NoProviderSelected"));
      return;
    }
    session.setAdvisorPluginId(selectedAdvisorId());
    session.setScenarioId(selectedScenarioId());
    session.setProviderName(wProvider.getText());
    includeLogAfterARun();
    for (Map.Entry<String, Button> entry : inclusionButtons.entrySet()) {
      // A blocked option keeps the user's choice for when the configuration allows it again.
      if (!isBlockedByConfig(entry.getKey())) {
        session.getInclusions().put(entry.getKey(), entry.getValue().getSelection());
      }
    }

    AiAdvisorTurn turn = new AiAdvisorTurn();
    turn.setUserPrompt(prompt);
    turn.setStartedAtMillis(System.currentTimeMillis());
    session.addTurn(turn);
    session.setCancelled(false);
    session.setWorking(true);
    // The transcript shows a live waiting line; the status line is for problems.
    session.setStatusMessage("");
    wPrompt.setText("");
    updateSendButton();
    setStatus(session.getStatusMessage());
    transcript.showSession(session, this::reviewProposalsForTurn);
    store.fireChanged();

    AiAdvisorSession target = session;
    sentFromHere.add(target);
    // Build the question here on the UI thread: the log widgets are SWT, and the pipeline or
    // workflow may be edited while the model works. Only the model call runs in the background.
    AiAdvisorEngine.Prepared prepared;
    try {
      String logExcerpt = target.getLogSupplier() != null ? target.getLogSupplier().get() : null;
      prepared =
          AiAdvisorEngine.prepare(
              target, advisor, host.getVariables(), host.getMetadataProvider(), logExcerpt);
    } catch (Exception e) {
      completeTurn(target, turn, null, e);
      return;
    }
    turn.setEstimatedPromptTokens(prepared.estimatedTokens());
    turn.setProviderLabel(prepared.provider().getName());
    startWorkingLine(target);
    IVariables variables = host.getVariables();
    Display display = getDisplay();
    Thread worker =
        BackgroundThreadFacade.start(
            () -> {
              AiAdvisorResponse response = null;
              Throwable error = null;
              try {
                response = AiAdvisorEngine.execute(target, advisor, variables, prepared);
              } catch (Throwable t) {
                if (t instanceof InterruptedException) {
                  Thread.currentThread().interrupt();
                }
                error = t;
              }
              AiAdvisorResponse result = response;
              Throwable failure = error;
              Thread self = Thread.currentThread();
              // Record the answer even when this window was closed in the meantime (unless the
              // configuration cancels it on close): the session is shared with the other views
              // and would otherwise stay "working" for good.
              if (!display.isDisposed()) {
                display.asyncExec(
                    () -> {
                      // After Stop, or once a newer question runs, this answer is no longer
                      // wanted and must not touch the session.
                      if (target.getWorkerThread() == self) {
                        completeTurn(target, turn, result, failure);
                      }
                    });
              }
            },
            "hop-ai-advisor");
    target.setWorkerThread(worker);
  }

  private void completeTurn(
      AiAdvisorSession target, AiAdvisorTurn turn, AiAdvisorResponse response, Throwable error) {
    boolean cancelled = target.isCancelled();
    sentFromHere.remove(target);
    recordResult(target, turn, response, error, cancelled);
    store.fireChanged();
    if (isDisposed()) {
      return;
    }
    if (session == target) {
      setStatus(Const.NVL(target.getStatusMessage(), ""));
      transcript.showSession(target, this::reviewProposalsForTurn);
      updateSendButton();
    }
    if (error != null && !cancelled && !isExplained(error)) {
      // The status line already says what to do about an explained problem. Other failures, such
      // as a network or authentication error from the provider, get the details as well.
      new ErrorDialog(
          host.getShell(),
          BaseMessages.getString(PKG, "AiAdvisor.Send.Error.Title"),
          BaseMessages.getString(PKG, "AiAdvisor.Send.Error.Message"),
          error);
    }
  }

  /** Put the outcome of a question on the session; needs no widget of this pane. */
  static void recordResult(
      AiAdvisorSession target,
      AiAdvisorTurn turn,
      AiAdvisorResponse response,
      Throwable error,
      boolean cancelled) {
    target.setWorking(false);
    target.setWorkerThread(null);
    if (cancelled) {
      target.setStatusMessage(BaseMessages.getString(PKG, "AiAdvisor.Status.Cancelled"));
      if (Utils.isEmpty(turn.getAssistantAdvice())) {
        turn.setErrorMessage(target.getStatusMessage());
      }
    } else if (error != null) {
      String message = userVisibleError(error);
      turn.setErrorMessage(message);
      target.setStatusMessage(message);
    } else if (response != null) {
      turn.setAssistantAdvice(response.getMarkdownAdvice());
      if (Utils.isEmpty(turn.getAssistantAdvice())
          && response.getProposals() != null
          && !response.getProposals().isEmpty()) {
        // Some models answer a change request with the proposals only.
        turn.setAssistantAdvice(BaseMessages.getString(PKG, "AiAdvisor.Transcript.ProposalsOnly"));
      }
      turn.setRawAnswer(response.getRawResponse());
      turn.setProposalBlockPresent(response.isProposalBlockPresent());
      turn.setProposalParseError(response.getProposalParseError());
      turn.setInputTokenCount(response.getInputTokenCount());
      turn.setOutputTokenCount(response.getOutputTokenCount());
      turn.setDurationMs(response.getDurationMs());
      if (response.getProposals() != null) {
        turn.getProposals().addAll(response.getProposals());
      }
      if (turn.getProposals().isEmpty()) {
        target.setStatusMessage("");
      } else {
        target.setStatusMessage(BaseMessages.getString(PKG, "AiAdvisor.Status.WithProposals"));
      }
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
      setStatus(noAdvisorMessage());
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
    request.getAttributes().put(AiAdvisorRequest.ATTR_HOP_GUI, host.getHopGui());

    List<AiProposalValidation> validation = advisor.validateProposals(request, proposals);
    markOptIn(proposals, validation);
    AiAdvisorProposalReviewDialog reviewDialog =
        new AiAdvisorProposalReviewDialog(
            host.getShell(),
            proposals,
            validation,
            proposal -> {
              String custom = advisor.previewProposal(proposal);
              return !Utils.isEmpty(custom) ? custom : AiProposalPreview.format(proposal);
            });
    if (!reviewDialog.open()) {
      return;
    }
    List<AiProposal> selected = reviewDialog.getSelectedProposals();
    if (selected.isEmpty()) {
      return;
    }
    if (!confirmOverwrites(selected)) {
      return;
    }
    try {
      // All or nothing: the metadata saves undo themselves when one fails, and are undone when the
      // graph changes fail. The graph changes roll themselves back.
      AiMetadataProposalSupport.checkAll(selected, host.getMetadataProvider());
      List<AiMetadataBackup> backups =
          AiMetadataProposalSupport.saveAll(selected, host.getMetadataProvider());
      try {
        advisor.applyProposals(request, selected);
      } catch (Exception e) {
        try {
          AiMetadataProposalSupport.revert(backups, host.getMetadataProvider());
        } catch (Exception revertError) {
          e.addSuppressed(revertError);
        }
        throw e;
      }
      turn.getMetadataBackups().addAll(backups);
      int copied = AiClipboardProposals.copy(selected);
      int saved = backups.size();
      session.recordApplied(turn, selected, advisor);
      advisor.afterApply(request, selected);
      refreshBoundGraph();
      if (saved > 0) {
        fireMetadataChanged();
      }
      session.setStatusMessage(appliedStatusMessage(selected, copied, saved));
      setStatus(Const.NVL(session.getStatusMessage(), ""));
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

  /**
   * Deletes, replacements, settings changes and whatever the model itself rates HIGH risk are
   * opt-in for every advisor, whatever its validator says.
   */
  public static void markOptIn(List<AiProposal> proposals, List<AiProposalValidation> validations) {
    for (int i = 0; i < proposals.size() && i < validations.size(); i++) {
      AiProposal proposal = proposals.get(i);
      AiProposalTypes type = AiProposalTypes.of(proposal);
      AiProposalValidation validation = validations.get(i);
      if (validation != null
          && ((type != null && type.isOptIn())
              || "HIGH".equalsIgnoreCase(Const.NVL(proposal.getRiskLevel(), "").trim()))) {
        validation.setOptIn(true);
      }
    }
  }

  private boolean confirmOverwrites(List<AiProposal> selected) {
    List<String> overwritten = new ArrayList<>();
    for (AiProposal proposal : selected) {
      if (AiMetadataProposalSupport.exists(proposal, host.getMetadataProvider())) {
        overwritten.add(
            "- "
                + Const.NVL(proposal.parameter("typeKey"), "")
                + " "
                + Const.NVL(
                    AiMetadataProposalSupport.targetName(proposal, host.getMetadataProvider()),
                    ""));
      }
    }
    if (overwritten.isEmpty()) {
      return true;
    }
    MessageBox box = new MessageBox(host.getShell(), SWT.ICON_WARNING | SWT.YES | SWT.NO);
    box.setText(BaseMessages.getString(PKG, "AiAdvisor.Overwrite.Title"));
    box.setMessage(
        BaseMessages.getString(PKG, "AiAdvisor.Overwrite.Message", String.join("\n", overwritten)));
    return box.open() == SWT.YES;
  }

  private void undoMetadataForTurn(int turnIndex) {
    if (session == null || turnIndex < 0 || turnIndex >= session.getTurns().size()) {
      return;
    }
    AiAdvisorTurn turn = session.getTurns().get(turnIndex);
    if (turn.getMetadataBackups().isEmpty()) {
      return;
    }
    // Undo puts back what was there before the assistant saved. Changes made since are lost then,
    // which the user decides.
    List<String> changed =
        AiMetadataProposalSupport.changedSinceSave(
            turn.getMetadataBackups(), host.getMetadataProvider());
    if (!changed.isEmpty()) {
      MessageBox box = new MessageBox(host.getShell(), SWT.ICON_WARNING | SWT.YES | SWT.NO);
      box.setText(BaseMessages.getString(PKG, "AiAdvisor.UndoMetadata.Changed.Title"));
      box.setMessage(
          BaseMessages.getString(
              PKG, "AiAdvisor.UndoMetadata.Changed.Message", "- " + String.join("\n- ", changed)));
      if (box.open() != SWT.YES) {
        return;
      }
    }
    try {
      AiMetadataProposalSupport.revert(turn.getMetadataBackups(), host.getMetadataProvider());
      int count = turn.getMetadataBackups().size();
      turn.getMetadataBackups().clear();
      fireMetadataChanged();
      session.setStatusMessage(
          BaseMessages.getString(PKG, "AiAdvisor.Status.MetadataUndone", count));
      setStatus(Const.NVL(session.getStatusMessage(), ""));
      transcript.showSession(session, this::reviewProposalsForTurn);
      store.fireChanged();
    } catch (Exception ex) {
      new ErrorDialog(
          host.getShell(),
          BaseMessages.getString(PKG, "AiAdvisor.Apply.Error.Title"),
          BaseMessages.getString(PKG, "AiAdvisor.UndoMetadata.Error.Message"),
          ex instanceof HopException ? ex : new HopException(ex));
    }
  }

  private String appliedStatusMessage(List<AiProposal> selected, int copied, int saved) {
    int graph = 0;
    for (AiProposal proposal : selected) {
      AiProposalTypes type = AiProposalTypes.of(proposal);
      if (type != null && !type.isWorkbenchOwned()) {
        graph++;
      }
    }
    if (copied <= 0 && saved <= 0) {
      return BaseMessages.getString(PKG, "AiAdvisor.Transcript.Applied", selected.size());
    }
    StringBuilder message = new StringBuilder();
    if (graph > 0) {
      message.append(BaseMessages.getString(PKG, "AiAdvisor.Transcript.AppliedGraph", graph));
    }
    if (copied > 0) {
      if (!message.isEmpty()) {
        message.append(' ');
      }
      message.append(BaseMessages.getString(PKG, "AiAdvisor.Transcript.CopiedClipboard"));
    }
    if (saved > 0) {
      if (!message.isEmpty()) {
        message.append(' ');
      }
      message.append(BaseMessages.getString(PKG, "AiAdvisor.Transcript.SavedMetadata", saved));
    }
    return message.toString();
  }

  private void fireMetadataChanged() throws HopException {
    if (host.getHopGui() == null || host.getHopGui().getEventsHandler() == null) {
      return;
    }
    host.getHopGui().getEventsHandler().fire(HopGuiEvents.MetadataChanged.name());
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
      IHopFileTypeHandler handler = item.getTypeHandler();
      if (handler == null || handler.getSubject() != artifact) {
        continue;
      }
      if (handler instanceof HopGuiAbstractGraph graph) {
        graph.setChanged();
      }
      handler.redraw();
      handler.updateGui();
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
