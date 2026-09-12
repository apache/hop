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
import java.util.List;
import java.util.function.IntConsumer;
import org.apache.hop.ai.engine.AiAdvisorMarkdown;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorTurn;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.FontData;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.ScrollBar;
import org.eclipse.swt.widgets.Text;

/** Scrollable transcript of one {@link AiAdvisorSession}. */
public class AiAdvisorTranscriptPanel extends Composite {

  private static final Class<?> PKG = AiAdvisorPerspective.class;

  enum Role {
    USER,
    ASSISTANT,
    ERROR,
    SYSTEM
  }

  private final ScrolledComposite scroll;
  private final Composite content;
  private final List<Control> bodies = new ArrayList<>();
  private boolean scrollToBottom;
  private Font usageFont;

  public AiAdvisorTranscriptPanel(Composite parent) {
    super(parent, SWT.NONE);
    PropsUi.setLook(this);
    setLayout(new FillLayout());

    scroll = new ScrolledComposite(this, SWT.V_SCROLL | SWT.BORDER);
    PropsUi.setLook(scroll);
    scroll.setExpandHorizontal(true);
    scroll.setExpandVertical(true);
    scroll.setAlwaysShowScrollBars(false);

    content = new Composite(scroll, SWT.NONE);
    PropsUi.setLook(content);
    GridLayout contentLayout = new GridLayout(1, true);
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    contentLayout.verticalSpacing = PropsUi.getMargin();
    content.setLayout(contentLayout);
    scroll.setContent(content);

    scroll.addListener(SWT.Resize, e -> refreshScroll(false));
    installWheelForwarding();
  }

  public void showSession(AiAdvisorSession session) {
    showSession(session, null);
  }

  public void showSession(AiAdvisorSession session, IntConsumer reviewTurn) {
    for (Control child : content.getChildren()) {
      child.dispose();
    }
    bodies.clear();
    if (session == null || session.getTurns().isEmpty()) {
      Composite block = appendBlock(Role.SYSTEM);
      appendHeading(
          block, Role.SYSTEM, BaseMessages.getString(PKG, "AiAdvisor.Transcript.Empty"), null);
      appendNote(block, Role.SYSTEM, BaseMessages.getString(PKG, "AiAdvisor.GitWarning"));
    } else {
      List<AiAdvisorTurn> turns = session.getTurns();
      for (int i = 0; i < turns.size(); i++) {
        AiAdvisorTurn turn = turns.get(i);
        if (!Utils.isEmpty(turn.getUserPrompt())) {
          Composite block = appendBlock(Role.USER);
          appendHeading(
              block, Role.USER, BaseMessages.getString(PKG, "AiAdvisor.Transcript.You"), null);
          appendBody(block, Role.USER, turn.getUserPrompt());
        }
        Composite responseBlock = null;
        if (!Utils.isEmpty(turn.getErrorMessage())) {
          responseBlock = appendBlock(Role.ERROR);
          appendHeading(
              responseBlock,
              Role.ERROR,
              BaseMessages.getString(PKG, "AiAdvisor.Transcript.Error"),
              turn.getErrorMessage());
          appendBody(responseBlock, Role.ERROR, turn.getErrorMessage());
        } else if (!Utils.isEmpty(turn.getAssistantAdvice())) {
          responseBlock = appendBlock(Role.ASSISTANT);
          appendHeading(
              responseBlock,
              Role.ASSISTANT,
              BaseMessages.getString(PKG, "AiAdvisor.Transcript.Assistant"),
              turn.getAssistantAdvice(),
              formatUsage(
                  turn.getInputTokenCount(), turn.getOutputTokenCount(), turn.getDurationMs()));
          appendBody(responseBlock, Role.ASSISTANT, turn.getAssistantAdvice());
        }
        if (turn.getProposals() != null && !turn.getProposals().isEmpty()) {
          if (responseBlock == null) {
            responseBlock = appendBlock(Role.ASSISTANT);
          }
          int turnIndex = i;
          appendReviewButton(
              responseBlock,
              BaseMessages.getString(PKG, "AiAdvisor.Review.Label", turn.getProposals().size()),
              reviewTurn != null ? () -> reviewTurn.accept(turnIndex) : null);
        } else if (turn.isProposalBlockPresent()) {
          if (responseBlock == null) {
            responseBlock = appendBlock(Role.ASSISTANT);
          }
          appendNote(
              responseBlock,
              Role.ASSISTANT,
              BaseMessages.getString(PKG, "AiAdvisor.Review.Dropped"));
        }
        if (turn.getAppliedSummaries() != null && !turn.getAppliedSummaries().isEmpty()) {
          Composite block = appendBlock(Role.SYSTEM);
          appendNote(
              block,
              Role.SYSTEM,
              BaseMessages.getString(
                  PKG, "AiAdvisor.Transcript.Applied", turn.getAppliedSummaries().size()));
        }
      }
    }
    refreshScroll(true);
  }

  private Composite appendBlock(Role role) {
    Composite block = new Composite(content, SWT.BORDER);
    GridLayout layout = new GridLayout(1, true);
    layout.marginWidth = PropsUi.getMargin();
    layout.marginHeight = PropsUi.getMargin();
    layout.verticalSpacing = PropsUi.getFormMargin();
    block.setLayout(layout);
    block.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, false));
    block.setBackgroundMode(SWT.INHERIT_FORCE);
    applyRoleLook(block, role);
    return block;
  }

  private void appendHeading(Composite block, Role role, String text, String copyText) {
    appendHeading(block, role, text, copyText, "");
  }

  private void appendHeading(
      Composite block, Role role, String text, String copyText, String usageText) {
    Composite row = new Composite(block, SWT.NONE);
    int columns = 1;
    if (!Utils.isEmpty(usageText)) {
      columns++;
    }
    if (copyText != null) {
      columns++;
    }
    GridLayout rowLayout = new GridLayout(columns, false);
    rowLayout.marginWidth = 0;
    rowLayout.marginHeight = 0;
    rowLayout.horizontalSpacing = PropsUi.getMargin();
    row.setLayout(rowLayout);
    row.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
    applyRoleLook(row, role);

    Label label = new Label(row, SWT.LEFT | SWT.WRAP);
    label.setText(text);
    label.setFont(GuiResource.getInstance().getFontMediumBold());
    label.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
    applyRoleLook(label, role);

    if (!Utils.isEmpty(usageText)) {
      Label usage = new Label(row, SWT.RIGHT);
      usage.setText(usageText);
      usage.setFont(usageFont());
      usage.setAlignment(SWT.RIGHT);
      usage.setToolTipText(usageText);
      GridData usageLayout = new GridData(SWT.END, SWT.CENTER, false, false);
      usage.setLayoutData(usageLayout);
      applyRoleLook(usage, role);
    }

    if (copyText != null) {
      Button copy = new Button(row, SWT.PUSH | SWT.FLAT);
      copy.setImage(GuiResource.getInstance().getImageCopy());
      copy.setToolTipText(BaseMessages.getString(PKG, "AiAdvisor.Transcript.Copy.Tooltip"));
      final String payload = copyText;
      copy.addListener(SWT.Selection, e -> GuiResource.getInstance().toClipboard(payload));
    }
  }

  private Font usageFont() {
    if (usageFont == null || usageFont.isDisposed()) {
      Font base = GuiResource.getInstance().getFontSmall();
      FontData[] data = base.getFontData();
      for (FontData fontData : data) {
        fontData.setHeight(Math.max(1, fontData.getHeight() * 2));
        fontData.setStyle(SWT.ITALIC);
      }
      usageFont = new Font(getDisplay(), data);
      addDisposeListener(
          event -> {
            if (usageFont != null && !usageFont.isDisposed()) {
              usageFont.dispose();
            }
          });
    }
    return usageFont;
  }

  static String formatUsage(Integer inputTokens, Integer outputTokens, Long durationMs) {
    boolean hasTokens = inputTokens != null || outputTokens != null;
    boolean hasTime = durationMs != null && durationMs > 0;
    if (!hasTokens && !hasTime) {
      return "";
    }
    String time = hasTime ? formatDuration(durationMs) : "";
    if (hasTokens) {
      String in = formatTokenCount(inputTokens);
      String out = formatTokenCount(outputTokens);
      if (hasTime) {
        return usageMessage(
            "AiAdvisor.Transcript.Usage", in + " in · " + out + " out · " + time, in, out, time);
      }
      return usageMessage(
          "AiAdvisor.Transcript.UsageTokens", in + " in · " + out + " out", in, out);
    }
    return time;
  }

  private static String usageMessage(String key, String fallback, String... args) {
    String formatted = BaseMessages.getString(PKG, key, (Object[]) args);
    if (Utils.isEmpty(formatted) || formatted.contains(key)) {
      return fallback;
    }
    return formatted;
  }

  static String formatTokenCount(Integer count) {
    if (count == null) {
      return "—";
    }
    return String.format("%,d", count);
  }

  static String formatDuration(long durationMs) {
    if (durationMs < 1000) {
      return durationMs + " ms";
    }
    if (durationMs < 60_000) {
      double seconds = durationMs / 1000.0;
      if (durationMs % 1000 == 0) {
        return (durationMs / 1000) + " s";
      }
      return String.format("%.1f s", seconds);
    }
    long totalSeconds = Math.round(durationMs / 1000.0);
    return (totalSeconds / 60) + " m " + String.format("%02d", totalSeconds % 60) + " s";
  }

  private void appendNote(Composite block, Role role, String text) {
    Label label = new Label(block, SWT.LEFT | SWT.WRAP);
    label.setText(text);
    label.setLayoutData(new GridData(SWT.FILL, SWT.CENTER, true, false));
    applyRoleLook(label, role);
  }

  private void appendReviewButton(Composite block, String text, Runnable onSelect) {
    Button button = new Button(block, SWT.PUSH);
    button.setText(text);
    if (onSelect != null) {
      button.addListener(SWT.Selection, e -> onSelect.run());
    } else {
      button.setEnabled(false);
    }
    button.setLayoutData(new GridData(SWT.LEFT, SWT.CENTER, false, false));
  }

  private void appendBody(Composite block, Role role, String text) {
    GridData gd = new GridData(SWT.FILL, SWT.FILL, true, false);
    gd.widthHint = 1;
    if (role == Role.USER) {
      Text body = new Text(block, SWT.MULTI | SWT.WRAP | SWT.READ_ONLY);
      body.setText(text != null ? text : "");
      body.setLayoutData(gd);
      applyRoleLook(body, role);
      bodies.add(body);
      return;
    }
    StyledText body = new StyledText(block, SWT.MULTI | SWT.WRAP | SWT.READ_ONLY);
    AiAdvisorMarkdown.Document document = AiAdvisorMarkdown.render(text);
    body.setText(document.text());
    body.setLayoutData(gd);
    applyRoleLook(body, role);
    applyMarkdownStyles(body, document);
    bodies.add(body);
  }

  private void applyMarkdownStyles(StyledText widget, AiAdvisorMarkdown.Document document) {
    GuiResource gui = GuiResource.getInstance();
    for (AiAdvisorMarkdown.Span span : document.spans()) {
      StyleRange range = new StyleRange();
      range.start = span.start();
      range.length = span.length();
      switch (span.kind()) {
        case HEADING -> range.font = gui.getFontMediumBold();
        case BOLD -> range.fontStyle = SWT.BOLD;
        case EMPHASIS -> range.fontStyle = SWT.ITALIC;
        case CODE -> range.font = gui.getFontFixed();
      }
      widget.setStyleRange(range);
    }
  }

  /**
   * Subtle role colors so questions and answers are distinct in both light and dark mode. Values
   * are {background R,G,B, foreground R,G,B}.
   */
  static int[] roleRgb(Role role, boolean dark) {
    return switch (role) {
      case USER ->
          dark ? new int[] {40, 72, 56, 214, 236, 222} : new int[] {232, 248, 238, 20, 45, 35};
      case ASSISTANT ->
          dark ? new int[] {44, 52, 68, 220, 226, 236} : new int[] {234, 240, 248, 20, 32, 48};
      case ERROR ->
          dark ? new int[] {72, 42, 42, 236, 214, 214} : new int[] {250, 236, 236, 48, 20, 20};
      case SYSTEM ->
          dark ? new int[] {48, 48, 52, 210, 210, 214} : new int[] {244, 244, 246, 40, 40, 44};
    };
  }

  static void applyRoleLook(Control control, Role role) {
    GuiResource gui = GuiResource.getInstance();
    int[] rgb = roleRgb(role, PropsUi.getInstance().isDarkMode());
    control.setBackground(gui.getColor(rgb[0], rgb[1], rgb[2]));
    control.setForeground(gui.getColor(rgb[3], rgb[4], rgb[5]));
  }

  public void refreshScroll() {
    refreshScroll(scrollToBottom);
  }

  private void refreshScroll(boolean toBottom) {
    if (isDisposed() || scroll.isDisposed() || content.isDisposed()) {
      return;
    }
    scrollToBottom = toBottom;
    int clientWidth = scroll.getClientArea().width;
    if (clientWidth <= 0) {
      getDisplay().asyncExec(() -> refreshScroll(toBottom));
      return;
    }
    // First pass: let GridLayout assign real widths.
    for (Control body : bodies) {
      if (body.isDisposed()) {
        continue;
      }
      GridData gd = (GridData) body.getLayoutData();
      gd.widthHint = 1;
      gd.heightHint = SWT.DEFAULT;
    }
    content.layout(true, true);
    // Second pass: wrap height from the width the widget actually received. Using the
    // scroller client width here under-counts block margins and clips long replies.
    for (Control body : bodies) {
      if (body.isDisposed()) {
        continue;
      }
      GridData gd = (GridData) body.getLayoutData();
      int wrapWidth = Math.max(body.getSize().x, 80);
      Point size = body.computeSize(wrapWidth, SWT.DEFAULT);
      gd.widthHint = wrapWidth;
      gd.heightHint = Math.max(size.y, lineHeight(body));
    }
    content.layout(true, true);
    Point size = content.computeSize(clientWidth, SWT.DEFAULT);
    scroll.setMinSize(clientWidth, size.y);
    if (toBottom) {
      scroll.setOrigin(0, Math.max(0, size.y));
    }
  }

  /**
   * Inner read-only {@link Text} widgets swallow the mouse wheel, so the outer scroller never
   * moves. Forward wheel events from anything under this panel to the {@link ScrolledComposite}.
   */
  private void installWheelForwarding() {
    Display display = getDisplay();
    Listener wheelFilter =
        event -> {
          if (isDisposed() || scroll.isDisposed()) {
            return;
          }
          if (!(event.widget instanceof Control control) || !isUnder(this, control)) {
            return;
          }
          ScrollBar bar = scroll.getVerticalBar();
          if (bar == null || control == scroll) {
            return;
          }
          int increment = Math.max(bar.getIncrement(), 16);
          Point origin = scroll.getOrigin();
          scroll.setOrigin(origin.x, origin.y - event.count * increment);
          event.doit = false;
        };
    display.addFilter(SWT.MouseVerticalWheel, wheelFilter);
    addDisposeListener(
        e -> {
          if (!display.isDisposed()) {
            display.removeFilter(SWT.MouseVerticalWheel, wheelFilter);
          }
        });
  }

  private static int lineHeight(Control body) {
    if (body instanceof Text text) {
      return text.getLineHeight();
    }
    if (body instanceof StyledText styled) {
      return styled.getLineHeight();
    }
    return 16;
  }

  static boolean isUnder(Control ancestor, Control control) {
    Control current = control;
    while (current != null) {
      if (current == ancestor) {
        return true;
      }
      current = current.getParent();
    }
    return false;
  }
}
