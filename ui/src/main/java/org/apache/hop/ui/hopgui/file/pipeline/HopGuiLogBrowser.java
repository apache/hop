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

package org.apache.hop.ui.hopgui.file.pipeline;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.logging.FixedWidthLogLayout;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.logging.IHasLogChannel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILogParentProvided;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.ExecutorUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.OsHelper;
import org.apache.hop.ui.core.widget.StyledTextVar;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;

public class HopGuiLogBrowser {
  private static final Class<?> PKG = HopGui.class;

  @Getter private TextComposite text;
  @Getter private ILogParentProvided logProvider;
  private List<String> childIds = new ArrayList<>();
  private Date lastLogRegistryChange;
  private AtomicBoolean paused;
  private final AtomicInteger lastLogId = new AtomicInteger(-1);

  /** First log buffer id included in the current view (advanced when the user clears the log). */
  private final AtomicInteger firstLogId = new AtomicInteger(-1);

  private final AtomicBoolean busy = new AtomicBoolean(false);

  private final FixedWidthLogLayout logLayout = new FixedWidthLogLayout(true);

  private volatile String filterText = "";
  private volatile boolean highlightMatches;
  private volatile boolean caseSensitive;
  private volatile boolean excludeMatches;

  public HopGuiLogBrowser(final TextComposite text, final ILogParentProvided logProvider) {
    this.text = text;
    this.logProvider = logProvider;
    this.paused = new AtomicBoolean(false);
  }

  /**
   * Update log filter options used when appending and when rebuilding the view.
   *
   * @param text filter string; empty disables filtering
   * @param highlight when true and filter is set, show all lines and highlight matches
   * @param caseSensitive case-sensitive contains match when true
   * @param exclude when true and filter is set, hide matching lines (wins over highlight)
   */
  public void setFilter(String text, boolean highlight, boolean caseSensitive, boolean exclude) {
    this.filterText = Const.NVL(text, "").trim();
    this.highlightMatches = highlight;
    this.caseSensitive = caseSensitive;
    this.excludeMatches = exclude;
  }

  /**
   * Rebuild the log text widget from the central log buffer using the current filter settings.
   * Called after the user changes the filter text or options.
   */
  public void refreshFilteredView() {
    if (text == null || text.isDisposed()) {
      return;
    }

    Runnable rebuild =
        () -> {
          if (text.isDisposed()) {
            return;
          }
          busy.set(true);
          try {
            ensureChildIds();
            int lastNr = HopLogStore.getLastBufferLineNr();
            List<HopLoggingEvent> logLines =
                HopLogStore.getLogBufferFromTo(childIds, false, firstLogId.get(), lastNr);

            StyledText styledText = getStyledText();
            synchronized (text) {
              String initial = OsHelper.isMac() ? Const.CR : "";
              if (styledText != null && !styledText.isDisposed()) {
                styledText.setText(initial);
              } else {
                text.setText(initial);
              }

              for (HopLoggingEvent event : logLines) {
                appendLogEvent(event, styledText);
              }

              trimToMaxSize(styledText);

              if (!text.isDisposed()) {
                text.setSelection(text.getCharCount());
              }
            }
            lastLogId.set(lastNr);
          } finally {
            busy.set(false);
          }
        };

    if (text.getDisplay().getThread() == Thread.currentThread()) {
      rebuild.run();
    } else {
      text.getDisplay().asyncExec(rebuild);
    }
  }

  public void installLogSniffer() {

    // Refresh the log every second or so
    //
    final Timer logRefreshTimer = new Timer("log sniffer Timer");
    TimerTask timerTask =
        new TimerTask() {
          @Override
          public void run() {
            if (text.isDisposed() || text.getDisplay().isDisposed()) {
              return;
            }

            text.getDisplay()
                .asyncExec(
                    () -> {
                      IHasLogChannel provider = logProvider.getLogChannelProvider();

                      if (provider != null
                          && !text.isDisposed()
                          && text.isVisible()
                          && !busy.get()
                          && !paused.get()) {
                        busy.set(true);

                        ILogChannel logChannel = provider.getLogChannel();
                        // The log channel can still be initializing.
                        // It happens with slow writing of execution information to a location.
                        //
                        if (logChannel != null) {
                          ensureChildIds();

                          // See if we need to log any lines...
                          //
                          int lastNr = HopLogStore.getLastBufferLineNr();
                          if (lastNr > lastLogId.get()) {
                            // Only show logs for this pipeline/workflow and its children, not the
                            // shared Hop GUI log channel (which would mix logs from other tabs).
                            List<HopLoggingEvent> logLines =
                                HopLogStore.getLogBufferFromTo(
                                    childIds, false, lastLogId.get(), lastNr);

                            StyledText styledText = getStyledText();

                            synchronized (text) {
                              for (HopLoggingEvent event : logLines) {
                                appendLogEvent(event, styledText);
                              }

                              trimToMaxSize(styledText);

                              if (!text.isDisposed()) {
                                text.setSelection(text.getCharCount());
                              }
                            }
                            lastLogId.set(lastNr);
                          }
                        }
                        busy.set(false);
                      }
                    });
          }
        };

    // Refresh every often enough
    //
    logRefreshTimer.schedule(
        timerTask,
        Const.toInt(EnvUtil.getSystemProperty(Const.HOP_LOG_TAB_REFRESH_DELAY), 1000),
        Const.toInt(EnvUtil.getSystemProperty(Const.HOP_LOG_TAB_REFRESH_PERIOD), 1000));

    // Make sure the timer goes down when the widget is disposed
    //
    text.addDisposeListener(event -> ExecutorUtil.cleanup(logRefreshTimer));

    // Make sure the timer goes down when the Display is disposed
    // Lambda expression cannot be used here as it causes SecurityException in RAP.
    text.getDisplay().disposeExec(logRefreshTimer::cancel);

    final Menu menu = new Menu(text);
    MenuItem item = new MenuItem(menu, SWT.NONE);
    item.setText(BaseMessages.getString(PKG, "LogBrowser.CopySelectionToClipboard.MenuItem"));
    item.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent event) {
            String selection = text.getSelectionText();
            if (!Utils.isEmpty(selection)) {
              GuiResource.getInstance().toClipboard(selection);
            }
          }
        });
    text.setMenu(menu);

    text.addMouseListener(
        new MouseAdapter() {
          @Override
          public void mouseDown(MouseEvent event) {
            if (event.button == 3) {
              ConstUi.displayMenu(menu, text);
            }
          }
        });
  }

  private void ensureChildIds() {
    IHasLogChannel provider = logProvider.getLogChannelProvider();
    if (provider == null) {
      return;
    }
    ILogChannel logChannel = provider.getLogChannel();
    if (logChannel == null) {
      return;
    }
    String parentLogChannelId = logChannel.getLogChannelId();
    LoggingRegistry registry = LoggingRegistry.getInstance();
    Date registryModDate = registry.getLastModificationTime();

    if (childIds == null
        || lastLogRegistryChange == null
        || registryModDate.compareTo(lastLogRegistryChange) > 0) {
      lastLogRegistryChange = registry.getLastModificationTime();
      childIds = LoggingRegistry.getInstance().getLogChannelChildren(parentLogChannelId);
    }
  }

  private StyledText getStyledText() {
    if (text instanceof StyledTextVar) {
      return ((StyledTextVar) text).getTextWidget();
    }
    return null;
  }

  private void appendLogEvent(HopLoggingEvent event, StyledText styledText) {
    String line = logLayout.format(event).trim();
    if (line.isEmpty() || !shouldDisplayLine(line)) {
      return;
    }

    boolean isError =
        event.getLevel() != null && event.getLevel().getLevel() == LogLevel.ERROR.getLevel();

    if (styledText != null && !styledText.isDisposed()) {
      try {
        int startOffset = styledText.getCharCount();
        String textToAdd = line + Const.CR;
        styledText.replaceTextRange(startOffset, 0, textToAdd);
        applyLineStyles(styledText, startOffset, line, isError);
      } catch (Exception e) {
        String currentText = text.getText();
        text.setText(currentText + line + Const.CR);
      }
    } else {
      // Fallback for non-StyledText widgets (e.g., web mode)
      String currentText = text.getText();
      text.setText(currentText + line + Const.CR);
    }
  }

  /**
   * Decide whether a log line should appear in the view given the current filter options.
   *
   * <ul>
   *   <li>Empty filter: show all
   *   <li>Exclude on: hide matching lines
   *   <li>Highlight on: show all (matches get styles elsewhere)
   *   <li>Otherwise: only-matching mode
   * </ul>
   */
  boolean shouldDisplayLine(String line) {
    if (StringUtils.isEmpty(filterText)) {
      return true;
    }
    boolean matches = lineMatches(line);
    if (excludeMatches) {
      return !matches;
    }
    if (highlightMatches) {
      return true;
    }
    return matches;
  }

  boolean lineMatches(String line) {
    if (StringUtils.isEmpty(filterText) || line == null) {
      return false;
    }
    if (caseSensitive) {
      return line.contains(filterText);
    }
    return StringUtils.containsIgnoreCase(line, filterText);
  }

  private void applyLineStyles(
      StyledText styledText, int startOffset, String line, boolean isError) {
    if (isError) {
      StyleRange styleRange = new StyleRange();
      styleRange.start = startOffset;
      styleRange.length = line.length();
      styleRange.foreground = GuiResource.getInstance().getColorRed();
      styleRange.fontStyle = SWT.NORMAL;
      styledText.setStyleRange(styleRange);
    }

    if (highlightMatches && StringUtils.isNotEmpty(filterText) && lineMatches(line)) {
      applyHighlightRanges(styledText, startOffset, line, isError);
    }
  }

  private void applyHighlightRanges(
      StyledText styledText, int startOffset, String line, boolean isError) {
    String haystack = caseSensitive ? line : line.toLowerCase();
    String needle = caseSensitive ? filterText : filterText.toLowerCase();
    if (needle.isEmpty()) {
      return;
    }

    int from = 0;
    while (from <= haystack.length() - needle.length()) {
      int idx = haystack.indexOf(needle, from);
      if (idx < 0) {
        break;
      }
      StyleRange range = new StyleRange();
      range.start = startOffset + idx;
      range.length = needle.length();
      // Use getColor(r,g,b) so dark-mode contrast remapping does not invert black/yellow.
      // Dark mode: dark orange background with default (light) text is readable.
      // Light mode: bright yellow; keep red text on error lines.
      if (PropsUi.getInstance().isDarkMode()) {
        range.background = GuiResource.getInstance().getColor(180, 90, 0);
      } else {
        range.background = GuiResource.getInstance().getColorYellow();
        if (isError) {
          range.foreground = GuiResource.getInstance().getColorRed();
        }
      }
      range.fontStyle = SWT.NORMAL;
      styledText.setStyleRange(range);
      from = idx + Math.max(needle.length(), 1);
    }
  }

  private void trimToMaxSize(StyledText styledText) {
    int maxSize = getMaxLogSize();
    String textContent = text.getText();
    int size;
    if (textContent == null || textContent.isEmpty()) {
      size = 0;
    } else {
      size = 1;
      for (int i = 0; i < textContent.length(); i++) {
        if (textContent.charAt(i) == '\n') {
          size++;
        }
      }
    }

    if (maxSize > 0 && size > maxSize) {
      int dropIndex = StringUtils.lastOrdinalIndexOf(textContent, "\n", maxSize + 1);
      if (dropIndex < 0) {
        return;
      }
      if (styledText != null && !styledText.isDisposed()) {
        styledText.replaceTextRange(0, dropIndex + 1, "");
      } else {
        text.setText(textContent.substring(dropIndex + 1));
      }
    }
  }

  private int getMaxLogSize() {
    DescribedVariable describedVariable =
        HopConfig.getInstance().findDescribedVariable(Const.HOP_MAX_LOG_SIZE_IN_LINES);
    if (describedVariable == null) {
      return Const.MAX_NR_LOG_LINES;
    }
    return Const.toInt(describedVariable.getValue(), Const.MAX_NR_LOG_LINES);
  }

  public boolean isPaused() {
    return paused.get();
  }

  public void setPaused(boolean paused) {
    this.paused.set(paused);
  }

  /**
   * Reset cached log channel state so the next refresh will use the current log channel provider
   * (e.g. after attaching to a different running pipeline or workflow).
   */
  public void resetLogChannels() {
    childIds = new ArrayList<>();
    lastLogRegistryChange = null;
  }

  /** Skip log lines already in the central buffer (e.g. after the user clears the log view). */
  public void resetLogPosition() {
    int nr = HopLogStore.getLastBufferLineNr();
    lastLogId.set(nr);
    firstLogId.set(nr);
  }
}
