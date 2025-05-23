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
import org.apache.hop.core.logging.HopLogLayout;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.HopLoggingEvent;
import org.apache.hop.core.logging.IHasLogChannel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILogParentProvided;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.ExecutorUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.text.Format;
import org.apache.hop.ui.core.widget.text.TextFormatter;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Text;

public class HopGuiLogBrowser {
  private static final Class<?> PKG = HopGui.class;

  @Getter private Text text;
  @Getter private ILogParentProvided logProvider;
  private List<String> childIds = new ArrayList<>();
  private Date lastLogRegistryChange;
  private AtomicBoolean paused;

  public HopGuiLogBrowser(final Text text, final ILogParentProvided logProvider) {
    this.text = text;
    this.logProvider = logProvider;
    this.paused = new AtomicBoolean(false);
  }

  public void installLogSniffer() {

    // Create a new buffer appender to the log and capture that directly...
    //
    final AtomicInteger lastLogId = new AtomicInteger(-1);
    final AtomicBoolean busy = new AtomicBoolean(false);
    final HopLogLayout logLayout = new HopLogLayout(true);

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
                          && !busy.get()
                          && !paused.get()
                          && text.isVisible()) {
                        busy.set(true);

                        ILogChannel logChannel = provider.getLogChannel();
                        // The log channel can still be initializing.
                        // It happens with slow writing of execution information to a location.
                        //
                        if (logChannel != null) {
                          String parentLogChannelId = logChannel.getLogChannelId();
                          LoggingRegistry registry = LoggingRegistry.getInstance();
                          Date registryModDate = registry.getLastModificationTime();

                          if (childIds == null
                              || lastLogRegistryChange == null
                              || registryModDate.compareTo(lastLogRegistryChange) > 0) {
                            lastLogRegistryChange = registry.getLastModificationTime();
                            childIds =
                                LoggingRegistry.getInstance()
                                    .getLogChannelChildren(parentLogChannelId);
                          }

                          // See if we need to log any lines...
                          //
                          int lastNr = HopLogStore.getLastBufferLineNr();
                          if (lastNr > lastLogId.get()) {
                            List<HopLoggingEvent> logLines =
                                HopLogStore.getLogBufferFromTo(
                                    childIds, true, lastLogId.get(), lastNr);

                            // The maximum size of the log buffer
                            //
                            int maxSize;
                            DescribedVariable describedVariable =
                                HopConfig.getInstance()
                                    .findDescribedVariable(Const.HOP_MAX_LOG_SIZE_IN_LINES);
                            if (describedVariable == null) {
                              maxSize = Const.MAX_NR_LOG_LINES;
                            } else {
                              maxSize =
                                  Const.toInt(describedVariable.getValue(), Const.MAX_NR_LOG_LINES);
                            }

                            synchronized (text) {
                              for (HopLoggingEvent event : logLines) {
                                String line = logLayout.format(event).trim();
                                int length = line.length();

                                if (length > 0) {
                                  Format format = TextFormatter.getInstance().execute(line);
                                  text.append(format.getText());
                                  text.append(Const.CR);
                                }
                              }
                            }

                            // Erase it all in one go
                            // This makes it a bit more efficient
                            // getLineCount is not supported in RAP
                            int size;
                            if (!EnvironmentUtils.getInstance().isWeb()) {
                              size = text.getLineCount();
                            } else {
                              size = text.getText().length();
                            }

                            if (maxSize > 0 && size > maxSize) {
                              int dropIndex =
                                  StringUtils.lastOrdinalIndexOf(text.getText(), "\n", maxSize + 1);
                              text.setText(text.getText().substring(dropIndex + 1));
                            }

                            text.setSelection(text.getText().length());
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

  public boolean isPaused() {
    return paused.get();
  }

  public void setPaused(boolean paused) {
    this.paused.set(paused);
  }
}
