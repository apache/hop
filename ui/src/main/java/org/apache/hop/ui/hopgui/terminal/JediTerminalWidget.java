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

package org.apache.hop.ui.hopgui.terminal;

import com.jediterm.core.util.TermSize;
import com.jediterm.terminal.ProcessTtyConnector;
import com.jediterm.terminal.TerminalColor;
import com.jediterm.terminal.TextStyle;
import com.jediterm.terminal.model.StyleState;
import com.jediterm.terminal.model.TerminalTextBuffer;
import com.jediterm.terminal.ui.JediTermWidget;
import com.jediterm.terminal.ui.TerminalPanel;
import com.jediterm.terminal.ui.settings.DefaultSettingsProvider;
import com.jediterm.terminal.ui.settings.SettingsProvider;
import com.pty4j.PtyProcess;
import com.pty4j.PtyProcessBuilder;
import com.pty4j.WinSize;
import java.awt.Font;
import java.awt.Frame;
import java.awt.GraphicsEnvironment;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.core.PropsUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.awt.SWT_AWT;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.TraverseListener;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;

/** JediTerm-based terminal widget using SWT-AWT bridge for embedding AWT components in SWT. */
public class JediTerminalWidget implements ITerminalWidget {

  private static final LogChannel log = new LogChannel("JediTerminal");

  private final String shellPath;
  private final String workingDirectory;

  /** Font scale percent (100 = base size). Mutable so toolbar +/- can update all tabs. */
  private final AtomicInteger fontScalePercent = new AtomicInteger(100);

  private Composite bridgeComposite;
  private Frame awtFrame;
  private JediTermWidget jediTermWidget;
  private PtyProcess ptyProcess;
  private Pty4JTtyConnector ttyConnector;

  public JediTerminalWidget(Composite parent, String shellPath, String workingDirectory) {
    this(parent, shellPath, workingDirectory, 100);
  }

  public JediTerminalWidget(
      Composite parent, String shellPath, String workingDirectory, int fontScalePercent) {
    this.shellPath = shellPath;
    this.workingDirectory = workingDirectory;
    this.fontScalePercent.set(Math.clamp(fontScalePercent, 50, 200));

    createWidget(parent, parent.getDisplay());

    // Defer shell process start to avoid blocking UI
    parent
        .getDisplay()
        .asyncExec(
            () -> {
              if (!bridgeComposite.isDisposed()) {
                startShellProcess();
              }
            });
  }

  public void createWidget(Composite parent, Display display) {
    // Create SWT_AWT bridge composite
    bridgeComposite = new Composite(parent, SWT.EMBEDDED | SWT.NO_BACKGROUND);
    PropsUi.setLook(bridgeComposite);

    // Layout the bridge composite to fill parent
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 0);
    fd.right = new FormAttachment(100, 0);
    fd.top = new FormAttachment(0, 0);
    fd.bottom = new FormAttachment(100, 0);
    bridgeComposite.setLayoutData(fd);

    // Create AWT Frame for JediTerm
    awtFrame = SWT_AWT.new_Frame(bridgeComposite);

    // Resize AWT frame when composite resizes
    bridgeComposite.addListener(
        SWT.Resize,
        event -> {
          if (bridgeComposite.isDisposed() || awtFrame == null) {
            return;
          }
          try {
            Rectangle rect = bridgeComposite.getClientArea();
            java.awt.EventQueue.invokeLater(
                () -> {
                  if (awtFrame != null) {
                    try {
                      awtFrame.setSize(rect.width, rect.height);
                      awtFrame.validate();
                    } catch (Exception e) {
                      log.logDebug("Error resizing AWT frame: " + e.getMessage());
                    }
                  }
                });
          } catch (Exception e) {
            log.logDebug("Error in resize handler: " + e.getMessage());
          }
        });

    // Create JediTerm widget with Hop dark mode support
    boolean isDarkMode = PropsUi.getInstance().isDarkMode();
    DefaultSettingsProvider settings = createHopSettingsProvider(isDarkMode, display);
    jediTermWidget =
        new JediTermWidget(settings) {
          @Override
          protected TerminalPanel createTerminalPanel(
              SettingsProvider settingsProvider,
              StyleState styleState,
              TerminalTextBuffer terminalTextBuffer) {
            return new FallbackFontTerminalPanel(settingsProvider, terminalTextBuffer, styleState);
          }
        };
    awtFrame.add(jediTermWidget);

    awtFrame.setVisible(true);

    // Defer focus until after terminal initialization
    bridgeComposite.addListener(
        SWT.FocusIn,
        event ->
            display.asyncExec(
                () -> {
                  if (bridgeComposite.isDisposed() || jediTermWidget == null) {
                    return;
                  }
                  new Thread(
                          () -> {
                            try {
                              java.awt.EventQueue.invokeLater(
                                  () -> {
                                    if (jediTermWidget != null) {
                                      try {
                                        jediTermWidget.requestFocusInWindow();
                                      } catch (Exception e) {
                                        log.logDebug("Could not request focus: " + e.getMessage());
                                      }
                                    }
                                  });
                            } catch (Exception e) {
                              log.logDebug("Error requesting AWT focus: " + e.getMessage());
                            }
                          })
                      .start();
                }));

    // Override tab listener and send it to jediTerm
    if (Const.isLinux()) {
      TraverseListener tabTraverse =
          e -> {
            if (e.detail == SWT.TRAVERSE_TAB_NEXT || e.detail == SWT.TRAVERSE_TAB_PREVIOUS) {
              e.doit = false;
            }
          };
      KeyAdapter tabKey =
          new KeyAdapter() {
            @Override
            public void keyPressed(KeyEvent e) {
              if (e.keyCode == SWT.TAB) {
                sendTabToShell();
              }
            }
          };
      bridgeComposite.addTraverseListener(tabTraverse);
      bridgeComposite.addKeyListener(tabKey);
      parent.addTraverseListener(tabTraverse);
      parent.addKeyListener(tabKey);
    }
  }

  /** Send Tab character directly to the shell PTY so completion works (used on Linux). */
  private void sendTabToShell() {
    final Pty4JTtyConnector connector = ttyConnector;
    if (connector == null || !connector.isConnected()) {
      return;
    }
    try {
      connector.write("\t");
    } catch (IOException e) {
      log.logDebug("Could not send Tab to terminal: " + e.getMessage());
    }
  }

  /**
   * On Linux, SWT follows the desktop scale (GTK window scaling) but AWT does not, so on a HiDPI
   * screen the terminal font comes out smaller than the rest of Hop. Return the factor that makes
   * up the difference: the SWT zoom divided by the AWT scale. On Windows and macOS AWT scales
   * itself, so the factor is 1.
   */
  private float getHiDpiFontScale() {
    if (!Const.isLinux()) {
      return 1.0f;
    }
    try {
      double awtScale =
          GraphicsEnvironment.getLocalGraphicsEnvironment()
              .getDefaultScreenDevice()
              .getDefaultConfiguration()
              .getDefaultTransform()
              .getScaleX();
      int swtZoom = bridgeComposite.getMonitor().getZoom();
      float scale = (float) Math.max(1.0, swtZoom / (100.0 * awtScale));
      if (scale != 1.0f) {
        log.logDetailed(
            "JediTerm: SWT zoom " + swtZoom + "%, AWT scale " + awtScale + ", font scale " + scale);
      }
      return scale;
    } catch (Exception e) {
      log.logDebug("Could not determine HiDPI font scale: " + e.getMessage());
      return 1.0f;
    }
  }

  /**
   * Create SettingsProvider with Hop theme and fixed-width font; font size uses fontScalePercent.
   */
  private DefaultSettingsProvider createHopSettingsProvider(
      final boolean isDarkMode, final Display display) {
    final String fontName = java.awt.Font.MONOSPACED;
    final int baseFontHeight = 13;

    float fontScale = 1.0f;
    String manualScale = System.getProperty("JediTerm.fontScale");
    if (manualScale != null && !manualScale.isEmpty()) {
      try {
        fontScale = Float.parseFloat(manualScale);
        log.logBasic("JediTerm: Using font scale from system property: " + fontScale);
      } catch (NumberFormatException e) {
        log.logError("JediTerm: Invalid JediTerm.fontScale value: " + manualScale, e);
      }
    }
    final float systemPropScale = fontScale;
    final float hiDpiScale = getHiDpiFontScale();

    final int finalAwtStyle = java.awt.Font.PLAIN;

    return new DefaultSettingsProvider() {
      private int computeFontSize() {
        return Math.round(
            baseFontHeight * (fontScalePercent.get() / 100f) * systemPropScale * hiDpiScale);
      }

      @Override
      public TerminalColor getDefaultBackground() {
        if (Const.isWindows() && !isDarkMode) {
          // Windows light mode: PowerShell blue background
          return TerminalColor.rgb(1, 36, 86);
        } else if (Const.isWindows() && isDarkMode) {
          // Windows dark mode: dark gray background
          return TerminalColor.rgb(43, 43, 43);
        } else if (isDarkMode) {
          // Mac/Linux dark mode: dark gray background
          return TerminalColor.rgb(43, 43, 43);
        } else {
          // Mac/Linux light mode: white background
          return TerminalColor.rgb(255, 255, 255);
        }
      }

      @Override
      public TerminalColor getDefaultForeground() {
        if (Const.isWindows() && !isDarkMode) {
          // Windows light mode: PowerShell gray foreground
          return TerminalColor.rgb(204, 204, 204);
        } else if (Const.isWindows() && isDarkMode) {
          // Windows dark mode: light gray foreground
          return TerminalColor.rgb(187, 187, 187);
        } else if (isDarkMode) {
          // Mac/Linux dark mode: light gray foreground
          return TerminalColor.rgb(187, 187, 187);
        } else {
          // Mac/Linux light mode: black foreground
          return TerminalColor.rgb(0, 0, 0);
        }
      }

      /**
       * JediTerm seeds its style state (which paints the block cursor as the inverse of the default
       * style) from this method, not from the foreground/background getters above. Without it the
       * cursor stays black, which is invisible on a dark background.
       */
      @Override
      public TextStyle getDefaultStyle() {
        return new TextStyle(getDefaultForeground(), getDefaultBackground());
      }

      @Override
      public TextStyle getFoundPatternColor() {
        TerminalColor bg = TerminalColor.rgb(255, 255, 0);
        TerminalColor fg = TerminalColor.rgb(0, 0, 0);
        return new TextStyle(fg, bg);
      }

      @Override
      public TextStyle getHyperlinkColor() {
        TerminalColor linkColor =
            isDarkMode ? TerminalColor.rgb(96, 161, 255) : TerminalColor.rgb(0, 0, 238);
        return new TextStyle(linkColor, null);
      }

      @Override
      public java.awt.Font getTerminalFont() {
        return new java.awt.Font(fontName, finalAwtStyle, computeFontSize());
      }

      @Override
      public float getTerminalFontSize() {
        return (float) computeFontSize();
      }
    };
  }

  @Override
  public void setFontScalePercent(int percent) {
    int clamped = Math.clamp(percent, 50, 200);
    fontScalePercent.set(clamped);
    if (jediTermWidget == null) {
      return;
    }
    // The DefaultSettingsProvider already reads fontScalePercent via computeFontSize(),
    // so updating the AtomicInteger is enough for the provider. We now need to tell
    // TerminalPanel to re-read the font from the provider and recalculate its character
    // grid. TerminalPanel.reinitFontAndResize() does exactly that but is protected,
    // so we call it via reflection.
    java.awt.EventQueue.invokeLater(
        () -> {
          try {
            TerminalPanel panel = jediTermWidget.getTerminalPanel();
            MethodUtils.invokeMethod(panel, true, "reinitFontAndResize");
          } catch (Exception e) {
            log.logDebug("Terminal font reinit: " + e.getMessage());
          }
        });
  }

  public void startShellProcess() {
    try {
      // Build PTY process
      String[] command = getShellCommand();
      Map<String, String> env = new HashMap<>(System.getenv());
      env.put("TERM", "xterm-256color");

      PtyProcessBuilder builder =
          new PtyProcessBuilder()
              .setCommand(command)
              .setDirectory(workingDirectory)
              .setEnvironment(env)
              .setInitialColumns(80)
              .setInitialRows(24);

      ptyProcess = builder.start();

      // Create connector to bridge Pty4J and JediTerm
      ttyConnector = new Pty4JTtyConnector(ptyProcess);

      // Start the terminal session
      jediTermWidget.setTtyConnector(ttyConnector);
      jediTermWidget.start();

      // Request focus after terminal initialization
      Thread.ofVirtual()
          .start(
              () -> {
                try {
                  Thread.sleep(100);
                  java.awt.EventQueue.invokeLater(
                      () -> {
                        if (jediTermWidget != null) {
                          try {
                            jediTermWidget.requestFocusInWindow();
                          } catch (Exception e) {
                            log.logDebug("Could not request initial focus: " + e.getMessage());
                          }
                        }
                      });
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                } catch (Exception e) {
                  log.logDebug("Error requesting initial AWT focus: " + e.getMessage());
                }
              });

    } catch (Exception e) {
      log.logError("Error starting JediTerm shell process", e);
    }
  }

  private String[] getShellCommand() {
    String shell = shellPath != null ? shellPath : TerminalShellDetector.detectDefaultShell();

    if (Const.isWindows()) {
      if (shell.toLowerCase().contains("powershell")) {
        return new String[] {shell, "-NoLogo"};
      } else {
        return new String[] {shell};
      }
    } else {
      return new String[] {shell, "-i", "-l"};
    }
  }

  public void sendRawInput(String input) {
    // JediTerm handles input directly
  }

  @Override
  public void dispose() {
    try {
      if (bridgeComposite != null && !bridgeComposite.isDisposed()) {
        bridgeComposite.dispose();
      }

      // Clean up in a background thread. close() (not stop()) also closes the connector, which
      // destroys the shell, and shuts down JediTerm's executor threads and repaint timer.
      final JediTermWidget termWidget = jediTermWidget;
      final PtyProcess process = ptyProcess;

      new Thread(
              () -> {
                try {
                  if (termWidget != null) {
                    termWidget.close();
                  }
                  if (process != null && process.isAlive()) {
                    process.destroy();
                  }
                } catch (Exception e) {
                  log.logError("Error cleaning up JediTerm PTY", e);
                }
              })
          .start();

    } catch (Exception e) {
      log.logError("Error disposing JediTerm", e);
    }
  }

  @Override
  public Composite getTerminalComposite() {
    return bridgeComposite;
  }

  @Override
  public StyledText getOutputText() {
    // JediTerm uses AWT, not StyledText
    return null;
  }

  @Override
  public String getTerminalType() {
    return "JediTerm (POC)";
  }

  @Override
  public boolean isConnected() {
    return ptyProcess != null && ptyProcess.isAlive();
  }

  @Override
  public String getShellPath() {
    return shellPath;
  }

  @Override
  public String getWorkingDirectory() {
    return workingDirectory;
  }

  /**
   * TerminalPanel that falls back to another installed font for characters the terminal font cannot
   * draw. On Linux, Java's logical Monospaced font leaves out the fallback fonts fontconfig would
   * use, so CJK and similar characters otherwise render as empty boxes.
   */
  private static class FallbackFontTerminalPanel extends TerminalPanel {

    /** Fallback per code point; empty when no installed font can draw it. Shared by all tabs. */
    private static final Map<Integer, Optional<Font>> FALLBACK_FONTS = new ConcurrentHashMap<>();

    FallbackFontTerminalPanel(
        SettingsProvider settingsProvider,
        TerminalTextBuffer terminalTextBuffer,
        StyleState styleState) {
      super(settingsProvider, terminalTextBuffer, styleState);
    }

    @Override
    protected Font getFontToDisplay(char[] text, int start, int end, TextStyle style) {
      Font font = super.getFontToDisplay(text, start, end, style);
      if (font.canDisplayUpTo(text, start, end) == -1) {
        return font;
      }
      return FALLBACK_FONTS
          .computeIfAbsent(
              Character.codePointAt(text, start), codePoint -> findFallbackFont(text, start, end))
          .map(fallback -> fallback.deriveFont(font.getStyle(), font.getSize2D()))
          .orElse(font);
    }

    /** First installed font family that can draw the text, preferring a monospaced one. */
    private static Optional<Font> findFallbackFont(char[] text, int start, int end) {
      Font fallback = null;
      for (String family :
          GraphicsEnvironment.getLocalGraphicsEnvironment().getAvailableFontFamilyNames()) {
        Font candidate = new Font(family, Font.PLAIN, 1);
        if (candidate.canDisplayUpTo(text, start, end) == -1) {
          if (family.toLowerCase().contains("mono")) {
            return Optional.of(candidate);
          }
          if (fallback == null) {
            fallback = candidate;
          }
        }
      }
      return Optional.ofNullable(fallback);
    }
  }

  /**
   * Adapter connecting Pty4J to JediTerm's TtyConnector. ProcessTtyConnector decodes the output
   * with a stateful reader, so multi-byte characters split across reads stay intact.
   */
  private static class Pty4JTtyConnector extends ProcessTtyConnector {

    private final PtyProcess ptyProcess;

    public Pty4JTtyConnector(PtyProcess ptyProcess) {
      super(ptyProcess, StandardCharsets.UTF_8);
      this.ptyProcess = ptyProcess;
    }

    @Override
    public String getName() {
      return "Pty4J Connector";
    }

    /**
     * Pass the terminal size on to the PTY so the shell wraps lines at the visible width. JediTerm
     * only calls this overload; its default implementation throws.
     */
    @Override
    public void resize(TermSize termSize) {
      if (ptyProcess.isAlive()) {
        try {
          ptyProcess.setWinSize(new WinSize(termSize.getColumns(), termSize.getRows()));
        } catch (Exception e) {
          log.logError("Error resizing PTY", e);
        }
      }
    }
  }
}
