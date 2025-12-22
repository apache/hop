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

import com.jediterm.terminal.TerminalColor;
import com.jediterm.terminal.TextStyle;
import com.jediterm.terminal.TtyConnector;
import com.jediterm.terminal.ui.JediTermWidget;
import com.jediterm.terminal.ui.settings.DefaultSettingsProvider;
import com.pty4j.PtyProcess;
import com.pty4j.PtyProcessBuilder;
import com.pty4j.WinSize;
import java.awt.Frame;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.core.PropsUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.awt.SWT_AWT;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;

/**
 * JediTerm-based terminal widget (POC).
 *
 * <p>Uses JetBrains JediTerm library for full VT100/xterm emulation. This is embedded via SWT_AWT
 * bridge.
 *
 * <p>Uses JetBrains JediTerm library for full VT100/xterm emulation with PTY support.
 */
public class JediTerminalWidget implements ITerminalWidget {

  private static final LogChannel log = new LogChannel("JediTerminal");

  private final String shellPath;
  private final String workingDirectory;

  private Composite bridgeComposite;
  private Frame awtFrame;
  private JediTermWidget jediTermWidget;
  private PtyProcess ptyProcess;
  private Pty4JTtyConnector ttyConnector;

  public JediTerminalWidget(Composite parent, String shellPath, String workingDirectory) {
    this.shellPath = shellPath;
    this.workingDirectory = workingDirectory;

    createWidget(parent, parent.getDisplay());

    // Defer shell process start to avoid blocking UI during terminal creation
    // This significantly improves perceived startup time for the first terminal
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

    // Create JediTerm widget with Hop dark mode support
    boolean isDarkMode = PropsUi.getInstance().isDarkMode();
    DefaultSettingsProvider settings = createHopSettingsProvider(isDarkMode, display);
    jediTermWidget = new JediTermWidget(settings);
    awtFrame.add(jediTermWidget);

    // Make the frame visible and focusable
    awtFrame.setVisible(true);

    // Request focus on the JediTerm widget (needed for keyboard input)
    java.awt.EventQueue.invokeLater(
        () -> {
          jediTermWidget.requestFocusInWindow();
          jediTermWidget.requestFocus();
        });

    // Add SWT focus listener to forward focus to AWT component
    bridgeComposite.addListener(
        org.eclipse.swt.SWT.FocusIn,
        event -> {
          java.awt.EventQueue.invokeLater(
              () -> {
                if (jediTermWidget != null) {
                  jediTermWidget.requestFocusInWindow();
                }
              });
        });
  }

  /** Create a SettingsProvider that respects Hop's dark mode setting */
  private DefaultSettingsProvider createHopSettingsProvider(
      final boolean isDarkMode, Display display) {
    // Get Windows system colors if on Windows and not in dark mode
    RGB windowsBg = null;
    RGB windowsFg = null;
    if (Const.isWindows() && !isDarkMode && display != null) {
      try {
        // Get Windows system colors for console (white background, black foreground)
        org.eclipse.swt.graphics.Color bgColor = display.getSystemColor(SWT.COLOR_WHITE);
        org.eclipse.swt.graphics.Color fgColor = display.getSystemColor(SWT.COLOR_BLACK);
        if (bgColor != null && fgColor != null) {
          windowsBg = bgColor.getRGB();
          windowsFg = fgColor.getRGB();
        }
      } catch (Exception e) {
        // Fall back to defaults if system colors unavailable
      }
    }

    final RGB finalWindowsBg = windowsBg;
    final RGB finalWindowsFg = windowsFg;

    return new DefaultSettingsProvider() {
      @Override
      public TerminalColor getDefaultBackground() {
        if (Const.isWindows() && !isDarkMode && finalWindowsBg != null) {
          // Windows light mode: Use system white color
          return new TerminalColor(finalWindowsBg.red, finalWindowsBg.green, finalWindowsBg.blue);
        } else if (Const.isWindows() && isDarkMode) {
          // Windows dark mode: dark gray background
          return new TerminalColor(43, 43, 43);
        } else if (isDarkMode) {
          // Mac/Linux dark mode: dark gray background
          return new TerminalColor(43, 43, 43);
        } else {
          // Mac/Linux light mode: white background
          return new TerminalColor(255, 255, 255);
        }
      }

      @Override
      public TerminalColor getDefaultForeground() {
        if (Const.isWindows() && !isDarkMode && finalWindowsFg != null) {
          // Windows light mode: Use system black color
          return new TerminalColor(finalWindowsFg.red, finalWindowsFg.green, finalWindowsFg.blue);
        } else if (Const.isWindows() && isDarkMode) {
          // Windows dark mode: light gray foreground
          return new TerminalColor(187, 187, 187);
        } else if (isDarkMode) {
          // Mac/Linux dark mode: light gray foreground
          return new TerminalColor(187, 187, 187);
        } else {
          // Mac/Linux light mode: black foreground
          return new TerminalColor(0, 0, 0);
        }
      }

      @Override
      public TextStyle getFoundPatternColor() {
        // Yellow highlight for search results
        TerminalColor bg = new TerminalColor(255, 255, 0);
        TerminalColor fg = new TerminalColor(0, 0, 0);
        return new TextStyle(fg, bg);
      }

      @Override
      public TextStyle getHyperlinkColor() {
        // Dark: bright blue, Light: dark blue
        TerminalColor linkColor =
            isDarkMode ? new TerminalColor(96, 161, 255) : new TerminalColor(0, 0, 238);
        return new TextStyle(linkColor, null);
      }
    };
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

    } catch (Exception e) {
      log.logError("Error starting JediTerm shell process", e);
    }
  }

  private String[] getShellCommand() {
    String shell = shellPath != null ? shellPath : TerminalShellDetector.detectDefaultShell();

    if (Const.isWindows()) {
      // Windows: PowerShell or cmd
      if (shell.toLowerCase().contains("powershell")) {
        // Use -NoLogo and -NoExit with -Command to set colors and keep PowerShell interactive
        // This prevents PowerShell's default yellow text color which is hard to read on white
        return new String[] {
          shell,
          "-NoLogo",
          "-NoExit",
          "-Command",
          "$Host.UI.RawUI.ForegroundColor = 'Black'; $Host.UI.RawUI.BackgroundColor = 'White'"
        };
      } else {
        return new String[] {shell};
      }
    } else {
      // Unix-like: bash, zsh, etc. - start as interactive login shell
      return new String[] {shell, "-i", "-l"};
    }
  }

  public void sendRawInput(String input) {
    // Not needed - JediTerm handles input directly
  }

  @Override
  public void dispose() {
    try {
      // Dispose SWT/AWT widgets immediately (must be on UI thread, but is fast)
      if (bridgeComposite != null && !bridgeComposite.isDisposed()) {
        bridgeComposite.dispose(); // This also disposes awtFrame
      }

      // Clean up PTY/terminal in background (can be slow, don't block)
      final JediTermWidget termWidget = jediTermWidget;
      final Pty4JTtyConnector connector = ttyConnector;
      final PtyProcess process = ptyProcess;

      new Thread(
              () -> {
                try {
                  if (termWidget != null) {
                    termWidget.stop();
                  }
                  if (connector != null) {
                    connector.close();
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
    // JediTerm doesn't expose a StyledText widget (it's AWT/Swing)
    // Return null - this is only used for focus in HopGuiTerminalPanel
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
   * Adapter to connect Pty4J processes to JediTerm's TtyConnector interface.
   *
   * <p>JediTerm expects a TtyConnector, but we use Pty4J for PTY management.
   */
  private static class Pty4JTtyConnector implements TtyConnector {

    private final PtyProcess ptyProcess;
    private final InputStream inputStream;
    private final OutputStream outputStream;

    public Pty4JTtyConnector(PtyProcess ptyProcess) {
      this.ptyProcess = ptyProcess;
      this.inputStream = ptyProcess.getInputStream();
      this.outputStream = ptyProcess.getOutputStream();
    }

    public boolean init() {
      return true; // PTY already initialized by Pty4J
    }

    public void close() {
      try {
        if (ptyProcess != null && ptyProcess.isAlive()) {
          ptyProcess.destroy();
        }
      } catch (Exception e) {
        log.logError("Error closing Pty4J connector", e);
      }
    }

    public String getName() {
      return "Pty4J Connector";
    }

    public int read(char[] buf, int offset, int length) throws IOException {
      byte[] bytes = new byte[length];
      int bytesRead = inputStream.read(bytes, 0, length);
      if (bytesRead > 0) {
        String str = new String(bytes, 0, bytesRead, StandardCharsets.UTF_8);
        char[] chars = str.toCharArray();
        System.arraycopy(chars, 0, buf, offset, Math.min(chars.length, length));
        return chars.length;
      }
      return bytesRead;
    }

    public void write(byte[] bytes) throws IOException {
      outputStream.write(bytes);
      outputStream.flush();
    }

    public boolean isConnected() {
      return ptyProcess != null && ptyProcess.isAlive();
    }

    public void write(String string) throws IOException {
      write(string.getBytes(StandardCharsets.UTF_8));
    }

    public int waitFor() throws InterruptedException {
      return ptyProcess.waitFor();
    }

    public boolean ready() throws IOException {
      return inputStream.available() > 0;
    }

    public void resize(int cols, int rows) {
      if (ptyProcess != null && ptyProcess.isAlive()) {
        try {
          ptyProcess.setWinSize(new WinSize(cols, rows));
        } catch (Exception e) {
          log.logError("Error resizing PTY", e);
        }
      }
    }
  }
}
