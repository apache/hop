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

import com.pty4j.PtyProcess;
import com.pty4j.PtyProcessBuilder;
import com.pty4j.WinSize;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.core.PropsUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.dnd.Clipboard;
import org.eclipse.swt.dnd.TextTransfer;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;

/**
 * Advanced terminal widget using Pty4J for full PTY support.
 *
 * <p>This terminal provides: - Full shell experience with custom prompts (Oh My Zsh, Starship,
 * etc.) - Tab completion - Command history - 24-bit color support - Interactive programs support
 * (experimental)
 *
 * <p>Note: This is the advanced terminal. For a simpler, more reliable console, see {@link
 * SimpleTerminalWidget}.
 */
public class TerminalWidget implements ITerminalWidget {

  private static final Class<?> PKG = TerminalWidget.class;
  private final ILogChannel log;

  private final Composite parent;
  private final String shellPath;
  private String workingDirectory;

  private Composite terminalComposite;
  private StyledText outputText; // Used for display only - PTY handles all text

  // Flag to allow programmatic text changes (from PTY) but block user typing
  private volatile boolean allowTextChange = false;

  private PtyProcess shellProcess;
  private BufferedWriter processInput;
  private BufferedReader processOutput;

  // PTY window size (rows x columns)
  private int terminalRows = 24;
  private int terminalCols = 80;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private Thread outputThread;

  // ANSI escape code handler for terminal output
  private AnsiEscapeCodeHandler ansiHandler;

  // No longer need command history - PTY handles it

  /**
   * Constructor - Creates and initializes the terminal widget
   *
   * @param parent Parent composite to render terminal in
   * @param shellPath Full path to shell executable
   * @param workingDirectory Initial working directory
   */
  public TerminalWidget(Composite parent, String shellPath, String workingDirectory) {
    this.parent = parent;
    this.shellPath = shellPath;
    this.workingDirectory = workingDirectory;
    this.log = new LogChannel("Terminal");

    createTerminal();
  }

  /** Create the terminal UI and start shell process */
  private void createTerminal() {
    try {
      // Create composite for terminal
      terminalComposite = new Composite(parent, SWT.NONE);
      terminalComposite.setLayout(new FormLayout());
      FormData fdTerminal = new FormData();
      fdTerminal.left = new FormAttachment(0, 0);
      fdTerminal.top = new FormAttachment(0, 0);
      fdTerminal.right = new FormAttachment(100, 0);
      fdTerminal.bottom = new FormAttachment(100, 0);
      terminalComposite.setLayoutData(fdTerminal);

      // Create terminal text area (EDITABLE for both input and output)
      outputText =
          new StyledText(terminalComposite, SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL | SWT.BORDER);
      PropsUi.setLook(outputText);

      // Load terminal font (Nerd Font for Powerline symbols, fallback to system monospace)
      Font terminalFont = loadTerminalFont(parent.getDisplay());
      outputText.setFont(terminalFont);

      // Dark terminal colors
      Color bgColor = new Color(parent.getDisplay(), 30, 30, 30);
      Color fgColor = new Color(parent.getDisplay(), 200, 200, 200);
      outputText.setBackground(bgColor);
      outputText.setForeground(fgColor);

      // Fill entire composite - no separate input area
      FormData fdOutput = new FormData();
      fdOutput.left = new FormAttachment(0, 0);
      fdOutput.top = new FormAttachment(0, 0);
      fdOutput.right = new FormAttachment(100, 0);
      fdOutput.bottom = new FormAttachment(100, 0);
      outputText.setLayoutData(fdOutput);

      // Handle resize events - update PTY window size
      outputText.addListener(SWT.Resize, e -> updatePtyWindowSize());

      // Mark this as a terminal widget so HopGuiKeyHandler won't interfere
      outputText.setData("HOP_TERMINAL_WIDGET", Boolean.TRUE);

      // Initialize ANSI escape code handler with text change callbacks
      ansiHandler =
          new AnsiEscapeCodeHandler(
              outputText, () -> allowTextChange = true, () -> allowTextChange = false);

      // Add key listener for character-by-character input
      outputText.addKeyListener(
          new KeyAdapter() {
            @Override
            public void keyPressed(KeyEvent e) {
              handleKeyPress(e);
            }
          });

      // Add verify listener to BLOCK ALL user typing (we send keystrokes to PTY instead)
      // BUT allow paste operations
      outputText.addVerifyListener(
          e -> {
            // Allow programmatic changes (from PTY output via ANSI handler)
            if (allowTextChange) {
              return;
            }

            // Allow paste operations - text will be sent to PTY
            if (e.text != null && e.text.length() > 1) {
              // This is likely a paste operation (multiple characters at once)
              sendRawInput(e.text);
              e.doit = false;
              return;
            }

            // Block all other user-initiated text changes
            e.doit = false;
          });

      // Set focus to terminal
      Display display = parent.getDisplay();
      display.asyncExec(
          () -> {
            if (outputText != null && !outputText.isDisposed()) {
              outputText.setFocus();
              outputText.forceFocus();
            }
          });

      // Start shell process
      startShellProcess();

      log.logBasic("Terminal created: " + shellPath + " in " + workingDirectory);

    } catch (Exception e) {
      log.logError("Error creating terminal widget", e);
      createErrorComposite("Error: " + e.getMessage());
    }
  }

  /** Handle key press - character-by-character terminal input */
  private void handleKeyPress(KeyEvent e) {
    // All keystrokes go to PTY, nothing displayed locally

    // Handle copy separately - don't send to PTY
    if ((e.stateMask & SWT.MOD1) != 0 && (e.keyCode == 'c' || e.keyCode == 'C')) {
      // Cmd+C on Mac, Ctrl+C on Windows/Linux for COPY (not signal)
      if (outputText.getSelectionCount() > 0) {
        // User has text selected - allow copy to clipboard
        e.doit = true;
        return;
      }
      // No selection - send Ctrl+C signal to PTY
      e.doit = false;
      sendRawInput("\u0003");
      return;
    }

    // Handle paste - Cmd+V / Ctrl+V
    if ((e.stateMask & SWT.MOD1) != 0 && (e.keyCode == 'v' || e.keyCode == 'V')) {
      e.doit = false;
      // Use asyncExec to ensure paste happens after event processing
      Display.getDefault().asyncExec(() -> pasteIntoTerminal());
      return;
    }

    if (e.keyCode == SWT.CR || e.keyCode == SWT.LF) {
      // Enter pressed - send to PTY
      e.doit = false;
      sendRawInput("\n");
    } else if (e.keyCode == SWT.TAB) {
      // Tab pressed - send to PTY for completion
      e.doit = false;
      sendRawInput("\t");
    } else if (e.keyCode == SWT.ARROW_UP) {
      // Up arrow - let PTY handle command history
      e.doit = false;
      sendRawInput("\033[A");
    } else if (e.keyCode == SWT.ARROW_DOWN) {
      // Down arrow - let PTY handle command history
      e.doit = false;
      sendRawInput("\033[B");
    } else if (e.keyCode == SWT.ARROW_LEFT) {
      // Left arrow
      e.doit = false;
      sendRawInput("\033[D");
    } else if (e.keyCode == SWT.ARROW_RIGHT) {
      // Right arrow
      e.doit = false;
      sendRawInput("\033[C");
    } else if (e.keyCode == SWT.BS) {
      // Backspace
      e.doit = false;
      sendRawInput("\b");
    } else if (e.keyCode == SWT.DEL) {
      // Delete key
      e.doit = false;
      sendRawInput("\033[3~");
    } else if ((e.stateMask & SWT.CTRL) != 0) {
      // Ctrl+key combinations (signals, not copy/paste)
      e.doit = false;
      if (e.keyCode == 'c' || e.keyCode == 'C') {
        sendRawInput("\u0003"); // Ctrl+C signal
      } else if (e.keyCode == 'd' || e.keyCode == 'D') {
        sendRawInput("\u0004"); // Ctrl+D
      } else if (e.keyCode == 'z' || e.keyCode == 'Z') {
        sendRawInput("\u001A"); // Ctrl+Z
      } else if (e.keyCode == 'l' || e.keyCode == 'L') {
        sendRawInput("\f"); // Ctrl+L (clear)
      }
    } else if (e.character != 0 && !Character.isISOControl(e.character)) {
      // Regular character - send to PTY
      e.doit = false;
      sendRawInput(String.valueOf(e.character));
    }
  }

  /** Paste clipboard content into terminal */
  private void pasteIntoTerminal() {
    try {
      Clipboard clipboard = new Clipboard(outputText.getDisplay());
      TextTransfer textTransfer = TextTransfer.getInstance();
      String clipboardText = (String) clipboard.getContents(textTransfer);
      clipboard.dispose();

      if (clipboardText != null && !clipboardText.isEmpty()) {
        log.logDebug("Pasting " + clipboardText.length() + " characters into terminal");
        sendRawInput(clipboardText);
      }
    } catch (Exception e) {
      log.logError("Error pasting into terminal", e);
    }
  }

  /** Send raw input directly to PTY (for character-by-character mode) */
  private void sendRawInput(String input) {
    if (processInput == null || !running.get()) {
      return;
    }

    try {
      processInput.write(input);
      processInput.flush();
    } catch (IOException e) {
      log.logError("Error sending input to PTY", e);
    }
  }

  /** Start the shell process with PTY (Pseudo-Terminal) support */
  private void startShellProcess() {
    try {
      // Prepare environment variables
      Map<String, String> env = new HashMap<>(System.getenv());

      // Set TERM variable for proper terminal emulation
      if (!env.containsKey("TERM")) {
        env.put("TERM", "xterm-256color");
      }

      // Configure shell command for PTY mode
      String[] command;
      String os = System.getProperty("os.name").toLowerCase();
      if (os.contains("win")) {
        // Windows: use PowerShell or cmd
        if (shellPath.toLowerCase().contains("powershell")) {
          command = new String[] {shellPath, "-NoLogo"};
        } else {
          command = new String[] {shellPath};
        }
      } else {
        // Unix: use interactive login shell (PTY enables this properly)
        command = new String[] {shellPath, "-i", "-l"};
      }

      // Build and start PTY process
      shellProcess =
          new PtyProcessBuilder()
              .setCommand(command)
              .setEnvironment(env)
              .setDirectory(workingDirectory)
              .setInitialColumns(terminalCols)
              .setInitialRows(terminalRows)
              .setConsole(false)
              .start();

      running.set(true);

      // Get I/O streams - PTY merges stdout and stderr into single stream
      processInput =
          new BufferedWriter(
              new OutputStreamWriter(shellProcess.getOutputStream(), StandardCharsets.UTF_8));
      processOutput =
          new BufferedReader(
              new InputStreamReader(shellProcess.getInputStream(), StandardCharsets.UTF_8));
      // Note: PTY doesn't have separate error stream - stderr is merged with stdout

      // Start output reader thread
      startOutputReaders();

      // No welcome message needed - PTY will show its own prompt

    } catch (IOException e) {
      log.logError("Failed to start PTY shell process", e);
      appendOutput("ERROR: Failed to start shell with PTY\n", SWT.COLOR_RED);
      appendOutput(e.getMessage() + "\n", SWT.COLOR_RED);
    }
  }

  /** Start thread to read PTY output stream (stdout and stderr merged) */
  private void startOutputReaders() {
    // Output reader thread - PTY merges stdout and stderr
    outputThread =
        new Thread(
            () -> {
              try {
                char[] buffer = new char[8192]; // Larger buffer for PTY output
                int bytesRead;
                while (running.get() && (bytesRead = processOutput.read(buffer)) != -1) {
                  final String outputChunk = new String(buffer, 0, bytesRead);
                  Display.getDefault()
                      .asyncExec(
                          () -> {
                            if (ansiHandler != null && !outputText.isDisposed()) {
                              ansiHandler.appendText(outputChunk);
                              // Auto-scroll to bottom
                              outputText.setTopIndex(outputText.getLineCount() - 1);
                            }
                          });
                }
              } catch (IOException e) {
                if (running.get()) {
                  log.logError("Error reading PTY output", e);
                }
              } finally {
                Display.getDefault()
                    .asyncExec(
                        () -> {
                          if (!outputText.isDisposed()) {
                            appendOutput("\n[Process terminated]\n", SWT.COLOR_DARK_GRAY);
                          }
                        });
              }
            });
    outputThread.setDaemon(true);
    outputThread.setName("Terminal-PTY-Output-Reader");
    outputThread.start();
  }

  /**
   * Append text to output with color
   *
   * @param text Text to append
   * @param colorId SWT color constant
   */
  private void appendOutput(String text, int colorId) {
    if (outputText == null || outputText.isDisposed()) {
      return;
    }

    int startOffset = outputText.getCharCount();
    outputText.append(text);
    int endOffset = outputText.getCharCount();

    // Apply color
    if (colorId != SWT.COLOR_WHITE) { // Only style if not default
      StyleRange styleRange = new StyleRange();
      styleRange.start = startOffset;
      styleRange.length = endOffset - startOffset;
      styleRange.foreground = outputText.getDisplay().getSystemColor(colorId);
      outputText.setStyleRange(styleRange);
    }

    // Auto-scroll to bottom
    outputText.setTopIndex(outputText.getLineCount() - 1);
  }

  /**
   * Execute a command in the shell
   *
   * @param command Command to execute
   */
  public void executeCommand(String command) {
    if (processInput == null || !running.get()) {
      appendOutput("ERROR: Terminal not running\n", SWT.COLOR_RED);
      return;
    }

    try {
      // Show command being executed
      appendOutput("$ " + command + "\n", SWT.COLOR_CYAN);

      // Handle special commands
      if (command.startsWith("cd ")) {
        handleCdCommand(command.substring(3).trim());
        return;
      }

      // Send command to shell
      processInput.write(command);
      processInput.newLine();
      processInput.flush();

    } catch (IOException e) {
      log.logError("Error executing command: " + command, e);
      appendOutput("ERROR: " + e.getMessage() + "\n", SWT.COLOR_RED);
    }
  }

  /**
   * Handle cd (change directory) command
   *
   * @param path Path to change to
   */
  private void handleCdCommand(String path) {
    try {
      File newDir;
      if (path.startsWith("/") || path.matches("[A-Za-z]:.*")) {
        // Absolute path
        newDir = new File(path);
      } else {
        // Relative path
        newDir = new File(workingDirectory, path);
      }

      if (newDir.exists() && newDir.isDirectory()) {
        workingDirectory = newDir.getCanonicalPath();
        appendOutput("Changed directory to: " + workingDirectory + "\n", SWT.COLOR_GREEN);

        // Update shell's working directory (restart needed for actual effect)
        appendOutput("(Note: Restart terminal for cd to take full effect)\n", SWT.COLOR_YELLOW);
      } else {
        appendOutput("ERROR: Directory not found: " + path + "\n", SWT.COLOR_RED);
      }
    } catch (IOException e) {
      appendOutput("ERROR: " + e.getMessage() + "\n", SWT.COLOR_RED);
    }
  }

  /** Create an error composite when terminal creation fails */
  private void createErrorComposite(String message) {
    org.eclipse.swt.widgets.Label errorLabel =
        new org.eclipse.swt.widgets.Label(terminalComposite, SWT.WRAP);
    errorLabel.setText(
        "Terminal Error:\n\n"
            + message
            + "\n\n"
            + "Check logs for details.\n"
            + "Shell path: "
            + shellPath);
    FormData fdError = new FormData();
    fdError.left = new FormAttachment(0, 10);
    fdError.top = new FormAttachment(0, 10);
    fdError.right = new FormAttachment(100, -10);
    errorLabel.setLayoutData(fdError);
  }

  /** Clear the terminal output */
  public void clearTerminal() {
    if (outputText != null && !outputText.isDisposed()) {
      Display.getDefault().asyncExec(() -> outputText.setText(""));
    }
  }

  /** Kill the terminal process */
  public void killProcess() {
    running.set(false);

    if (shellProcess != null && shellProcess.isAlive()) {
      shellProcess.destroy();
      try {
        if (!shellProcess.waitFor(2, java.util.concurrent.TimeUnit.SECONDS)) {
          shellProcess.destroyForcibly();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        shellProcess.destroyForcibly();
      }
    }

    appendOutput("\n[Terminal process terminated]\n", SWT.COLOR_YELLOW);
    log.logBasic("Terminal process killed");
  }

  /** Dispose of the terminal widget and cleanup resources */
  @Override
  public void dispose() {
    running.set(false);

    // Close streams
    try {
      if (processInput != null) processInput.close();
      if (processOutput != null) processOutput.close();
      // Note: PTY doesn't have separate error stream
    } catch (IOException e) {
      log.logError("Error closing streams", e);
    }

    // Kill process
    if (shellProcess != null && shellProcess.isAlive()) {
      shellProcess.destroy();
    }

    // Wait for threads to finish
    try {
      if (outputThread != null) outputThread.join(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Dispose ANSI handler
    if (ansiHandler != null) {
      ansiHandler.dispose();
    }

    // Dispose composite
    if (terminalComposite != null && !terminalComposite.isDisposed()) {
      terminalComposite.dispose();
    }

    log.logBasic("Terminal widget disposed");
  }

  /** Check if terminal is running */
  public boolean isConnected() {
    return running.get() && shellProcess != null && shellProcess.isAlive();
  }

  /** Get the terminal composite */
  public Composite getTerminalComposite() {
    return terminalComposite;
  }

  /** Get shell path */
  public String getShellPath() {
    return shellPath;
  }

  /** Get working directory */
  public String getWorkingDirectory() {
    return workingDirectory;
  }

  @Override
  public StyledText getOutputText() {
    return outputText;
  }

  @Override
  public String getTerminalType() {
    return "Advanced (PTY)";
  }

  // getInputText() removed - no longer have separate input field

  /**
   * Load terminal font with Powerline symbol support.
   *
   * <p>Attempts to load JetBrains Mono Nerd Font from resources. Falls back to system monospace
   * font if not available.
   *
   * @param display SWT display
   * @return Terminal font
   */
  private Font loadTerminalFont(Display display) {
    Font loadedFont = null;

    try {
      // Try to load JetBrains Mono Nerd Font from resources
      java.io.InputStream fontStream =
          getClass().getResourceAsStream("/ui/fonts/JetBrainsMonoNerdFont-Regular.ttf");

      if (fontStream != null) {
        // Note: SWT doesn't have a simple cross-platform way to load fonts from streams
        // Font must be installed system-wide or we need platform-specific code
        // For now, we'll just check if the Nerd Font is installed system-wide
        fontStream.close();
        log.logDebug("Custom font loading from resources not yet supported in SWT");
        // Fall through to system font detection
      }
    } catch (Exception e) {
      log.logDebug("Could not load Nerd Font, using system monospace: " + e.getMessage());
    }

    // Fallback to system monospace font
    log.logBasic(
        "Using system monospace font (Nerd Font not found). "
            + "Download from: https://github.com/ryanoasis/nerd-fonts");

    // Try platform-specific monospace fonts
    String[] monospaceFonts = {
      "JetBrains Mono", // If installed system-wide
      "Fira Code", // If installed system-wide
      "Consolas", // Windows
      "Monaco", // macOS
      "Menlo", // macOS
      "DejaVu Sans Mono", // Linux
      "Liberation Mono", // Linux
      "Courier New", // Fallback
      "Monospace" // Generic fallback
    };

    for (String fontName : monospaceFonts) {
      try {
        Font testFont = new Font(display, fontName, 11, SWT.NORMAL);
        if (testFont != null) {
          log.logDebug("Using system font: " + fontName);
          return testFont;
        }
      } catch (Exception e) {
        // Try next font
      }
    }

    // Ultimate fallback
    return new Font(display, "Monospace", 11, SWT.NORMAL);
  }

  /** Update PTY window size based on terminal widget size */
  private void updatePtyWindowSize() {
    if (shellProcess != null
        && shellProcess.isAlive()
        && outputText != null
        && !outputText.isDisposed()) {
      try {
        // Calculate rows and columns based on widget size and font metrics
        org.eclipse.swt.graphics.GC gc = new org.eclipse.swt.graphics.GC(outputText);
        org.eclipse.swt.graphics.Point charSize =
            gc.textExtent("M"); // Use 'M' for average char width
        gc.dispose();

        org.eclipse.swt.graphics.Rectangle bounds = outputText.getBounds();
        int cols = Math.max(20, bounds.width / charSize.x);
        int rows = Math.max(5, bounds.height / charSize.y);

        // Only update if size actually changed
        if (cols != terminalCols || rows != terminalRows) {
          terminalCols = cols;
          terminalRows = rows;
          shellProcess.setWinSize(new WinSize(cols, rows));
          log.logDebug("PTY window size updated: " + cols + "x" + rows);
        }
      } catch (Exception e) {
        log.logError("Failed to update PTY window size", e);
      }
    }
  }
}
