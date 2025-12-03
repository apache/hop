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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.core.PropsUi;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Text;

/**
 * Simple terminal widget using ProcessBuilder and SWT StyledText.
 *
 * <p>This terminal console provides: - Command execution with output display - Command history
 * (up/down arrows) - Basic ANSI color support - Multiple output streams (stdout/stderr)
 *
 * <p>Suitable for: - Running hop-run, git, npm, pip, maven commands - Viewing logs (tail -f) - File
 * operations (ls, cd, etc.) - Quick CLI tasks
 *
 * <p>Note: This is not a full terminal emulator (no interactive programs like vim/top/nano). It's
 * designed for command execution and output viewing.
 */
public class TerminalWidget {

  private static final Class<?> PKG = TerminalWidget.class;
  private final ILogChannel log;

  private final Composite parent;
  private final String shellPath;
  private String workingDirectory;

  private Composite terminalComposite;
  private StyledText outputText;
  private Text inputText;
  private org.eclipse.swt.widgets.Label promptLabel;

  private Process shellProcess;
  private BufferedWriter processInput;
  private BufferedReader processOutput;
  private BufferedReader processError;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private Thread outputThread;
  private Thread errorThread;

  private final List<String> commandHistory = new ArrayList<>();
  private int historyIndex = -1;

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

      // Create output text area (read-only, multi-line)
      outputText =
          new StyledText(
              terminalComposite,
              SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL | SWT.READ_ONLY | SWT.BORDER);
      PropsUi.setLook(outputText);

      // Use monospace font
      Font terminalFont = new Font(parent.getDisplay(), "Monospace", 11, SWT.NORMAL);
      outputText.setFont(terminalFont);

      // Dark terminal colors
      Color bgColor = new Color(parent.getDisplay(), 30, 30, 30);
      Color fgColor = new Color(parent.getDisplay(), 200, 200, 200);
      outputText.setBackground(bgColor);
      outputText.setForeground(fgColor);

      FormData fdOutput = new FormData();
      fdOutput.left = new FormAttachment(0, 0);
      fdOutput.top = new FormAttachment(0, 0);
      fdOutput.right = new FormAttachment(100, 0);
      fdOutput.bottom = new FormAttachment(100, -30); // Leave space for input area
      outputText.setLayoutData(fdOutput);

      // Allow clicking on output to focus input
      outputText.addListener(
          SWT.MouseDown,
          e -> {
            if (inputText != null && !inputText.isDisposed()) {
              inputText.forceFocus();
            }
          });

      // Create a composite for the input area at the bottom
      Composite inputComposite = new Composite(terminalComposite, SWT.NONE);
      inputComposite.setLayout(new FormLayout());
      inputComposite.setBackground(bgColor);

      FormData fdInputComposite = new FormData();
      fdInputComposite.left = new FormAttachment(0, 0);
      fdInputComposite.top = new FormAttachment(outputText, 0);
      fdInputComposite.right = new FormAttachment(100, 0);
      fdInputComposite.bottom = new FormAttachment(100, 0);
      fdInputComposite.height = 30; // Fixed height
      inputComposite.setLayoutData(fdInputComposite);

      // Create prompt label ($ or >)
      promptLabel = new org.eclipse.swt.widgets.Label(inputComposite, SWT.NONE);
      promptLabel.setText("$ ");
      promptLabel.setFont(terminalFont);
      promptLabel.setBackground(bgColor);
      promptLabel.setForeground(new Color(parent.getDisplay(), 0, 255, 0)); // Bright green

      FormData fdPrompt = new FormData();
      fdPrompt.left = new FormAttachment(0, 5);
      fdPrompt.top = new FormAttachment(0, 5);
      fdPrompt.bottom = new FormAttachment(100, -5);
      promptLabel.setLayoutData(fdPrompt);

      // Create input text field at bottom (after prompt)
      inputText = new Text(inputComposite, SWT.SINGLE);
      inputText.setFont(terminalFont);
      inputText.setBackground(bgColor);
      inputText.setForeground(new Color(parent.getDisplay(), 255, 255, 255)); // White text
      inputText.setMessage("Type command and press Enter");

      FormData fdInput = new FormData();
      fdInput.left = new FormAttachment(promptLabel, 2);
      fdInput.top = new FormAttachment(0, 3);
      fdInput.right = new FormAttachment(100, -5);
      fdInput.bottom = new FormAttachment(100, -3);
      inputText.setLayoutData(fdInput);

      // Add visual separator line above input area
      org.eclipse.swt.widgets.Label separator =
          new org.eclipse.swt.widgets.Label(terminalComposite, SWT.SEPARATOR | SWT.HORIZONTAL);
      FormData fdSeparator = new FormData();
      fdSeparator.left = new FormAttachment(0, 0);
      fdSeparator.right = new FormAttachment(100, 0);
      fdSeparator.top = new FormAttachment(outputText, 0);
      fdSeparator.height = 2;
      separator.setLayoutData(fdSeparator);

      // CRITICAL: Set focus to input field when terminal is created
      Display display = parent.getDisplay();
      display.asyncExec(
          () -> {
            if (inputText != null && !inputText.isDisposed()) {
              inputText.setFocus();
              inputText.forceFocus();
            }
          });

      // Handle command input
      inputText.addKeyListener(
          new KeyAdapter() {
            @Override
            public void keyPressed(KeyEvent e) {
              handleKeyPress(e);
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

  /** Handle key press in input field */
  private void handleKeyPress(KeyEvent e) {
    if (e.keyCode == SWT.CR || e.keyCode == SWT.LF) {
      // Enter pressed - execute command
      e.doit = false;
      String command = inputText.getText().trim();
      if (!command.isEmpty()) {
        executeCommand(command);
        commandHistory.add(command);
        historyIndex = commandHistory.size();
        inputText.setText("");
      }
    } else if (e.keyCode == SWT.ARROW_UP) {
      // Up arrow - previous command
      e.doit = false;
      if (historyIndex > 0) {
        historyIndex--;
        inputText.setText(commandHistory.get(historyIndex));
        inputText.setSelection(inputText.getText().length());
      }
    } else if (e.keyCode == SWT.ARROW_DOWN) {
      // Down arrow - next command
      e.doit = false;
      if (historyIndex < commandHistory.size() - 1) {
        historyIndex++;
        inputText.setText(commandHistory.get(historyIndex));
        inputText.setSelection(inputText.getText().length());
      } else if (historyIndex == commandHistory.size() - 1) {
        historyIndex = commandHistory.size();
        inputText.setText("");
      }
    }
  }

  /** Start the shell process */
  private void startShellProcess() {
    try {
      ProcessBuilder pb = new ProcessBuilder();

      // Configure shell command - DON'T use interactive mode, it causes terminal control chars
      String os = System.getProperty("os.name").toLowerCase();
      if (os.contains("win")) {
        // Windows: keep shell open
        if (shellPath.toLowerCase().contains("powershell")) {
          pb.command(shellPath, "-NoLogo", "-NoExit");
        } else if (shellPath.toLowerCase().contains("cmd")) {
          pb.command(shellPath, "/Q", "/A");
        } else {
          pb.command(shellPath);
        }
      } else {
        // Unix: use login shell mode (better than interactive for non-PTY)
        // -l for login shell (loads profile but doesn't need PTY)
        pb.command(shellPath, "-l");
      }

      // Set working directory
      pb.directory(new File(workingDirectory));

      // Don't redirect error stream (we want to color it differently)
      pb.redirectErrorStream(false);

      // Start process
      shellProcess = pb.start();
      running.set(true);

      // Get I/O streams
      processInput = new BufferedWriter(new OutputStreamWriter(shellProcess.getOutputStream()));
      processOutput = new BufferedReader(new InputStreamReader(shellProcess.getInputStream()));
      processError = new BufferedReader(new InputStreamReader(shellProcess.getErrorStream()));

      // Start output reader threads
      startOutputReaders();

      // Show welcome message
      appendOutput("═══════════════════════════════════════════════════\n", SWT.COLOR_DARK_GRAY);
      appendOutput("  Hop Terminal Console\n", SWT.COLOR_CYAN);
      appendOutput("═══════════════════════════════════════════════════\n", SWT.COLOR_DARK_GRAY);
      appendOutput("Shell: " + shellPath + "\n", SWT.COLOR_GREEN);
      appendOutput("Working directory: " + workingDirectory + "\n", SWT.COLOR_GREEN);
      appendOutput("\nType commands in the input field at the bottom.\n", SWT.COLOR_YELLOW);
      appendOutput("Press Enter to execute. Use ↑↓ for history.\n\n", SWT.COLOR_YELLOW);
      appendOutput("Ready for commands.\n\n", SWT.COLOR_GREEN);

    } catch (IOException e) {
      log.logError("Failed to start shell process", e);
      appendOutput("ERROR: Failed to start shell\n", SWT.COLOR_RED);
      appendOutput(e.getMessage() + "\n", SWT.COLOR_RED);
    }
  }

  /** Start threads to read process output and error streams */
  private void startOutputReaders() {
    // Output reader thread
    outputThread =
        new Thread(
            () -> {
              try {
                char[] buffer = new char[1024];
                int bytesRead;
                while (running.get() && (bytesRead = processOutput.read(buffer)) != -1) {
                  final String outputChunk = new String(buffer, 0, bytesRead);
                  Display.getDefault().asyncExec(() -> appendOutput(outputChunk, SWT.COLOR_WHITE));
                }
              } catch (IOException e) {
                if (running.get()) {
                  log.logError("Error reading process output", e);
                }
              }
            });
    outputThread.setDaemon(true);
    outputThread.setName("Terminal-Output-Reader");
    outputThread.start();

    // Error reader thread
    errorThread =
        new Thread(
            () -> {
              try {
                char[] buffer = new char[1024];
                int bytesRead;
                while (running.get() && (bytesRead = processError.read(buffer)) != -1) {
                  final String errorChunk = new String(buffer, 0, bytesRead);
                  Display.getDefault().asyncExec(() -> appendOutput(errorChunk, SWT.COLOR_RED));
                }
              } catch (IOException e) {
                if (running.get()) {
                  log.logError("Error reading process error", e);
                }
              }
            });
    errorThread.setDaemon(true);
    errorThread.setName("Terminal-Error-Reader");
    errorThread.start();
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
  public void dispose() {
    running.set(false);

    // Close streams
    try {
      if (processInput != null) processInput.close();
      if (processOutput != null) processOutput.close();
      if (processError != null) processError.close();
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
      if (errorThread != null) errorThread.join(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
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

  /** Get the output text widget (for advanced use) */
  public StyledText getOutputText() {
    return outputText;
  }

  /** Get the input text widget (for advanced use) */
  public Text getInputText() {
    return inputText;
  }
}
