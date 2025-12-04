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
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;

/**
 * Simple terminal console widget using ProcessBuilder (no PTY).
 *
 * <p>This is a reliable, basic terminal that provides:
 *
 * <ul>
 *   <li>Command input field with history (arrow up/down)
 *   <li>Output display area
 *   <li>Simple command execution
 *   <li>No complex ANSI handling or cursor positioning
 * </ul>
 *
 * <p>Use this for reliable command execution. For advanced features like tab completion and full
 * shell prompts, use {@link TerminalWidget} instead.
 */
public class SimpleTerminalWidget implements ITerminalWidget {

  private static final ILogChannel log = LogChannel.GENERAL;

  private final Composite parent;
  private final String workingDirectory;

  private Composite terminalComposite;
  private Control outputText; // StyledText in desktop, Text in web mode
  private Label promptLabel;
  private Text inputText;
  private boolean isWebMode;

  private String shellPath;
  private Process shellProcess;
  private BufferedWriter processInput;
  private BufferedReader processOutput;
  private BufferedReader processError;

  private final AtomicBoolean running = new AtomicBoolean(false);
  private Thread outputThread;
  private Thread errorThread;

  // Command history
  private final List<String> commandHistory = new ArrayList<>();
  private int historyIndex = -1;

  public SimpleTerminalWidget(Composite parent, String workingDirectory) {
    this.parent = parent;
    this.workingDirectory =
        workingDirectory != null ? workingDirectory : System.getProperty("user.home");
    this.isWebMode = EnvironmentUtils.getInstance().isWeb();

    createTerminal();
  }

  /** Create the simple terminal UI */
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

      // Output area (read-only)
      // Use Text widget in web mode (RAP doesn't support StyledText)
      if (isWebMode) {
        outputText =
            new Text(
                terminalComposite,
                SWT.MULTI | SWT.READ_ONLY | SWT.V_SCROLL | SWT.H_SCROLL | SWT.BORDER);
      } else {
        outputText =
            new StyledText(
                terminalComposite,
                SWT.MULTI | SWT.READ_ONLY | SWT.V_SCROLL | SWT.H_SCROLL | SWT.BORDER);
      }
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
      fdOutput.bottom = new FormAttachment(100, -30); // Leave space for input
      outputText.setLayoutData(fdOutput);

      // Separator
      Label separator = new Label(terminalComposite, SWT.SEPARATOR | SWT.HORIZONTAL);
      FormData fdSeparator = new FormData();
      fdSeparator.left = new FormAttachment(0, 0);
      fdSeparator.top = new FormAttachment(outputText, 0);
      fdSeparator.right = new FormAttachment(100, 0);
      separator.setLayoutData(fdSeparator);

      // Input area composite
      Composite inputComposite = new Composite(terminalComposite, SWT.NONE);
      inputComposite.setLayout(new FormLayout());
      FormData fdInputComp = new FormData();
      fdInputComp.left = new FormAttachment(0, 0);
      fdInputComp.top = new FormAttachment(separator, 0);
      fdInputComp.right = new FormAttachment(100, 0);
      fdInputComp.bottom = new FormAttachment(100, 0);
      inputComposite.setLayoutData(fdInputComp);

      // Prompt label
      promptLabel = new Label(inputComposite, SWT.NONE);
      promptLabel.setText("$");
      promptLabel.setBackground(bgColor);
      promptLabel.setForeground(fgColor);
      FormData fdPrompt = new FormData();
      fdPrompt.left = new FormAttachment(0, 5);
      fdPrompt.top = new FormAttachment(0, 5);
      promptLabel.setLayoutData(fdPrompt);

      // Input field
      inputText = new Text(inputComposite, SWT.SINGLE | SWT.BORDER);
      inputText.setFont(terminalFont);
      inputText.setBackground(bgColor);
      inputText.setForeground(fgColor);
      FormData fdInput = new FormData();
      fdInput.left = new FormAttachment(promptLabel, 5);
      fdInput.top = new FormAttachment(0, 2);
      fdInput.right = new FormAttachment(100, -5);
      fdInput.bottom = new FormAttachment(100, -2);
      inputText.setLayoutData(fdInput);

      // Handle Enter key to execute command
      inputText.addKeyListener(
          new KeyAdapter() {
            @Override
            public void keyPressed(KeyEvent e) {
              if (e.keyCode == SWT.CR || e.keyCode == SWT.LF) {
                executeCommand(inputText.getText());
                inputText.setText("");
                historyIndex = -1;
              } else if (e.keyCode == SWT.ARROW_UP) {
                // Navigate command history backward
                if (!commandHistory.isEmpty()) {
                  if (historyIndex == -1) {
                    historyIndex = commandHistory.size() - 1;
                  } else if (historyIndex > 0) {
                    historyIndex--;
                  }
                  if (historyIndex >= 0 && historyIndex < commandHistory.size()) {
                    inputText.setText(commandHistory.get(historyIndex));
                    inputText.setSelection(inputText.getText().length());
                  }
                }
              } else if (e.keyCode == SWT.ARROW_DOWN) {
                // Navigate command history forward
                if (!commandHistory.isEmpty() && historyIndex != -1) {
                  historyIndex++;
                  if (historyIndex >= commandHistory.size()) {
                    historyIndex = -1;
                    inputText.setText("");
                  } else {
                    inputText.setText(commandHistory.get(historyIndex));
                    inputText.setSelection(inputText.getText().length());
                  }
                }
              }
            }
          });

      // Set focus to input
      Display display = parent.getDisplay();
      display.asyncExec(
          () -> {
            if (inputText != null && !inputText.isDisposed()) {
              inputText.setFocus();
              inputText.forceFocus();
            }
          });

      // Start shell process
      startShellProcess();

    } catch (Exception e) {
      log.logError("Error creating simple terminal widget", e);
      createErrorComposite("Error: " + e.getMessage());
    }
  }

  /** Start shell process using ProcessBuilder */
  private void startShellProcess() {
    try {
      // Detect shell
      shellPath = detectShell();
      log.logBasic("Simple Terminal: Starting shell: " + shellPath);

      // Create process
      ProcessBuilder processBuilder = new ProcessBuilder(shellPath);
      processBuilder.directory(new File(workingDirectory));
      processBuilder.redirectErrorStream(false);

      shellProcess = processBuilder.start();
      running.set(true);

      // Set up streams
      processInput = new BufferedWriter(new OutputStreamWriter(shellProcess.getOutputStream()));
      processOutput = new BufferedReader(new InputStreamReader(shellProcess.getInputStream()));
      processError = new BufferedReader(new InputStreamReader(shellProcess.getErrorStream()));

      // Start output readers
      startOutputReaders();

      // Welcome message
      appendOutput("Simple Terminal Console\n");
      appendOutput("Working Directory: " + workingDirectory + "\n");
      appendOutput("Shell: " + shellPath + "\n");
      appendOutput("Type commands and press Enter\n");
      appendOutput("Use arrow keys for command history\n\n");

    } catch (Exception e) {
      log.logError("Error starting shell process", e);
      appendOutput("Error starting shell: " + e.getMessage() + "\n");
    }
  }

  /** Detect appropriate shell for the platform */
  private String detectShell() {
    if (Const.isWindows()) {
      // Try PowerShell first, fallback to cmd
      String powerShell =
          System.getenv("SystemRoot") + "\\System32\\WindowsPowerShell\\v1.0\\powershell.exe";
      File psFile = new File(powerShell);
      if (psFile.exists()) {
        return powerShell;
      }
      return "cmd.exe";
    } else {
      // Unix-like: try user's shell, then common shells
      String userShell = System.getenv("SHELL");
      if (userShell != null && new File(userShell).exists()) {
        return userShell;
      }

      // Try common shells
      String[] shells = {"/bin/zsh", "/bin/bash", "/bin/sh"};
      for (String shell : shells) {
        if (new File(shell).exists()) {
          return shell;
        }
      }

      return "/bin/sh"; // Ultimate fallback
    }
  }

  /** Start threads to read output and error streams */
  private void startOutputReaders() {
    // Output reader thread
    outputThread =
        new Thread(
            () -> {
              try {
                String line;
                while (running.get() && (line = processOutput.readLine()) != null) {
                  final String outputLine = line;
                  Display.getDefault()
                      .asyncExec(
                          () -> {
                            if (!outputText.isDisposed()) {
                              appendOutput(outputLine + "\n");
                            }
                          });
                }
              } catch (IOException e) {
                if (running.get()) {
                  log.logError("Error reading terminal output", e);
                }
              }
            },
            "Terminal-Output");
    outputThread.start();

    // Error reader thread
    errorThread =
        new Thread(
            () -> {
              try {
                String line;
                while (running.get() && (line = processError.readLine()) != null) {
                  final String errorLine = line;
                  Display.getDefault()
                      .asyncExec(
                          () -> {
                            if (!outputText.isDisposed()) {
                              appendOutput("[ERROR] " + errorLine + "\n");
                            }
                          });
                }
              } catch (IOException e) {
                if (running.get()) {
                  log.logError("Error reading terminal errors", e);
                }
              }
            },
            "Terminal-Error");
    errorThread.start();
  }

  /** Execute a command */
  private void executeCommand(String command) {
    if (command == null || command.trim().isEmpty()) {
      return;
    }

    // Add to history
    commandHistory.add(command);

    // Echo command to output
    appendOutput("$ " + command + "\n");

    try {
      // Send command to shell
      processInput.write(command);
      processInput.newLine();
      processInput.flush();
    } catch (IOException e) {
      log.logError("Error executing command", e);
      appendOutput("Error: " + e.getMessage() + "\n");
    }
  }

  /** Append text to output area */
  private void appendOutput(String text) {
    if (!outputText.isDisposed()) {
      if (isWebMode) {
        // Text widget
        Text textWidget = (Text) outputText;
        textWidget.append(text);
        // Auto-scroll to bottom
        int lineCount = textWidget.getLineCount();
        if (lineCount > 0) {
          textWidget.setTopIndex(lineCount - 1);
        }
      } else {
        // StyledText widget
        StyledText styledText = (StyledText) outputText;
        styledText.append(text);
        // Auto-scroll to bottom
        styledText.setTopIndex(styledText.getLineCount() - 1);
      }
    }
  }

  /** Create error composite when terminal fails to initialize */
  private void createErrorComposite(String errorMessage) {
    terminalComposite = new Composite(parent, SWT.NONE);
    terminalComposite.setLayout(new FormLayout());

    Label errorLabel = new Label(terminalComposite, SWT.NONE);
    errorLabel.setText(errorMessage);
    FormData fd = new FormData();
    fd.left = new FormAttachment(0, 10);
    fd.top = new FormAttachment(0, 10);
    errorLabel.setLayoutData(fd);
  }

  @Override
  public void dispose() {
    running.set(false);

    try {
      if (processInput != null) {
        processInput.close();
      }
      if (processOutput != null) {
        processOutput.close();
      }
      if (processError != null) {
        processError.close();
      }
    } catch (IOException e) {
      log.logError("Error closing streams", e);
    }

    if (shellProcess != null && shellProcess.isAlive()) {
      shellProcess.destroy();
      try {
        shellProcess.waitFor();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (outputThread != null) {
      try {
        outputThread.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (errorThread != null) {
      try {
        errorThread.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (terminalComposite != null && !terminalComposite.isDisposed()) {
      terminalComposite.dispose();
    }
  }

  @Override
  public Composite getTerminalComposite() {
    return terminalComposite;
  }

  @Override
  public boolean isConnected() {
    return running.get() && shellProcess != null && shellProcess.isAlive();
  }

  @Override
  public String getShellPath() {
    return shellPath;
  }

  @Override
  public String getWorkingDirectory() {
    return workingDirectory;
  }

  @Override
  public StyledText getOutputText() {
    // Return null in web mode (StyledText not available)
    // Callers should check for null and use getOutputControl() instead
    if (isWebMode) {
      return null;
    }
    return (StyledText) outputText;
  }

  /**
   * Get the output control (works in both desktop and web mode)
   *
   * @return The output control (StyledText in desktop, Text in web)
   */
  public Control getOutputControl() {
    return outputText;
  }

  @Override
  public String getTerminalType() {
    return "Simple";
  }

  /**
   * Get the input text field (for focus operations)
   *
   * @return The Text widget for command input
   */
  public Text getInputText() {
    return inputText;
  }
}
