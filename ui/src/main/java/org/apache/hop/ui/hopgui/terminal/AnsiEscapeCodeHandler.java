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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.Const;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.widgets.Display;

/**
 * Handles ANSI escape code parsing and rendering for terminal output.
 *
 * <p>Supports:
 *
 * <ul>
 *   <li>SGR (Select Graphic Rendition) codes for colors and text styles
 *   <li>Cursor movement and positioning
 *   <li>Clear screen and line operations
 *   <li>Terminal mode switching
 * </ul>
 */
public class AnsiEscapeCodeHandler {

  // ANSI escape sequence patterns - comprehensive matching
  private static final Pattern ANSI_PATTERN =
      Pattern.compile(
          // CSI sequences: ESC [ params letter
          "\u001B\\[([0-9;?]*)([a-zA-Z])"
              // OSC sequences: ESC ] ... BEL or ESC ] ... ST
              + "|\u001B\\]([^\u0007\u001B]*?)(?:\u0007|\u001B\\\\)"
              // Other ESC sequences
              + "|\u001B[=>]"
              // Backspace (to handle delete operations)
              + "|\b"
              // Carriage return
              + "|\\r");

  // Current text style state
  private boolean bold = false;
  private boolean italic = false;
  private boolean underline = false;
  private Color foregroundColor = null;
  private Color backgroundColor = null;

  private final Display display;
  private final StyledText outputText;
  private final Runnable enableTextChange;
  private final Runnable disableTextChange;

  // Default terminal colors (matching typical terminal themes)
  private final Color[] ansiColors;
  private final Color defaultForeground;
  private final Color defaultBackground;

  // Color cache for dynamically created colors (256-color and RGB)
  // Key format: "r,g,b" for RGB colors
  private final Map<String, Color> colorCache = new HashMap<>();

  // Cursor position tracking
  private int cursorOffset = 0; // Current cursor position in the text

  public AnsiEscapeCodeHandler(
      StyledText outputText, Runnable enableTextChange, Runnable disableTextChange) {
    this.outputText = outputText;
    this.display = outputText.getDisplay();
    this.enableTextChange = enableTextChange;
    this.disableTextChange = disableTextChange;

    // Use widget's actual colors as defaults
    this.defaultBackground = outputText.getBackground();
    this.defaultForeground = outputText.getForeground();

    // Standard ANSI color palette (0-15)
    this.ansiColors = new Color[16];
    initializeAnsiColors();

    // Initialize cursor at end of text
    this.cursorOffset = outputText.getCharCount();
  }

  /** Initialize standard ANSI color palette */
  private void initializeAnsiColors() {
    // Check if background is light (white/light colored) for Windows yellow fix
    boolean isLightBackground = false;
    if (defaultBackground != null && !defaultBackground.isDisposed()) {
      RGB bgRgb = defaultBackground.getRGB();
      // Consider background light if average RGB > 200 (closer to white)
      int avgBrightness = (bgRgb.red + bgRgb.green + bgRgb.blue) / 3;
      isLightBackground = avgBrightness > 200;
    }

    // On Windows with light background, use black instead of yellow for readability
    boolean useBlackForYellow = Const.isWindows() && isLightBackground;

    // Standard colors (0-7)
    ansiColors[0] = new Color(display, 0, 0, 0); // Black
    ansiColors[1] = new Color(display, 205, 49, 49); // Red
    ansiColors[2] = new Color(display, 13, 188, 121); // Green
    ansiColors[3] =
        useBlackForYellow
            ? new Color(display, 0, 0, 0)
            : new Color(display, 229, 229, 16); // Yellow (or black on Windows/light bg)
    ansiColors[4] = new Color(display, 36, 114, 200); // Blue
    ansiColors[5] = new Color(display, 188, 63, 188); // Magenta
    ansiColors[6] = new Color(display, 17, 168, 205); // Cyan
    ansiColors[7] = new Color(display, 229, 229, 229); // White

    // Bright colors (8-15)
    ansiColors[8] = new Color(display, 102, 102, 102); // Bright Black (Gray)
    ansiColors[9] = new Color(display, 241, 76, 76); // Bright Red
    ansiColors[10] = new Color(display, 35, 209, 139); // Bright Green
    ansiColors[11] =
        useBlackForYellow
            ? new Color(display, 0, 0, 0)
            : new Color(display, 245, 245, 67); // Bright Yellow (or black on Windows/light bg)
    ansiColors[12] = new Color(display, 59, 142, 234); // Bright Blue
    ansiColors[13] = new Color(display, 214, 112, 214); // Bright Magenta
    ansiColors[14] = new Color(display, 41, 184, 219); // Bright Cyan
    ansiColors[15] = new Color(display, 255, 255, 255); // Bright White
  }

  /**
   * Process text with ANSI escape codes and append to StyledText widget
   *
   * @param text Raw text with ANSI codes
   */
  public void appendText(String text) {
    if (text == null || text.isEmpty()) {
      return;
    }

    // Enable text changes for PTY output
    enableTextChange.run();

    try {
      Matcher matcher = ANSI_PATTERN.matcher(text);
      int lastEnd = 0;
      List<TextSegment> segments = new ArrayList<>();

      while (matcher.find()) {
        // Add text before escape code
        if (matcher.start() > lastEnd) {
          String plainText = text.substring(lastEnd, matcher.start());
          segments.add(new TextSegment(plainText, getCurrentStyle()));
        }

        // Process escape code or control character
        String matchedText = matcher.group();
        if (matchedText.equals("\b")) {
          // Backspace - delete previous character
          handleBackspace();
        } else if (matchedText.equals("\r")) {
          // Carriage return - move to start of current line and clear to end
          handleCarriageReturn();
        } else if (matcher.group(1) != null) {
          // CSI sequence (e.g., \033[31m for red)
          processCSI(matcher.group(1), matcher.group(2));
        } else if (matcher.group(3) != null) {
          // OSC sequence (e.g., \033]0;Title\007 for window title)
          processOSC(matcher.group(3));
        }
        // Ignore other sequences

        lastEnd = matcher.end();
      }

      // Add remaining text
      if (lastEnd < text.length()) {
        String plainText = text.substring(lastEnd);
        segments.add(new TextSegment(plainText, getCurrentStyle()));
      }

      // Append all segments to StyledText
      for (TextSegment segment : segments) {
        appendStyledText(segment.text, segment.style);
      }
    } finally {
      // Disable text changes again
      disableTextChange.run();
    }
  }

  /** Handle backspace - move cursor back and delete character */
  private void handleBackspace() {
    if (cursorOffset > 0) {
      // Delete character before cursor
      outputText.replaceTextRange(cursorOffset - 1, 1, "");
      cursorOffset--;
      outputText.setCaretOffset(cursorOffset);
    }
  }

  /** Process CSI (Control Sequence Introducer) escape codes */
  private void processCSI(String params, String command) {
    switch (command) {
      case "m": // SGR (Select Graphic Rendition)
        processSGR(params);
        break;
      case "H":
      case "f": // Cursor position (absolute)
        processCursorPosition(params);
        break;
      case "A": // Cursor up
        processCursorUp(params);
        break;
      case "B": // Cursor down
        processCursorDown(params);
        break;
      case "C": // Cursor forward (right)
        processCursorForward(params);
        break;
      case "D": // Cursor back (left)
        processCursorBack(params);
        break;
      case "J": // Clear display
        // Ignore clear commands (we're in append mode)
        break;
      case "K": // Clear line (EL)
        processClearLine(params);
        break;
      case "h":
      case "l": // Set/Reset mode
        // Ignore mode changes (like bracketed paste mode ?2004h, ?1h, ?25h, etc.)
        // These control terminal behavior but don't affect text rendering
        break;
      case "n": // Device Status Report
        // Ignore - shell querying terminal capabilities
        break;
      case "c": // Device Attributes
        // Ignore - shell querying what kind of terminal we are
        break;
      default:
        // Ignore unknown commands
        break;
    }
  }

  /** Handle carriage return: move cursor to start of current line */
  private void handleCarriageReturn() {
    int charCount = outputText.getCharCount();
    // Find start of current line based on cursor position
    String allText = outputText.getText();
    int searchStart = Math.min(cursorOffset, charCount);
    int lastNewline = allText.lastIndexOf('\n', searchStart);
    int lineStart = lastNewline >= 0 ? lastNewline + 1 : 0;
    cursorOffset = lineStart;
    outputText.setCaretOffset(cursorOffset);
  }

  /** Process EL (Erase in Line) - ESC [ K / ESC [ n K */
  private void processClearLine(String params) {
    String p = (params == null || params.isEmpty()) ? "0" : params;
    int total = outputText.getCharCount();
    if (total == 0) {
      return;
    }
    // Use cursor position for clearing
    int lineIndex = outputText.getLineAtOffset(Math.min(cursorOffset, total));
    int lineStart = outputText.getOffsetAtLine(lineIndex);
    String lineText = outputText.getLine(lineIndex);
    int lineEnd = lineStart + (lineText != null ? lineText.length() : 0);

    switch (p) {
      case "0": // Clear from cursor to end of line
        if (lineEnd > cursorOffset) {
          outputText.replaceTextRange(cursorOffset, lineEnd - cursorOffset, "");
        }
        break;
      case "1": // Clear from start of line to cursor
        if (cursorOffset > lineStart) {
          outputText.replaceTextRange(lineStart, cursorOffset - lineStart, "");
          cursorOffset = lineStart;
        }
        break;
      case "2": // Clear entire line
        if (lineEnd > lineStart) {
          outputText.replaceTextRange(lineStart, lineEnd - lineStart, "");
          cursorOffset = lineStart;
        }
        break;
      default:
        // Unknown parameter - ignore
        break;
    }
    outputText.setCaretOffset(cursorOffset);
  }

  /** Process cursor up movement (CSI n A) */
  private void processCursorUp(String params) {
    int n = (params == null || params.isEmpty()) ? 1 : Integer.parseInt(params);
    int lineIndex = outputText.getLineAtOffset(Math.min(cursorOffset, outputText.getCharCount()));
    if (lineIndex > 0) {
      // Move up n lines
      int targetLine = Math.max(0, lineIndex - n);
      int lineOffset = cursorOffset - outputText.getOffsetAtLine(lineIndex);
      int targetLineStart = outputText.getOffsetAtLine(targetLine);
      String targetLineText = outputText.getLine(targetLine);
      int targetLineLen = targetLineText != null ? targetLineText.length() : 0;
      // Try to maintain column position
      cursorOffset = targetLineStart + Math.min(lineOffset, targetLineLen);
      outputText.setCaretOffset(cursorOffset);
    }
  }

  /** Process cursor down movement (CSI n B) */
  private void processCursorDown(String params) {
    int n = (params == null || params.isEmpty()) ? 1 : Integer.parseInt(params);
    int totalChars = outputText.getCharCount();
    if (totalChars == 0) {
      return;
    }
    int lineIndex = outputText.getLineAtOffset(Math.min(cursorOffset, totalChars));
    int maxLine = outputText.getLineCount() - 1;
    if (lineIndex < maxLine) {
      // Move down n lines
      int targetLine = Math.min(maxLine, lineIndex + n);
      int lineOffset = cursorOffset - outputText.getOffsetAtLine(lineIndex);
      int targetLineStart = outputText.getOffsetAtLine(targetLine);
      String targetLineText = outputText.getLine(targetLine);
      int targetLineLen = targetLineText != null ? targetLineText.length() : 0;
      // Try to maintain column position
      cursorOffset = targetLineStart + Math.min(lineOffset, targetLineLen);
      outputText.setCaretOffset(cursorOffset);
    }
  }

  /** Process cursor forward movement (CSI n C) - move right */
  private void processCursorForward(String params) {
    int n = (params == null || params.isEmpty()) ? 1 : Integer.parseInt(params);
    int totalChars = outputText.getCharCount();
    cursorOffset = Math.min(cursorOffset + n, totalChars);
    outputText.setCaretOffset(cursorOffset);
  }

  /** Process cursor back movement (CSI n D) - move left */
  private void processCursorBack(String params) {
    int n = (params == null || params.isEmpty()) ? 1 : Integer.parseInt(params);
    cursorOffset = Math.max(cursorOffset - n, 0);
    outputText.setCaretOffset(cursorOffset);
  }

  /** Process absolute cursor positioning (CSI row;col H or CSI row;col f) */
  private void processCursorPosition(String params) {
    // Parse row;col (both 1-based in terminal, 0-based in our system)
    String[] parts =
        (params == null || params.isEmpty()) ? new String[] {"1", "1"} : params.split(";");
    int row = parts.length > 0 && !parts[0].isEmpty() ? Integer.parseInt(parts[0]) - 1 : 0;
    int col = parts.length > 1 && !parts[1].isEmpty() ? Integer.parseInt(parts[1]) - 1 : 0;

    int totalLines = outputText.getLineCount();
    row = Math.max(0, Math.min(row, totalLines - 1));

    try {
      int lineStart = outputText.getOffsetAtLine(row);
      String lineText = outputText.getLine(row);
      int lineLen = lineText != null ? lineText.length() : 0;
      col = Math.max(0, Math.min(col, lineLen));

      cursorOffset = lineStart + col;
      outputText.setCaretOffset(cursorOffset);
    } catch (Exception e) {
      // If line doesn't exist yet, set to end
      cursorOffset = outputText.getCharCount();
      outputText.setCaretOffset(cursorOffset);
    }
  }

  /** Process SGR (Select Graphic Rendition) parameters for text styling */
  private void processSGR(String params) {
    if (params == null || params.isEmpty()) {
      params = "0"; // Default to reset
    }

    String[] codes = params.split(";");
    for (int i = 0; i < codes.length; i++) {
      if (codes[i].isEmpty()) {
        continue;
      }

      int code = Integer.parseInt(codes[i]);

      switch (code) {
        case 0: // Reset
          resetStyle();
          break;
        case 1: // Bold
          bold = true;
          break;
        case 2: // Dim (treat as normal)
          bold = false;
          break;
        case 3: // Italic
          italic = true;
          break;
        case 4: // Underline
          underline = true;
          break;
        case 22: // Normal intensity
          bold = false;
          break;
        case 23: // Not italic
          italic = false;
          break;
        case 24: // Not underlined
          underline = false;
          break;
        case 27: // Positive (not inverse)
          // Ignore inverse mode
          break;
        case 30:
        case 31:
        case 32:
        case 33:
        case 34:
        case 35:
        case 36:
        case 37: // Foreground colors (standard)
          foregroundColor = ansiColors[code - 30];
          break;
        case 39: // Default foreground
          foregroundColor = null;
          break;
        case 40:
        case 41:
        case 42:
        case 43:
        case 44:
        case 45:
        case 46:
        case 47: // Background colors (standard)
          backgroundColor = ansiColors[code - 40];
          break;
        case 49: // Default background
          backgroundColor = null;
          break;
        case 90:
        case 91:
        case 92:
        case 93:
        case 94:
        case 95:
        case 96:
        case 97: // Foreground colors (bright)
          foregroundColor = ansiColors[code - 90 + 8];
          break;
        case 100:
        case 101:
        case 102:
        case 103:
        case 104:
        case 105:
        case 106:
        case 107: // Background colors (bright)
          backgroundColor = ansiColors[code - 100 + 8];
          break;
        case 38: // Extended foreground color (256-color or RGB)
          i = processExtendedColor(codes, i, true);
          break;
        case 48: // Extended background color (256-color or RGB)
          i = processExtendedColor(codes, i, false);
          break;
        default:
          // Ignore unknown codes
          break;
      }
    }
  }

  /** Process extended color codes (38;5;N or 38;2;R;G;B) */
  private int processExtendedColor(String[] codes, int index, boolean foreground) {
    if (index + 1 >= codes.length) {
      return index;
    }

    int type = Integer.parseInt(codes[index + 1]);
    if (type == 5 && index + 2 < codes.length) {
      // 256-color mode
      int colorIndex = Integer.parseInt(codes[index + 2]);
      Color color = get256Color(colorIndex);
      if (foreground) {
        foregroundColor = color;
      } else {
        backgroundColor = color;
      }
      return index + 2;
    } else if (type == 2 && index + 4 < codes.length) {
      // RGB mode (24-bit true color)
      int r = Integer.parseInt(codes[index + 2]);
      int g = Integer.parseInt(codes[index + 3]);
      int b = Integer.parseInt(codes[index + 4]);
      Color color = getRgbColor(r, g, b);
      if (foreground) {
        foregroundColor = color;
      } else {
        backgroundColor = color;
      }
      return index + 4;
    }

    return index + 1;
  }

  /** Get color from 256-color palette */
  private Color get256Color(int index) {
    if (index < 16) {
      // Use standard ANSI colors for 0-15
      return ansiColors[index];
    } else if (index < 232) {
      // 216-color cube (6x6x6) - indices 16-231
      int colorIndex = index - 16;
      int r = (colorIndex / 36) * 51;
      int g = ((colorIndex % 36) / 6) * 51;
      int b = (colorIndex % 6) * 51;
      return getRgbColor(r, g, b);
    } else if (index < 256) {
      // Grayscale ramp - indices 232-255
      int gray = 8 + (index - 232) * 10;
      return getRgbColor(gray, gray, gray);
    } else {
      // Invalid index - return default
      return defaultForeground;
    }
  }

  /** Get or create an RGB color with caching to prevent resource leaks */
  private Color getRgbColor(int r, int g, int b) {
    // Clamp values to valid range
    final int red = Math.max(0, Math.min(255, r));
    final int green = Math.max(0, Math.min(255, g));
    final int blue = Math.max(0, Math.min(255, b));

    // Use cache to avoid creating duplicate colors
    String key = red + "," + green + "," + blue;
    return colorCache.computeIfAbsent(key, k -> new Color(display, red, green, blue));
  }

  /** Process OSC (Operating System Command) sequences */
  private void processOSC(String params) {
    // Ignore all OSC sequences:
    // - OSC 0: Set window title
    // - OSC 1: Set icon name
    // - OSC 2: Set window title
    // - OSC 7: Set current working directory (file://...)
    // - OSC 8: Hyperlinks
    // - OSC 9: iTerm2 notifications
    // - OSC 133: Shell integration markers
    // - etc.
    // We don't implement any of these in the embedded terminal
  }

  /** Reset all text styling to defaults */
  private void resetStyle() {
    bold = false;
    italic = false;
    underline = false;
    foregroundColor = null;
    backgroundColor = null;
  }

  /** Get current style as StyleRange (without position/length) */
  private TextStyle getCurrentStyle() {
    return new TextStyle(
        foregroundColor != null ? foregroundColor : defaultForeground,
        backgroundColor,
        bold,
        italic,
        underline);
  }

  /** Append styled text to output widget at cursor position */
  private void appendStyledText(String text, TextStyle style) {
    if (text.isEmpty()) {
      return;
    }

    // Ensure cursor is within valid range
    int totalChars = outputText.getCharCount();
    cursorOffset = Math.max(0, Math.min(cursorOffset, totalChars));

    // Insert text at cursor position
    outputText.replaceTextRange(cursorOffset, 0, text);
    int length = text.length();

    // Apply style
    StyleRange styleRange = new StyleRange();
    styleRange.start = cursorOffset;
    styleRange.length = length;
    styleRange.foreground = style.foreground;
    styleRange.background = style.background;

    int fontStyle = SWT.NORMAL;
    if (style.bold) fontStyle |= SWT.BOLD;
    if (style.italic) fontStyle |= SWT.ITALIC;
    styleRange.fontStyle = fontStyle;

    styleRange.underline = style.underline;

    outputText.setStyleRange(styleRange);

    // Move cursor forward by inserted text length
    cursorOffset += length;
    outputText.setCaretOffset(cursorOffset);
  }

  /** Dispose of allocated colors */
  public void dispose() {
    // Dispose standard ANSI colors
    if (ansiColors != null) {
      for (Color color : ansiColors) {
        if (color != null && !color.isDisposed()) {
          color.dispose();
        }
      }
    }

    // Dispose cached RGB colors (256-color and 24-bit)
    for (Color color : colorCache.values()) {
      if (color != null && !color.isDisposed()) {
        color.dispose();
      }
    }
    colorCache.clear();

    // Note: Don't dispose defaultForeground/defaultBackground as they belong to the widget
  }

  /** Text segment with associated style */
  private static class TextSegment {
    final String text;
    final TextStyle style;

    TextSegment(String text, TextStyle style) {
      this.text = text;
      this.style = style;
    }
  }

  /** Text style information */
  private static class TextStyle {
    final Color foreground;
    final Color background;
    final boolean bold;
    final boolean italic;
    final boolean underline;

    TextStyle(Color foreground, Color background, boolean bold, boolean italic, boolean underline) {
      this.foreground = foreground;
      this.background = background;
      this.bold = bold;
      this.italic = italic;
      this.underline = underline;
    }
  }
}
