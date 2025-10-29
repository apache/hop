/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.core.widget.highlight;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.util.Utils;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.LineStyleEvent;
import org.eclipse.swt.custom.LineStyleListener;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;

public class GenericCodeHighlight implements LineStyleListener {

  CodeScanner scanner;
  StyleAttribute[] styleAttributes;
  ArrayList<int[]> blockComments = new ArrayList<>();

  public static final int EOF = -1;
  public static final int EOL = 10;

  public static final int WORD = 0;
  public static final int WHITE = 1;
  public static final int KEY = 2;
  public static final int COMMENT = 3; // single line comment: //
  public static final int SYMBOL = 4;
  public static final int STRING = 5;
  public static final int OTHER = 6;
  public static final int NUMBER = 7;
  public static final int FUNCTION = 8;

  public static final int MAXIMUM_TOKEN = 9;

  public GenericCodeHighlight(ScriptEngine engine) {
    initializeStyles();
    scanner = new CodeScanner(engine.getKeywords(), engine.getBuiltInFunctions());
    scanner.addKeywords(engine.getKeywords());
    scanner.addFunctionNames(engine.getBuiltInFunctions());
  }

  StyleAttribute getStyleAttribute(int type) {
    if (type < 0 || type >= styleAttributes.length) {
      return null;
    }
    return styleAttributes[type];
  }

  boolean inBlockComment(int start, int end) {
    for (int i = 0; i < blockComments.size(); i++) {
      int[] offsets = blockComments.get(i);
      // start of comment in the line
      if ((offsets[0] >= start) && (offsets[0] <= end)) {
        return true;
      }
      // end of comment in the line
      if ((offsets[1] >= start) && (offsets[1] <= end)) {
        return true;
      }
      if ((offsets[0] <= start) && (offsets[1] >= end)) {
        return true;
      }
    }
    return false;
  }

  void initializeStyles() {
    GuiResource resource = GuiResource.getInstance();
    styleAttributes = new StyleAttribute[MAXIMUM_TOKEN];
    styleAttributes[WORD] = new StyleAttribute(resource.getColorBlack(), SWT.NORMAL);
    styleAttributes[WHITE] = new StyleAttribute(resource.getColorBlack(), SWT.NORMAL);
    styleAttributes[STRING] = new StyleAttribute(resource.getColorDarkGreen(), SWT.NORMAL);
    styleAttributes[OTHER] = new StyleAttribute(resource.getColorBlack(), SWT.NORMAL);
    styleAttributes[NUMBER] = new StyleAttribute(resource.getColorOrange(), SWT.NORMAL);
    if (PropsUi.getInstance().isDarkMode()) {
      styleAttributes[COMMENT] = new StyleAttribute(resource.getColorGray(), SWT.ITALIC);
      styleAttributes[KEY] = new StyleAttribute(resource.getColor(30, 144, 255), SWT.NORMAL);
      styleAttributes[SYMBOL] = new StyleAttribute(resource.getColor(243, 126, 131), SWT.NORMAL);
      styleAttributes[FUNCTION] = new StyleAttribute(resource.getColor(177, 102, 218), SWT.NORMAL);
    } else {
      styleAttributes[COMMENT] = new StyleAttribute(resource.getColorDarkGray(), SWT.ITALIC);
      styleAttributes[KEY] = new StyleAttribute(resource.getColorBlue(), SWT.NORMAL);
      styleAttributes[SYMBOL] = new StyleAttribute(resource.getColorDarkRed(), SWT.NORMAL);
      styleAttributes[FUNCTION] = new StyleAttribute(resource.getColor(148, 0, 211), SWT.NORMAL);
    }
  }

  /**
   * Event.detail line start offset (input) Event.text line text (input) LineStyleEvent.styles
   * Enumeration of StyleRanges, need to be in order. (output) LineStyleEvent.background line
   * background color (output)
   */
  public void lineGetStyle(LineStyleEvent event) {
    ArrayList<StyleRange> styles = new ArrayList<>();
    StyleRange lastStyle;

    if (inBlockComment(event.lineOffset, event.lineOffset + event.lineText.length())) {
      StyleAttribute attribute = getStyleAttribute(COMMENT);
      styles.add(
          new StyleRange(
              event.lineOffset,
              event.lineText.length() + 4,
              attribute.getForeground(),
              null,
              attribute.getStyle()));
      event.styles = styles.toArray(new StyleRange[styles.size()]);
      return;
    }
    scanner.setRange(event.lineText);
    String xs = ((StyledText) event.widget).getText();
    if (xs != null) {
      parseBlockComments(xs);
    }
    int token = scanner.nextToken();
    while (token != EOF) {
      if (token != OTHER) {
        if ((token == WHITE) && (!styles.isEmpty())) {
          int start = scanner.getStartOffset() + event.lineOffset;
          lastStyle = styles.get(styles.size() - 1);
          if (lastStyle.fontStyle != SWT.NORMAL && lastStyle.start + lastStyle.length == start) {
            // have the white space take on the style before it to minimize font style
            // changes
            lastStyle.length += scanner.getLength();
          }
        } else {
          StyleAttribute attribute = getStyleAttribute(token);
          if (attribute != styleAttributes[0]) { // hardcoded default foreground color, black
            StyleRange style =
                new StyleRange(
                    scanner.getStartOffset() + event.lineOffset,
                    scanner.getLength(),
                    attribute.getForeground(),
                    null,
                    attribute.getStyle());
            if (token == KEY) {
              style.fontStyle = SWT.BOLD;
            }
            if (styles.isEmpty()) {
              styles.add(style);
            } else {
              lastStyle = styles.get(styles.size() - 1);
              if (lastStyle.similarTo(style)
                  && (lastStyle.start + lastStyle.length == style.start)) {
                lastStyle.length += style.length;
              } else {
                styles.add(style);
              }
            }
          }
        }
      }
      token = scanner.nextToken();
    }
    event.styles = styles.toArray(new StyleRange[styles.size()]);
  }

  public void parseBlockComments(String text) {
    blockComments = new ArrayList<>();
    StringReader buffer = new StringReader(text);
    int ch;
    boolean blkComment = false;
    int cnt = 0;
    int[] offsets = new int[2];
    boolean done = false;

    try {
      while (!done) {
        switch (ch = buffer.read()) {
          case -1:
            {
              if (blkComment) {
                offsets[1] = cnt;
                blockComments.add(offsets);
              }
              done = true;
              break;
            }
          case '/':
            {
              ch = buffer.read();
              if ((ch == '*') && (!blkComment)) {
                offsets = new int[2];
                offsets[0] = cnt;
                blkComment = true;
                cnt++;
              } else {
                cnt++;
              }
              cnt++;
              break;
            }
          case '*':
            {
              if (blkComment) {
                ch = buffer.read();
                cnt++;
                if (ch == '/') {
                  blkComment = false;
                  offsets[1] = cnt;
                  blockComments.add(offsets);
                }
              }
              cnt++;
              break;
            }
          default:
            {
              cnt++;
              break;
            }
        }
      }
    } catch (IOException e) {
      // ignore errors
    }
  }

  /** A simple fuzzy scanner for Java */
  public class CodeScanner {

    protected Map<String, Integer> reservedKeywords = new HashMap<>();
    protected Map<String, Integer> reservedFunctionNames = new HashMap<>();
    protected StringBuilder fBuffer = new StringBuilder();
    protected String fDoc;
    protected int fPos;
    protected int fEnd;
    protected int fStartToken;
    protected boolean fEofSeen = false;

    public CodeScanner(List<String> keywords, List<String> functions) {
      this.addKeywords(keywords);
      this.addFunctionNames(functions);
    }

    /** Returns the ending location of the current token in the document. */
    public final int getLength() {
      return fPos - fStartToken;
    }

    public void addKeywords(List<String> reservedWords) {
      if (Utils.isEmpty(reservedWords)) {
        return;
      }
      reservedWords.forEach(name -> reservedKeywords.put(name, Integer.valueOf(KEY)));
    }

    public void addFunctionNames(List<String> functionNames) {
      if (Utils.isEmpty(functionNames)) {
        return;
      }
      functionNames.forEach(name -> reservedFunctionNames.put(name, Integer.valueOf(FUNCTION)));
    }

    /** Returns the starting location of the current token in the document. */
    public final int getStartOffset() {
      return fStartToken;
    }

    /** Returns the next lexical token in the document. */
    public int nextToken() {
      int c;
      fStartToken = fPos;
      while (true) {
        switch (c = read()) {
          case EOF:
            return EOF;
          case '/': // comment
            c = read();
            if (c == '/') {
              while (true) {
                c = read();
                if ((c == EOF) || (c == EOL)) {
                  unread(c);
                  return COMMENT;
                }
              }
            } else {
              unread(c);
            }
            return OTHER;
          case '\'': // char const
            for (; ; ) {
              c = read();
              switch (c) {
                case '\'':
                  return STRING;
                case EOF:
                  unread(c);
                  return STRING;
                case '\\':
                  c = read();
                  break;
                default:
                  break;
              }
            }

          case '"': // string
            for (; ; ) {
              c = read();
              switch (c) {
                case '"':
                  return STRING;
                case EOF:
                  unread(c);
                  return STRING;
                case '\\':
                  c = read();
                  break;
                default:
                  break;
              }
            }

          case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
            do {
              c = read();
            } while (Character.isDigit((char) c));
            unread(c);
            return NUMBER;
          default:
            if (Character.isWhitespace((char) c)) {
              do {
                c = read();
              } while (Character.isWhitespace((char) c));
              unread(c);
              return WHITE;
            }
            if (Character.isJavaIdentifierStart((char) c)) {
              fBuffer.setLength(0);
              do {
                fBuffer.append((char) c);
                c = read();
              } while (Character.isJavaIdentifierPart((char) c));
              unread(c);
              String name = fBuffer.toString();
              Integer token = reservedKeywords.get(name);
              if (token != null) {
                return token.intValue();
              }
              token = reservedFunctionNames.get(name);
              if (token != null) {
                return token.intValue();
              }
              return WORD;
            }
            return OTHER;
        }
      }
    }

    /** Returns next character. */
    protected int read() {
      if (fPos <= fEnd) {
        return fDoc.charAt(fPos++);
      }
      return EOF;
    }

    public void setRange(String text) {
      fDoc = text;
      fPos = 0;
      fEnd = fDoc.length() - 1;
    }

    protected void unread(int c) {
      if (c != EOF) {
        fPos--;
      }
    }
  }
}
