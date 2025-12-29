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
import org.apache.hop.core.database.SqlScriptStatement;
import org.apache.hop.core.util.Utils;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.LineStyleEvent;
import org.eclipse.swt.custom.LineStyleListener;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;

public class SqlHighlight implements LineStyleListener {
  SqlScanner scanner;
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
  public static final int PARAM = 9;

  public static final int MAXIMUM_TOKEN = 10;

  private List<SqlScriptStatement> scriptStatements;

  public SqlHighlight() {
    this(List.of());
  }

  public SqlHighlight(List<String> functionNames) {
    initializeStyles();
    scriptStatements = new ArrayList<>();
    scanner = new SqlScanner(functionNames);
  }

  StyleAttribute getStyleAttribute(int type) {
    if (type < 0 || type >= styleAttributes.length) {
      return null;
    }
    return styleAttributes[type];
  }

  boolean inBlockComment(int start, int end) {
    for (int[] offsets : blockComments) {
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
    styleAttributes[PARAM] = new StyleAttribute(resource.getColor(148, 0, 211), SWT.BOLD);
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
            StyleRange styleRange =
                new StyleRange(
                    scanner.getStartOffset() + event.lineOffset,
                    scanner.getLength(),
                    attribute.getForeground(),
                    null,
                    attribute.getStyle());
            if (styles.isEmpty()) {
              styles.add(styleRange);
            } else {
              lastStyle = styles.get(styles.size() - 1);
              if (lastStyle.similarTo(styleRange)
                  && (lastStyle.start + lastStyle.length == styleRange.start)) {
                lastStyle.length += styleRange.length;
              } else {
                styles.add(styleRange);
              }
            }
          }
        }
      }
      token = scanner.nextToken();
    }

    // See which backgrounds to color...
    //
    if (scriptStatements != null) {
      for (SqlScriptStatement statement : scriptStatements) {
        // Leave non-executed statements alone.
        //
        StyleRange styleRange = new StyleRange();
        styleRange.start = statement.getFromIndex();
        styleRange.length = statement.getToIndex() - statement.getFromIndex();

        if (statement.isComplete()) {
          if (statement.isOk()) {
            // GuiResource.getInstance().getColor(63, 127, 95), // green

            styleRange.background = GuiResource.getInstance().getColor(244, 238, 224); // honey dew
          } else {
            styleRange.background =
                GuiResource.getInstance().getColor(250, 235, 215); // Antique White
          }
        } else {
          styleRange.background = GuiResource.getInstance().getColorWhite();
        }

        styles.add(styleRange);
      }
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

  /** A simple fuzzy scanner for SQL */
  public class SqlScanner {
    protected Map<String, Integer> reservedKeywords = new HashMap<>();
    protected Map<String, Integer> reservedFunctionNames = new HashMap<>();
    protected StringBuilder fBuffer = new StringBuilder();
    protected String fDoc;
    protected int fPos;
    protected int fEnd;
    protected int fStartToken;
    protected boolean fEofSeen = false;

    private static final List<String> DEFAULT_FUNCTIONS =
        List.of(
            "getdate",
            "case",
            "convert",
            "left",
            "right",
            "isnumeric",
            "isdate",
            "isnumber",
            "number",
            "finally",
            "cast",
            "var",
            "fetch_status",
            "isnull",
            "charindex",
            "difference",
            "len",
            "nchar",
            "quotename",
            "replicate",
            "reverse",
            "str",
            "stuff",
            "unicode",
            "ascii",
            "char",
            "to_char",
            "to_date",
            "to_number",
            "nvl",
            "sysdate",
            "corr",
            "count",
            "grouping",
            "max",
            "min",
            "stdev",
            "sum",
            "concat",
            "length",
            "locate",
            "ltrim",
            "posstr",
            "repeat",
            "replace",
            "rtrim",
            "soundex",
            "space",
            "substr",
            "substring",
            "trunc",
            "nextval",
            "currval",
            "getclobval",
            "char_length",
            "compare",
            "patindex",
            "sortkey",
            "uscalar",
            "current_date",
            "current_time",
            "current_timestamp",
            "current_user",
            "session_user",
            "system_user",
            "curdate",
            "curtime",
            "database",
            "now",
            "sysdate",
            "today",
            "user",
            "version",
            "coalesce",
            "nullif",
            "octet_length",
            "datalength",
            "decode",
            "greatest",
            "ifnull",
            "least",
            "char_length",
            "character_length",
            "collate",
            "concatenate",
            "like",
            "lower",
            "position",
            "translate",
            "upper",
            "char_octet_length",
            "character_maximum_length",
            "character_octet_length",
            "ilike",
            "initcap",
            "instr",
            "lcase",
            "lpad",
            "patindex",
            "rpad",
            "ucase",
            "bit_length",
            "abs",
            "asin",
            "atan",
            "ceiling",
            "cos",
            "cot",
            "exp",
            "floor",
            "ln",
            "log",
            "log10",
            "mod",
            "pi",
            "power",
            "rand",
            "round",
            "sign",
            "sin",
            "sqrt",
            "tan",
            "trunc",
            "extract",
            "interval",
            "overlaps",
            "adddate",
            "age",
            "date_add",
            "dateformat",
            "date_part",
            "date_sub",
            "datediff",
            "dateadd",
            "datename",
            "datepart",
            "day",
            "dayname",
            "dayofmonth",
            "dayofweek",
            "dayofyear",
            "hour",
            "last_day",
            "minute",
            "month",
            "month_between",
            "monthname",
            "next_day",
            "second",
            "sub_date",
            "week",
            "year",
            "dbo",
            "log",
            "objectproperty");

    private static final List<String> KEYWORDS =
        List.of(
            "create",
            "procedure",
            "as",
            "set",
            "nocount",
            "on",
            "declare",
            "varchar",
            "print",
            "table",
            "int",
            "tintytext",
            "select",
            "from",
            "where",
            "and",
            "or",
            "insert",
            "into",
            "cursor",
            "read_only",
            "for",
            "open",
            "fetch",
            "next",
            "end",
            "deallocate",
            "table",
            "drop",
            "exec",
            "begin",
            "close",
            "update",
            "delete",
            "truncate",
            "left",
            "inner",
            "outer",
            "cross",
            "join",
            "union",
            "all",
            "float",
            "when",
            "nolock",
            "with",
            "false",
            "datetime",
            "dare",
            "time",
            "hour",
            "array",
            "minute",
            "second",
            "millisecond",
            "view",
            "function",
            "catch",
            "const",
            "continue",
            "compute",
            "browse",
            "option",
            "date",
            "default",
            "do",
            "raw",
            "auto",
            "explicit",
            "xmldata",
            "elements",
            "binary",
            "base64",
            "read",
            "outfile",
            "asc",
            "desc",
            "else",
            "eval",
            "escape",
            "having",
            "limit",
            "offset",
            "of",
            "intersect",
            "except",
            "using",
            "variance",
            "specific",
            "language",
            "body",
            "returns",
            "specific",
            "deterministic",
            "not",
            "external",
            "action",
            "reads",
            "static",
            "inherit",
            "called",
            "order",
            "group",
            "by",
            "natural",
            "full",
            "exists",
            "between",
            "some",
            "any",
            "unique",
            "match",
            "value",
            "limite",
            "minus",
            "references",
            "grant",
            "on",
            "top",
            "index",
            "bigint",
            "text",
            "char",
            "use",
            "move",
            "exec",
            "init",
            "name",
            "noskip",
            "skip",
            "noformat",
            "format",
            "stats",
            "disk",
            "from",
            "to",
            "rownum",
            "alter",
            "add",
            "remove",
            "move",
            "alter",
            "add",
            "remove",
            "lineno",
            "modify",
            "if",
            "else",
            "in",
            "is",
            "new",
            "Number",
            "null",
            "string",
            "switch",
            "this",
            "then",
            "throw",
            "true",
            "false",
            "try",
            "return",
            "with",
            "while",
            "start",
            "connect",
            "optimize",
            "first",
            "only",
            "rows",
            "sequence",
            "blob",
            "clob",
            "image",
            "binary",
            "column",
            "decimal",
            "distinct",
            "primary",
            "key",
            "timestamp",
            "varbinary",
            "nvarchar",
            "nchar",
            "longnvarchar",
            "nclob",
            "numeric",
            "constraint",
            "dbcc",
            "backup",
            "bit",
            "clustered",
            "pad_index",
            "off",
            "statistics_norecompute",
            "ignore_dup_key",
            "allow_row_locks",
            "allow_page_locks",
            "textimage_on",
            "double",
            "rollback",
            "tran",
            "transaction",
            "commit");

    public SqlScanner(List<String> functionNames) {
      addKeywords(KEYWORDS);
      addFunctionNames(functionNames);

      // Use default functions
      if (reservedFunctionNames.isEmpty()) {
        addFunctionNames(SqlScanner.DEFAULT_FUNCTIONS);
      }
    }

    public void addKeywords(List<String> keywords) {
      if (Utils.isEmpty(keywords)) {
        return;
      }
      keywords.forEach(name -> reservedKeywords.put(name, KEY));
    }

    public void addFunctionNames(List<String> functionNames) {
      if (Utils.isEmpty(functionNames)) {
        return;
      }
      functionNames.forEach(name -> reservedFunctionNames.put(name, FUNCTION));
    }

    /** Returns the ending location of the current token in the document. */
    public final int getLength() {
      return fPos - fStartToken;
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
            }
            unread(c);
            return SYMBOL;
          case '-': // comment
            c = read();
            if (c == '-') {
              while (true) {
                c = read();
                if ((c == EOF) || (c == EOL)) {
                  unread(c);
                  return COMMENT;
                }
              }
            }
            unread(c);
            return SYMBOL;

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

          case '(', ')', '*', '+', '%', '=', '>', '<', '^', '!', ':', '.', ';':
            return SYMBOL;

          case '?':
            return PARAM;

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
              // Keywords and functions are not case-sensitive
              String name = fBuffer.toString();
              if (c == '(') {
                Integer token = reservedFunctionNames.get(name);
                if (token != null) {
                  return token;
                }
              } else {
                Integer token = reservedKeywords.get(name);
                if (token != null) {
                  return token;
                }
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
      fDoc = text.toLowerCase();
      fPos = 0;
      fEnd = fDoc.length() - 1;
    }

    protected void unread(int c) {
      if (c != EOF) {
        fPos--;
      }
    }
  }

  /**
   * @return the scriptStatements
   */
  public List<SqlScriptStatement> getScriptStatements() {
    return scriptStatements;
  }

  /**
   * @param scriptStatements the scriptStatements to set
   */
  public void setScriptStatements(List<SqlScriptStatement> scriptStatements) {
    this.scriptStatements = scriptStatements;
  }
}
