/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.ui.core.PropsUi;
import org.eclipse.jface.text.IDocument;
import org.eclipse.jface.text.TextAttribute;
import org.eclipse.jface.text.rules.IToken;
import org.eclipse.jface.text.rules.Token;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.RGB;
import org.eclipse.swt.widgets.Display;
import org.eclipse.tm4e.core.grammar.IGrammar;
import org.eclipse.tm4e.core.grammar.IStateStack;
import org.eclipse.tm4e.core.grammar.ITokenizeLineResult;
import org.eclipse.tm4e.core.registry.IGrammarSource;
import org.eclipse.tm4e.core.registry.IRegistryOptions;
import org.eclipse.tm4e.core.registry.Registry;

/**
 * TM4E-based parsing for the content editor. Loads TextMate grammars, tokenizes with TM4E, and
 * applies Hop's own color palette (light/dark) for highlighting.
 */
final class ContentEditorTm4eSupport {

  /** Enable with -Dhop.contenteditor.trace.scopes=true to print TM4E token scopes to stderr. */
  private static final boolean TRACE_SCOPES = Boolean.getBoolean("hop.contenteditor.trace.scopes");

  private static final String SCOPE_JSON = "source.json";
  private static final String SCOPE_TEXT = "text.plain";
  private static final String SCOPE_XML = "text.xml";
  private static final String SCOPE_SQL = "source.sql";
  private static final String SCOPE_PYTHON = "source.python";
  private static final String SCOPE_YAML = "source.yaml";
  private static final String SCOPE_SHELL = "source.shell";
  private static final String SCOPE_BATCH = "source.batchfile";
  private static final String SCOPE_MARKDOWN = "text.html.markdown";

  /** Maps TM4E scope names to grammar resource filenames (classpath-relative to grammars/). */
  private static final Map<String, String> GRAMMAR_FILES =
      Map.of(
          SCOPE_JSON, "json.json",
          SCOPE_XML, "xml.json",
          SCOPE_SQL, "sql.json",
          SCOPE_TEXT, "text.json",
          SCOPE_PYTHON, "python.json",
          SCOPE_YAML, "yaml.json",
          SCOPE_SHELL, "shell.json",
          SCOPE_BATCH, "bat.json",
          SCOPE_MARKDOWN, "markdown.json");

  // Same palette as before (light/dark) for consistency
  private static final RGB L_COMMENT = new RGB(128, 128, 128);
  private static final RGB L_STRING = new RGB(0, 128, 0);
  private static final RGB L_JSON_KEY = new RGB(128, 0, 0);
  private static final RGB L_KEYWORD = new RGB(0, 0, 255);
  private static final RGB L_TAG = new RGB(128, 0, 128);
  private static final RGB L_XML_HEADER = new RGB(100, 100, 180);
  private static final RGB L_NUMBER = new RGB(0, 128, 128);
  private static final RGB L_CONSTANT = new RGB(0, 0, 255);
  private static final RGB L_DEFAULT = new RGB(0, 0, 0);

  private static final RGB D_COMMENT = new RGB(106, 153, 85);
  private static final RGB D_STRING = new RGB(206, 145, 120);
  private static final RGB D_JSON_KEY = new RGB(156, 220, 254);
  private static final RGB D_KEYWORD = new RGB(86, 156, 214);
  private static final RGB D_TAG = new RGB(192, 150, 210);
  private static final RGB D_XML_HEADER = new RGB(150, 180, 220);
  private static final RGB D_NUMBER = new RGB(181, 206, 168);
  private static final RGB D_CONSTANT = new RGB(86, 156, 214);
  private static final RGB D_DEFAULT = new RGB(212, 212, 212);

  private final Registry registry;
  private final Display display;
  private final Map<String, TextAttribute> attributeCache = new HashMap<>();
  private final Map<RGB, Color> colorCache = new HashMap<>();

  private ContentEditorTm4eSupport(Display display) {
    this.display = display != null && !display.isDisposed() ? display : Display.getDefault();
    this.registry = new Registry(new GrammarRegistryOptions());
  }

  private static boolean isDark() {
    try {
      return PropsUi.getInstance().isDarkMode();
    } catch (Exception e) {
      return false;
    }
  }

  /** Returns the grammar scope name for the given language id, or null if not supported by TM4E. */
  static String scopeForLanguage(String languageId) {
    if (languageId == null) {
      return null;
    }

    return switch (languageId.toLowerCase(java.util.Locale.ROOT)) {
      case "json" -> SCOPE_JSON;
      case "xml" -> SCOPE_XML;
      case "sql" -> SCOPE_SQL;
      case "python", "py" -> SCOPE_PYTHON;
      case "yaml", "yml" -> SCOPE_YAML;
      case "shell", "bash", "sh" -> SCOPE_SHELL;
      case "bat", "cmd", "batch" -> SCOPE_BATCH;
      case "markdown", "md" -> SCOPE_MARKDOWN;
      case "plaintext" -> SCOPE_TEXT;
      default -> null;
    };
  }

  /**
   * Creates a TM4E-backed configuration for the given language if a grammar is available; otherwise
   * returns null (caller should fall back to rule-based).
   */
  static org.eclipse.jface.text.source.SourceViewerConfiguration createConfiguration(
      String languageId, Display display) {
    String scopeName = scopeForLanguage(languageId);
    if (scopeName == null) {
      return null;
    }
    ContentEditorTm4eSupport support = new ContentEditorTm4eSupport(display);
    IGrammar grammar = support.getGrammar(scopeName);
    if (grammar == null) {
      return null;
    }
    return ContentEditorTm4eSupport.createReconciler(support, grammar);
  }

  private IGrammar getGrammar(String scopeName) {
    try {
      return registry.loadGrammar(scopeName);
    } catch (Exception e) {
      org.apache.hop.core.logging.LogChannel.UI.logError(
          "Failed to load TM4E grammar '" + scopeName + "': " + e.getMessage(), e);
      return null;
    }
  }

  /** Grammar source from classpath resources under grammars/. All grammar files are JSON format. */
  private static final class GrammarRegistryOptions implements IRegistryOptions {
    @Override
    public IGrammarSource getGrammarSource(String scopeName) {
      String fileName = GRAMMAR_FILES.get(scopeName);
      if (fileName == null) {
        return null;
      }
      return new IGrammarSource() {
        @Override
        public URI getURI() {
          return URI.create("hop://grammar/" + scopeName);
        }

        @Override
        public Reader getReader() throws IOException {
          java.io.InputStream in =
              ContentEditorTm4eSupport.class.getResourceAsStream("grammars/" + fileName);
          if (in == null) {
            throw new IOException("Grammar resource not found: grammars/" + fileName);
          }
          return new InputStreamReader(in, StandardCharsets.UTF_8);
        }

        @Override
        public long getLastModified() {
          return 0;
        }

        @Override
        public IGrammarSource.ContentType getContentType() {
          return IGrammarSource.ContentType.JSON;
        }
      };
    }

    @Override
    public Collection<String> getInjections(String scopeName) {
      return List.of();
    }
  }

  private static org.eclipse.jface.text.source.SourceViewerConfiguration createReconciler(
      ContentEditorTm4eSupport support, IGrammar grammar) {
    return new org.eclipse.jface.text.source.SourceViewerConfiguration() {
      @Override
      public org.eclipse.jface.text.presentation.IPresentationReconciler getPresentationReconciler(
          org.eclipse.jface.text.source.ISourceViewer viewer) {
        org.eclipse.jface.text.presentation.PresentationReconciler reconciler =
            new org.eclipse.jface.text.presentation.PresentationReconciler();
        reconciler.setDocumentPartitioning(
            org.eclipse.jface.text.IDocumentExtension3.DEFAULT_PARTITIONING);
        Tm4eDamagerRepairer repairer = new Tm4eDamagerRepairer(grammar, support::scopeToAttribute);
        reconciler.setDamager(repairer, org.eclipse.jface.text.IDocument.DEFAULT_CONTENT_TYPE);
        reconciler.setRepairer(repairer, org.eclipse.jface.text.IDocument.DEFAULT_CONTENT_TYPE);
        return reconciler;
      }
    };
  }

  /**
   * Maps TM4E scope list to our TextAttribute (same palette as rule-based). Attributes are cached
   * per scope combination: a document produces the same handful of combinations over and over.
   */
  private org.eclipse.jface.text.TextAttribute scopeToAttribute(List<String> scopes) {
    String scope = scopes == null ? "" : String.join(" ", scopes);
    return attributeCache.computeIfAbsent(
        scope,
        key -> {
          Color color = colorCache.computeIfAbsent(scopeToRgb(key), rgb -> new Color(display, rgb));
          return new org.eclipse.jface.text.TextAttribute(color, null, scopeToFontStyle(key));
        });
  }

  /**
   * Font style (bold, italic, underline, strike-through) for the given scope. Only Markdown markup
   * scopes use anything but the plain style today.
   */
  private static int scopeToFontStyle(String scope) {
    int style = SWT.NORMAL;
    if (scope.contains("markup.heading") || scope.contains("markup.bold")) {
      style |= SWT.BOLD;
    }
    if (scope.contains(".italic") || scope.contains("markup.quote")) {
      style |= SWT.ITALIC;
    }
    if (scope.contains("markup.underline")) {
      style |= org.eclipse.jface.text.TextAttribute.UNDERLINE;
    }
    if (scope.contains("markup.strikethrough")) {
      style |= org.eclipse.jface.text.TextAttribute.STRIKETHROUGH;
    }
    return style;
  }

  private RGB scopeToRgb(String scope) {
    if (scope.isEmpty()) {
      return isDark() ? D_DEFAULT : L_DEFAULT;
    }
    boolean dark = isDark();

    if (TRACE_SCOPES) {
      System.err.println("[TM4E scopes] " + scope);
    }

    // Comments (all languages)
    if (scope.contains("comment")) {
      return dark ? D_COMMENT : L_COMMENT;
    }

    // Markdown markup. Checked before the generic rules below because the inner scopes are named
    // after what they mean in a document (heading, quote, raw, ...) rather than after a token type.
    if (scope.contains("markup.heading")) {
      return dark ? D_KEYWORD : L_JSON_KEY;
    }
    if (scope.contains("markup.underline.link")) {
      return dark ? D_JSON_KEY : L_KEYWORD;
    }
    if (scope.contains("markup.quote")
        || scope.contains("meta.separator")
        || scope.contains("punctuation.definition.table")) {
      return dark ? D_COMMENT : L_COMMENT;
    }
    if (scope.contains("fenced_code.block.language")
        || scope.contains("punctuation.definition.list")) {
      return dark ? D_KEYWORD : L_KEYWORD;
    }
    if (scope.contains("markup.inline.raw") || scope.contains("markup.fenced_code")) {
      return dark ? D_STRING : L_STRING;
    }
    if (scope.contains("markup.bold")
        || scope.contains("markup.italic")
        || scope.contains("markup.strikethrough")) {
      return dark ? D_KEYWORD : L_KEYWORD;
    }

    // JSON keys: must be checked before "string" - VS Code uses "support.type.property-name.json"
    if (scope.contains("support.type.property-name") || scope.contains("property-name")) {
      return dark ? D_JSON_KEY : L_JSON_KEY;
    }

    // YAML keys: TextMate uses entity.name.tag.yaml (often with string.unquoted.*)
    if (scope.contains("entity.name.tag.yaml")) {
      return dark ? D_JSON_KEY : L_JSON_KEY;
    }

    // Strings (JSON: string.quoted.double; XML/SQL: string.quoted.*; YAML: string.*)
    if (scope.contains("string")) {
      return dark ? D_STRING : L_STRING;
    }

    // Numbers and constants
    if (scope.contains("constant.numeric") || scope.contains("number")) {
      return dark ? D_NUMBER : L_NUMBER;
    }
    if (scope.contains("constant.language")) {
      return dark ? D_CONSTANT : L_CONSTANT;
    }
    if (scope.contains("constant.other") || scope.contains("constant.character")) {
      return dark ? D_CONSTANT : L_CONSTANT;
    }

    // Keywords (SQL, etc.)
    if (scope.contains("keyword")
        || scope.contains("support.function")
        || scope.contains("support.type")) {
      return dark ? D_KEYWORD : L_KEYWORD;
    }
    if (scope.contains("storage.type") || scope.contains("storage.modifier")) {
      return dark ? D_KEYWORD : L_KEYWORD;
    }
    if (scope.contains("entity.name.function")) {
      return dark ? D_KEYWORD : L_KEYWORD;
    }

    // Variables ($VAR in shell, %VAR% in batch): VS Code renders these in the same light blue as
    // JSON keys. Checked after string so interpolation inside a string stays string-colored.
    if (scope.contains("variable")) return dark ? D_JSON_KEY : L_JSON_KEY;

    // XML: <? ?> then tags then attribute names
    if (scope.contains("meta.tag.preprocessor")) {
      return dark ? D_XML_HEADER : L_XML_HEADER;
    }
    if (scope.contains("entity.name.tag") || scope.contains(".tag")) {
      return dark ? D_TAG : L_TAG;
    }
    if (scope.contains("entity.other.attribute-name")) {
      return dark ? D_JSON_KEY : L_JSON_KEY;
    }

    return dark ? D_DEFAULT : L_DEFAULT;
  }

  /** A stretch of text and the TM4E scopes covering it. No scopes means unscoped text. */
  record ScopedRange(int offset, int length, List<String> scopes) {}

  /**
   * Tokenizes the text with the grammar and returns the part covering [rangeOffset, rangeOffset +
   * rangeLength) as consecutive scoped ranges.
   *
   * <p>The ranges tile that interval: every character is covered exactly once, including the line
   * delimiters TM4E doesn't tokenize. That is not cosmetic. {@link
   * org.eclipse.jface.text.rules.DefaultDamagerRepairer#createPresentation} merges neighbouring
   * tokens that share a {@link TextAttribute} by adding up their lengths, without looking at their
   * offsets, so a gap in the stream shortens the resulting style range by the size of that gap.
   * Leaving the delimiters out cost a run of same-coloured lines one character per line with LF and
   * two with CRLF, which is why highlighting stopped short of the end of a run and why it looked
   * like a Windows bug (issue #7971).
   */
  static List<ScopedRange> tokenize(
      IGrammar grammar, String text, int rangeOffset, int rangeLength) {
    int rangeEnd = Math.min(rangeOffset + rangeLength, text.length());
    List<ScopedRange> ranges = new java.util.ArrayList<>();
    int cursor = Math.max(rangeOffset, 0);

    for (ScopedRange token : tokenizeLines(grammar, text, cursor, rangeEnd)) {
      if (token.offset() > cursor) {
        // Text no token covers, i.e. a line delimiter: keep the stream contiguous
        ranges.add(new ScopedRange(cursor, token.offset() - cursor, List.of()));
      }
      ranges.add(token);
      cursor = token.offset() + token.length();
    }
    if (cursor < rangeEnd) {
      ranges.add(new ScopedRange(cursor, rangeEnd - cursor, List.of()));
    }
    return ranges;
  }

  /** Damager/repairer that uses TM4E to tokenize and applies our attributes. */
  private static final class Tm4eDamagerRepairer
      extends org.eclipse.jface.text.rules.DefaultDamagerRepairer {

    Tm4eDamagerRepairer(
        IGrammar grammar,
        java.util.function.Function<List<String>, org.eclipse.jface.text.TextAttribute>
            scopeToAttr) {
      super(new Tm4eScanner(grammar, scopeToAttr));
    }
  }

  /** JFace ITokenScanner that tokenizes with TM4E and returns tokens with our attributes. */
  private static final class Tm4eScanner implements org.eclipse.jface.text.rules.ITokenScanner {

    private final IGrammar grammar;
    private final java.util.function.Function<List<String>, org.eclipse.jface.text.TextAttribute>
        scopeToAttr;

    private java.util.List<ScopedRange> ranges;
    private int index;
    private int tokenOffset;
    private int tokenLength;

    Tm4eScanner(
        IGrammar grammar,
        java.util.function.Function<List<String>, org.eclipse.jface.text.TextAttribute>
            scopeToAttr) {
      this.grammar = grammar;
      this.scopeToAttr = scopeToAttr;
    }

    @Override
    public void setRange(IDocument doc, int offset, int length) {
      try {
        this.ranges = tokenize(grammar, doc.get(), offset, length);
      } catch (Exception ignored) {
        this.ranges = Collections.emptyList();
      }
      this.index = 0;
      this.tokenOffset = 0;
      this.tokenLength = 0;
    }

    @Override
    public IToken nextToken() {
      if (ranges == null || index >= ranges.size()) {
        tokenOffset = tokenLength = 0;
        return Token.EOF;
      }
      ScopedRange range = ranges.get(index++);
      tokenOffset = range.offset();
      tokenLength = range.length();
      return new Token(scopeToAttr.apply(range.scopes()));
    }

    @Override
    public int getTokenOffset() {
      return tokenOffset;
    }

    @Override
    public int getTokenLength() {
      return tokenLength;
    }
  }

  private static final int MAX_LINES_TO_TOKENIZE = 100_000;
  private static final int MAX_LINE_LENGTH = 100_000;

  /** The scoped ranges the grammar produces, line by line, clipped to the requested range. */
  private static List<ScopedRange> tokenizeLines(
      IGrammar grammar, String text, int rangeOffset, int rangeEnd) {
    List<ScopedRange> result = new java.util.ArrayList<>();
    try {
      String[] lines = text.split("\\n", -1);
      long lineStart = 0;
      IStateStack state = null;

      for (int lineIndex = 0; lineIndex < lines.length; lineIndex++) {
        if (lineIndex >= MAX_LINES_TO_TOKENIZE) {
          break;
        }

        String raw = lines[lineIndex];
        int rawLen = raw.length();
        String line = raw;
        if (rawLen > 0 && raw.charAt(rawLen - 1) == '\r') {
          line = raw.substring(0, rawLen - 1);
        }
        if (line.length() > MAX_LINE_LENGTH) {
          lineStart += rawLen + 1L;
          continue;
        }

        long lineEnd = lineStart + rawLen;
        if (lineEnd <= rangeOffset) {
          ITokenizeLineResult<org.eclipse.tm4e.core.grammar.IToken[]> res =
              grammar.tokenizeLine(line, state, null);
          state = res.getRuleStack();
          lineStart = lineEnd + 1;
          continue;
        }
        if (lineStart >= rangeEnd) break;

        ITokenizeLineResult<org.eclipse.tm4e.core.grammar.IToken[]> res =
            grammar.tokenizeLine(line, state, null);
        state = res.getRuleStack();

        for (org.eclipse.tm4e.core.grammar.IToken t : res.getTokens()) {
          long tStartLong = lineStart + t.getStartIndex();
          long tEndLong = lineStart + t.getEndIndex();
          if (tStartLong > Integer.MAX_VALUE || tEndLong > Integer.MAX_VALUE) continue;
          int tStart = (int) tStartLong;
          int tEnd = (int) tEndLong;
          if (tEnd <= rangeOffset || tStart >= rangeEnd) continue;
          int o = Math.max(tStart, rangeOffset);
          int l = Math.min(tEnd, rangeEnd) - o;
          if (l <= 0) continue;
          List<String> tokenScopes = t.getScopes();
          if (TRACE_SCOPES && tokenScopes != null) {
            System.err.println(
                "[TM4E token] offset=" + o + " len=" + l + " | " + String.join(" ", tokenScopes));
          }
          result.add(new ScopedRange(o, l, tokenScopes == null ? List.of() : tokenScopes));
        }
        lineStart = lineEnd + 1;
      }
    } catch (Exception ignored) {
      // ignore
    }
    return result;
  }
}
