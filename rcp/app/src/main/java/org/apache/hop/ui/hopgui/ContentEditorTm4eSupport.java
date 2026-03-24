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
import java.util.List;
import java.util.Map;
import org.apache.hop.ui.core.PropsUi;
import org.eclipse.jface.text.IDocument;
import org.eclipse.jface.text.rules.IToken;
import org.eclipse.jface.text.rules.Token;
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
  private static final String SCOPE_XML = "text.xml";
  private static final String SCOPE_SQL = "source.sql";

  /** Maps TM4E scope names to grammar resource filenames (classpath-relative to grammars/). */
  private static final Map<String, String> GRAMMAR_FILES =
      Map.of(
          SCOPE_JSON, "json.json",
          SCOPE_XML, "xml.json",
          SCOPE_SQL, "sql.json");

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
    if (languageId == null) return null;
    switch (languageId.toLowerCase(java.util.Locale.ROOT)) {
      case "json":
        return SCOPE_JSON;
      case "xml":
        return SCOPE_XML;
      case "sql":
        return SCOPE_SQL;
      default:
        return null;
    }
  }

  /**
   * Creates a TM4E-backed configuration for the given language if a grammar is available; otherwise
   * returns null (caller should fall back to rule-based).
   */
  static org.eclipse.jface.text.source.SourceViewerConfiguration createConfiguration(
      String languageId, Display display) {
    String scopeName = scopeForLanguage(languageId);
    if (scopeName == null) return null;
    ContentEditorTm4eSupport support = new ContentEditorTm4eSupport(display);
    IGrammar grammar = support.getGrammar(scopeName);
    if (grammar == null) return null;
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
        Tm4eDamagerRepairer repairer =
            new Tm4eDamagerRepairer(grammar, support::scopeToAttribute, support.display);
        reconciler.setDamager(repairer, org.eclipse.jface.text.IDocument.DEFAULT_CONTENT_TYPE);
        reconciler.setRepairer(repairer, org.eclipse.jface.text.IDocument.DEFAULT_CONTENT_TYPE);
        return reconciler;
      }
    };
  }

  /** Maps TM4E scope list to our TextAttribute (same palette as rule-based). */
  private org.eclipse.jface.text.TextAttribute scopeToAttribute(List<String> scopes) {
    RGB rgb = scopeToRgb(scopes);
    Color color = new Color(display, rgb);
    return new org.eclipse.jface.text.TextAttribute(color);
  }

  private RGB scopeToRgb(List<String> scopes) {
    if (scopes == null || scopes.isEmpty()) return isDark() ? D_DEFAULT : L_DEFAULT;
    String scope = String.join(" ", scopes);
    boolean dark = isDark();

    if (TRACE_SCOPES) {
      System.err.println("[TM4E scopes] " + scope);
    }

    // Comments (all languages)
    if (scope.contains("comment")) return dark ? D_COMMENT : L_COMMENT;

    // JSON keys: must be checked before "string" - VS Code uses "support.type.property-name.json"
    if (scope.contains("support.type.property-name") || scope.contains("property-name"))
      return dark ? D_JSON_KEY : L_JSON_KEY;

    // Strings (JSON: string.quoted.double; XML/SQL: string.quoted.*)
    if (scope.contains("string")) return dark ? D_STRING : L_STRING;

    // Numbers and constants
    if (scope.contains("constant.numeric") || scope.contains("number"))
      return dark ? D_NUMBER : L_NUMBER;
    if (scope.contains("constant.language")) return dark ? D_CONSTANT : L_CONSTANT;
    if (scope.contains("constant.other")) return dark ? D_CONSTANT : L_CONSTANT;

    // Keywords (SQL, etc.)
    if (scope.contains("keyword")
        || scope.contains("support.function")
        || scope.contains("support.type")) return dark ? D_KEYWORD : L_KEYWORD;
    if (scope.contains("storage.type") || scope.contains("storage.modifier"))
      return dark ? D_KEYWORD : L_KEYWORD;
    if (scope.contains("entity.name.function")) return dark ? D_KEYWORD : L_KEYWORD;

    // XML: <? ?> then tags then attribute names
    if (scope.contains("meta.tag.preprocessor")) return dark ? D_XML_HEADER : L_XML_HEADER;
    if (scope.contains("entity.name.tag") || scope.contains(".tag")) return dark ? D_TAG : L_TAG;
    if (scope.contains("entity.other.attribute-name")) return dark ? D_JSON_KEY : L_JSON_KEY;

    return dark ? D_DEFAULT : L_DEFAULT;
  }

  /** Damager/repairer that uses TM4E to tokenize and applies our attributes. */
  private static final class Tm4eDamagerRepairer
      extends org.eclipse.jface.text.rules.DefaultDamagerRepairer {
    private final IGrammar grammar;
    private final java.util.function.Function<List<String>, org.eclipse.jface.text.TextAttribute>
        scopeToAttr;
    private final Display display;

    Tm4eDamagerRepairer(
        IGrammar grammar,
        java.util.function.Function<List<String>, org.eclipse.jface.text.TextAttribute> scopeToAttr,
        Display display) {
      super(new Tm4eScanner(grammar, scopeToAttr, display));
      this.grammar = grammar;
      this.scopeToAttr = scopeToAttr;
      this.display = display;
    }
  }

  /** JFace ITokenScanner that tokenizes with TM4E and returns tokens with our attributes. */
  private static final class Tm4eScanner implements org.eclipse.jface.text.rules.ITokenScanner {

    private static final int MAX_LINES_TO_TOKENIZE = 100_000;
    private static final int MAX_LINE_LENGTH = 100_000;

    private final IGrammar grammar;
    private final java.util.function.Function<List<String>, org.eclipse.jface.text.TextAttribute>
        scopeToAttr;
    private final Display display;

    private IDocument document;
    private int rangeOffset;
    private int rangeLength;
    private java.util.List<ColoredToken> tokens;
    private int index;
    private int tokenOffset;
    private int tokenLength;

    Tm4eScanner(
        IGrammar grammar,
        java.util.function.Function<List<String>, org.eclipse.jface.text.TextAttribute> scopeToAttr,
        Display display) {
      this.grammar = grammar;
      this.scopeToAttr = scopeToAttr;
      this.display = display;
    }

    @Override
    public void setRange(IDocument doc, int offset, int length) {
      this.document = doc;
      this.rangeOffset = offset;
      this.rangeLength = length;
      try {
        this.tokens = tokenize(doc, offset, length);
      } catch (Exception ignored) {
        this.tokens = Collections.emptyList();
      }
      this.index = 0;
      this.tokenOffset = 0;
      this.tokenLength = 0;
    }

    @Override
    public IToken nextToken() {
      if (tokens == null || index >= tokens.size()) {
        tokenOffset = tokenLength = 0;
        return Token.EOF;
      }
      ColoredToken t = tokens.get(index++);
      tokenOffset = t.offset;
      tokenLength = t.length;
      return new Token(t.attribute);
    }

    @Override
    public int getTokenOffset() {
      return tokenOffset;
    }

    @Override
    public int getTokenLength() {
      return tokenLength;
    }

    /** Tokenize document; runs synchronously on the UI thread. */
    private java.util.List<ColoredToken> tokenize(IDocument doc, int rangeOffset, int rangeLength) {
      String text;
      try {
        text = doc.get();
      } catch (Exception e) {
        return Collections.emptyList();
      }
      text = normalizeLineEndings(text);
      return tokenizeLines(text, rangeOffset, rangeLength);
    }

    private static String normalizeLineEndings(String text) {
      if (text == null || text.isEmpty()) return text;
      if (!text.contains("\r")) return text;
      return text.replace("\r\n", "\n").replace("\r", "\n");
    }

    /** Tokenize using a string with normalized line endings (\\n only). */
    private java.util.List<ColoredToken> tokenizeLines(
        String text, int rangeOffset, int rangeLength) {
      java.util.List<ColoredToken> result = new java.util.ArrayList<>();
      try {
        int len = text.length();
        String[] lines = text.split("\\n", -1);
        long lineStart = 0;
        IStateStack state = null;
        int rangeEnd = Math.min(rangeOffset + rangeLength, len);

        for (int lineIndex = 0; lineIndex < lines.length; lineIndex++) {
          if (lineIndex >= MAX_LINES_TO_TOKENIZE) break;

          String line = lines[lineIndex];
          int lineLen = line.length();
          if (lineLen > MAX_LINE_LENGTH) {
            lineStart += lineLen + 1L;
            continue;
          }

          long lineEnd = lineStart + lineLen;
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
            if (ContentEditorTm4eSupport.TRACE_SCOPES && tokenScopes != null) {
              String joined = String.join(" ", tokenScopes);
              System.err.println("[TM4E token] offset=" + o + " len=" + l + " | " + joined);
            }
            result.add(new ColoredToken(o, l, scopeToAttr.apply(tokenScopes)));
          }
          lineStart = lineEnd + 1;
        }
      } catch (Exception ignored) {
        // ignore
      }
      return result;
    }

    private static final class ColoredToken {
      final int offset;
      final int length;
      final org.eclipse.jface.text.TextAttribute attribute;

      ColoredToken(int offset, int length, org.eclipse.jface.text.TextAttribute attribute) {
        this.offset = offset;
        this.length = length;
        this.attribute = attribute;
      }
    }
  }
}
