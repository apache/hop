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

package org.apache.hop.ui.hopgui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.eclipse.tm4e.core.grammar.IGrammar;
import org.eclipse.tm4e.core.grammar.IStateStack;
import org.eclipse.tm4e.core.grammar.ITokenizeLineResult;
import org.eclipse.tm4e.core.registry.IGrammarSource;
import org.eclipse.tm4e.core.registry.IRegistryOptions;
import org.eclipse.tm4e.core.registry.Registry;
import org.junit.jupiter.api.Test;

class ContentEditorTm4eSupportTest {

  /**
   * Loads a vendored grammar the same way ContentEditorTm4eSupport does, without an SWT Display.
   */
  private static IGrammar loadGrammar(String scopeName, String fileName) throws Exception {
    Map<String, String> files = Map.of(scopeName, fileName);
    Registry registry =
        new Registry(
            new IRegistryOptions() {
              @Override
              public IGrammarSource getGrammarSource(String scope) {
                String file = files.get(scope);
                if (file == null) {
                  return null;
                }
                return new IGrammarSource() {
                  @Override
                  public URI getURI() {
                    return URI.create("hop://grammar/" + scope);
                  }

                  @Override
                  public Reader getReader() throws IOException {
                    InputStream in =
                        ContentEditorTm4eSupport.class.getResourceAsStream("grammars/" + file);
                    if (in == null) {
                      throw new IOException("Grammar resource not found: grammars/" + file);
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
              public Collection<String> getInjections(String scope) {
                return List.of();
              }
            });
    return registry.loadGrammar(scopeName);
  }

  @Test
  void scopeForLanguage_python_returnsSourcePython() {
    assertEquals("source.python", ContentEditorTm4eSupport.scopeForLanguage("python"));
  }

  @Test
  void scopeForLanguage_py_returnsSourcePython() {
    assertEquals("source.python", ContentEditorTm4eSupport.scopeForLanguage("py"));
  }

  @Test
  void pythonGrammarResourceIsOnClasspath() throws Exception {
    try (InputStream in =
        ContentEditorTm4eSupport.class.getResourceAsStream("grammars/python.json")) {
      assertNotNull(in, "grammars/python.json should be on the classpath");
      assertTrue(in.read() >= 0, "python.json should not be empty");
    }
  }

  @Test
  void scopeForLanguage_shellAliases_returnSourceShell() {
    assertEquals("source.shell", ContentEditorTm4eSupport.scopeForLanguage("shell"));
    assertEquals("source.shell", ContentEditorTm4eSupport.scopeForLanguage("bash"));
    assertEquals("source.shell", ContentEditorTm4eSupport.scopeForLanguage("sh"));
  }

  @Test
  void scopeForLanguage_batchAliases_returnSourceBatchfile() {
    assertEquals("source.batchfile", ContentEditorTm4eSupport.scopeForLanguage("bat"));
    assertEquals("source.batchfile", ContentEditorTm4eSupport.scopeForLanguage("cmd"));
    assertEquals("source.batchfile", ContentEditorTm4eSupport.scopeForLanguage("batch"));
  }

  @Test
  void shellGrammarResourceIsOnClasspath() throws Exception {
    try (InputStream in =
        ContentEditorTm4eSupport.class.getResourceAsStream("grammars/shell.json")) {
      assertNotNull(in, "grammars/shell.json should be on the classpath");
      assertTrue(in.read() >= 0, "shell.json should not be empty");
    }
  }

  @Test
  void batGrammarResourceIsOnClasspath() throws Exception {
    try (InputStream in = ContentEditorTm4eSupport.class.getResourceAsStream("grammars/bat.json")) {
      assertNotNull(in, "grammars/bat.json should be on the classpath");
      assertTrue(in.read() >= 0, "bat.json should not be empty");
    }
  }

  @Test
  void shellGrammarLoadsAndTokenizes() throws Exception {
    IGrammar grammar = loadGrammar("source.shell", "shell.json");
    assertNotNull(grammar, "TM4E should load the shell grammar");
    ITokenizeLineResult<org.eclipse.tm4e.core.grammar.IToken[]> result =
        grammar.tokenizeLine("# comment\necho \"$HOME\"", null, null);
    assertNotNull(result);
    assertTrue(result.getTokens().length > 0, "shell line should produce tokens");
  }

  @Test
  void batGrammarLoadsAndTokenizes() throws Exception {
    IGrammar grammar = loadGrammar("source.batchfile", "bat.json");
    assertNotNull(grammar, "TM4E should load the batch grammar");
    ITokenizeLineResult<org.eclipse.tm4e.core.grammar.IToken[]> result =
        grammar.tokenizeLine("REM comment\nECHO %PATH%", null, null);
    assertNotNull(result);
    assertTrue(result.getTokens().length > 0, "batch line should produce tokens");
  }

  @Test
  void scopeForLanguage_markdownAliases_returnTextHtmlMarkdown() {
    assertEquals("text.html.markdown", ContentEditorTm4eSupport.scopeForLanguage("markdown"));
    assertEquals("text.html.markdown", ContentEditorTm4eSupport.scopeForLanguage("md"));
  }

  @Test
  void markdownGrammarResourceIsOnClasspath() throws Exception {
    try (InputStream in =
        ContentEditorTm4eSupport.class.getResourceAsStream("grammars/markdown.json")) {
      assertNotNull(in, "grammars/markdown.json should be on the classpath");
      assertTrue(in.read() >= 0, "markdown.json should not be empty");
    }
  }

  @Test
  void markdownGrammarScopesBlockElements() throws Exception {
    IGrammar grammar = loadGrammar("text.html.markdown", "markdown.json");
    assertNotNull(grammar, "TM4E should load the markdown grammar");

    assertTrue(
        scopesOf(grammar, "# Title").contains("markup.heading.markdown"),
        "a heading line should be scoped as a heading");
    assertTrue(
        scopesOf(grammar, "> quoted").contains("markup.quote.markdown"),
        "a blockquote line should be scoped as a quote");
    assertTrue(
        scopesOf(grammar, "- item").contains("punctuation.definition.list.begin.markdown"),
        "a list bullet should be scoped as a list");
    assertTrue(
        scopesOf(grammar, "---").contains("meta.separator.markdown"),
        "a thematic break should be scoped as a separator");
    assertTrue(
        scopesOf(grammar, "| --- | :-: |").contains("punctuation.definition.table.markdown"),
        "a table delimiter row should be scoped as a table");
  }

  @Test
  void markdownGrammarScopesInlineElements() throws Exception {
    IGrammar grammar = loadGrammar("text.html.markdown", "markdown.json");

    assertTrue(
        scopesOf(grammar, "some **bold** text").contains("markup.bold.markdown"),
        "**...** should be scoped as bold");
    assertTrue(
        scopesOf(grammar, "some *italic* text").contains("markup.italic.markdown"),
        "*...* should be scoped as italic");
    assertTrue(
        scopesOf(grammar, "snake_case_word").stream().noneMatch(s -> s.contains("markup.italic")),
        "underscores inside a word should not turn into italics");
    assertTrue(
        scopesOf(grammar, "an `inline` snippet").contains("markup.inline.raw.string.markdown"),
        "`...` should be scoped as inline raw");
    assertTrue(
        scopesOf(grammar, "see [Hop](https://hop.apache.org)")
            .contains("markup.underline.link.markdown"),
        "a link target should be scoped as a link");
  }

  @Test
  void markdownGrammarKeepsFencedCodeBlockStateAcrossLines() throws Exception {
    IGrammar grammar = loadGrammar("text.html.markdown", "markdown.json");

    IStateStack state = null;
    List<String> insideScopes = List.of();
    List<String> afterScopes = List.of();
    for (String line : List.of("```sql", "SELECT 1", "```", "# after")) {
      ITokenizeLineResult<org.eclipse.tm4e.core.grammar.IToken[]> result =
          grammar.tokenizeLine(line, state, null);
      state = result.getRuleStack();
      if ("SELECT 1".equals(line)) {
        insideScopes = scopesOf(result);
      } else if ("# after".equals(line)) {
        afterScopes = scopesOf(result);
      }
    }

    assertTrue(
        insideScopes.contains("markup.fenced_code.block.markdown"),
        "lines between fences should stay inside the fenced code block");
    assertTrue(
        afterScopes.contains("markup.heading.markdown"),
        "the closing fence should end the block so following markup is scoped again");
  }

  /** All scopes produced for a single line, without carrying over any state. */
  private static List<String> scopesOf(IGrammar grammar, String line) {
    return scopesOf(grammar.tokenizeLine(line, null, null));
  }

  private static List<String> scopesOf(
      ITokenizeLineResult<org.eclipse.tm4e.core.grammar.IToken[]> result) {
    List<String> scopes = new ArrayList<>();
    for (org.eclipse.tm4e.core.grammar.IToken token : result.getTokens()) {
      scopes.addAll(token.getScopes());
    }
    return scopes;
  }

  @Test
  void scopeForLanguage_yaml_returnsSourceYaml() {
    assertEquals("source.yaml", ContentEditorTm4eSupport.scopeForLanguage("yaml"));
  }

  @Test
  void scopeForLanguage_yml_returnsSourceYaml() {
    assertEquals("source.yaml", ContentEditorTm4eSupport.scopeForLanguage("yml"));
  }

  @Test
  void yamlGrammarResourceIsOnClasspath() throws Exception {
    try (InputStream in =
        ContentEditorTm4eSupport.class.getResourceAsStream("grammars/yaml.json")) {
      assertNotNull(in, "grammars/yaml.json should be on the classpath");
      assertTrue(in.read() >= 0, "yaml.json should not be empty");
    }
  }
}
