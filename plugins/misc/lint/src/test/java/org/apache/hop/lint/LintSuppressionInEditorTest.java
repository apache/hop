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

package org.apache.hop.lint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A suppression has to hold where the person who wrote it is looking.
 *
 * <p>The editor lints an open pipeline through {@code lintPipelineLikeVerify}, a different path
 * from the command line's {@code lintFile}. When only the latter applied the project's {@code
 * suppress:} configuration, a team could accept a finding, watch it disappear from their build, and
 * still have the red badge sitting on the canvas — which is exactly where they wanted it gone.
 */
public class LintSuppressionInEditorTest {

  private final IVariables variables = Variables.getADefaultVariableSpace();

  @TempDir private Path projectDir;

  /**
   * Two transforms with no hop between them. Hop's own check reports each as unused, with no
   * transform plugin needed, so the finding under test is a native remark rather than a lint rule.
   */
  private PipelineMeta pipelineWithUnusedTransforms(String fileName) {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName("template");
    pipelineMeta.setFilename(fileName);

    TransformMeta source = new TransformMeta();
    source.setName("Fonte Sql");
    source.setTransformPluginId("TableInput");
    pipelineMeta.addTransform(source);

    TransformMeta target = new TransformMeta();
    target.setName("Salva S3");
    target.setTransformPluginId("TextFileOutput");
    pipelineMeta.addTransform(target);

    return pipelineMeta;
  }

  private List<LintResult> lintAsEditor() throws Exception {
    String fileName = projectDir.resolve("template.hpl").toString();
    return new HopLinter()
        .lintPipelineLikeVerify(pipelineWithUnusedTransforms(fileName), fileName, null, variables);
  }

  private void writeProjectConfig(String yaml) throws Exception {
    Files.writeString(projectDir.resolve("hop-lint.yml"), yaml, StandardCharsets.UTF_8);
  }

  @Test
  public void nativeFindingsReachTheEditorWhenNothingIsSuppressed() throws Exception {
    List<LintResult> results = lintAsEditor();

    assertTrue(
        results.stream().anyMatch(r -> "Fonte Sql".equals(sourceName(r))),
        "expected a native finding on Fonte Sql, got: " + results);
    assertTrue(
        results.stream().anyMatch(r -> "Salva S3".equals(sourceName(r))),
        "expected a native finding on Salva S3, got: " + results);
  }

  @Test
  public void suppressedTransformIsSilentInTheEditor() throws Exception {
    writeProjectConfig(
        """
        suppress:
          - rule: HOP-CHECK
            source: "Fonte Sql"
            reason: "Fields are injected at runtime, nothing to check at design time"
        """);

    List<LintResult> results = lintAsEditor();

    assertEquals(
        0,
        results.stream().filter(r -> "Fonte Sql".equals(sourceName(r))).count(),
        "the accepted finding should be gone from the editor: " + results);
    assertTrue(
        results.stream().anyMatch(r -> "Salva S3".equals(sourceName(r))),
        "a suppression naming one transform must not silence the other: " + results);
  }

  /**
   * Path patterns are written against the project root, the folder holding hop-lint.yml, and a
   * suppression narrowed to one rule leaves the other findings on that file alone.
   */
  @Test
  public void suppressionPathIsRootedAtTheProjectConfig() throws Exception {
    List<LintResult> before = lintAsEditor();
    assertTrue(countOfRule(before, "HOP-CHECK") > 0, "no native findings to suppress: " + before);

    writeProjectConfig(
        """
        suppress:
          - rule: HOP-CHECK
            path: "template.hpl"
            reason: "This whole template is driven by metadata injection"
        """);

    List<LintResult> after = lintAsEditor();

    assertEquals(0, countOfRule(after, "HOP-CHECK"), "native findings should be gone: " + after);
    assertEquals(
        before.size() - countOfRule(before, "HOP-CHECK"),
        after.size(),
        "only the named rule should have been silenced: " + after);
  }

  private long countOfRule(List<LintResult> results, String ruleId) {
    return results.stream().filter(r -> ruleId.equals(r.getRuleId())).count();
  }

  /**
   * An exclusion has to hold for a file the editor opens, not only for the project-wide walk that
   * discovers files. It did not: the file was excluded from the run that lists what to lint, and
   * then linted anyway the moment somebody opened it, so the badges came back on every reopen.
   */
  @Test
  public void excludedFilesAreNotLintedInTheEditor() throws Exception {
    assertTrue(lintAsEditor().size() > 0, "nothing to exclude");

    writeProjectConfig("""
        exclude:
          - "template.hpl"
        """);

    assertEquals(List.of(), lintAsEditor());
  }

  /** A rule of "*" accepts whatever is reported on the element, including rules added later. */
  @Test
  public void wildcardSuppressionCoversEveryRuleOnTheElement() throws Exception {
    writeProjectConfig(
        """
        suppress:
          - rule: "*"
            source: "Fonte Sql"
            reason: "Everything about this transform arrives at runtime"
        """);

    List<LintResult> results = lintAsEditor();

    assertEquals(
        0,
        results.stream().filter(r -> "Fonte Sql".equals(sourceName(r))).count(),
        "every finding on the element should be gone: " + results);
    assertTrue(
        results.stream().anyMatch(r -> "Salva S3".equals(sourceName(r))),
        "and only on that element: " + results);
  }

  /** Without a path or a source, "*" is the linter switched off under another name. */
  @Test
  public void bareWildcardSuppressionIsRefused() throws Exception {
    writeProjectConfig(
        """
        suppress:
          - rule: "*"
            reason: "silence everything"
        """);

    assertTrue(lintAsEditor().size() > 0, "the bare wildcard should have been ignored");
  }

  private String sourceName(LintResult result) {
    return result.getSource() == null ? null : result.getSource().getName();
  }
}
