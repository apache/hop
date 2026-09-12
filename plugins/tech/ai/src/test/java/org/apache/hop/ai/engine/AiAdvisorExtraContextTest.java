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

package org.apache.hop.ai.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.ai.advisor.AiAdvisorPrompt;
import org.apache.hop.ai.advisors.pipeline.PipelineAiAdvisor;
import org.apache.hop.ai.advisors.workflow.WorkflowAiAdvisor;
import org.apache.hop.ai.config.HopAiConfig;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.Test;

class AiAdvisorExtraContextTest {

  @Test
  void notesAreAppendedToSystemPrompt() {
    HopAiConfig config = new HopAiConfig();
    config.setExtraContext("Metadata names are case-sensitive.");
    config.setExtraContextFiles("");
    AiAdvisorPrompt prompt = new AiAdvisorPrompt("system", "user");
    AiAdvisorExtraContext.apply(prompt, config, new Variables());
    assertTrue(prompt.getSystemPrompt().contains("system"));
    assertTrue(prompt.getSystemPrompt().contains("Metadata names are case-sensitive."));
    assertEquals("user", prompt.getUserPrompt());
  }

  @Test
  void missingContextFileIsSkipped() {
    HopAiConfig config = new HopAiConfig();
    config.setExtraContext("");
    config.setExtraContextFiles("${PROJECT_HOME}/AGENTS.md");
    Variables variables = new Variables();
    variables.setVariable("PROJECT_HOME", "ram:///missing-ai-context-" + UUID.randomUUID());
    assertTrue(AiAdvisorExtraContext.buildSystemAppendix(config, variables).isEmpty());
    assertTrue(AiAdvisorExtraContext.sharingPhrases(config, variables, "extra notes").isEmpty());
  }

  @Test
  void existingAgentsMdIsIncluded() throws Exception {
    String home = "ram:///ai-context-" + UUID.randomUUID();
    writeRamFile(home + "/AGENTS.md", "Run configuration names are case-sensitive.");
    HopAiConfig config = new HopAiConfig();
    config.setExtraContext("");
    config.setExtraContextFiles("${PROJECT_HOME}/AGENTS.md");
    Variables variables = new Variables();
    variables.setVariable("PROJECT_HOME", home);

    String extra = AiAdvisorExtraContext.buildSystemAppendix(config, variables);
    assertTrue(extra.contains("Run configuration names are case-sensitive."));
    assertTrue(extra.contains("AGENTS.md"));
    assertEquals(
        List.of("AGENTS.md"),
        AiAdvisorExtraContext.sharingPhrases(config, variables, "extra notes"));
  }

  @Test
  void commentAndUnresolvedLinesAreSkipped() {
    HopAiConfig config = new HopAiConfig();
    config.setExtraContextFiles("# ignore\n${UNSET}/AGENTS.md\n");
    assertTrue(AiAdvisorExtraContext.loadFiles(config, new Variables()).isEmpty());
  }

  @Test
  void emptyFilesFieldOptsOutOfDefaultAgentsMd() {
    HopAiConfig config = new HopAiConfig();
    config.setExtraContextFiles("");
    assertEquals(HopAiConfig.DEFAULT_EXTRA_CONTEXT_FILES, new HopAiConfig().getExtraContextFiles());
    assertTrue(AiAdvisorExtraContext.loadFiles(config, new Variables()).isEmpty());
  }

  @Test
  void notesResolveVariables() {
    HopAiConfig config = new HopAiConfig();
    config.setExtraContext("Project ${PROJECT_NAME} uses case-sensitive metadata names.");
    config.setExtraContextFiles("");
    Variables variables = new Variables();
    variables.setVariable("PROJECT_NAME", "sales");
    String extra = AiAdvisorExtraContext.buildSystemAppendix(config, variables);
    assertTrue(extra.contains("Project sales uses case-sensitive metadata names."));
  }

  @Test
  void secretsInNotesAreRedacted() {
    HopAiConfig config = new HopAiConfig();
    config.setExtraContext("apiKey=super-secret");
    config.setExtraContextFiles("");
    String extra = AiAdvisorExtraContext.buildSystemAppendix(config, new Variables());
    assertFalse(extra.contains("super-secret"));
    assertTrue(extra.contains("***"));
  }

  @Test
  void sharingPhrasesIncludeNotesAndFile() throws Exception {
    String home = "ram:///ai-context-" + UUID.randomUUID();
    writeRamFile(home + "/AGENTS.md", "case-sensitive metadata");
    HopAiConfig config = new HopAiConfig();
    config.setExtraContext("Prefer local run configs.");
    config.setExtraContextFiles("${PROJECT_HOME}/AGENTS.md");
    Variables variables = new Variables();
    variables.setVariable("PROJECT_HOME", home);
    assertEquals(
        List.of("extra notes", "AGENTS.md"),
        AiAdvisorExtraContext.sharingPhrases(config, variables, "extra notes"));
  }

  @Test
  void bundledPluginContextIsOnClasspath() {
    List<AiAdvisorExtraContext.LoadedFile> pipeline =
        AiAdvisorExtraContext.loadPluginContext(new PipelineAiAdvisor());
    assertTrue(
        pipeline.stream().anyMatch(file -> "ai-context.md".equals(file.label)),
        pipeline.toString());
    assertTrue(
        pipeline.stream().anyMatch(file -> "pipeline-advisor.md".equals(file.label)),
        pipeline.toString());
    assertTrue(
        pipeline.stream()
            .anyMatch(file -> file.text.contains("metadata names are case-sensitive")));

    List<AiAdvisorExtraContext.LoadedFile> workflow =
        AiAdvisorExtraContext.loadPluginContext(new WorkflowAiAdvisor());
    assertTrue(workflow.stream().anyMatch(file -> "workflow-advisor.md".equals(file.label)));
    assertTrue(
        workflow.stream()
            .anyMatch(file -> file.text.contains("local") && file.text.contains("Local")));
  }

  @Test
  void pluginFolderOverridesClasspath() throws Exception {
    String dir = "ram:///ai-plugin-" + UUID.randomUUID();
    writeRamFile(dir + "/ai-context.md", "Folder overlay wins.");
    FileObject pluginDir = HopVfs.getFileObject(dir);
    List<AiAdvisorExtraContext.LoadedFile> loaded =
        AiAdvisorExtraContext.loadPluginContext(
            PipelineAiAdvisor.class.getClassLoader(), "pipeline-advisor", pluginDir);
    assertEquals("Folder overlay wins.", loaded.get(0).text.trim());
    assertEquals("ai-context.md", loaded.get(0).label);
    assertTrue(loaded.stream().anyMatch(file -> "pipeline-advisor.md".equals(file.label)));
  }

  @Test
  void standingContextFromAdvisorIsAppended() {
    HopAiConfig config = new HopAiConfig();
    config.setExtraContextFiles("");
    PipelineAiAdvisor advisor =
        new PipelineAiAdvisor() {
          @Override
          public String getStandingContext() {
            return "Prefer Dummy over Filter Rows for this project.";
          }
        };
    String extra = AiAdvisorExtraContext.buildSystemAppendix(advisor, config, new Variables());
    assertTrue(extra.contains("Prefer Dummy over Filter Rows for this project."));
    assertTrue(extra.contains("ai-context.md"));
  }

  private static void writeRamFile(String path, String content) throws Exception {
    FileObject file = HopVfs.getFileObject(path);
    FileObject parent = file.getParent();
    if (parent != null && !parent.exists()) {
      parent.createFolder();
    }
    try (OutputStream out = HopVfs.getOutputStream(file, false)) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
  }
}
