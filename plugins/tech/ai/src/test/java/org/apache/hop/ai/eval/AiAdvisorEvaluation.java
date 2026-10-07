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
package org.apache.hop.ai.eval;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.ai.advisor.AiAdvisorLocations;
import org.apache.hop.ai.advisor.AiAdvisorResponse;
import org.apache.hop.ai.advisor.IAiAdvisor;
import org.apache.hop.ai.advisors.AiAdvisorInclusions;
import org.apache.hop.ai.advisors.pipeline.PipelineAiAdvisor;
import org.apache.hop.ai.advisors.workflow.WorkflowAiAdvisor;
import org.apache.hop.ai.config.HopAiConfigSingleton;
import org.apache.hop.ai.engine.AiAdvisorEngine;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.provider.AiProviderPlugin;
import org.apache.hop.ai.provider.AiProviderPluginType;
import org.apache.hop.ai.providers.AnthropicProvider;
import org.apache.hop.ai.providers.CustomOpenAiProvider;
import org.apache.hop.ai.providers.GeminiProvider;
import org.apache.hop.ai.providers.GrokProvider;
import org.apache.hop.ai.providers.HuggingFaceProvider;
import org.apache.hop.ai.providers.MistralProvider;
import org.apache.hop.ai.providers.OllamaProvider;
import org.apache.hop.ai.providers.OpenAiProvider;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorTurn;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.dummy.ActionDummy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Runs the pipeline and workflow AI advisors against a real model and checks the answers.
 *
 * <p>Not part of the normal build: the class name does not match the surefire patterns, and it only
 * runs when a provider is configured. Use it to check prompt changes against the models people use:
 *
 * <pre>
 * HOP_AI_EVAL_PROVIDER=ollama HOP_AI_EVAL_MODEL=llama3.2 \
 *   ./mvnw -pl plugins/tech/ai -am test -Dtest=AiAdvisorEvaluation \
 *   -Dsurefire.failIfNoSpecifiedTests=false
 * </pre>
 *
 * <p>Settings: {@code HOP_AI_EVAL_PROVIDER} (a provider plugin id: ollama, openai, anthropic,
 * mistral, gemini, grok, custom-openai, huggingface), {@code HOP_AI_EVAL_MODEL}, and optionally
 * {@code HOP_AI_EVAL_BASE_URL}, {@code HOP_AI_EVAL_API_KEY}, {@code HOP_AI_EVAL_CONTEXT_SIZE},
 * {@code HOP_AI_EVAL_MAX_OUTPUT_TOKENS} (small local models can write until the window is full
 * without it) and {@code HOP_AI_EVAL_TIMEOUT} in seconds (default 300). {@code HOP_AI_EVAL_CASES}
 * limits the run to a comma-separated list of case ids. {@code HOP_AI_EVAL_STRUCTURED=Y} switches
 * on Structured answers on the provider, to compare a model with and without it.
 *
 * <p>The report, with every answer, is written to {@code target/ai-eval/}, with a JSON file of the
 * result per case next to it. Answers vary between runs, so read the report rather than relying on
 * a single pass or fail. To see what a prompt change did, compare with a baseline: the results of
 * an earlier run, kept in {@code src/test/resources/org/apache/hop/ai/eval/baseline/} under the
 * same file name. The report lists the cases that passed in the baseline and fail now. {@code
 * HOP_AI_EVAL_BASELINE} points to another results file.
 */
@EnabledIfEnvironmentVariable(named = "HOP_AI_EVAL_PROVIDER", matches = ".+")
class AiAdvisorEvaluation {

  private static final Pattern INTERNALS =
      Pattern.compile(
          "\\bJSON\\b|provided context|prompt context|<\\/?(pipeline_|plugin_catalog|execution_log|question)",
          Pattern.CASE_INSENSITIVE);

  private static final Map<String, List<String>> LANGUAGE_WORDS =
      Map.of(
          "en", List.of(" the ", " and ", " is ", " this ", " to ", " with "),
          "es", List.of(" el ", " la ", " los ", " que ", " para ", " con ", " una "),
          "nl", List.of(" het ", " een ", " van ", " deze ", " naar ", " met ", " wordt "));

  /** An action with the settings an explanation needs, under any plugin id. */
  @Getter
  @Setter
  public static class EvalActionMeta extends ActionDummy {
    @HopMetadataProperty private String filename;
    @HopMetadataProperty private String connection;
    @HopMetadataProperty private String sql;

    public EvalActionMeta(String name) {
      super(name);
    }
  }

  /** A transform with the settings an explanation needs, under any plugin id. */
  @Getter
  @Setter
  public static class EvalTransformMeta extends DummyMeta {
    @HopMetadataProperty private String connection;
    @HopMetadataProperty private String sql;
    @HopMetadataProperty private String condition;
    @HopMetadataProperty private String filename;
  }

  @Test
  void evaluate() throws Exception {
    // The full environment, so the plugin catalog and the validator know the real transforms.
    HopEnvironment.init();
    registerProviders();
    HopAiConfigSingleton.getConfig().setAiEnabled(true);

    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    AiProvider provider = new AiProvider();
    provider.setName("eval");
    provider.setProviderType(System.getenv("HOP_AI_EVAL_PROVIDER"));
    provider.setModelName(Utils.isEmpty(env("HOP_AI_EVAL_MODEL")) ? "" : env("HOP_AI_EVAL_MODEL"));
    if (!Utils.isEmpty(env("HOP_AI_EVAL_BASE_URL"))) {
      provider.setBaseUrl(env("HOP_AI_EVAL_BASE_URL"));
    }
    provider.setApiKey(Utils.isEmpty(env("HOP_AI_EVAL_API_KEY")) ? "" : env("HOP_AI_EVAL_API_KEY"));
    provider.setContextSize(
        Utils.isEmpty(env("HOP_AI_EVAL_CONTEXT_SIZE")) ? "" : env("HOP_AI_EVAL_CONTEXT_SIZE"));
    provider.setTimeoutSeconds(
        Utils.isEmpty(env("HOP_AI_EVAL_TIMEOUT")) ? "300" : env("HOP_AI_EVAL_TIMEOUT"));
    provider.setMaxOutputTokens(
        Utils.isEmpty(env("HOP_AI_EVAL_MAX_OUTPUT_TOKENS"))
            ? ""
            : env("HOP_AI_EVAL_MAX_OUTPUT_TOKENS"));
    provider.setStructuredAnswers("Y".equalsIgnoreCase(env("HOP_AI_EVAL_STRUCTURED")));
    metadataProvider.getSerializer(AiProvider.class).save(provider);

    JsonNode root;
    try (InputStream in = getClass().getResourceAsStream("cases.json")) {
      root = new ObjectMapper().readTree(in);
    }
    List<String> only =
        Utils.isEmpty(env("HOP_AI_EVAL_CASES"))
            ? List.of()
            : List.of(env("HOP_AI_EVAL_CASES").split("\\s*,\\s*"));

    StringBuilder report = new StringBuilder();
    report
        .append("# AI advisor evaluation\n\n")
        .append("Provider: ")
        .append(provider.getPluginId())
        .append(", model: ")
        .append(provider.getModelName())
        .append(provider.isStructuredAnswers() ? ", structured answers" : "")
        .append("\n\n");
    Map<String, Boolean> results = new LinkedHashMap<>();
    int passed = 0;
    int total = 0;
    for (JsonNode testCase : root.path("cases")) {
      String id = testCase.path("id").asText();
      if (!only.isEmpty() && !only.contains(id)) {
        continue;
      }
      total++;
      List<String> failures = new ArrayList<>();
      String answer = "";
      AiAdvisorResponse response = null;
      try {
        boolean workflowCase = testCase.has("workflow");
        advisor = workflowCase ? new WorkflowAiAdvisor() : new PipelineAiAdvisor();
        artifact =
            workflowCase
                ? buildWorkflow(root.path("workflows").path(testCase.path("workflow").asText()))
                : buildPipeline(root.path("pipelines").path(testCase.path("pipeline").asText()));
        AiAdvisorSession session = newSession(advisor, artifact);
        for (JsonNode turnNode : testCase.path("turns")) {
          response = ask(session, advisor, turnNode, metadataProvider);
        }
        answer = response == null ? "" : response.getMarkdownAdvice();
        check(testCase.path("expect"), response, answer, failures);
      } catch (Exception e) {
        failures.add("error: " + e.getMessage());
      }
      if (failures.isEmpty()) {
        passed++;
      }
      results.put(id, failures.isEmpty());
      report
          .append("## ")
          .append(id)
          .append(failures.isEmpty() ? " — pass" : " — FAIL")
          .append("\n\n");
      for (String failure : failures) {
        report.append("- ").append(failure).append('\n');
      }
      if (response != null) {
        report
            .append("- proposals: ")
            .append(response.getProposals() == null ? 0 : response.getProposals().size())
            .append(", tokens in/out: ")
            .append(response.getInputTokenCount())
            .append('/')
            .append(response.getOutputTokenCount())
            .append('\n');
        if (response.getProposals() != null && !response.getProposals().isEmpty()) {
          List<String> types = new ArrayList<>();
          response.getProposals().forEach(proposal -> types.add(proposal.getType()));
          report.append("- proposal types: ").append(types).append('\n');
        }
      }
      report.append("\n```\n").append(answer).append("\n```\n\n");
    }
    report.append("Passed ").append(passed).append(" of ").append(total).append('\n');

    Path dir = Path.of("target", "ai-eval");
    Files.createDirectories(dir);
    String model = provider.getModelName().replaceAll("[^A-Za-z0-9._-]", "_");
    String name =
        provider.getPluginId()
            + "-"
            + model
            + (provider.isStructuredAnswers() ? "-structured" : "");
    appendBaselineComparison(report, name, results);
    Path file = dir.resolve(name + ".md");
    Files.writeString(file, report.toString(), StandardCharsets.UTF_8);
    new ObjectMapper()
        .writerWithDefaultPrettyPrinter()
        .writeValue(dir.resolve(name + ".json").toFile(), results);
    System.out.println("AI advisor evaluation report: " + file.toAbsolutePath());

    assertTrue(passed == total, "Passed " + passed + " of " + total + ", see " + file);
  }

  /**
   * The cases that passed in the baseline and fail now. The baseline is the results file of an
   * earlier run of the same provider and model.
   */
  private static void appendBaselineComparison(
      StringBuilder report, String name, Map<String, Boolean> results) throws Exception {
    JsonNode baseline = null;
    String path = env("HOP_AI_EVAL_BASELINE");
    if (!Utils.isEmpty(path)) {
      baseline = new ObjectMapper().readTree(Path.of(path).toFile());
    } else {
      try (InputStream in =
          AiAdvisorEvaluation.class.getResourceAsStream("baseline/" + name + ".json")) {
        if (in != null) {
          baseline = new ObjectMapper().readTree(in);
        }
      }
    }
    if (baseline == null) {
      report.append("\nNo baseline for ").append(name).append(".\n");
      return;
    }
    List<String> regressions = new ArrayList<>();
    List<String> fixed = new ArrayList<>();
    for (Map.Entry<String, Boolean> result : results.entrySet()) {
      JsonNode before = baseline.get(result.getKey());
      if (before == null) {
        continue;
      }
      if (before.asBoolean() && !result.getValue()) {
        regressions.add(result.getKey());
      } else if (!before.asBoolean() && result.getValue()) {
        fixed.add(result.getKey());
      }
    }
    report
        .append("\nCompared with the baseline: ")
        .append(regressions.isEmpty() ? "no regressions" : "regressions: " + regressions)
        .append(fixed.isEmpty() ? "" : "; now passing: " + fixed)
        .append(".\n");
  }

  private static AiAdvisorResponse ask(
      AiAdvisorSession session,
      IAiAdvisor advisor,
      JsonNode turnNode,
      MemoryMetadataProvider metadataProvider)
      throws Exception {
    String scenario =
        turnNode
            .path("scenario")
            .asText(advisor instanceof WorkflowAiAdvisor ? "workflow-general" : "pipeline-general");
    session.setScenarioId(scenario);
    String log = turnNode.path("log").asText(null);
    session.getInclusions().put(AiAdvisorInclusions.LOGS, log != null);
    AiAdvisorTurn turn = new AiAdvisorTurn();
    turn.setUserPrompt(turnNode.path("question").asText());
    session.addTurn(turn);
    AiAdvisorResponse response =
        AiAdvisorEngine.advise(session, advisor, new Variables(), metadataProvider, log);
    turn.setAssistantAdvice(response.getMarkdownAdvice());
    return response;
  }

  /** The advisor and the pipeline or workflow of the case being checked. */
  private IAiAdvisor advisor;

  private Object artifact;

  private void check(
      JsonNode expect, AiAdvisorResponse response, String answer, List<String> failures) {
    if (response == null) {
      failures.add("no response");
      return;
    }
    int proposals = response.getProposals() == null ? 0 : response.getProposals().size();
    switch (expect.path("proposals").asText("")) {
      case "none" -> {
        if (proposals > 0 || response.isProposalBlockPresent()) {
          failures.add("proposals were made for a question that asked for none");
        }
      }
      case "some" -> {
        if (proposals == 0) {
          failures.add(
              "no proposals"
                  + (response.getProposalParseError() == null
                      ? ""
                      : " (" + response.getProposalParseError() + ")"));
        }
      }
      default -> {
        // Not checked.
      }
    }
    if (expect.has("types") && response.getProposals() != null) {
      // The kinds of change the question asks for; anything else is the wrong change.
      List<String> allowed = new ArrayList<>();
      expect.path("types").forEach(type -> allowed.add(type.asText()));
      for (org.apache.hop.ai.advisor.AiProposal proposal : response.getProposals()) {
        if (!allowed.contains(proposal.getType())) {
          failures.add("unexpected proposal type " + proposal.getType() + ", expected " + allowed);
        }
      }
    }
    // How many proposals of a type the question needs at least, such as two deletes.
    expect
        .path("requiredTypes")
        .fields()
        .forEachRemaining(
            required -> {
              long count =
                  response.getProposals() == null
                      ? 0
                      : response.getProposals().stream()
                          .filter(proposal -> required.getKey().equals(proposal.getType()))
                          .count();
              if (count < required.getValue().asInt()) {
                failures.add(
                    count
                        + " "
                        + required.getKey()
                        + " proposals, expected at least "
                        + required.getValue().asInt());
              }
            });
    if (expect.path("validTypes").asBoolean(false) && response.getProposals() != null) {
      // What the user sees in the review: the proposals after the advisor's own clean-up.
      org.apache.hop.ai.advisor.AiAdvisorRequest request =
          new org.apache.hop.ai.advisor.AiAdvisorRequest();
      request.setArtifact(artifact);
      List<org.apache.hop.ai.advisor.AiProposalValidation> validations =
          advisor.validateProposals(request, response.getProposals());
      for (int i = 0; i < validations.size(); i++) {
        if (validations.get(i).isBlocked()) {
          failures.add(
              "blocked: "
                  + response.getProposals().get(i).getType()
                  + " ("
                  + validations.get(i).getReason()
                  + ")");
        }
      }
    }
    if (INTERNALS.matcher(answer).find()) {
      failures.add("the answer mentions prompt internals");
    }
    String language = expect.path("language").asText("");
    // An answer that is only a proposal block has no prose to tell the language from.
    if (!language.isEmpty() && !answer.isBlank() && !detectLanguage(answer).equals(language)) {
      failures.add("answered in " + detectLanguage(answer) + ", expected " + language);
    }
    String lower = answer.toLowerCase(Locale.ROOT);
    for (JsonNode mention : expect.path("mentions")) {
      if (!lower.contains(mention.asText().toLowerCase(Locale.ROOT))) {
        failures.add("does not mention " + mention.asText());
      }
    }
  }

  /** The language whose common words occur most. Crude, but enough to tell en, es and nl apart. */
  static String detectLanguage(String text) {
    String padded = " " + text.toLowerCase(Locale.ROOT).replaceAll("[^\\p{L}]+", " ") + " ";
    String best = "unknown";
    int bestCount = 0;
    for (Map.Entry<String, List<String>> entry : LANGUAGE_WORDS.entrySet()) {
      int count = 0;
      for (String word : entry.getValue()) {
        int from = 0;
        while ((from = padded.indexOf(word, from)) >= 0) {
          count++;
          from += word.length() - 1;
        }
      }
      if (count > bestCount) {
        bestCount = count;
        best = entry.getKey();
      }
    }
    return best;
  }

  private static AiAdvisorSession newSession(IAiAdvisor advisor, Object artifact) {
    boolean workflow = advisor instanceof WorkflowAiAdvisor;
    AiAdvisorSession session = new AiAdvisorSession();
    session.setAdvisorPluginId(workflow ? WorkflowAiAdvisor.ID : PipelineAiAdvisor.ID);
    session.setLocation(
        workflow ? AiAdvisorLocations.WORKFLOW_GRAPH : AiAdvisorLocations.PIPELINE_GRAPH);
    session.setArtifact(artifact);
    session.setArtifactName("eval");
    session.setArtifactKind(workflow ? "workflow" : "pipeline");
    session.setProviderName("eval");
    session.getInclusions().put(AiAdvisorInclusions.SETTINGS, true);
    session.getInclusions().put(AiAdvisorInclusions.CATALOG, true);
    return session;
  }

  private static PipelineMeta buildPipeline(JsonNode spec) {
    PipelineMeta pipeline = new PipelineMeta();
    pipeline.setName("eval");
    Map<String, TransformMeta> byName = new HashMap<>();
    int x = 100;
    for (JsonNode node : spec.path("transforms")) {
      EvalTransformMeta meta = new EvalTransformMeta();
      meta.setConnection(node.path("connection").asText(null));
      meta.setSql(node.path("sql").asText(null));
      meta.setCondition(node.path("condition").asText(null));
      meta.setFilename(node.path("filename").asText(null));
      TransformMeta transform =
          new TransformMeta(node.path("pluginId").asText(), node.path("name").asText(), meta);
      transform.setLocation(x, 100);
      x += 150;
      pipeline.addTransform(transform);
      byName.put(transform.getName(), transform);
    }
    for (JsonNode hop : spec.path("hops")) {
      pipeline.addPipelineHop(
          new PipelineHopMeta(byName.get(hop.get(0).asText()), byName.get(hop.get(1).asText())));
    }
    return pipeline;
  }

  /**
   * A workflow from the cases file: actions with a plugin id and settings, and hops as {@code
   * [from, to, "success" | "failure" | "unconditional"]}.
   */
  private static WorkflowMeta buildWorkflow(JsonNode spec) {
    WorkflowMeta workflow = new WorkflowMeta();
    workflow.setName("eval");
    Map<String, ActionMeta> byName = new HashMap<>();
    int x = 100;
    for (JsonNode node : spec.path("actions")) {
      EvalActionMeta action = new EvalActionMeta(node.path("name").asText());
      action.setPluginId(node.path("pluginId").asText());
      action.setFilename(node.path("filename").asText(null));
      action.setConnection(node.path("connection").asText(null));
      action.setSql(node.path("sql").asText(null));
      ActionMeta actionMeta = new ActionMeta(action);
      actionMeta.setLocation(x, 100);
      x += 150;
      workflow.addAction(actionMeta);
      byName.put(actionMeta.getName(), actionMeta);
    }
    for (JsonNode hop : spec.path("hops")) {
      WorkflowHopMeta hopMeta =
          new WorkflowHopMeta(byName.get(hop.get(0).asText()), byName.get(hop.get(1).asText()));
      switch (hop.path(2).asText("success")) {
        case "unconditional" -> hopMeta.setUnconditional();
        case "failure" -> {
          hopMeta.setUnconditional(false);
          hopMeta.setEvaluation(false);
        }
        default -> {
          hopMeta.setUnconditional(false);
          hopMeta.setEvaluation(true);
        }
      }
      workflow.addWorkflowHop(hopMeta);
    }
    return workflow;
  }

  private static void registerProviders() throws Exception {
    PluginRegistry.addPluginType(AiProviderPluginType.getInstance());
    PluginRegistry registry = PluginRegistry.getInstance();
    for (Class<?> provider :
        List.of(
            OllamaProvider.class,
            OpenAiProvider.class,
            AnthropicProvider.class,
            MistralProvider.class,
            GeminiProvider.class,
            GrokProvider.class,
            CustomOpenAiProvider.class,
            HuggingFaceProvider.class)) {
      registry.registerPluginClass(
          provider.getName(), AiProviderPluginType.class, AiProviderPlugin.class);
    }
  }

  private static String env(String name) {
    return System.getenv(name);
  }
}
