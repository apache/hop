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

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.UserMessage;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.ai.advisor.AiAdvisorPrompt;
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.ai.advisor.AiAdvisorResponse;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.advisor.AiProposalValidation;
import org.apache.hop.ai.advisor.IAiAdvisor;
import org.apache.hop.ai.advisors.pipeline.PipelineAiAdvisor;
import org.apache.hop.ai.advisors.pipeline.PipelineAiProposalApplier;
import org.apache.hop.ai.advisors.workflow.WorkflowAiAdvisor;
import org.apache.hop.ai.advisors.workflow.WorkflowAiProposalApplier;
import org.apache.hop.ai.config.HopAiConfig;
import org.apache.hop.ai.config.HopAiConfigSingleton;
import org.apache.hop.ai.metadata.AiProvider;
import org.apache.hop.ai.session.AiAdvisorSession;
import org.apache.hop.ai.session.AiAdvisorTurn;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.workflow.WorkflowMeta;

/** Runs one advisory turn: advisor prompt → {@link AiChatFactory} → parsed response. */
public final class AiAdvisorEngine {

  private static final Class<?> PKG = AiAdvisorEngine.class;

  private AiAdvisorEngine() {}

  public static AiAdvisorResponse advise(
      AiAdvisorSession session,
      IAiAdvisor advisor,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    return advise(session, advisor, variables, metadataProvider, null);
  }

  /**
   * Prepare and send in one go, for callers without a UI thread (tests, the evaluation).
   *
   * @param logExcerpt execution log captured on the UI thread, or null to read it from the
   *     session's log supplier
   */
  public static AiAdvisorResponse advise(
      AiAdvisorSession session,
      IAiAdvisor advisor,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      String logExcerpt)
      throws HopException {
    return execute(
        session,
        advisor,
        variables,
        prepare(session, advisor, variables, metadataProvider, logExcerpt));
  }

  /** What {@link #prepare} built: everything the model call needs, taken from the open file. */
  public record Prepared(
      AiProvider provider,
      AiAdvisorPrompt prompt,
      List<ChatMessage> history,
      AiAdvisorRequest request) {

    /** Rough size of what is sent: instructions, history and the question with its context. */
    public int estimatedTokens() {
      int tokens =
          estimateTokens(prompt.getSystemPrompt()) + estimateTokens(prompt.getUserPrompt());
      for (ChatMessage message : history) {
        tokens += estimateTokens(messageText(message));
      }
      return tokens;
    }
  }

  /**
   * Build the question on the UI thread. It reads the open pipeline or workflow (XML, check
   * results, settings), which the user may be editing; doing that from a background thread can fail
   * half way or send a mix of old and new.
   *
   * @param logExcerpt execution log captured on the UI thread. When non-null it is used as-is so
   *     SWT log widgets are not touched here.
   */
  public static Prepared prepare(
      AiAdvisorSession session,
      IAiAdvisor advisor,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      String logExcerpt)
      throws HopException {
    HopAiConfig config = HopAiConfigSingleton.getConfig();
    if (!config.isAiEnabled()) {
      throw new AiUserException(BaseMessages.getString(PKG, "AiAdvisorEngine.Disabled"));
    }
    if (advisor == null) {
      throw new AiUserException(BaseMessages.getString(PKG, "AiAdvisorEngine.NoAdvisor"));
    }
    AiProvider provider = loadProvider(session, metadataProvider);
    AiAdvisorRequest request = toRequest(session, variables, metadataProvider, logExcerpt);
    AiAdvisorPrompt prompt = advisor.buildPrompt(request);
    AiAdvisorExtraContext.apply(prompt, advisor, variables);
    // The answer is checked in the background against a copy, not the file the user may edit.
    request.setArtifact(snapshot(request.getArtifact(), metadataProvider, variables));
    List<ChatMessage> history = historyFrom(session);
    checkPromptFits(
        provider.getName(),
        AiProviderSettings.contextBudget(provider, variables),
        AiProviderSettings.maxOutputTokens(provider, variables),
        prompt.getSystemPrompt(),
        prompt.getUserPrompt(),
        history);
    return new Prepared(provider, prompt, history, request);
  }

  /** Send a prepared question and read the answer. Runs in the background. */
  public static AiAdvisorResponse execute(
      AiAdvisorSession session, IAiAdvisor advisor, IVariables variables, Prepared prepared)
      throws HopException {
    if (session.isCancelled()) {
      throw new HopException(BaseMessages.getString(PKG, "AiAdvisorEngine.Cancelled"));
    }
    AiAdvisorPrompt prompt = prepared.prompt();
    AiChatResult chat = ask(session, advisor, prepared, variables);
    if (session.isCancelled() || Thread.currentThread().isInterrupted()) {
      throw new HopException(BaseMessages.getString(PKG, "AiAdvisorEngine.Cancelled"));
    }
    AiAdvisorResponse parsed = advisor.parseResponse(chat.getText());
    if (parsed == null) {
      parsed = new AiAdvisorResponse();
      parsed.setMarkdownAdvice("");
    }
    parsed.setInputTokenCount(chat.getInputTokenCount());
    parsed.setOutputTokenCount(chat.getOutputTokenCount());
    parsed.setDurationMs(chat.getDurationMs());
    // Parameters written as "key: value" text instead of in the block.
    AiProposalTextRecovery.fill(parsed, chat.getText());
    if (parsed.getProposalParseError() == null
        && (parsed.getProposals() == null || parsed.getProposals().isEmpty())
        && mentionsProposals(chat.getText())) {
      // The answer talks about proposals or names proposal types, but holds none: small models
      // sometimes describe the change and leave the block out or empty.
      parsed.setProposalParseError(BaseMessages.getString(PKG, "AiAdvisorEngine.ProposalsMissing"));
    }
    if (parsed.getProposalParseError() == null && usesHopProposalSchema(advisor)) {
      // Proposals outside the schema (an unknown type, a made-up risk level, no parameters).
      parsed.setProposalParseError(AiProposalSchema.check(parsed.getProposals()));
    }
    if (parsed.getProposalParseError() == null) {
      // Proposals that would be blocked in the review: tell the model why, once.
      String blocked = blockedReasons(advisor, prepared.request(), parsed.getProposals());
      if (blocked != null) {
        parsed.setProposalParseError(blocked);
      }
    }
    if (parsed.getProposalParseError() != null && !session.isCancelled()) {
      List<AiProposal> before =
          parsed.getProposals() == null ? List.of() : new ArrayList<>(parsed.getProposals());
      repairProposals(
          prepared.provider(), variables, advisor, prompt, prepared.history(), chat, parsed);
      // Keep whichever set the review can apply more of.
      if (!before.isEmpty()
          && blockedCount(advisor, prepared.request(), before)
              < blockedCount(advisor, prepared.request(), parsed.getProposals())) {
        parsed.setProposals(new ArrayList<>(before));
      }
      if (parsed.getProposalParseError() != null
          && parsed.getProposals() != null
          && !parsed.getProposals().isEmpty()) {
        // The review shows what is still wrong per proposal; no need to repeat it in the turn.
        parsed.setProposalParseError(null);
      }
    }
    return parsed;
  }

  /**
   * Send the question. With Structured answers on the provider, the answer is held to {@link
   * AiProposalSchema} where the provider type allows it, and turned back into the usual text. When
   * the provider refuses the schema, the question is asked again without it.
   */
  static AiChatResult ask(
      AiAdvisorSession session, IAiAdvisor advisor, Prepared prepared, IVariables variables)
      throws HopException {
    AiProvider provider = prepared.provider();
    AiAdvisorPrompt prompt = prepared.prompt();
    if (provider.isStructuredAnswers() && usesHopProposalSchema(advisor)) {
      try {
        AiChatResult structured =
            AiChatFactory.generateStructured(
                provider,
                variables,
                structuredSystemPrompt(prompt.getSystemPrompt()),
                prompt.getUserPrompt(),
                prepared.history(),
                AiProposalSchema.schema());
        if (structured != null) {
          return new AiChatResult(
              AiProposalSchema.toAnswerText(structured.getText()),
              structured.getInputTokenCount(),
              structured.getOutputTokenCount(),
              structured.getDurationMs());
        }
      } catch (HopException e) {
        if (session.isCancelled() || Thread.currentThread().isInterrupted()) {
          throw e;
        }
        LogChannel.GENERAL.logBasic(
            BaseMessages.getString(
                PKG, "AiAdvisorEngine.StructuredFailed", provider.getName(), e.getMessage()));
      }
    }
    return AiChatFactory.generateResult(
        provider, variables, prompt.getSystemPrompt(), prompt.getUserPrompt(), prepared.history());
  }

  /**
   * Whether the advisor's proposals are the Hop pipeline and workflow types of {@link
   * AiProposalSchema}. Advisors of other plugins have types of their own (CREATE_HUB, …), which the
   * schema would reject.
   */
  static boolean usesHopProposalSchema(IAiAdvisor advisor) {
    return advisor instanceof PipelineAiAdvisor || advisor instanceof WorkflowAiAdvisor;
  }

  /** The instructions with the answer format of {@link AiProposalSchema} added. */
  static String structuredSystemPrompt(String systemPrompt) throws HopException {
    return systemPrompt
        + "\n\n"
        + AiPromptLoader.load(AiM2PromptSupport.PROMPT_ROOT, "structured-answer.txt");
  }

  /**
   * A copy of the open pipeline or workflow for the background checks. When it cannot be copied (a
   * plugin is missing) the checks read the original: they only read, and a failure there only skips
   * the repair. Artifacts of other advisors are passed on as they are.
   */
  static Object snapshot(Object artifact, IHopMetadataProvider provider, IVariables variables) {
    Object copy = null;
    if (artifact instanceof PipelineMeta pipelineMeta) {
      copy = PipelineAiProposalApplier.copyForDryRun(pipelineMeta, provider);
    } else if (artifact instanceof WorkflowMeta workflowMeta) {
      copy =
          WorkflowAiProposalApplier.copyForDryRun(
              workflowMeta,
              provider,
              variables != null ? variables : Variables.getADefaultVariableSpace());
    }
    return copy != null ? copy : artifact;
  }

  static int blockedCount(
      IAiAdvisor advisor, AiAdvisorRequest request, List<AiProposal> proposals) {
    if (request == null
        || request.getArtifact() == null
        || proposals == null
        || proposals.isEmpty()) {
      return Integer.MAX_VALUE;
    }
    try {
      int blocked = 0;
      for (AiProposalValidation validation : advisor.validateProposals(request, proposals)) {
        if (validation != null && validation.isBlocked()) {
          blocked++;
        }
      }
      // Nothing to apply is worse than anything to apply.
      return blocked == proposals.size() ? Integer.MAX_VALUE - 1 : blocked;
    } catch (RuntimeException e) {
      return Integer.MAX_VALUE;
    }
  }

  /**
   * Why proposals would be blocked in the review, one line each, or null when none would be. Only
   * reads the open file; a failure to check is not a reason to bother the model.
   */
  static String blockedReasons(
      IAiAdvisor advisor, AiAdvisorRequest request, List<AiProposal> proposals) {
    if (request == null
        || request.getArtifact() == null
        || proposals == null
        || proposals.isEmpty()) {
      return null;
    }
    try {
      List<AiProposalValidation> validations = advisor.validateProposals(request, proposals);
      StringBuilder reasons = new StringBuilder();
      for (int i = 0; i < validations.size() && i < proposals.size(); i++) {
        AiProposalValidation validation = validations.get(i);
        if (validation != null && validation.isBlocked()) {
          reasons
              .append("\n- proposal ")
              .append(i + 1)
              .append(" (")
              .append(proposals.get(i).getType())
              .append("): ")
              .append(validation.getReason());
        }
      }
      return reasons.isEmpty()
          ? null
          : BaseMessages.getString(PKG, "AiAdvisorEngine.ProposalsBlocked") + reasons;
    } catch (RuntimeException e) {
      return null;
    }
  }

  /**
   * Ask once for a corrected {@code hop_proposals} block when the first one could not be read.
   * Small models get the JSON wrong more often than the advice. The advice text of the first answer
   * is kept; only the proposals come from the repair. When the repair fails as well, the first
   * error stays on the response so the user sees why there is nothing to review.
   */
  static void repairProposals(
      AiProvider provider,
      IVariables variables,
      IAiAdvisor advisor,
      AiAdvisorPrompt prompt,
      List<ChatMessage> history,
      AiChatResult first,
      AiAdvisorResponse parsed) {
    List<ChatMessage> conversation = new ArrayList<>(history);
    conversation.add(new UserMessage(prompt.getUserPrompt()));
    conversation.add(new AiMessage(first.getText()));
    String instruction =
        parsed.getProposalParseError()
            + " Reply with only the corrected ```hop_proposals block: a JSON object with a"
            + " \"proposals\" array, as described in the proposal schema. Every proposal needs"
            + " its \"parameters\" object, and names must be the exact names of existing"
            + " transforms or actions, or of ones added in the same block. No other text. If you"
            + " did not mean to propose a change, reply with {\"proposals\": []} in that block.";
    try {
      AiChatResult repair =
          AiChatFactory.generateResult(
              provider, variables, prompt.getSystemPrompt(), instruction, conversation, true);
      AiAdvisorResponse repaired = advisor.parseResponse(fenced(repair.getText()));
      if (repaired != null
          && repaired.isProposalBlockPresent()
          && repaired.getProposalParseError() == null) {
        // Either the proposals, or an empty list: the model confirms there is no change.
        AiProposalTextRecovery.fill(repaired, repair.getText());
        parsed.setProposals(
            repaired.getProposals() != null ? repaired.getProposals() : new ArrayList<>());
        parsed.setProposalParseError(null);
        parsed.setProposalBlockPresent(!parsed.getProposals().isEmpty());
      }
      parsed.setInputTokenCount(sum(parsed.getInputTokenCount(), repair.getInputTokenCount()));
      parsed.setOutputTokenCount(sum(parsed.getOutputTokenCount(), repair.getOutputTokenCount()));
      parsed.setDurationMs(sum(parsed.getDurationMs(), repair.getDurationMs()));
    } catch (HopException e) {
      // Keep the first answer and its parse error.
    }
  }

  /** In JSON-only mode the reply is the bare object; give it the fence the parser looks for. */
  static String fenced(String text) {
    if (text == null) {
      return "";
    }
    String trimmed = text.trim();
    if (trimmed.startsWith("{") && !trimmed.contains("```")) {
      return "```hop_proposals\n" + trimmed + "\n```";
    }
    return text;
  }

  private static Integer sum(Integer a, Integer b) {
    return a == null ? b : b == null ? a : Integer.valueOf(a + b);
  }

  private static Long sum(Long a, Long b) {
    return a == null ? b : b == null ? a : Long.valueOf(a + b);
  }

  static AiAdvisorRequest toRequest(
      AiAdvisorSession session,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      String logExcerpt) {
    return toRequest(session, variables, metadataProvider, logExcerpt, true);
  }

  /**
   * @param consumeApplied false for a preview, which must leave the applied-change summaries for
   *     the next real question
   */
  static AiAdvisorRequest toRequest(
      AiAdvisorSession session,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      String logExcerpt,
      boolean consumeApplied) {
    AiAdvisorRequest request = new AiAdvisorRequest();
    if (session == null) {
      return request;
    }
    request.setLocation(session.getLocation());
    request.setScenarioId(session.getScenarioId());
    request.setUserPrompt(
        session.isEmpty()
            ? ""
            : session.getTurns().get(session.getTurns().size() - 1).getUserPrompt());
    request.setFocusNodeName(session.getFocusNodeName());
    request.setAiProviderName(session.getProviderName());
    request.setVariables(variables);
    request.setMetadataProvider(metadataProvider);
    request.setArtifact(session.getArtifact());
    request.setInclusions(
        session.getInclusions() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(session.getInclusions()));
    request.setMetadataSelections(
        session.getMetadataSelections() == null
            ? new ArrayList<>()
            : new ArrayList<>(session.getMetadataSelections()));
    request.setAttributes(copyAttributes(session.getAttributes()));
    request.setInclusionSelections(copyInclusionSelections(session.getInclusionSelections()));
    request.setFollowUp(hasSuccessfulPriorTurn(session));
    request.setAppliedChangeSummaries(
        consumeApplied
            ? session.consumePendingAppliedSummaries()
            : List.copyOf(session.getPendingAppliedSummaries()));
    if (logExcerpt != null) {
      request.setLogExcerpt(logExcerpt);
    } else if (session.getLogSupplier() != null) {
      request.setLogExcerpt(session.getLogSupplier().get());
    }
    return request;
  }

  static Map<String, Object> copyAttributes(Map<String, Object> source) {
    return source == null ? new LinkedHashMap<>() : new LinkedHashMap<>(source);
  }

  static Map<String, List<String>> copyInclusionSelections(Map<String, List<String>> source) {
    Map<String, List<String>> copy = new LinkedHashMap<>();
    if (source == null) {
      return copy;
    }
    for (Map.Entry<String, List<String>> entry : source.entrySet()) {
      copy.put(
          entry.getKey(),
          entry.getValue() == null ? new ArrayList<>() : new ArrayList<>(entry.getValue()));
    }
    return copy;
  }

  public static AiProvider loadProvider(
      AiAdvisorSession session, IHopMetadataProvider metadataProvider) throws HopException {
    String name = session.getProviderName();
    if (Utils.isEmpty(name)) {
      name = HopAiConfigSingleton.getConfig().getDefaultProviderName();
    }
    if (Utils.isEmpty(name)) {
      throw new AiUserException(BaseMessages.getString(PKG, "AiAdvisorEngine.NoProvider"));
    }
    AiProvider provider;
    try {
      provider = metadataProvider.getSerializer(AiProvider.class).load(name);
    } catch (HopException e) {
      throw new AiUserException(
          BaseMessages.getString(PKG, "AiAdvisorEngine.ProviderNotLoaded", name), e);
    }
    if (provider == null) {
      throw new AiUserException(
          BaseMessages.getString(PKG, "AiAdvisorEngine.ProviderNotFound", name));
    }
    return provider;
  }

  private static final java.util.regex.Pattern PROPOSAL_MENTION =
      java.util.regex.Pattern.compile(
          "\\b(ADD_TRANSFORM|ADD_ACTION|ADD_PIPELINE_HOP|ADD_WORKFLOW_HOP|CONFIGURE_TRANSFORM"
              + "|CONFIGURE_ACTION|SAVE_METADATA|DELETE_TRANSFORM|DELETE_ACTION|RENAME_TRANSFORM"
              + "|RENAME_ACTION|REPLACE_TRANSFORM|REPLACE_ACTION|hop_proposals"
              // Parameters written out as a list instead of a block.
              + "|transformPluginId|actionPluginId|fromTransform|toTransform|fromAction|toAction)\\b");

  static boolean mentionsProposals(String text) {
    return text != null && PROPOSAL_MENTION.matcher(text).find();
  }

  /**
   * An earlier answer as the model should remember it: its prose and a clean, valid block of the
   * proposals as they were read. Not the original text: a small model copies its own earlier
   * answers, so a broken block there gets repeated in every follow-up.
   */
  static String answerForHistory(AiAdvisorTurn turn) {
    String advice = Const.NVL(turn.getAssistantAdvice(), "");
    if (turn.getProposals() == null || turn.getProposals().isEmpty()) {
      return advice;
    }
    List<Map<String, Object>> proposals = new ArrayList<>();
    for (AiProposal proposal : turn.getProposals()) {
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("id", Const.NVL(proposal.getId(), Integer.toString(proposals.size() + 1)));
      entry.put("description", Const.NVL(proposal.getDescription(), ""));
      entry.put("riskLevel", Const.NVL(proposal.getRiskLevel(), "LOW"));
      entry.put("type", proposal.getType());
      entry.put("parameters", proposal.getParameters());
      proposals.add(entry);
    }
    try {
      return advice
          + "\n\n```hop_proposals\n"
          + HopJson.newMapper().writeValueAsString(Map.of("proposals", proposals))
          + "\n```";
    } catch (Exception e) {
      return advice;
    }
  }

  /** Roughly four characters per token for English text and code; JSON packs a little tighter. */
  static final int CHARS_PER_TOKEN = 4;

  /** Kept free for the answer when the provider sets no output limit. */
  static final int DEFAULT_ANSWER_RESERVE = 2_048;

  static int estimateTokens(String text) {
    return text == null ? 0 : (text.length() + CHARS_PER_TOKEN - 1) / CHARS_PER_TOKEN;
  }

  /**
   * Stop before sending a prompt that clearly does not fit the provider's context window. Ollama
   * would drop the start of it without a word, which loses the instructions and the context, and
   * hosted providers reject it. Only clear overflows are stopped: the estimate is rough.
   *
   * @param contextSize the window in tokens; see {@link AiProviderSettings#contextBudget}
   */
  static void checkPromptFits(
      String providerName,
      int contextSize,
      Integer maxOutputTokens,
      String systemPrompt,
      String userPrompt,
      List<ChatMessage> history)
      throws HopException {
    int tokens = estimateTokens(systemPrompt) + estimateTokens(userPrompt);
    for (ChatMessage message : history) {
      if (message instanceof UserMessage user && user.hasSingleText()) {
        tokens += estimateTokens(user.singleText());
      } else if (message instanceof AiMessage ai) {
        tokens += estimateTokens(ai.text());
      }
    }
    int reserve =
        maxOutputTokens != null
            ? maxOutputTokens
            : Math.min(DEFAULT_ANSWER_RESERVE, contextSize / 4);
    if (tokens + reserve > contextSize) {
      throw new AiUserException(
          BaseMessages.getString(
              PKG,
              "AiAdvisorEngine.PromptTooLarge",
              Integer.toString(tokens),
              providerName,
              Integer.toString(contextSize),
              Integer.toString(reserve)));
    }
  }

  /**
   * Exactly what the next question would send, for the user to read before sending it: the
   * instructions, how much conversation history goes along, and the question with its context.
   *
   * @param question the text in the question field; a placeholder is used when it is empty
   */
  public static String preview(
      AiAdvisorSession session,
      IAiAdvisor advisor,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      String logExcerpt,
      String question)
      throws HopException {
    if (advisor == null) {
      throw new HopException(BaseMessages.getString(PKG, "AiAdvisorEngine.NoAdvisor"));
    }
    AiAdvisorRequest request = toRequest(session, variables, metadataProvider, logExcerpt, false);
    request.setUserPrompt(Utils.isEmpty(question) ? "(your question)" : question.trim());
    request.setFollowUp(hasAnswer(session.getTurns(), session.getTurns().size()));
    AiAdvisorPrompt prompt = advisor.buildPrompt(request);
    AiAdvisorExtraContext.apply(prompt, advisor, variables);
    if (usesHopProposalSchema(advisor) && usesStructuredAnswers(session, metadataProvider)) {
      prompt.setSystemPrompt(structuredSystemPrompt(prompt.getSystemPrompt()));
    }
    List<ChatMessage> history = historyFrom(session.getTurns(), session.getTurns().size());
    int historyTokens = 0;
    for (ChatMessage message : history) {
      historyTokens += estimateTokens(messageText(message));
    }
    int total =
        estimateTokens(prompt.getSystemPrompt())
            + estimateTokens(prompt.getUserPrompt())
            + historyTokens;
    return "About "
        + total
        + " tokens in total.\n\n"
        + "=== Instructions (system message, about "
        + estimateTokens(prompt.getSystemPrompt())
        + " tokens) ===\n"
        + prompt.getSystemPrompt()
        + "\n\n=== Conversation history: "
        + history.size() / 2
        + " earlier question(s) and answer(s), about "
        + historyTokens
        + " tokens ===\n\n"
        + "=== Your question with its context (about "
        + estimateTokens(prompt.getUserPrompt())
        + " tokens) ===\n"
        + prompt.getUserPrompt();
  }

  /** For the preview: whether the session's provider asks for structured answers. */
  private static boolean usesStructuredAnswers(
      AiAdvisorSession session, IHopMetadataProvider metadataProvider) {
    try {
      return loadProvider(session, metadataProvider).isStructuredAnswers();
    } catch (HopException e) {
      return false;
    }
  }

  private static String messageText(ChatMessage message) {
    if (message instanceof UserMessage user && user.hasSingleText()) {
      return user.singleText();
    }
    if (message instanceof AiMessage ai) {
      return ai.text();
    }
    return "";
  }

  static boolean hasSuccessfulPriorTurn(AiAdvisorSession session) {
    return hasAnswer(session.getTurns(), session.getTurns().size() - 1);
  }

  /** Whether one of the first {@code count} turns has an answer. */
  private static boolean hasAnswer(List<AiAdvisorTurn> turns, int count) {
    for (int i = 0; i < count && i < turns.size(); i++) {
      if (!Utils.isEmpty(turns.get(i).getAssistantAdvice())) {
        return true;
      }
    }
    return false;
  }

  static List<ChatMessage> historyFrom(AiAdvisorSession session) {
    return historyFrom(session.getTurns(), session.getTurns().size() - 1);
  }

  /** The last answered turns before turn {@code end}, as chat messages. */
  static List<ChatMessage> historyFrom(List<AiAdvisorTurn> turns, int end) {
    if (end <= 0) {
      return List.of();
    }
    int from = Math.max(0, end - AiAdvisorSession.MAX_HISTORY_TURNS);
    List<ChatMessage> history = new ArrayList<>();
    for (int i = from; i < end; i++) {
      AiAdvisorTurn turn = turns.get(i);
      if (!Utils.isEmpty(turn.getUserPrompt()) && !Utils.isEmpty(turn.getAssistantAdvice())) {
        history.add(new UserMessage(turn.getUserPrompt()));
        history.add(new AiMessage(answerForHistory(turn)));
      }
    }
    return history;
  }
}
