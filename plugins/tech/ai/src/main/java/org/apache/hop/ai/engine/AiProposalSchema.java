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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.langchain4j.model.chat.request.json.JsonArraySchema;
import dev.langchain4j.model.chat.request.json.JsonEnumSchema;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import dev.langchain4j.model.chat.request.json.JsonStringSchema;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.core.Const;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;

/**
 * The answer of an advisor as a JSON schema, for providers that can hold a model to one, and the
 * same rules as a check on proposals however they were written.
 *
 * <p>With a schema the model answers {@code {"answer": "...", "proposals": [...]}}, every parameter
 * as a {@code {"name", "value"}} pair: strict schemas allow no free-form objects. {@link
 * #toAnswerText(String)} turns that back into the Markdown answer with a {@code hop_proposals}
 * block, so parsing, repair and review work the same whichever way the answer came.
 */
public final class AiProposalSchema {

  private static final Class<?> PKG = AiProposalSchema.class;

  static final String SCHEMA_NAME = "hop_answer";

  static final Set<String> RISK_LEVELS = Set.of("LOW", "MEDIUM", "HIGH");

  private AiProposalSchema() {}

  public static JsonSchema schema() {
    List<String> types = new ArrayList<>();
    for (AiProposalTypes type : AiProposalTypes.values()) {
      types.add(type.name());
    }
    JsonObjectSchema parameter =
        JsonObjectSchema.builder()
            .addStringProperty("name", "Parameter name, for example transformName")
            .addStringProperty(
                "value", "Parameter value as text; JSON objects and XML are written as a string")
            .required("name", "value")
            .additionalProperties(false)
            .build();
    JsonObjectSchema proposal =
        JsonObjectSchema.builder()
            .addStringProperty("id", "1, 2, 3, ... in the order to apply")
            .addStringProperty("description", "What the change does, in the user's language")
            .addProperty(
                "riskLevel", JsonEnumSchema.builder().enumValues("LOW", "MEDIUM", "HIGH").build())
            .addProperty("type", JsonEnumSchema.builder().enumValues(types).build())
            .addProperty("parameters", JsonArraySchema.builder().items(parameter).build())
            .required("id", "description", "riskLevel", "type", "parameters")
            .additionalProperties(false)
            .build();
    return JsonSchema.builder()
        .name(SCHEMA_NAME)
        .rootElement(
            JsonObjectSchema.builder()
                .addProperty(
                    "answer",
                    JsonStringSchema.builder()
                        .description("The whole answer for the user, in Markdown")
                        .build())
                .addProperty(
                    "proposals",
                    JsonArraySchema.builder()
                        .description("Proposed changes; empty unless the user asked for a change")
                        .items(proposal)
                        .build())
                .required("answer", "proposals")
                .additionalProperties(false)
                .build())
        .build();
  }

  /**
   * The structured answer as the text an advisor parses: the Markdown answer, followed by a {@code
   * hop_proposals} block when there are proposals. Text that is not such an object is returned as
   * it is, for the parser and the repair to deal with.
   */
  public static String toAnswerText(String json) {
    if (Utils.isEmpty(json)) {
      return "";
    }
    try {
      ObjectMapper mapper = HopJson.newMapper();
      JsonNode root = mapper.readTree(json.trim());
      if (root == null || !root.isObject() || !root.has("answer")) {
        return json;
      }
      String answer = withoutProposalBlocks(root.path("answer").asText(""), mapper);
      List<Map<String, Object>> proposals = new ArrayList<>();
      for (JsonNode node : root.path("proposals")) {
        Map<String, Object> proposal = new LinkedHashMap<>();
        proposal.put("id", node.path("id").asText(Integer.toString(proposals.size() + 1)));
        proposal.put("description", node.path("description").asText(""));
        proposal.put("riskLevel", node.path("riskLevel").asText("LOW"));
        proposal.put("type", node.path("type").asText(""));
        Map<String, String> parameters = new LinkedHashMap<>();
        JsonNode list = node.path("parameters");
        if (list.isArray()) {
          for (JsonNode parameter : list) {
            String name = parameter.path("name").asText("");
            if (!name.isEmpty()) {
              parameters.put(name, valueText(parameter.path("value")));
            }
          }
        } else if (list.isObject()) {
          // A provider that does not enforce the schema may still write the usual object.
          list.fields()
              .forEachRemaining(
                  entry -> parameters.put(entry.getKey(), valueText(entry.getValue())));
        }
        proposal.put("parameters", parameters);
        proposals.add(proposal);
      }
      if (proposals.isEmpty()) {
        return answer;
      }
      return answer
          + "\n\n```hop_proposals\n"
          + mapper.writeValueAsString(Map.of("proposals", proposals))
          + "\n```";
    } catch (Exception e) {
      return json;
    }
  }

  private static final Pattern FENCED_BLOCK =
      Pattern.compile("```[A-Za-z_]*\\s*\\n(.*?)\\n?```\\s*", Pattern.DOTALL);

  /**
   * The answer without code blocks that repeat the proposals: some models also write them out in
   * the answer, as a hop_proposals or json block. The proposals of the structured answer are the
   * ones that count, and a second copy would be parsed instead of them.
   */
  static String withoutProposalBlocks(String answer, ObjectMapper mapper) {
    Matcher matcher = FENCED_BLOCK.matcher(answer);
    StringBuilder kept = new StringBuilder();
    while (matcher.find()) {
      boolean proposals;
      try {
        JsonNode block = mapper.readTree(matcher.group(1).trim());
        proposals = block != null && block.isObject() && block.has("proposals");
      } catch (Exception e) {
        proposals = false;
      }
      matcher.appendReplacement(kept, proposals ? "" : Matcher.quoteReplacement(matcher.group()));
    }
    matcher.appendTail(kept);
    return kept.toString().strip();
  }

  private static String valueText(JsonNode value) {
    if (value == null || value.isNull() || value.isMissingNode()) {
      return "";
    }
    return value.isValueNode() ? value.asText() : value.toString();
  }

  /**
   * Check proposals against the rules of the schema: a known type, a risk level of LOW, MEDIUM or
   * HIGH, and parameters. Applies to every answer, also one written as free text.
   *
   * @return one line per problem, or null when the proposals follow the schema
   */
  public static String check(List<AiProposal> proposals) {
    if (proposals == null || proposals.isEmpty()) {
      return null;
    }
    StringBuilder problems = new StringBuilder();
    for (int i = 0; i < proposals.size(); i++) {
      AiProposal proposal = proposals.get(i);
      String problem = null;
      if (proposal == null || AiProposalTypes.of(proposal) == null) {
        problem =
            BaseMessages.getString(
                PKG,
                "AiProposalSchema.UnknownType",
                proposal == null ? "" : Const.NVL(proposal.getType(), ""));
      } else if (!Utils.isEmpty(proposal.getRiskLevel())
          && !RISK_LEVELS.contains(proposal.getRiskLevel().trim().toUpperCase())) {
        problem =
            BaseMessages.getString(PKG, "AiProposalSchema.UnknownRisk", proposal.getRiskLevel());
      } else if (proposal.getParameters() == null || proposal.getParameters().isEmpty()) {
        problem = BaseMessages.getString(PKG, "AiProposalSchema.NoParameters");
      }
      if (problem != null) {
        problems.append("\n- proposal ").append(i + 1).append(": ").append(problem);
      }
    }
    return problems.isEmpty()
        ? null
        : BaseMessages.getString(PKG, "AiProposalSchema.Problems") + problems;
  }
}
