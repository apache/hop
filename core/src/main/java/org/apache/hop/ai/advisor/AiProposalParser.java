/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ai.advisor;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.util.Utils;

/**
 * Parses advisory text and extracts {@code hop_proposals} JSON blocks. Third-party advisors that
 * use a different fence override {@link IAiAdvisor#parseResponse(String)}.
 */
public final class AiProposalParser {

  private static final Pattern PROPOSAL_BLOCK =
      Pattern.compile("```hop_proposals\\s*([\\s\\S]*?)```", Pattern.CASE_INSENSITIVE);

  private AiProposalParser() {}

  public static boolean hasProposalBlock(String rawResponse) {
    return !Utils.isEmpty(rawResponse) && PROPOSAL_BLOCK.matcher(rawResponse).find();
  }

  public static AiAdvisorResponse parse(String rawResponse) {
    AiAdvisorResponse response = new AiAdvisorResponse();
    response.setRawResponse(rawResponse);
    if (Utils.isEmpty(rawResponse)) {
      response.setMarkdownAdvice("");
      return response;
    }

    String advice = rawResponse;
    List<AiProposal> proposals = new ArrayList<>();
    boolean blockPresent = false;
    Matcher matcher = PROPOSAL_BLOCK.matcher(rawResponse);
    String error = null;
    while (matcher.find()) {
      blockPresent = true;
      advice = advice.replace(matcher.group(0), "").trim();
      try {
        proposals.addAll(parseProposalJson(matcher.group(1)));
      } catch (IllegalArgumentException e) {
        error = e.getMessage();
      }
    }
    response.setProposalBlockPresent(blockPresent);
    response.setProposalParseError(error);
    response.setMarkdownAdvice(advice.trim());
    response.setProposals(proposals);
    return response;
  }

  /**
   * @throws IllegalArgumentException with a short reason when the block is not a proposals object
   */
  private static List<AiProposal> parseProposalJson(String jsonText) {
    List<AiProposal> proposals = new ArrayList<>();
    if (Utils.isEmpty(jsonText)) {
      throw new IllegalArgumentException("The hop_proposals block is empty.");
    }
    JsonNode root;
    try {
      root = HopJson.newMapper().readTree(jsonText.trim());
    } catch (Exception e) {
      String reason = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
      int newline = reason.indexOf('\n');
      throw new IllegalArgumentException(
          "The hop_proposals block is not valid JSON: "
              + (newline > 0 ? reason.substring(0, newline) : reason));
    }
    JsonNode array = root == null ? null : root.path("proposals");
    if (array == null || !array.isArray()) {
      throw new IllegalArgumentException(
          "The hop_proposals block has no \"proposals\" array at the top level.");
    }
    for (JsonNode node : array) {
      AiProposal proposal = toProposal(node);
      if (proposal != null) {
        // Small models copy a list of allowed types into one proposal ("ADD_TRANSFORM|ADD_HOP").
        // That can never be applied; reading it as unreadable gets it corrected.
        String type = proposal.getType();
        if (type != null && (type.contains("|") || type.contains(","))) {
          throw new IllegalArgumentException(
              "Proposal '"
                  + (Utils.isEmpty(proposal.getDescription()) ? type : proposal.getDescription())
                  + "' has several types ("
                  + type
                  + "). Give each proposal exactly one type: adding a transform and its hop takes"
                  + " two proposals.");
        }
        proposals.add(proposal);
      }
    }
    return proposals;
  }

  private static AiProposal toProposal(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    // An item without a type is kept: the validator blocks it and the user sees why.
    String typeValue = node.path("type").asText("");
    AiProposal proposal = new AiProposal();
    String id = node.path("id").asText("");
    proposal.setId(Utils.isEmpty(id) ? null : id);
    proposal.setDescription(node.path("description").asText(""));
    proposal.setRiskLevel(parseRisk(node.path("riskLevel").asText("MEDIUM")));
    proposal.setType(typeValue.trim());
    JsonNode parameters = node.path("parameters");
    if (parameters.isObject()) {
      Map<String, String> map = new LinkedHashMap<>();
      Iterator<Map.Entry<String, JsonNode>> fields = parameters.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        map.put(entry.getKey(), parameterValue(entry.getValue()));
      }
      proposal.setParameters(map);
    }
    return proposal;
  }

  /**
   * Object and array parameter values (typical for {@code json} / {@code config}) must be kept as
   * JSON text. {@link JsonNode#asText()} returns empty for those nodes.
   */
  public static String parameterValue(JsonNode value) {
    if (value == null || value.isNull() || value.isMissingNode()) {
      return "";
    }
    if (value.isTextual() || value.isNumber() || value.isBoolean()) {
      return value.asText("");
    }
    if (value.isObject() || value.isArray()) {
      try {
        return HopJson.newMapper().writeValueAsString(value);
      } catch (Exception e) {
        return "";
      }
    }
    return value.asText("");
  }

  private static String parseRisk(String value) {
    if (Utils.isEmpty(value)) {
      return "MEDIUM";
    }
    String normalized = value.trim().toUpperCase();
    if ("LOW".equals(normalized) || "MEDIUM".equals(normalized) || "HIGH".equals(normalized)) {
      return normalized;
    }
    return "MEDIUM";
  }
}
