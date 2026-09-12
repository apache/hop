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
import com.fasterxml.jackson.databind.ObjectMapper;
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
    while (matcher.find()) {
      blockPresent = true;
      advice = advice.replace(matcher.group(0), "").trim();
      proposals.addAll(parseProposalJson(matcher.group(1)));
    }
    response.setProposalBlockPresent(blockPresent);
    response.setMarkdownAdvice(advice.trim());
    response.setProposals(proposals);
    return response;
  }

  private static List<AiProposal> parseProposalJson(String jsonText) {
    List<AiProposal> proposals = new ArrayList<>();
    if (Utils.isEmpty(jsonText)) {
      return proposals;
    }
    try {
      ObjectMapper mapper = HopJson.newMapper();
      JsonNode root = mapper.readTree(jsonText.trim());
      JsonNode array = root.path("proposals");
      if (!array.isArray()) {
        return proposals;
      }
      for (JsonNode node : array) {
        AiProposal proposal = toProposal(node);
        if (proposal != null) {
          proposals.add(proposal);
        }
      }
    } catch (Exception ignored) {
      // Malformed blocks are dropped; advice text is still shown.
    }
    return proposals;
  }

  private static AiProposal toProposal(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    String typeValue = node.path("type").asText("");
    if (Utils.isEmpty(typeValue)) {
      return null;
    }
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
        map.put(entry.getKey(), entry.getValue().asText(""));
      }
      proposal.setParameters(map);
    }
    return proposal;
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
