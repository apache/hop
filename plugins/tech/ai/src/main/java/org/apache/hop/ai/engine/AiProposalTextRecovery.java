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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.ai.advisor.AiAdvisorResponse;
import org.apache.hop.ai.advisor.AiProposal;

/**
 * Reads proposals that a model wrote as text instead of in the {@code hop_proposals} block. Small
 * models often describe each change as a type on its own line followed by {@code key: value} lines,
 * and leave the parameters out of the block:
 *
 * <pre>
 * ADD_PIPELINE_HOP
 *   fromTransform: Output
 *   toTransform: Dummy
 * </pre>
 */
public final class AiProposalTextRecovery {

  private static final Pattern PARAMETER =
      Pattern.compile("^\\s*[-*]?\\s*\\**`?([A-Za-z][\\w.]*)`?\\**\\s*:\\s*(.+?)\\s*$");

  private AiProposalTextRecovery() {}

  static List<AiProposal> recover(String text) {
    List<AiProposal> proposals = new ArrayList<>();
    if (text == null) {
      return proposals;
    }
    Set<String> types = new HashSet<>();
    for (AiProposalTypes type : AiProposalTypes.values()) {
      types.add(type.name());
    }
    AiProposal current = null;
    for (String line : text.split("\\R")) {
      String bare = line.replaceAll("[#*`\\[\\]:]", "").trim();
      if (types.contains(bare)) {
        current = new AiProposal();
        current.setType(bare);
        current.setRiskLevel("LOW");
        proposals.add(current);
        continue;
      }
      if (current == null) {
        continue;
      }
      Matcher matcher = PARAMETER.matcher(line);
      if (matcher.matches()) {
        current.getParameters().put(matcher.group(1), unquote(matcher.group(2)));
      } else if (!line.isBlank() && !line.trim().startsWith("```")) {
        current = null;
      }
    }
    proposals.removeIf(proposal -> proposal.getParameters().isEmpty());
    return proposals;
  }

  /**
   * Fill proposals that came without parameters from the ones written in the text, in order and by
   * type. With no usable proposals at all, the ones from the text are taken.
   *
   * @return true when something was recovered
   */
  public static boolean fill(AiAdvisorResponse response, String text) {
    List<AiProposal> recovered = recover(text);
    if (recovered.isEmpty()) {
      return false;
    }
    List<AiProposal> proposals = response.getProposals();
    boolean usable =
        proposals != null && proposals.stream().anyMatch(p -> !p.getParameters().isEmpty());
    if (!usable
        && (proposals == null || proposals.isEmpty() || response.getProposalParseError() != null)) {
      response.setProposals(new ArrayList<>(recovered));
      response.setProposalParseError(null);
      response.setProposalBlockPresent(true);
      return true;
    }
    boolean changed = false;
    List<AiProposal> unused = new ArrayList<>(recovered);
    for (AiProposal proposal : proposals) {
      if (!proposal.getParameters().isEmpty()) {
        continue;
      }
      for (AiProposal candidate : unused) {
        if (candidate.getType().equals(proposal.getType())) {
          proposal.getParameters().putAll(candidate.getParameters());
          unused.remove(candidate);
          changed = true;
          break;
        }
      }
    }
    return changed;
  }

  private static String unquote(String value) {
    String trimmed = value.trim();
    if (trimmed.length() >= 2
        && (trimmed.startsWith("\"") && trimmed.endsWith("\"")
            || trimmed.startsWith("'") && trimmed.endsWith("'")
            || trimmed.startsWith("`") && trimmed.endsWith("`"))) {
      return trimmed.substring(1, trimmed.length() - 1);
    }
    return trimmed;
  }
}
