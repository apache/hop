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

import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.core.util.Utils;

/** Human-readable preview of one hop_proposals item. */
public final class AiProposalPreview {

  private AiProposalPreview() {}

  public static String format(AiProposal proposal) {
    if (proposal == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    if (!Utils.isEmpty(proposal.getDescription())) {
      sb.append(proposal.getDescription());
    } else if (!Utils.isEmpty(proposal.getType())) {
      sb.append(proposal.getType());
    }
    if (!Utils.isEmpty(proposal.getId())) {
      sb.append(" [").append(proposal.getId()).append(']');
    }
    sb.append("\nType: ").append(proposal.getType());
    sb.append("\nRisk: ").append(proposal.getRiskLevel());
    if (proposal.getParameters() != null && !proposal.getParameters().isEmpty()) {
      sb.append("\nParameters:");
      proposal
          .getParameters()
          .forEach((k, v) -> sb.append("\n  ").append(k).append(" = ").append(v));
    }
    return sb.toString();
  }

  public static String appliedSummary(AiProposal proposal) {
    AiProposalTypes type = AiProposalTypes.of(proposal);
    if (type == null) {
      return "unknown change";
    }
    return switch (type) {
      case ADD_TRANSFORM ->
          "ADD_TRANSFORM: "
              + proposal.parameter("name")
              + " ("
              + proposal.parameter("transformPluginId")
              + ")";
      case DELETE_TRANSFORM -> "DELETE_TRANSFORM: " + proposal.parameter("transformName");
      case RENAME_TRANSFORM ->
          "RENAME_TRANSFORM: "
              + proposal.parameter("transformName")
              + " -> "
              + proposal.parameter("newName");
      case ADD_PIPELINE_HOP ->
          "ADD_PIPELINE_HOP: "
              + proposal.parameter("fromTransform")
              + " -> "
              + proposal.parameter("toTransform");
      case DELETE_PIPELINE_HOP ->
          "DELETE_PIPELINE_HOP: "
              + proposal.parameter("fromTransform")
              + " -> "
              + proposal.parameter("toTransform");
      case SET_TRANSFORM_LOCATION ->
          "SET_TRANSFORM_LOCATION: "
              + proposal.parameter("transformName")
              + " @ "
              + proposal.parameter("locationX")
              + ","
              + proposal.parameter("locationY");
      case ADD_PIPELINE_NOTE -> "ADD_PIPELINE_NOTE: " + truncate(proposal.parameter("text"), 120);
      case ADD_ACTION ->
          "ADD_ACTION: "
              + proposal.parameter("name")
              + " ("
              + proposal.parameter("actionPluginId")
              + ")";
      case DELETE_ACTION -> "DELETE_ACTION: " + proposal.parameter("actionName");
      case RENAME_ACTION ->
          "RENAME_ACTION: "
              + proposal.parameter("actionName")
              + " -> "
              + proposal.parameter("newName");
      case ADD_WORKFLOW_HOP ->
          "ADD_WORKFLOW_HOP: "
              + proposal.parameter("fromAction")
              + " -> "
              + proposal.parameter("toAction");
      case DELETE_WORKFLOW_HOP ->
          "DELETE_WORKFLOW_HOP: "
              + proposal.parameter("fromAction")
              + " -> "
              + proposal.parameter("toAction");
      case SET_ACTION_LOCATION ->
          "SET_ACTION_LOCATION: "
              + proposal.parameter("actionName")
              + " @ "
              + proposal.parameter("locationX")
              + ","
              + proposal.parameter("locationY");
      case ADD_WORKFLOW_NOTE -> "ADD_WORKFLOW_NOTE: " + truncate(proposal.parameter("text"), 120);
    };
  }

  private static String truncate(String value, int max) {
    if (value == null) {
      return "";
    }
    if (value.length() <= max) {
      return value;
    }
    return value.substring(0, max) + "...";
  }
}
