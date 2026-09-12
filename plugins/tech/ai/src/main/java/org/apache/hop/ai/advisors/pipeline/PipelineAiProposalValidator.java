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

package org.apache.hop.ai.advisors.pipeline;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.advisor.AiProposalValidation;
import org.apache.hop.ai.engine.AiProposalParamSupport;
import org.apache.hop.ai.engine.AiProposalTypes;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Validates AI pipeline proposals against the open graph before the user applies them. */
public final class PipelineAiProposalValidator {

  private PipelineAiProposalValidator() {}

  public static List<AiProposalValidation> validate(
      PipelineMeta pipelineMeta, List<AiProposal> proposals) {
    List<AiProposalValidation> results = new ArrayList<>();
    if (proposals == null) {
      return results;
    }
    Set<String> reservedNames = new HashSet<>();
    for (AiProposal proposal : proposals) {
      results.add(validateOne(pipelineMeta, proposal, reservedNames));
    }
    return results;
  }

  private static AiProposalValidation validateOne(
      PipelineMeta pipelineMeta, AiProposal proposal, Set<String> reservedNames) {
    AiProposalTypes type = AiProposalTypes.of(proposal);
    if (type == null) {
      return blocked(proposal, "Missing or unknown proposal type");
    }
    if (!type.isPipelineType()) {
      return blocked(proposal, "Not a pipeline proposal type: " + type);
    }
    if (pipelineMeta == null) {
      return blocked(proposal, "No pipeline is open");
    }
    return switch (type) {
      case ADD_TRANSFORM -> validateAddTransform(pipelineMeta, proposal, reservedNames);
      case DELETE_TRANSFORM -> validateDeleteTransform(pipelineMeta, proposal);
      case RENAME_TRANSFORM -> validateRenameTransform(pipelineMeta, proposal, reservedNames);
      case ADD_PIPELINE_HOP -> validateAddPipelineHop(pipelineMeta, proposal, reservedNames);
      case DELETE_PIPELINE_HOP -> validateDeletePipelineHop(pipelineMeta, proposal);
      case SET_TRANSFORM_LOCATION -> validateSetTransformLocation(pipelineMeta, proposal);
      case ADD_PIPELINE_NOTE -> validateAddPipelineNote(proposal);
      default -> blocked(proposal, "Unsupported proposal type");
    };
  }

  private static AiProposalValidation validateAddTransform(
      PipelineMeta pipelineMeta, AiProposal proposal, Set<String> reservedNames) {
    String pluginId = proposal.parameter("transformPluginId");
    String name = proposal.parameter("name");
    if (Utils.isEmpty(pluginId)) {
      return blocked(proposal, "transformPluginId is required");
    }
    if (Utils.isEmpty(name)) {
      return blocked(proposal, "name is required");
    }
    if (PluginRegistry.getInstance().findPluginWithId(TransformPluginType.class, pluginId)
        == null) {
      return blocked(proposal, "Unknown transform plugin: " + pluginId);
    }
    if (pipelineMeta.findTransform(name) != null || reservedNames.contains(name.trim())) {
      return blocked(proposal, "Transform name already exists: " + name);
    }
    if (!AiProposalParamSupport.parseLocation(proposal).isValid()) {
      return blocked(proposal, "locationX and locationY must be integers");
    }
    reservedNames.add(name.trim());
    return ok(proposal);
  }

  private static AiProposalValidation validateDeleteTransform(
      PipelineMeta pipelineMeta, AiProposal proposal) {
    String transformName = proposal.parameter("transformName");
    if (Utils.isEmpty(transformName)) {
      return blocked(proposal, "transformName is required");
    }
    if (pipelineMeta.findTransform(transformName) == null) {
      return blocked(proposal, "Transform not found: " + transformName);
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateRenameTransform(
      PipelineMeta pipelineMeta, AiProposal proposal, Set<String> reservedNames) {
    String transformName = proposal.parameter("transformName");
    String newName = proposal.parameter("newName");
    if (Utils.isEmpty(transformName)) {
      return blocked(proposal, "transformName is required");
    }
    if (Utils.isEmpty(newName)) {
      return blocked(proposal, "newName is required");
    }
    if (pipelineMeta.findTransform(transformName) == null) {
      return blocked(proposal, "Transform not found: " + transformName);
    }
    if (!transformName.trim().equals(newName.trim())
        && (pipelineMeta.findTransform(newName) != null
            || reservedNames.contains(newName.trim()))) {
      return blocked(proposal, "Transform name already exists: " + newName);
    }
    reservedNames.add(newName.trim());
    return ok(proposal);
  }

  private static AiProposalValidation validateAddPipelineHop(
      PipelineMeta pipelineMeta, AiProposal proposal, Set<String> reservedNames) {
    String fromName = proposal.parameter("fromTransform");
    String toName = proposal.parameter("toTransform");
    if (Utils.isEmpty(fromName) || Utils.isEmpty(toName)) {
      return blocked(proposal, "fromTransform and toTransform are required");
    }
    if (!transformExists(pipelineMeta, fromName, reservedNames)) {
      return blocked(proposal, "From transform not found: " + fromName);
    }
    if (!transformExists(pipelineMeta, toName, reservedNames)) {
      return blocked(proposal, "To transform not found: " + toName);
    }
    TransformMeta from = pipelineMeta.findTransform(fromName);
    TransformMeta to = pipelineMeta.findTransform(toName);
    if (fromName.trim().equals(toName.trim())) {
      return blocked(proposal, "Hop cannot connect a transform to itself");
    }
    if (from != null && to != null && pipelineMeta.findPipelineHop(from, to) != null) {
      return warning(proposal, "Hop already exists");
    }
    String enabled = proposal.parameter("enabled");
    if (!Utils.isEmpty(enabled) && !AiProposalParamSupport.isYesNo(enabled)) {
      return blocked(proposal, "enabled must be Y or N");
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateDeletePipelineHop(
      PipelineMeta pipelineMeta, AiProposal proposal) {
    String fromName = proposal.parameter("fromTransform");
    String toName = proposal.parameter("toTransform");
    if (Utils.isEmpty(fromName) || Utils.isEmpty(toName)) {
      return blocked(proposal, "fromTransform and toTransform are required");
    }
    TransformMeta from = pipelineMeta.findTransform(fromName);
    TransformMeta to = pipelineMeta.findTransform(toName);
    if (from == null || to == null) {
      return blocked(proposal, "Hop endpoints not found");
    }
    if (pipelineMeta.findPipelineHop(from, to) == null) {
      return blocked(proposal, "Hop not found");
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateSetTransformLocation(
      PipelineMeta pipelineMeta, AiProposal proposal) {
    String transformName = proposal.parameter("transformName");
    if (Utils.isEmpty(transformName)) {
      return blocked(proposal, "transformName is required");
    }
    if (pipelineMeta.findTransform(transformName) == null) {
      return blocked(proposal, "Transform not found: " + transformName);
    }
    if (!AiProposalParamSupport.parseLocation(proposal).isValid()) {
      return blocked(proposal, "locationX and locationY must be integers");
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateAddPipelineNote(AiProposal proposal) {
    if (Utils.isEmpty(proposal.parameter("text"))) {
      return blocked(proposal, "text is required");
    }
    if (!AiProposalParamSupport.parseLocation(proposal).isValid()) {
      return blocked(proposal, "locationX and locationY must be integers");
    }
    return ok(proposal);
  }

  private static AiProposalValidation ok(AiProposal proposal) {
    return validation(proposal, false, null, null);
  }

  private static AiProposalValidation warning(AiProposal proposal, String message) {
    return validation(proposal, false, null, message);
  }

  private static AiProposalValidation blocked(AiProposal proposal, String message) {
    return validation(proposal, true, message, null);
  }

  private static AiProposalValidation validation(
      AiProposal proposal, boolean blocked, String reason, String warning) {
    AiProposalValidation result = new AiProposalValidation();
    result.setProposalId(proposal != null ? proposal.getId() : null);
    result.setBlocked(blocked);
    result.setReason(reason);
    result.setWarning(warning);
    return result;
  }

  private static boolean transformExists(
      PipelineMeta pipelineMeta, String name, Set<String> reservedNames) {
    return pipelineMeta.findTransform(name) != null
        || (reservedNames != null && reservedNames.contains(name.trim()));
  }
}
