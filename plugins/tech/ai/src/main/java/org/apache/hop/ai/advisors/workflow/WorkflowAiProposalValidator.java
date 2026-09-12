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

package org.apache.hop.ai.advisors.workflow;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.advisor.AiProposalValidation;
import org.apache.hop.ai.engine.AiProposalParamSupport;
import org.apache.hop.ai.engine.AiProposalTypes;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/** Validates AI workflow proposals against the open graph before the user applies them. */
public final class WorkflowAiProposalValidator {

  private WorkflowAiProposalValidator() {}

  public static List<AiProposalValidation> validate(
      WorkflowMeta workflowMeta, List<AiProposal> proposals) {
    List<AiProposalValidation> results = new ArrayList<>();
    if (proposals == null) {
      return results;
    }
    Set<String> reservedNames = new HashSet<>();
    for (AiProposal proposal : proposals) {
      results.add(validateOne(workflowMeta, proposal, reservedNames));
    }
    return results;
  }

  private static AiProposalValidation validateOne(
      WorkflowMeta workflowMeta, AiProposal proposal, Set<String> reservedNames) {
    AiProposalTypes type = AiProposalTypes.of(proposal);
    if (type == null) {
      return blocked(proposal, "Missing or unknown proposal type");
    }
    if (!type.isWorkflowType()) {
      return blocked(proposal, "Not a workflow proposal type: " + type);
    }
    if (workflowMeta == null) {
      return blocked(proposal, "No workflow is open");
    }
    return switch (type) {
      case ADD_ACTION -> validateAddAction(workflowMeta, proposal, reservedNames);
      case DELETE_ACTION -> validateDeleteAction(workflowMeta, proposal);
      case RENAME_ACTION -> validateRenameAction(workflowMeta, proposal, reservedNames);
      case ADD_WORKFLOW_HOP -> validateAddWorkflowHop(workflowMeta, proposal, reservedNames);
      case DELETE_WORKFLOW_HOP -> validateDeleteWorkflowHop(workflowMeta, proposal);
      case SET_ACTION_LOCATION -> validateSetActionLocation(workflowMeta, proposal);
      case ADD_WORKFLOW_NOTE -> validateAddWorkflowNote(proposal);
      default -> blocked(proposal, "Unsupported proposal type");
    };
  }

  private static AiProposalValidation validateAddAction(
      WorkflowMeta workflowMeta, AiProposal proposal, Set<String> reservedNames) {
    String pluginId = proposal.parameter("actionPluginId");
    String name = proposal.parameter("name");
    if (Utils.isEmpty(pluginId)) {
      return blocked(proposal, "actionPluginId is required");
    }
    if (Utils.isEmpty(name)) {
      return blocked(proposal, "name is required");
    }
    if (PluginRegistry.getInstance().findPluginWithId(ActionPluginType.class, pluginId) == null) {
      return blocked(proposal, "Unknown action plugin: " + pluginId);
    }
    if (workflowMeta.findAction(name) != null || reservedNames.contains(name.trim())) {
      return blocked(proposal, "Action name already exists: " + name);
    }
    if (!AiProposalParamSupport.parseLocation(proposal).isValid()) {
      return blocked(proposal, "locationX and locationY must be integers");
    }
    reservedNames.add(name.trim());
    return ok(proposal);
  }

  private static AiProposalValidation validateDeleteAction(
      WorkflowMeta workflowMeta, AiProposal proposal) {
    String actionName = proposal.parameter("actionName");
    if (Utils.isEmpty(actionName)) {
      return blocked(proposal, "actionName is required");
    }
    if (workflowMeta.findAction(actionName) == null) {
      return blocked(proposal, "Action not found: " + actionName);
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateRenameAction(
      WorkflowMeta workflowMeta, AiProposal proposal, Set<String> reservedNames) {
    String actionName = proposal.parameter("actionName");
    String newName = proposal.parameter("newName");
    if (Utils.isEmpty(actionName)) {
      return blocked(proposal, "actionName is required");
    }
    if (Utils.isEmpty(newName)) {
      return blocked(proposal, "newName is required");
    }
    if (workflowMeta.findAction(actionName) == null) {
      return blocked(proposal, "Action not found: " + actionName);
    }
    if (!actionName.trim().equals(newName.trim())
        && (workflowMeta.findAction(newName) != null || reservedNames.contains(newName.trim()))) {
      return blocked(proposal, "Action name already exists: " + newName);
    }
    reservedNames.add(newName.trim());
    return ok(proposal);
  }

  private static AiProposalValidation validateAddWorkflowHop(
      WorkflowMeta workflowMeta, AiProposal proposal, Set<String> reservedNames) {
    String fromName = proposal.parameter("fromAction");
    String toName = proposal.parameter("toAction");
    if (Utils.isEmpty(fromName) || Utils.isEmpty(toName)) {
      return blocked(proposal, "fromAction and toAction are required");
    }
    if (!actionExists(workflowMeta, fromName, reservedNames)) {
      return blocked(proposal, "From action not found: " + fromName);
    }
    if (!actionExists(workflowMeta, toName, reservedNames)) {
      return blocked(proposal, "To action not found: " + toName);
    }
    ActionMeta from = workflowMeta.findAction(fromName);
    ActionMeta to = workflowMeta.findAction(toName);
    if (fromName.trim().equals(toName.trim())) {
      return blocked(proposal, "Hop cannot connect an action to itself");
    }
    if (from != null && to != null && workflowMeta.findWorkflowHop(from, to) != null) {
      return warning(proposal, "Hop already exists");
    }
    if (!Utils.isEmpty(proposal.parameter("unconditional"))
        && !AiProposalParamSupport.isYesNo(proposal.parameter("unconditional"))) {
      return blocked(proposal, "unconditional must be Y or N");
    }
    if (!Utils.isEmpty(proposal.parameter("evaluation"))
        && !AiProposalParamSupport.isYesNo(proposal.parameter("evaluation"))) {
      return blocked(proposal, "evaluation must be Y or N");
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateDeleteWorkflowHop(
      WorkflowMeta workflowMeta, AiProposal proposal) {
    String fromName = proposal.parameter("fromAction");
    String toName = proposal.parameter("toAction");
    if (Utils.isEmpty(fromName) || Utils.isEmpty(toName)) {
      return blocked(proposal, "fromAction and toAction are required");
    }
    ActionMeta from = workflowMeta.findAction(fromName);
    ActionMeta to = workflowMeta.findAction(toName);
    if (from == null || to == null) {
      return blocked(proposal, "Hop endpoints not found");
    }
    if (workflowMeta.findWorkflowHop(from, to) == null) {
      return blocked(proposal, "Hop not found");
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateSetActionLocation(
      WorkflowMeta workflowMeta, AiProposal proposal) {
    String actionName = proposal.parameter("actionName");
    if (Utils.isEmpty(actionName)) {
      return blocked(proposal, "actionName is required");
    }
    if (workflowMeta.findAction(actionName) == null) {
      return blocked(proposal, "Action not found: " + actionName);
    }
    if (!AiProposalParamSupport.parseLocation(proposal).isValid()) {
      return blocked(proposal, "locationX and locationY must be integers");
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateAddWorkflowNote(AiProposal proposal) {
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

  private static boolean actionExists(
      WorkflowMeta workflowMeta, String name, Set<String> reservedNames) {
    return workflowMeta.findAction(name) != null
        || (reservedNames != null && reservedNames.contains(name.trim()));
  }
}
