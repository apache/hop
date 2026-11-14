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
import org.apache.hop.ai.engine.AiMetadataProposalSupport;
import org.apache.hop.ai.engine.AiProposalParamSupport;
import org.apache.hop.ai.engine.AiProposalTypes;
import org.apache.hop.ai.engine.AiProposalXmlSupport;
import org.apache.hop.ai.engine.AiTransformConfigSupport;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

/** Validates AI workflow proposals against the open graph before the user applies them. */
public final class WorkflowAiProposalValidator {
  private static final String CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_LOCATION_NOT_INTEGERS =
      "WorkflowAiProposalValidator.LocationNotIntegers";
  private static final String CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_ACTION_NAME_REQUIRED =
      "WorkflowAiProposalValidator.ActionNameRequired";
  private static final String CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_ACTION_NOT_FOUND =
      "WorkflowAiProposalValidator.ActionNotFound";
  private static final String CONST_ACTION_NAME = "actionName";

  private static final Class<?> PKG = WorkflowAiProposalValidator.class;

  private WorkflowAiProposalValidator() {}

  public static List<AiProposalValidation> validate(
      WorkflowMeta workflowMeta, List<AiProposal> proposals) {
    return validate(workflowMeta, proposals, null);
  }

  public static List<AiProposalValidation> validate(
      WorkflowMeta workflowMeta,
      List<AiProposal> proposals,
      IHopMetadataProvider metadataProvider) {
    List<AiProposalValidation> results = new ArrayList<>();
    if (proposals == null) {
      return results;
    }
    Set<String> reservedNames = new HashSet<>();
    for (AiProposal proposal : proposals) {
      results.add(validateOne(workflowMeta, proposal, reservedNames, metadataProvider));
    }
    return results;
  }

  private static AiProposalValidation validateOne(
      WorkflowMeta workflowMeta,
      AiProposal proposal,
      Set<String> reservedNames,
      IHopMetadataProvider metadataProvider) {
    AiProposalTypes type = AiProposalTypes.of(proposal);
    if (type == null) {
      return blocked(
          proposal,
          Utils.isEmpty(proposal.getType())
              ? BaseMessages.getString(PKG, "WorkflowAiProposalValidator.NoType")
              : BaseMessages.getString(
                  PKG, "WorkflowAiProposalValidator.UnknownType", proposal.getType()));
    }
    if (!type.isWorkflowType()) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.NotOwnType", type));
    }
    if (workflowMeta == null) {
      return blocked(proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.NoGraph"));
    }
    return switch (type) {
      case ADD_ACTION -> validateAddAction(workflowMeta, proposal, reservedNames);
      case DELETE_ACTION -> validateDeleteAction(workflowMeta, proposal);
      case RENAME_ACTION -> validateRenameAction(workflowMeta, proposal, reservedNames);
      case ADD_WORKFLOW_HOP -> validateAddWorkflowHop(workflowMeta, proposal, reservedNames);
      case DELETE_WORKFLOW_HOP -> validateDeleteWorkflowHop(workflowMeta, proposal);
      case SET_ACTION_LOCATION -> validateSetActionLocation(workflowMeta, proposal);
      case ADD_WORKFLOW_NOTE -> validateAddWorkflowNote(proposal);
      case CONFIGURE_ACTION -> validateConfigureAction(workflowMeta, proposal, reservedNames);
      case CLIPBOARD_ACTIONS -> validateClipboardActions(proposal);
      case REPLACE_ACTION -> validateReplaceAction(workflowMeta, proposal);
      case CLIPBOARD_METADATA, SAVE_METADATA ->
          AiMetadataProposalSupport.validate(proposal, metadataProvider);
      default ->
          blocked(
              proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.UnsupportedType"));
    };
  }

  private static AiProposalValidation validateAddAction(
      WorkflowMeta workflowMeta, AiProposal proposal, Set<String> reservedNames) {
    String pluginId = proposal.parameter("actionPluginId");
    String name = proposal.parameter("name");
    if (Utils.isEmpty(pluginId)) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.PluginIdRequired"));
    }
    if (Utils.isEmpty(name)) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.NameRequired"));
    }
    if (PluginRegistry.getInstance().findPluginWithId(ActionPluginType.class, pluginId) == null) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, "WorkflowAiProposalValidator.UnknownPlugin", pluginId));
    }
    if (workflowMeta.findAction(name) != null || reservedNames.contains(name.trim())) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.NameExists", name));
    }
    if (!AiProposalParamSupport.parseLocation(proposal).isValid()) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_LOCATION_NOT_INTEGERS));
    }
    reservedNames.add(name.trim());
    return ok(proposal);
  }

  private static AiProposalValidation validateConfigureAction(
      WorkflowMeta workflowMeta, AiProposal proposal, Set<String> reservedNames) {
    String actionName = proposal.parameter(CONST_ACTION_NAME);
    if (Utils.isEmpty(actionName)) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_ACTION_NAME_REQUIRED));
    }
    // An action added earlier in the same list exists by the time this one is applied.
    if (workflowMeta.findAction(actionName) == null && !reservedNames.contains(actionName.trim())) {
      return blocked(
          proposal,
          BaseMessages.getString(
              PKG, CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_ACTION_NOT_FOUND, actionName));
    }
    if (!AiTransformConfigSupport.hasConfig(proposal)) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.NoConfiguration"));
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateDeleteAction(
      WorkflowMeta workflowMeta, AiProposal proposal) {
    String actionName = proposal.parameter(CONST_ACTION_NAME);
    if (Utils.isEmpty(actionName)) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_ACTION_NAME_REQUIRED));
    }
    if (workflowMeta.findAction(actionName) == null) {
      return blocked(
          proposal,
          BaseMessages.getString(
              PKG, CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_ACTION_NOT_FOUND, actionName));
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateRenameAction(
      WorkflowMeta workflowMeta, AiProposal proposal, Set<String> reservedNames) {
    String actionName = proposal.parameter(CONST_ACTION_NAME);
    String newName = proposal.parameter("newName");
    if (Utils.isEmpty(actionName)) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_ACTION_NAME_REQUIRED));
    }
    if (Utils.isEmpty(newName)) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.NewNameRequired"));
    }
    if (workflowMeta.findAction(actionName) == null) {
      return blocked(
          proposal,
          BaseMessages.getString(
              PKG, CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_ACTION_NOT_FOUND, actionName));
    }
    if (!actionName.trim().equals(newName.trim())
        && (workflowMeta.findAction(newName) != null || reservedNames.contains(newName.trim()))) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.NameExists", newName));
    }
    reservedNames.add(newName.trim());
    return ok(proposal);
  }

  private static AiProposalValidation validateAddWorkflowHop(
      WorkflowMeta workflowMeta, AiProposal proposal, Set<String> reservedNames) {
    String fromName = proposal.parameter("fromAction");
    String toName = proposal.parameter("toAction");
    if (Utils.isEmpty(fromName) || Utils.isEmpty(toName)) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.HopEndsRequired"));
    }
    if (!actionExists(workflowMeta, fromName, reservedNames)) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, "WorkflowAiProposalValidator.FromActionNotFound", fromName));
    }
    if (!actionExists(workflowMeta, toName, reservedNames)) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, "WorkflowAiProposalValidator.ToActionNotFound", toName));
    }
    ActionMeta from = workflowMeta.findAction(fromName);
    ActionMeta to = workflowMeta.findAction(toName);
    if (fromName.trim().equals(toName.trim())) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.HopToItself"));
    }
    if (from != null && to != null && workflowMeta.findWorkflowHop(from, to) != null) {
      return warning(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.HopExists"));
    }
    if (!Utils.isEmpty(proposal.parameter("unconditional"))
        && !AiProposalParamSupport.isYesNo(proposal.parameter("unconditional"))) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.UnconditionalYesNo"));
    }
    if (!Utils.isEmpty(proposal.parameter("evaluation"))
        && !AiProposalParamSupport.isYesNo(proposal.parameter("evaluation"))) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.EvaluationYesNo"));
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateDeleteWorkflowHop(
      WorkflowMeta workflowMeta, AiProposal proposal) {
    String fromName = proposal.parameter("fromAction");
    String toName = proposal.parameter("toAction");
    if (Utils.isEmpty(fromName) || Utils.isEmpty(toName)) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.HopEndsRequired"));
    }
    ActionMeta from = workflowMeta.findAction(fromName);
    ActionMeta to = workflowMeta.findAction(toName);
    if (from == null || to == null) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, "WorkflowAiProposalValidator.HopEndpointsNotFound"));
    }
    if (workflowMeta.findWorkflowHop(from, to) == null) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.HopNotFound"));
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateSetActionLocation(
      WorkflowMeta workflowMeta, AiProposal proposal) {
    String actionName = proposal.parameter(CONST_ACTION_NAME);
    if (Utils.isEmpty(actionName)) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_ACTION_NAME_REQUIRED));
    }
    if (workflowMeta.findAction(actionName) == null) {
      return blocked(
          proposal,
          BaseMessages.getString(
              PKG, CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_ACTION_NOT_FOUND, actionName));
    }
    if (!AiProposalParamSupport.parseLocation(proposal).isValid()) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_LOCATION_NOT_INTEGERS));
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateClipboardActions(AiProposal proposal) {
    String xml = AiProposalXmlSupport.xmlParam(proposal);
    String error = AiProposalXmlSupport.validateWorkflowXml(xml);
    if (error != null) {
      return blocked(proposal, error);
    }
    if (AiProposalXmlSupport.containsSecrets(xml)) {
      return warning(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.XmlSecrets"));
    }
    return warning(
        proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.ClipboardPaste"));
  }

  private static AiProposalValidation validateReplaceAction(
      WorkflowMeta workflowMeta, AiProposal proposal) {
    String actionName = proposal.parameter(CONST_ACTION_NAME);
    if (Utils.isEmpty(actionName)) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_ACTION_NAME_REQUIRED));
    }
    ActionMeta existing = workflowMeta.findAction(actionName);
    if (existing == null) {
      return blocked(
          proposal,
          BaseMessages.getString(
              PKG, CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_ACTION_NOT_FOUND, actionName));
    }
    String xml = AiProposalXmlSupport.xmlParam(proposal);
    String error = AiProposalXmlSupport.validateWorkflowXml(xml);
    if (error != null) {
      return blocked(proposal, error);
    }
    try {
      List<String> ids = AiProposalXmlSupport.actionPluginIds(xml);
      String existingId = existing.getAction() != null ? existing.getAction().getPluginId() : "";
      if (!ids.isEmpty() && !Utils.isEmpty(existingId) && !existingId.equals(ids.get(0))) {
        return blocked(
            proposal,
            BaseMessages.getString(
                PKG, "WorkflowAiProposalValidator.PluginIdMismatch", ids.get(0), existingId));
      }
    } catch (Exception e) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.InvalidXml"));
    }
    if (AiProposalXmlSupport.containsSecrets(xml)) {
      return warning(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.ReplaceSecrets"));
    }
    return warning(
        proposal,
        BaseMessages.getString(
            PKG, "WorkflowAiProposalValidator.ReplaceConfiguration", actionName));
  }

  private static AiProposalValidation validateAddWorkflowNote(AiProposal proposal) {
    if (Utils.isEmpty(proposal.parameter("text"))) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "WorkflowAiProposalValidator.TextRequired"));
    }
    if (!AiProposalParamSupport.parseLocation(proposal).isValid()) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, CONST_WORKFLOW_AI_PROPOSAL_VALIDATOR_LOCATION_NOT_INTEGERS));
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
