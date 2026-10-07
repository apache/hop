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
import org.apache.hop.ai.engine.AiMetadataProposalSupport;
import org.apache.hop.ai.engine.AiProposalParamSupport;
import org.apache.hop.ai.engine.AiProposalTypes;
import org.apache.hop.ai.engine.AiProposalXmlSupport;
import org.apache.hop.ai.engine.AiTransformConfigSupport;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/** Validates AI pipeline proposals against the open graph before the user applies them. */
public final class PipelineAiProposalValidator {

  private static final Class<?> PKG = PipelineAiProposalValidator.class;

  private PipelineAiProposalValidator() {}

  public static List<AiProposalValidation> validate(
      PipelineMeta pipelineMeta, List<AiProposal> proposals) {
    return validate(pipelineMeta, proposals, null);
  }

  public static List<AiProposalValidation> validate(
      PipelineMeta pipelineMeta,
      List<AiProposal> proposals,
      IHopMetadataProvider metadataProvider) {
    List<AiProposalValidation> results = new ArrayList<>();
    if (proposals == null) {
      return results;
    }
    Set<String> reservedNames = new HashSet<>();
    for (AiProposal proposal : proposals) {
      results.add(validateOne(pipelineMeta, proposal, reservedNames, metadataProvider));
    }
    return results;
  }

  private static AiProposalValidation validateOne(
      PipelineMeta pipelineMeta,
      AiProposal proposal,
      Set<String> reservedNames,
      IHopMetadataProvider metadataProvider) {
    AiProposalTypes type = AiProposalTypes.of(proposal);
    if (type == null) {
      return blocked(
          proposal,
          Utils.isEmpty(proposal.getType())
              ? BaseMessages.getString(PKG, "PipelineAiProposalValidator.NoType")
              : BaseMessages.getString(
                  PKG, "PipelineAiProposalValidator.UnknownType", proposal.getType()));
    }
    if (!type.isPipelineType()) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.NotOwnType", type));
    }
    if (pipelineMeta == null) {
      return blocked(proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.NoGraph"));
    }
    return switch (type) {
      case ADD_TRANSFORM -> validateAddTransform(pipelineMeta, proposal, reservedNames);
      case DELETE_TRANSFORM -> validateDeleteTransform(pipelineMeta, proposal);
      case RENAME_TRANSFORM -> validateRenameTransform(pipelineMeta, proposal, reservedNames);
      case ADD_PIPELINE_HOP -> validateAddPipelineHop(pipelineMeta, proposal, reservedNames);
      case DELETE_PIPELINE_HOP -> validateDeletePipelineHop(pipelineMeta, proposal);
      case SET_TRANSFORM_LOCATION -> validateSetTransformLocation(pipelineMeta, proposal);
      case ADD_PIPELINE_NOTE -> validateAddPipelineNote(proposal);
      case CONFIGURE_TRANSFORM -> validateConfigureTransform(pipelineMeta, proposal, reservedNames);
      case CLIPBOARD_TRANSFORMS -> validateClipboardTransforms(proposal);
      case REPLACE_TRANSFORM -> validateReplaceTransform(pipelineMeta, proposal);
      case CLIPBOARD_METADATA, SAVE_METADATA ->
          AiMetadataProposalSupport.validate(proposal, metadataProvider);
      default ->
          blocked(
              proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.UnsupportedType"));
    };
  }

  private static AiProposalValidation validateAddTransform(
      PipelineMeta pipelineMeta, AiProposal proposal, Set<String> reservedNames) {
    String pluginId = proposal.parameter("transformPluginId");
    String name = proposal.parameter("name");
    if (Utils.isEmpty(pluginId)) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.PluginIdRequired"));
    }
    if (Utils.isEmpty(name)) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.NameRequired"));
    }
    if (PluginRegistry.getInstance().findPluginWithId(TransformPluginType.class, pluginId)
        == null) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, "PipelineAiProposalValidator.UnknownPlugin", pluginId));
    }
    if (pipelineMeta.findTransform(name) != null || reservedNames.contains(name.trim())) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.NameExists", name));
    }
    if (!AiProposalParamSupport.parseLocation(proposal).isValid()) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.LocationNotIntegers"));
    }
    reservedNames.add(name.trim());
    String xml = AiProposalXmlSupport.xmlParam(proposal);
    if (!Utils.isEmpty(xml)) {
      String xmlError = AiProposalXmlSupport.validatePipelineXml(xml);
      if (xmlError != null) {
        return blocked(proposal, xmlError);
      }
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateConfigureTransform(
      PipelineMeta pipelineMeta, AiProposal proposal, Set<String> reservedNames) {
    String transformName = proposal.parameter("transformName");
    if (Utils.isEmpty(transformName)) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, "PipelineAiProposalValidator.TransformNameRequired"));
    }
    // A transform added earlier in the same list exists by the time this one is applied.
    if (pipelineMeta.findTransform(transformName) == null
        && !reservedNames.contains(transformName.trim())) {
      return blocked(
          proposal,
          BaseMessages.getString(
              PKG, "PipelineAiProposalValidator.TransformNotFound", transformName));
    }
    if (!AiTransformConfigSupport.hasConfig(proposal)) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.NoConfiguration"));
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateDeleteTransform(
      PipelineMeta pipelineMeta, AiProposal proposal) {
    String transformName = proposal.parameter("transformName");
    if (Utils.isEmpty(transformName)) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, "PipelineAiProposalValidator.TransformNameRequired"));
    }
    if (pipelineMeta.findTransform(transformName) == null) {
      return blocked(
          proposal,
          BaseMessages.getString(
              PKG, "PipelineAiProposalValidator.TransformNotFound", transformName));
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateRenameTransform(
      PipelineMeta pipelineMeta, AiProposal proposal, Set<String> reservedNames) {
    String transformName = proposal.parameter("transformName");
    String newName = proposal.parameter("newName");
    if (Utils.isEmpty(transformName)) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, "PipelineAiProposalValidator.TransformNameRequired"));
    }
    if (Utils.isEmpty(newName)) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.NewNameRequired"));
    }
    if (pipelineMeta.findTransform(transformName) == null) {
      return blocked(
          proposal,
          BaseMessages.getString(
              PKG, "PipelineAiProposalValidator.TransformNotFound", transformName));
    }
    if (!transformName.trim().equals(newName.trim())
        && (pipelineMeta.findTransform(newName) != null
            || reservedNames.contains(newName.trim()))) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.NameExists", newName));
    }
    reservedNames.add(newName.trim());
    return ok(proposal);
  }

  private static AiProposalValidation validateAddPipelineHop(
      PipelineMeta pipelineMeta, AiProposal proposal, Set<String> reservedNames) {
    String fromName = proposal.parameter("fromTransform");
    String toName = proposal.parameter("toTransform");
    if (Utils.isEmpty(fromName) || Utils.isEmpty(toName)) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.HopEndsRequired"));
    }
    if (!transformExists(pipelineMeta, fromName, reservedNames)) {
      return blocked(
          proposal,
          BaseMessages.getString(
              PKG, "PipelineAiProposalValidator.FromTransformNotFound", fromName));
    }
    if (!transformExists(pipelineMeta, toName, reservedNames)) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, "PipelineAiProposalValidator.ToTransformNotFound", toName));
    }
    TransformMeta from = pipelineMeta.findTransform(fromName);
    TransformMeta to = pipelineMeta.findTransform(toName);
    if (fromName.trim().equals(toName.trim())) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.HopToItself"));
    }
    // A pipeline cannot loop: the reverse hop, already there or proposed earlier, would.
    if ((from != null && to != null && pipelineMeta.findPipelineHop(to, from) != null)
        || reservedNames.contains("hop:" + toName + "->" + fromName)) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, "PipelineAiProposalValidator.HopLoop", toName, fromName));
    }
    reservedNames.add("hop:" + fromName + "->" + toName);
    if (from != null && to != null && pipelineMeta.findPipelineHop(from, to) != null) {
      return warning(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.HopExists"));
    }
    String enabled = proposal.parameter("enabled");
    if (!Utils.isEmpty(enabled) && !AiProposalParamSupport.isYesNo(enabled)) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.EnabledYesNo"));
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateDeletePipelineHop(
      PipelineMeta pipelineMeta, AiProposal proposal) {
    String fromName = proposal.parameter("fromTransform");
    String toName = proposal.parameter("toTransform");
    if (Utils.isEmpty(fromName) || Utils.isEmpty(toName)) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.HopEndsRequired"));
    }
    TransformMeta from = pipelineMeta.findTransform(fromName);
    TransformMeta to = pipelineMeta.findTransform(toName);
    if (from == null || to == null) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, "PipelineAiProposalValidator.HopEndpointsNotFound"));
    }
    if (pipelineMeta.findPipelineHop(from, to) == null) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.HopNotFound"));
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateSetTransformLocation(
      PipelineMeta pipelineMeta, AiProposal proposal) {
    String transformName = proposal.parameter("transformName");
    if (Utils.isEmpty(transformName)) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, "PipelineAiProposalValidator.TransformNameRequired"));
    }
    if (pipelineMeta.findTransform(transformName) == null) {
      return blocked(
          proposal,
          BaseMessages.getString(
              PKG, "PipelineAiProposalValidator.TransformNotFound", transformName));
    }
    if (!AiProposalParamSupport.parseLocation(proposal).isValid()) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.LocationNotIntegers"));
    }
    return ok(proposal);
  }

  private static AiProposalValidation validateClipboardTransforms(AiProposal proposal) {
    String xml = AiProposalXmlSupport.xmlParam(proposal);
    String error = AiProposalXmlSupport.validatePipelineXml(xml);
    if (error != null) {
      return blocked(proposal, error);
    }
    if (AiProposalXmlSupport.containsSecrets(xml)) {
      return warning(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.XmlSecrets"));
    }
    return warning(
        proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.ClipboardPaste"));
  }

  private static AiProposalValidation validateReplaceTransform(
      PipelineMeta pipelineMeta, AiProposal proposal) {
    String transformName = proposal.parameter("transformName");
    if (Utils.isEmpty(transformName)) {
      return blocked(
          proposal,
          BaseMessages.getString(PKG, "PipelineAiProposalValidator.TransformNameRequired"));
    }
    TransformMeta existing = pipelineMeta.findTransform(transformName);
    if (existing == null) {
      return blocked(
          proposal,
          BaseMessages.getString(
              PKG, "PipelineAiProposalValidator.TransformNotFound", transformName));
    }
    String xml = AiProposalXmlSupport.xmlParam(proposal);
    String error = AiProposalXmlSupport.validatePipelineXml(xml);
    if (error != null) {
      return blocked(proposal, error);
    }
    try {
      List<String> ids = AiProposalXmlSupport.transformPluginIds(xml);
      if (!ids.isEmpty()
          && !Utils.isEmpty(existing.getTransformPluginId())
          && !existing.getTransformPluginId().equals(ids.get(0))) {
        return blocked(
            proposal,
            BaseMessages.getString(
                PKG,
                "PipelineAiProposalValidator.PluginIdMismatch",
                ids.get(0),
                existing.getTransformPluginId()));
      }
    } catch (Exception e) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.InvalidXml"));
    }
    if (AiProposalXmlSupport.containsSecrets(xml)) {
      return warning(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.ReplaceSecrets"));
    }
    return warning(
        proposal,
        BaseMessages.getString(
            PKG, "PipelineAiProposalValidator.ReplaceConfiguration", transformName));
  }

  private static AiProposalValidation validateAddPipelineNote(AiProposal proposal) {
    if (Utils.isEmpty(proposal.parameter("text"))) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.TextRequired"));
    }
    if (!AiProposalParamSupport.parseLocation(proposal).isValid()) {
      return blocked(
          proposal, BaseMessages.getString(PKG, "PipelineAiProposalValidator.LocationNotIntegers"));
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
