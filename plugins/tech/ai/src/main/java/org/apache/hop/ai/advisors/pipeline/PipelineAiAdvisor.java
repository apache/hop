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

import java.util.List;
import org.apache.hop.ai.advisor.AiAdvisorInclusion;
import org.apache.hop.ai.advisor.AiAdvisorLocations;
import org.apache.hop.ai.advisor.AiAdvisorPlugin;
import org.apache.hop.ai.advisor.AiAdvisorPrompt;
import org.apache.hop.ai.advisor.AiAdvisorRequest;
import org.apache.hop.ai.advisor.AiAdvisorResponse;
import org.apache.hop.ai.advisor.AiAdvisorScenario;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.advisor.AiProposalValidation;
import org.apache.hop.ai.advisor.IAiAdvisor;
import org.apache.hop.ai.advisors.AiAdvisorInclusions;
import org.apache.hop.ai.engine.AiM2PromptSupport;
import org.apache.hop.ai.engine.AiProposalParser;
import org.apache.hop.ai.engine.AiProposalPreview;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.hopgui.HopGui;

@AiAdvisorPlugin(
    id = PipelineAiAdvisor.ID,
    name = "i18n::PipelineAiAdvisor.Name",
    description = "i18n::PipelineAiAdvisor.Description",
    image = "ai-provider.svg",
    locations = {AiAdvisorLocations.PIPELINE_GRAPH},
    classLoaderGroup = "hop-ai")
public class PipelineAiAdvisor implements IAiAdvisor {

  public static final String ID = "pipeline-advisor";
  private static final Class<?> PKG = PipelineAiAdvisor.class;

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public String getName() {
    return BaseMessages.getString(PKG, "PipelineAiAdvisor.Name");
  }

  @Override
  public String[] getLocations() {
    return new String[] {AiAdvisorLocations.PIPELINE_GRAPH};
  }

  @Override
  public List<AiAdvisorScenario> listScenarios() {
    return List.of(
        new AiAdvisorScenario(
            "pipeline-general",
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Scenario.General.Label"),
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Scenario.General.Description")),
        new AiAdvisorScenario(
            "transform-selection",
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Scenario.TransformSelection.Label"),
            BaseMessages.getString(
                PKG, "PipelineAiAdvisor.Scenario.TransformSelection.Description")),
        new AiAdvisorScenario(
            "pipeline-error-diagnosis",
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Scenario.ErrorDiagnosis.Label"),
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Scenario.ErrorDiagnosis.Description")),
        new AiAdvisorScenario(
            "pipeline-design",
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Scenario.Design.Label"),
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Scenario.Design.Description")));
  }

  @Override
  public List<AiAdvisorInclusion> listInclusions() {
    return List.of(
        new AiAdvisorInclusion(
            AiAdvisorInclusions.CHECKS,
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Inclusion.Checks"),
            false,
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Inclusion.Checks.Tooltip"),
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Inclusion.Checks.Summary")),
        new AiAdvisorInclusion(
            AiAdvisorInclusions.CATALOG,
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Inclusion.Catalog"),
            false,
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Inclusion.Catalog.Tooltip"),
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Inclusion.Catalog.Summary")),
        new AiAdvisorInclusion(
            AiAdvisorInclusions.XML,
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Inclusion.Xml"),
            false,
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Inclusion.Xml.Tooltip"),
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Inclusion.Xml.Summary")),
        new AiAdvisorInclusion(
            AiAdvisorInclusions.LOGS,
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Inclusion.Logs"),
            false,
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Inclusion.Logs.Tooltip"),
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Inclusion.Logs.Summary")),
        new AiAdvisorInclusion(
            AiAdvisorInclusions.METADATA,
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Inclusion.Metadata"),
            false,
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Inclusion.Metadata.Tooltip"),
            BaseMessages.getString(PKG, "PipelineAiAdvisor.Inclusion.Metadata.Summary")));
  }

  @Override
  public List<String> listBaselineSharing() {
    return List.of(BaseMessages.getString(PKG, "PipelineAiAdvisor.Sharing.Baseline"));
  }

  @Override
  public AiAdvisorPrompt buildPrompt(AiAdvisorRequest request) throws HopException {
    return PipelineAiContextBuilder.buildPrompt(request);
  }

  @Override
  public AiAdvisorResponse parseResponse(String raw) {
    return AiProposalParser.parse(raw);
  }

  @Override
  public List<AiProposalValidation> validateProposals(
      AiAdvisorRequest request, List<AiProposal> proposals) {
    PipelineMeta pipelineMeta =
        request != null && request.getArtifact() instanceof PipelineMeta meta ? meta : null;
    return PipelineAiProposalValidator.validate(pipelineMeta, proposals);
  }

  @Override
  public void applyProposals(AiAdvisorRequest request, List<AiProposal> selected)
      throws HopException {
    if (!(request.getArtifact() instanceof PipelineMeta pipelineMeta)) {
      throw new HopException("No pipeline is bound to this session.");
    }
    HopGui hopGui = hopGuiFrom(request);
    PipelineAiProposalApplier.apply(pipelineMeta, selected, hopGui);
  }

  @Override
  public String summarizeApplied(AiProposal proposal) {
    return AiProposalPreview.appliedSummary(proposal);
  }

  private static HopGui hopGuiFrom(AiAdvisorRequest request) {
    if (request == null || request.getAttributes() == null) {
      return null;
    }
    Object value = request.getAttributes().get(AiM2PromptSupport.ATTR_HOP_GUI);
    return value instanceof HopGui hopGui ? hopGui : null;
  }
}
