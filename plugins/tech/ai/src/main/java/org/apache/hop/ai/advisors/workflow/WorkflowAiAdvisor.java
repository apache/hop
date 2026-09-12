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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.workflow.WorkflowMeta;

@AiAdvisorPlugin(
    id = WorkflowAiAdvisor.ID,
    name = "i18n::WorkflowAiAdvisor.Name",
    description = "i18n::WorkflowAiAdvisor.Description",
    image = "ai-provider.svg",
    locations = {AiAdvisorLocations.WORKFLOW_GRAPH},
    classLoaderGroup = "hop-ai")
public class WorkflowAiAdvisor implements IAiAdvisor {

  public static final String ID = "workflow-advisor";
  private static final Class<?> PKG = WorkflowAiAdvisor.class;

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public String getName() {
    return BaseMessages.getString(PKG, "WorkflowAiAdvisor.Name");
  }

  @Override
  public String[] getLocations() {
    return new String[] {AiAdvisorLocations.WORKFLOW_GRAPH};
  }

  @Override
  public List<AiAdvisorScenario> listScenarios() {
    return List.of(
        new AiAdvisorScenario(
            "workflow-general",
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Scenario.General.Label"),
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Scenario.General.Description")),
        new AiAdvisorScenario(
            "action-selection",
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Scenario.ActionSelection.Label"),
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Scenario.ActionSelection.Description")),
        new AiAdvisorScenario(
            "workflow-error-diagnosis",
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Scenario.ErrorDiagnosis.Label"),
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Scenario.ErrorDiagnosis.Description")),
        new AiAdvisorScenario(
            "workflow-design",
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Scenario.Design.Label"),
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Scenario.Design.Description")));
  }

  @Override
  public List<AiAdvisorInclusion> listInclusions() {
    return List.of(
        new AiAdvisorInclusion(
            AiAdvisorInclusions.CHECKS,
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Inclusion.Checks"),
            false,
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Inclusion.Checks.Tooltip"),
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Inclusion.Checks.Summary")),
        new AiAdvisorInclusion(
            AiAdvisorInclusions.CATALOG,
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Inclusion.Catalog"),
            false,
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Inclusion.Catalog.Tooltip"),
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Inclusion.Catalog.Summary")),
        new AiAdvisorInclusion(
            AiAdvisorInclusions.XML,
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Inclusion.Xml"),
            false,
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Inclusion.Xml.Tooltip"),
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Inclusion.Xml.Summary")),
        new AiAdvisorInclusion(
            AiAdvisorInclusions.LOGS,
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Inclusion.Logs"),
            false,
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Inclusion.Logs.Tooltip"),
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Inclusion.Logs.Summary")),
        new AiAdvisorInclusion(
            AiAdvisorInclusions.METADATA,
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Inclusion.Metadata"),
            false,
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Inclusion.Metadata.Tooltip"),
            BaseMessages.getString(PKG, "WorkflowAiAdvisor.Inclusion.Metadata.Summary")));
  }

  @Override
  public List<String> listBaselineSharing() {
    return List.of(BaseMessages.getString(PKG, "WorkflowAiAdvisor.Sharing.Baseline"));
  }

  @Override
  public AiAdvisorPrompt buildPrompt(AiAdvisorRequest request) throws HopException {
    return WorkflowAiContextBuilder.buildPrompt(request);
  }

  @Override
  public AiAdvisorResponse parseResponse(String raw) {
    return AiProposalParser.parse(raw);
  }

  @Override
  public List<AiProposalValidation> validateProposals(
      AiAdvisorRequest request, List<AiProposal> proposals) {
    WorkflowMeta workflowMeta =
        request != null && request.getArtifact() instanceof WorkflowMeta meta ? meta : null;
    return WorkflowAiProposalValidator.validate(workflowMeta, proposals);
  }

  @Override
  public void applyProposals(AiAdvisorRequest request, List<AiProposal> selected)
      throws HopException {
    if (!(request.getArtifact() instanceof WorkflowMeta workflowMeta)) {
      throw new HopException("No workflow is bound to this session.");
    }
    HopGui hopGui = hopGuiFrom(request);
    WorkflowAiProposalApplier.apply(workflowMeta, selected, hopGui);
  }

  private static HopGui hopGuiFrom(AiAdvisorRequest request) {
    if (request == null || request.getAttributes() == null) {
      return null;
    }
    Object value = request.getAttributes().get(AiM2PromptSupport.ATTR_HOP_GUI);
    return value instanceof HopGui hopGui ? hopGui : null;
  }
}
