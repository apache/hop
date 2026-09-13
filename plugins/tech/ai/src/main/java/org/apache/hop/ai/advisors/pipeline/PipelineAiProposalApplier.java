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
import java.util.List;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.ai.engine.AiProposalParamSupport;
import org.apache.hop.ai.engine.AiProposalTypes;
import org.apache.hop.ai.engine.AiProposalXmlSupport;
import org.apache.hop.ai.engine.AiTransformConfigSupport;
import org.apache.hop.ai.engine.AiTransformPluginSupport;
import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.hopgui.HopGui;

/** Previews and applies validated AI proposals to an open pipeline. */
public final class PipelineAiProposalApplier {

  private PipelineAiProposalApplier() {}

  public static void apply(PipelineMeta pipelineMeta, List<AiProposal> proposals)
      throws HopException {
    apply(pipelineMeta, proposals, null);
  }

  public static void apply(PipelineMeta pipelineMeta, List<AiProposal> proposals, HopGui hopGui)
      throws HopException {
    apply(
        pipelineMeta,
        proposals,
        hopGui,
        pipelineMeta != null ? pipelineMeta.getMetadataProvider() : null);
  }

  public static void apply(
      PipelineMeta pipelineMeta,
      List<AiProposal> proposals,
      HopGui hopGui,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    if (pipelineMeta == null) {
      throw new HopException("No pipeline is open");
    }
    if (proposals == null || proposals.isEmpty()) {
      return;
    }
    IHopMetadataProvider provider =
        metadataProvider != null ? metadataProvider : pipelineMeta.getMetadataProvider();
    for (int i = 0; i < proposals.size(); i++) {
      boolean chainUndo = hopGui != null && i < proposals.size() - 1;
      applyOne(pipelineMeta, proposals.get(i), hopGui, chainUndo, provider);
    }
  }

  private static void applyOne(
      PipelineMeta pipelineMeta,
      AiProposal proposal,
      HopGui hopGui,
      boolean chainUndo,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    AiProposalTypes type = AiProposalTypes.of(proposal);
    if (type == null || type.isWorkbenchOwned()) {
      return;
    }
    switch (type) {
      case ADD_TRANSFORM -> addTransform(pipelineMeta, proposal, hopGui, chainUndo);
      case DELETE_TRANSFORM -> deleteTransform(pipelineMeta, proposal, hopGui, chainUndo);
      case RENAME_TRANSFORM -> renameTransform(pipelineMeta, proposal, hopGui, chainUndo);
      case ADD_PIPELINE_HOP -> addPipelineHop(pipelineMeta, proposal, hopGui, chainUndo);
      case DELETE_PIPELINE_HOP -> deletePipelineHop(pipelineMeta, proposal, hopGui, chainUndo);
      case SET_TRANSFORM_LOCATION ->
          setTransformLocation(pipelineMeta, proposal, hopGui, chainUndo);
      case ADD_PIPELINE_NOTE -> addPipelineNote(pipelineMeta, proposal, hopGui, chainUndo);
      case CONFIGURE_TRANSFORM -> configureTransform(pipelineMeta, proposal, hopGui, chainUndo);
      case REPLACE_TRANSFORM ->
          replaceTransform(pipelineMeta, proposal, hopGui, chainUndo, metadataProvider);
      default -> throw new HopException("Unsupported proposal type: " + type);
    }
  }

  private static void addTransform(
      PipelineMeta pipelineMeta, AiProposal proposal, HopGui hopGui, boolean chainUndo)
      throws HopException {
    String pluginId = proposal.parameter("transformPluginId");
    String name = proposal.parameter("name");
    AiProposalParamSupport.Location location = AiProposalParamSupport.parseLocation(proposal);
    ITransformMeta meta;
    if (!Utils.isEmpty(AiProposalXmlSupport.xmlParam(proposal))) {
      TransformMeta parsed =
          AiProposalXmlSupport.parseFirstTransform(
              AiProposalXmlSupport.requireXml(proposal), pipelineMeta.getMetadataProvider());
      meta = parsed.getTransform();
      if (Utils.isEmpty(pluginId) && parsed.getTransformPluginId() != null) {
        pluginId = parsed.getTransformPluginId();
      }
    } else {
      meta = AiTransformPluginSupport.loadTransformMeta(pluginId);
    }
    AiTransformConfigSupport.apply(meta, proposal);
    TransformMeta transformMeta = AiTransformPluginSupport.newTransformMeta(pluginId, name, meta);
    transformMeta.setLocation(new Point(location.x(), location.y()));
    pipelineMeta.addTransform(transformMeta);
    if (hopGui != null) {
      hopGui.undoDelegate.addUndoNew(
          pipelineMeta,
          new TransformMeta[] {transformMeta},
          new int[] {pipelineMeta.indexOfTransform(transformMeta)},
          chainUndo);
    }
  }

  private static void deleteTransform(
      PipelineMeta pipelineMeta, AiProposal proposal, HopGui hopGui, boolean chainUndo)
      throws HopException {
    String transformName = proposal.parameter("transformName");
    TransformMeta transform = pipelineMeta.findTransform(transformName);
    if (transform == null) {
      throw new HopException("Transform not found: " + transformName);
    }
    List<PipelineHopMeta> hopsToRemove = new ArrayList<>();
    for (PipelineHopMeta hop : pipelineMeta.getPipelineHops()) {
      if ((hop.getFromTransform() != null && hop.getFromTransform().equals(transform))
          || (hop.getToTransform() != null && hop.getToTransform().equals(transform))) {
        hopsToRemove.add(hop);
      }
    }
    for (PipelineHopMeta hop : hopsToRemove) {
      int hopIndex = pipelineMeta.indexOfPipelineHop(hop);
      if (hopGui != null) {
        hopGui.undoDelegate.addUndoDelete(
            pipelineMeta, new PipelineHopMeta[] {hop}, new int[] {hopIndex}, chainUndo);
      }
      pipelineMeta.removePipelineHop(hop);
    }
    int transformIndex = pipelineMeta.indexOfTransform(transform);
    if (hopGui != null) {
      hopGui.undoDelegate.addUndoDelete(
          pipelineMeta, new TransformMeta[] {transform}, new int[] {transformIndex}, chainUndo);
    }
    pipelineMeta.removeTransform(transformIndex);
  }

  private static void renameTransform(
      PipelineMeta pipelineMeta, AiProposal proposal, HopGui hopGui, boolean chainUndo)
      throws HopException {
    TransformMeta transform = requireTransform(pipelineMeta, proposal.parameter("transformName"));
    TransformMeta before = (TransformMeta) transform.clone();
    transform.setName(proposal.parameter("newName"));
    if (hopGui != null) {
      hopGui.undoDelegate.addUndoChange(
          pipelineMeta,
          new TransformMeta[] {before},
          new TransformMeta[] {transform},
          new int[] {pipelineMeta.indexOfTransform(transform)},
          chainUndo);
    }
    pipelineMeta.clearCaches();
    pipelineMeta.setChanged();
  }

  private static void addPipelineHop(
      PipelineMeta pipelineMeta, AiProposal proposal, HopGui hopGui, boolean chainUndo)
      throws HopException {
    TransformMeta from = requireTransform(pipelineMeta, proposal.parameter("fromTransform"));
    TransformMeta to = requireTransform(pipelineMeta, proposal.parameter("toTransform"));
    PipelineHopMeta hop = new PipelineHopMeta(from, to);
    hop.setEnabled(AiProposalParamSupport.parseEnabled(proposal.parameter("enabled")));
    pipelineMeta.addPipelineHop(hop);
    if (hopGui != null) {
      hopGui.undoDelegate.addUndoNew(
          pipelineMeta,
          new PipelineHopMeta[] {hop},
          new int[] {pipelineMeta.indexOfPipelineHop(hop)},
          chainUndo);
    }
  }

  private static void deletePipelineHop(
      PipelineMeta pipelineMeta, AiProposal proposal, HopGui hopGui, boolean chainUndo)
      throws HopException {
    TransformMeta from = requireTransform(pipelineMeta, proposal.parameter("fromTransform"));
    TransformMeta to = requireTransform(pipelineMeta, proposal.parameter("toTransform"));
    PipelineHopMeta hop = pipelineMeta.findPipelineHop(from, to);
    if (hop == null) {
      throw new HopException("Hop not found: " + from.getName() + " -> " + to.getName());
    }
    int hopIndex = pipelineMeta.indexOfPipelineHop(hop);
    if (hopGui != null) {
      hopGui.undoDelegate.addUndoDelete(
          pipelineMeta, new PipelineHopMeta[] {hop}, new int[] {hopIndex}, chainUndo);
    }
    pipelineMeta.removePipelineHop(hop);
  }

  private static void setTransformLocation(
      PipelineMeta pipelineMeta, AiProposal proposal, HopGui hopGui, boolean chainUndo)
      throws HopException {
    TransformMeta transform = requireTransform(pipelineMeta, proposal.parameter("transformName"));
    AiProposalParamSupport.Location location = AiProposalParamSupport.parseLocation(proposal);
    Point previous = new Point(transform.getLocation().x, transform.getLocation().y);
    transform.setLocation(new Point(location.x(), location.y()));
    if (hopGui != null) {
      hopGui.undoDelegate.addUndoPosition(
          pipelineMeta,
          new TransformMeta[] {transform},
          new int[] {pipelineMeta.indexOfTransform(transform)},
          new Point[] {previous},
          new Point[] {transform.getLocation()},
          chainUndo);
    }
    pipelineMeta.setChanged();
  }

  private static void configureTransform(
      PipelineMeta pipelineMeta, AiProposal proposal, HopGui hopGui, boolean chainUndo)
      throws HopException {
    TransformMeta existing = requireTransform(pipelineMeta, proposal.parameter("transformName"));
    if (existing.getTransform() == null) {
      throw new HopException("Transform has no metadata: " + existing.getName());
    }
    TransformMeta before = (TransformMeta) existing.clone();
    AiTransformConfigSupport.apply(existing.getTransform(), proposal);
    if (hopGui != null) {
      hopGui.undoDelegate.addUndoChange(
          pipelineMeta,
          new TransformMeta[] {before},
          new TransformMeta[] {existing},
          new int[] {pipelineMeta.indexOfTransform(existing)},
          chainUndo);
    }
    pipelineMeta.clearCaches();
    pipelineMeta.setChanged();
  }

  private static void replaceTransform(
      PipelineMeta pipelineMeta,
      AiProposal proposal,
      HopGui hopGui,
      boolean chainUndo,
      IHopMetadataProvider metadataProvider)
      throws HopException {
    TransformMeta existing = requireTransform(pipelineMeta, proposal.parameter("transformName"));
    TransformMeta parsed =
        AiProposalXmlSupport.parseFirstTransform(
            AiProposalXmlSupport.requireXml(proposal), metadataProvider);
    TransformMeta before = (TransformMeta) existing.clone();
    Point location =
        existing.getLocation() == null
            ? null
            : new Point(existing.getLocation().x, existing.getLocation().y);
    String name = existing.getName();
    existing.replaceMeta(parsed);
    existing.setName(name);
    if (location != null) {
      existing.setLocation(location);
    }
    if (hopGui != null) {
      hopGui.undoDelegate.addUndoChange(
          pipelineMeta,
          new TransformMeta[] {before},
          new TransformMeta[] {existing},
          new int[] {pipelineMeta.indexOfTransform(existing)},
          chainUndo);
    }
    pipelineMeta.clearCaches();
    pipelineMeta.setChanged();
  }

  private static void addPipelineNote(
      PipelineMeta pipelineMeta, AiProposal proposal, HopGui hopGui, boolean chainUndo) {
    AiProposalParamSupport.Location location = AiProposalParamSupport.parseLocation(proposal);
    NotePadMeta note = new NotePadMeta();
    note.setNote(proposal.parameter("text").trim());
    note.setLocation(new Point(location.x(), location.y()));
    note.setWidth(
        AiProposalParamSupport.parseOptionalSize(proposal.parameter("width"), note.width));
    note.setHeight(
        AiProposalParamSupport.parseOptionalSize(proposal.parameter("height"), note.getHeight()));
    pipelineMeta.addNote(note);
    if (hopGui != null) {
      hopGui.undoDelegate.addUndoNew(
          pipelineMeta,
          new NotePadMeta[] {note},
          new int[] {pipelineMeta.indexOfNote(note)},
          chainUndo);
    }
  }

  private static TransformMeta requireTransform(PipelineMeta pipelineMeta, String name)
      throws HopException {
    TransformMeta transform = pipelineMeta.findTransform(name);
    if (transform == null) {
      throw new HopException("Transform not found: " + name);
    }
    return transform;
  }
}
