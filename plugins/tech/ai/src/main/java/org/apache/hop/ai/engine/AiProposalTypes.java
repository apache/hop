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
import java.util.List;
import java.util.Set;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.core.util.Utils;

/** Supported {@code hop_proposals} type names for pipeline and workflow advisors. */
public enum AiProposalTypes {
  ADD_TRANSFORM,
  DELETE_TRANSFORM,
  RENAME_TRANSFORM,
  ADD_PIPELINE_HOP,
  DELETE_PIPELINE_HOP,
  SET_TRANSFORM_LOCATION,
  ADD_PIPELINE_NOTE,
  CONFIGURE_TRANSFORM,
  CLIPBOARD_TRANSFORMS,
  REPLACE_TRANSFORM,
  ADD_ACTION,
  DELETE_ACTION,
  RENAME_ACTION,
  ADD_WORKFLOW_HOP,
  DELETE_WORKFLOW_HOP,
  SET_ACTION_LOCATION,
  ADD_WORKFLOW_NOTE,
  CONFIGURE_ACTION,
  CLIPBOARD_ACTIONS,
  REPLACE_ACTION,
  CLIPBOARD_METADATA,
  SAVE_METADATA;

  private static final Set<AiProposalTypes> PIPELINE_TYPES =
      Set.of(
          ADD_TRANSFORM,
          DELETE_TRANSFORM,
          RENAME_TRANSFORM,
          ADD_PIPELINE_HOP,
          DELETE_PIPELINE_HOP,
          SET_TRANSFORM_LOCATION,
          ADD_PIPELINE_NOTE,
          CONFIGURE_TRANSFORM,
          CLIPBOARD_TRANSFORMS,
          REPLACE_TRANSFORM,
          CLIPBOARD_METADATA,
          SAVE_METADATA);

  private static final Set<AiProposalTypes> WORKFLOW_TYPES =
      Set.of(
          ADD_ACTION,
          DELETE_ACTION,
          RENAME_ACTION,
          ADD_WORKFLOW_HOP,
          DELETE_WORKFLOW_HOP,
          SET_ACTION_LOCATION,
          ADD_WORKFLOW_NOTE,
          CONFIGURE_ACTION,
          CLIPBOARD_ACTIONS,
          REPLACE_ACTION,
          CLIPBOARD_METADATA,
          SAVE_METADATA);

  public boolean isPipelineType() {
    return PIPELINE_TYPES.contains(this);
  }

  public boolean isWorkflowType() {
    return WORKFLOW_TYPES.contains(this);
  }

  public boolean isClipboardType() {
    return this == CLIPBOARD_TRANSFORMS || this == CLIPBOARD_ACTIONS || this == CLIPBOARD_METADATA;
  }

  public boolean isMetadataType() {
    return this == CLIPBOARD_METADATA || this == SAVE_METADATA;
  }

  /**
   * Proposals that remove, replace or overwrite existing work. The review leaves them unselected so
   * the user has to choose them.
   */
  public boolean isOptIn() {
    return this == DELETE_TRANSFORM
        || this == DELETE_PIPELINE_HOP
        || this == REPLACE_TRANSFORM
        || this == CONFIGURE_TRANSFORM
        || this == DELETE_ACTION
        || this == DELETE_WORKFLOW_HOP
        || this == REPLACE_ACTION
        || this == CONFIGURE_ACTION;
  }

  /**
   * Types the workbench copies or saves after {@code applyProposals}. Pipeline/workflow appliers
   * skip these so mixed selections do not throw.
   */
  public boolean isWorkbenchOwned() {
    return isClipboardType() || this == SAVE_METADATA;
  }

  /**
   * The proposals in the order to apply them: hop deletes first, the rest in the order given.
   * Deleting a transform or action also removes its hops, so a hop delete after it would fail on a
   * hop that is already gone; and a hop that reverses a deleted one is only valid once that one is
   * gone. The validator checks the batch in this order too.
   */
  public static List<AiProposal> inApplyOrder(List<AiProposal> proposals) {
    List<AiProposal> hopDeletes = new ArrayList<>();
    List<AiProposal> rest = new ArrayList<>();
    for (AiProposal proposal : proposals) {
      AiProposalTypes type = of(proposal);
      if (type == DELETE_PIPELINE_HOP || type == DELETE_WORKFLOW_HOP) {
        hopDeletes.add(proposal);
      } else {
        rest.add(proposal);
      }
    }
    hopDeletes.addAll(rest);
    return hopDeletes;
  }

  public static AiProposalTypes of(AiProposal proposal) {
    if (proposal == null || Utils.isEmpty(proposal.getType())) {
      return null;
    }
    try {
      return valueOf(proposal.getType().trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
