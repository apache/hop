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

import java.util.List;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.core.util.Utils;
import org.apache.hop.ui.core.gui.GuiResource;

/**
 * Copies CLIPBOARD_* proposal payloads to the system clipboard. Does not mutate the graph; the user
 * pastes (Ctrl-V) on the canvas.
 */
public final class AiClipboardProposals {

  private AiClipboardProposals() {}

  /**
   * @return 1 if something was copied, 0 otherwise. Last clipboard payload of the preferred type
   *     wins (transforms, then actions, then metadata JSON).
   */
  public static int copy(List<AiProposal> selected) {
    String text = buildClipboardText(selected);
    if (Utils.isEmpty(text)) {
      return 0;
    }
    GuiResource.getInstance().toClipboard(text);
    return 1;
  }

  public static int clipboardCount(List<AiProposal> selected) {
    int count = 0;
    for (AiProposal proposal : selectedOrEmpty(selected)) {
      AiProposalTypes type = AiProposalTypes.of(proposal);
      if (type != null && type.isClipboardType()) {
        count++;
      }
    }
    return count;
  }

  public static String buildClipboardText(List<AiProposal> selected) {
    String lastTransform = null;
    String lastAction = null;
    String lastMetadata = null;
    for (AiProposal proposal : selectedOrEmpty(selected)) {
      AiProposalTypes type = AiProposalTypes.of(proposal);
      if (type == null) {
        continue;
      }
      switch (type) {
        case CLIPBOARD_TRANSFORMS -> {
          String xml = AiProposalXmlSupport.xmlParam(proposal);
          if (!Utils.isEmpty(xml)) {
            lastTransform = xml;
          }
        }
        case CLIPBOARD_ACTIONS -> {
          String xml = AiProposalXmlSupport.xmlParam(proposal);
          if (!Utils.isEmpty(xml)) {
            lastAction = xml;
          }
        }
        case CLIPBOARD_METADATA -> {
          String json = proposal.parameter("json");
          if (!Utils.isEmpty(json)) {
            lastMetadata = json;
          }
        }
        default -> {
          // topology / replace types are not clipboard payloads
        }
      }
    }
    if (lastTransform != null) {
      return AiProposalXmlSupport.wrapPipelineClipboard(lastTransform);
    }
    if (lastAction != null) {
      return AiProposalXmlSupport.wrapWorkflowClipboard(lastAction);
    }
    return lastMetadata == null ? "" : lastMetadata;
  }

  private static List<AiProposal> selectedOrEmpty(List<AiProposal> selected) {
    return selected == null ? List.of() : selected;
  }
}
