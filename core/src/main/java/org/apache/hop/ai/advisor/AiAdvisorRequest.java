/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ai.advisor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;

/**
 * One advisory turn. {@link #artifact} is the open pipeline, workflow, or other model; the advisor
 * knows the concrete type for its context.
 */
@Getter
@Setter
public class AiAdvisorRequest {
  /** Free-form location id from {@link AiAdvisorOpenRequest#location}. */
  private String location;

  private String scenarioId;
  private String userPrompt;
  private String focusNodeName;
  private String logExcerpt;
  private String aiProviderName;
  private IVariables variables;
  private IHopMetadataProvider metadataProvider;
  private Object artifact;
  private Map<String, Boolean> inclusions = new LinkedHashMap<>();
  private Map<String, Object> attributes = new LinkedHashMap<>();
  private List<AiAdvisorMetadataSelection> metadataSelections = new ArrayList<>();

  /** inclusion id → selected {@link AiAdvisorInclusionChoice} ids (order preserved). */
  private Map<String, List<String>> inclusionSelections = new LinkedHashMap<>();

  /** True when this is not the first turn of the session. */
  private boolean followUp;

  /** Summaries of graph edits the user applied since the previous turn. */
  private List<String> appliedChangeSummaries = new ArrayList<>();

  public boolean inclusionEnabled(String id) {
    return Boolean.TRUE.equals(inclusions.get(id));
  }

  public List<String> selectedInclusionIds(String inclusionId) {
    if (inclusionSelections == null || inclusionId == null) {
      return List.of();
    }
    List<String> selected = inclusionSelections.get(inclusionId);
    return selected == null ? List.of() : List.copyOf(selected);
  }
}
