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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.Setter;

/**
 * How hopper-edw and Hop GUI plugins ask the AI Assistant workbench to open or reuse a session.
 * Fire {@code HopExtensionPoint.HopGuiAiAdvisorOpenSession} with this payload so callers do not
 * compile against {@code hop-tech-ai}.
 */
@Getter
@Setter
public class AiAdvisorOpenRequest {
  private String advisorPluginId;
  private String title;

  /**
   * Free-form location id (pipeline graph, data-vault graph, lineage view, a metadata editor, …).
   * Use {@link AiAdvisorLocations} for Hop's own ids; other plugins define their own.
   */
  private String location = AiAdvisorLocations.PERSPECTIVE;

  /** Session-tree group, for example "Pipelines" or "Data Vault". */
  private String areaLabel = "";

  /**
   * When true, open a floating window so the subject (pipeline, vault model, …) stays visible.
   * Pipeline/workflow toolbar Help sets this; hopper-edw graph toolbars should too.
   */
  private boolean preferFloatingWindow;

  private String artifactName;
  private String artifactKind;
  private String focusNodeName;
  private Object artifact;
  private Supplier<String> logSupplier;
  private boolean reuseExisting = true;

  /**
   * Session-scoped extras copied onto {@link AiAdvisorRequest#attributes} for {@code
   * buildPrompt} / {@code applyProposals}. Do not put SWT objects here.
   */
  private Map<String, Object> attributes = new LinkedHashMap<>();
}
