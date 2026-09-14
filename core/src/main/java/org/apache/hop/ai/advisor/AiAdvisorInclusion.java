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

package org.apache.hop.ai.advisor;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * One optional piece of context the workbench can send. Advisors list these via {@link
 * IAiAdvisor#listInclusions()}. Sensitive extras should keep {@link #defaultSelected} {@code
 * false}. A plugin-id catalog may default on.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AiAdvisorInclusion {
  private String id;
  private String label;
  private boolean defaultSelected;

  /** Shown as the checkbox tooltip. Explains cost and sensitivity of this inclusion. */
  private String description;

  /**
   * Short phrase for the collapsed "Sharing: …" line (for example {@code check results}). When
   * empty, the workbench uses {@link #label}.
   */
  private String summary;

  /** When true, the workbench shows a Select… button next to the checkbox. */
  private boolean picker;

  /** When {@link #picker} is true, allow multiple choices. Default true. */
  private boolean multiSelect = true;

  public AiAdvisorInclusion(String id, String label, boolean defaultSelected) {
    this(id, label, defaultSelected, null, null, false, true);
  }

  public AiAdvisorInclusion(String id, String label, boolean defaultSelected, String description) {
    this(id, label, defaultSelected, description, null, false, true);
  }

  public AiAdvisorInclusion(
      String id, String label, boolean defaultSelected, String description, String summary) {
    this(id, label, defaultSelected, description, summary, false, true);
  }
}
