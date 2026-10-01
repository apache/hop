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

package org.apache.hop.core;

import lombok.Getter;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/**
 * Semantic type for Markdown notes on pipeline and workflow canvases. When a note is rendered as
 * Markdown, the type selects the system-owned visual style (colors, border, accent icon) so notes
 * look consistent across platforms and projects.
 */
@Getter
public enum NotePadType implements IEnumHasCodeAndDescription {
  GENERAL("GENERAL", "General"),
  INFORMATION("INFORMATION", "Information"),
  IMPORTANT("IMPORTANT", "Important"),
  WARNING("WARNING", "Warning");

  private final String code;
  private final String description;

  NotePadType(String code, String description) {
    this.code = code;
    this.description = description;
  }

  public static String[] getDescriptions() {
    return IEnumHasCodeAndDescription.getDescriptions(NotePadType.class);
  }

  public static NotePadType lookupDescription(String description) {
    return IEnumHasCodeAndDescription.lookupDescription(NotePadType.class, description, GENERAL);
  }

  public static NotePadType lookupCode(String code) {
    return IEnumHasCode.lookupCode(NotePadType.class, code, GENERAL);
  }
}
