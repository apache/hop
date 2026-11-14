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

package org.apache.hop.pipeline.transforms.maskfields;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiTableColumn;
import org.apache.hop.core.gui.plugin.GuiTableColumnType;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * A tag or glossary term in a data catalog, and the masking pattern for every field the catalog
 * labels with it.
 */
@Getter
@Setter
public class MaskTagRule {

  // Injection keys of their own: "pattern" is the injection key of the Fields table
  @HopMetadataProperty(
      key = "tag",
      injectionKey = "tag_rule_tag",
      injectionKeyDescription = "MaskFields.Injection.Tag")
  @GuiTableColumn(
      id = "tag",
      order = "10",
      type = GuiTableColumnType.TEXT,
      label = "i18n::MaskFields.Column.Tag.Label",
      variables = true,
      width = 200)
  private String tag = "";

  @HopMetadataProperty(
      key = "pattern",
      injectionKey = "tag_rule_pattern",
      injectionKeyDescription = "MaskFields.Injection.TagPattern")
  @GuiTableColumn(
      id = "patternName",
      order = "20",
      type = GuiTableColumnType.COMBO,
      comboValuesMethod = "patternNames",
      label = "i18n::MaskFields.Column.Pattern.Label",
      variables = false,
      width = 240)
  private String patternName = "";

  public MaskTagRule() {}

  public MaskTagRule(String tag, String patternName) {
    this.tag = tag;
    this.patternName = patternName;
  }
}
