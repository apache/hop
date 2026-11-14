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
package org.apache.hop.pipeline.transforms.language;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiTableColumn;
import org.apache.hop.core.gui.plugin.GuiTableColumnType;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** One language the Detect language transform may answer. */
@Getter
@Setter
public class DetectLanguageChoice {

  /** The Lingua language name, for example ENGLISH. A variable is allowed. */
  @HopMetadataProperty(
      key = "name",
      injectionKey = "LANGUAGE",
      injectionKeyDescription = "DetectLanguage.Injection.Language")
  @GuiTableColumn(
      id = "name",
      order = "10",
      type = GuiTableColumnType.COMBO,
      comboValuesMethod = "languageNames",
      label = "i18n::DetectLanguage.Column.Language.Label",
      variables = true,
      width = 250)
  private String name = "";

  public DetectLanguageChoice() {}

  public DetectLanguageChoice(String name) {
    this.name = name;
  }
}
