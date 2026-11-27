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
package org.apache.hop.airflow.metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiTableColumn;
import org.apache.hop.core.gui.plugin.GuiTableColumnType;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** A parameter value that every run of an Airflow DAG gets, unless its conf sets it. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AirflowDagParameter {

  @GuiTableColumn(
      order = "010",
      label = "i18n::AirflowDag.Parameters.Name.Label",
      toolTip = "i18n::AirflowDag.Parameters.Name.Tooltip",
      type = GuiTableColumnType.TEXT,
      variables = false)
  @HopMetadataProperty
  private String name;

  @GuiTableColumn(
      order = "020",
      label = "i18n::AirflowDag.Parameters.Value.Label",
      toolTip = "i18n::AirflowDag.Parameters.Value.Tooltip",
      type = GuiTableColumnType.TEXT,
      variables = false)
  @HopMetadataProperty
  private String value;

  public AirflowDagParameter(AirflowDagParameter parameter) {
    this(parameter.name, parameter.value);
  }
}
