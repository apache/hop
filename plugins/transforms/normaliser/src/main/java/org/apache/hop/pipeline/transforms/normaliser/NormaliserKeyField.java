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

package org.apache.hop.pipeline.transforms.normaliser;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiTableColumn;
import org.apache.hop.core.gui.plugin.GuiTableColumnType;
import org.apache.hop.metadata.api.HopMetadataProperty;

/**
 * An input field that is kept as it is on every output row when no fields are listed and every
 * other input field is normalised.
 */
@Getter
@Setter
@EqualsAndHashCode
public class NormaliserKeyField {

  @GuiTableColumn(
      id = "name",
      order = "10",
      type = GuiTableColumnType.COMBO,
      label = "i18n::NormaliserDialog.ColumnInfo.Fieldname",
      variables = true,
      width = 200)
  @HopMetadataProperty(
      key = "name",
      injectionKey = "KEY_NAME",
      injectionKeyDescription = "NormaliserMeta.Injection.KEY_NAME")
  private String name;

  public NormaliserKeyField() {
    // Do nothing
  }

  public NormaliserKeyField(String name) {
    this.name = name;
  }
}
