/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.core.variables.resolver;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
    key = "variable-resolver",
    name = "i18n::VariableResolver.name",
    description = "i18n::VariableResolver.Description",
    image = "ui/images/variable.svg",
    documentationUrl = "/metadata-types/variable-resolver/",
    hopMetadataPropertyType = HopMetadataPropertyType.RDBMS_CONNECTION)
@Getter
@Setter
public class VariableResolver extends HopMetadataBase implements IHopMetadata {
  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID =
      "VariableResolver-PluginSpecific-Options";

  @HopMetadataProperty private String description;

  @HopMetadataProperty(key = "variable-resolver")
  private IVariableResolver iResolver;

  public String getPluginName() {
    if (iResolver == null) {
      return null;
    }
    return iResolver.getPluginName();
  }
}
