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
 */

package org.apache.hop.neo4j.transforms.split;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
@Transform(
    id = "Neo4jSplitGraph",
    name = "i18n::SplitGraphMeta.name",
    description = "i18n::SplitGraphMeta.description",
    image = "neo4j_split.svg",
    categoryDescription = "i18n::SplitGraphMeta.categoryDescription",
    keywords = "i18n::SplitGraphMeta.keyword",
    documentationUrl = "/plugins/transforms/split-graph.html")
public class SplitGraphMeta extends BaseTransformMeta<SplitGraph, SplitGraphData> {

  @HopMetadataProperty(key = "graph_field")
  protected String graphField;

  @HopMetadataProperty(key = "type_field")
  protected String typeField;

  @HopMetadataProperty(key = "id_field")
  protected String idField;

  @HopMetadataProperty(key = "property_set_field")
  protected String propertySetField;

  @Override
  public void setDefault() {
    graphField = "graph";
    typeField = "type";
    idField = "id";
    propertySetField = "propertySet";
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextStep,
      IVariables space,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (StringUtils.isNotEmpty(typeField)) {
      ValueMetaString typeValueMeta = new ValueMetaString(space.resolve(typeField));
      typeValueMeta.setOrigin(name);
      inputRowMeta.addValueMeta(typeValueMeta);
    }
    if (StringUtils.isNotEmpty(idField)) {
      ValueMetaString idValueMeta = new ValueMetaString(space.resolve(idField));
      idValueMeta.setOrigin(name);
      inputRowMeta.addValueMeta(idValueMeta);
    }
    if (StringUtils.isNotEmpty(propertySetField)) {
      ValueMetaString propertySetValueMeta = new ValueMetaString(space.resolve(propertySetField));
      propertySetValueMeta.setOrigin(name);
      inputRowMeta.addValueMeta(propertySetValueMeta);
    }
  }
}
