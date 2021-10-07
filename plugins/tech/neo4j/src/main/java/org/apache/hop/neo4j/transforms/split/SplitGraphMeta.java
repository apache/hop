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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

@Transform(
    id = "Neo4jSplitGraph",
    name = "i18n::SplitGraphMeta.name",
    description = "i18n::SplitGraphMeta.description",
    image = "neo4j_split.svg",
    categoryDescription = "i18n::SplitGraphMeta.categoryDescription",
        keywords = "i18n::SplitGraphMeta.keyword",
    documentationUrl = "/plugins/transforms/split-graph.html")
public class SplitGraphMeta extends BaseTransformMeta
    implements ITransformMeta<SplitGraph, SplitGraphData> {

  public static final String GRAPH_FIELD = "graph_field";
  public static final String TYPE_FIELD = "type_field";
  public static final String ID_FIELD = "id_field";
  public static final String PROPERTY_SET_FIELD = "property_set_field";

  protected String graphField;
  protected String typeField;
  protected String idField;
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

  @Override
  public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer();
    xml.append(XmlHandler.addTagValue(GRAPH_FIELD, graphField));
    xml.append(XmlHandler.addTagValue(TYPE_FIELD, typeField));
    xml.append(XmlHandler.addTagValue(ID_FIELD, idField));
    xml.append(XmlHandler.addTagValue(PROPERTY_SET_FIELD, propertySetField));
    return xml.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {
    graphField = XmlHandler.getTagValue(transformNode, GRAPH_FIELD);
    typeField = XmlHandler.getTagValue(transformNode, TYPE_FIELD);
    idField = XmlHandler.getTagValue(transformNode, ID_FIELD);
    propertySetField = XmlHandler.getTagValue(transformNode, PROPERTY_SET_FIELD);
  }

  @Override
  public SplitGraph createTransform(
      TransformMeta transformMeta,
      SplitGraphData iTransformData,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new SplitGraph(transformMeta, this, iTransformData, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public SplitGraphData getTransformData() {
    return new SplitGraphData();
  }

  /**
   * Gets graphField
   *
   * @return value of graphField
   */
  public String getGraphField() {
    return graphField;
  }

  /** @param graphField The graphField to set */
  public void setGraphField(String graphField) {
    this.graphField = graphField;
  }

  /**
   * Gets typeField
   *
   * @return value of typeField
   */
  public String getTypeField() {
    return typeField;
  }

  /** @param typeField The typeField to set */
  public void setTypeField(String typeField) {
    this.typeField = typeField;
  }

  /**
   * Gets idField
   *
   * @return value of idField
   */
  public String getIdField() {
    return idField;
  }

  /** @param idField The idField to set */
  public void setIdField(String idField) {
    this.idField = idField;
  }

  /**
   * Gets propertySetField
   *
   * @return value of propertySetField
   */
  public String getPropertySetField() {
    return propertySetField;
  }

  /** @param propertySetField The propertySetField to set */
  public void setPropertySetField(String propertySetField) {
    this.propertySetField = propertySetField;
  }
}
