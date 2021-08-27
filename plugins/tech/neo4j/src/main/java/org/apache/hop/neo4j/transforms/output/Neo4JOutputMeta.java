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

package org.apache.hop.neo4j.transforms.output;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.neo4j.core.value.ValueMetaGraph;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.eclipse.swt.widgets.Shell;
import org.w3c.dom.Node;

import java.util.List;

@Transform(
    id = "Neo4JOutput",
    image = "neo4j_output.svg",
    name = "i18n::Neo4JOutput.Transform.Name",
    description = "i18n::Neo4JOutput.Transform.Description",
    categoryDescription = "i18n::Neo4JOutput.Transform.Category",
    documentationUrl = "https://hop.apache.org/manual/latest/pipeline/transforms/neo4j-output.html")
@InjectionSupported(
    localizationPrefix = "Neo4JOutput.Injection.",
    groups = {"FROM_NODE_PROPS", "FROM_LABELS", "TO_NODE_PROPS", "TO_LABELS", "REL_PROPS"})
public class Neo4JOutputMeta extends BaseTransformMeta
    implements ITransformMeta<Neo4JOutput, Neo4JOutputData> {

  private static final String STRING_CONNECTION = "connection";
  private static final String STRING_BATCH_SIZE = "batch_size";
  private static final String STRING_KEY = "key";
  private static final String STRING_FROM = "from";
  private static final String STRING_LABELS = "labels";
  private static final String STRING_LABEL = "label";
  private static final String STRING_VALUE = "value";
  private static final String STRING_PROPERTIES = "properties";
  private static final String STRING_PROPERTY = "property";
  private static final String STRING_PROPERTY_NAME = "name";
  private static final String STRING_PROPERTY_VALUE = "value";
  private static final String STRING_PROPERTY_TYPE = "type";
  private static final String STRING_PROPERTY_PRIMARY = "primary";
  private static final String STRING_TO = "to";
  private static final String STRING_RELATIONSHIP = "relationship";
  private static final String STRING_RELATIONSHIP_VALUE = "relationship_value";
  private static final String STRING_RELPROPS = "relprops";
  private static final String STRING_RELPROP = "relprop";
  private static final String STRING_CREATE_INDEXES = "create_indexes";
  private static final String STRING_USE_CREATE = "use_create";
  private static final String STRING_ONLY_CREATE_RELATIONSHIPS = "only_create_relationships";
  private static final String STRING_READ_ONLY_FROM_NODE = "read_only_from_node";
  private static final String STRING_READ_ONLY_TO_NODE = "read_only_to_node";

  private static final String STRING_RETURNING_GRAPH = "returning_graph";
  private static final String STRING_RETURN_GRAPH_FIELD = "return_graph_field";

  @Injection(name = "CONNECTION")
  private String connection;

  @Injection(name = "BATCH_SIZE")
  private String batchSize;

  @Injection(name = "CREATE_INDEXES")
  private boolean creatingIndexes;

  @Injection(name = "USE_CREATE")
  private boolean usingCreate;

  @Injection(name = "ONLY_CREATE_RELATIONSHIPS")
  private boolean onlyCreatingRelationships;

  @Injection(name = "READ_ONLY_FROM_NODE")
  private boolean readOnlyFromNode;

  @Injection(name = "READ_ONLY_TO_NODE")
  private boolean readOnlyToNode;

  @Injection(name = "FROM_NODE_PROPERTY_FIELD", group = "FROM_NODE_PROPS")
  private String[] fromNodeProps;

  @Injection(name = "FROM_NODE_PROPERTY_NAME", group = "FROM_NODE_PROPS")
  private String[] fromNodePropNames;

  @Injection(name = "FROM_NODE_PROPERTY_TYPE", group = "FROM_NODE_PROPS")
  private String[] fromNodePropTypes;

  @Injection(name = "FROM_NODE_PROPERTY_PRIMARY", group = "FROM_NODE_PROPS")
  private boolean[] fromNodePropPrimary;

  @Injection(name = "TO_NODE_PROPERTY_FIELD", group = "TO_NODE_PROPS")
  private String[] toNodeProps;

  @Injection(name = "TO_NODE_PROPERTY_NAME", group = "TO_NODE_PROPS")
  private String[] toNodePropNames;

  @Injection(name = "TO_NODE_PROPERTY_TYPE", group = "TO_NODE_PROPS")
  private String[] toNodePropTypes;

  @Injection(name = "TO_NODE_PROPERTY_PRIMARY", group = "TO_NODE_PROPS")
  private boolean[] toNodePropPrimary;

  @Injection(name = "FROM_LABEL", group = "FROM_LABELS")
  private String[] fromNodeLabels;

  @Injection(name = "TO_LABEL", group = "TO_LABELS")
  private String[] toNodeLabels;

  @Injection(name = "FROM_LABEL_VALUE", group = "FROM_LABELS")
  private String[] fromNodeLabelValues;

  @Injection(name = "TO_LABEL_VALUE", group = "TO_LABELS")
  private String[] toNodeLabelValues;

  @Injection(name = "REL_PROPERTY_FIELD", group = "REL_PROPS")
  private String[] relProps;

  @Injection(name = "REL_PROPERTY_NAME", group = "REL_PROPS")
  private String[] relPropNames;

  @Injection(name = "REL_PROPERTY_TYPE", group = "REL_PROPS")
  private String[] relPropTypes;

  @Injection(name = "REL_LABEL")
  private String relationship;

  @Injection(name = "REL_VALUE")
  private String relationshipValue;

  @Injection(name = "RETURNING_GRAPH")
  private boolean returningGraph;

  @Injection(name = "RETURN_GRAPH_FIELD")
  private String returnGraphField;

  // Unused fields from previous version
  //
  private String key;

  public Neo4JOutput createTransform(
      TransformMeta transformMeta,
      Neo4JOutputData iTransformData,
      int cnr,
      PipelineMeta pipelineMeta,
      Pipeline disp) {
    return new Neo4JOutput(transformMeta, this, iTransformData, cnr, pipelineMeta, disp);
  }

  public Neo4JOutputData getTransformData() {
    return new Neo4JOutputData();
  }

  public ITransformDialog getDialog(
      Shell shell,
      IVariables variables,
      ITransformMeta meta,
      PipelineMeta pipelineMeta,
      String name) {
    return new Neo4JOutputDialog(shell, variables, meta, pipelineMeta, name);
  }

  public void setDefault() {
    connection = "";
    batchSize = "1000";
    creatingIndexes = true;
    usingCreate = false;
    onlyCreatingRelationships = false;

    fromNodeLabels = new String[0];
    fromNodeLabelValues = new String[0];
    fromNodeProps = new String[0];
    fromNodePropNames = new String[0];
    fromNodePropTypes = new String[0];
    fromNodePropPrimary = new boolean[0];

    toNodeLabels = new String[0];
    toNodeLabelValues = new String[0];
    toNodeProps = new String[0];
    toNodePropNames = new String[0];
    toNodePropTypes = new String[0];
    toNodePropPrimary = new boolean[0];

    relProps = new String[0];
    relPropNames = new String[0];
    relPropTypes = new String[0];

    returnGraphField = "graph";
  }

  public String getXml() throws HopException {
    StringBuffer xml = new StringBuffer();

    xml.append(XmlHandler.addTagValue(STRING_CONNECTION, connection));
    xml.append(XmlHandler.addTagValue(STRING_BATCH_SIZE, batchSize));
    xml.append(XmlHandler.addTagValue(STRING_KEY, key));
    xml.append(XmlHandler.addTagValue(STRING_CREATE_INDEXES, creatingIndexes));
    xml.append(XmlHandler.addTagValue(STRING_USE_CREATE, usingCreate));
    xml.append(XmlHandler.addTagValue(STRING_ONLY_CREATE_RELATIONSHIPS, onlyCreatingRelationships));
    xml.append(XmlHandler.addTagValue(STRING_RETURNING_GRAPH, returningGraph));
    xml.append(XmlHandler.addTagValue(STRING_RETURN_GRAPH_FIELD, returnGraphField));

    xml.append(XmlHandler.openTag(STRING_FROM));

    xml.append(XmlHandler.addTagValue(STRING_READ_ONLY_FROM_NODE, readOnlyFromNode));

    xml.append(XmlHandler.openTag(STRING_LABELS));
    for (int i = 0; i < fromNodeLabels.length; i++) {
      xml.append(XmlHandler.addTagValue(STRING_LABEL, fromNodeLabels[i]));
      xml.append(XmlHandler.addTagValue(STRING_VALUE, fromNodeLabelValues[i]));
    }
    xml.append(XmlHandler.closeTag(STRING_LABELS));

    xml.append(XmlHandler.openTag(STRING_PROPERTIES));
    for (int i = 0; i < fromNodeProps.length; i++) {
      xml.append(XmlHandler.openTag(STRING_PROPERTY));
      xml.append(XmlHandler.addTagValue(STRING_PROPERTY_NAME, fromNodePropNames[i]));
      xml.append(XmlHandler.addTagValue(STRING_PROPERTY_VALUE, fromNodeProps[i]));
      xml.append(XmlHandler.addTagValue(STRING_PROPERTY_TYPE, fromNodePropTypes[i]));
      xml.append(XmlHandler.addTagValue(STRING_PROPERTY_PRIMARY, fromNodePropPrimary[i]));
      xml.append(XmlHandler.closeTag(STRING_PROPERTY));
    }
    xml.append(XmlHandler.closeTag(STRING_PROPERTIES));
    xml.append(XmlHandler.closeTag(STRING_FROM));

    xml.append(XmlHandler.openTag(STRING_TO));

    xml.append(XmlHandler.addTagValue(STRING_READ_ONLY_TO_NODE, readOnlyToNode));

    xml.append(XmlHandler.openTag(STRING_LABELS));
    for (int i = 0; i < toNodeLabels.length; i++) {
      xml.append(XmlHandler.addTagValue(STRING_LABEL, toNodeLabels[i]));
      xml.append(XmlHandler.addTagValue(STRING_VALUE, toNodeLabelValues[i]));
    }
    xml.append(XmlHandler.closeTag(STRING_LABELS));

    xml.append(XmlHandler.openTag(STRING_PROPERTIES));
    for (int i = 0; i < toNodeProps.length; i++) {
      xml.append(XmlHandler.openTag(STRING_PROPERTY));
      xml.append(XmlHandler.addTagValue(STRING_PROPERTY_NAME, toNodePropNames[i]));
      xml.append(XmlHandler.addTagValue(STRING_PROPERTY_VALUE, toNodeProps[i]));
      xml.append(XmlHandler.addTagValue(STRING_PROPERTY_TYPE, toNodePropTypes[i]));
      xml.append(XmlHandler.addTagValue(STRING_PROPERTY_PRIMARY, toNodePropPrimary[i]));
      xml.append(XmlHandler.closeTag(STRING_PROPERTY));
    }
    xml.append(XmlHandler.closeTag(STRING_PROPERTIES));
    xml.append(XmlHandler.closeTag(STRING_TO));

    xml.append(XmlHandler.addTagValue(STRING_RELATIONSHIP, relationship));
    xml.append(XmlHandler.addTagValue(STRING_RELATIONSHIP_VALUE, relationshipValue));

    xml.append(XmlHandler.openTag(STRING_RELPROPS));
    for (int i = 0; i < relProps.length; i++) {
      xml.append(XmlHandler.openTag(STRING_RELPROP));
      xml.append(XmlHandler.addTagValue(STRING_PROPERTY_NAME, relPropNames[i]));
      xml.append(XmlHandler.addTagValue(STRING_PROPERTY_VALUE, relProps[i]));
      xml.append(XmlHandler.addTagValue(STRING_PROPERTY_TYPE, relPropTypes[i]));
      xml.append(XmlHandler.closeTag(STRING_RELPROP));
    }
    xml.append(XmlHandler.closeTag(STRING_RELPROPS));

    return xml.toString();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
      throws HopXmlException {

    connection = XmlHandler.getTagValue(transformNode, STRING_CONNECTION);
    batchSize = XmlHandler.getTagValue(transformNode, STRING_BATCH_SIZE);
    key = XmlHandler.getTagValue(transformNode, STRING_KEY);
    creatingIndexes =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, STRING_CREATE_INDEXES));
    usingCreate = "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, STRING_USE_CREATE));
    onlyCreatingRelationships =
        "Y"
            .equalsIgnoreCase(
                XmlHandler.getTagValue(transformNode, STRING_ONLY_CREATE_RELATIONSHIPS));
    returningGraph =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(transformNode, STRING_RETURNING_GRAPH));
    returnGraphField = XmlHandler.getTagValue(transformNode, STRING_RETURN_GRAPH_FIELD);

    Node fromNode = XmlHandler.getSubNode(transformNode, STRING_FROM);

    readOnlyFromNode =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(fromNode, STRING_READ_ONLY_FROM_NODE));

    Node fromLabelsNode = XmlHandler.getSubNode(fromNode, STRING_LABELS);
    List<Node> fromLabelNodes = XmlHandler.getNodes(fromLabelsNode, STRING_LABEL);
    List<Node> fromLabelValueNodes = XmlHandler.getNodes(fromLabelsNode, STRING_VALUE);

    fromNodeLabels = new String[fromLabelNodes.size()];
    fromNodeLabelValues = new String[Math.max(fromLabelValueNodes.size(), fromLabelNodes.size())];

    for (int i = 0; i < fromLabelNodes.size(); i++) {
      Node labelNode = fromLabelNodes.get(i);
      fromNodeLabels[i] = XmlHandler.getNodeValue(labelNode);
    }
    for (int i = 0; i < fromLabelValueNodes.size(); i++) {
      Node valueNode = fromLabelValueNodes.get(i);
      fromNodeLabelValues[i] = XmlHandler.getNodeValue(valueNode);
    }

    Node fromPropsNode = XmlHandler.getSubNode(fromNode, STRING_PROPERTIES);
    List<Node> fromPropertyNodes = XmlHandler.getNodes(fromPropsNode, STRING_PROPERTY);

    fromNodeProps = new String[fromPropertyNodes.size()];
    fromNodePropNames = new String[fromPropertyNodes.size()];
    fromNodePropTypes = new String[fromPropertyNodes.size()];
    fromNodePropPrimary = new boolean[fromPropertyNodes.size()];

    for (int i = 0; i < fromPropertyNodes.size(); i++) {
      Node propNode = fromPropertyNodes.get(i);
      fromNodeProps[i] = XmlHandler.getTagValue(propNode, STRING_PROPERTY_VALUE);
      fromNodePropNames[i] = XmlHandler.getTagValue(propNode, STRING_PROPERTY_NAME);
      fromNodePropTypes[i] = XmlHandler.getTagValue(propNode, STRING_PROPERTY_TYPE);
      fromNodePropPrimary[i] =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(propNode, STRING_PROPERTY_PRIMARY));
    }

    Node toNode = XmlHandler.getSubNode(transformNode, STRING_TO);

    readOnlyToNode = "Y".equalsIgnoreCase(XmlHandler.getTagValue(toNode, STRING_READ_ONLY_TO_NODE));

    Node toLabelsNode = XmlHandler.getSubNode(toNode, STRING_LABELS);
    List<Node> toLabelNodes = XmlHandler.getNodes(toLabelsNode, STRING_LABEL);
    List<Node> toLabelValueNodes = XmlHandler.getNodes(toLabelsNode, STRING_VALUE);

    toNodeLabels = new String[toLabelNodes.size()];
    toNodeLabelValues = new String[Math.max(toLabelValueNodes.size(), toLabelNodes.size())];

    for (int i = 0; i < toLabelNodes.size(); i++) {
      Node labelNode = toLabelNodes.get(i);
      toNodeLabels[i] = XmlHandler.getNodeValue(labelNode);
    }
    for (int i = 0; i < toLabelValueNodes.size(); i++) {
      Node valueNode = toLabelValueNodes.get(i);
      toNodeLabelValues[i] = XmlHandler.getNodeValue(valueNode);
    }

    Node toPropsNode = XmlHandler.getSubNode(toNode, STRING_PROPERTIES);
    List<Node> toPropertyNodes = XmlHandler.getNodes(toPropsNode, STRING_PROPERTY);

    toNodeProps = new String[toPropertyNodes.size()];
    toNodePropNames = new String[toPropertyNodes.size()];
    toNodePropTypes = new String[toPropertyNodes.size()];
    toNodePropPrimary = new boolean[toPropertyNodes.size()];

    for (int i = 0; i < toPropertyNodes.size(); i++) {
      Node propNode = toPropertyNodes.get(i);

      toNodeProps[i] = XmlHandler.getTagValue(propNode, STRING_PROPERTY_VALUE);
      toNodePropNames[i] = XmlHandler.getTagValue(propNode, STRING_PROPERTY_NAME);
      toNodePropTypes[i] = XmlHandler.getTagValue(propNode, STRING_PROPERTY_TYPE);
      toNodePropPrimary[i] =
          "Y".equalsIgnoreCase(XmlHandler.getTagValue(propNode, STRING_PROPERTY_PRIMARY));
    }

    relationship = XmlHandler.getTagValue(transformNode, STRING_RELATIONSHIP);
    relationshipValue = XmlHandler.getTagValue(transformNode, STRING_RELATIONSHIP_VALUE);

    Node relPropsNode = XmlHandler.getSubNode(transformNode, STRING_RELPROPS);
    List<Node> relPropNodes = XmlHandler.getNodes(relPropsNode, STRING_RELPROP);

    relProps = new String[relPropNodes.size()];
    relPropNames = new String[relPropNodes.size()];
    relPropTypes = new String[relPropNodes.size()];

    for (int i = 0; i < relPropNodes.size(); i++) {
      Node propNode = relPropNodes.get(i);

      relProps[i] = XmlHandler.getTagValue(propNode, STRING_PROPERTY_VALUE);
      relPropNames[i] = XmlHandler.getTagValue(propNode, STRING_PROPERTY_NAME);
      relPropTypes[i] = XmlHandler.getTagValue(propNode, STRING_PROPERTY_TYPE);
    }
  }

  @Override
  public void check(
      List<ICheckResult> remarks,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      IRowMeta prev,
      String[] input,
      String[] output,
      IRowMeta info,
      IVariables space,
      IHopMetadataProvider metadataProvider) {
    CheckResult cr;
    if (prev == null || prev.size() == 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_WARNING,
              "Not receiving any fields from previous transform!",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              "Transform is connected to previous one, receiving " + prev.size() + " fields",
              transformMeta);
      remarks.add(cr);
    }

    if (input.length > 0) {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_OK,
              "Transform is receiving info from other transform.",
              transformMeta);
      remarks.add(cr);
    } else {
      cr =
          new CheckResult(
              CheckResult.TYPE_RESULT_ERROR,
              "No input received from other transform!",
              transformMeta);
      remarks.add(cr);
    }
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
    if (returningGraph) {

      IValueMeta valueMetaGraph = new ValueMetaGraph(Const.NVL(returnGraphField, "graph"));
      valueMetaGraph.setOrigin(name);
      inputRowMeta.addValueMeta(valueMetaGraph);
    }
  }

  public Object clone() {
    return super.clone();
  }

  protected boolean dynamicLabels(String[] nodeLabelsFields) {
    for (String nodeLabelField : nodeLabelsFields) {
      if (StringUtils.isNotEmpty(nodeLabelField)) {
        return true;
      }
    }
    return false;
  }

  public boolean dynamicFromLabels() {
    return dynamicLabels(fromNodeLabels);
  }

  public boolean dynamicToLabels() {
    return dynamicLabels(toNodeLabels);
  }

  public boolean dynamicRelationshipLabel() {
    return StringUtils.isNotEmpty(relationship);
  }

  public boolean isCreatingRelationships() {
    return StringUtils.isNotEmpty(relationship) || StringUtils.isNotEmpty(relationshipValue);
  }

  /**
   * Gets connection
   *
   * @return value of connection
   */
  public String getConnection() {
    return connection;
  }

  /** @param connection The connection to set */
  public void setConnection(String connection) {
    this.connection = connection;
  }

  /**
   * Gets batchSize
   *
   * @return value of batchSize
   */
  public String getBatchSize() {
    return batchSize;
  }

  /** @param batchSize The batchSize to set */
  public void setBatchSize(String batchSize) {
    this.batchSize = batchSize;
  }

  /**
   * Gets creatingIndexes
   *
   * @return value of creatingIndexes
   */
  public boolean isCreatingIndexes() {
    return creatingIndexes;
  }

  /** @param creatingIndexes The creatingIndexes to set */
  public void setCreatingIndexes(boolean creatingIndexes) {
    this.creatingIndexes = creatingIndexes;
  }

  /**
   * Gets usingCreate
   *
   * @return value of usingCreate
   */
  public boolean isUsingCreate() {
    return usingCreate;
  }

  /** @param usingCreate The usingCreate to set */
  public void setUsingCreate(boolean usingCreate) {
    this.usingCreate = usingCreate;
  }

  /**
   * Gets onlyCreatingRelationships
   *
   * @return value of onlyCreatingRelationships
   */
  public boolean isOnlyCreatingRelationships() {
    return onlyCreatingRelationships;
  }

  /** @param onlyCreatingRelationships The onlyCreatingRelationships to set */
  public void setOnlyCreatingRelationships(boolean onlyCreatingRelationships) {
    this.onlyCreatingRelationships = onlyCreatingRelationships;
  }

  /**
   * Gets fromNodeProps
   *
   * @return value of fromNodeProps
   */
  public String[] getFromNodeProps() {
    return fromNodeProps;
  }

  /** @param fromNodeProps The fromNodeProps to set */
  public void setFromNodeProps(String[] fromNodeProps) {
    this.fromNodeProps = fromNodeProps;
  }

  /**
   * Gets fromNodePropNames
   *
   * @return value of fromNodePropNames
   */
  public String[] getFromNodePropNames() {
    return fromNodePropNames;
  }

  /** @param fromNodePropNames The fromNodePropNames to set */
  public void setFromNodePropNames(String[] fromNodePropNames) {
    this.fromNodePropNames = fromNodePropNames;
  }

  /**
   * Gets fromNodePropTypes
   *
   * @return value of fromNodePropTypes
   */
  public String[] getFromNodePropTypes() {
    return fromNodePropTypes;
  }

  /** @param fromNodePropTypes The fromNodePropTypes to set */
  public void setFromNodePropTypes(String[] fromNodePropTypes) {
    this.fromNodePropTypes = fromNodePropTypes;
  }

  /**
   * Gets fromNodePropPrimary
   *
   * @return value of fromNodePropPrimary
   */
  public boolean[] getFromNodePropPrimary() {
    return fromNodePropPrimary;
  }

  /** @param fromNodePropPrimary The fromNodePropPrimary to set */
  public void setFromNodePropPrimary(boolean[] fromNodePropPrimary) {
    this.fromNodePropPrimary = fromNodePropPrimary;
  }

  /**
   * Gets toNodeProps
   *
   * @return value of toNodeProps
   */
  public String[] getToNodeProps() {
    return toNodeProps;
  }

  /** @param toNodeProps The toNodeProps to set */
  public void setToNodeProps(String[] toNodeProps) {
    this.toNodeProps = toNodeProps;
  }

  /**
   * Gets toNodePropNames
   *
   * @return value of toNodePropNames
   */
  public String[] getToNodePropNames() {
    return toNodePropNames;
  }

  /** @param toNodePropNames The toNodePropNames to set */
  public void setToNodePropNames(String[] toNodePropNames) {
    this.toNodePropNames = toNodePropNames;
  }

  /**
   * Gets toNodePropTypes
   *
   * @return value of toNodePropTypes
   */
  public String[] getToNodePropTypes() {
    return toNodePropTypes;
  }

  /** @param toNodePropTypes The toNodePropTypes to set */
  public void setToNodePropTypes(String[] toNodePropTypes) {
    this.toNodePropTypes = toNodePropTypes;
  }

  /**
   * Gets toNodePropPrimary
   *
   * @return value of toNodePropPrimary
   */
  public boolean[] getToNodePropPrimary() {
    return toNodePropPrimary;
  }

  /** @param toNodePropPrimary The toNodePropPrimary to set */
  public void setToNodePropPrimary(boolean[] toNodePropPrimary) {
    this.toNodePropPrimary = toNodePropPrimary;
  }

  /**
   * Gets fromNodeLabels
   *
   * @return value of fromNodeLabels
   */
  public String[] getFromNodeLabels() {
    return fromNodeLabels;
  }

  /** @param fromNodeLabels The fromNodeLabels to set */
  public void setFromNodeLabels(String[] fromNodeLabels) {
    this.fromNodeLabels = fromNodeLabels;
  }

  /**
   * Gets toNodeLabels
   *
   * @return value of toNodeLabels
   */
  public String[] getToNodeLabels() {
    return toNodeLabels;
  }

  /** @param toNodeLabels The toNodeLabels to set */
  public void setToNodeLabels(String[] toNodeLabels) {
    this.toNodeLabels = toNodeLabels;
  }

  /**
   * Gets relProps
   *
   * @return value of relProps
   */
  public String[] getRelProps() {
    return relProps;
  }

  /** @param relProps The relProps to set */
  public void setRelProps(String[] relProps) {
    this.relProps = relProps;
  }

  /**
   * Gets relPropNames
   *
   * @return value of relPropNames
   */
  public String[] getRelPropNames() {
    return relPropNames;
  }

  /** @param relPropNames The relPropNames to set */
  public void setRelPropNames(String[] relPropNames) {
    this.relPropNames = relPropNames;
  }

  /**
   * Gets relPropTypes
   *
   * @return value of relPropTypes
   */
  public String[] getRelPropTypes() {
    return relPropTypes;
  }

  /** @param relPropTypes The relPropTypes to set */
  public void setRelPropTypes(String[] relPropTypes) {
    this.relPropTypes = relPropTypes;
  }

  /**
   * Gets key
   *
   * @return value of key
   */
  public String getKey() {
    return key;
  }

  /** @param key The key to set */
  public void setKey(String key) {
    this.key = key;
  }

  /**
   * Gets relationship
   *
   * @return value of relationship
   */
  public String getRelationship() {
    return relationship;
  }

  /** @param relationship The relationship to set */
  public void setRelationship(String relationship) {
    this.relationship = relationship;
  }

  /**
   * Gets fromNodeLabelValues
   *
   * @return value of fromNodeLabelValues
   */
  public String[] getFromNodeLabelValues() {
    return fromNodeLabelValues;
  }

  /** @param fromNodeLabelValues The fromNodeLabelValues to set */
  public void setFromNodeLabelValues(String[] fromNodeLabelValues) {
    this.fromNodeLabelValues = fromNodeLabelValues;
  }

  /**
   * Gets toNodeLabelValues
   *
   * @return value of toNodeLabelValues
   */
  public String[] getToNodeLabelValues() {
    return toNodeLabelValues;
  }

  /** @param toNodeLabelValues The toNodeLabelValues to set */
  public void setToNodeLabelValues(String[] toNodeLabelValues) {
    this.toNodeLabelValues = toNodeLabelValues;
  }

  /**
   * Gets relationshipValue
   *
   * @return value of relationshipValue
   */
  public String getRelationshipValue() {
    return relationshipValue;
  }

  /** @param relationshipValue The relationshipValue to set */
  public void setRelationshipValue(String relationshipValue) {
    this.relationshipValue = relationshipValue;
  }

  /**
   * Gets returningGraph
   *
   * @return value of returningGraph
   */
  public boolean isReturningGraph() {
    return returningGraph;
  }

  /** @param returningGraph The returningGraph to set */
  public void setReturningGraph(boolean returningGraph) {
    this.returningGraph = returningGraph;
  }

  /**
   * Gets returnGraphField
   *
   * @return value of returnGraphField
   */
  public String getReturnGraphField() {
    return returnGraphField;
  }

  /** @param returnGraphField The returnGraphField to set */
  public void setReturnGraphField(String returnGraphField) {
    this.returnGraphField = returnGraphField;
  }

  /**
   * Gets readOnlyFromNode
   *
   * @return value of readOnlyFromNode
   */
  public boolean isReadOnlyFromNode() {
    return readOnlyFromNode;
  }

  /** @param readOnlyFromNode The readOnlyFromNode to set */
  public void setReadOnlyFromNode(boolean readOnlyFromNode) {
    this.readOnlyFromNode = readOnlyFromNode;
  }

  /**
   * Gets readOnlyToNode
   *
   * @return value of readOnlyToNode
   */
  public boolean isReadOnlyToNode() {
    return readOnlyToNode;
  }

  /** @param readOnlyToNode The readOnlyToNode to set */
  public void setReadOnlyToNode(boolean readOnlyToNode) {
    this.readOnlyToNode = readOnlyToNode;
  }
}
