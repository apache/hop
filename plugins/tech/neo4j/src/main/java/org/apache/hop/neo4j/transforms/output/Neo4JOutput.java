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

package org.apache.hop.neo4j.transforms.output;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.neo4j.core.GraphUsage;
import org.apache.hop.neo4j.core.data.GraphData;
import org.apache.hop.neo4j.core.data.GraphNodeData;
import org.apache.hop.neo4j.core.data.GraphPropertyData;
import org.apache.hop.neo4j.core.data.GraphPropertyDataType;
import org.apache.hop.neo4j.core.data.GraphRelationshipData;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.apache.hop.neo4j.transforms.BaseNeoTransform;
import org.apache.hop.neo4j.transforms.output.fields.LabelField;
import org.apache.hop.neo4j.transforms.output.fields.NodeField;
import org.apache.hop.neo4j.transforms.output.fields.PropertyField;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.neo4j.driver.Result;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.ResultSummary;

public class Neo4JOutput extends BaseNeoTransform<Neo4JOutputMeta, Neo4JOutputData> {

  public Neo4JOutput(
      TransformMeta s,
      Neo4JOutputMeta meta,
      Neo4JOutputData data,
      int c,
      PipelineMeta t,
      Pipeline dis) {
    super(s, meta, data, c, t, dis);
  }

  @Override
  public boolean processRow() throws HopException {

    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.fieldNames = data.outputRowMeta.getFieldNames();
      data.fromNodePropIndexes = new int[meta.getNodeFromField().getProperties().size()];
      data.fromNodePropTypes =
          new GraphPropertyType[meta.getNodeFromField().getProperties().size()];
      for (int i = 0; i < meta.getNodeFromField().getProperties().size(); i++) {
        data.fromNodePropIndexes[i] =
            data.outputRowMeta.indexOfValue(
                meta.getNodeFromField().getProperties().get(i).getPropertyValue());
        if (data.fromNodePropIndexes[i] < 0) {
          throw new HopException(
              "From node: Unable to find field '"
                  + meta.getNodeFromField().getProperties().get(i).getPropertyValue()
                  + "' for property name '"
                  + meta.getNodeFromField().getProperties().get(i).getPropertyName()
                  + "'");
        }
        data.fromNodePropTypes[i] =
            GraphPropertyType.parseCode(
                meta.getNodeFromField().getProperties().get(i).getPropertyType());
      }
      data.fromNodeLabelIndexes = new int[meta.getNodeFromField().getLabels().size()];
      for (int i = 0; i < meta.getNodeFromField().getLabels().size(); i++) {
        data.fromNodeLabelIndexes[i] =
            data.outputRowMeta.indexOfValue(
                meta.getNodeFromField().getLabels().get(i).getLabelField());
        if (data.fromNodeLabelIndexes[i] < 0
            && StringUtils.isEmpty(meta.getNodeFromField().getLabels().get(i).getLabel())) {
          throw new HopException(
              "From node : please provide either a static label value or a field name to determine the label");
        }
      }
      data.toNodePropIndexes = new int[meta.getNodeToField().getProperties().size()];
      data.toNodePropTypes = new GraphPropertyType[meta.getNodeToField().getProperties().size()];
      for (int i = 0; i < meta.getNodeToField().getProperties().size(); i++) {
        data.toNodePropIndexes[i] =
            data.outputRowMeta.indexOfValue(
                meta.getNodeToField().getProperties().get(i).getPropertyValue());
        data.toNodePropTypes[i] =
            GraphPropertyType.parseCode(
                meta.getNodeToField().getProperties().get(i).getPropertyType());
      }
      if (Utils.isEmpty(meta.getNodeToField().getLabels().get(0).getLabel())
          && Utils.isEmpty(meta.getNodeToField().getLabels().get(0).getLabelField())) {
        data.toNodeLabelIndexes = new int[0];
      } else {
        data.toNodeLabelIndexes = new int[meta.getNodeToField().getLabels().size()];
        for (int i = 0; i < meta.getNodeToField().getLabels().size(); i++) {
          data.toNodeLabelIndexes[i] =
              data.outputRowMeta.indexOfValue(
                  meta.getNodeToField().getLabels().get(i).getLabelField());
          if (data.toNodeLabelIndexes[i] < 0
              && StringUtils.isEmpty(meta.getNodeToField().getLabels().get(i).getLabel())) {
            throw new HopException(
                "To node : please provide either a static label value or a field name to determine the label");
          }
        }
      }

      data.relPropIndexes = new int[meta.getRelProps().size()];
      data.relPropTypes = new GraphPropertyType[meta.getRelProps().size()];
      for (int i = 0; i < meta.getRelProps().size(); i++) {
        data.relPropIndexes[i] =
            data.outputRowMeta.indexOfValue(meta.getRelProps().get(i).getPropertyValue());
        data.relPropTypes[i] =
            GraphPropertyType.parseCode(meta.getRelProps().get(i).getPropertyType());
      }
      data.relationshipIndex = data.outputRowMeta.indexOfValue(meta.getRelationship());
      data.fromLabelValues = new String[meta.getNodeFromField().getLabels().size()];
      for (int i = 0; i < meta.getNodeFromField().getLabels().size(); i++) {
        data.fromLabelValues[i] = resolve(meta.getNodeFromField().getLabels().get(i).getLabel());
      }
      data.toLabelValues = new String[meta.getNodeToField().getLabels().size()];
      for (int i = 0; i < meta.getNodeToField().getLabels().size(); i++) {
        data.toLabelValues[i] = resolve(meta.getNodeToField().getLabels().get(i).getLabel());
      }
      data.relationshipLabelValue = resolve(meta.getRelationshipValue());

      data.unwindList = new ArrayList<>();

      data.dynamicFromLabels = determineDynamicLabels(meta.getNodeFromField().getLabels());
      data.dynamicToLabels = determineDynamicLabels(meta.getNodeToField().getLabels());
      data.dynamicRelLabel = StringUtils.isNotEmpty(meta.getRelationship());

      data.previousFromLabelsClause = null;
      data.previousToLabelsClause = null;
      data.previousRelationshipLabel = null;

      // Calculate the operation types
      //
      data.fromOperationType = OperationType.MERGE;
      data.toOperationType = OperationType.MERGE;
      data.relOperationType = OperationType.MERGE;
      if (meta.isUsingCreate()) {
        data.fromOperationType = OperationType.CREATE;
        data.toOperationType = OperationType.CREATE;
        data.relOperationType = OperationType.CREATE;
      }
      if (meta.isOnlyCreatingRelationships()) {
        data.fromOperationType = OperationType.MATCH;
        data.toOperationType = OperationType.MATCH;
        data.relOperationType = OperationType.CREATE;
      }
      if (meta.getNodeFromField().isReadOnly()) {
        data.fromOperationType = OperationType.MATCH;
      }
      if (meta.getNodeToField().isReadOnly()) {
        data.toOperationType = OperationType.MATCH;
      }

      // No 'From' Node activity?
      //
      if (meta.getNodeFromField().getLabels().isEmpty()) {
        data.fromOperationType = OperationType.NONE;
      }
      // No 'To' Node activity?
      //
      if (meta.getNodeToField().getLabels().isEmpty()
          || (Utils.isEmpty(meta.getNodeToField().getLabels().get(0).getLabel())
              && Utils.isEmpty(meta.getNodeToField().getLabels().get(0).getLabelField()))) {
        data.toOperationType = OperationType.NONE;
      }
      // No relationship activity?
      //
      if (StringUtils.isEmpty(meta.getRelationship())
          && StringUtils.isEmpty(meta.getRelationshipValue())) {
        data.relOperationType = OperationType.NONE;
      }

      // Create a session
      //
      if (meta.isReturningGraph()) {
        logBasic("Writing to output graph field, not to Neo4j");
      } else {
        data.driver = data.neoConnection.getDriver(getLogChannel(), this);
        data.session = data.neoConnection.getSession(getLogChannel(), data.driver, this);

        // Create indexes for the primary properties of the From and To nodes
        //
        if (meta.isCreatingIndexes()) {
          try {
            createNodePropertyIndexes(meta, data, getInputRowMeta(), row);
          } catch (HopException e) {
            logError("Unable to create indexes", e);
            return false;
          }
        }
      }

      // Finally, perform some extra validation (beyond the field names).
      //
      validateConfiguration();
    }

    if (meta.isReturningGraph()) {
      // Let the next transform handle writing to Neo4j
      //
      outputGraphValue(getInputRowMeta(), row);

    } else {

      boolean changedLabel = calculateLabelsAndDetectChanges(row);
      if (changedLabel || data.unwindList.size() >= data.batchSize) {
        emptyUnwindList();
      }

      // Add rows to the UNWIND list.  Just put all the properties from the nodes and relationship
      // in there
      // This could lead to property name collisions, so we prepend the properties in the list with
      // alias and underscore
      //
      Map<String, Object> propsMap = new HashMap<>();

      if (data.fromOperationType != OperationType.NONE) {
        addPropertiesToMap(
            propsMap, data.fromNodePropIndexes, getInputRowMeta(), row, data.fromNodePropTypes);
      }
      if (data.toOperationType != OperationType.NONE) {
        addPropertiesToMap(
            propsMap, data.toNodePropIndexes, getInputRowMeta(), row, data.toNodePropTypes);
      }
      if (data.relOperationType != OperationType.NONE) {
        addPropertiesToMap(
            propsMap, data.relPropIndexes, getInputRowMeta(), row, data.relPropTypes);
      }
      data.unwindList.add(propsMap);

      // Simply pass on the current row .
      //
      putRow(data.outputRowMeta, row);

      // Remember the previous labels
      //
      data.previousFromLabelsClause = data.fromLabelsClause;
      data.previousToLabelsClause = data.toLabelsClause;
      data.previousRelationshipLabel = data.relationshipLabel;
    }
    return true;
  }

  private void validateConfiguration() throws HopException {
    // Is there a primary key field specified for the To and From nodes?
    //
    boolean hasFromNode = !meta.getNodeFromField().getLabels().isEmpty();
    boolean hasToNode = !meta.getNodeToField().getLabels().isEmpty();
    boolean hasRelationship =
        StringUtils.isNotEmpty(meta.getRelationship())
            || StringUtils.isNotEmpty(meta.getRelationshipValue());
    if (hasRelationship) {
      // We need both nodes to be specified
      //
      if (!hasFromNode || !hasToNode) {
        throw new HopException("Please specify both nodes to be able to update relationships");
      }
      // Make sure both nodes have fields
      //
      boolean noFromKey = true;
      for (PropertyField propertyField : meta.getNodeFromField().getProperties()) {
        if (propertyField.isPropertyPrimary()) {
          noFromKey = false;
          break;
        }
      }
      if (noFromKey) {
        throw new HopException(
            "Please specify at least one or more primary key properties in the 'from' node");
      }
      boolean noToKey = true;
      for (PropertyField propertyField : meta.getNodeToField().getProperties()) {
        if (propertyField.isPropertyPrimary()) {
          noToKey = false;
          break;
        }
      }
      if (noToKey) {
        throw new HopException(
            "Please specify at least one or more primary key properties in the 'to' node");
      }
    }
  }

  private void addPropertiesToMap(
      Map<String, Object> rowMap,
      int[] nodePropIndexes,
      IRowMeta rowMeta,
      Object[] row,
      GraphPropertyType[] propertyTypes)
      throws HopValueException {

    // Add all the node properties for the current row to the rowMap
    //
    for (int i = 0; i < nodePropIndexes.length; i++) {

      IValueMeta valueMeta = rowMeta.getValueMeta(nodePropIndexes[i]);
      Object valueData = row[nodePropIndexes[i]];

      GraphPropertyType propertyType = propertyTypes[i];
      Object neoValue = propertyType.convertFromHop(valueMeta, valueData);

      String propName = "p" + nodePropIndexes[i];
      rowMap.put(propName, neoValue);
    }
  }

  private void emptyUnwindList() throws HopException {

    try {
      Map<String, Object> properties = Collections.singletonMap("props", data.unwindList);

      StringBuilder cypher = new StringBuilder();
      cypher.append("UNWIND $props as pr ").append(Const.CR);

      // The cypher for the 'from' node:
      //

      String fromLabelClause = data.previousFromLabelsClause;
      String fromMatchClause =
          getMatchClause(meta.getNodeFromField().getProperties(), data.fromNodePropIndexes);
      switch (data.fromOperationType) {
        case NONE:
          break;
        case CREATE:
          cypher
              .append("CREATE( ")
              .append(fromLabelClause)
              .append(" ")
              .append(fromMatchClause)
              .append(") ")
              .append(Const.CR);
          String setClause =
              getSetClause("f", meta.getNodeFromField().getProperties(), data.fromNodePropIndexes);
          if (StringUtils.isNotEmpty(setClause)) {
            cypher.append(setClause).append(Const.CR);
          }
          updateUsageMap(data.fromLabels, GraphUsage.NODE_CREATE);
          break;
        case MERGE:
          cypher
              .append("MERGE( ")
              .append(fromLabelClause)
              .append(" ")
              .append(fromMatchClause)
              .append(") ")
              .append(Const.CR);
          setClause =
              getSetClause("f", meta.getNodeFromField().getProperties(), data.fromNodePropIndexes);
          if (StringUtils.isNotEmpty(setClause)) {
            cypher.append(setClause).append(Const.CR);
          }
          updateUsageMap(data.fromLabels, GraphUsage.NODE_UPDATE);
          break;
        case MATCH:
          cypher
              .append("MATCH( ")
              .append(fromLabelClause)
              .append(" ")
              .append(fromMatchClause)
              .append(") ")
              .append(Const.CR);
          updateUsageMap(data.toLabels, GraphUsage.NODE_READ);
          break;
        default:
          throw new HopException(
              "Unsupported operation type for the 'from' node: " + data.fromOperationType);
      }

      // The cypher for the 'to' node:
      //
      String toLabelsClause = data.previousToLabelsClause;
      String toMatchClause =
          getMatchClause(meta.getNodeToField().getProperties(), data.toNodePropIndexes);
      switch (data.toOperationType) {
        case NONE:
          break;
        case CREATE:
          cypher
              .append("CREATE( ")
              .append(toLabelsClause)
              .append(" ")
              .append(toMatchClause)
              .append(") ")
              .append(Const.CR);
          String setClause =
              getSetClause("t", meta.getNodeToField().getProperties(), data.toNodePropIndexes);
          if (StringUtils.isNotEmpty(setClause)) {
            cypher.append(setClause).append(Const.CR);
          }
          updateUsageMap(data.toLabels, GraphUsage.NODE_CREATE);
          break;
        case MERGE:
          cypher
              .append("MERGE( ")
              .append(toLabelsClause)
              .append(" ")
              .append(toMatchClause)
              .append(") ")
              .append(Const.CR);
          setClause =
              getSetClause("t", meta.getNodeToField().getProperties(), data.toNodePropIndexes);
          if (StringUtils.isNotEmpty(setClause)) {
            cypher.append(setClause).append(Const.CR);
          }
          updateUsageMap(data.toLabels, GraphUsage.NODE_UPDATE);
          break;
        case MATCH:
          cypher
              .append("MATCH( ")
              .append(toLabelsClause)
              .append(" ")
              .append(toMatchClause)
              .append(") ")
              .append(Const.CR);
          updateUsageMap(data.toLabels, GraphUsage.NODE_READ);
          break;
        default:
          throw new HopException(
              "Unsupported operation type for the 'to' node: " + data.toOperationType);
      }

      // The cypher for the relationship:
      //
      String relationshipSetClause = getSetClause("r", meta.getRelProps(), data.relPropIndexes);
      switch (data.relOperationType) {
        case NONE:
          break;
        case MERGE:
          cypher
              .append("MERGE (f)-[")
              .append("r:")
              .append(data.relationshipLabel)
              .append("]->(t) ")
              .append(Const.CR)
              .append(relationshipSetClause)
              .append(Const.CR);
          updateUsageMap(List.of(data.relationshipLabel), GraphUsage.RELATIONSHIP_UPDATE);
          break;
        case CREATE:
          cypher
              .append("CREATE (f)-[")
              .append("r:")
              .append(data.relationshipLabel)
              .append("]->(t) ")
              .append(Const.CR)
              .append(getSetClause("r", meta.getRelProps(), data.relPropIndexes))
              .append(Const.CR);
          updateUsageMap(List.of(data.relationshipLabel), GraphUsage.RELATIONSHIP_CREATE);
          break;
      }

      data.cypher = cypher.toString();

      // OK now we have the cypher statement, we can execute it...
      //
      if (isDebug()) {
        logDebug("Running Cypher: " + data.cypher);
        logDebug("properties list size : " + data.unwindList.size());
      }

      // Run it always without beginTransaction()...
      //
      Result result = data.session.writeTransaction(tx -> tx.run(data.cypher, properties));
      processSummary(result);

      setLinesOutput(getLinesOutput() + data.unwindList.size());

      // Clear the list
      //
      data.unwindList.clear();
    } catch (Exception e) {
      throw new HopException("Error writing unwind statement to Neo4j", e);
    }
  }

  private String getMatchClause(List<PropertyField> nodePropertyFields, int[] nodePropIndexes) {
    StringBuilder clause = new StringBuilder();

    for (int i = 0; i < nodePropertyFields.size(); i++) {
      if (nodePropertyFields.get(i).isPropertyPrimary()) {
        if (clause.length() > 0) {
          clause.append(", ");
        }
        clause
            .append(nodePropertyFields.get(i).getPropertyName())
            .append(": pr.p")
            .append(nodePropIndexes[i]);
      }
    }

    if (clause.length() == 0) {
      return "";
    } else {
      return "{ " + clause + " }";
    }
  }

  private String getSetClause(
      String alias, List<PropertyField> propertyFields, int[] nodePropIndexes) {
    StringBuilder clause = new StringBuilder();

    for (int i = 0; i < propertyFields.size(); i++) {
      if (!propertyFields.get(i).isPropertyPrimary()) {
        if (clause.length() > 0) {
          clause.append(", ");
        }
        clause
            .append(alias)
            .append(".")
            .append(propertyFields.get(i).getPropertyName())
            .append("= pr.p")
            .append(nodePropIndexes[i]);
      }
    }

    if (clause.length() == 0) {
      return "";
    } else {
      return "SET " + clause;
    }
  }

  private boolean calculateLabelsAndDetectChanges(Object[] row) throws HopException {
    boolean changedLabel = false;

    if (data.fromOperationType != OperationType.NONE) {
      if (data.fromLabelsClause == null || data.dynamicFromLabels) {
        List<String> fLabels =
            getNodeLabels(
                meta.getNodeFromField().getLabels(),
                getInputRowMeta(),
                row,
                data.fromNodeLabelIndexes);
        data.fromLabelsClause = getLabels("f", fLabels);
      }
      if (data.dynamicFromLabels
          && data.previousFromLabelsClause != null
          && data.fromLabelsClause != null
          && !data.fromLabelsClause.equals(data.previousFromLabelsClause)) {
        changedLabel = true;
      }
    }

    if (data.toOperationType != OperationType.NONE) {
      if ((data.toLabelsClause == null || data.dynamicToLabels)
          && data.toNodeLabelIndexes.length > 0) {
        List<String> tLabels =
            getNodeLabels(
                meta.getNodeToField().getLabels(), getInputRowMeta(), row, data.toNodeLabelIndexes);
        data.toLabelsClause = getLabels("t", tLabels);
      }
      if (data.dynamicToLabels
          && data.previousToLabelsClause != null
          && data.toLabelsClause != null
          && !data.toLabelsClause.equals(data.previousToLabelsClause)) {
        changedLabel = true;
      }
    }

    if (data.relOperationType != OperationType.NONE) {
      if (data.dynamicRelLabel) {
        data.relationshipLabel = getInputRowMeta().getString(row, data.relationshipIndex);
      }
      if (StringUtils.isEmpty(data.relationshipLabel)
          && StringUtils.isNotEmpty(data.relationshipLabelValue)) {
        data.relationshipLabel = data.relationshipLabelValue;
      }
      if (data.dynamicRelLabel
          && data.previousRelationshipLabel != null
          && data.relationshipLabel != null
          && !data.relationshipLabel.equals(data.previousRelationshipLabel)) {
        changedLabel = true;
      }
    }

    return changedLabel;
  }

  private boolean determineDynamicLabels(List<LabelField> nodeLabelFields) {
    for (LabelField nodeLabelField : nodeLabelFields) {
      if (StringUtils.isNotEmpty(nodeLabelField.getLabelField())) {
        return true;
      }
    }
    return false;
  }

  private void outputGraphValue(IRowMeta rowMeta, Object[] row) throws HopException {

    try {

      GraphData graphData = new GraphData();
      graphData.setSourcePipelineName(getPipelineMeta().getName());
      graphData.setSourceTransformName(getTransformMeta().getName());

      GraphNodeData sourceNodeData = null;
      GraphNodeData targetNodeData = null;
      GraphRelationshipData relationshipData;

      if (!meta.getNodeFromField().getProperties().isEmpty()) {
        sourceNodeData = createGraphNodeData(rowMeta, row, meta.getNodeFromField(), "from");
        if (!meta.isOnlyCreatingRelationships()) {
          graphData.getNodes().add(sourceNodeData);
        }
      }
      if (!meta.getNodeToField().getProperties().isEmpty()) {
        targetNodeData = createGraphNodeData(rowMeta, row, meta.getNodeToField(), "to");
        if (!meta.isOnlyCreatingRelationships()) {
          graphData.getNodes().add(targetNodeData);
        }
      }

      String relationshipLabel = null;
      if (data.relationshipIndex >= 0) {
        relationshipLabel = getInputRowMeta().getString(row, data.relationshipIndex);
      }
      if (StringUtil.isEmpty(relationshipLabel)
          && StringUtils.isNotEmpty(data.relationshipLabelValue)) {
        relationshipLabel = data.relationshipLabelValue;
      }
      if (sourceNodeData != null
          && targetNodeData != null
          && StringUtils.isNotEmpty(relationshipLabel)) {

        relationshipData = new GraphRelationshipData();
        relationshipData.setSourceNodeId(sourceNodeData.getId());
        relationshipData.setTargetNodeId(targetNodeData.getId());
        relationshipData.setLabel(relationshipLabel);
        relationshipData.setId(sourceNodeData.getId() + " -> " + targetNodeData.getId());
        relationshipData.setPropertySetId("relationship");

        // Add relationship properties...
        //
        // Set the properties
        //
        for (int i = 0; i < data.relPropIndexes.length; i++) {

          IValueMeta valueMeta = rowMeta.getValueMeta(data.relPropIndexes[i]);
          Object valueData = row[data.relPropIndexes[i]];

          String propertyName = meta.getRelProps().get(i).getPropertyName();
          GraphPropertyDataType propertyType = GraphPropertyDataType.getTypeFromHop(valueMeta);
          Object propertyNeoValue = propertyType.convertFromHop(valueMeta, valueData);
          boolean propertyPrimary = false;

          relationshipData
              .getProperties()
              .add(
                  new GraphPropertyData(
                      propertyName, propertyNeoValue, propertyType, propertyPrimary));
        }

        graphData.getRelationships().add(relationshipData);
      }

      // Pass it forward...
      //
      Object[] outputRowData = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
      outputRowData[rowMeta.size()] = graphData;
      putRow(data.outputRowMeta, outputRowData);

    } catch (Exception e) {
      throw new HopException("Unable to calculate graph output value", e);
    }
  }

  private GraphNodeData createGraphNodeData(
      IRowMeta rowMeta, Object[] row, NodeField nodeField, String propertySetId)
      throws HopException {
    GraphNodeData nodeData = new GraphNodeData();

    // The property set ID is simply either "Source" or "Target"
    //
    nodeData.setPropertySetId(propertySetId);

    // Set the label(s)
    //
    List<String> labels =
        getNodeLabels(
            nodeField.getLabels(),
            rowMeta,
            row,
            new int[meta.getNodeFromField().getLabels().size()]);
    for (String label : labels) {
      nodeData.getLabels().add(label);
    }

    StringBuilder nodeId = new StringBuilder();

    // Set the properties
    //
    for (int i = 0; i < nodeField.getProperties().size(); i++) {

      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      Object valueData = row[i];

      String propertyName = nodeField.getProperties().get(i).getPropertyName();
      GraphPropertyDataType propertyType = GraphPropertyDataType.getTypeFromHop(valueMeta);
      Object propertyNeoValue = propertyType.convertFromHop(valueMeta, valueData);
      boolean propertyPrimary = nodeField.getProperties().get(i).isPropertyPrimary();

      nodeData
          .getProperties()
          .add(
              new GraphPropertyData(propertyName, propertyNeoValue, propertyType, propertyPrimary));

      // Part of the key...
      if (nodeField.getProperties().get(i).isPropertyPrimary()) {
        if (nodeId.length() > 0) {
          nodeId.append("-");
        }
        nodeId.append(valueMeta.getString(valueData));
      }
    }

    if (nodeId.length() > 0) {
      nodeData.setId(nodeId.toString());
    }

    return nodeData;
  }

  @Override
  public boolean init() {
    if (!meta.isReturningGraph()) {

      // Connect to Neo4j using info metastore Neo4j Connection metadata
      //
      if (StringUtils.isEmpty(resolve(meta.getConnection()))) {
        logError("You need to specify a Neo4j connection to use in this transform");
        return false;
      }

      try {
        data.neoConnection =
            metadataProvider.getSerializer(NeoConnection.class).load(resolve(meta.getConnection()));
        if (data.neoConnection == null) {
          logError(
              "Connection '"
                  + resolve(meta.getConnection())
                  + "' could not be found in the metastore : "
                  + metadataProvider.getDescription());
          return false;
        }
        data.version4 = data.neoConnection.isVersion4();
      } catch (HopException e) {
        logError(
            "Could not gencsv Neo4j connection '"
                + resolve(meta.getConnection())
                + "' from the metastore",
            e);
        return false;
      }

      data.batchSize = Const.toLong(resolve(meta.getBatchSize()), 1);
    }

    return super.init();
  }

  @Override
  public void dispose() {
    if (!isStopped()) {
      try {
        wrapUpTransaction();
      } catch (HopException e) {
        logError("Error wrapping up transaction", e);
        setErrors(1L);
        stopAll();
      }
    }

    if (data.session != null) {
      data.session.close();
    }
    if (data.driver != null) {
      data.driver.close();
    }

    super.dispose();
  }

  private String getLabels(String nodeAlias, List<String> nodeLabels) {

    if (nodeLabels.isEmpty()) {
      return null;
    }

    StringBuilder labels = new StringBuilder(nodeAlias);
    for (String nodeLabel : nodeLabels) {
      labels.append(":");
      labels.append(escapeLabel(nodeLabel));
    }
    return labels.toString();
  }

  private void processSummary(Result result) throws HopException {
    boolean error = false;
    ResultSummary summary = result.consume();
    for (Notification notification : summary.notifications()) {
      logError(notification.title() + " (" + notification.severity() + ")");
      logError(
          notification.code()
              + " : "
              + notification.description()
              + ", position "
              + notification.position());
      error = true;
    }
    if (error) {
      throw new HopException("Error found while executing cypher statement(s)");
    }
  }

  public List<String> getNodeLabels(
      List<LabelField> labelFields, IRowMeta rowMeta, Object[] rowData, int[] labelIndexes)
      throws HopValueException {
    List<String> labels = new ArrayList<>();

    for (int a = 0; a < labelFields.size(); a++) {
      String label = null;
      if (StringUtils.isNotEmpty(labelFields.get(a).getLabelField())) {
        label = rowMeta.getString(rowData, labelIndexes[a]);
      }
      if (StringUtils.isEmpty(label) && StringUtils.isNotEmpty(labelFields.get(a).getLabel())) {
        label = labelFields.get(a).getLabel();
      }
      if (StringUtils.isNotEmpty(label)) {
        labels.add(label);
      }
    }
    return labels;
  }

  public String escapeLabel(String str) {
    if (str.contains(" ") || str.contains(".")) {
      str = "`" + str + "`";
    }
    return str;
  }

  private void createNodePropertyIndexes(
      Neo4JOutputMeta meta, Neo4JOutputData data, IRowMeta rowMeta, Object[] rowData)
      throws HopException {

    // Only create indexes on the first copy
    //
    if (getCopy() != 0) {
      return;
    }

    createIndexForNode(data, meta.getNodeFromField(), rowMeta, rowData);
    createIndexForNode(data, meta.getNodeToField(), rowMeta, rowData);
  }

  private void createIndexForNode(
      Neo4JOutputData data, NodeField theNode, IRowMeta rowMeta, Object[] rowData)
      throws HopValueException {

    // Which labels to index?
    //
    Set<String> labels = new HashSet<>();

    for (LabelField labelField : theNode.getLabels()) {
      if (!Utils.isEmpty(labelField.getLabel())) {
        labels.add(labelField.getLabel());
      }
      if (!Utils.isEmpty(labelField.getLabelField())) {
        String label = rowMeta.getString(rowData, labelField.getLabelField(), null);
        if (StringUtils.isNotEmpty(label)) {
          labels.add(label);
        }
      }
    }

    // Create an index on the primary fields of the node properties
    //
    for (String label : labels) {
      List<String> primaryProperties = new ArrayList<>();
      for (int f = 0; f < theNode.getProperties().size(); f++) {
        if (theNode.getProperties().get(f).isPropertyPrimary()) {
          if (StringUtils.isNotEmpty(theNode.getProperties().get(f).getPropertyName())) {
            primaryProperties.add(theNode.getProperties().get(f).getPropertyName());
          } else {
            primaryProperties.add(theNode.getProperties().get(f).getPropertyValue());
          }
        }
      }

      if (label != null && !primaryProperties.isEmpty()) {
        NeoConnectionUtils.createNodeIndex(
            getLogChannel(), data.session, Collections.singletonList(label), primaryProperties);
      }
    }
  }

  @Override
  public void batchComplete() throws HopException {
    wrapUpTransaction();
  }

  private void wrapUpTransaction() throws HopException {

    if (!isStopped() && data.unwindList != null && !data.unwindList.isEmpty()) {
      emptyUnwindList(); // force write!
    }

    // Allow gc
    //
    data.unwindList = new ArrayList<>();
  }

  /**
   * Update the usagemap. Add all the labels to the node usage.
   *
   * @param labels The labels of the usage
   * @param usage The usage itself
   */
  protected void updateUsageMap(List<String> labels, GraphUsage usage) {

    if (labels == null) {
      return;
    }

    Map<String, Set<String>> transformsMap =
        data.usageMap.computeIfAbsent(usage.name(), k -> new HashMap<>());

    Set<String> labelSet = transformsMap.computeIfAbsent(getTransformName(), k -> new HashSet<>());

    for (String label : labels) {
      if (StringUtils.isNotEmpty(label)) {
        labelSet.add(label);
      }
    }
  }
}
