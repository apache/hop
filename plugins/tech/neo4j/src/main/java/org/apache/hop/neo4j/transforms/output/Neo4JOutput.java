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

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.neo4j.core.GraphUsage;
import org.apache.hop.neo4j.core.data.*;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.apache.hop.neo4j.transforms.BaseNeoTransform;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.neo4j.driver.Result;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.ResultSummary;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Neo4JOutput extends BaseNeoTransform<Neo4JOutputMeta, Neo4JOutputData>
    implements ITransform<Neo4JOutputMeta, Neo4JOutputData> {

  private static Class<?> PKG = Neo4JOutput.class; // for i18n purposes, needed by Translator2!!

  public Neo4JOutput(
      TransformMeta s,
      Neo4JOutputMeta meta,
      Neo4JOutputData data,
      int c,
      PipelineMeta t,
      Pipeline dis) {
    super(s, meta, data, c, t, dis);
  }

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
      data.fromNodePropIndexes = new int[meta.getFromNodeProps().length];
      data.fromNodePropTypes = new GraphPropertyType[meta.getFromNodeProps().length];
      for (int i = 0; i < meta.getFromNodeProps().length; i++) {
        data.fromNodePropIndexes[i] = data.outputRowMeta.indexOfValue(meta.getFromNodeProps()[i]);
        if (data.fromNodePropIndexes[i] < 0) {
          throw new HopException(
              "From node: Unable to find field '"
                  + meta.getFromNodeProps()[i]
                  + "' for property name '"
                  + meta.getFromNodePropNames()[i]
                  + "'");
        }
        data.fromNodePropTypes[i] = GraphPropertyType.parseCode(meta.getFromNodePropTypes()[i]);
      }
      data.fromNodeLabelIndexes = new int[meta.getFromNodeLabels().length];
      for (int i = 0; i < meta.getFromNodeLabels().length; i++) {
        data.fromNodeLabelIndexes[i] = data.outputRowMeta.indexOfValue(meta.getFromNodeLabels()[i]);
        if (data.fromNodeLabelIndexes[i] < 0
            && StringUtils.isEmpty(meta.getFromNodeLabelValues()[i])) {
          throw new HopException(
              "From node : please provide either a static label value or a field name to determine the label");
        }
      }
      data.toNodePropIndexes = new int[meta.getToNodeProps().length];
      data.toNodePropTypes = new GraphPropertyType[meta.getToNodeProps().length];
      for (int i = 0; i < meta.getToNodeProps().length; i++) {
        data.toNodePropIndexes[i] = data.outputRowMeta.indexOfValue(meta.getToNodeProps()[i]);
        data.toNodePropTypes[i] = GraphPropertyType.parseCode(meta.getToNodePropTypes()[i]);
      }
      data.toNodeLabelIndexes = new int[meta.getToNodeLabels().length];
      for (int i = 0; i < meta.getToNodeLabels().length; i++) {
        data.toNodeLabelIndexes[i] = data.outputRowMeta.indexOfValue(meta.getToNodeLabels()[i]);
        if (data.toNodeLabelIndexes[i] < 0 && StringUtils.isEmpty(meta.getToNodeLabelValues()[i])) {
          throw new HopException(
              "To node : please provide either a static label value or a field name to determine the label");
        }
      }
      data.relPropIndexes = new int[meta.getRelProps().length];
      data.relPropTypes = new GraphPropertyType[meta.getRelProps().length];
      for (int i = 0; i < meta.getRelProps().length; i++) {
        data.relPropIndexes[i] = data.outputRowMeta.indexOfValue(meta.getRelProps()[i]);
        data.relPropTypes[i] = GraphPropertyType.parseCode(meta.getRelPropTypes()[i]);
      }
      data.relationshipIndex = data.outputRowMeta.indexOfValue(meta.getRelationship());
      data.fromLabelValues = new String[meta.getFromNodeLabelValues().length];
      for (int i = 0; i < meta.getFromNodeLabelValues().length; i++) {
        data.fromLabelValues[i] = resolve(meta.getFromNodeLabelValues()[i]);
      }
      data.toLabelValues = new String[meta.getToNodeLabelValues().length];
      for (int i = 0; i < meta.getToNodeLabelValues().length; i++) {
        data.toLabelValues[i] = resolve(meta.getToNodeLabelValues()[i]);
      }
      data.relationshipLabelValue = resolve(meta.getRelationshipValue());

      data.unwindList = new ArrayList<>();

      data.dynamicFromLabels = determineDynamicLabels(meta.getFromNodeLabels());
      data.dynamicToLabels = determineDynamicLabels(meta.getToNodeLabels());
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
      if (meta.isReadOnlyFromNode()) {
        data.fromOperationType = OperationType.MATCH;
      }
      if (meta.isReadOnlyToNode()) {
        data.toOperationType = OperationType.MATCH;
      }

      // No 'From' Node activity?
      //
      if (meta.getFromNodeLabels().length == 0 && meta.getFromNodeLabelValues().length == 0) {
        data.fromOperationType = OperationType.NONE;
      }
      // No 'To' Node activity?
      //
      if (meta.getToNodeLabels().length == 0 && meta.getToNodeLabelValues().length == 0) {
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
        log.logBasic("Writing to output graph field, not to Neo4j");
      } else {
        data.session = data.neoConnection.getSession(log, this);

        // Create indexes for the primary properties of the From and To nodes
        //
        if (meta.isCreatingIndexes()) {
          try {
            createNodePropertyIndexes(meta, data, getInputRowMeta(), row);
          } catch (HopException e) {
            log.logError("Unable to create indexes", e);
            return false;
          }
        }
      }
    }

    if (meta.isReturningGraph()) {
      // Let the next transform handle writing to Neo4j
      //
      outputGraphValue(getInputRowMeta(), row);

    } else {

      boolean changedLabel = calculateLabelsAndDetectChanges(row);
      if (changedLabel) {
        emptyUnwindList(changedLabel);
      }

      // Add rows to the unwind list.  Just put all the properties from the nodes and relationship
      // in there
      // This could lead to property name collisions so we prepend the properties in the list with
      // alias and underscore
      //
      Map<String, Object> propsMap = new HashMap<>();

      if (data.fromOperationType != OperationType.NONE) {
        addPropertiesToMap(
            propsMap,
            "f",
            data.fromNodePropIndexes,
            getInputRowMeta(),
            row,
            meta.getFromNodePropNames(),
            data.fromNodePropTypes);
      }
      if (data.toOperationType != OperationType.NONE) {
        addPropertiesToMap(
            propsMap,
            "t",
            data.toNodePropIndexes,
            getInputRowMeta(),
            row,
            meta.getToNodePropNames(),
            data.toNodePropTypes);
      }
      if (data.relOperationType != OperationType.NONE) {
        addPropertiesToMap(
            propsMap,
            "r",
            data.relPropIndexes,
            getInputRowMeta(),
            row,
            meta.getRelPropNames(),
            data.relPropTypes);
      }
      data.unwindList.add(propsMap);

      if (data.unwindList.size() >= data.batchSize) {
        emptyUnwindList(changedLabel);
      }

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

  private void addPropertiesToMap(
      Map<String, Object> rowMap,
      String alias,
      int[] nodePropIndexes,
      IRowMeta rowMeta,
      Object[] row,
      String[] nodePropNames,
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

  private void emptyUnwindList(boolean changedLabel) throws HopException {

    try {
      Map<String, Object> properties = Collections.singletonMap("props", data.unwindList);

      if (data.cypher == null || changedLabel) {

        StringBuilder cypher = new StringBuilder();
        cypher.append("UNWIND $props as pr ").append(Const.CR);

        // The cypher for the 'from' node:
        //
        boolean takePreviousFrom =
            data.dynamicFromLabels && changedLabel && data.previousFromLabelsClause != null;
        String fromLabelClause =
            takePreviousFrom ? data.previousFromLabelsClause : data.fromLabelsClause;
        String fromMatchClause =
            getMatchClause(
                meta.getFromNodePropNames(),
                meta.getFromNodePropPrimary(),
                data.fromNodePropIndexes,
                "f");
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
                getSetClause(
                    false,
                    "f",
                    meta.getFromNodePropNames(),
                    meta.getFromNodePropPrimary(),
                    data.fromNodePropIndexes);
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
                getSetClause(
                    false,
                    "f",
                    meta.getFromNodePropNames(),
                    meta.getFromNodePropPrimary(),
                    data.fromNodePropIndexes);
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
        boolean takePreviousTo = data.dynamicToLabels && changedLabel;
        String toLabelsClause = takePreviousTo ? data.previousToLabelsClause : data.toLabelsClause;
        String toMatchClause =
            getMatchClause(
                meta.getToNodePropNames(),
                meta.getToNodePropPrimary(),
                data.toNodePropIndexes,
                "f");
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
                getSetClause(
                    false,
                    "t",
                    meta.getToNodePropNames(),
                    meta.getToNodePropPrimary(),
                    data.toNodePropIndexes);
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
                getSetClause(
                    false,
                    "t",
                    meta.getToNodePropNames(),
                    meta.getToNodePropPrimary(),
                    data.toNodePropIndexes);
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
        String relationshipSetClause =
            getSetClause(
                false,
                "r",
                meta.getRelPropNames(),
                new boolean[meta.getRelPropNames().length],
                data.relPropIndexes);
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
            updateUsageMap(Arrays.asList(data.relationshipLabel), GraphUsage.RELATIONSHIP_UPDATE);
            ;
            break;
          case CREATE:
            cypher
                .append("CREATE (f)-[")
                .append("r:")
                .append(data.relationshipLabel)
                .append("]->(t) ")
                .append(Const.CR)
                .append(
                    getSetClause(
                        false,
                        "r",
                        meta.getRelPropNames(),
                        new boolean[meta.getRelPropNames().length],
                        data.relPropIndexes))
                .append(Const.CR);
            updateUsageMap(Arrays.asList(data.relationshipLabel), GraphUsage.RELATIONSHIP_CREATE);
            break;
        }

        data.cypher = cypher.toString();
      }

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

  private String getMatchClause(
      String[] propertyNames, boolean[] propertyPrimary, int[] nodePropIndexes, String alias) {
    StringBuilder clause = new StringBuilder();

    for (int i = 0; i < propertyNames.length; i++) {
      if (propertyPrimary[i]) {
        if (clause.length() > 0) {
          clause.append(", ");
        }
        clause.append(propertyNames[i]).append(": pr.p").append(nodePropIndexes[i]);
      }
    }

    if (clause.length() == 0) {
      return "";
    } else {
      return "{ " + clause + " }";
    }
  }

  private String getSetClause(
      boolean allProperties,
      String alias,
      String[] propertyNames,
      boolean[] propertyPrimary,
      int[] nodePropIndexes) {
    StringBuilder clause = new StringBuilder();

    for (int i = 0; i < propertyNames.length; i++) {
      if (allProperties || !propertyPrimary[i]) {
        if (clause.length() > 0) {
          clause.append(", ");
        }
        clause
            .append(alias)
            .append(".")
            .append(propertyNames[i])
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
                meta.getFromNodeLabels(),
                data.fromLabelValues,
                getInputRowMeta(),
                row,
                data.fromNodeLabelIndexes);
        data.fromLabelsClause = getLabels("f", fLabels);
      }
      if (data.dynamicFromLabels
          && data.previousFromLabelsClause != null
          && data.fromLabelsClause != null) {
        if (!data.fromLabelsClause.equals(data.previousFromLabelsClause)) {
          changedLabel = true;
        }
      }
    }

    if (data.toOperationType != OperationType.NONE) {
      if (data.toLabelsClause == null || data.dynamicToLabels) {
        List<String> tLabels =
            getNodeLabels(
                meta.getToNodeLabels(),
                data.toLabelValues,
                getInputRowMeta(),
                row,
                data.toNodeLabelIndexes);
        data.toLabelsClause = getLabels("t", tLabels);
      }
      if (data.dynamicToLabels
          && data.previousToLabelsClause != null
          && data.toLabelsClause != null) {
        if (!data.toLabelsClause.equals(data.previousToLabelsClause)) {
          changedLabel = true;
        }
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
          && data.relationshipLabel != null) {
        if (!data.relationshipLabel.equals(data.previousRelationshipLabel)) {
          changedLabel = true;
        }
      }
    }

    return changedLabel;
  }

  private boolean determineDynamicLabels(String[] nodeLabelFields) {
    for (String nodeLabelField : nodeLabelFields) {
      if (StringUtils.isNotEmpty(nodeLabelField)) {
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

      if (meta.getFromNodeProps().length > 0) {
        sourceNodeData =
            createGraphNodeData(
                rowMeta,
                row,
                meta.getFromNodeLabels(),
                data.fromLabelValues,
                data.fromNodeLabelIndexes,
                data.fromNodePropIndexes,
                meta.getFromNodePropNames(),
                meta.getFromNodePropPrimary(),
                "from");
        if (!meta.isOnlyCreatingRelationships()) {
          graphData.getNodes().add(sourceNodeData);
        }
      }
      if (meta.getToNodeProps().length > 0) {
        targetNodeData =
            createGraphNodeData(
                rowMeta,
                row,
                meta.getToNodeLabels(),
                data.toLabelValues,
                data.toNodeLabelIndexes,
                data.toNodePropIndexes,
                meta.getToNodePropNames(),
                meta.getToNodePropPrimary(),
                "to");
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

          String propertyName = meta.getRelPropNames()[i];
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
      int startIndex = rowMeta.size();
      outputRowData[rowMeta.size()] = graphData;
      putRow(data.outputRowMeta, outputRowData);

    } catch (Exception e) {
      throw new HopException("Unable to calculate graph output value", e);
    }
  }

  private GraphNodeData createGraphNodeData(
      IRowMeta rowMeta,
      Object[] row,
      String[] nodeLabels,
      String[] nodeLabelValues,
      int[] nodeLabelIndexes,
      int[] nodePropIndexes,
      String[] nodePropNames,
      boolean[] nodePropPrimary,
      String propertySetId)
      throws HopException {
    GraphNodeData nodeData = new GraphNodeData();

    // The property set ID is simply either "Source" or "Target"
    //
    nodeData.setPropertySetId(propertySetId);

    // Set the label(s)
    //
    List<String> labels =
        getNodeLabels(nodeLabels, nodeLabelValues, rowMeta, row, nodeLabelIndexes);
    for (String label : labels) {
      nodeData.getLabels().add(label);
    }

    StringBuilder nodeId = new StringBuilder();

    // Set the properties
    //
    for (int i = 0; i < nodePropIndexes.length; i++) {

      IValueMeta valueMeta = rowMeta.getValueMeta(nodePropIndexes[i]);
      Object valueData = row[nodePropIndexes[i]];

      String propertyName = nodePropNames[i];
      GraphPropertyDataType propertyType = GraphPropertyDataType.getTypeFromHop(valueMeta);
      Object propertyNeoValue = propertyType.convertFromHop(valueMeta, valueData);
      boolean propertyPrimary = nodePropPrimary[i];

      nodeData
          .getProperties()
          .add(
              new GraphPropertyData(propertyName, propertyNeoValue, propertyType, propertyPrimary));

      // Part of the key...
      if (nodePropPrimary[i]) {
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
      if (StringUtils.isEmpty(meta.getConnection())) {
        log.logError("You need to specify a Neo4j connection to use in this transform");
        return false;
      }

      try {
        data.neoConnection =
            metadataProvider.getSerializer(NeoConnection.class).load(meta.getConnection());
        if (data.neoConnection == null) {
          log.logError(
              "Connection '"
                  + meta.getConnection()
                  + "' could not be found in the metastore : "
                  + metadataProvider.getDescription());
          return false;
        }
        data.version4 = data.neoConnection.isVersion4();
      } catch (HopException e) {
        log.logError(
            "Could not gencsv Neo4j connection '" + meta.getConnection() + "' from the metastore",
            e);
        return false;
      }

      data.batchSize = Const.toLong(resolve(meta.getBatchSize()), 1);
    }

    return super.init();
  }

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
      log.logError(notification.title() + " (" + notification.severity() + ")");
      log.logError(
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

  private String buildParameterClause(String parameterName) {
    if (data.version4) {
      return "$" + parameterName;
    } else {
      return "{" + parameterName + "}";
    }
  }

  private String generateMatchClause(
      String alias,
      String mapName,
      List<String> nodeLabels,
      String[] nodeProps,
      String[] nodePropNames,
      GraphPropertyType[] nodePropTypes,
      boolean[] nodePropPrimary,
      IRowMeta rowMeta,
      Object[] rowData,
      int[] nodePropIndexes,
      Map<String, Object> parameters,
      AtomicInteger paramNr)
      throws HopValueException {
    String matchClause = "(" + alias;
    for (int i = 0; i < nodeLabels.size(); i++) {
      String label = escapeProp(nodeLabels.get(i));
      matchClause += ":" + label;
    }
    matchClause += " {";

    boolean firstProperty = true;
    for (int i = 0; i < nodeProps.length; i++) {
      if (nodePropPrimary[i]) {
        if (firstProperty) {
          firstProperty = false;
        } else {
          matchClause += ", ";
        }
        String propName;
        if (StringUtils.isNotEmpty(nodePropNames[i])) {
          propName = nodePropNames[i];
        } else {
          propName = nodeProps[i];
        }
        String parameterName = "param" + paramNr.incrementAndGet();

        if (mapName == null) {
          matchClause += propName + " : " + buildParameterClause(parameterName);
        } else {
          matchClause += propName + " : " + mapName + "." + parameterName;
        }

        if (parameters != null) {
          IValueMeta valueMeta = rowMeta.getValueMeta(nodePropIndexes[i]);
          Object valueData = rowData[nodePropIndexes[i]];

          GraphPropertyType propertyType = nodePropTypes[i];
          Object neoValue = propertyType.convertFromHop(valueMeta, valueData);

          parameters.put(parameterName, neoValue);
        }
      }
    }
    matchClause += " })";

    return matchClause;
  }

  public List<String> getNodeLabels(
      String[] labelFields,
      String[] labelValues,
      IRowMeta rowMeta,
      Object[] rowData,
      int[] labelIndexes)
      throws HopValueException {
    List<String> labels = new ArrayList<>();

    for (int a = 0; a < labelFields.length; a++) {
      String label = null;
      if (StringUtils.isNotEmpty(labelFields[a])) {
        label = rowMeta.getString(rowData, labelIndexes[a]);
      }
      if (StringUtils.isEmpty(label) && StringUtils.isNotEmpty(labelValues[a])) {
        label = labelValues[a];
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

  public String escapeProp(String str) {
    return StringEscapeUtils.escapeJava(str);
  }

  private void createNodePropertyIndexes(
      Neo4JOutputMeta meta, Neo4JOutputData data, IRowMeta rowMeta, Object[] rowData)
      throws HopException {

    // Only create indexes on the first copy
    //
    if (getCopy() != 0) {
      return;
    }

    createIndexForNode(
        data,
        meta.getFromNodeLabels(),
        meta.getFromNodeLabelValues(),
        meta.getFromNodeProps(),
        meta.getFromNodePropNames(),
        meta.getFromNodePropPrimary(),
        rowMeta,
        rowData);
    createIndexForNode(
        data,
        meta.getToNodeLabels(),
        meta.getToNodeLabelValues(),
        meta.getToNodeProps(),
        meta.getToNodePropNames(),
        meta.getToNodePropPrimary(),
        rowMeta,
        rowData);
  }

  private void createIndexForNode(
      Neo4JOutputData data,
      String[] nodeLabelFields,
      String[] nodeLabelValues,
      String[] nodeProps,
      String[] nodePropNames,
      boolean[] nodePropPrimary,
      IRowMeta rowMeta,
      Object[] rowData)
      throws HopValueException {

    // Which labels to index?
    //
    Set<String> labels = new HashSet<>();
    labels.addAll(
        Arrays.asList(nodeLabelValues).stream()
            .filter(s -> StringUtils.isNotEmpty(s))
            .collect(Collectors.toList()));

    for (String nodeLabelField : nodeLabelFields) {
      if (StringUtils.isNotEmpty(nodeLabelField)) {
        String label = rowMeta.getString(rowData, nodeLabelField, null);
        if (StringUtils.isNotEmpty(label)) {
          labels.add(label);
        }
      }
    }

    // Create a index on the primary fields of the node properties
    //
    for (String label : labels) {
      List<String> primaryProperties = new ArrayList<>();
      for (int f = 0; f < nodeProps.length; f++) {
        if (nodePropPrimary[f]) {
          if (StringUtils.isNotEmpty(nodePropNames[f])) {
            primaryProperties.add(nodePropNames[f]);
          } else {
            primaryProperties.add(nodeProps[f]);
          }
        }
      }

      if (label != null && primaryProperties.size() > 0) {
        NeoConnectionUtils.createNodeIndex(
            log, data.session, Collections.singletonList(label), primaryProperties);
      }
    }
  }

  @Override
  public void batchComplete() throws HopException {
    wrapUpTransaction();
  }

  private void wrapUpTransaction() throws HopException {

    if (!isStopped()) {
      if (data.unwindList != null && data.unwindList.size() > 0) {
        emptyUnwindList(true); // force write!
      }
    }

    // Allow gc
    //
    data.unwindList = new ArrayList<>();
  }

  /**
   * Update the usagemap. Add all the labels to the node usage.
   *
   * @param labels
   * @param usage
   */
  protected void updateUsageMap(List<String> labels, GraphUsage usage) throws HopValueException {

    if (labels == null) {
      return;
    }

    Map<String, Set<String>> transformsMap = data.usageMap.get(usage.name());
    if (transformsMap == null) {
      transformsMap = new HashMap<>();
      data.usageMap.put(usage.name(), transformsMap);
    }

    Set<String> labelSet = transformsMap.get(getTransformName());
    if (labelSet == null) {
      labelSet = new HashSet<>();
      transformsMap.put(getTransformName(), labelSet);
    }

    for (String label : labels) {
      if (StringUtils.isNotEmpty(label)) {
        labelSet.add(label);
      }
    }
  }
}
