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

package org.apache.hop.neo4j.transforms.graph;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.neo4j.core.GraphUsage;
import org.apache.hop.neo4j.core.data.GraphData;
import org.apache.hop.neo4j.core.data.GraphNodeData;
import org.apache.hop.neo4j.core.data.GraphPropertyData;
import org.apache.hop.neo4j.core.data.GraphPropertyDataType;
import org.apache.hop.neo4j.core.data.GraphRelationshipData;
import org.apache.hop.neo4j.model.GraphModel;
import org.apache.hop.neo4j.model.GraphNode;
import org.apache.hop.neo4j.model.GraphProperty;
import org.apache.hop.neo4j.model.GraphPropertyType;
import org.apache.hop.neo4j.model.GraphRelationship;
import org.apache.hop.neo4j.model.validation.ModelValidator;
import org.apache.hop.neo4j.model.validation.NodeProperty;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class GraphOutput extends BaseNeoTransform<GraphOutputMeta, GraphOutputData>
    implements ITransform<GraphOutputMeta, GraphOutputData> {

  public GraphOutput(
      TransformMeta transformMeta,
      GraphOutputMeta meta,
      GraphOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {

    try {
      if (!meta.isReturningGraph()) {
        // Verify some extra metadata...
        //
        if (StringUtils.isEmpty(meta.getConnectionName())) {
          log.logError("You need to specify a Neo4j connection to use in this transform");
          return false;
        }

        IHopMetadataSerializer<NeoConnection> serializer =
            metadataProvider.getSerializer(NeoConnection.class);
        data.neoConnection = serializer.load(meta.getConnectionName());
        if (data.neoConnection == null) {
          log.logError(
              "Connection '"
                  + meta.getConnectionName()
                  + "' could not be found in the metadata : "
                  + metadataProvider.getDescription());
          return false;
        }

        try {
          data.driver = data.neoConnection.getDriver(log, this);
          data.session = data.neoConnection.getSession(log, data.driver, this);
          data.version4 = data.neoConnection.isVersion4();
        } catch (Exception e) {
          log.logError(
              "Unable to get or create Neo4j database driver for database '"
                  + data.neoConnection.getName()
                  + "'",
              e);
          return false;
        }

        data.batchSize = Const.toLong(resolve(meta.getBatchSize()), 1);
      }

      if (StringUtils.isEmpty(meta.getModel())) {
        logError("No model name is specified");
        return false;
      }
      IHopMetadataSerializer<GraphModel> modelSerializer =
          metadataProvider.getSerializer(GraphModel.class);
      data.graphModel = modelSerializer.load(meta.getModel());
      if (data.graphModel == null) {
        logError("Model '" + meta.getModel() + "' could not be found!");
        return false;
      }

      data.modelValidator = null;
      if (meta.isValidatingAgainstModel()) {
        // Validate the model...
        //
        List<NodeProperty> usedNodeProperties = findUsedNodeProperties();
        data.modelValidator = new ModelValidator(data.graphModel, usedNodeProperties);
        int nrErrors = data.modelValidator.validateBeforeLoad(log, data.session);
        if (nrErrors > 0) {
          // There were validation errors, we can stop here...
          log.logError(
              "Validation against graph model '"
                  + data.graphModel.getName()
                  + "' failed with "
                  + nrErrors
                  + " errors.");
          return false;
        } else {
          log.logBasic(
              "Validation against graph model '" + data.graphModel.getName() + "' was successful.");
        }
      }
    } catch (HopException e) {
      log.logError("Could not find Neo4j connection'" + meta.getConnectionName() + "'", e);
      return false;
    }

    data.nodeCount = countDistinctNodes(meta.getFieldModelMappings());

    return super.init();
  }

  private List<NodeProperty> findUsedNodeProperties() {
    List<NodeProperty> list = new ArrayList<>();
    for (FieldModelMapping fieldModelMapping : meta.getFieldModelMappings()) {
      if (fieldModelMapping.getTargetType() == ModelTargetType.Node) {
        list.add(
            new NodeProperty(
                fieldModelMapping.getTargetName(), fieldModelMapping.getTargetProperty()));
      }
    }
    return list;
  }

  private int countDistinctNodes(List<FieldModelMapping> fieldModelMappings) {
    List<String> nodes = new ArrayList<>();
    for (FieldModelMapping mapping : fieldModelMappings) {
      if (!nodes.contains(mapping.getTargetName())) {
        nodes.add(mapping.getTargetName());
      }
    }
    return nodes.size();
  }

  @Override
  public void dispose() {

    wrapUpTransaction();

    if (data.session != null) {
      data.session.close();
    }
    if (data.driver != null) {
      data.driver.close();
    }
    if (data.cypherMap != null) {
      data.cypherMap.clear();
    }

    super.dispose();
  }

  @Override
  public boolean processRow() throws HopException {

    // Only if we actually have previous transform to read from...
    // This way the transform also acts as an GraphOutput query transform
    //
    Object[] row = getRow();
    if (row == null) {
      // End of transaction
      //
      wrapUpTransaction();

      // Signal next transform(s) we're done processing
      //
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      // get the output fields...
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(
          data.outputRowMeta, getTransformName(), null, getTransformMeta(), this, metadataProvider);

      // Get parameter field indexes
      data.fieldIndexes = new int[meta.getFieldModelMappings().size()];
      for (int i = 0; i < meta.getFieldModelMappings().size(); i++) {
        String field = meta.getFieldModelMappings().get(i).getField();
        data.fieldIndexes[i] = getInputRowMeta().indexOfValue(field);
        if (data.fieldIndexes[i] < 0) {
          throw new HopException("Unable to find parameter field '" + field);
        }
      }

      // Index all the mapped relationship properties
      // Relationship name --> Property --> field index
      //
      data.relationshipPropertyIndexMap = new HashMap<>();
      for (FieldModelMapping mapping : meta.getFieldModelMappings()) {
        if (mapping.getTargetType().equals(ModelTargetType.Relationship)) {
          String relationshipName = mapping.getTargetName();
          GraphRelationship relationship = data.graphModel.findRelationship(relationshipName);
          if (relationship == null) {
            throw new HopException(
                "Unable to find relationship '" + relationshipName + "' in the graph model");
          }
          String propertyName = mapping.getTargetProperty();
          GraphProperty graphProperty = relationship.findProperty(propertyName);
          if (graphProperty == null) {
            throw new HopException(
                "Unable to find relationship property '"
                    + relationshipName
                    + "."
                    + propertyName
                    + "' in the graph model");
          }

          String fieldName = mapping.getField();
          int fieldIndex = getInputRowMeta().indexOfValue(fieldName);
          if (fieldIndex < 0) {
            throw new HopException(
                "Unable to find field to map to relationship property: "
                    + relationshipName
                    + "."
                    + propertyName);
          }
          // Save the index...
          //
          Map<GraphProperty, Integer> propertyIndexMap =
              data.relationshipPropertyIndexMap.get(relationshipName);
          if (propertyIndexMap == null) {
            propertyIndexMap = new HashMap<>();
            data.relationshipPropertyIndexMap.put(relationshipName, propertyIndexMap);
          }
          // Find the property in the graph model...
          //
          propertyIndexMap.put(graphProperty, fieldIndex);
        }
      }

      if (!meta.isReturningGraph()) {

        // See if we need to create indexes...
        //
        if (meta.isCreatingIndexes()) {
          createNodePropertyIndexes(meta, data);
        }
      }

      data.cypherMap = new HashMap<>();
    }

    if (meta.isReturningGraph()) {

      //
      GraphData graphData =
          getGraphData(
              data.graphModel,
              meta.getFieldModelMappings(),
              data.nodeCount,
              row,
              getInputRowMeta(),
              data.fieldIndexes);

      Object[] outputRowData = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
      outputRowData[getInputRowMeta().size()] = graphData;

      putRow(data.outputRowMeta, outputRowData);

    } else {

      // Calculate cypher statement, parameters, ... based on field-model-mappings
      //
      Map<String, Object> parameters = new HashMap<>();
      String cypher =
          getCypher(
              data.graphModel,
              meta.getFieldModelMappings(),
              data.nodeCount,
              row,
              getInputRowMeta(),
              data.fieldIndexes,
              parameters);
      if (log.isDebug()) {
        logDebug("Parameters found : " + parameters.size());
        logDebug("Merge statement : " + cypher);
      }

      boolean errors = executeStatement(data, cypher, parameters);
      if (errors) {
        // Stop processing on error
        //
        setErrors(1L);
        setOutputDone();
        return false;
      }

      putRow(getInputRowMeta(), row);
    }
    return true;
  }

  private void createNodePropertyIndexes(GraphOutputMeta meta, GraphOutputData data)
      throws HopException {

    // Only try to create an index on the first transform copy
    //
    if (getCopy() > 0) {
      return;
    }

    Map<GraphNode, List<String>> nodePropertiesMap = new HashMap<>();

    for (int f = 0; f < meta.getFieldModelMappings().size(); f++) {
      FieldModelMapping fieldModelMapping = meta.getFieldModelMappings().get(f);
      if (fieldModelMapping.getTargetType() == ModelTargetType.Node) {
        // We pre-calculated the field indexes
        //
        int index = data.fieldIndexes[f];

        // Determine the target property and type
        //
        GraphNode node = data.graphModel.findNode(fieldModelMapping.getTargetName());
        if (node == null) {
          throw new HopException(
              "Unable to find target node '" + fieldModelMapping.getTargetName() + "'");
        }
        GraphProperty graphProperty = node.findProperty(fieldModelMapping.getTargetProperty());
        if (graphProperty == null) {
          throw new HopException(
              "Unable to find target property '"
                  + fieldModelMapping.getTargetProperty()
                  + "' of node '"
                  + fieldModelMapping.getTargetName()
                  + "'");
        }

        // See if this is a primary property...
        //
        if (graphProperty.isPrimary()) {

          List<String> propertiesList = nodePropertiesMap.get(node);
          if (propertiesList == null) {
            propertiesList = new ArrayList<>();
            nodePropertiesMap.put(node, propertiesList);
          }
          propertiesList.add(graphProperty.getName());
        }
      }
    }

    // Loop over map keys...
    //
    for (GraphNode node : nodePropertiesMap.keySet()) {
      NeoConnectionUtils.createNodeIndex(
          log, data.session, node.getLabels(), nodePropertiesMap.get(node));
    }
  }

  private boolean executeStatement(
      GraphOutputData data, String cypher, Map<String, Object> parameters) {
    boolean errors = false;
    if (data.batchSize <= 1) {
      Result result = data.session.run(cypher, parameters);
      errors = processSummary(result);
    } else {

      if (meta.isOutOfOrderAllowed()) {
        // Group the records into unwind statements...
        //
        if (data.unwindCount == 0) {
          // Create a new map between the cypher statement and the unwind list
          //
          data.unwindMapList = new HashMap<>();
        }

        // Add the statement to the list...
        //
        List<Map<String, Object>> unwindList =
            data.unwindMapList.computeIfAbsent(cypher, k -> new ArrayList<>());
        unwindList.add(parameters);

        data.unwindCount++;

        // See if it's time to write the statements to Neo4j...
        //
        if (data.unwindCount >= data.batchSize) {
          errors = emptyUnwindMap();
        }

      } else {
        // Normal batching
        //
        if (data.outputCount == 0) {
          data.transaction = data.session.beginTransaction();
        }

        Result result = data.transaction.run(cypher, parameters);
        errors = processSummary(result);

        data.outputCount++;
        incrementLinesOutput();

        if (!errors && data.outputCount >= data.batchSize) {
          data.transaction.commit();
          data.transaction.close();
          data.outputCount = 0;
        }
      }
    }

    if (errors) {
      setErrors(1L);
      stopAll();
      setOutputDone();
    }

    return errors;
  }

  private boolean emptyUnwindMap() {

    // See if there's actual work to be done...
    //
    if (data.unwindCount == 0 || data.unwindMapList == null || data.unwindMapList.isEmpty()) {
      return false;
    }

    boolean errors = false;
    for (String unwindCypher : data.unwindMapList.keySet()) {
      List<Map<String, Object>> unwindList = data.unwindMapList.get(unwindCypher);

      // The unwind parameters list is called "props" :
      //
      final Map<String, Object> props = Collections.singletonMap("props", unwindList);

      // Execute this unwind cypher statement...
      //
      Result result = data.session.writeTransaction(tx -> tx.run(unwindCypher, props));
      errors = processSummary(result);

      if (errors) {
        // The error is already logged, simply break out of the loop...
        //
        break;
      } else {
        setLinesOutput(getLinesOutput() + unwindList.size());
      }
    }

    data.unwindCount = 0;
    data.unwindMapList.clear();

    return errors;
  }

  private boolean processSummary(Result result) {
    boolean errors = false;
    ResultSummary summary = result.consume();
    for (Notification notification : summary.notifications()) {
      log.logError(notification.title() + " (" + notification.severity() + ")");
      log.logError(
          notification.code()
              + " : "
              + notification.description()
              + ", position "
              + notification.position());
      errors = true;
    }
    return errors;
  }

  private static class NodeAndPropertyData {
    public GraphNode node;
    public GraphProperty property;
    public IValueMeta sourceValueMeta;
    public Object sourceValueData;
    public int sourceFieldIndex;

    public NodeAndPropertyData(
        GraphNode node,
        GraphProperty property,
        IValueMeta sourceValueMeta,
        Object sourceValueData,
        int sourceFieldIndex) {
      this.node = node;
      this.property = property;
      this.sourceValueMeta = sourceValueMeta;
      this.sourceValueData = sourceValueData;
      this.sourceFieldIndex = sourceFieldIndex;
    }
  }

  private static class RelationshipAndPropertyData {
    public GraphRelationship relationship;
    public GraphProperty property;
    public IValueMeta sourceValueMeta;
    public Object sourceValueData;
    public int sourceFieldIndex;

    public RelationshipAndPropertyData(
        GraphRelationship relationship,
        GraphProperty property,
        IValueMeta sourceValueMeta,
        Object sourceValueData,
        int sourceFieldIndex) {
      this.relationship = relationship;
      this.property = property;
      this.sourceValueMeta = sourceValueMeta;
      this.sourceValueData = sourceValueData;
      this.sourceFieldIndex = sourceFieldIndex;
    }
  }

  /**
   * Generate the Cypher statement and parameters to use to update using a graph model, a field
   * mapping and a row of data
   *
   * @param graphModel The model to use
   * @param fieldModelMappings The mappings
   * @param nodeCount
   * @param row The input row
   * @param rowMeta the input row metadata
   * @param parameters The parameters map to update
   * @return The generated cypher statement
   */
  protected String getCypher(
      GraphModel graphModel,
      List<FieldModelMapping> fieldModelMappings,
      int nodeCount,
      Object[] row,
      IRowMeta rowMeta,
      int[] fieldIndexes,
      Map<String, Object> parameters)
      throws HopException {

    // We need to cache the Cypher and parameter mappings for performance
    // Basically this is determined by the bitmap of used fields being null or not null
    //
    StringBuffer pattern = new StringBuffer();
    for (int index : data.fieldIndexes) {
      boolean isNull = rowMeta.isNull(row, index);
      pattern.append(isNull ? '0' : '1');
    }
    CypherParameters cypherParameters = data.cypherMap.get(pattern.toString());
    if (cypherParameters != null) {
      setParameters(rowMeta, row, parameters, cypherParameters);

      // That's it, return the cypher we previously calculated
      //
      return cypherParameters.getCypher();
    }

    cypherParameters = new CypherParameters();

    // The strategy is to determine all the nodes involved and the properties to set.
    // Then we can determine the relationships between the nodes
    //
    List<GraphNode> nodes = new ArrayList<>();
    List<NodeAndPropertyData> nodeProperties = new ArrayList<>();
    for (int f = 0; f < fieldModelMappings.size(); f++) {
      FieldModelMapping fieldModelMapping = fieldModelMappings.get(f);

      if (fieldModelMapping.getTargetType() == ModelTargetType.Node) {
        // We pre-calculated the field indexes
        //
        int index = fieldIndexes[f];

        IValueMeta valueMeta = rowMeta.getValueMeta(index);
        Object valueData = row[index];

        // Determine the target property and type
        //
        GraphNode node = graphModel.findNode(fieldModelMapping.getTargetName());
        if (node == null) {
          throw new HopException(
              "Unable to find target node '" + fieldModelMapping.getTargetName() + "'");
        }
        GraphProperty graphProperty = node.findProperty(fieldModelMapping.getTargetProperty());
        if (graphProperty == null) {
          throw new HopException(
              "Unable to find target property '"
                  + fieldModelMapping.getTargetProperty()
                  + "' of node '"
                  + fieldModelMapping.getTargetName()
                  + "'");
        }
        if (!nodes.contains(node)) {
          nodes.add(node);
        }
        nodeProperties.add(
            new NodeAndPropertyData(node, graphProperty, valueMeta, valueData, index));
      }
    }

    // Evaluate whether or not the node property is primary and null
    // In that case, we remove these nodes from the lists...
    //
    Set<GraphNode> ignored = new HashSet<>();
    for (NodeAndPropertyData nodeProperty : nodeProperties) {
      if (nodeProperty.property.isPrimary()) {
        // Null value?
        //
        if (nodeProperty.sourceValueMeta.isNull(nodeProperty.sourceValueData)) {
          if (log.isDebug()) {
            logDebug(
                "Detected primary null property for node "
                    + nodeProperty.node
                    + " property "
                    + nodeProperty.property
                    + " value : "
                    + nodeProperty.sourceValueMeta.getString(nodeProperty.sourceValueData));
          }

          if (!ignored.contains(nodeProperty.node)) {
            ignored.add(nodeProperty.node);
          }
        }
      }
    }

    // Now we'll see which relationships are involved between any 2 nodes.
    // Then we can generate the cypher statement as well...
    //
    // v1.0 vanilla algorithm test
    //
    List<GraphRelationship> relationships = new ArrayList<>();
    for (int x = 0; x < nodes.size(); x++) {
      for (int y = 0; y < nodes.size(); y++) {
        if (x == y) {
          continue;
        }
        GraphNode sourceNode = nodes.get(x);
        GraphNode targetNode = nodes.get(y);

        GraphRelationship relationship =
            graphModel.findRelationship(sourceNode.getName(), targetNode.getName());
        if (relationship != null) {
          if (!relationships.contains(relationship)) {
            // A new relationship we don't have yet.
            //
            relationships.add(relationship);
          }
        }
      }
    }

    if (log.isDebug()) {
      logDebug(
          "Found "
              + relationships.size()
              + " relationships to consider : "
              + relationships.toString());
      logDebug("Found " + ignored.size() + " nodes to ignore : " + ignored.toString());
    }

    // Now we have a bunch of Node-Pairs to update...
    //
    int relationshipIndex = 0;
    AtomicInteger parameterIndex = new AtomicInteger(0);
    AtomicInteger nodeIndex = new AtomicInteger(0);

    StringBuilder cypher = new StringBuilder();

    Set<GraphNode> handled = new HashSet<>();
    Map<GraphNode, Integer> nodeIndexMap = new HashMap<>();

    if (meta.isOutOfOrderAllowed()) {
      cypher.append("UNWIND $props AS pr ");
      cypher.append(Const.CR);
    }

    // No relationships case...
    //
    if (nodes.size() == 1) {
      GraphNode node = nodes.get(0);
      addNodeCypher(
          cypher,
          node,
          handled,
          ignored,
          parameterIndex,
          nodeIndex,
          nodeIndexMap,
          nodeProperties,
          parameters,
          cypherParameters);
    } else {
      for (GraphRelationship relationship : relationships) {
        relationshipIndex++;
        if (log.isDebug()) {
          logDebug("Handling relationship : " + relationship.getName());
        }
        GraphNode nodeSource = graphModel.findNode(relationship.getNodeSource());
        GraphNode nodeTarget = graphModel.findNode(relationship.getNodeTarget());

        for (GraphNode node : new GraphNode[] {nodeSource, nodeTarget}) {
          addNodeCypher(
              cypher,
              node,
              handled,
              ignored,
              parameterIndex,
              nodeIndex,
              nodeIndexMap,
              nodeProperties,
              parameters,
              cypherParameters);
        }

        // Now add the merge on the relationship...
        //
        if (nodeIndexMap.get(nodeSource) != null && nodeIndexMap.get(nodeTarget) != null) {
          String sourceNodeName = "node" + nodeIndexMap.get(nodeSource);
          String targetNodeName = "node" + nodeIndexMap.get(nodeTarget);
          String relationshipAlias = "rel" + relationshipIndex;

          cypher.append(
              "MERGE("
                  + sourceNodeName
                  + ")-["
                  + relationshipAlias
                  + ":"
                  + relationship.getLabel()
                  + "]-("
                  + targetNodeName
                  + ") ");
          cypher.append(Const.CR);

          // Also add the optional property updates...
          //
          List<GraphProperty> relProps = new ArrayList<>();
          List<Integer> relPropIndexes = new ArrayList<>();
          Map<GraphProperty, Integer> propertyIndexMap =
              data.relationshipPropertyIndexMap.get(relationship.getName());
          if (propertyIndexMap != null) {
            for (GraphProperty relProp : propertyIndexMap.keySet()) {
              Integer relFieldIndex = propertyIndexMap.get(relProp);
              if (relFieldIndex != null) {
                relProps.add(relProp);
                relPropIndexes.add(relFieldIndex);
              }
            }
          }
          if (!relProps.isEmpty()) {
            // Add a set clause...
            //
            cypher.append("SET ");
            for (int i = 0; i < relProps.size(); i++) {

              parameterIndex.incrementAndGet();
              String parameterName = "param" + parameterIndex;

              if (i > 0) {
                cypher.append(", ");
              }
              GraphProperty relProp = relProps.get(i);
              int propFieldIndex = relPropIndexes.get(i);

              IValueMeta sourceFieldMeta = rowMeta.getValueMeta(propFieldIndex);
              Object sourceFieldValue = row[propFieldIndex];
              boolean isNull = sourceFieldMeta.isNull(sourceFieldValue);

              cypher.append(relationshipAlias + "." + relProp.getName());
              cypher.append(" = ");
              if (isNull) {
                cypher.append("NULL");
              } else {
                Object neoValue =
                    relProp.getType().convertFromHop(sourceFieldMeta, sourceFieldValue);
                parameters.put(parameterName, neoValue);
                cypher.append(buildParameterClause(parameterName));

                TargetParameter targetParameter =
                    new TargetParameter(
                        sourceFieldMeta.getName(),
                        propFieldIndex,
                        parameterName,
                        relProp.getType());
                cypherParameters.getTargetParameters().add(targetParameter);
              }
            }
            cypher.append(Const.CR);
          }

          updateUsageMap(Arrays.asList(relationship.getLabel()), GraphUsage.RELATIONSHIP_UPDATE);
        }
      }
      cypher.append(";" + Const.CR);
    }

    cypherParameters.setCypher(cypher.toString());
    data.cypherMap.put(pattern.toString(), cypherParameters);

    return cypher.toString();
  }

  private void setParameters(
      IRowMeta rowMeta,
      Object[] row,
      Map<String, Object> parameters,
      CypherParameters cypherParameters)
      throws HopValueException {
    for (TargetParameter targetParameter : cypherParameters.getTargetParameters()) {
      int fieldIndex = targetParameter.getInputFieldIndex();
      IValueMeta valueMeta = rowMeta.getValueMeta(fieldIndex);
      Object valueData = row[fieldIndex];
      String parameterName = targetParameter.getParameterName();
      GraphPropertyType parameterType = targetParameter.getParameterType();

      // Convert to the neo type
      //
      Object neoObject = parameterType.convertFromHop(valueMeta, valueData);

      parameters.put(parameterName, neoObject);
    }
  }

  private void addNodeCypher(
      StringBuilder cypher,
      GraphNode node,
      Set<GraphNode> handled,
      Set<GraphNode> ignored,
      AtomicInteger parameterIndex,
      AtomicInteger nodeIndex,
      Map<GraphNode, Integer> nodeIndexMap,
      List<NodeAndPropertyData> nodeProperties,
      Map<String, Object> parameters,
      CypherParameters cypherParameters)
      throws HopValueException {
    if (!ignored.contains(node) && !handled.contains(node)) {

      // Don't update twice.
      //
      handled.add(node);
      nodeIndexMap.put(node, nodeIndex.incrementAndGet());

      // Calculate the node labels
      //
      String nodeLabels = "";
      for (String nodeLabel : node.getLabels()) {
        nodeLabels += ":";
        nodeLabels += nodeLabel;
      }

      StringBuilder matchCypher = new StringBuilder();

      String nodeAlias = "node" + nodeIndex;

      cypher.append("MERGE (" + nodeAlias + nodeLabels + " { ");

      updateUsageMap(node.getLabels(), GraphUsage.NODE_UPDATE);

      if (log.isDebug()) {
        logBasic(" - node merge : " + node.getName());
      }

      // Look up the properties to update in the node
      //
      boolean firstPrimary = true;
      boolean firstMatch = true;
      for (NodeAndPropertyData napd : nodeProperties) {
        if (napd.node.equals(node)) {
          // Handle the property
          //
          parameterIndex.incrementAndGet();
          boolean isNull = napd.sourceValueMeta.isNull(napd.sourceValueData);
          String parameterName = "param" + parameterIndex;

          if (napd.property.isPrimary()) {

            if (!firstPrimary) {
              cypher.append(", ");
            }
            cypher.append(
                napd.property.getName() + " : " + buildParameterClause(parameterName) + " ");

            firstPrimary = false;

            if (log.isDebug()) {
              logBasic(
                  "   * property match/create : "
                      + napd.property.getName()
                      + " with value "
                      + napd.sourceValueMeta.toStringMeta()
                      + " : "
                      + napd.sourceValueMeta.getString(napd.sourceValueData));
            }

          } else {
            // On match statement
            //
            if (firstMatch) {
              matchCypher.append("SET ");
            } else {
              matchCypher.append(", ");
            }

            firstMatch = false;

            matchCypher.append(nodeAlias + "." + napd.property.getName() + " = ");
            if (isNull) {
              matchCypher.append("NULL ");
            } else {
              matchCypher.append(buildParameterClause(parameterName) + " ");
            }

            if (log.isDebug()) {
              logBasic(
                  "   * property update : "
                      + napd.property.getName()
                      + " with value "
                      + napd.sourceValueMeta.toStringMeta()
                      + " : "
                      + napd.sourceValueMeta.getString(napd.sourceValueData));
            }
          }

          // NULL parameters are better set with NULL directly
          //
          if (!isNull) {
            parameters.put(
                parameterName,
                napd.property.getType().convertFromHop(napd.sourceValueMeta, napd.sourceValueData));
            TargetParameter targetParameter =
                new TargetParameter(
                    napd.sourceValueMeta.getName(),
                    napd.sourceFieldIndex,
                    parameterName,
                    napd.property.getType());
            cypherParameters.getTargetParameters().add(targetParameter);
          }
        }
      }
      cypher.append("}) " + Const.CR);

      // Add a SET clause if there are any non-primary key fields to update
      //
      if (matchCypher.length() > 0) {
        cypher.append(matchCypher);
      }
    }
  }

  private String buildParameterClause(String parameterName) {
    if (meta.isOutOfOrderAllowed()) {
      return "pr." + parameterName;
    } else {
      return "$" + parameterName;
    }
  }

  @Override
  public void batchComplete() {
    wrapUpTransaction();
  }

  private void wrapUpTransaction() {

    if (meta.isOutOfOrderAllowed()) {
      boolean errors = emptyUnwindMap();
      if (errors) {
        stopAll();
        setErrors(1L);
      }
    } else {
      if (data.outputCount > 0) {
        data.transaction.commit();
        data.transaction.close();

        // Force creation of a new transaction on the next batch of records
        //
        data.outputCount = 0;
      }
    }
  }

  /**
   * Update the usagemap. Add all the labels to the node usage.
   *
   * @param nodeLabels
   * @param usage
   */
  protected void updateUsageMap(List<String> nodeLabels, GraphUsage usage)
      throws HopValueException {
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

    for (String label : nodeLabels) {
      if (StringUtils.isNotEmpty(label)) {
        labelSet.add(label);
      }
    }
  }

  /**
   * Generate the Cypher statement and parameters to use to update using a graph model, a field
   * mapping and a row of data
   *
   * @param graphModel The model to use
   * @param fieldModelMappings The mappings
   * @param nodeCount
   * @param row The input row
   * @param rowMeta the input row metadata
   * @return The graph with nodes and relationships
   */
  protected GraphData getGraphData(
      GraphModel graphModel,
      List<FieldModelMapping> fieldModelMappings,
      int nodeCount,
      Object[] row,
      IRowMeta rowMeta,
      int[] fieldIndexes)
      throws HopException {

    GraphData graphData = new GraphData();
    graphData.setSourcePipelineName(getPipelineMeta().getName());
    graphData.setSourceTransformName(getTransformMeta().getName());

    // The strategy is to determine all the nodes involved and the properties to set.
    // Then we can determine the relationships between the nodes
    //
    List<GraphNode> nodes = new ArrayList<>();
    List<GraphRelationship> relationships = new ArrayList<>();
    List<NodeAndPropertyData> nodeProperties = new ArrayList<>();
    List<RelationshipAndPropertyData> relationshipProperties = new ArrayList<>();
    for (int f = 0; f < fieldModelMappings.size(); f++) {
      FieldModelMapping fieldModelMapping = fieldModelMappings.get(f);

      // We pre-calculated the field indexes
      //
      int index = fieldIndexes[f];

      IValueMeta valueMeta = rowMeta.getValueMeta(index);
      Object valueData = row[index];

      // Determine the target property and type
      //
      if (fieldModelMapping.getTargetType() == ModelTargetType.Node) {
        GraphNode node = graphModel.findNode(fieldModelMapping.getTargetName());
        if (node == null) {
          throw new HopException(
              "Unable to find target node '" + fieldModelMapping.getTargetName() + "'");
        }
        GraphProperty graphProperty = node.findProperty(fieldModelMapping.getTargetProperty());
        if (graphProperty == null) {
          throw new HopException(
              "Unable to find target property '"
                  + fieldModelMapping.getTargetProperty()
                  + "' of node '"
                  + fieldModelMapping.getTargetName()
                  + "'");
        }
        if (!nodes.contains(node)) {
          nodes.add(node);
        }
        nodeProperties.add(
            new NodeAndPropertyData(node, graphProperty, valueMeta, valueData, index));
      } else {
        // Relationship
        //
        GraphRelationship relationship =
            graphModel.findRelationship(fieldModelMapping.getTargetName());
        if (relationship == null) {
          throw new HopException(
              "Unable to find target relationship '" + fieldModelMapping.getTargetName() + "'");
        }
        GraphProperty graphProperty =
            relationship.findProperty(fieldModelMapping.getTargetProperty());
        if (graphProperty == null) {
          throw new HopException(
              "Unable to find target property '"
                  + fieldModelMapping.getTargetProperty()
                  + "' of relationship '"
                  + fieldModelMapping.getTargetName()
                  + "'");
        }
        if (!relationships.contains(relationship)) {
          relationships.add(relationship);
        }
        relationshipProperties.add(
            new RelationshipAndPropertyData(
                relationship, graphProperty, valueMeta, valueData, index));
      }
    }

    // Evaluate whether or not the node property is primary and null
    // In that case, we remove these nodes from the lists...
    //
    Set<GraphNode> ignored = new HashSet<>();
    for (NodeAndPropertyData nodeProperty : nodeProperties) {
      if (nodeProperty.property.isPrimary()) {
        // Null value?
        //
        if (nodeProperty.sourceValueMeta.isNull(nodeProperty.sourceValueData)) {
          if (log.isDebug()) {
            logDebug(
                "Detected primary null property for node "
                    + nodeProperty.node
                    + " property "
                    + nodeProperty.property
                    + " value : "
                    + nodeProperty.sourceValueMeta.getString(nodeProperty.sourceValueData));
          }

          if (!ignored.contains(nodeProperty.node)) {
            ignored.add(nodeProperty.node);
          }
        }
      }
    }

    // Now we'll see which relationships are involved between any 2 nodes.
    // Then we can generate the cypher statement as well...
    //
    // v1.0 vanilla algorithm test
    //
    for (int x = 0; x < nodes.size(); x++) {
      for (int y = 0; y < nodes.size(); y++) {
        if (x == y) {
          continue;
        }
        GraphNode sourceNode = nodes.get(x);
        GraphNode targetNode = nodes.get(y);

        GraphRelationship relationship =
            graphModel.findRelationship(sourceNode.getName(), targetNode.getName());
        if (relationship != null) {
          if (!relationships.contains(relationship)) {
            // A new relationship we don't have yet.
            //
            relationships.add(relationship);
          }
        }
      }
    }

    // Now we have a bunch of Node-Pairs to update...
    //
    AtomicInteger nodeIndex = new AtomicInteger(0);

    Set<GraphNode> handled = new HashSet<>();
    Map<GraphNode, Integer> nodeIndexMap = new HashMap<>();

    // No relationships case...
    //
    if (nodes.size() == 1) {
      GraphNode node = nodes.get(0);

      GraphNodeData nodeData =
          getGraphNodeData(node, handled, ignored, nodeIndex, nodeIndexMap, nodeProperties);
      if (nodeData != null) {
        graphData.getNodes().add(nodeData);
      }
    } else {
      for (GraphRelationship relationship : relationships) {
        GraphNode nodeSource = graphModel.findNode(relationship.getNodeSource());
        GraphNode nodeTarget = graphModel.findNode(relationship.getNodeTarget());

        for (GraphNode node : new GraphNode[] {nodeSource, nodeTarget}) {
          GraphNodeData nodeData =
              getGraphNodeData(node, handled, ignored, nodeIndex, nodeIndexMap, nodeProperties);
          if (nodeData != null) {
            graphData.getNodes().add(nodeData);
          }
        }

        // Now add the relationship...
        //
        if (nodeIndexMap.get(nodeSource) != null && nodeIndexMap.get(nodeTarget) != null) {

          String sourceNodeId = getGraphNodeDataId(nodeSource, nodeProperties);
          String targetNodeId = getGraphNodeDataId(nodeTarget, nodeProperties);

          String id = sourceNodeId + " -> " + targetNodeId;

          GraphRelationshipData relationshipData = new GraphRelationshipData();
          relationshipData.setId(id);
          relationshipData.setLabel(relationship.getLabel());
          relationshipData.setSourceNodeId(sourceNodeId);
          relationshipData.setTargetNodeId(targetNodeId);

          // The property set ID is simply the name of the GraphRelationship (metadata)
          //
          relationshipData.setPropertySetId(relationship.getName());

          // Also add the optional properties...
          //
          List<GraphProperty> relProps = new ArrayList<>();
          List<Integer> relPropIndexes = new ArrayList<>();
          Map<GraphProperty, Integer> propertyIndexMap =
              data.relationshipPropertyIndexMap.get(relationship.getName());
          if (propertyIndexMap != null) {
            for (GraphProperty relProp : propertyIndexMap.keySet()) {
              Integer relFieldIndex = propertyIndexMap.get(relProp);
              if (relFieldIndex != null) {
                relProps.add(relProp);
                relPropIndexes.add(relFieldIndex);
              }
            }
          }
          if (!relProps.isEmpty()) {
            for (int i = 0; i < relProps.size(); i++) {

              GraphProperty relProp = relProps.get(i);
              int propFieldIndex = relPropIndexes.get(i);

              IValueMeta sourceFieldMeta = rowMeta.getValueMeta(propFieldIndex);
              Object sourceFieldValue = row[propFieldIndex];

              String propId = relProp.getName();
              GraphPropertyDataType relPropType =
                  GraphPropertyDataType.getTypeFromHop(sourceFieldMeta);
              Object neoValue = relPropType.convertFromHop(sourceFieldMeta, sourceFieldValue);
              boolean primary = false; // TODO: implement this

              relationshipData
                  .getProperties()
                  .add(new GraphPropertyData(propId, neoValue, relPropType, primary));
            }
          }

          graphData.getRelationships().add(relationshipData);
        }
      }
    }

    return graphData;
  }

  private GraphNodeData getGraphNodeData(
      GraphNode node,
      Set<GraphNode> handled,
      Set<GraphNode> ignored,
      AtomicInteger nodeIndex,
      Map<GraphNode, Integer> nodeIndexMap,
      List<NodeAndPropertyData> nodeProperties)
      throws HopValueException {
    if (!ignored.contains(node) && !handled.contains(node)) {

      GraphNodeData graphNodeData = new GraphNodeData();

      // The property set ID is simply the name of the GraphNode (metadata)
      //
      graphNodeData.setPropertySetId(node.getName());

      // Don't update twice.
      //
      handled.add(node);
      nodeIndexMap.put(node, nodeIndex.incrementAndGet());

      // Calculate the node labels
      //
      graphNodeData.getLabels().addAll(node.getLabels());

      // Look up the properties to update in the node
      //
      boolean firstPrimary = true;
      boolean firstMatch = true;
      for (NodeAndPropertyData napd : nodeProperties) {
        if (napd.node.equals(node)) {
          // Handle the property
          //
          boolean isNull = napd.sourceValueMeta.isNull(napd.sourceValueData);

          if (napd.property.isPrimary()) {

            String oldId = graphNodeData.getId();
            String propertyString = napd.sourceValueMeta.getString(napd.sourceValueData);
            if (oldId == null) {
              graphNodeData.setId(propertyString);
            } else {
              graphNodeData.setId(oldId + "-" + propertyString);
            }
          }

          if (!isNull) {
            GraphPropertyData propertyData = new GraphPropertyData();
            propertyData.setId(napd.property.getName());
            GraphPropertyDataType type = GraphPropertyDataType.getTypeFromHop(napd.sourceValueMeta);
            propertyData.setType(type);
            propertyData.setValue(type.convertFromHop(napd.sourceValueMeta, napd.sourceValueData));
            propertyData.setPrimary(napd.property.isPrimary());

            graphNodeData.getProperties().add(propertyData);
          }
        }
      }
      return graphNodeData;
    }
    return null;
  }

  public String getGraphNodeDataId(GraphNode node, List<NodeAndPropertyData> nodeProperties)
      throws HopValueException {

    StringBuffer id = new StringBuffer();

    for (NodeAndPropertyData napd : nodeProperties) {
      if (napd.node.equals(node)) {
        if (napd.property.isPrimary()) {

          String propertyString = napd.sourceValueMeta.getString(napd.sourceValueData);
          if (id.length() > 0) {
            id.append("-");
          }
          id.append(propertyString);
        }
      }
    }
    return id.toString();
  }
}
