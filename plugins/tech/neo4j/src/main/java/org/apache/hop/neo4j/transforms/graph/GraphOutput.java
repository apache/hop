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

      // Perform a few sanity checks...
      //
      data.graphModel.validateIntegrity();

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

      // Relationship mappings field indexes
      //
      data.relMappingIndexes = new ArrayList();
      data.fieldValueRelationshipMap = new HashMap<>();
      for (int i = 0; i < meta.getRelationshipMappings().size(); i++) {
        RelationshipMapping relationshipMapping = meta.getRelationshipMappings().get(i);
        GraphOutputData.RelelationshipMappingIndexes mappingIndexes =
            new GraphOutputData.RelelationshipMappingIndexes();

        // If we update a relationship with a specific label, index the field used.
        //
        if (relationshipMapping.getType() == RelationshipMappingType.UsingValue) {
          String field = relationshipMapping.getFieldName();
          if (StringUtils.isEmpty(field)) {
            throw new HopException(
                "Please specify a field to use to select a relationship for mapping: "
                    + relationshipMapping);
          }
          if (StringUtils.isEmpty(relationshipMapping.getFieldValue())) {
            throw new HopException(
                "Please specify a field value to use to select a relationship for mapping: "
                    + relationshipMapping);
          }
          if (StringUtils.isEmpty(relationshipMapping.getTargetRelationship())) {
            throw new HopException(
                "Please specify a relationship to map to for relationship mapping: "
                    + relationshipMapping);
          }
          mappingIndexes.fieldIndex = getInputRowMeta().indexOfValue(field);
          if (mappingIndexes.fieldIndex < 0) {
            throw new HopException(
                "Unable to find relationship mapping field '"
                    + field
                    + "' to update a specific relationship");
          }

          // Can we find the specified relationship?
          //
          GraphRelationship relationship =
              data.graphModel.findRelationship(relationshipMapping.getTargetRelationship());
          if (relationship == null) {
            throw new HopException(
                "Unable to find relationship mapping target mapping '"
                    + relationshipMapping.getTargetRelationship());
          }

          // Also create a map between the specified value and the relationship
          //
          Map<String, GraphRelationship> valueRelationshipMap =
              data.fieldValueRelationshipMap.computeIfAbsent(field, k -> new HashMap<>());
          valueRelationshipMap.put(
              Const.NVL(relationshipMapping.getFieldValue(), ""), relationship);
        }

        data.relMappingIndexes.add(mappingIndexes);
      }

      // Index all the mapped relationship properties
      // Relationship name --> Property --> field index
      //
      data.relationshipPropertyIndexMap = new HashMap<>();
      Map<String, Set<ModelTargetHint>> nodeTargetHintMap = new HashMap<>();
      for (FieldModelMapping mapping : meta.getFieldModelMappings()) {
        if (mapping.getTargetType() == ModelTargetType.Relationship) {
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
              data.relationshipPropertyIndexMap.computeIfAbsent(
                  relationshipName, k -> new HashMap<>());

          // Find the property in the graph model...
          //
          propertyIndexMap.put(graphProperty, fieldIndex);
        } else if (mapping.getTargetType() == ModelTargetType.Node) {
          // Keep track of the ways that a particular node is addressed
          //
          Set<ModelTargetHint> modelTargetHints =
              nodeTargetHintMap.computeIfAbsent(mapping.getTargetName(), f -> new HashSet<>());
          modelTargetHints.add(mapping.getTargetHint());
        }
      }

      // Let's now see if there are nodes addressed by SelfSource/SelfTarget but also addressed with
      // None.
      //
      for (String nodeName : nodeTargetHintMap.keySet()) {
        Set<ModelTargetHint> hints = nodeTargetHintMap.get(nodeName);
        if (hints.contains(ModelTargetHint.SelfRelationshipSource)
                && hints.contains(ModelTargetHint.None)
            || hints.contains(ModelTargetHint.SelfRelationshipTarget)
                && hints.contains(ModelTargetHint.None)) {
          throw new HopException(
              "There are mappings to node "
                  + nodeName
                  + " with both a specific Self relationship target and None. This is not allowed.");
        }
      }

      // Node mapping field indexes
      //
      data.nodeMappingIndexes = new ArrayList<>();
      data.nodeValueLabelMap = new HashMap<>();
      for (int i = 0; i < meta.getNodeMappings().size(); i++) {
        NodeMapping nodeMapping = meta.getNodeMappings().get(i);

        // Sanity checking...
        //
        if (StringUtils.isEmpty(nodeMapping.getTargetNode())) {
          throw new HopException(
              "Please specify a valid target node for node mapping: " + nodeMapping);
        }
        GraphNode targetNode = data.graphModel.findNode(nodeMapping.getTargetNode());
        if (targetNode == null) {
          throw new HopException(
              "Target node '"
                  + nodeMapping.getTargetNode()
                  + "' can not be found in the graph model. Specified in node mapping: "
                  + nodeMapping);
        }

        int index = -1;
        if (nodeMapping.getType() == NodeMappingType.UsingValue) {
          if (StringUtils.isEmpty(nodeMapping.getFieldName())) {
            throw new HopException(
                "Please specify an input field name to use to determine the label to use in node mapping: "
                    + nodeMapping);
          }
          if (StringUtils.isEmpty(nodeMapping.getFieldValue())) {
            throw new HopException(
                "Please specify a field value to use to determine the label to use in node mapping: "
                    + nodeMapping);
          }
          if (StringUtils.isEmpty(nodeMapping.getTargetLabel())) {
            throw new HopException("Please specify a target label in node mapping: " + nodeMapping);
          }
          if (!targetNode.getLabels().contains(nodeMapping.getTargetLabel())) {
            throw new HopException(
                "The specified target label '"
                    + nodeMapping.getTargetLabel()
                    + "' doesn't exist in the graph model for node '"
                    + targetNode.getName()
                    + "'");
          }

          index = getInputRowMeta().indexOfValue(nodeMapping.getFieldName());
          if (index < 0) {
            throw new HopException(
                "Unable to find field '"
                    + nodeMapping.getFieldName()
                    + "' in node mapping: "
                    + nodeMapping);
          }

          // Also keep track of the field value to node label mapping:
          //
          Map<String, String> valueLabelMap =
              data.nodeValueLabelMap.computeIfAbsent(
                  nodeMapping.getTargetNode(), f -> new HashMap<>());
          valueLabelMap.put(nodeMapping.getFieldValue(), nodeMapping.getTargetLabel());
        }
        data.nodeMappingIndexes.add(index);
      }

      if (!meta.isReturningGraph()) {

        // See if we need to create indexes...
        //
        if (meta.isCreatingIndexes()) {
          createNodePropertyIndexes(meta, data);
        }
      }

      data.relationshipsCache = new HashMap<>();
      data.cypherMap = new HashMap<>();
    }

    if (meta.isReturningGraph()) {

      //
      GraphData graphData = getGraphData(row, getInputRowMeta());

      Object[] outputRowData = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
      outputRowData[getInputRowMeta().size()] = graphData;

      putRow(data.outputRowMeta, outputRowData);

    } else {

      // Calculate cypher statement, parameters, ... based on field-model-mappings
      //
      Map<String, Object> parameters = new HashMap<>();
      String cypher = getCypher(row, getInputRowMeta(), parameters);
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
    public SelectedNode node;
    public GraphProperty property;
    public IValueMeta sourceValueMeta;
    public Object sourceValueData;
    public int sourceFieldIndex;

    public NodeAndPropertyData(
        SelectedNode node,
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

  private class SelectedNodesAndRelationships {
    public Set<SelectedNode> nodes;
    public Set<SelectedRelationship> relationships;
    public Set<SelectedRelationship> avoided;
    public List<NodeAndPropertyData> nodeProperties;
    public Set<SelectedNode> ignored;

    public SelectedNodesAndRelationships(
        Set<SelectedNode> nodes,
        Set<SelectedRelationship> relationships,
        Set<SelectedRelationship> avoided,
        List<NodeAndPropertyData> nodeProperties,
        Set<SelectedNode> ignored) {
      this.nodes = nodes;
      this.relationships = relationships;
      this.avoided = avoided;
      this.nodeProperties = nodeProperties;
      this.ignored = ignored;
    }
  }

  /**
   * Generate the Cypher statement and parameters to use to update using a graph model, a field
   * mapping and a row of data
   *
   * @param row The input row
   * @param rowMeta the input row metadata
   * @param parameters The parameters map to update
   * @return The generated cypher statement
   */
  protected String getCypher(Object[] row, IRowMeta rowMeta, Map<String, Object> parameters)
      throws HopException {

    // We need to cache the Cypher and parameter mappings for performance
    // Basically this is determined by the bitmap of used fields being null or not null
    //
    String pattern = buildCypherKeyPattern(row, rowMeta);
    CypherParameters cypherParameters = data.cypherMap.get(pattern);
    if (cypherParameters != null) {
      setParameters(rowMeta, row, parameters, cypherParameters);

      // That's it, return the cypher we previously calculated
      //
      return cypherParameters.getCypher();
    }

    cypherParameters = new CypherParameters();

    SelectedNodesAndRelationships nar = selectNodesAndRelationships(rowMeta, row);

    // Now we have a bunch of Node-Pairs to update...
    //
    int relationshipIndex = 0;
    AtomicInteger parameterIndex = new AtomicInteger(0);
    AtomicInteger nodeIndex = new AtomicInteger(0);

    StringBuilder cypher = new StringBuilder();

    Set<SelectedNode> handled = new HashSet<>();
    Map<SelectedNode, Integer> nodeIndexMap = new HashMap<>();

    if (meta.isOutOfOrderAllowed()) {
      cypher.append("UNWIND $props AS pr ");
      cypher.append(Const.CR);
    }

    // No relationships case: only node(s)
    //
    if (nar.relationships.isEmpty()) {
      if (nar.nodes.isEmpty()) {
        throw new HopException(
            "We didn't find a node to write to.  Did you specify a field mapping to node properties?");
      }
      if (nar.nodes.size() > 1) {
        log.logBasic("Warning: writing to multiple nodes but not to any relationships");
      }
      for (SelectedNode node : nar.nodes) {
        addNodeCypher(
            cypher,
            node,
            handled,
            nar.ignored,
            parameterIndex,
            nodeIndex,
            nodeIndexMap,
            nar.nodeProperties,
            parameters,
            cypherParameters);
      }
    } else {
      // Relationships & nodes
      //
      for (SelectedRelationship selectedRelationship : nar.relationships) {
        if (nar.avoided.contains(selectedRelationship)) {
          continue;
        }

        SelectedNode sourceNode = selectedRelationship.getSourceNode();
        SelectedNode targetNode = selectedRelationship.getTargetNode();
        GraphRelationship relationship = selectedRelationship.getRelationship();

        relationshipIndex++;
        if (log.isDebug()) {
          logDebug("Handling relationship : " + relationship.getName());
        }
        // Add the source node to the cypher
        //
        addNodeCypher(
            cypher,
            sourceNode,
            handled,
            nar.ignored,
            parameterIndex,
            nodeIndex,
            nodeIndexMap,
            nar.nodeProperties,
            parameters,
            cypherParameters);

        // Add the target node
        //
        addNodeCypher(
            cypher,
            targetNode,
            handled,
            nar.ignored,
            parameterIndex,
            nodeIndex,
            nodeIndexMap,
            nar.nodeProperties,
            parameters,
            cypherParameters);

        // Now add the merge on the relationship...
        //
        if (nodeIndexMap.get(sourceNode) != null && nodeIndexMap.get(targetNode) != null) {
          String sourceNodeName = "node" + nodeIndexMap.get(sourceNode);
          String targetNodeName = "node" + nodeIndexMap.get(targetNode);
          String relationshipAlias = "rel" + relationshipIndex;

          cypher.append(
              "MERGE("
                  + sourceNodeName
                  + ")-["
                  + relationshipAlias
                  + ":"
                  + relationship.getLabel()
                  + "]->("
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
    data.cypherMap.put(pattern, cypherParameters);

    return cypher.toString();
  }

  private SelectedNodesAndRelationships selectNodesAndRelationships(IRowMeta rowMeta, Object[] row)
      throws HopException {

    // The strategy is to determine all the nodes involved and the properties to set.
    // Then we can determine the relationships between the nodes
    //
    List<NodeAndPropertyData> nodeProperties = new ArrayList<>();
    Set<SelectedNode> nodesSet = getInvolvedNodes(row, rowMeta, nodeProperties);

    // Evaluate if the node property is primary and null
    // In that case, we remove these nodes from the lists...
    //
    Set<SelectedNode> ignored = new HashSet<>();
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

          ignored.add(nodeProperty.node);
        }
      }
    }

    // Now we'll see which relationships are involved between any 2 nodes.
    // Then we can generate the cypher statement as well...
    //
    // v1.0 vanilla algorithm test
    //
    Set<SelectedRelationship> selectedRelationships = new HashSet<>();
    Set<SelectedRelationship> avoidedRelationships = new HashSet<>();

    for (SelectedNode source : nodesSet) {
      for (SelectedNode target : nodesSet) {
        // Skip when we have invalid combinations
        if (source.equals(target)) {
          continue;
        }

        String sourceNodeName = source.getNode().getName();
        String targetNodeName = target.getNode().getName();

        // Do we have a relationship in the model?
        // For performance this uses a hashmaps to cache these graph model lookups.
        //
        List<GraphRelationship> relationships = findRelationships(sourceNodeName, targetNodeName);
        if (relationships == null || relationships.isEmpty()) {
          continue;
        }

        // Stick to the source-target as selected, nothing else
        //
        if (source.getHint() == ModelTargetHint.SelfRelationshipTarget
            || target.getHint() == ModelTargetHint.SelfRelationshipSource) {
          continue;
        }

        for (GraphRelationship relationship : relationships) {
          // Check the mappings for this source-target combination...
          //
          boolean isApplicable = true;
          boolean avoidRelationship = false;
          boolean addRelationship = false;
          for (int i = 0; i < meta.getRelationshipMappings().size(); i++) {
            RelationshipMapping mapping = meta.getRelationshipMappings().get(i);
            GraphOutputData.RelelationshipMappingIndexes mappingIndexes =
                data.relMappingIndexes.get(i);
            boolean nodesMatch =
                sourceNodeName.equals(mapping.getSourceNode())
                    && targetNodeName.equals(mapping.getTargetNode());
            boolean relationshipMatch =
                relationship.getName().equals(mapping.getTargetRelationship());

            switch (mapping.getType()) {
              case NoMapping:
                // To be ignored, nothing is added
                break;
              case NoRelationship:
                // No relationship is ever desired between the 2 nodes
                //
                if (nodesMatch) {
                  avoidRelationship = true;
                }
                break;
              case All:
                // All relationships are added between the 2 nodes
                //
                if (nodesMatch) {
                  addRelationship = true;
                }
                break;
              case UsingValue:
                // Select this relationship only if the specified value is matching
                // with the input data
                //
                if (relationshipMatch) {
                  String value = rowMeta.getString(row, mappingIndexes.fieldIndex);
                  if (value != null && value.equals(mapping.getFieldValue())) {
                    addRelationship = true;
                  }
                  isApplicable = false;
                }
                break;
            }
          }

          SelectedRelationship selectedRelationship =
              new SelectedRelationship(source, target, relationship);
          if (addRelationship) {
            selectedRelationships.add(selectedRelationship);
          } else if (avoidRelationship) {
            avoidedRelationships.add(selectedRelationship);
          } else if (isApplicable) {
            // We didn't find an applicable mapping for this relationship. We want to add it
            // anyway because this is the default.  It's likely just a standard relationship
            // between 2 nodes.
            //
            selectedRelationships.add(new SelectedRelationship(source, target, relationship));
          }
        }
      }
    }

    if (log.isDebug()) {
      logDebug(
          "Found "
              + selectedRelationships.size()
              + " relationships to consider : "
              + selectedRelationships.toString());
      logDebug("Found " + ignored.size() + " nodes to ignore : " + ignored.toString());
    }

    return new SelectedNodesAndRelationships(
        nodesSet, selectedRelationships, avoidedRelationships, nodeProperties, ignored);
  }

  /**
   * Cache the relationships lists. This will speed up lookup in the graph model for larger models.
   *
   * @param sourceNodeName The name of the source node of the relationship to find
   * @param targetNodeName The name of the target node of the relationship to find
   * @return The list of relationships. If no relationships are found you get an empty list.
   */
  private List<GraphRelationship> findRelationships(String sourceNodeName, String targetNodeName) {
    Map<String, List<GraphRelationship>> targetsMap =
        data.relationshipsCache.computeIfAbsent(sourceNodeName, f -> new HashMap<>());
    return targetsMap.computeIfAbsent(
        targetNodeName, f -> data.graphModel.findRelationships(sourceNodeName, targetNodeName));
  }

  private String buildCypherKeyPattern(Object[] row, IRowMeta rowMeta) throws HopValueException {
    StringBuffer pattern = new StringBuffer();
    for (int index : data.fieldIndexes) {
      boolean isNull = rowMeta.isNull(row, index);
      pattern.append(isNull ? '0' : '1');
    }
    for (int i = 0; i < data.relMappingIndexes.size(); i++) {
      RelationshipMapping relationshipMapping = meta.getRelationshipMappings().get(i);
      GraphOutputData.RelelationshipMappingIndexes mappingIndexes = data.relMappingIndexes.get(i);
      if (relationshipMapping.getType() == RelationshipMappingType.UsingValue) {
        int index = mappingIndexes.fieldIndex;
        String value = Const.NVL(rowMeta.getString(row, index), "");
        pattern.append('-').append(value);
      }
    }
    for (int i = 0; i < data.nodeMappingIndexes.size(); i++) {
      int index = data.nodeMappingIndexes.get(i);
      if (index >= 0) {
        String value = Const.NVL(rowMeta.getString(row, index), "");
        pattern.append('-').append(value);
      }
    }
    return pattern.toString();
  }

  /**
   * Get the involved nodes both as source and target nodes. Specifying the source/target node is
   * only ever needed for self-relationships where The source and target node label is the same.
   *
   * @param row
   * @param rowMeta
   * @param nodeProperties
   * @return
   * @throws HopException
   */
  private Set<SelectedNode> getInvolvedNodes(
      Object[] row, IRowMeta rowMeta, List<NodeAndPropertyData> nodeProperties)
      throws HopException {

    List<FieldModelMapping> fieldModelMappings = meta.getFieldModelMappings();

    Set<SelectedNode> nodesSet = new HashSet<>();
    for (int f = 0; f < fieldModelMappings.size(); f++) {
      FieldModelMapping fieldModelMapping = fieldModelMappings.get(f);
      ModelTargetHint targetHint = fieldModelMapping.getTargetHint();

      if (fieldModelMapping.getTargetType() == ModelTargetType.Node) {
        // We pre-calculated the field indexes
        //
        int index = data.fieldIndexes[f];

        IValueMeta valueMeta = rowMeta.getValueMeta(index);
        Object valueData = row[index];

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

        // For this selected node we need to see if there are any specific node mappings.
        // These serve the purpose of helping with label selection if this is needed.
        //
        Set<String> labels = new HashSet<>();
        boolean implicitNodeMapping = true;
        for (int i = 0; i < meta.getNodeMappings().size(); i++) {
          NodeMapping mapping = meta.getNodeMappings().get(i);
          if (node.getName().equals(mapping.getTargetNode())) {
            implicitNodeMapping = false;
            // The mapping applies to this node
            //
            switch (mapping.getType()) {
              case All:
                labels.addAll(node.getLabels());
                break;
              case First:
                if (!node.getLabels().isEmpty()) {
                  labels.add(node.getLabels().get(0));
                }
                break;
              case UsingValue:
                // We select the label using the value in a specific field
                //
                int valueIndex = data.nodeMappingIndexes.get(i);
                if (rowMeta.isNull(row, valueIndex)) {
                  throw new HopException(
                      "Null value found for field "
                          + mapping.getFieldName()
                          + " in node mapping "
                          + mapping);
                }
                String value = rowMeta.getString(row, valueIndex);

                // With this value we can now look up the target label
                //
                Map<String, String> valueLabelMap = data.nodeValueLabelMap.get(node.getName());
                if (valueLabelMap == null) {
                  throw new HopException(
                      "No field-to-label mapping was specified for node " + node.getName());
                }
                String label = valueLabelMap.get(value);
                if (label != null) {
                  labels.add(label);
                }
                break;
            }
          }
        }

        // If we didn't see an explicit node label mapping we just assume all labels
        // from the model need to be applied.
        //
        if (implicitNodeMapping) {
          labels.addAll(node.getLabels());
        }

        if (labels.isEmpty()) {
          throw new HopException("No node labels could be found for node: " + node.getName());
        }

        SelectedNode selectedNode = new SelectedNode(node, targetHint, new ArrayList<>(labels));
        nodesSet.add(selectedNode);

        nodeProperties.add(
            new NodeAndPropertyData(selectedNode, graphProperty, valueMeta, valueData, index));
      }
    }
    return nodesSet;
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
      SelectedNode selectedNode,
      Set<SelectedNode> handled,
      Set<SelectedNode> ignored,
      AtomicInteger parameterIndex,
      AtomicInteger nodeIndex,
      Map<SelectedNode, Integer> nodeIndexMap,
      List<NodeAndPropertyData> nodeProperties,
      Map<String, Object> parameters,
      CypherParameters cypherParameters)
      throws HopValueException {
    if (ignored.contains(selectedNode) || handled.contains(selectedNode)) {
      return;
    }
    // Don't update twice.
    //
    handled.add(selectedNode);
    nodeIndexMap.put(selectedNode, nodeIndex.incrementAndGet());

    GraphNode node = selectedNode.getNode();

    // Calculate the node labels clause.
    // The list of labels was calculated during selection in selectNodesAndRelationships()
    //
    StringBuilder nodeLabels = new StringBuilder();
    for (String nodeLabel : selectedNode.getLabels()) {
      nodeLabels.append(":");
      nodeLabels.append(nodeLabel);
    }

    StringBuilder matchCypher = new StringBuilder();

    String nodeAlias = "node" + nodeIndex;

    cypher.append("MERGE (").append(nodeAlias).append(nodeLabels).append(" { ");

    updateUsageMap(node.getLabels(), GraphUsage.NODE_UPDATE);

    if (log.isDebug()) {
      logBasic(" - node merge : " + node.getName());
    }

    // Look up the properties to update in the node
    //
    boolean firstPrimary = true;
    boolean firstMatch = true;
    for (NodeAndPropertyData napd : nodeProperties) {
      if (napd.node.equals(selectedNode)) {
        // Handle the property
        //
        parameterIndex.incrementAndGet();
        boolean isNull = napd.sourceValueMeta.isNull(napd.sourceValueData);
        String parameterName = "param" + parameterIndex;

        if (napd.property.isPrimary()) {

          if (!firstPrimary) {
            cypher.append(", ");
          }
          cypher
              .append(napd.property.getName())
              .append(" : ")
              .append(buildParameterClause(parameterName))
              .append(" ");

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

          matchCypher.append(nodeAlias).append(".").append(napd.property.getName()).append(" = ");
          if (isNull) {
            matchCypher.append("NULL ");
          } else {
            matchCypher.append(buildParameterClause(parameterName)).append(" ");
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
    cypher.append("}) ").append(Const.CR);

    // Add a SET clause if there are any non-primary key fields to update
    //
    if (matchCypher.length() > 0) {
      cypher.append(matchCypher).append(Const.CR);
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
  protected void updateUsageMap(List<String> nodeLabels, GraphUsage usage) {
    Map<String, Set<String>> transformsMap =
        data.usageMap.computeIfAbsent(usage.name(), k -> new HashMap<>());
    Set<String> labelSet = transformsMap.computeIfAbsent(getTransformName(), k -> new HashSet<>());

    for (String label : nodeLabels) {
      if (StringUtils.isNotEmpty(label)) {
        labelSet.add(label);
      }
    }
  }

  /**
   * Generate the graph data containing the update
   *
   * @param row The input row
   * @param rowMeta the input row metadata
   * @return The graph with nodes and relationships
   */
  protected GraphData getGraphData(Object[] row, IRowMeta rowMeta) throws HopException {

    GraphData graphData = new GraphData();
    graphData.setSourcePipelineName(getPipelineMeta().getName());
    graphData.setSourceTransformName(getTransformMeta().getName());

    SelectedNodesAndRelationships nar = selectNodesAndRelationships(rowMeta, row);

    // Now we have a bunch of Node-Pairs to update...
    //
    AtomicInteger nodeIndex = new AtomicInteger(0);

    Set<SelectedNode> handled = new HashSet<>();
    Map<SelectedNode, Integer> nodeIndexMap = new HashMap<>();

    // No relationships case: only node(s)
    //
    if (nar.relationships.isEmpty()) {
      if (nar.nodes.isEmpty()) {
        throw new HopException(
            "We didn't find a node to write to.  Did you specify a field mapping to node properties?");
      }
      if (nar.nodes.size() > 1) {
        log.logBasic("Warning: writing to multiple nodes but not to any relationships");
      }

      for (SelectedNode node : nar.nodes) {
        GraphNodeData nodeData =
            getGraphNodeData(
                node, handled, nar.ignored, nodeIndex, nodeIndexMap, nar.nodeProperties);
        if (nodeData != null) {
          graphData.getNodes().add(nodeData);
        }
      }
    } else {
      for (SelectedRelationship relationship : nar.relationships) {
        if (nar.avoided.contains(relationship)) {
          continue;
        }

        SelectedNode nodeSource = relationship.getSourceNode();
        SelectedNode nodeTarget = relationship.getTargetNode();

        for (SelectedNode node : new SelectedNode[] {nodeSource, nodeTarget}) {
          GraphNodeData nodeData =
              getGraphNodeData(
                  node, handled, nar.ignored, nodeIndex, nodeIndexMap, nar.nodeProperties);
          if (nodeData != null) {
            graphData.getNodes().add(nodeData);
          }
        }

        // Now add the relationship...
        //
        if (nodeIndexMap.get(nodeSource) != null && nodeIndexMap.get(nodeTarget) != null) {
          String sourceNodeId = getGraphNodeDataId(nodeSource, nar.nodeProperties);
          String targetNodeId = getGraphNodeDataId(nodeTarget, nar.nodeProperties);

          String id = sourceNodeId + " -> " + targetNodeId;

          GraphRelationshipData relationshipData = new GraphRelationshipData();
          relationshipData.setId(id);
          relationshipData.setLabel(relationship.getRelationship().getLabel());
          relationshipData.setSourceNodeId(sourceNodeId);
          relationshipData.setTargetNodeId(targetNodeId);

          // The property set ID is simply the name of the GraphRelationship (metadata)
          //
          relationshipData.setPropertySetId(relationship.getRelationship().getName());

          // Also add the optional properties...
          //
          List<GraphProperty> relProps = new ArrayList<>();
          List<Integer> relPropIndexes = new ArrayList<>();
          Map<GraphProperty, Integer> propertyIndexMap =
              data.relationshipPropertyIndexMap.get(relationship.getRelationship().getName());
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

  /**
   * Evaluate if the node property is primary and null. In that case, we ignore these nodes.
   *
   * @param nodeProperties
   * @return
   * @throws HopValueException
   */
  private Set<SelectedNode> getIgnoredNodesWithPkNull(List<NodeAndPropertyData> nodeProperties)
      throws HopValueException {
    Set<SelectedNode> ignored = new HashSet<>();
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
    return ignored;
  }

  private GraphNodeData getGraphNodeData(
      SelectedNode node,
      Set<SelectedNode> handled,
      Set<SelectedNode> ignored,
      AtomicInteger nodeIndex,
      Map<SelectedNode, Integer> nodeIndexMap,
      List<NodeAndPropertyData> nodeProperties)
      throws HopValueException {
    if (ignored.contains(node) || handled.contains(node)) {
      return null;
    }

    GraphNodeData graphNodeData = new GraphNodeData();

    // The property set ID is simply the name of the GraphNode (metadata)
    //
    graphNodeData.setPropertySetId(node.getNode().getName());

    // Don't update twice.
    //
    handled.add(node);
    nodeIndexMap.put(node, nodeIndex.incrementAndGet());

    // Calculate the node labels
    //
    graphNodeData.getLabels().addAll(node.getNode().getLabels());

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

  public String getGraphNodeDataId(SelectedNode node, List<NodeAndPropertyData> nodeProperties)
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
