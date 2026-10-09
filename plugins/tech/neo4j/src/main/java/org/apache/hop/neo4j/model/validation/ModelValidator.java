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

package org.apache.hop.neo4j.model.validation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.graph.GraphIndex;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.neo4j.model.GraphModel;
import org.apache.hop.neo4j.model.GraphNode;
import org.apache.hop.neo4j.model.GraphProperty;

/** Validates the data input and the indexes of a graph database against a graph model. */
public class ModelValidator {

  private List<NodeProperty> usedNodeProperties;
  private GraphModel graphModel;
  private List<GraphIndex> indexesList;

  public ModelValidator() {
    indexesList = new ArrayList<>();
    usedNodeProperties = new ArrayList<>();
  }

  public ModelValidator(GraphModel graphModel, List<NodeProperty> usedNodeProperties) {
    this();
    this.graphModel = graphModel;
    this.usedNodeProperties = usedNodeProperties;
  }

  /**
   * Validate the used nodes and properties against the model and, if the database can list them,
   * the existence of the indexes and constraints the model asks for, before any load takes place.
   *
   * @param log The log channel to write to when there are validation errors
   * @param indexes The indexes and unique constraints of the database, null if it can't list them:
   *     then only the model is validated
   * @return the number of validation errors
   */
  public int validateBeforeLoad(ILogChannel log, List<GraphIndex> indexes) {
    int nrErrors = 0;

    boolean validatingIndexes = indexes != null;
    if (validatingIndexes) {
      indexesList = indexes;
    } else {
      log.logBasic(
          "This graph database can't list its indexes: only the use of the graph model is"
              + " validated, not its indexes and unique constraints");
    }

    for (NodeProperty nodeProperty : usedNodeProperties) {
      GraphNode node = graphModel.findNode(nodeProperty.getNodeName());
      if (node == null) {
        log.logError(
            "Used node '"
                + nodeProperty.getNodeName()
                + "' could not be found in model '"
                + graphModel.getName());
        nrErrors++;
      } else {
        GraphProperty property = node.findProperty(nodeProperty.getPropertyName());
        if (property == null) {
          log.logError(
              "Used node property "
                  + nodeProperty.getNodeName()
                  + "."
                  + nodeProperty.getPropertyName()
                  + " could not be found in model '"
                  + graphModel.getName());
          nrErrors++;
        } else {
          if (validatingIndexes && property.isIndexed()) {
            nrErrors += validateNodePropertyIndexed(log, node, property, false);
          }
          if (validatingIndexes && property.isUnique()) {
            nrErrors += validateNodePropertyIndexed(log, node, property, true);
          }
        }
      }
    }

    nrErrors += validateMandatoryFields(log);

    return nrErrors;
  }

  /**
   * See if all the used node and relationship properties are used
   *
   * @param log
   * @return the number of validation errors.
   */
  private int validateMandatoryFields(ILogChannel log) {
    int nrErrors = 0;
    Set<String> nodeNames = new HashSet<>();
    for (NodeProperty nodeProperty : usedNodeProperties) {
      nodeNames.add(nodeProperty.getNodeName());
    }

    // For every used node, see if the mandatory properties are present
    //
    for (String nodeName : nodeNames) {
      GraphNode node = graphModel.findNode(nodeName);
      if (node != null) {
        for (GraphProperty nodeProperty : node.getProperties()) {
          if (nodeProperty.isMandatory()) {
            NodeProperty usedProperty = findUsedProperty(node.getName(), nodeProperty.getName());
            if (usedProperty == null) {
              log.logError(
                  "Node property "
                      + node.getName()
                      + "."
                      + nodeProperty.getName()
                      + " is mandatory but not used.");
              nrErrors++;
            }
          }
        }
      }
    }

    return nrErrors;
  }

  private NodeProperty findUsedProperty(String nodeName, String propertyName) {
    for (NodeProperty nodeProperty : usedNodeProperties) {
      if (nodeProperty.getNodeName().equals(nodeName)
          && nodeProperty.getPropertyName().equals(propertyName)) {
        return nodeProperty;
      }
    }
    return null;
  }

  /**
   * See if the specified node property is indexes. If it's not, write an error to the log and
   * return true
   *
   * @param log
   * @param node
   * @param property
   * @return the number of validation errors.
   */
  private int validateNodePropertyIndexed(
      ILogChannel log, GraphNode node, GraphProperty property, boolean unique) {
    int nrErrors = 0;
    boolean found = false;
    for (GraphIndex index : indexesList) {
      if (!index.relationship() && (!unique || index.unique())) {
        for (String label : node.getLabels()) {
          if (index.covers(label, property.getName())) {
            found = true;
          }
        }
      }
    }
    if (!found) {
      log.logError(
          "Property '"
              + property.getName()
              + "' of node '"
              + node.getName()
              + "' doesn't seem to be "
              + (unique ? "uniquely " : "")
              + "indexed.");
      nrErrors++;
    }

    return nrErrors;
  }

  /**
   * Gets usedNodeProperties
   *
   * @return value of usedNodeProperties
   */
  public List<NodeProperty> getUsedNodeProperties() {
    return usedNodeProperties;
  }

  /**
   * @param usedNodeProperties The usedNodeProperties to set
   */
  public void setUsedNodeProperties(List<NodeProperty> usedNodeProperties) {
    this.usedNodeProperties = usedNodeProperties;
  }

  /**
   * Gets graphModel
   *
   * @return value of graphModel
   */
  public GraphModel getGraphModel() {
    return graphModel;
  }

  /**
   * @param graphModel The graphModel to set
   */
  public void setGraphModel(GraphModel graphModel) {
    this.graphModel = graphModel;
  }

  /**
   * Gets indexesList
   *
   * @return value of indexesList
   */
  public List<GraphIndex> getIndexesList() {
    return indexesList;
  }

  /**
   * @param indexesList The indexesList to set
   */
  public void setIndexesList(List<GraphIndex> indexesList) {
    this.indexesList = indexesList;
  }
}
