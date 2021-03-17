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

import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.neo4j.model.GraphModel;
import org.apache.hop.neo4j.model.GraphNode;
import org.apache.hop.neo4j.model.GraphProperty;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** This class will help you validate data input and your Neo4j data against a Graph Model */
public class ModelValidator {

  private List<NodeProperty> usedNodeProperties;
  private GraphModel graphModel;
  private List<IndexDetails> indexesList;
  private List<ConstraintDetails> constraintsList;

  public ModelValidator() {
    indexesList = new ArrayList<>();
    constraintsList = new ArrayList<>();
    usedNodeProperties = new ArrayList<>();
  }

  public ModelValidator(GraphModel graphModel, List<NodeProperty> usedNodeProperties) {
    this();
    this.graphModel = graphModel;
    this.usedNodeProperties = usedNodeProperties;
  }

  /**
   * Validate the existence of indexes and constraints in the Neo4j database before any load takes
   * place
   *
   * @param log The log channel to write to when there are validation errors. Validation successes
   *     are logged in Detailed
   * @param session The Neo4j session to use to validate
   * @return the number of validation errors
   */
  public int validateBeforeLoad(ILogChannel log, Session session) {
    // Validate the nodes
    //
    int nrErrors = 0;

    readIndexesData(session);
    readConstraintsData(session);

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
          if (property.isIndexed()) {
            nrErrors += validateNodePropertyIndexed(log, node, property, false);
          }
          if (property.isUnique()) {
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
    for (IndexDetails indexDetails : indexesList) {
      if (!unique || "UNIQUE".equalsIgnoreCase(indexDetails.getUniqueness())) {
        for (String label : node.getLabels()) {
          if (indexDetails.getLabelsOrTypes().contains(label)) {
            if (indexDetails.getProperties().contains(property.getName())) {
              found = true;
            }
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

  private void readIndexesData(Session session) {
    indexesList =
        session.readTransaction(
            transaction -> {
              List<IndexDetails> list = new ArrayList<>();
              Result result = transaction.run("call db.indexes()");
              while (result.hasNext()) {
                list.add(new IndexDetails(result.next()));
              }
              return list;
            });
  }

  private void readConstraintsData(Session session) {
    constraintsList =
        session.readTransaction(
            new TransactionWork<List<ConstraintDetails>>() {
              @Override
              public List<ConstraintDetails> execute(Transaction transaction) {
                List<ConstraintDetails> list = new ArrayList<>();
                Result result = transaction.run("call db.constraints()");
                while (result.hasNext()) {
                  list.add(new ConstraintDetails(result.next()));
                }
                return list;
              }
            });
  }

  /**
   * Gets usedNodeProperties
   *
   * @return value of usedNodeProperties
   */
  public List<NodeProperty> getUsedNodeProperties() {
    return usedNodeProperties;
  }

  /** @param usedNodeProperties The usedNodeProperties to set */
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

  /** @param graphModel The graphModel to set */
  public void setGraphModel(GraphModel graphModel) {
    this.graphModel = graphModel;
  }

  /**
   * Gets indexesList
   *
   * @return value of indexesList
   */
  public List<IndexDetails> getIndexesList() {
    return indexesList;
  }

  /** @param indexesList The indexesList to set */
  public void setIndexesList(List<IndexDetails> indexesList) {
    this.indexesList = indexesList;
  }

  /**
   * Gets constraintsList
   *
   * @return value of constraintsList
   */
  public List<ConstraintDetails> getConstraintsList() {
    return constraintsList;
  }

  /** @param constraintsList The constraintsList to set */
  public void setConstraintsList(List<ConstraintDetails> constraintsList) {
    this.constraintsList = constraintsList;
  }
}
