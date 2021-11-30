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

import org.apache.hop.neo4j.model.GraphRelationship;

import java.util.Objects;

public class SelectedRelationship {
  private SelectedNode sourceNode;
  private SelectedNode targetNode;
  private GraphRelationship relationship;

  public SelectedRelationship(
      SelectedNode sourceNode, SelectedNode targetNode, GraphRelationship relationship) {
    this.sourceNode = sourceNode;
    this.targetNode = targetNode;
    this.relationship = relationship;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SelectedRelationship that = (SelectedRelationship) o;
    return Objects.equals(sourceNode, that.sourceNode)
        && Objects.equals(targetNode, that.targetNode)
        && Objects.equals(relationship, that.relationship);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceNode, targetNode, relationship);
  }

  @Override
  public String toString() {
    return "SelectedRelationship{"
        + "sourceNode="
        + sourceNode
        + ", targetNode="
        + targetNode
        + '}';
  }

  /**
   * Gets sourceNode
   *
   * @return value of sourceNode
   */
  public SelectedNode getSourceNode() {
    return sourceNode;
  }

  /** @param sourceNode The sourceNode to set */
  public void setSourceNode(SelectedNode sourceNode) {
    this.sourceNode = sourceNode;
  }

  /**
   * Gets targetNode
   *
   * @return value of targetNode
   */
  public SelectedNode getTargetNode() {
    return targetNode;
  }

  /** @param targetNode The targetNode to set */
  public void setTargetNode(SelectedNode targetNode) {
    this.targetNode = targetNode;
  }

  /**
   * Gets relationship
   *
   * @return value of relationship
   */
  public GraphRelationship getRelationship() {
    return relationship;
  }

  /** @param relationship The relationship to set */
  public void setRelationship(GraphRelationship relationship) {
    this.relationship = relationship;
  }
}
