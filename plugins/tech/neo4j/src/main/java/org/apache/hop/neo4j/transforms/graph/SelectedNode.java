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

import org.apache.hop.neo4j.model.GraphNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SelectedNode {
  private GraphNode node;
  private ModelTargetHint hint;
  private List<String> labels;

  public SelectedNode() {
    this.labels = new ArrayList<>();
  }

  public SelectedNode(GraphNode node, ModelTargetHint hint, List<String> labels) {
    this.node = node;
    this.hint = hint;
    this.labels = labels;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SelectedNode that = (SelectedNode) o;
    return Objects.equals(node, that.node)
        && hint == that.hint
        && Objects.equals(labels, that.labels);
  }

  @Override
  public int hashCode() {
    return Objects.hash(node, hint, labels);
  }

  @Override
  public String toString() {
    return "SelectedNode{" + "node=" + node + ", hint=" + hint + ", labels=" + labels + '}';
  }

  /**
   * Gets node
   *
   * @return value of node
   */
  public GraphNode getNode() {
    return node;
  }

  /** @param node The node to set */
  public void setNode(GraphNode node) {
    this.node = node;
  }

  /**
   * Gets hint
   *
   * @return value of hint
   */
  public ModelTargetHint getHint() {
    return hint;
  }

  /** @param hint The hint to set */
  public void setHint(ModelTargetHint hint) {
    this.hint = hint;
  }

  /**
   * Gets labels
   *
   * @return value of labels
   */
  public List<String> getLabels() {
    return labels;
  }

  /** @param labels The labels to set */
  public void setLabels(List<String> labels) {
    this.labels = labels;
  }
}
