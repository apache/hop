/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.neo4j.transforms.cypherbuilder.operation;

import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.neo4j.transforms.cypherbuilder.Parameter;

public class EdgeMatchOperation extends BaseOperation {

  @HopMetadataProperty protected String sourceAlias;

  @HopMetadataProperty protected String edgeAlias;

  @HopMetadataProperty protected String edgeLabel;

  @HopMetadataProperty protected String targetAlias;

  public EdgeMatchOperation() {
    super(OperationType.EDGE_MATCH);
  }

  protected EdgeMatchOperation(OperationType operationType) {
    super(operationType);
  }

  public EdgeMatchOperation(
      String sourceAlias, String edgeAlias, String edgeLabel, String targetAlias) {
    this();
    this.sourceAlias = sourceAlias;
    this.edgeAlias = edgeAlias;
    this.edgeLabel = edgeLabel;
    this.targetAlias = targetAlias;
  }

  public EdgeMatchOperation(EdgeMatchOperation o) {
    super(o.operationType);
    this.sourceAlias = o.sourceAlias;
    this.edgeAlias = o.edgeAlias;
    this.edgeLabel = o.edgeLabel;
    this.targetAlias = o.targetAlias;
  }

  @Override
  public EdgeMatchOperation clone() {
    return new EdgeMatchOperation(this);
  }

  @Override
  public String getCypherClause(String unwindAlias, List<Parameter> parameters)
      throws HopException {
    // MATCH
    StringBuilder cypher = new StringBuilder(operationType.keyWord());
    // (a)
    cypher.append("(").append(sourceAlias).append(")");
    // -[r:REL_LABEL]->
    cypher.append("-[").append(edgeAlias).append(":").append(edgeLabel).append("]->");
    // (b)
    cypher.append("(").append(targetAlias).append(") ");
    return cypher.toString();
  }

  @Override
  public boolean needsWriteTransaction() {
    return false;
  }

  /**
   * Gets sourceAlias
   *
   * @return value of sourceAlias
   */
  public String getSourceAlias() {
    return sourceAlias;
  }

  /**
   * Sets sourceAlias
   *
   * @param sourceAlias value of sourceAlias
   */
  public void setSourceAlias(String sourceAlias) {
    this.sourceAlias = sourceAlias;
  }

  /**
   * Gets edgeAlias
   *
   * @return value of edgeAlias
   */
  public String getEdgeAlias() {
    return edgeAlias;
  }

  /**
   * Sets edgeAlias
   *
   * @param edgeAlias value of edgeAlias
   */
  public void setEdgeAlias(String edgeAlias) {
    this.edgeAlias = edgeAlias;
  }

  /**
   * Gets edgeLabel
   *
   * @return value of edgeLabel
   */
  public String getEdgeLabel() {
    return edgeLabel;
  }

  /**
   * Sets edgeLabel
   *
   * @param edgeLabel value of edgeLabel
   */
  public void setEdgeLabel(String edgeLabel) {
    this.edgeLabel = edgeLabel;
  }

  /**
   * Gets targetAlias
   *
   * @return value of targetAlias
   */
  public String getTargetAlias() {
    return targetAlias;
  }

  /**
   * Sets targetAlias
   *
   * @param targetAlias value of targetAlias
   */
  public void setTargetAlias(String targetAlias) {
    this.targetAlias = targetAlias;
  }
}
