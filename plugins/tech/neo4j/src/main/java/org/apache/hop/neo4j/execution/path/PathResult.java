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

package org.apache.hop.neo4j.execution.path;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.Const;

public class PathResult {

  public static final String CONST_TYPE = "\", type : \"";
  public static final String CONST_ID = "\", id : \"";
  private String id;
  private String name;
  private String type;
  private String copy;
  private Date registrationDate;
  private Boolean failed;

  private List<List<PathResult>> shortestPaths;
  private Boolean root;

  public PathResult() {
    shortestPaths = new ArrayList<>();
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public String getId() {
    return id;
  }

  /**
   * @param id The id to set
   */
  public void setId(String id) {
    this.id = id;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public String getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * Gets registrationDate
   *
   * @return value of registrationDate
   */
  public Date getRegistrationDate() {
    return registrationDate;
  }

  /**
   * @param registrationDate The registrationDate to set
   */
  public void setRegistrationDate(Date registrationDate) {
    this.registrationDate = registrationDate;
  }

  /**
   * Gets root
   *
   * @return value of root
   */
  public Boolean getRoot() {
    return root;
  }

  /**
   * Gets copy
   *
   * @return value of copy
   */
  public String getCopy() {
    return copy;
  }

  /**
   * Sets copy
   *
   * @param copy value of copy
   */
  public void setCopy(String copy) {
    this.copy = copy;
  }

  /**
   * Gets failed
   *
   * @return value of failed
   */
  public Boolean getFailed() {
    return failed;
  }

  /**
   * Sets failed
   *
   * @param failed value of failed
   */
  public void setFailed(Boolean failed) {
    this.failed = failed;
  }

  /**
   * Gets shortestPaths
   *
   * @return value of shortestPaths
   */
  public List<List<PathResult>> getShortestPaths() {
    return shortestPaths;
  }

  /**
   * @param shortestPaths The shortestPaths to set
   */
  public void setShortestPaths(List<List<PathResult>> shortestPaths) {
    this.shortestPaths = shortestPaths;
  }

  public void setRoot(Boolean root) {
    this.root = root;
  }

  public Boolean isRoot() {
    return root;
  }

  public String getExecutionInfoCommand() {
    StringBuilder cmd = new StringBuilder();

    cmd.append(
            "MATCH(ex:Execution { name : \""
                + getName()
                + CONST_TYPE
                + getType()
                + CONST_ID
                + getId()
                + "\"}) ")
        .append(Const.CR);
    cmd.append("RETURN ex ").append(Const.CR);

    return cmd.toString();
  }

  public String getErrorPathCommand() {
    StringBuilder cmd = new StringBuilder();

    cmd.append(
            "MATCH(top:Execution { name : \""
                + getName()
                + CONST_TYPE
                + getType()
                + CONST_ID
                + getId()
                + "\"})-[rel:EXECUTES*]-(err:Execution) ")
        .append(Const.CR);
    cmd.append("   , p=shortestpath((top)-[:EXECUTES*]-(err)) ").append(Const.CR);
    cmd.append("WHERE top.registrationDate IS NOT NULL ").append(Const.CR);
    cmd.append("  AND err.errors > 0 ").append(Const.CR);
    cmd.append("  AND size((err)-[:EXECUTES]->())=0 ").append(Const.CR);
    cmd.append("RETURN p ").append(Const.CR);
    cmd.append("ORDER BY size(RELATIONSHIPS(p)) DESC ").append(Const.CR);
    cmd.append("LIMIT 5").append(Const.CR);

    return cmd.toString();
  }

  public String getErrorPathWithMetadataCommand(int pathIndex) {
    StringBuilder cmd = new StringBuilder();

    cmd.append(
            "MATCH(top:Execution { name : \""
                + getName()
                + CONST_TYPE
                + getType()
                + CONST_ID
                + getId()
                + "\"})-[rel:EXECUTES*]-(err:Execution) ")
        .append(Const.CR);
    cmd.append("   , p=shortestpath((top)-[:EXECUTES*]-(err)) ").append(Const.CR);
    cmd.append("WHERE top.registrationDate IS NOT NULL ").append(Const.CR);
    cmd.append("  AND err.errors > 0 ").append(Const.CR);
    cmd.append("  AND size((err)-[:EXECUTES]->())=0 ").append(Const.CR);

    // Now link the metadata...
    //
    StringBuilder returns = new StringBuilder("RETURN p");
    int matchIndex = 1;

    List<PathResult> shortestPath = getShortestPaths().get(pathIndex);

    for (PathResult result : shortestPath) {
      String metaLabel =
          switch (result.getType()) {
            case "PIPELINE" -> "Pipeline";
            case "WORKFLOW" -> "Workflow";
            case "ACTION" -> "Action";
            case "TRANSFORM" -> "Transform";
            default -> null;
          };
      if (metaLabel != null) {
        cmd.append(
                "MATCH (:Execution { type : \""
                    + result.getType()
                    + CONST_ID
                    + result.getId()
                    + "\"})-[metaRel"
                    + matchIndex
                    + "]->(meta"
                    + matchIndex
                    + ":"
                    + metaLabel
                    + ") ")
            .append(Const.CR);
        returns.append(", metaRel" + matchIndex + ", meta" + matchIndex);
        matchIndex++;
      }
    }
    cmd.append(returns.toString()).append(" ").append(Const.CR);
    cmd.append("LIMIT 1 ").append(Const.CR);

    return cmd.toString();
  }
}
